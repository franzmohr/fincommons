"""The Kenneth R. French Data Library: one long table in the Silver layer.

Every file of the Bronze layer is read into the same seven columns -- the
reference date, the file, a code for the table within it, the frequency, the
series, the value and the title of the table -- so that a factor, the return
of a portfolio and a breakpoint are all found the same way. How a file is read
is :mod:`fincommons.frenchdatalibrary.parse`; this module runs it over the
Bronze layer and writes the result.

**The table is rebuilt file by file, and only for the files that changed.**
Each file is a partition of its own, ``DATASET``, and each one that the Bronze
layer holds a newer collection of is parsed again and replaces its partition,
leaving every other partition as it was. Kenneth French rebuilds the whole
library each month, so in the month's first run that is every file; but a run
in which a few files failed to download, or a run repeated after one that
failed half-way, parses only what it has to. Which collection each partition
was built from is recorded in a mark beside the table, written after the
partition is in place, so that a run that dies leaves the mark behind and the
next run builds the file again.

The parsing runs on the executors, since the library is about a hundred
million values and the driver is not the place to hold them. Each file reaches
them as one row holding its lines, and its values come back in chunks of
:data:`CHUNK_ROWS` through :meth:`pyspark.sql.DataFrame.mapInPandas`. It used
to come back whole, as the one frame :meth:`~pyspark.sql.GroupedData.applyInPandas`
returns per group, and a daily file of a hundred portfolios is a frame of more
than a gigabyte: the first collection into a real lake failed on it, out of the
gigabyte of direct memory a session has by default. A file
that cannot be parsed does not fail the others: its error is carried back to
the driver as a row of its own, logged with the traceback of the executor at
``ERROR`` and counted, and the file's partition is left as it was.
"""

import functools
import logging
import traceback

import pandas
import pyspark.sql.functions as func
from pyspark import StorageLevel
from pyspark.sql.types import StringType, StructField, StructType

from ..log import context, pipeline_run
from ..storage import replace_silver_partitions, silver_format
from .common import *
from .parse import TABLE_CODES, parse_lines

logger = logging.getLogger(__name__)

#: How many files are parsed and written in one go. The parsed values of a
#: batch are kept until its partitions are written, so the batch bounds what a
#: run holds at once: the largest file of the library is about ten million
#: values, the whole library about a hundred million.
BATCH_SIZE = 50

#: How many lines of Bronze text one batch may hold at most. A batch gathers
#: the lines of each of its files into a single row before parsing it, so its
#: size in memory follows its lines rather than its number of files: fifty
#: monthly files are a few hundred thousand short lines, while one daily file
#: of a hundred portfolios is a hundred thousand lines of a kilobyte each. A
#: batch of fifty files that took several of those together ran the session
#: out of heap on the first collection into a real lake. A file larger than
#: the budget is a batch of its own.
BATCH_LINES = 400_000

#: How many parsed values travel back from the executors in one frame. Each
#: frame becomes one Arrow batch, which has to fit into the direct memory of
#: the session; a quarter of a million rows is a few tens of megabytes.
CHUNK_ROWS = 250_000

#: The columns the parsing hands back: those of the table, and the error of a
#: file that could not be parsed, which travels in a row of its own.
_PARSED_SCHEMA = StructType(
    [StructField(f.name, f.dataType, True) for f in silver_schema.fields]
    + [StructField("_ERROR", StringType(), True)]
)

_COLUMNS = ["REFDT", "DATASET", "TABLE", "FREQUENCY", "SERIES", "VALUE",
            "TITLE", "_ERROR"]


def _parse_file(dataset, lines, chunk_rows=CHUNK_ROWS):
    """The values of one file, in chunks of at most ``chunk_rows`` rows.

    It never raises. A file that fails comes back as a single row that
    carries the traceback in ``_ERROR`` and nothing else, because an exception
    raised here would fail the whole Spark job, and with it every other file
    of the batch.

    Parameters
    ----------
    dataset: str
        The name of the file.
    lines: list of str
        Its lines, in the order of the file.
    """
    try:
        values = parse_lines(dataset, list(lines))
    except Exception:
        yield pandas.DataFrame({
            "REFDT": [None], "DATASET": [dataset], "TABLE": [None],
            "FREQUENCY": [None], "SERIES": [None], "VALUE": [None],
            "TITLE": [None], "_ERROR": [traceback.format_exc()],
        })
        return
    values["DATASET"] = dataset
    values["REFDT"] = values["REFDT"].dt.date
    values["_ERROR"] = None
    values = values[_COLUMNS]
    # pandas 3 keeps text in Arrow arrays, and a column the parser put
    # together from several tables is an Arrow array in several chunks, which
    # PySpark cannot hand back to Spark. As Python objects it travels as it
    # does under pandas 2.
    values = values.astype({
        column: object for column in values.columns
        if isinstance(values[column].dtype, pandas.StringDtype)
    })
    for start in range(0, len(values), chunk_rows):
        yield values.iloc[start:start + chunk_rows]


def _parse_files(frames, chunk_rows=CHUNK_ROWS):
    """Parse the files of a partition, as :meth:`mapInPandas` hands them over.

    Each row holds one file: its name and its lines, already in order.
    """
    for frame in frames:
        for dataset, lines in zip(frame["DATASET"], frame["LINES"]):
            yield from _parse_file(dataset, lines, chunk_rows)


def pending_datasets(config_path, full=False, datasets=None):
    """The files of the Bronze layer the table has to be rebuilt for.

    A file is pending where the Bronze layer holds a collection of it the
    table was not built from, by the SHA-256 of its text, or where ``full``
    asks for every file.
    """
    collections = read_collections(config_path)
    if datasets is not None:
        collections = {d: c for d, c in collections.items() if d in set(datasets)}
    return {
        dataset: collection
        for dataset, collection in collections.items()
        if full or read_watermark(config_path, dataset) != collection["sha256"]
    }


def batches(pending, size=None, lines=None):
    """The pending files in batches of at most ``size`` files and ``lines`` lines.

    The files are taken in order of their name, and a batch is closed as soon
    as the next file would take it over either bound, so a file larger than
    ``lines`` makes a batch of its own. The line count is the one recorded with
    the collection; a record without one counts as nothing.

    Parameters
    ----------
    pending: dict
        The collection of each pending file, by name, as
        :func:`pending_datasets` returns them.
    size, lines: int, optional
        The bounds, :data:`BATCH_SIZE` and :data:`BATCH_LINES` by default.
    """
    size = BATCH_SIZE if size is None else size
    lines = BATCH_LINES if lines is None else lines
    batch, held = [], 0
    for name in sorted(pending):
        count = int(pending[name].get("lines") or 0)
        if batch and (len(batch) >= size or held + count > lines):
            yield batch
            batch, held = [], 0
        batch.append(name)
        held += count
    if batch:
        yield batch


def update_silver_frenchdatalibrary(spark, config_path, full=False,
                                    file_format=None, datasets=None):
    """Rebuild the partitions of the files whose collection has changed.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Rebuild every file the Bronze layer holds rather than those whose
        collection has changed. A scheduled run never needs it; it is how the
        table is brought in line with a new version of the parser.
    file_format: str, optional
        Format the table is written in, ``'parquet'`` or ``'delta'``; see
        :func:`fincommons.storage.replace_silver_partitions`. Where it is left
        out the configuration file and the table on disk decide.
    datasets: list of str, optional
        Restrict the rebuild to these files.

    Returns
    -------
    dict
        The outcome of the stage: ``written`` counts the files whose
        partition was replaced and ``rows`` the rows written, ``failed`` the
        files that failed and ``failed_sources`` names them as
        ``silver/<name>``.
    """
    file_format = silver_format(ref_silver_frenchdatalibrary(config_path), file_format)
    with pipeline_run(
        logger,
        "update_silver_frenchdatalibrary",
        module="frenchdatalibrary",
        layer="silver",
        config_path=config_path,
        file_format=file_format,
    ) as outcome:
        outcome.update(written=0, rows=0, failed=0, failed_sources=[])
        pending = pending_datasets(config_path, full, datasets)
        if not pending:
            logger.info(
                "No file has changed since the table was built, nothing to do",
                extra=context(module="frenchdatalibrary", layer="silver"),
            )
            return outcome

        for batch in batches(pending):
            _update_batch(spark, config_path, batch, pending, file_format, outcome)

        if outcome["failed"]:
            logger.warning(
                "Files were set aside, the run did not update them",
                extra=context(module="frenchdatalibrary", layer="silver",
                              failed=outcome["failed"],
                              sources=",".join(outcome["failed_sources"])),
            )
    return outcome


def _update_batch(spark, config_path, batch, pending, file_format, outcome):
    """Parse a batch of files, write those that parsed, and mark them."""
    bronze = spark.read.schema(bronze_schema) \
        .option("basePath", path_bronze_files(config_path)) \
        .parquet(path_bronze_files(config_path)) \
        .filter(func.col("DATASET").isin(batch)) \
        .select("DATASET", "LINENO", "LINE")

    # One row per file, its lines in the order of the file, and one file per
    # partition as far as hashing allows, so that a task holds the text of a
    # file or two rather than of the whole batch.
    files = bronze.groupBy("DATASET").agg(
        func.transform(
            func.sort_array(func.collect_list(func.struct("LINENO", "LINE"))),
            lambda line: line["LINE"],
        ).alias("LINES")
    ).repartition(len(batch), "DATASET")

    # The chunk size is bound here, on the driver, because the executors
    # import this module afresh and would not see it changed.
    parse = functools.partial(_parse_files, chunk_rows=CHUNK_ROWS)
    parsed = files.mapInPandas(parse, schema=_PARSED_SCHEMA) \
        .persist(StorageLevel.MEMORY_AND_DISK)
    try:
        summary = {
            row["DATASET"]: row for row in parsed.groupBy("DATASET").agg(
                func.count(func.col("VALUE")).alias("rows"),
                func.max(func.col("_ERROR")).alias("error"),
            ).collect()
        }
        good = []
        for dataset in batch:
            row = summary.get(dataset)
            error = None
            if row is None:
                error = "The Bronze layer holds no lines of the file."
            elif row["error"] is not None:
                error = row["error"]
            elif row["rows"] == 0:
                error = "The file holds no value the parser could read."
            if error is None:
                good.append(dataset)
                continue
            # The traceback comes from the executor, so it is put into the
            # record rather than raised: the run carries on by design.
            logger.error(
                "A file could not be read into the table and was set aside; "
                "its partition is left as it was",
                extra=context(module="frenchdatalibrary", layer="silver",
                              source=dataset, error=error.strip().splitlines()[-1]),
            )
            logger.debug("The error of the file", extra=context(
                module="frenchdatalibrary", layer="silver", source=dataset,
                traceback=error))
            outcome["failed"] += 1
            outcome["failed_sources"].append(f"silver/{dataset}")

        if not good:
            return

        result = parsed.filter(func.col("DATASET").isin(good)) \
            .filter(func.col("_ERROR").isNull()) \
            .select(*[func.col(f.name) for f in silver_schema.fields])
        _report_unknown_tables(result)

        replace_silver_partitions(
            result, ref_silver_frenchdatalibrary(config_path), "DATASET", good,
            file_format=file_format,
        )
        for dataset in good:
            write_watermark(config_path, dataset, pending[dataset])
        rows = sum(summary[d]["rows"] for d in good)
        outcome["written"] += len(good)
        outcome["rows"] += rows
        logger.info(
            "Replaced the partitions of a batch of files",
            extra=context(module="frenchdatalibrary", layer="silver",
                          files=len(good), rows=rows),
        )
    finally:
        parsed.unpersist()


def _report_unknown_tables(result):
    """Log, on the driver, every table filed under a code of its own making.

    The parser logs a title it has no rule for, but it runs on the executors,
    whose log a reader of the run does not see. The codes are therefore
    looked at again here, where they can be reported.
    """
    unknown = result.filter(~func.col("TABLE").isin(list(TABLE_CODES))) \
        .select("DATASET", "TABLE", "TITLE").distinct().collect()
    for row in unknown:
        logger.warning(
            "A table is filed under a code that is not in the vocabulary",
            extra=context(module="frenchdatalibrary", layer="silver",
                          source=row["DATASET"], table=row["TABLE"],
                          title=row["TITLE"]),
        )


def series(data, dataset, table, frequency, columns=None):
    """One table of one file as a wide pandas frame, a column per series.

    This is the shape the library publishes a table in, and the one most
    analyses start from: ``series(df, "6_Portfolios_2x3", "RET_VW", "M")``
    is the monthly value-weighted returns of the six portfolios, indexed by
    the last day of each month.

    Parameters
    ----------
    data: pyspark.sql.DataFrame or pandas.DataFrame
        The table of the library, as :func:`connect_to_frenchdatalibrary`
        opens it, or rows of it already in pandas. A Spark frame is filtered
        before anything is collected, so only the rows asked for leave Spark.
    dataset: str
        The file, ``"F-F_Research_Data_Factors"``.
    table: str
        The code of the table, ``"FACTORS"``; see
        :data:`fincommons.frenchdatalibrary.parse.TABLE_CODES`.
    frequency: str
        ``"D"``, ``"W"``, ``"M"`` or ``"A"``.
    columns: list of str, optional
        The series to keep, in the order wanted. All of them, in alphabetical
        order, where left out: the table does not record the order of the
        columns of the file.

    Returns
    -------
    pandas.DataFrame
        Indexed by ``REFDT`` as a ``DatetimeIndex``, in order of time, with a
        column per series. A period a series has no value for is ``NaN``.

    Raises
    ------
    LookupError
        If the table holds no row for the file, table and frequency, which is
        a misspelt name far more often than an empty table; or if a series of
        ``columns`` is not among them.
    """
    where = {"DATASET": dataset, "TABLE": table, "FREQUENCY": frequency}
    if hasattr(data, "sparkSession"):
        selected = data
        for column, value in where.items():
            selected = selected.filter(func.col(column) == value)
        if columns is not None:
            selected = selected.filter(func.col("SERIES").isin(list(columns)))
        frame = selected.select("REFDT", "SERIES", "VALUE").toPandas()
    else:
        mask = pandas.Series(True, index=data.index)
        for column, value in where.items():
            mask &= data[column] == value
        frame = data.loc[mask, ["REFDT", "SERIES", "VALUE"]]

    if frame.empty:
        raise LookupError(
            f"The table of the library holds no row for {dataset!r}, table "
            f"{table!r} at frequency {frequency!r}. Check the names against "
            "the DATASET, TABLE and FREQUENCY columns."
        )
    wide = frame.assign(REFDT=pandas.to_datetime(frame["REFDT"])) \
        .pivot(index="REFDT", columns="SERIES", values="VALUE") \
        .sort_index()
    wide.columns.name = None
    if columns is not None:
        missing = [c for c in columns if c not in wide.columns]
        if missing:
            raise LookupError(
                f"The table {table!r} of {dataset!r} has no series {missing}; "
                f"it has {sorted(wide.columns)}."
            )
        wide = wide[list(columns)]
    return wide


__all__ = [
    "BATCH_LINES",
    "BATCH_SIZE",
    "CHUNK_ROWS",
    "batches",
    "pending_datasets",
    "series",
    "update_silver_frenchdatalibrary",
]
