"""EDINET: the tables of filings, of facts and of filers.

The Bronze layer holds one index per calendar day, one archive per filing
collected, and the agency's list of filers. This layer reads them into three
tables: one row per filing the indexes name, one row per fact of a filing
whose archive was collected, and one row per filer.

The table of filings and the table of filers are small and are built on the
driver: a decade of indexes is a few million rows of JSON and the list of
filers is eleven thousand lines. The table of facts is not. A converted
report runs to a thousand facts and a lake holding a year of them holds some
millions, so the archives are read on the executors, one at a time, and only
the rows come back.

Which filings are read for facts is decided by the index rather than by what
is on disk, and in two steps. A filing whose ``has_csv`` is not ``'1'`` has no
conversion to read, and the agency says so in the index, so nothing is
attempted for it; of the rest, those whose archive the Bronze layer holds are
read. A filing listed with a conversion and not collected is counted and
reported, because that is a gap a run can close, and a filing listed without
one never can be.

The table of facts is rebuilt a year at a time. The year is the year the
filing was submitted, which only moves forward, so a run that collects this
month's filings rewrites this year's partition and leaves the others alone.
The table of filings is rebuilt in full, being small and derived from every
index the Bronze layer holds.

An archive that cannot be read does not fail the run. It comes back as a
single row carrying its traceback, which the driver logs and counts, because
an exception raised on an executor would fail the whole job and with it every
other filing.
"""

import logging
import os
import traceback

import pandas
from pyspark.sql.types import StringType, StructField, StructType

from ...layout import rebalanced
from ...log import context, pipeline_run
from ...storage import replace_silver_partitions, silver_format, write_silver
from .bronze import read_filers, read_filings
from .common import (SOURCE, facts_schema, filers_schema, filings_schema,
                     path_bronze_document, ref_silver_facts,
                     ref_silver_filers, ref_silver_filings)
from .parse import facts_from_archive

logger = logging.getLogger(__name__)

#: How many facts travel back from the executors in one frame. Each becomes
#: one Arrow batch, which has to fit into the direct memory of the session;
#: the figure is the one the other two regimes use.
CHUNK_ROWS = 100_000

#: The columns the reading hands back: those of the table, and the error of an
#: archive that could not be read, which travels in a row of its own.
_FACTS_PARSED = StructType(
    [StructField(f.name, f.dataType, True) for f in facts_schema.fields]
    + [StructField("_ERROR", StringType(), True)]
)

_FACT_COLUMNS = [f.name for f in facts_schema.fields] + ["_ERROR"]

#: What the driver tells an executor about each filing it is to read. The
#: period and the day of submission resolve the relative contexts of the
#: conversion, so they travel with the path.
_WORK = StructType([
    StructField("filing", StringType(), False),
    StructField("PATH", StringType(), False),
    StructField("edinet_code", StringType(), True),
    StructField("period_start", StringType(), True),
    StructField("period_end", StringType(), True),
    StructField("FILED", StringType(), True),
    StructField("filed_year", StringType(), False),
])


def _error_frame(filing):
    """One row carrying a traceback, in the shape the reading hands back."""
    row = {column: [None] for column in _FACT_COLUMNS}
    row["filing"] = [filing]
    row["filed_year"] = [None]
    row["_ERROR"] = [f"reading {filing}\n{traceback.format_exc()}"]
    return pandas.DataFrame(row, columns=_FACT_COLUMNS)


def _as_date(text):
    from datetime import date
    if not text:
        return None
    try:
        return date.fromisoformat(str(text)[:10])
    except ValueError:
        return None


def _read_one(work, chunk_rows=CHUNK_ROWS):
    """The facts of one archive, in chunks. It never raises."""
    try:
        rows = facts_from_archive(
            work.PATH, work.filing,
            period_start=_as_date(work.period_start),
            period_end=_as_date(work.period_end),
            filed=_as_date(work.FILED),
            edinet_code=work.edinet_code,
            filed_year=int(work.filed_year),
        )
    except Exception:  # noqa: BLE001 - carried back, not swallowed
        yield _error_frame(work.filing)
        return

    names = _FACT_COLUMNS[:-1]
    starts = list(range(0, len(rows), chunk_rows)) or [0]
    for start in starts:
        frame = pandas.DataFrame(list(rows[start:start + chunk_rows]),
                                 columns=names)
        frame["_ERROR"] = None
        # As in the other two regimes: pandas 3 keeps text in Arrow arrays,
        # which PySpark cannot always hand back to Spark, and as Python
        # objects it travels as it does under pandas 2.
        frame = frame.astype({
            column: object for column in frame.columns
            if isinstance(frame[column].dtype, pandas.StringDtype)
        })
        yield frame[_FACT_COLUMNS]


def _read_all(frames, chunk_rows=CHUNK_ROWS):
    """Read every archive a partition names."""
    for frame in frames:
        for work in frame.itertuples():
            yield from _read_one(work, chunk_rows)


def build_filings(spark, config_path, days=None):
    """The table of filings, from the indexes the Bronze layer holds.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    days: iterable, optional
        The calendar days to read. By default every one collected.
    """
    rows = read_filings(config_path, days)
    if not rows:
        raise RuntimeError(
            "The Bronze layer holds no index. Collect some with "
            "update_bronze_edinet before building the tables."
        )
    return spark.createDataFrame(rows, schema=filings_schema)


def build_filers(spark, config_path):
    """The table of filers, from the list the Bronze layer holds."""
    return spark.createDataFrame(read_filers(config_path),
                                 schema=filers_schema)


def outstanding(config_path, filings):
    """The filings the agency has a conversion for that were not collected.

    A gap a run can close, as against a filing the agency never converted,
    which it cannot. The caller gets the document numbers so that it can
    collect them.
    """
    wanted = filings.where("has_csv = '1'").select(
        "filing", "edinet_code", "period_start", "period_end", "submitted",
        "filed_year").collect()
    return [row["filing"] for row in wanted
            if not os.path.exists(path_bronze_document(config_path,
                                                       row["filing"], "csv"))]


def build_facts(spark, config_path, filings, chunk_rows=CHUNK_ROWS):
    """Read the archives of the Bronze layer into the table of facts.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    filings: pyspark.sql.DataFrame
        The table of filings, which says which filings have a conversion and
        what period each reports on.

    Returns
    -------
    pyspark.sql.DataFrame
        The rows, with the errors in ``_ERROR``.
    """
    work = []
    for row in filings.where("has_csv = '1'").collect():
        path = path_bronze_document(config_path, row["filing"], "csv")
        if not os.path.exists(path):
            continue
        work.append((
            row["filing"], path, row["edinet_code"],
            row["period_start"].isoformat() if row["period_start"] else None,
            row["period_end"].isoformat() if row["period_end"] else None,
            row["submitted"].date().isoformat() if row["submitted"] else None,
            str(row["filed_year"]),
        ))

    if not work:
        raise RuntimeError(
            "No filing of the table has a conversion in the Bronze layer. "
            "Collect some with collect_document before building the facts."
        )

    listed = spark.createDataFrame(work, schema=_WORK) \
        .repartition(min(len(work), 200))

    return listed.mapInPandas(
        lambda frames: _read_all(frames, chunk_rows), schema=_FACTS_PARSED)


def _report_errors(frame):
    """Log the archives that could not be read, and return how many."""
    failed = frame.where("_ERROR IS NOT NULL").select("filing", "_ERROR").collect()
    for row in failed:
        logger.error(
            "The conversion could not be read\n%s", row["_ERROR"],
            extra=context(module="edinet", layer="silver", source=SOURCE,
                          table="facts", file=f"{row['filing']}.zip"),
        )
    return len(failed)


def update_silver_edinet(spark, config_path, days=None, file_format=None,
                         chunk_rows=CHUNK_ROWS, filers=True):
    """Rebuild the tables from the Bronze layer.

    The table of filings is rebuilt in full from every index collected; the
    table of facts is rewritten a year at a time, by the year a filing was
    submitted, so that a run over this month's filings leaves the other years
    alone.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    days: iterable, optional
        The calendar days to read. By default every one the Bronze layer
        holds.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    filers: bool
        Build the table of filers as well, where the Bronze layer holds the
        list.
    """
    with pipeline_run(
        logger,
        "update_silver_edinet",
        module="edinet",
        layer="silver",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        filings = build_filings(spark, config_path, days).cache()
        try:
            write_silver(rebalanced(filings, "filed_year"),
                         ref_silver_filings(config_path),
                         file_format=file_format,
                         partition_by=("filed_year",))
            outcome["filings"] = filings.count()
            outcome["with_conversion"] = filings.where("has_csv = '1'").count()
            outcome["not_collected"] = len(outstanding(config_path, filings))

            parsed = build_facts(spark, config_path, filings, chunk_rows).cache()
            try:
                outcome["failed"] = _report_errors(parsed)
                facts = parsed.where("_ERROR IS NULL") \
                    .select(*facts_schema.names)
                years = [row["filed_year"] for row in
                         facts.select("filed_year").distinct().collect()]
                replace_silver_partitions(
                    rebalanced(facts, "filed_year"),
                    ref_silver_facts(config_path),
                    "filed_year", sorted(years),
                    file_format=file_format or silver_format(config_path),
                )
                outcome["facts"] = facts.count()
                outcome["years"] = len(years)
            finally:
                parsed.unpersist()
        finally:
            filings.unpersist()

        if filers:
            try:
                people = build_filers(spark, config_path)
            except RuntimeError:
                outcome["filers"] = 0
            else:
                write_silver(rebalanced(people),
                             ref_silver_filers(config_path),
                             file_format=file_format)
                outcome["filers"] = people.count()


#: What this module offers. Building the tables from the Bronze layer.
__all__ = [
    "CHUNK_ROWS",
    "build_facts",
    "build_filers",
    "build_filings",
    "outstanding",
    "update_silver_edinet",
]
