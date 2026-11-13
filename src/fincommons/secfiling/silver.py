"""SEC filings: the tables of filings and of facts.

The Bronze layer holds one JSON file per registrant, each covering every
filing that registrant has tagged. This layer reads them into two tables: one
row per filing, and one row per fact of a filing.

The reading happens on the executors, one company file at a time, and only
the rows come back. A company file of a long-lived registrant runs to a few
megabytes and some tens of thousands of facts, so the file is the unit of
parallelism and a run scales with the number of companies collected.

Both tables are rebuilt in full from the files the Bronze layer holds. There
is no watermark, and the reason is that there is nothing to resume from: a
company file is republished whole whenever any of the registrant's filings
change, so a file that changed changes rows of every year it covers. What a
run avoids repeating is the collection, which the Bronze layer's records
handle, not the reading.

A company file that cannot be read does not fail the run. It comes back as a
single row carrying its traceback, which the driver logs and counts, because
an exception raised on an executor would fail the whole job and with it every
other company.
"""

import json
import logging
import os
import traceback

import pandas
from pyspark.sql.types import StringType, StructField, StructType

from ..layout import rebalanced
from ..log import context, pipeline_run
from ..storage import write_silver
from .bronze import collected_companies
from .common import (SOURCE, facts_schema, filings_schema,
                     path_bronze_company, ref_silver_facts, ref_silver_filings)
from .parse import PERIODIC_FORMS, read_company

logger = logging.getLogger(__name__)

#: How many facts travel back from the executors in one frame. Each frame
#: becomes one Arrow batch, which has to fit into the direct memory of the
#: session; the figure is the one :mod:`fincommons.esef.silver` uses.
CHUNK_ROWS = 100_000

#: The columns the reading hands back: those of the table, and the error of a
#: company file that could not be read, which travels in a row of its own.
_FACTS_PARSED = StructType(
    [StructField(f.name, f.dataType, True) for f in facts_schema.fields]
    + [StructField("_ERROR", StringType(), True)]
)
_FILINGS_PARSED = StructType(
    [StructField(f.name, f.dataType, True) for f in filings_schema.fields]
    + [StructField("_ERROR", StringType(), True)]
)

_FACT_COLUMNS = [f.name for f in facts_schema.fields] + ["_ERROR"]
_FILING_COLUMNS = [f.name for f in filings_schema.fields] + ["_ERROR"]


def _error_frame(columns, cik, where):
    """One row carrying a traceback, in the shape the reading hands back."""
    row = {column: [None] for column in columns}
    row["CIK"] = [cik]
    row["_ERROR"] = [f"{where}\n{traceback.format_exc()}"]
    return pandas.DataFrame(row, columns=columns)


def _read_one(cik, path, what, forms, chunk_rows=CHUNK_ROWS):
    """The filings or the facts of one company file, in chunks.

    It never raises; see the module docstring.
    """
    columns = _FILING_COLUMNS if what == "filings" else _FACT_COLUMNS
    try:
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
        filings, facts = read_company(document, forms)
    except Exception:  # noqa: BLE001 - carried back, not swallowed
        yield _error_frame(columns, cik, f"reading CIK{cik}")
        return

    rows = filings if what == "filings" else facts
    names = columns[:-1]
    # A company file whose filings are all of forms the run does not want
    # yields nothing, and one empty frame of the right shape is what
    # mapInPandas needs to see rather than no frame at all.
    starts = list(range(0, len(rows), chunk_rows)) or [0]
    for start in starts:
        frame = pandas.DataFrame(list(rows[start:start + chunk_rows]),
                                 columns=names)
        frame["_ERROR"] = None
        # As in the ESEF module: pandas 3 keeps text in Arrow arrays, which
        # PySpark cannot always hand back to Spark, and as Python objects it
        # travels as it does under pandas 2.
        frame = frame.astype({
            column: object for column in frame.columns
            if isinstance(frame[column].dtype, pandas.StringDtype)
        })
        yield frame[columns]


def _read_all(frames, what, forms, chunk_rows=CHUNK_ROWS):
    """Read every company file a partition names."""
    for frame in frames:
        for row in frame.itertuples():
            yield from _read_one(row.CIK, row.PATH, what, forms, chunk_rows)


def build(spark, config_path, what, ciks=None, forms=PERIODIC_FORMS,
          chunk_rows=CHUNK_ROWS):
    """Read the company files of the Bronze layer into one of the tables.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    what: str
        ``'filings'`` or ``'facts'``.
    ciks: iterable, optional
        The registrants to read. By default every one the Bronze layer holds.
    forms: iterable of str, optional
        The forms to keep; see
        :func:`fincommons.secfiling.parse.facts_from_company`.

    Returns
    -------
    pyspark.sql.DataFrame
        The rows, with the errors in ``_ERROR``.
    """
    if what not in ("filings", "facts"):
        raise ValueError(f"{what!r} is no table of this module; "
                         "use 'filings' or 'facts'")
    keys = list(ciks) if ciks else collected_companies(config_path)
    if not keys:
        raise RuntimeError(
            "The Bronze layer holds no company files. Collect some with "
            "update_bronze_secfiling before building the tables."
        )

    schema = _FILINGS_PARSED if what == "filings" else _FACTS_PARSED
    listed = spark.createDataFrame(
        [(key, path_bronze_company(config_path, key)) for key in keys],
        schema=StructType([StructField("CIK", StringType(), False),
                           StructField("PATH", StringType(), False)]),
    ).repartition(min(len(keys), 200))

    return listed.mapInPandas(
        lambda frames: _read_all(frames, what, forms, chunk_rows), schema=schema
    )


def _report_errors(frame, what):
    """Log the company files that could not be read, and return how many."""
    failed = frame.where("_ERROR IS NOT NULL").select("CIK", "_ERROR").collect()
    for row in failed:
        logger.error(
            "The company file could not be read\n%s", row["_ERROR"],
            extra=context(module="secfiling", layer="silver", source=SOURCE,
                          table=what, file=f"CIK{row['CIK']}.json"),
        )
    return len(failed)


def update_silver_secfiling(spark, config_path, ciks=None, forms=PERIODIC_FORMS,
                            file_format=None, chunk_rows=CHUNK_ROWS):
    """Rebuild the tables of filings and of facts from the Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    ciks: iterable, optional
        The registrants to read. By default every one the Bronze layer holds.
    forms: iterable of str, optional
        The forms to keep. The periodic reports by default; ``None`` keeps
        every form the files carry.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    """
    with pipeline_run(
        logger,
        "update_silver_secfiling",
        module="secfiling",
        layer="silver",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        parsed = build(spark, config_path, "filings", ciks, forms, chunk_rows).cache()
        try:
            outcome["failed"] = _report_errors(parsed, "filings")
            filings = parsed.where("_ERROR IS NULL").select(*filings_schema.names)
            write_silver(rebalanced(filings), ref_silver_filings(config_path),
                         file_format=file_format)
            outcome["filings"] = filings.count()
        finally:
            parsed.unpersist()

        parsed = build(spark, config_path, "facts", ciks, forms, chunk_rows).cache()
        try:
            _report_errors(parsed, "facts")
            facts = parsed.where("_ERROR IS NULL").select(*facts_schema.names)
            write_silver(rebalanced(facts, "FILEDYEAR"),
                         ref_silver_facts(config_path),
                         file_format=file_format, partition_by=("FILEDYEAR",))
            outcome["facts"] = facts.count()
        finally:
            parsed.unpersist()


#: What this module offers. Building the tables from the company files.
__all__ = [
    "CHUNK_ROWS",
    "build",
    "update_silver_secfiling",
]
