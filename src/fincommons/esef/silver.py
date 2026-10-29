"""ESEF filings: the table of filings and the table of facts in the Silver layer.

**The table of filings** is every filing the indexes of the Bronze layer list,
one row per filing, and is rebuilt in full on every run. It is a few tens of
thousands of rows, and it is the catalogue a reader looks in for which
filings there are, and which of them have a report.

**The table of facts is rebuilt country by country, and only for the
countries whose reports changed.** Each country is a partition of its own,
``COUNTRY``, and a country whose Bronze layer holds a report the partition
was not built from -- a new filing, or a filing the repository processed
anew -- is read again and replaces its partition, leaving every other
country as it was. Which reports each partition was built from is recorded
in a mark beside the table, written after the partition is in place, so that
a run that dies leaves the mark behind and the next run builds the country
again. A partition per filing would make the write smaller, but leave the
table in tens of thousands of directories of a few thousand rows each, which
every query over it has to open.

The reports are read on the executors, since a country is hundreds of
reports of up to tens of megabytes each, as for
:mod:`fincommons.famafrenchdata.silver`. Each report reaches them as the
path of its file, and its facts come back through
:meth:`pyspark.sql.DataFrame.mapInPandas`. A report that cannot be read does
not fail the others: its error is carried back to the driver as a row of its
own, logged at ``ERROR`` and counted, and the country is written without it
and left unmarked, so that the next run tries it again.
"""

import functools
import hashlib
import json
import logging
import traceback

import pandas
import pyspark.sql.functions as func
from pyspark import StorageLevel
from pyspark.sql.types import StringType, StructField, StructType

from ..log import context, pipeline_run
from ..storage import replace_silver_partitions, silver_format, write_silver
from .bronze import read_filings
from .common import *
from .parse import FACT_COLUMNS, facts_from_report

logger = logging.getLogger(__name__)

#: How many facts travel back from the executors in one frame. Each frame
#: becomes one Arrow batch, which has to fit into the direct memory of the
#: session.
CHUNK_ROWS = 100_000

#: The columns the reading hands back: those of the table, and the error of a
#: report that could not be read, which travels in a row of its own.
_PARSED_SCHEMA = StructType(
    [StructField(f.name, f.dataType, True) for f in facts_schema.fields]
    + [StructField("_ERROR", StringType(), True)]
)

_COLUMNS = [f.name for f in facts_schema.fields] + ["_ERROR"]


def _read_report(country, filing, period_end, path, chunk_rows=CHUNK_ROWS):
    """The facts of one report, in chunks of at most ``chunk_rows`` rows.

    It never raises. A report that fails comes back as a single row that
    carries the traceback in ``_ERROR``, because an exception raised here
    would fail the whole Spark job, and with it every other report of the
    country.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            facts = facts_from_report(json.load(handle))
    except Exception:
        error = {column: [None] for column in _COLUMNS}
        error.update(COUNTRY=[country], FILING=[filing],
                     _ERROR=[traceback.format_exc()])
        yield pandas.DataFrame(error, columns=_COLUMNS)
        return
    facts["COUNTRY"] = country
    facts["FILING"] = filing
    facts["PERIODEND"] = period_end
    facts["_ERROR"] = None
    facts = facts[_COLUMNS]
    # As in the French Data Library: pandas 3 keeps text in Arrow arrays,
    # which PySpark cannot always hand back to Spark, and as Python objects
    # it travels as it does under pandas 2.
    facts = facts.astype({
        column: object for column in facts.columns
        if isinstance(facts[column].dtype, pandas.StringDtype)
    })
    for start in range(0, max(len(facts), 1), chunk_rows):
        yield facts.iloc[start:start + chunk_rows]


def _read_reports(frames, chunk_rows=CHUNK_ROWS):
    """Read the reports of a partition, as :meth:`mapInPandas` hands them over."""
    for frame in frames:
        for row in frame.itertuples(index=False):
            yield from _read_report(row.COUNTRY, row.FILING, row.PERIODEND,
                                    row.PATH, chunk_rows)


def signature(records):
    """What a country's reports are: the SHA-256 of each, by filing.

    Two runs that find the same reports give the same signature, whatever
    order they find them in.
    """
    text = "\n".join(f"{r['filing']}:{r['sha256']}"
                     for r in sorted(records, key=lambda r: r["filing"]))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def pending_countries(config_path, full=False, countries=None):
    """The countries the table of facts has to be rebuilt for.

    A country is pending where the reports the Bronze layer holds for it are
    not those its partition was built from, or where ``full`` asks for every
    country.

    Returns
    -------
    dict
        The records of the reports of each pending country, by country.
    """
    by_country = {}
    for record in read_report_records(config_path).values():
        by_country.setdefault(record["country"], []).append(record)
    if countries is not None:
        wanted = {str(c).upper() for c in countries}
        by_country = {c: r for c, r in by_country.items() if c in wanted}
    return {
        country: records
        for country, records in sorted(by_country.items())
        if full or read_watermark(config_path, country) != signature(records)
    }


def build_filings(spark, config_path):
    """The table of filings as a Spark frame, from the indexes of the Bronze layer."""
    rows = list(read_filings(config_path).values())
    return spark.createDataFrame(
        [tuple(r[f.name] for f in filings_schema.fields) for r in rows],
        schema=filings_schema,
    )


def update_silver_esef(spark, config_path, full=False, file_format=None,
                       countries=None):
    """Rebuild the table of filings, and the countries whose reports changed.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Rebuild every country the Bronze layer holds reports of, rather than
        those whose reports changed. A scheduled run never needs it; it is how
        the table is brought in line with a new version of the reader.
    file_format: str, optional
        Format the tables are written in, ``'parquet'`` or ``'delta'``; see
        :func:`fincommons.storage.replace_silver_partitions`. Where it is left
        out the configuration file and the tables on disk decide.
    countries: list of str, optional
        Restrict the rebuild of the facts to these countries.

    Returns
    -------
    dict
        The outcome of the stage: ``filings`` counts the rows of the table of
        filings, ``written`` the countries whose partition was replaced and
        ``rows`` the facts written, ``failed`` the reports that could not be
        read and ``failed_sources`` names them as ``silver/<filing>``.
    """
    file_format = silver_format(ref_silver_facts(config_path), file_format)
    with pipeline_run(
        logger,
        "update_silver_esef",
        module="esef",
        layer="silver",
        config_path=config_path,
        file_format=file_format,
    ) as outcome:
        outcome.update(filings=0, written=0, rows=0, failed=0, failed_sources=[])

        filings = build_filings(spark, config_path)
        outcome["filings"] = filings.count()
        if outcome["filings"]:
            write_silver(filings, ref_silver_filings(config_path),
                         file_format=silver_format(ref_silver_filings(config_path),
                                                   file_format))

        pending = pending_countries(config_path, full, countries)
        if not pending:
            logger.info(
                "No report has changed since the table of facts was built, "
                "nothing to do",
                extra=context(module="esef", layer="silver"),
            )
            return outcome

        periods = {name: record["PERIODEND"]
                   for name, record in read_filings(config_path).items()}
        for country, records in pending.items():
            _update_country(spark, config_path, country, records, periods,
                            file_format, outcome)

        if outcome["failed"]:
            logger.warning(
                "Reports were set aside, the run did not update them",
                extra=context(module="esef", layer="silver",
                              failed=outcome["failed"],
                              sources=",".join(outcome["failed_sources"])),
            )
    return outcome


def _update_country(spark, config_path, country, records, periods, file_format,
                    outcome):
    """Read the reports of one country, write its partition, and mark it."""
    reports = pandas.DataFrame(
        [(country, r["filing"], periods.get(r["filing"]),
          path_bronze_report(config_path, country, r["filing"]))
         for r in records],
        columns=["COUNTRY", "FILING", "PERIODEND", "PATH"],
    )
    # A report whose filing no index lists any more has no period end to
    # file its facts under, and is passed over until one does again.
    reports = reports[reports["PERIODEND"].notna()]
    if reports.empty:
        return
    frame = spark.createDataFrame(reports, schema=StructType([
        StructField("COUNTRY", StringType(), False),
        StructField("FILING", StringType(), False),
        facts_schema["PERIODEND"],
        StructField("PATH", StringType(), False),
    ])).repartition(min(len(reports), 200))

    # The chunk size is bound here, on the driver, because the executors
    # import this module afresh and would not see it changed.
    read = functools.partial(_read_reports, chunk_rows=CHUNK_ROWS)
    parsed = frame.mapInPandas(read, schema=_PARSED_SCHEMA) \
        .persist(StorageLevel.MEMORY_AND_DISK)
    try:
        errors = parsed.filter(func.col("_ERROR").isNotNull()) \
            .select("FILING", "_ERROR").collect()
        for row in errors:
            # The traceback comes from the executor, so it is put into the
            # record rather than raised: the run carries on by design.
            logger.error(
                "A report could not be read into the table and was set aside",
                extra=context(module="esef", layer="silver", source=row["FILING"],
                              error=row["_ERROR"].strip().splitlines()[-1]),
            )
            logger.debug("The error of the report", extra=context(
                module="esef", layer="silver", source=row["FILING"],
                traceback=row["_ERROR"]))
            outcome["failed"] += 1
            outcome["failed_sources"].append(f"silver/{row['FILING']}")

        result = parsed.filter(func.col("_ERROR").isNull()) \
            .select(*[func.col(f.name) for f in facts_schema.fields])
        rows = result.count()
        if rows:
            replace_silver_partitions(result, ref_silver_facts(config_path),
                                      "COUNTRY", [country],
                                      file_format=file_format)
            outcome["written"] += 1
            outcome["rows"] += rows
        if not errors:
            write_watermark(config_path, country, signature(records),
                            len(records))
        logger.info(
            "Replaced the partition of a country",
            extra=context(module="esef", layer="silver", source=country,
                          reports=len(reports), rows=rows),
        )
    finally:
        parsed.unpersist()


__all__ = [
    "CHUNK_ROWS",
    "build_filings",
    "pending_countries",
    "signature",
    "update_silver_esef",
]
