"""Financial reports: building the combined tables.

Each source is reduced to the columns of
:data:`fincommons.financialreport.common.fact_schema` and the results are put
one after the other, which is what the two source modules carrying the same
spelling for the columns they share makes possible. Nothing is joined between
the regimes, and nothing could be: the one key that would join them, the
entity, is a legal entity identifier on one side and a central index key on
the other, with no authoritative mapping between them.

A source that has no tables is passed over rather than failing the run. A lake
that has collected only ESEF filings gets a combined table of ESEF filings,
which is the same question answered over the population there is, and a lake
that has collected neither is told so.

Both tables are rebuilt in full. They are derived entirely from tables that
are themselves rebuilt, so there is nothing to resume from and no watermark
to keep: what a run avoids repeating is the collection, which the two source
modules handle.
"""

import logging

import pyspark.sql.functions as func

from ..layout import rebalanced
from ..log import context, pipeline_run
from ..storage import write_silver
from .common import (FACT_PARTITION_BY, SOURCES, fact_schema, filing_schema,
                     ref_silver_fact, ref_silver_filing)

logger = logging.getLogger(__name__)

#: How the prefix a filing binds to a taxonomy says which framework it
#: reports under. Both tables spell a concept as prefix and name, so the
#: prefix is what there is to go on, and it is the same thing the Commission
#: states as the key of its facts.
FRAMEWORK_PREFIXES = {"ifrs-full": "IFRS", "us-gaap": "US-GAAP"}


def _framework_of(concept):
    """The framework a concept's prefix says, as a Spark expression."""
    expression = func.lit(None).cast("string")
    for prefix, framework in FRAMEWORK_PREFIXES.items():
        expression = func.when(concept.startswith(f"{prefix}:"),
                               func.lit(framework)).otherwise(expression)
    return expression


def available_sources(spark, config_path):
    """The sources whose tables this lake holds.

    Each source module is imported only where its tables are to be read, so
    that a lake with one of them installed and configured does not need the
    other. The modules are named in
    :data:`fincommons.financialreport.common.SOURCES`.
    """
    import importlib

    from ..storage import silver_table_exists

    found = []
    for source, module_name in SOURCES.items():
        try:
            module = importlib.import_module(module_name)
        except ImportError:  # pragma: no cover - depends on the install
            continue
        try:
            refs = (module.common.ref_silver_facts(config_path),
                    module.common.ref_silver_filings(config_path))
        except KeyError:
            # The configuration file names no such data set, which is how a
            # lake says it does not collect from that source.
            continue
        if all(silver_table_exists(spark, ref) for ref in refs):
            found.append(source)
    return found


def facts_from_esef(spark, config_path):
    """The ESEF facts, in the columns of the combined table."""
    from ..esef.common import ref_silver_facts
    from ..storage import read_silver

    facts = read_silver(spark, ref_silver_facts(config_path))
    return (
        facts
        .withColumn("SOURCE", func.lit("esef"))
        .withColumn("CIK", func.lit(None).cast("string"))
        .withColumn("FRAMEWORK", _framework_of(func.col("CONCEPT")))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .select(*fact_schema.names)
    )


def facts_from_secfiling(spark, config_path):
    """The SEC facts, in the columns of the combined table.

    The period the filing reports on is on that module's table of filings and
    not on its facts, so the two are joined on the accession number. ``TEXT``
    and ``DIMENSIONS`` come back empty because the company files carry
    neither.
    """
    from ..secfiling.common import ref_silver_facts, ref_silver_filings
    from ..storage import read_silver

    facts = read_silver(spark, ref_silver_facts(config_path))
    filings = read_silver(spark, ref_silver_filings(config_path)) \
        .select("FILING", "PERIODEND")

    return (
        facts.join(filings, "FILING", "left")
        .withColumn("SOURCE", func.lit("secfiling"))
        .withColumn("LEI", func.lit(None).cast("string"))
        .withColumn("FRAMEWORK", _framework_of(func.col("CONCEPT")))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .withColumn("TEXT", func.lit(None).cast("string"))
        .withColumn("DIMENSIONS", func.lit(None).cast("string"))
        .select(*fact_schema.names)
    )


def filings_from_esef(spark, config_path):
    """The ESEF filings, in the columns of the combined table."""
    from ..esef.common import ref_silver_facts, ref_silver_filings
    from ..storage import read_silver

    filings = read_silver(spark, ref_silver_filings(config_path))
    facts = read_silver(spark, ref_silver_facts(config_path))

    # The framework and the count come from the facts, which is where the
    # evidence for both is: the index says nothing about what a report tagged
    # against, and the regime states no form.
    about = (
        facts.groupBy("FILING")
        .agg(func.count(func.lit(1)).alias("FACTS"),
             func.max(_framework_of(func.col("CONCEPT"))).alias("FRAMEWORK"))
    )
    return (
        filings.join(about, "FILING", "left")
        .withColumn("SOURCE", func.lit("esef"))
        .withColumn("CIK", func.lit(None).cast("string"))
        .withColumn("FORM", func.lit(None).cast("string"))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .withColumn("FACTS", func.coalesce("FACTS", func.lit(0)))
        .select(*filing_schema.names)
    )


def filings_from_secfiling(spark, config_path):
    """The SEC filings, in the columns of the combined table."""
    from ..secfiling.common import ref_silver_filings
    from ..storage import read_silver

    return (
        read_silver(spark, ref_silver_filings(config_path))
        .withColumn("SOURCE", func.lit("secfiling"))
        .withColumn("LEI", func.lit(None).cast("string"))
        .withColumn("COUNTRY", func.lit(None).cast("string"))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .select(*filing_schema.names)
    )


#: How each source is read, by the name the combined tables record it under.
READERS = {
    "esef": (facts_from_esef, filings_from_esef),
    "secfiling": (facts_from_secfiling, filings_from_secfiling),
}


def combine(spark, config_path, what, sources=None):
    """Return the rows of one combined table, from the sources available.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    what: str
        ``'fact'`` or ``'filing'``.
    sources: iterable of str, optional
        The sources to read. By default every one whose tables the lake holds.
    """
    if what not in ("fact", "filing"):
        raise ValueError(f"{what!r} is no table of this module; "
                         "use 'fact' or 'filing'")

    found = list(sources) if sources else available_sources(spark, config_path)
    unknown = [source for source in found if source not in READERS]
    if unknown:
        raise ValueError(f"No source is read as {unknown[0]!r}; this module "
                         f"reads {', '.join(sorted(READERS))}")
    if not found:
        raise RuntimeError(
            "No source has tables to combine. Build the Silver tables of "
            + " or ".join(SOURCES.values())
            + " first; a lake that collects only one of them combines that one."
        )

    parts = [READERS[source][0 if what == "fact" else 1](spark, config_path)
             for source in found]
    combined = parts[0]
    for part in parts[1:]:
        combined = combined.unionByName(part)
    return combined


def update_silver_financialreport(spark, config_path, sources=None,
                                  file_format=None):
    """Rebuild the combined tables from the tables of the source modules.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    sources: iterable of str, optional
        The sources to read. By default every one whose tables the lake holds.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    """
    with pipeline_run(
        logger,
        "update_silver_financialreport",
        module="financialreport",
        layer="silver",
        config_path=config_path,
    ) as outcome:
        found = list(sources) if sources else available_sources(spark, config_path)
        outcome["sources"] = ";".join(found)

        filings = combine(spark, config_path, "filing", found)
        write_silver(rebalanced(filings), ref_silver_filing(config_path),
                     file_format=file_format)
        outcome["filings"] = filings.count()

        facts = combine(spark, config_path, "fact", found)
        write_silver(rebalanced(facts, *FACT_PARTITION_BY),
                     ref_silver_fact(config_path),
                     file_format=file_format, partition_by=FACT_PARTITION_BY)
        outcome["facts"] = facts.count()

        if len(found) < len(SOURCES):
            logger.info(
                "The combined tables were built from part of the sources",
                extra=context(module="financialreport", layer="silver",
                              source=";".join(found)),
            )


#: What this module offers. Building the combined tables.
__all__ = [
    "FRAMEWORK_PREFIXES",
    "READERS",
    "available_sources",
    "combine",
    "facts_from_esef",
    "facts_from_secfiling",
    "filings_from_esef",
    "filings_from_secfiling",
    "update_silver_financialreport",
]
