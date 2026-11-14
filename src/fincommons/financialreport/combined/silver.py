"""Financial reports: building the combined tables.

Each source is reduced to the columns of
:data:`fincommons.financialreport.combined.common.fact_schema` and the results are put
one after the other, which is what the two source modules carrying the same
spelling for the columns they share makes possible. Nothing is joined between
the regimes, and nothing could be: the one key that would join them, the
entity, is a legal entity identifier on one side and a central index key on
the other, and no mapping published by either body reaches the issuers these
tables hold.

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

from ...layout import rebalanced
from ...log import context, pipeline_run
from ...storage import write_silver
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
    :data:`fincommons.financialreport.combined.common.SOURCES`.
    """
    import importlib

    from ...storage import silver_table_exists

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
    from ...storage import read_silver

    facts = read_silver(spark, ref_silver_facts(config_path))
    return (
        facts
        .withColumn("SOURCE", func.lit("esef"))
        .withColumn("CIK", func.lit(None).cast("string"))
        .withColumn("EDINETCODE", func.lit(None).cast("string"))
        .withColumn("JCN", func.lit(None).cast("string"))
        .withColumn("FRAMEWORK", _framework_of(func.col("CONCEPT")))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .select(*fact_schema.names)
    )


def facts_from_sec(spark, config_path):
    """The SEC facts, in the columns of the combined table.

    The period the filing reports on is on that module's table of filings and
    not on its facts, so the two are joined on the accession number. ``TEXT``
    and ``DIMENSIONS`` come back empty because the company files carry
    neither.
    """
    from ..sec.common import ref_silver_facts, ref_silver_filings
    from ...storage import read_silver

    facts = read_silver(spark, ref_silver_facts(config_path))
    filings = read_silver(spark, ref_silver_filings(config_path)) \
        .select("FILING", "PERIODEND")

    return (
        facts.join(filings, "FILING", "left")
        .withColumn("SOURCE", func.lit("sec"))
        .withColumn("LEI", func.lit(None).cast("string"))
        .withColumn("EDINETCODE", func.lit(None).cast("string"))
        .withColumn("JCN", func.lit(None).cast("string"))
        .withColumn("FRAMEWORK", _framework_of(func.col("CONCEPT")))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .withColumn("TEXT", func.lit(None).cast("string"))
        .withColumn("DIMENSIONS", func.lit(None).cast("string"))
        .select(*fact_schema.names)
    )


def filings_from_esef(spark, config_path):
    """The ESEF filings, in the columns of the combined table."""
    from ..esef.common import ref_silver_facts, ref_silver_filings
    from ...storage import read_silver

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
        .withColumn("EDINETCODE", func.lit(None).cast("string"))
        .withColumn("JCN", func.lit(None).cast("string"))
        .withColumn("FORM", func.lit(None).cast("string"))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .withColumn("FACTS", func.coalesce("FACTS", func.lit(0)))
        .select(*filing_schema.names)
    )


def filings_from_sec(spark, config_path):
    """The SEC filings, in the columns of the combined table."""
    from ..sec.common import ref_silver_filings
    from ...storage import read_silver

    return (
        read_silver(spark, ref_silver_filings(config_path))
        .withColumn("SOURCE", func.lit("sec"))
        .withColumn("LEI", func.lit(None).cast("string"))
        .withColumn("EDINETCODE", func.lit(None).cast("string"))
        .withColumn("JCN", func.lit(None).cast("string"))
        .withColumn("COUNTRY", func.lit(None).cast("string"))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .select(*filing_schema.names)
    )


#: What a Japanese filing states it reports under, and the name the combined
#: table records it as. A Japanese report says so in its own document and
#: entity information rather than leaving it to be read off a prefix, which
#: is as well: a group reporting under IFRS tags its statements against the
#: Japanese IFRS taxonomy and its front matter against the same disclosure
#: taxonomy every other filer uses, so the prefix of a fact does not settle
#: it. ``JP-GAAP`` has no counterpart in the other two regimes.
JAPANESE_STANDARDS = {"IFRS": "IFRS", "US GAAP": "US-GAAP",
                      "Japan GAAP": "JP-GAAP"}

#: The element a Japanese filing states its accounting standard under.
STANDARDS_ELEMENT = "jpdei_cor:AccountingStandardsDEI"


#: The two Japanese taxonomies that settle the framework by themselves,
#: whatever the filing says it reports under. ``jpigp_cor`` is the taxonomy
#: of the consolidated statements of a filer that has adopted IFRS and
#: ``jppfs_cor`` the one of the statements the Ordinance on Financial
#: Statements prescribes, which are Japan GAAP by construction.
TAXONOMY_FRAMEWORKS = {"jpigp_cor": "IFRS", "jppfs_cor": "JP-GAAP"}


def _stated_framework(facts):
    """One row per filing: what it says it reports under.

    Mapped through :data:`JAPANESE_STANDARDS`; a filing stating a standard
    this module has no name for keeps it as filed rather than being called
    something it is not.
    """
    stated = func.trim(func.col("TEXT"))
    named = func.lit(None).cast("string")
    for japanese, name in JAPANESE_STANDARDS.items():
        named = func.when(stated == japanese, func.lit(name)).otherwise(named)
    return (facts
            .where(func.col("CONCEPT") == STANDARDS_ELEMENT)
            .groupBy("FILING")
            .agg(func.max(func.coalesce(named, stated)).alias("_STATED")))


def _japanese_framework(concept, stated):
    """What one Japanese fact was prepared under, as a Spark expression.

    The taxonomy decides it where the taxonomy is decisive, and the filing
    decides it otherwise. That order matters, and a Japanese IFRS filer is
    why: its consolidated statements are IFRS and its parent company's own
    statements are Japan GAAP, both in one report, and a framework taken
    from the filing alone would call the parent's revenue IFRS. Fujitsu
    states 3,502,971 million of revenue under ``jpigp_cor`` for the group
    and 1,692,618 million under ``jppfs_cor`` for the company, and they are
    not figures of one framework.

    What the taxonomy does not settle -- the disclosure ordinance's own
    elements, the document and entity information, a concept the filer
    created -- takes the filing's answer. One residue stays wrong and is
    worth naming: the five-year summary of an IFRS filer states the parent's
    Japan GAAP series under the same disclosure taxonomy as the group's IFRS
    one, and those rows carry the filing's framework.
    """
    expression = stated
    for prefix, framework in TAXONOMY_FRAMEWORKS.items():
        expression = func.when(concept.startswith(f"{prefix}:"),
                               func.lit(framework)).otherwise(expression)
    return expression


def facts_from_edinet(spark, config_path):
    """The EDINET facts, in the columns of the combined table.

    The period the filing reports on is on that module's table of filings and
    not on its facts, as with the Commission, so the two are joined on the
    document number. ``TEXT`` and ``DIMENSIONS`` are carried as they stand:
    the agency's conversion gives the whole of a report, and both the
    narrative facts and the breakdowns are in it.

    What travels in ``DIMENSIONS`` is worth knowing before the rows are used.
    A report that prepares consolidated statements states every figure twice,
    the group's with no dimension and the parent company's own under
    ``NonConsolidatedMember``, and both are here because both are in the
    source table.

    One kind of fact is left out, and it is the only thing this module drops.
    The agency's conversion carries no dates, so the edinet module resolves
    the period of a fact from what the filing says about itself and leaves
    both dates empty for a context whose shape it does not recognise rather
    than inventing them. ``PRDEND`` is the one column of the combined table
    of facts that is not allowed to be empty -- a fact that sits in no period
    can be compared with nothing, which is what this table is for -- so such
    a fact is not carried here. It is not lost: it is in the edinet table of
    facts, and the ``PERIOD_UNRESOLVED`` check of that module's quality
    module counts them.
    """
    from ...storage import read_silver
    from ..edinet.common import ref_silver_facts, ref_silver_filings

    facts = read_silver(spark, ref_silver_facts(config_path))
    filings = read_silver(spark, ref_silver_filings(config_path)) \
        .select("FILING", "PERIODEND", "JCN")

    return (
        facts.where(func.col("PRDEND").isNotNull())
        .join(filings, "FILING", "left")
        .join(_stated_framework(facts), "FILING", "left")
        .withColumn("SOURCE", func.lit("edinet"))
        .withColumn("LEI", func.lit(None).cast("string"))
        .withColumn("CIK", func.lit(None).cast("string"))
        .withColumn("FRAMEWORK",
                    _japanese_framework(func.col("CONCEPT"),
                                        func.col("_STATED")))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .select(*fact_schema.names)
    )


def filings_from_edinet(spark, config_path):
    """The EDINET filings, in the columns of the combined table.

    ``FORM`` is the document type as the agency codes it -- ``'120'`` is the
    annual securities report -- and not an English name this module made up
    for it; see
    :data:`fincommons.financialreport.edinet.common.REPORT_TYPES`. ``COUNTRY``
    is empty because the agency states none, as the Commission does not.

    The count of facts comes from the facts table, as it does for ESEF: the
    index says how many documents were filed on a day and nothing about how
    many facts are in one. A filing the agency never converted has no facts
    here and is carried with a count of nothing, which is what says the
    difference between a filing that is empty and one that was never read.
    """
    from ...storage import read_silver
    from ..edinet.common import ref_silver_facts, ref_silver_filings

    filings = read_silver(spark, ref_silver_filings(config_path))
    facts = read_silver(spark, ref_silver_facts(config_path))

    about = (facts.groupBy("FILING")
             .agg(func.count(func.lit(1)).alias("FACTS")))

    return (
        filings.join(about, "FILING", "left")
        .join(_stated_framework(facts), "FILING", "left")
        .withColumn("SOURCE", func.lit("edinet"))
        .withColumn("FRAMEWORK", func.col("_STATED"))
        .withColumn("LEI", func.lit(None).cast("string"))
        .withColumn("CIK", func.lit(None).cast("string"))
        .withColumn("COUNTRY", func.lit(None).cast("string"))
        .withColumn("FORM", func.col("DOCTYPE"))
        .withColumn("PERIODYEAR", func.year("PERIODEND"))
        .withColumn("FACTS", func.coalesce("FACTS", func.lit(0)))
        .select(*filing_schema.names)
    )


#: How each source is read, by the name the combined tables record it under.
READERS = {
    "esef": (facts_from_esef, filings_from_esef),
    "sec": (facts_from_sec, filings_from_sec),
    "edinet": (facts_from_edinet, filings_from_edinet),
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
    "JAPANESE_STANDARDS",
    "READERS",
    "STANDARDS_ELEMENT",
    "TAXONOMY_FRAMEWORKS",
    "available_sources",
    "combine",
    "facts_from_edinet",
    "facts_from_esef",
    "facts_from_sec",
    "filings_from_edinet",
    "filings_from_esef",
    "filings_from_sec",
    "update_silver_financialreport",
]
