"""Financial reports: the layout of the combined tables.

This module holds no data of its own and collects nothing. It reads the Silver
tables of :mod:`fincommons.financialreport.esef` and :mod:`fincommons.financialreport.sec` and writes two
tables derived from them, so that a question put to the filings of both
regimes at once is put to one table rather than to two of different shapes.

What it does not do is take anything away from either module. The tables of
those two stay authoritative, complete and queryable on their own; neither
imports this module and neither knows it exists. An ESEF-only lake builds
nothing here and loses nothing by it, and the factsheet and coverage report
that :mod:`fincommons.financialreport.esef` ships keep working untouched. The dependency runs
one way, and only one way.

The configuration file names a Silver layer and nothing else, since there is
no Bronze layer to name::

    financialreport:
        silver: /data/financialreport/silver

Which sources a run combines is decided by which of the two have tables to
read, not by this module: a lake that has collected only ESEF filings gets a
combined table of ESEF filings, which is the same question answered over the
population there is.

The two sources do not cover the same ground, and the difference is a property
of what each publisher gives out rather than of this module. filings.xbrl.org
publishes the facts of an ESEF report as xBRL-JSON, dimensions and text
blocks among them. The Commission publishes the undimensioned numeric facts
of a filing and not the whole of it: a quarterly report stating 941 facts is
carried as 321, with the breakdowns by segment and by class absent and the
narrative absent with them. A figure for an entity as a whole is therefore
comparable across the two; a breakdown is available on one side only.
:func:`coverage` is what reports that rather than leaving it to be discovered.
"""

import os

from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructType)

from ...addressing import resolve
from ...storage import read_silver

#: The sources the combined tables are built from, and the module each is
#: read from. They are named here rather than imported, so that this module
#: can be read, and its tables described, in a lake that has neither.
SOURCES = {"esef": "fincommons.financialreport.esef",
           "sec": "fincommons.financialreport.sec",
           "edinet": "fincommons.financialreport.edinet"}


fact_schema = StructType() \
    .add("SOURCE", StringType(), False) \
    .add("FILING", StringType(), False) \
    .add("FACT", StringType(), False) \
    .add("LEI", StringType(), True) \
    .add("CIK", StringType(), True) \
    .add("EDINETCODE", StringType(), True) \
    .add("JCN", StringType(), True) \
    .add("FRAMEWORK", StringType(), True) \
    .add("PERIODEND", DateType(), True) \
    .add("PERIODYEAR", IntegerType(), True) \
    .add("CONCEPT", StringType(), False) \
    .add("MEASURE", StringType(), True) \
    .add("PRDSTART", DateType(), True) \
    .add("PRDEND", DateType(), False) \
    .add("UNIT", StringType(), True) \
    .add("VALUE", DoubleType(), True) \
    .add("TEXT", StringType(), True) \
    .add("DIMENSIONS", StringType(), True)
"""The shape of the combined table of facts: one row per fact of a filing.

``SOURCE`` says which module the row came from and is the first partition
column, so that a question scoped to one regime passes over the other.

Four identifier columns are carried, one regime's own in each, and a row has
the column of its source filled and the others empty. ESEF prescribes the
legal entity identifier, which is the key of
:mod:`fincommons.entitymasterdata` as well; a filing with the Commission
states a central index key; a filing with Japan's agency states an EDINET
code, and ``JCN`` beside it is the corporate number of the National Tax
Agency.

Nothing here is derived from anything else here, and for two of the three
regimes nothing could be. Neither the Commission nor GLEIF publishes a
mapping from a central index key to an LEI: GLEIF publishes where an entity
is registered, and for some tens of thousands of records that register is
EDGAR, but nearly all of those are funds named by a series identifier, while
an operating company's record points at its state register instead.

``JCN`` is the exception and is the reason it is carried. The corporate
number is a check digit followed by the twelve-digit register number of the
commercial register, and GLEIF records that same register number against an
LEI issued to a Japanese company, so the two can be joined by arithmetic
rather than by a lookup table or a comparison of names. It reaches about one
listed Japanese filer in seven, the rest having no LEI at all. That join is
not made here: it needs GLEIF, which this module does not read, and the
column is carried so that whoever has GLEIF can make it. See the edinet
module's README for the check and what it found.

``FRAMEWORK`` is IFRS, US-GAAP or JP-GAAP, taken from what the filing reports
under rather than from the regime it was filed under. That is what makes the
population where the regimes genuinely meet usable: a foreign private issuer
reporting under IFRS files with the Commission and tags against the IFRS
taxonomy, and is comparable with the European reports rather than with the
American ones beside it -- and so is a Japanese group that has adopted IFRS,
which files in Tokyo and tags against the Japanese IFRS taxonomy. JP-GAAP has
no counterpart in either of the other two regimes and is not comparable with
them line by line; a Japanese profit and loss account has two results where
an IFRS one has a single operating line.

``CONCEPT`` is the concept as the filing tagged it, prefix and name, and is
never altered: ``ifrs-full:Revenue``, ``us-gaap:Revenues`` and
``jppfs_cor:NetSales`` stand as they were filed, so that what a filer said
is always in the table.

``MEASURE`` is the other half of that, and the one a question is put to. It
is the identifier of what the fact *is*, the same for every regime that
states the same figure, and it is filled only for the concepts
:data:`fincommons.financialreport.combined.mapping.MATERIALISED_MEASURES`
covers. Everything else carries a null, which today is the great majority of
the table: a filing states thousands of facts and the identifier is given to
a handful of them on purpose.

The restraint is the point. A materialised identifier is read by people who
will not go back to the caveat, so a measure is written into this column only
once its concepts are agreed to mean the same figure in every framework the
catalogue defines it for, and only through the first concept of each
framework. It covers total assets, total liabilities and revenue, and grows
one measure at a time. What a measure means, and what differs between the
frameworks that state it, is on the catalogue row; ``describe_measure``
prints it.

Only the first concept, because the catalogue ranks the others as
approximations and says so. A caller resolving a measure at query time wants
the best answer the filing affords and takes ``us-gaap:Revenues`` where there
is nothing better; this column is held to the stricter bar, and that element
"is the last resort and not an equivalent of the IFRS total" by its own
caveat. So a fact tagged with it keeps a null. The cost is coverage and not
correctness, which is the right way round for an identifier a query filters
on.

It is also what keeps the column answerable at all. A filing may tag several
concepts of one measure -- that is why the catalogue ranks them -- and
labelling all of them would put two different figures of one filing and one
period under one name. At the first concept there is one element per taxonomy
per measure, so that cannot happen, and
:func:`fincommons.financialreport.combined.mapping.canonical_concepts`
refuses a catalogue in which it could.

A null here therefore means one of two things, and the column cannot tell
them apart. Either no measure covers the concept the filer tagged, or the
measure covers a concept the filer did not tag at all. Total liabilities is
where the second matters: many filings with the Commission present only
liabilities and equity together and tag no total, so the figure is missing
for a substantial part of the US population. It is missing rather than zero,
and it is not derived from the balance sheet identity to fill the gap --
nothing in this family of modules invents a figure, and what that costs is
on record in the edinet checks, where reading a missing total as zero made
one filer's liabilities short by 244,695 million. A count of the rows
carrying a measure is a count of what was stated and named, never a
population.

``TEXT`` and ``DIMENSIONS`` are filled for the rows that came from ESEF and
from EDINET, and empty for every row the Commission contributes -- not
because this module drops them, but because the company files carry no
narrative facts and no breakdowns at all. An empty ``DIMENSIONS`` means the
fact is for the entity as a whole, which is true of every row from the
Commission and of only some of the rows from the other two.

A Japanese row needs one thing read before it is compared with anything: a
report that prepares consolidated statements states every figure twice, the
group's with no dimension and the parent company's own under
``NonConsolidatedMember``. Both are in this table, as both are in the source
table, so a question that does not say which it wants is asking for two sets
of accounts at once.
"""

filing_schema = StructType() \
    .add("SOURCE", StringType(), False) \
    .add("FILING", StringType(), False) \
    .add("LEI", StringType(), True) \
    .add("CIK", StringType(), True) \
    .add("EDINETCODE", StringType(), True) \
    .add("JCN", StringType(), True) \
    .add("NAME", StringType(), True) \
    .add("FRAMEWORK", StringType(), True) \
    .add("COUNTRY", StringType(), True) \
    .add("FORM", StringType(), True) \
    .add("PERIODEND", DateType(), True) \
    .add("PERIODYEAR", IntegerType(), True) \
    .add("FACTS", IntegerType(), False)
"""The shape of the combined table of filings: one row per filing.

``COUNTRY`` is filled for an ESEF filing, which is filed with the mechanism of
one member state, and empty for a filing with the Commission or with Japan's
agency, neither of which states one. ``FORM`` is the other way round: the
Commission names the form a filing was made on and the agency gives it a
numeric document type, while the ESEF regime has one kind of report and names
none. Each is empty where its regime states nothing, rather than being given
a value this module invented -- which is why an EDINET row carries ``'120'``
and not an English name for it; see
:data:`fincommons.financialreport.edinet.common.REPORT_TYPES` for what the
codes mean.
"""

#: The columns the combined table of facts is partitioned by. The source
#: first, because most questions are scoped to one regime; then the year of
#: the period reported on, which is what a question about a figure is scoped
#: to after that.
FACT_PARTITION_BY = ("SOURCE", "PERIODYEAR")


def path_silver_base(config_path):
    """Directory of the Silver layer, or ``None`` where it is a schema."""
    return resolve(config_path, "financialreport", "silver").path


def ref_silver_fact(config_path):
    """Where the combined table of facts lives."""
    return resolve(config_path, "financialreport", "silver", "fact")


def ref_silver_filing(config_path):
    """Where the combined table of filings lives."""
    return resolve(config_path, "financialreport", "silver", "filing")


def path_silver_fact(config_path):
    """Path of the combined table of facts, or ``None`` where it is named."""
    return ref_silver_fact(config_path).path


def path_silver_filing(config_path):
    """Path of the combined table of filings, or ``None`` where it is named."""
    return ref_silver_filing(config_path).path


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    There is only a Silver layer to create, and only where it is a directory
    rather than a schema in a catalogue.
    """
    silver = resolve(config_path, "financialreport", "silver")
    if not silver.is_named and silver.path is not None:
        os.makedirs(silver.path, exist_ok=True)


def connect_to_financialreport(spark, config_path, table="fact"):
    """Open one of the combined tables.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    table: str
        Name of the table, ``'fact'`` or ``'filing'``.
    """
    if table == "fact":
        return read_silver(spark, ref_silver_fact(config_path))
    if table == "filing":
        return read_silver(spark, ref_silver_filing(config_path))
    raise ValueError(f"Table {table!r} not recognised. Use 'fact' or 'filing'.")


def coverage():
    """What each source contributes, and what it cannot.

    A reader of the combined table is entitled to know that an absent
    breakdown may mean the issuer did not report one or may mean the
    publisher does not give them out, and that which of the two it is depends
    on the source. This says so in a form a notebook can print beside a
    result.

    Returns
    -------
    list of dict
        One entry per source.
    """
    return [
        {
            "source": "esef",
            "publisher": "filings.xbrl.org",
            "facts": "every fact of the report, as xBRL-JSON",
            "dimensions": True,
            "text": True,
            "entity": "LEI",
            "note": "The repository collects what it can from the national "
                    "mechanisms, which is a large part of the population and "
                    "not the whole of it.",
        },
        {
            "source": "sec",
            "publisher": "sec.gov",
            "facts": "the undimensioned numeric facts of the filing",
            "dimensions": False,
            "text": False,
            "entity": "CIK",
            "note": "A quarterly report stating 941 facts is carried as 321: "
                    "the breakdowns by segment and by class are absent, and "
                    "the narrative with them.",
        },
        {
            "source": "edinet",
            "publisher": "disclosure2.edinet-fsa.go.jp",
            "facts": "every fact of the report, as the agency's own CSV",
            "dimensions": True,
            "text": True,
            "entity": "EDINETCODE",
            "note": "A report that consolidates states every figure twice, "
                    "the group's without a dimension and the parent "
                    "company's under NonConsolidatedMember; and the period "
                    "of a fact older than the comparative is shifted by "
                    "whole years rather than reported. A fact the module "
                    "could place in no period at all is not carried here, "
                    "PRDEND being the one column that may not be empty; the "
                    "edinet table of facts has it and that module's "
                    "PERIOD_UNRESOLVED check counts them.",
        },
    ]


#: What this module offers. The layout of the combined tables, and what the
#: sources behind them do and do not cover.
__all__ = [
    "FACT_PARTITION_BY",
    "SOURCES",
    "connect_to_financialreport",
    "coverage",
    "create_directory_tree",
    "fact_schema",
    "filing_schema",
    "path_silver_base",
    "path_silver_fact",
    "path_silver_filing",
    "ref_silver_fact",
    "ref_silver_filing",
]
