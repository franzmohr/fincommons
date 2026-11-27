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
    .add("source", StringType(), False) \
    .add("filing", StringType(), False) \
    .add("fact", StringType(), False) \
    .add("lei", StringType(), True) \
    .add("cik", StringType(), True) \
    .add("edinet_code", StringType(), True) \
    .add("jcn", StringType(), True) \
    .add("framework", StringType(), True) \
    .add("period_end", DateType(), True) \
    .add("period_year", IntegerType(), True) \
    .add("concept", StringType(), False) \
    .add("measure", StringType(), True) \
    .add("prd_start", DateType(), True) \
    .add("prd_end", DateType(), False) \
    .add("unit", StringType(), True) \
    .add("value", DoubleType(), True) \
    .add("text", StringType(), True) \
    .add("dimensions", StringType(), True)
"""The shape of the combined table of facts: one row per fact of a filing.

``source`` says which module the row came from and is the first partition
column, so that a question scoped to one regime passes over the other.

Four identifier columns are carried, one regime's own in each, and a row has
the column of its source filled and the others empty. ESEF prescribes the
legal entity identifier, which is the key of
:mod:`fincommons.entitymasterdata` as well; a filing with the Commission
states a central index key; a filing with Japan's agency states an EDINET
code, and ``jcn`` beside it is the corporate number of the National Tax
Agency.

Nothing here is derived from anything else here, and for two of the three
regimes nothing could be. Neither the Commission nor GLEIF publishes a
mapping from a central index key to an LEI: GLEIF publishes where an entity
is registered, and for some tens of thousands of records that register is
EDGAR, but nearly all of those are funds named by a series identifier, while
an operating company's record points at its state register instead.

``jcn`` is the exception and is the reason it is carried. The corporate
number is a check digit followed by the twelve-digit register number of the
commercial register, and GLEIF records that same register number against an
LEI issued to a Japanese company, so the two can be joined by arithmetic
rather than by a lookup table or a comparison of names. It reaches about one
listed Japanese filer in seven, the rest having no LEI at all. That join is
not made here: it needs GLEIF, which this module does not read, and the
column is carried so that whoever has GLEIF can make it. See the edinet
module's README for the check and what it found.

``framework`` is IFRS, US-GAAP or JP-GAAP, taken from what the filing reports
under rather than from the regime it was filed under. That is what makes the
population where the regimes genuinely meet usable: a foreign private issuer
reporting under IFRS files with the Commission and tags against the IFRS
taxonomy, and is comparable with the European reports rather than with the
American ones beside it -- and so is a Japanese group that has adopted IFRS,
which files in Tokyo and tags against the Japanese IFRS taxonomy. JP-GAAP has
no counterpart in either of the other two regimes and is not comparable with
them line by line; a Japanese profit and loss account has two results where
an IFRS one has a single operating line.

``concept`` is the concept as the filing tagged it, prefix and name, and is
never altered: ``ifrs-full:Revenue``, ``us-gaap:Revenues`` and
``jppfs_cor:NetSales`` stand as they were filed, so that what a filer said
is always in the table.

``measure`` is the other half of that, and the one a question is put to. It
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
framework. It covers all eight measures the catalogue defines, each of them
written down one at a time and none of them materialised by being added to
the catalogue. What a measure means, and
what differs between the frameworks that state it, is on the catalogue row;
``describe_measure`` prints it.

The same figure is not always in the same place. Cash and cash equivalents
is on the balance sheet under IFRS and under US GAAP, and a Japanese balance
sheet has no line that means it -- it presents 現金及び預金, cash together
with deposits, time deposits of more than three months among them -- so for
Japan GAAP the identifier is given to the closing balance of the cash flow
statement. That is the case the column exists for: one name over one figure,
wherever its regime chose to present it.

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
for a substantial part of the US population.

Total equity and profit or loss sharpen that into a skew, and one that is
not the same in every regime. The first concept of each under US GAAP is the
total including the non-controlling interests, and many registrants tag only
``us-gaap:StockholdersEquity`` and ``us-gaap:NetIncomeLoss``, which exclude
them. For a registrant that has none the two are the same number -- but the
element does not say whether it has none, so naming it would be right for
most filings and wrong for exactly the groups a cross-regime comparison is
about.

Japan does not fail the same way. A company with no non-controlling
interests states 当期純利益 under ``jppfs_cor:ProfitLoss`` as every filer
does, and 純資産 likewise, so the column names it. The same company filing
with the Commission states only ``us-gaap:NetIncomeLoss`` and the column
names nothing, though the figure is the same one. The Japanese population
under these measures is close to every filer and the American population is
conditional on having non-controlling interests to report: two samples
selected by different rules, whose medians are not a comparison of the two
regimes. Counting the rows carrying a measure says how many filings were
named, never how many exist.

``profit_loss_attributable_to_parent`` names the American facts the other
declines, and inverts the skew rather than removing it. The attributable
line exists only where there are minorities to attribute away, so an IFRS
or Japanese filer preparing no consolidated statements states none, while
``us-gaap:NetIncomeLoss`` is the ordinary bottom line every registrant
tags. Between them the two measures cover the population well; neither does
alone.

That is the limit of this column, and it is where
:func:`fincommons.financialreport.combined.mapping.measure_values` is the
right tool instead. The figure a population-level comparison wants is the
result attributable to the parent falling back to the total where there are
no minorities to separate, which is a judgement per filing and not per
concept. Filter on ``measure`` to ask a precise question of named facts;
resolve through ``measure_values`` to ask about a population.

One gap is of a third kind and is the only one. Every absence described
above depends on what a filer tagged, and changing the filer closes it.
``equity_attributable_to_parent`` has none under Japan GAAP whatever the
filer does: there is no element for 自己資本, it being 株主資本 plus
評価・換算差額等, a sum rather than a line. A query filtering on that measure
excludes Japan entirely and says nothing about having done so. The sum is
not computed here to close it, and taking ``jppfs_cor:ShareholdersEquity``
instead is short by the valuation and translation adjustments, 9,191 million
of a correct 63,326 million on one report read for the catalogue. It is missing rather than zero,
and it is not derived from the balance sheet identity to fill the gap --
nothing in this family of modules invents a figure, and what that costs is
on record in the edinet checks, where reading a missing total as zero made
one filer's liabilities short by 244,695 million. A count of the rows
carrying a measure is a count of what was stated and named, never a
population.

``text`` and ``dimensions`` are filled for the rows that came from ESEF and
from EDINET, and empty for every row the Commission contributes -- not
because this module drops them, but because the company files carry no
narrative facts and no breakdowns at all. An empty ``dimensions`` means the
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
    .add("source", StringType(), False) \
    .add("filing", StringType(), False) \
    .add("lei", StringType(), True) \
    .add("cik", StringType(), True) \
    .add("edinet_code", StringType(), True) \
    .add("jcn", StringType(), True) \
    .add("name", StringType(), True) \
    .add("framework", StringType(), True) \
    .add("country", StringType(), True) \
    .add("form", StringType(), True) \
    .add("period_end", DateType(), True) \
    .add("period_year", IntegerType(), True) \
    .add("facts", IntegerType(), False)
"""The shape of the combined table of filings: one row per filing.

``country`` is filled for an ESEF filing, which is filed with the mechanism of
one member state, and empty for a filing with the Commission or with Japan's
agency, neither of which states one. ``form`` is the other way round: the
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
FACT_PARTITION_BY = ("source", "period_year")


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
    # Parquet puts the partition columns of the table of facts last; the
    # columns are returned in the declared order whatever the format.
    if table == "fact":
        return read_silver(spark, ref_silver_fact(config_path)) \
            .select(*fact_schema.names)
    if table == "filing":
        return read_silver(spark, ref_silver_filing(config_path)) \
            .select(*filing_schema.names)
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
            "entity": "lei",
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
            "entity": "cik",
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
            "entity": "edinet_code",
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
