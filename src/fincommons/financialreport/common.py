"""Financial reports: the layout of the combined tables.

This module holds no data of its own and collects nothing. It reads the Silver
tables of :mod:`fincommons.esef` and :mod:`fincommons.secfiling` and writes two
tables derived from them, so that a question put to the filings of both
regimes at once is put to one table rather than to two of different shapes.

What it does not do is take anything away from either module. The tables of
those two stay authoritative, complete and queryable on their own; neither
imports this module and neither knows it exists. An ESEF-only lake builds
nothing here and loses nothing by it, and the factsheet and coverage report
that :mod:`fincommons.esef` ships keep working untouched. The dependency runs
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

from ..addressing import resolve
from ..storage import read_silver

#: The sources the combined tables are built from, and the module each is
#: read from. They are named here rather than imported, so that this module
#: can be read, and its tables described, in a lake that has neither.
SOURCES = {"esef": "fincommons.esef", "secfiling": "fincommons.secfiling"}


fact_schema = StructType() \
    .add("SOURCE", StringType(), False) \
    .add("FILING", StringType(), False) \
    .add("FACT", StringType(), False) \
    .add("LEI", StringType(), True) \
    .add("CIK", StringType(), True) \
    .add("FRAMEWORK", StringType(), True) \
    .add("PERIODEND", DateType(), True) \
    .add("PERIODYEAR", IntegerType(), True) \
    .add("CONCEPT", StringType(), False) \
    .add("PRDSTART", DateType(), True) \
    .add("PRDEND", DateType(), False) \
    .add("UNIT", StringType(), True) \
    .add("VALUE", DoubleType(), True) \
    .add("TEXT", StringType(), True) \
    .add("DIMENSIONS", StringType(), True)
"""The shape of the combined table of facts: one row per fact of a filing.

``SOURCE`` says which module the row came from and is the first partition
column, so that a question scoped to one regime passes over the other.

``LEI`` and ``CIK`` are both carried and neither is derived from the other.
ESEF prescribes the legal entity identifier, which is the key of
:mod:`fincommons.entitymasterdata` as well; a filing with the Commission
states a central index key, and neither body publishes a mapping between the
two. GLEIF publishes where an entity is registered, and for some tens of
thousands of records that register is EDGAR -- but nearly all of those are
funds, named by a series identifier rather than by a central index key, and
an operating company's record points at its state register instead. A row
therefore has one of the two filled and the other empty, and joining the
regimes by entity is not possible here. It is the one thing a lake holding
both would most like to do, and saying so is better than filling the column
with a guess.

``FRAMEWORK`` is IFRS or US-GAAP, taken from the taxonomies the filing tagged
against rather than from the regime it was filed under. That is what makes the
population where the two genuinely meet usable: a foreign private issuer
reporting under IFRS files with the Commission and tags against the IFRS
taxonomy, and is comparable with the European reports rather than with the
American ones beside it.

``CONCEPT`` is the concept as the filing tagged it, prefix and name, and is
not resolved across frameworks. ``ifrs-full:Revenue`` and
``us-gaap:Revenues`` stand as they were filed; what the two have in common is
a judgement, and :mod:`fincommons.financialreport.mapping` is where it is
made.

``TEXT`` and ``DIMENSIONS`` are filled only for rows that came from ESEF, and
not because this module drops them for the other: the company files of the
Commission carry no narrative facts and no breakdowns at all. An empty
``DIMENSIONS`` means the fact is for the entity as a whole, which is true of
every row the Commission contributes.
"""

filing_schema = StructType() \
    .add("SOURCE", StringType(), False) \
    .add("FILING", StringType(), False) \
    .add("LEI", StringType(), True) \
    .add("CIK", StringType(), True) \
    .add("NAME", StringType(), True) \
    .add("FRAMEWORK", StringType(), True) \
    .add("COUNTRY", StringType(), True) \
    .add("FORM", StringType(), True) \
    .add("PERIODEND", DateType(), True) \
    .add("PERIODYEAR", IntegerType(), True) \
    .add("FACTS", IntegerType(), False)
"""The shape of the combined table of filings: one row per filing.

``COUNTRY`` is filled for an ESEF filing, which is filed with the mechanism of
one member state, and empty for a filing with the Commission, which is not.
``FORM`` is the other way round: the Commission names the form a filing was
made on, and the ESEF regime has one kind of report and names none. Each is
empty where its regime states nothing, rather than being given a value this
module invented.
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
            "source": "secfiling",
            "publisher": "sec.gov",
            "facts": "the undimensioned numeric facts of the filing",
            "dimensions": False,
            "text": False,
            "entity": "CIK",
            "note": "A quarterly report stating 941 facts is carried as 321: "
                    "the breakdowns by segment and by class are absent, and "
                    "the narrative with them.",
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
