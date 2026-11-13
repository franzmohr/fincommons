"""SEC filings: the layout of the data lake.

The Bronze and the Silver layer are addressed through
:mod:`fincommons.addressing`, which reads the ``secfiling`` section of the
configuration file, so that the same code runs against a local directory
tree, against a Unity Catalog volume and against a schema in a catalogue::

    secfiling:
        bronze: /data/secfiling/bronze
        silver: /data/secfiling/silver

The Commission publishes the tagged figures of every registrant twice over: as
the filings themselves, which are inline XBRL documents, and as one JSON file
per company holding the facts already parsed out of them. This module reads
the second, as :mod:`fincommons.esef` reads the xBRL-JSON that
filings.xbrl.org publishes beside the ESEF report packages. Neither module
parses inline XBRL, and neither needs a parser installed.

What that costs is worth stating where it will be read. A company file holds
the undimensioned numeric facts of a filing and not the whole of it: of a
quarterly report of 2026 that states 941 facts, 480 of them without a
dimension, the company file carries 321. The breakdowns by segment and by
class are absent, and so are the narrative facts and the text blocks of the
notes. What is there is the consolidated figures, which is what a comparison
across the two regimes is made of; what is not there needs the filings
themselves and a parser, and is a module of its own if it is ever wanted.

The Bronze layer keeps what the Commission publishes, as it was published:
``companies`` holds one JSON file per registrant, and ``collections`` a small
record of when each was collected and what the server said about it, which is
how the next run knows what it need not download again.

The Silver layer holds two tables. ``filings`` lists every filing the company
files name, one row per filing, and ``facts`` holds their facts, one row per
fact, partitioned by the year the filing was made and rebuilt year by year.

The two tables are keyed by the central index key of the registrant. There is
no authoritative mapping from it to the legal entity identifier that
:mod:`fincommons.esef` and :mod:`fincommons.entitymasterdata` are keyed by,
published by either body, and this module invents none.
"""

import json
import os

from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructType, TimestampType)

from ..addressing import resolve
from ..storage import read_silver

#: The server the company files are published on.
BASE_URL = "https://data.sec.gov"

#: The facts of one registrant, already parsed out of its filings. The key is
#: the central index key, padded to ten digits.
COMPANY_URL = BASE_URL + "/api/xbrl/companyfacts/CIK{cik}.json"

#: Every company file in one archive, about 1.4 GB. The Commission asks that
#: a client wanting many companies take this rather than a request each, and
#: a lake that wants the whole population wants this one file and not ten
#: thousand requests.
BULK_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"

#: The registrants of the Commission, by ticker and central index key. It is
#: how a lake that wants companies by name rather than by number finds them.
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

#: The name the publisher is given in log records and messages.
SOURCE = "sec.gov"


filings_schema = StructType() \
    .add("FILING", StringType(), False) \
    .add("CIK", StringType(), False) \
    .add("NAME", StringType(), True) \
    .add("FORM", StringType(), False) \
    .add("FILED", DateType(), False) \
    .add("PERIODEND", DateType(), True) \
    .add("FY", IntegerType(), True) \
    .add("FP", StringType(), True) \
    .add("TAXONOMIES", StringType(), True) \
    .add("FRAMEWORK", StringType(), True) \
    .add("FACTS", IntegerType(), False)
"""The shape of the table of filings: one row per filing the facts name.

``FILING`` is the accession number, which identifies a filing with the
Commission. The row is reconstructed from the facts rather than read from an
index of filings: a company file states, for every fact, the filing it came
from, so grouping the facts by accession gives back what each filing stated.

``PERIODEND`` is the latest period the filing reports on, which for a periodic
report is the period it is about; the earlier periods in it are its
comparatives. ``FRAMEWORK`` is IFRS or US-GAAP, taken from the taxonomies the
filing tagged against: a foreign private issuer reporting under IFRS tags
against the IFRS taxonomy and is recognised as such, rather than being taken
for a US GAAP filer because of where it filed.
"""

facts_schema = StructType() \
    .add("CIK", StringType(), False) \
    .add("FILING", StringType(), False) \
    .add("FILEDYEAR", IntegerType(), False) \
    .add("FACT", StringType(), False) \
    .add("CONCEPT", StringType(), False) \
    .add("PRDSTART", DateType(), True) \
    .add("PRDEND", DateType(), False) \
    .add("UNIT", StringType(), True) \
    .add("VALUE", DoubleType(), True) \
    .add("FORM", StringType(), True) \
    .add("FILED", DateType(), True) \
    .add("FRAME", StringType(), True)
"""The shape of the table of facts: one row per fact of a filing.

The columns a fact shares with :data:`fincommons.esef.common.facts_schema`
are spelled as that module spells them -- ``FILING``, ``FACT``, ``CONCEPT``,
``PRDSTART``, ``PRDEND``, ``UNIT``, ``VALUE`` -- so that a module combining
the two is combining two tables of the same shape rather than translating one
into the other. A fact of a duration has ``PRDSTART``; one of an instant has
none, and ``PRDEND`` is the day it stands for.

Four columns of that table are missing here, and each is missing because the
company files do not carry it rather than because this module dropped it.
There is no ``DIMENSIONS`` and no ``TEXT``: the files hold the undimensioned
numeric facts and neither the breakdowns nor the narrative. There is no
``DECIMALS``: the Commission publishes the value and not the precision the
filing claimed for it. There is no ``LANG``, the Commission filing in English.
A column that is always empty would say none of this; an absent column says
it to anyone who looks at the table.

``FILEDYEAR`` is the partition column, the year of ``FILED``. The year a
filing was made is monotonic and is what a run collects by; the fiscal year is
neither, since a filing states several of them.

``FRAME`` is the Commission's own label for a period it considers comparable
across companies, where it gave the fact one. It is carried as published and
nothing here depends on it.
"""


# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "secfiling", "bronze").path


def path_bronze_companies(config_path):
    """Folder holding one JSON file per registrant, as published."""
    return os.path.join(path_bronze_base(config_path), "companies")


def path_bronze_company(config_path, cik):
    """The company file of one registrant."""
    return os.path.join(path_bronze_companies(config_path), f"CIK{cik}.json")


def path_bronze_collections(config_path):
    """Folder holding the record of what was collected, and when."""
    return os.path.join(path_bronze_base(config_path), "collections")


def path_company_record(config_path, cik):
    """The record of when one company file was collected."""
    return os.path.join(path_bronze_collections(config_path), f"CIK{cik}.json")


# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer, or ``None`` where it is a schema."""
    return resolve(config_path, "secfiling", "silver").path


def ref_silver_filings(config_path):
    """Where the table of filings lives."""
    return resolve(config_path, "secfiling", "silver", "filings")


def ref_silver_facts(config_path):
    """Where the table of facts lives."""
    return resolve(config_path, "secfiling", "silver", "facts")


def path_silver_filings(config_path):
    """Path of the table of filings, or ``None`` where it is addressed by name."""
    return ref_silver_filings(config_path).path


def path_silver_facts(config_path):
    """Path of the table of facts, or ``None`` where it is addressed by name."""
    return ref_silver_facts(config_path).path


def read_json(path):
    """A small JSON file, or ``None`` where it cannot be read.

    A record that cannot be read is treated as one that was never written,
    which costs a download and nothing else.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def write_bytes(path, content):
    """Write a file under a temporary name and put it in place once complete.

    A run that dies while writing leaves the file that was there before,
    rather than half of the new one.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    partial = path + ".partial"
    try:
        with open(partial, "wb") as handle:
            handle.write(content)
        os.replace(partial, path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


def write_json(path, content):
    """Write a small JSON file under a temporary name and put it in place."""
    write_bytes(path, json.dumps(content, indent=1).encode("utf-8"))


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    Directories that already exist are left alone and missing parents are
    created along the way. It is meant for a local data lake; on Databricks
    the volume and the directories in it are provisioned outside the pipeline.
    """
    folders = [path_bronze_base(config_path),
               path_bronze_companies(config_path),
               path_bronze_collections(config_path)]

    silver = resolve(config_path, "secfiling", "silver")
    if not silver.is_named and silver.path is not None:
        folders.append(silver.path)

    for folder in folders:
        os.makedirs(folder, exist_ok=True)


def connect_to_secfiling(spark, config_path, table="facts"):
    """Open a table of the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    table: str
        Name of the table, ``'facts'`` or ``'filings'``.
    """
    if table == "facts":
        return read_silver(spark, ref_silver_facts(config_path))
    if table == "filings":
        return read_silver(spark, ref_silver_filings(config_path))
    raise ValueError(f"Table {table!r} not recognised. Use 'facts' or 'filings'.")


#: What this module offers. The layout of the data lake, and the tables read
#: from it.
__all__ = [
    "BASE_URL",
    "BULK_URL",
    "COMPANY_URL",
    "SOURCE",
    "TICKERS_URL",
    "connect_to_secfiling",
    "read_json",
    "create_directory_tree",
    "facts_schema",
    "filings_schema",
    "path_bronze_base",
    "path_bronze_collections",
    "path_bronze_companies",
    "path_bronze_company",
    "path_company_record",
    "path_silver_base",
    "path_silver_facts",
    "path_silver_filings",
    "ref_silver_facts",
    "ref_silver_filings",
    "write_bytes",
    "write_json",
]
