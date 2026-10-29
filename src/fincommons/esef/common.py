"""ESEF filings: the layout of the data lake.

The Bronze and the Silver layer are addressed through
:mod:`fincommons.addressing`, which reads the ``esef`` section of the
configuration file, so that the same code runs against a local directory
tree, against a Unity Catalog volume and against a schema in a catalogue::

    esef:
        bronze: /data/esef/bronze
        silver: /data/esef/silver

The Bronze layer keeps what filings.xbrl.org publishes, as it was published:
``indexes`` holds the index files, one per year of the period end, which list
every filing of the repository; ``reports`` holds the xBRL-JSON of each
filing collected, one file per filing below a directory per country; and
``packages``, where they are asked for, the report packages as filed. Beside
them, ``collections`` holds a small record of when each index and each report
was collected and what the server said about it, which is how the next run
knows what it need not download again.

The Silver layer holds two tables. ``filings`` lists every filing of the
indexes, one row per filing, and is rebuilt in full on every run: it is a
few tens of thousands of rows. ``facts`` holds the facts of the reports
collected, one row per fact, partitioned by ``COUNTRY``, and is rebuilt
country by country, for the countries whose reports changed.
"""

import json
import os

from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructType, TimestampType)

from ..addressing import resolve
from ..storage import read_silver

#: The server of the repository. The addresses an index gives for a filing's
#: files are relative to it.
BASE_URL = "https://filings.xbrl.org"

#: The index file of one year of the period end. The repository asks a client
#: that keeps a copy of the index to read these files rather than every page
#: of its API, and to ask for each with ``If-Modified-Since``.
INDEX_URL = BASE_URL + "/indexes/filings-{year}.json"

#: The first year an index file is asked for. ESEF applies to financial years
#: beginning on or after 1 January 2020, and the repository holds a handful of
#: earlier filings, under 2018; a year without an index file is passed over.
FIRST_YEAR = 2018

#: The name the repository is given in log records and messages.
SOURCE = "filings.xbrl.org"


filings_schema = StructType() \
    .add("FILING", StringType(), False) \
    .add("LEI", StringType(), False) \
    .add("NAME", StringType(), True) \
    .add("COUNTRY", StringType(), False) \
    .add("SYSTEM", StringType(), True) \
    .add("PERIODEND", DateType(), False) \
    .add("LANG", StringType(), True) \
    .add("PUBLISHED", TimestampType(), True) \
    .add("ADDED", TimestampType(), True) \
    .add("PROCESSED", TimestampType(), True) \
    .add("ERRORS", IntegerType(), True) \
    .add("WARNINGS", IntegerType(), True) \
    .add("INCONSISTENCIES", IntegerType(), True) \
    .add("PACKAGEURL", StringType(), True) \
    .add("REPORTURL", StringType(), True) \
    .add("VIEWERURL", StringType(), True) \
    .add("JSONURL", StringType(), True) \
    .add("PACKAGESHA256", StringType(), True)
"""The shape of the table of filings: one row per filing of the indexes.

A filing without ``JSONURL`` is one the repository could not convert to
xBRL-JSON, usually because of a technical error in the filing; it has no
facts. The addresses are those of the repository, relative to
:data:`BASE_URL`.
"""


facts_schema = StructType() \
    .add("COUNTRY", StringType(), False) \
    .add("FILING", StringType(), False) \
    .add("LEI", StringType(), False) \
    .add("PERIODEND", DateType(), False) \
    .add("FACT", StringType(), False) \
    .add("CONCEPT", StringType(), False) \
    .add("PRDSTART", DateType(), True) \
    .add("PRDEND", DateType(), False) \
    .add("UNIT", StringType(), True) \
    .add("DIMENSIONS", StringType(), True) \
    .add("VALUE", DoubleType(), True) \
    .add("TEXT", StringType(), True) \
    .add("DECIMALS", IntegerType(), True) \
    .add("LANG", StringType(), True)
"""The shape of the table of facts: one row per fact of a report.

A numeric fact has ``VALUE`` and ``UNIT``, any other fact ``TEXT``. A fact
of a duration has ``PRDSTART``; one of an instant has none, and ``PRDEND``
is the day it stands for. ``COUNTRY`` is the partition column.
"""


# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "esef", "bronze").path


def path_bronze_indexes(config_path):
    """The index files, one per year of the period end, as published."""
    return os.path.join(path_bronze_base(config_path), "indexes")


def path_bronze_index(config_path, year):
    """The index file of one year."""
    return os.path.join(path_bronze_indexes(config_path), f"filings-{year}.json")


def path_bronze_reports(config_path):
    """The xBRL-JSON of the filings, below a directory per country."""
    return os.path.join(path_bronze_base(config_path), "reports")


def path_bronze_report(config_path, country, filing):
    """The xBRL-JSON of one filing."""
    return os.path.join(path_bronze_reports(config_path), f"COUNTRY={country}",
                        f"{filing}.json")


def path_bronze_packages(config_path):
    """The report packages of the filings, below a directory per country."""
    return os.path.join(path_bronze_base(config_path), "packages")


def path_bronze_collections(config_path):
    """The records of the collections, of the indexes and of the reports."""
    return os.path.join(path_bronze_base(config_path), "collections")


def path_index_record(config_path, year):
    return os.path.join(path_bronze_collections(config_path), "indexes",
                        f"{year}.json")


def path_report_record(config_path, filing):
    return os.path.join(path_bronze_collections(config_path), "reports",
                        f"{filing}.json")


def read_json(path):
    """A small JSON file, or None where it cannot be read.

    A record that cannot be read is treated as one that was never written,
    which costs a download and nothing else.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def write_json(path, content):
    """Write a small JSON file under a temporary name and put it in place.

    A record that a run dies while writing would otherwise be left truncated,
    and be read back as no record at all.
    """
    write_bytes(path, json.dumps(content, indent=1).encode("utf-8"))


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


def read_report_records(config_path):
    """The records of every report the Bronze layer holds, by filing."""
    folder = os.path.join(path_bronze_collections(config_path), "reports")
    if not os.path.isdir(folder):
        return {}
    records = {}
    for name in sorted(os.listdir(folder)):
        if name.endswith(".json"):
            record = read_json(os.path.join(folder, name))
            if record is not None:
                records[record["filing"]] = record
    return records


# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer.

    ``None`` where the layer is a schema in a catalogue rather than a
    directory, which is what setting up the directory tree checks for.
    """
    return resolve(config_path, "esef", "silver").path


def ref_silver_filings(config_path):
    """Where the table of filings lives, as a path or a catalogue name."""
    return resolve(config_path, "esef", "silver", "filings")


def ref_silver_facts(config_path):
    """Where the table of facts lives, as a path or a catalogue name."""
    return resolve(config_path, "esef", "silver", "facts")


def path_silver_watermarks(config_path):
    """Where the table of facts records which reports each country was built from.

    A country whose reports have not changed since its partition was last
    built is not parsed again. The marks are files, and a file needs a
    directory to be written to, which a schema in a catalogue is not; a
    configuration file that addresses the Silver layer by catalogue name
    therefore has to give a ``path`` beside the ``table`` for them, as for
    :mod:`fincommons.famafrenchdata`.
    """
    silver = resolve(config_path, "esef", "silver")
    if silver.path is None:
        raise ValueError(
            f"the 'silver' layer of 'esef' in {config_path!r} is addressed by "
            "catalogue name alone, and the marks that say which reports each "
            "country was built from are files. Give the layer a 'path' beside "
            "its 'table' for them to be written to."
        )
    return os.path.join(silver.path, "_watermarks")


def read_watermark(config_path, country):
    """The signature of the reports a country was last built from, or None."""
    mark = read_json(os.path.join(path_silver_watermarks(config_path),
                                  f"{country}.json"))
    return None if mark is None else mark.get("signature")


def write_watermark(config_path, country, signature, reports):
    """Record that the table now holds the facts of a country's reports."""
    write_json(
        os.path.join(path_silver_watermarks(config_path), f"{country}.json"),
        {"country": country, "signature": signature, "reports": reports},
    )


def connect_to_esef(spark, config_path):
    """Opens the table of facts in the Silver layer.

    One row per fact of a report: the filing (``FILING``, ``LEI``,
    ``COUNTRY``, ``PERIODEND``), the concept (``ifrs-full:Revenue``), the
    period (``PRDSTART``, ``PRDEND``), ``UNIT``, ``DIMENSIONS`` and the
    value, ``VALUE`` for a number and ``TEXT`` otherwise. Filter on
    ``COUNTRY`` first: the table is partitioned by it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return read_silver(spark, ref_silver_facts(config_path))


def connect_to_esef_filings(spark, config_path):
    """Opens the table of filings in the Silver layer.

    One row per filing the indexes of the repository list, whether or not its
    report was collected: who filed it (``LEI``, ``NAME``), where
    (``COUNTRY``), for which period (``PERIODEND``), when it was processed,
    how its validation went and where its files are.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return read_silver(spark, ref_silver_filings(config_path))


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    Creates the basic directory tree which is used for the ESEF filings.

    Directories that already exist are left alone and missing parents are
    created along the way, so that the call can be repeated and so that a data
    lake configured below a directory that does not exist yet is set up in one
    go. It is meant for a local data lake; on Databricks the volume and the
    directories in it are provisioned outside the pipeline.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    folder_list = [path_bronze_base(config_path),
                   path_bronze_indexes(config_path),
                   path_bronze_reports(config_path),
                   path_bronze_collections(config_path)]

    silver = resolve(config_path, "esef", "silver")
    if not silver.is_named and silver.path is not None:
        folder_list.append(silver.path)

    for folder in folder_list:
        os.makedirs(folder, exist_ok=True)


#: What this module offers. The layout of the data lake, the schemas of the
#: two tables, and the tables the result is read from.
__all__ = [
    "BASE_URL",
    "FIRST_YEAR",
    "INDEX_URL",
    "SOURCE",
    "connect_to_esef",
    "connect_to_esef_filings",
    "create_directory_tree",
    "facts_schema",
    "filings_schema",
    "path_bronze_base",
    "path_bronze_collections",
    "path_bronze_index",
    "path_bronze_indexes",
    "path_bronze_packages",
    "path_bronze_report",
    "path_bronze_reports",
    "path_index_record",
    "path_report_record",
    "path_silver_base",
    "path_silver_watermarks",
    "read_json",
    "read_report_records",
    "read_watermark",
    "ref_silver_facts",
    "ref_silver_filings",
    "write_bytes",
    "write_json",
    "write_watermark",
]
