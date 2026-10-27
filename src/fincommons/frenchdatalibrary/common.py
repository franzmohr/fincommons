"""The Kenneth R. French Data Library: the layout of the data lake.

The Bronze and the Silver layer are addressed through
:mod:`fincommons.addressing`, which reads the ``frenchdatalibrary`` section of
the configuration file, so that the same code runs against a local directory
tree, against a Unity Catalog volume and against a schema in a catalogue::

    frenchdatalibrary:
        bronze: /data/frenchdatalibrary/bronze
        silver: /data/frenchdatalibrary/silver

The Bronze layer holds two things. ``files`` is the text of every file of the
library, one row per line and partitioned by ``DATASET``, the name of the
file; ``collections`` holds, per file, a small record of when it was collected
and what the server said about it, which is how the next run knows that a file
has not changed and need not be downloaded again. The Silver layer holds one
table, ``frenchdatalibrary``, with every value of every file in long form,
also partitioned by ``DATASET``, and beside it the marks that say which
collection of each file the table was last built from.

The library is kept apart from :mod:`fincommons.marketpricedata` on purpose.
That table holds about a million rows and is kept by an upsert of the days a
run collected. The library holds about a hundred million values, and Kenneth
French rebuilds every file from the start each month as CRSP is updated, so
a month of the library is not a few new rows but a new edition of the whole
history. The write that fits is to replace a file's partition, not to merge
into it.
"""

import json
import os

from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructType, TimestampType)

from ..addressing import resolve
from ..storage import read_silver

#: The page of the library, which links every file it offers. It is read on
#: every run, rather than a list of the files kept in the package, because the
#: library adds files, and renames them, without notice.
LIBRARY_URL = ("https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"
               "data_library.html")

#: The name the library is given in log records and messages.
SOURCE = "French Data Library"


bronze_schema = StructType() \
    .add("DATASET", StringType(), False) \
    .add("LINENO", IntegerType(), False) \
    .add("LINE", StringType(), False) \
    .add("LASTMODIFIED", TimestampType(), True) \
    .add("COLLECTDT", DateType(), False)
"""The shape of the Bronze layer: one row per line of a file.

``DATASET`` is the partition column and is written as the name of the
partition directory rather than into the files. The schema is given to the
reader rather than inferred, because Spark infers the type of a partition
column from its values, and a reader should not have to wonder whether a
file named ``10_Industry_Portfolios`` was read as a name.
"""


silver_schema = StructType() \
    .add("REFDT", DateType(), False) \
    .add("DATASET", StringType(), False) \
    .add("TABLE", StringType(), False) \
    .add("FREQUENCY", StringType(), False) \
    .add("SERIES", StringType(), False) \
    .add("VALUE", DoubleType(), False) \
    .add("TITLE", StringType(), False)
"""The shape of the Silver table: one row per file, table, series and period.

No column is ever null. A missing value of the library is left out rather
than written as a null, so that a reader who asks for a series finds the
periods it has.
"""


# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "frenchdatalibrary", "bronze").path


def path_bronze_files(config_path):
    """The text of the files, one row per line, partitioned by ``DATASET``."""
    return os.path.join(path_bronze_base(config_path), "files")


def path_bronze_collections(config_path):
    """The record of each file's last collection, one JSON file per file."""
    return os.path.join(path_bronze_base(config_path), "collections")


def path_bronze_dataset(config_path, dataset):
    """The partition of the Bronze layer that holds one file."""
    return os.path.join(path_bronze_files(config_path), f"DATASET={dataset}")


def read_collection(config_path, dataset):
    """The record of the last collection of a file, or None where it has none.

    The record says which address the file was fetched from, the
    ``Last-Modified`` and ``ETag`` the server sent with it, the SHA-256 of
    its text, how many lines it had and when it was collected; see
    :func:`fincommons.frenchdatalibrary.bronze.collect_dataset`. A record
    that cannot be read is treated as one that was never written, which
    costs a download and nothing else.
    """
    record = os.path.join(path_bronze_collections(config_path), f"{dataset}.json")
    try:
        with open(record, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def read_collections(config_path):
    """The records of every file the Bronze layer holds, by name of the file."""
    folder = path_bronze_collections(config_path)
    if not os.path.isdir(folder):
        return {}
    records = {}
    for name in sorted(os.listdir(folder)):
        if name.endswith(".json"):
            record = read_collection(config_path, name[:-len(".json")])
            if record is not None:
                records[record["dataset"]] = record
    return records


def write_json(path, content):
    """Write a small JSON file under a temporary name and put it in place.

    A record that a run dies while writing would otherwise be left truncated,
    and be read back as no record at all.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    partial = path + ".partial"
    with open(partial, "w", encoding="utf-8") as handle:
        json.dump(content, handle, indent=1)
    os.replace(partial, path)


# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer.

    ``None`` where the layer is a schema in a catalogue rather than a
    directory, which is what setting up the directory tree checks for.
    """
    return resolve(config_path, "frenchdatalibrary", "silver").path


def ref_silver_frenchdatalibrary(config_path):
    """Where the table of the library lives.

    In a local data lake this is a directory in the Silver layer. Where the
    configuration file gives the layer a catalogue prefix instead, it is a
    table in that schema and the pipeline addresses it by name, which is what
    puts it within reach of Unity Catalog's grants and lineage.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return resolve(config_path, "frenchdatalibrary", "silver", "frenchdatalibrary")


def path_silver_frenchdatalibrary(config_path):
    """Path of the table of the library.

    Kept for the tests and for a notebook that looks at the files themselves.
    It is ``None`` where the table is addressed by catalogue name, so the
    pipeline goes through :func:`ref_silver_frenchdatalibrary` instead.
    """
    return ref_silver_frenchdatalibrary(config_path).path


def path_silver_watermarks(config_path):
    """Where the table records which collection of each file it was built from.

    A file whose collection has not changed since the table was last built
    from it is not parsed again. Nothing in the table says which collection
    that was, so a mark per file is kept beside the table: the SHA-256 of the
    text it was built from, written after the file's partition has been
    replaced, so that a run that fails leaves the mark where it was and the
    next run builds the file again.

    The marks are files, and a file needs a directory to be written to, which
    a schema in a catalogue is not. A configuration file that addresses the
    Silver layer by catalogue name therefore has to give a ``path`` beside the
    ``table`` for the marks, as for :mod:`fincommons.marketpricedata`, and is
    refused here where it gives none: without the marks every run would parse
    all three hundred files again.
    """
    silver = resolve(config_path, "frenchdatalibrary", "silver")
    if silver.path is None:
        raise ValueError(
            f"the 'silver' layer of 'frenchdatalibrary' in {config_path!r} is "
            "addressed by catalogue name alone, and the marks that say which "
            "collection of each file the table was built from are files. Give "
            "the layer a 'path' beside its 'table' for them to be written to."
        )
    return os.path.join(silver.path, "_watermarks")


def read_watermark(config_path, dataset):
    """The SHA-256 of the collection the table was last built from, or None.

    A mark that cannot be read is treated as one that was never written. The
    cost is that the file is parsed again, which is correct if slow; refusing
    to run over it would not be.
    """
    mark = os.path.join(path_silver_watermarks(config_path), f"{dataset}.json")
    try:
        with open(mark, encoding="utf-8") as handle:
            return json.load(handle).get("sha256")
    except (OSError, ValueError):
        return None


def write_watermark(config_path, dataset, collection):
    """Record that the table now holds a collection of a file.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    dataset: str
        The name of the file.
    collection: dict
        Its record in the Bronze layer, see :func:`read_collection`.
    """
    write_json(
        os.path.join(path_silver_watermarks(config_path), f"{dataset}.json"),
        {"dataset": dataset, "sha256": collection["sha256"],
         "collected": collection["collected"]},
    )


def connect_to_frenchdatalibrary(spark, config_path):
    """Opens the table of the library in the Silver layer.

    One row per file, table, series and period: ``REFDT``, ``DATASET`` (the
    name of the file, ``F-F_Research_Data_Factors``), ``TABLE`` (a code such
    as ``RET_VW``, see :data:`fincommons.frenchdatalibrary.parse.TABLE_CODES`),
    ``FREQUENCY`` (``D``, ``W``, ``M`` or ``A``), ``SERIES`` (the column as
    the file names it), ``VALUE`` as published -- returns in percent -- and
    ``TITLE``, the title of the table in the file. Filter on ``DATASET``
    first: the table is partitioned by it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return read_silver(spark, ref_silver_frenchdatalibrary(config_path))


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    Creates the basic directory tree which is used for the French Data Library.

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
                   path_bronze_files(config_path),
                   path_bronze_collections(config_path)]

    silver = resolve(config_path, "frenchdatalibrary", "silver")
    if not silver.is_named and silver.path is not None:
        folder_list.append(silver.path)

    for folder in folder_list:
        os.makedirs(folder, exist_ok=True)


#: What this module offers. The layout of the data lake, the schemas of its
#: two layers, and the table the result is read from.
__all__ = [
    "LIBRARY_URL",
    "SOURCE",
    "bronze_schema",
    "connect_to_frenchdatalibrary",
    "create_directory_tree",
    "path_bronze_base",
    "path_bronze_collections",
    "path_bronze_dataset",
    "path_bronze_files",
    "path_silver_base",
    "path_silver_frenchdatalibrary",
    "path_silver_watermarks",
    "read_collection",
    "read_collections",
    "read_watermark",
    "ref_silver_frenchdatalibrary",
    "silver_schema",
    "write_json",
    "write_watermark",
]
