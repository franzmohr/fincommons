
import os

from ..storage import check_local_path
from ..addressing import (
    delta_table,
    delta_table_exists,
    load_paths,
    read_delta,
    read_delta_version,
    resolve,
    sql_table,
    write_delta,
)

# The watermark functions are re-exported here, so that the layout of the data
# lake and the bookkeeping that goes with it are reached through one module.
# :mod:`fincommons.firds.metadata` imports this module in turn, which it can
# do because it only uses it inside its functions, by which time both modules
# are loaded.
from .metadata import *


def ref_metadata(config_path):
    """Where the Delta table holding the watermarks lives.

    In a local data lake this is a directory in the metadata layer. Where the
    configuration file gives the layer a catalogue prefix instead, it is a
    table in that schema and the pipeline addresses it by name; see
    :mod:`fincommons.addressing`.

    The watermark table is always Delta whichever way it is addressed. It is
    updated in place, one row per table of the Silver layer, which is what a
    transaction log is for.
    """
    return resolve(config_path, "firds", "metadata", "watermarks")


def path_metadata(config_path):
    """Return the path of the Delta table holding the watermarks.

    ``None`` where the table is addressed by catalogue name;
    :func:`ref_metadata` is the accessor the pipeline goes through.
    """
    return ref_metadata(config_path).path



def path_landing_base(config_path):
    return resolve(config_path, "firds", "landing").path

def path_landing_error(config_path):
    return os.path.join(path_landing_base(config_path), "error")

def path_landing_finished(config_path):
    return os.path.join(path_landing_base(config_path), "finished")

def path_landing_import(config_path):
    return os.path.join(path_landing_base(config_path), "import")

def path_landing_watermark(config_path):
    """Path of the file recording up to which day the landing zone is complete."""
    return os.path.join(path_landing_base(config_path), "watermark.json")




def path_bronze_base(config_path):
    return resolve(config_path, "firds", "bronze").path

def path_bronze_auth_017_001_02(config_path):
    return os.path.join(path_bronze_base(config_path), "auth_017_001_02")

def path_bronze_auth_036_001_02(config_path):
    """Path of the delta files ESMA published as ``auth.036.001.02``.

    The message definition it revised into ``auth.036.001.03`` on 2022-02-01.
    Each definition keeps a folder of its own, named after it like every other
    folder of the Bronze layer, so that the layer says which message a file was
    published under. The Silver layer reads the two together, a reference date
    at a time; see :func:`fincommons.firds.common.paths_bronze_dltins`.
    """
    return os.path.join(path_bronze_base(config_path), "auth_036_001_02")

def path_bronze_auth_036_001_03(config_path):
    return os.path.join(path_bronze_base(config_path), "auth_036_001_03")

def paths_bronze_dltins(config_path):
    """The Bronze folders holding delta files, oldest definition first.

    A delta file is published under one message definition or another, and the
    history is built from both without caring which: what matters to it is the
    reference date. The folders are returned together so that every part of
    the Silver layer that looks for a date looks in all of them.
    """
    return (
        path_bronze_auth_036_001_02(config_path),
        path_bronze_auth_036_001_03(config_path),
    )

def path_bronze_auth_102_001_01(config_path):
    """Path of the cancellations files in the Bronze layer.

    ESMA publishes a cancellations file every morning next to the delta file.
    It is not an input to the historical database -- section 4.2 of
    ESMA65-8-5014 builds that from the full and the delta files alone, and a
    cancellation reaches it as a ``<CancRcrd>`` record of a delta file. The
    cancellations file is the consolidated list of every record that has ever
    been cancelled, which is a different question, so it is kept in the Bronze
    layer as its own data set rather than folded into the history.
    """
    return os.path.join(path_bronze_base(config_path), "auth_102_001_01")




def path_silver_base(config_path):
    """Directory of the Silver layer.

    ``None`` where the layer is a schema in a catalogue rather than a
    directory, which is what setting up the directory tree checks for.
    """
    return resolve(config_path, "firds", "silver").path


def ref_silver_main(config_path):
    """Where the historical database lives.

    The main table is always Delta whichever way it is addressed: it is added
    to one delivery at a time and its versions are closed in place, which is
    what a transaction log is for. Only the compact table has a format to
    choose; see :func:`compact_format`.
    """
    return resolve(config_path, "firds", "silver", "main")


def ref_silver_compact(config_path):
    """Where the compact table lives."""
    return resolve(config_path, "firds", "silver", "compact")


def path_silver_main(config_path):
    """Path of the historical database.

    ``None`` where the table is addressed by catalogue name;
    :func:`ref_silver_main` is the accessor the pipeline goes through.
    """
    return ref_silver_main(config_path).path


def path_silver_compact(config_path):
    """Path of the compact table.

    ``None`` where the table is addressed by catalogue name;
    :func:`ref_silver_compact` is the accessor the pipeline goes through.
    """
    return ref_silver_compact(config_path).path




# The storage format of the compact table
# --------------------------------------------------------------------------
#
# Only the compact table has a format to choose. The main table is added to a
# delivery at a time and the watermarks are updated in place, which is what a
# transaction log is for; the compact table is rebuilt from the main table in
# full, so nothing is lost by keeping it in a format that carries no log.


COMPACT_FORMATS = ("parquet", "delta")
"""The storage formats the compact table may be written in."""

DEFAULT_COMPACT_FORMAT = "parquet"
"""The format used where a caller does not ask for one.

Parquet is the default because it is the format every engine reads without a
further dependency, which is what the table meant to be handed on to analysis
should be. Delta buys two things parquet does not have and is worth choosing
where they matter: the rebuild replaces the table in one transaction rather
than deleting it first, so a concurrent reader never finds it missing or
half-written, and the previous versions of the table remain readable.
"""


def check_file_format(file_format):
    """Reject a storage format the compact table is not written in.

    Parameters
    ----------
    file_format: str
        One of :data:`COMPACT_FORMATS`.
    """
    if file_format not in COMPACT_FORMATS:
        raise ValueError(
            f"File format {file_format!r} not recognised. Use "
            + " or ".join(repr(f) for f in COMPACT_FORMATS)
            + "."
        )
    return file_format


def compact_format(ref, file_format = None):
    """The format the compact table is written in.

    A table addressed by catalogue name is a Delta table, because that is what
    a Unity Catalog managed table is. There is no choice to make there, and an
    explicit ``file_format`` may only confirm it. A table addressed by path
    takes the format the caller asks for, and :data:`DEFAULT_COMPACT_FORMAT`
    where it asks for none.

    Parameters
    ----------
    ref: fincommons.addressing.TableRef
        Where the table lives.
    file_format: str, optional
        One of :data:`COMPACT_FORMATS`.

    Raises
    ------
    ValueError
        If parquet is asked of a table addressed by catalogue name.
    """
    if ref.is_named:
        if file_format not in (None, "delta"):
            raise ValueError(
                f"A table addressed by catalogue name is a Delta table, so "
                f"{ref.name} cannot be written as {file_format!r}. Address the "
                f"Silver layer of the configuration file by path to write parquet."
            )
        return "delta"
    return check_file_format(file_format or DEFAULT_COMPACT_FORMAT)


def read_compact(spark, ref):
    """Read the compact table in whichever form it is addressed and stored.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    ref: fincommons.addressing.TableRef
        Where the table lives.
    """
    if ref.is_named:
        return spark.read.table(ref.name)
    # The format is read off the table rather than defaulted, because
    # compact_format answers what a write would choose and not what is there.
    return spark.read.format(file_format_on_disk(ref.path)).load(ref.path)


def file_format_on_disk(path, default = DEFAULT_COMPACT_FORMAT):
    """The format the table at ``path`` is stored in.

    A Delta table keeps its transaction log in a ``_delta_log`` directory
    beside the parquet files the table is made of, and that directory is what
    tells the two formats apart. The format is read off the table rather than
    asked of the caller because reading a Delta table as plain parquet does not
    fail: it returns the superseded files of earlier versions along with the
    current ones, and so quietly answers with rows that are no longer in the
    table.

    Where there is no table yet, ``default`` is returned.

    Parameters
    ----------
    path: str
        Path of the table.
    default: str
        The format assumed where the path holds no table.
    """
    check_local_path(path)
    if os.path.isdir(os.path.join(path, "_delta_log")):
        return "delta"
    return default


def write_compact(df, ref, file_format = None):
    """Write a data frame over the compact table.

    Both path formats replace the table rather than adding to it, but they do
    not replace it in the same way: a Delta overwrite is one transaction,
    while a parquet overwrite deletes the table before it writes the new one
    and so leaves a window in which a reader finds it missing or half-written.
    A table addressed by catalogue name is replaced the way a Delta table at a
    path is, and stays the same table throughout, so what is granted on it
    survives the write.

    The format of a table that already exists is not changed in place. Writing
    parquet over a Delta table works, since the delete that precedes it removes
    the transaction log along with everything else, but writing Delta over a
    plain parquet table is refused by Delta itself, because it will not adopt
    files it has no log for. Remove the directory first to go that way.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        The data frame to be written.
    ref: fincommons.addressing.TableRef
        Where the table lives, from :func:`ref_silver_compact`.
    file_format: str, optional
        One of :data:`COMPACT_FORMATS`. Only a table addressed by path has a
        choice; see :func:`compact_format`.

    Returns
    -------
    fincommons.addressing.TableRef
        The table that was written to.
    """
    file_format = compact_format(ref, file_format)

    writer = df.write.format(file_format).mode("overwrite")
    if file_format == "delta":
        # Delta keeps the schema of the table across an overwrite unless it
        # is told otherwise. Parquet needs no such option, because the files
        # it writes are the whole of the table and carry their own schema.
        writer = writer.option("overwriteSchema", "true")

    if ref.is_named:
        writer.saveAsTable(ref.name)
    else:
        writer.save(ref.path)
    return ref



def create_directory_tree(config_path):
    """Create an empty data lake direcotry tree.
    
    Creates the basic directory tree for FIRDS data. Folders that already exist are not overwritten.
    
    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """

    folder_list = [path_landing_base(config_path), \
                   path_landing_error(config_path), \
                   path_landing_finished(config_path), \
                   path_landing_import(config_path), \
                   path_bronze_base(config_path), \
                   path_bronze_auth_017_001_02(config_path), \
                   path_bronze_auth_036_001_02(config_path), \
                   path_bronze_auth_036_001_03(config_path), \
                   path_bronze_auth_102_001_01(config_path)]

    # A layer the configuration file hands to a catalogue has no directory to
    # create. The metadata and Silver layers are the two that may be addressed
    # that way, so they are added only where they are still directories.
    for ref in (resolve(config_path, "firds", "metadata"),
                resolve(config_path, "firds", "silver"),
                ref_silver_main(config_path),
                ref_silver_compact(config_path)):
        if not ref.is_named and ref.path is not None:
            folder_list.append(ref.path)

    for cand_i in folder_list:
        os.makedirs(cand_i, exist_ok=True)
            




def connect_to_firds(spark, config_path, table):
    """Opens FIRDS data of a table in the silver layer.
    
    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    table: str
        Name of the table. Current choices are 'main' and 'compact'.

        The main table is always Delta. The compact table is read in whichever
        of :data:`COMPACT_FORMATS` it was written in, so a caller need not
        repeat the choice made when it was built.
    """
    if table == "main":
        return read_delta(spark, ref_silver_main(config_path))
    elif table == "compact":
        return read_compact(spark, ref_silver_compact(config_path))
    else:
        raise ValueError(f"Table {table!r} not recognised. Use 'main' or 'compact'.")


#: What this module offers. The layout of the data lake, and the tables read from it. The
#: watermark functions of :mod:`fincommons.firds.metadata` are re-exported
#: below, so a caller reaches them through this module as well.
__all__ = [
    "COMPACT_FORMATS",
    "DEFAULT_COMPACT_FORMAT",
    "METADATA_SCHEMA",
    "check_file_format",
    "compact_format",
    "connect_to_firds",
    "create_directory_tree",
    "file_format_on_disk",
    "delta_table",
    "delta_table_exists",
    "read_compact",
    "read_delta",
    "read_delta_version",
    "sql_table",
    "write_delta",
    "ref_metadata",
    "ref_silver_compact",
    "ref_silver_main",
    "get_metadata_maxdate",
    "initialise_metadata",
    "load_paths",
    "metadata_exists",
    "path_bronze_auth_017_001_02",
    "path_bronze_auth_036_001_02",
    "path_bronze_auth_036_001_03",
    "paths_bronze_dltins",
    "path_bronze_auth_102_001_01",
    "path_bronze_base",
    "path_landing_base",
    "path_landing_error",
    "path_landing_finished",
    "path_landing_import",
    "path_landing_watermark",
    "path_metadata",
    "path_silver_base",
    "path_silver_compact",
    "path_silver_main",
    "read_metadata",
    "update_metadata_maxdate",
    "write_compact",
]
