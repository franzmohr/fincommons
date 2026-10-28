
import os

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
    choose; see :func:`fincommons.storage.silver_format`.
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
#
# How that choice is made is not a question about FIRDS, and the answer is
# :mod:`fincommons.storage`, which the other modules with a Silver table
# rebuilt in full already share. This module had a copy of it, and the copy
# had fallen behind: it wrote Delta without stating the partition overwrite
# mode, which a session configured for dynamic partition overwrite makes
# Delta refuse together with the schema overwrite the rebuild asks for, and
# it left the table without ``optimizeWrite``. The names are imported here so
# that they remain reachable from this module, where callers have found them
# so far.
#
# One rule of the shared module is stricter than the copy's: a table that
# already stands keeps the format it is stored in, and asking for the other
# one raises instead of converting the table. Overwriting a Delta table as
# parquet deletes its transaction log along with the superseded files, which
# is not something to do as a side effect of naming a format. Remove the
# directory to change the format of a table that exists.


from ..storage import (  # noqa: F401
    DEFAULT_SILVER_FORMAT,
    SILVER_FORMATS,
    check_file_format,
    check_local_path,
    file_format_on_disk,
    read_silver,
    silver_format,
    write_silver,
)


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.
    
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
        of :data:`fincommons.storage.SILVER_FORMATS` it was written in, so a
        caller need not repeat the choice made when it was built.

        The cancellations file is not a table of the Silver layer and is not
        reached through here. It is kept in the Bronze layer as its own data
        set, under :func:`path_bronze_auth_102_001_01`, and is read as the
        partitioned parquet it is.
    """
    if table == "main":
        return read_delta(spark, ref_silver_main(config_path))
    if table == "compact":
        return read_silver(spark, ref_silver_compact(config_path))
    raise ValueError(f"Table {table!r} not recognised. Use 'main' or 'compact'.")


#: What this module offers. The layout of the data lake, and the tables read from it. The
#: watermark functions of :mod:`fincommons.firds.metadata` are re-exported
#: below, so a caller reaches them through this module as well.
__all__ = [
    "DEFAULT_SILVER_FORMAT",
    "METADATA_SCHEMA",
    "SILVER_FORMATS",
    "check_file_format",
    "check_local_path",
    "connect_to_firds",
    "create_directory_tree",
    "file_format_on_disk",
    "delta_table",
    "delta_table_exists",
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
    "read_silver",
    "silver_format",
    "update_metadata_maxdate",
    "write_silver",
]
