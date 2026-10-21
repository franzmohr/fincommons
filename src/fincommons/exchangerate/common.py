"""Exchange rates: the configuration and the layout of the data lake.

The paths of the Bronze and the Silver layer are read from the configuration
file, so that the same code runs against a local directory tree and against a
Unity Catalog volume on Databricks. The schema of the proprietary source is
declared here as well, because it is the only source whose shape the module
does not learn from the downloaded file itself.

The Silver table is written as parquet or as Delta. Which of the two is
decided by :func:`fincommons.storage.silver_format`: a table addressed by
catalogue name is Delta, and one addressed by path keeps the format it is
stored in, or takes the one the ``file_format`` argument of the update
functions asks for where it does not exist yet.
"""

import os

from pyspark.sql.types import DoubleType, StringType, StructType

from ..addressing import load_paths, resolve  # noqa: F401

internal_schema = StructType() \
    .add("REFDATE", StringType(), False) \
    .add("BASE", StringType(), False) \
    .add("QUOTE", StringType(), False) \
    .add("VALUE", DoubleType(), False) \
    .add("TYPE", StringType(), False) \
    .add("SOURCE", StringType(), False) 


def path_bronze_base(config_path):
    return resolve(config_path, "exchangerate", "bronze").path


def path_bronze_bis_exchangerate(config_path):
    return os.path.join(path_bronze_base(config_path), "bis")


def path_bronze_ecb_exchangerate(config_path):
    return os.path.join(path_bronze_base(config_path), "ecb")


def path_bronze_internal_exchangerate(config_path):
    return os.path.join(path_bronze_base(config_path), "internal")


def path_silver_exchangerate(config_path):
    """Directory of the Silver layer.

    ``None`` where the layer is a schema in a catalogue rather than a
    directory, which is what setting up the directory tree checks for.
    """
    return resolve(config_path, "exchangerate", "silver").path

def ref_silver_exchangerate_table(config_path):
    """Where the combined exchange rate table lives.

    In a local data lake this is a directory in the Silver layer. Where the
    configuration file gives the layer a catalogue prefix instead, it is a
    table in that schema and the pipeline addresses it by name, which is what
    puts it within reach of Unity Catalog's grants and lineage.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml file.
    """
    return resolve(config_path, "exchangerate", "silver", "exchangerate")


def path_silver_exchangerate_table(config_path):
    """Path of the table that holds the combined exchange rate data.

    Kept for the tests and for a notebook that looks at the files themselves.
    It is ``None`` where the table is addressed by catalogue name, so the
    pipeline goes through :func:`ref_silver_exchangerate_table` instead.
    """
    return ref_silver_exchangerate_table(config_path).path



# The storage format of the Silver layer
# --------------------------------------------------------------------------
#
# How the Silver table is stored is decided by :mod:`fincommons.storage`, which
# the other modules with a Silver table rebuilt in full share. The names are
# imported here so that they remain reachable from this module, where callers
# have found them so far.


from ..storage import (  # noqa: E402,F401
    DEFAULT_SILVER_FORMAT,
    SILVER_FORMATS,
    check_file_format,
    file_format_on_disk,
    read_silver,
    silver_format,
    write_silver,
)


def migrate_folder_names(config_path):
    """Rename the hyphenated folders and files of the lake to snake_case.

    The exchangerate module spells its folders and the files it writes in
    snake_case -- ``ecb_exchangerate.parquet`` rather than
    ``ecb-exchangerate.parquet`` -- since names travel into a SQL catalogue,
    where a hyphen has to be quoted, and it reads them under those names only.
    This renames the folders and files of the module that are spelled with
    hyphens to the snake_case names it reads. Partition directories and
    folders named by a date are left alone, and a name whose snake_case form
    exists as well is raised rather than merged, see
    :func:`fincommons.addressing.rename_folders_to_snake_case`. A lake already
    in snake_case is left as it is.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    list of tuple
        The folders and files renamed, as pairs of old and new path.
    """
    import logging

    from ..addressing import rename_files, rename_folders_to_snake_case
    from ..log import context

    renamed = rename_folders_to_snake_case(
        path_bronze_base(config_path),
    )
    renamed += rename_files([
        (os.path.join(path_bronze_ecb_exchangerate(config_path), "ecb-exchangerate.parquet"), os.path.join(path_bronze_ecb_exchangerate(config_path), "ecb_exchangerate.parquet")),
        (os.path.join(path_bronze_bis_exchangerate(config_path), "bis-exchangerate.parquet"), os.path.join(path_bronze_bis_exchangerate(config_path), "bis_exchangerate.parquet")),
    ])
    logging.getLogger(__name__).info(
        "Renamed the folders and files of the data lake to snake_case",
        extra=context(module="exchangerate", rows=len(renamed)),
    )
    return renamed


def create_directory_tree(config_path):
    """Create an empty data lake direcotry tree.
    
    Creates the basic directory tree which is used for exchange rate data.

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
                   path_bronze_bis_exchangerate(config_path),
                   path_bronze_ecb_exchangerate(config_path),
                   path_bronze_internal_exchangerate(config_path)]
    
    silver = resolve(config_path, "exchangerate", "silver")
    if not silver.is_named and silver.path is not None:
        folder_list.append(silver.path)

    for folder in folder_list:
        os.makedirs(folder, exist_ok=True)
