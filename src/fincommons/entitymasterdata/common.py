"""Entity master data: the layout of the data lake.

The paths of the landing zone, the Bronze layer and the Silver layer are read
from the configuration file, so that the same code runs against a local
directory tree and against a Unity Catalog volume on Databricks. The module
draws on four publishers, each of which gets a directory of its own in both the
landing zone and the Bronze layer, so that a file is always traceable to the
register it came from.

The Silver tables are written as parquet or as Delta. Which of the two is
decided by :func:`fincommons.storage.silver_format`: a table addressed by
catalogue name is Delta, and one addressed by path keeps the format it is
stored in, or takes the one the ``file_format`` argument of the update
functions asks for where it does not exist yet.
"""

import os

from ..addressing import resolve




# The landing zone
# --------------------------------------------------------------------------


def path_landing_base(config_path):
    return resolve(config_path, "entitymasterdata", "landing").path



def path_landing_ecb(config_path):
    return os.path.join(path_landing_base(config_path), "ecb")

def path_landing_ecb_investment_fund(config_path):
    return os.path.join(path_landing_ecb(config_path), "investment_fund")

def path_landing_ecb_mfi(config_path):
    return os.path.join(path_landing_ecb(config_path), "mfi")

def path_landing_ecb_fvc(config_path):
    return os.path.join(path_landing_ecb(config_path), "fvc")

def path_landing_ecb_psri(config_path):
    return os.path.join(path_landing_ecb(config_path), "psri")



def path_landing_eiopa(config_path):
    return os.path.join(path_landing_base(config_path), "eiopa")

def path_landing_eiopa_insurance_undertaking(config_path):
    return os.path.join(path_landing_eiopa(config_path), "insurance_undertaking")

def path_landing_eiopa_iorp(config_path):
    return os.path.join(path_landing_eiopa(config_path), "iorp")



def path_landing_esma(config_path):
    return os.path.join(path_landing_base(config_path), "esma")

def path_landing_esma_ccp(config_path):
    return os.path.join(path_landing_esma(config_path), "ccp")



def path_landing_gleif(config_path):
    return os.path.join(path_landing_base(config_path), "gleif")

def path_landing_gleif_level_1(config_path):
    return os.path.join(path_landing_gleif(config_path), "level_1")

def path_landing_gleif_level_2(config_path):
    return os.path.join(path_landing_gleif(config_path), "level_2")




# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "entitymasterdata", "bronze").path



def path_bronze_ecb(config_path):
    return os.path.join(path_bronze_base(config_path), "ecb")

def path_bronze_ecb_investment_fund(config_path):
    return os.path.join(path_bronze_ecb(config_path), "investment_fund")

def path_bronze_ecb_mfi(config_path):
    return os.path.join(path_bronze_ecb(config_path), "mfi")

def path_bronze_ecb_fvc(config_path):
    return os.path.join(path_bronze_ecb(config_path), "fvc")

def path_bronze_ecb_psri(config_path):
    return os.path.join(path_bronze_ecb(config_path), "psri")



def path_bronze_eiopa(config_path):
    return os.path.join(path_bronze_base(config_path), "eiopa")

def path_bronze_eiopa_insurance_undertaking(config_path):
    return os.path.join(path_bronze_eiopa(config_path), "insurance_undertaking")

def path_bronze_eiopa_iorp(config_path):
    return os.path.join(path_bronze_eiopa(config_path), "iorp")



def path_bronze_esma(config_path):
    return os.path.join(path_bronze_base(config_path), "esma")

def path_bronze_esma_ccp(config_path):
    return os.path.join(path_bronze_esma(config_path), "ccp")

def path_bronze_gleif(config_path):
    return os.path.join(path_bronze_base(config_path), "gleif")

def path_bronze_gleif_level_1(config_path):
    return os.path.join(path_bronze_gleif(config_path), "level_1")

def path_bronze_gleif_level_2(config_path):
    return os.path.join(path_bronze_gleif(config_path), "level_2")

def path_bronze_internal_entitymasterdata(config_path):
    """Folder the proprietary entity master data is picked up from.

    Nothing writes into it. Like the proprietary source of the exchange rate
    module, the file is placed there by whoever maintains the internal list,
    because the shape of such a list differs from one organisation to the next.
    """
    return os.path.join(path_bronze_base(config_path), "internal")




# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer.

    ``None`` where the layer is a schema in a catalogue rather than a
    directory, which is what setting up the directory tree checks for.
    """
    return resolve(config_path, "entitymasterdata", "silver").path


def ref_silver_public(config_path):
    """Where the table holding the public entity master data lives.

    In a local data lake this is a directory in the Silver layer. Where the
    configuration file gives the layer a catalogue prefix instead, it is a
    table in that schema and the pipeline addresses it by name, which is what
    puts it within reach of Unity Catalog's grants and lineage.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return resolve(config_path, "entitymasterdata", "silver", "public")


def ref_silver_main(config_path):
    """Where the table holding the entity master data set itself lives.

    Addressed the way :func:`ref_silver_public` is.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return resolve(config_path, "entitymasterdata", "silver", "main")


def path_silver_public(config_path):
    """Path of the table holding the public entity master data.

    Kept for the tests and for a notebook that looks at the files themselves.
    It is ``None`` where the table is addressed by catalogue name, so the
    pipeline goes through :func:`ref_silver_public` instead.
    """
    return ref_silver_public(config_path).path


def path_silver_main(config_path):
    """Path of the table holding the entity master data set itself.

    ``None`` where the table is addressed by catalogue name;
    :func:`ref_silver_main` is the accessor the pipeline goes through.
    """
    return ref_silver_main(config_path).path




# The storage format of the Silver layer
# --------------------------------------------------------------------------
#
# How the Silver tables are stored is decided by :mod:`fincommons.storage`,
# which the other modules with a Silver table rebuilt in full share. The names
# are imported here so that they are reachable from this module as well. The
# helpers take a :class:`fincommons.addressing.TableRef` or a path.


from ..storage import (  # noqa: E402
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

    The entitymasterdata module spells its folders and the files it writes in
    snake_case -- ``level_1`` rather than ``level-1`` -- since names travel
    into a SQL catalogue, where a hyphen has to be quoted, and it reads them
    under those names only. This renames the folders and files of the module
    that are spelled with hyphens to the snake_case names it reads. Partition
    directories and folders named by a date are left alone, and a name whose
    snake_case form exists as well is raised rather than merged, see
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

    # A Silver layer in the catalogue has no folders, and its tables are
    # named in snake_case.
    renamed = rename_folders_to_snake_case(*[
        root for root in (
            path_landing_base(config_path),
            path_bronze_base(config_path),
            path_silver_base(config_path),
        )
        if root is not None
    ])
    renamed += rename_files([
        (os.path.join(path_landing_esma_ccp(config_path), "ccp-list.csv"), os.path.join(path_landing_esma_ccp(config_path), "ccp_list.csv")),
        (os.path.join(path_bronze_esma_ccp(config_path), "ccp-list.parquet"), os.path.join(path_bronze_esma_ccp(config_path), "ccp_list.parquet")),
        (os.path.join(path_bronze_internal_entitymasterdata(config_path), "entitymasterdata-internal.csv"), os.path.join(path_bronze_internal_entitymasterdata(config_path), "entitymasterdata_internal.csv")),
    ])
    logging.getLogger(__name__).info(
        "Renamed the folders and files of the data lake to snake_case",
        extra=context(module="entitymasterdata", rows=len(renamed)),
    )
    return renamed


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    Creates the basic directory tree which is used for entity master data.

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

    folder_list = [path_landing_base(config_path),
                   path_landing_ecb(config_path),
                   path_landing_ecb_investment_fund(config_path),
                   path_landing_ecb_mfi(config_path),
                   path_landing_ecb_fvc(config_path),
                   path_landing_ecb_psri(config_path),
                   path_landing_eiopa(config_path),
                   path_landing_eiopa_insurance_undertaking(config_path),
                   path_landing_eiopa_iorp(config_path),
                   path_landing_esma(config_path),
                   path_landing_esma_ccp(config_path),
                   path_landing_gleif(config_path),
                   path_landing_gleif_level_1(config_path),
                   path_landing_gleif_level_2(config_path),
                   path_bronze_base(config_path),
                   path_bronze_ecb(config_path),
                   path_bronze_ecb_investment_fund(config_path),
                   path_bronze_ecb_mfi(config_path),
                   path_bronze_ecb_fvc(config_path),
                   path_bronze_ecb_psri(config_path),
                   path_bronze_eiopa(config_path),
                   path_bronze_eiopa_insurance_undertaking(config_path),
                   path_bronze_eiopa_iorp(config_path),
                   path_bronze_esma(config_path),
                   path_bronze_esma_ccp(config_path),
                   path_bronze_gleif(config_path),
                   path_bronze_gleif_level_1(config_path),
                   path_bronze_gleif_level_2(config_path),
                   path_bronze_internal_entitymasterdata(config_path)]

    # A Silver layer in the catalogue is a schema, which is provisioned where
    # the catalogue is rather than created here.
    silver = resolve(config_path, "entitymasterdata", "silver")
    if not silver.is_named and silver.path is not None:
        folder_list.append(silver.path)

    for folder in folder_list:
        os.makedirs(folder, exist_ok=True)


def connect_to_entitymasterdata(spark, config_path, table = "main"):
    """Opens entity master data of a table in the Silver layer.

    The table is read in whichever form it is addressed and stored in, so a
    caller need not repeat the choice made when it was built. A table that has
    not been built yet is reported as such; see
    :func:`fincommons.storage.read_silver`.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    table: str
        Name of the table. Current choices are 'main', the entity master data set
        itself, and 'public', the part of it that rests on public sources alone.
    """
    if table == "main":
        return read_silver(spark, ref_silver_main(config_path))
    elif table == "public":
        return read_silver(spark, ref_silver_public(config_path))
    else:
        raise ValueError(f"Table {table!r} not recognised. Use 'main' or 'public'.")


#: What this module offers. The layout of the data lake, the storage format of
#: the Silver layer, and the table the result is read from.
__all__ = [
    "DEFAULT_SILVER_FORMAT",
    "SILVER_FORMATS",
    "check_file_format",
    "connect_to_entitymasterdata",
    "create_directory_tree",
    "file_format_on_disk",
    "migrate_folder_names",
    "path_bronze_base",
    "path_bronze_ecb",
    "path_bronze_ecb_investment_fund",
    "path_bronze_ecb_mfi",
    "path_bronze_ecb_fvc",
    "path_bronze_ecb_psri",
    "path_bronze_eiopa",
    "path_bronze_eiopa_insurance_undertaking",
    "path_bronze_eiopa_iorp",
    "path_bronze_esma",
    "path_bronze_esma_ccp",
    "path_bronze_gleif",
    "path_bronze_gleif_level_1",
    "path_bronze_gleif_level_2",
    "path_bronze_internal_entitymasterdata",
    "path_landing_base",
    "path_landing_ecb",
    "path_landing_ecb_investment_fund",
    "path_landing_ecb_mfi",
    "path_landing_ecb_fvc",
    "path_landing_ecb_psri",
    "path_landing_eiopa",
    "path_landing_eiopa_insurance_undertaking",
    "path_landing_eiopa_iorp",
    "path_landing_esma",
    "path_landing_esma_ccp",
    "path_landing_gleif",
    "path_landing_gleif_level_1",
    "path_landing_gleif_level_2",
    "path_silver_base",
    "path_silver_main",
    "path_silver_public",
    "read_silver",
    "ref_silver_main",
    "ref_silver_public",
    "silver_format",
    "write_silver",
]
