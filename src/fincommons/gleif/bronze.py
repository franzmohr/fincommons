"""The GLEIF files of the landing zone, as the Bronze layer keeps them.

One parquet table per file, named as the file is -- the publication, the level
and whether it is a full copy or a delta -- and holding the columns of
:data:`fincommons.gleif.common.SOURCE_COLUMNS` under the names GLEIF gives
them, every one as text. A column the file does not carry, as the full copy of
Level 2 does not carry the deletions only a delta reports, is empty rather
than missing, so every file of a level has the same columns.

Spark reads the csv, since the full copy of Level 1 is 4.7 GB unzipped; see
:func:`fincommons.entitymasterdata.bronze.import_registers_with_spark` for why
it is extracted beside the Bronze layer rather than into a temporary directory
and why the reader needs both the quote as escape and ``multiLine``.
"""

from __future__ import annotations

import os
import shutil
import zipfile

from . import common
from .landing import parse_landing_name
import logging

from ..log import context

logger = logging.getLogger(__name__)


def bronze_name(label, kind):
    """The name of the Bronze table of one file of a level."""
    return f"{label}-{kind}.parquet"


def bronze_files(config_path, level):
    """The files of one level the Bronze layer holds, oldest first.

    Returns
    -------
    list of tuple
        ``(label, kind, path)``.
    """
    folder = common.path_bronze(config_path, level)
    if not os.path.isdir(folder):
        return []
    found = []
    for name in os.listdir(folder):
        if not name.endswith(".parquet"):
            continue
        label, _, kind = name[:-len(".parquet")].rpartition("-")
        if kind in (common.FULL, common.DELTA) and label:
            found.append((label, kind, os.path.join(folder, name)))
    return sorted(found)


def import_file(spark, config_path, path):
    """Import one downloaded file into the Bronze layer.

    A file already imported is not imported again, and the table is written
    under another name and renamed once complete, so an import that dies half
    way leaves nothing the Silver layer would read.

    Returns
    -------
    str
        The path of the Bronze table.
    """
    from pyspark.sql import functions as func

    parsed = parse_landing_name(os.path.basename(path))
    if parsed is None:
        raise ValueError(f"{path!r} is not named as a GLEIF file is")
    label, level, kind = parsed
    folder = common.path_bronze(config_path, level)
    os.makedirs(folder, exist_ok=True)
    target = os.path.join(folder, bronze_name(label, kind))
    if os.path.exists(target):
        return target
    unzipped = target + ".unzip"
    partial = target + ".partial"
    try:
        with zipfile.ZipFile(path) as archive:
            members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
            if len(members) != 1:
                raise ValueError(f"{path!r} holds {len(members)} csv files, not one")
            shutil.rmtree(unzipped, ignore_errors=True)
            os.makedirs(unzipped)
            archive.extract(members[0], unzipped)
        raw = (spark.read
               .option("header", "true")
               .option("inferSchema", "false")
               .option("nullValue", "")
               .option("quote", '"')
               .option("escape", '"')
               .option("multiLine", "true")
               .csv(os.path.join(unzipped, members[0])))
        columns = [
            (func.col(f"`{name}`") if name in raw.columns
             else func.lit(None)).cast("string").alias(name)
            for name in common.SOURCE_COLUMNS[level]
        ]
        shutil.rmtree(partial, ignore_errors=True)
        raw.select(*columns).write.mode("overwrite").parquet(partial)
        os.replace(partial, target)
    finally:
        shutil.rmtree(unzipped, ignore_errors=True)
        shutil.rmtree(partial, ignore_errors=True)
    logger.info("Imported", extra=context(module="gleif", layer="bronze",
                                   level=level, file=os.path.basename(target)))
    return target


def read_bronze(spark, path):
    """One Bronze table of a level."""
    return spark.read.parquet(path)


__all__ = ["bronze_files", "bronze_name", "import_file", "read_bronze"]
