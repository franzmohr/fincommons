"""The size of the files a partitioned Parquet table is written in.

Most tables of the data lake are plain Parquet, partitioned by a reference
date, and are written one reference date at a time. The frame that reaches the
write has usually been through joins and window functions, each of which
spreads its rows over ``spark.sql.shuffle.partitions`` tasks, and a write
produces one file per task and partition. Written as it comes, a single day
therefore arrives as up to two hundred files holding a fraction of a megabyte
each, and every query of the table has to open all of them: the cost of
reading a partitioned table follows the number of its files more than the
volume of its data.

Two functions address that. :func:`rebalanced` is applied to a frame before it
is written and lets Spark choose the number of files from the volume of the
data. :func:`compact_partitions` rewrites the partitions of any Parquet table
that holds many small files, whatever left them behind.

Delta tables are not handled here. They size their files through
``optimizeWrite`` and are compacted with ``OPTIMIZE``, both of which work
within the transaction log rather than around it.
"""

from __future__ import annotations

import logging
import math
import os
import shutil

from .log import context

logger = logging.getLogger(__name__)

#: The size :func:`compact_partitions` aims at for a file, in bytes. Large
#: enough that the fixed cost of opening a file is small against reading it,
#: small enough that a partition of a few hundred megabytes is still read by
#: several tasks at once.
TARGET_FILE_SIZE = 128 * 1024 * 1024

#: Prefixes of the folders a compaction works in, beside the partition it
#: rewrites. Spark and Arrow both pass over a folder whose name begins with a
#: dot, so a reader never takes either for a partition, even while it exists.
#: An underscore would not do: Spark ignores such a folder only where its name
#: carries no ``=``, and a partition's name always does.
STAGING_PREFIX = ".compact_new_"
RETIRED_PREFIX = ".compact_old_"


def rebalanced(df, *columns):
    """``df`` arranged so that writing it produces files of a sensible size.

    The ``rebalance`` hint adds one shuffle by the given columns before the
    write, and adaptive query execution then merges the small partitions of
    that shuffle and splits the large ones until each is about
    ``spark.sql.adaptive.advisoryPartitionSizeInBytes``. A small reference
    date is written as one file, a large one as a few files of about that
    size, and a frame that spans many reference dates as about one file per
    date instead of one per date and task.

    ``coalesce`` would avoid the shuffle but would also run every step before
    it in as few tasks as it leaves, window functions included, and a fixed
    ``repartition`` gets the size wrong for any day that is unusually large or
    small.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        The frame about to be written.
    *columns: str
        The columns the write is partitioned by. Without them the rows are
        spread evenly, which suits a table that is not partitioned.

    Returns
    -------
    pyspark.sql.DataFrame
        The same rows, to be written as before.
    """
    return df.hint("rebalance", *columns)


def compact_partitions(spark, path, target_file_size=TARGET_FILE_SIZE):
    """Rewrite the partitions of a Parquet table that consist of small files.

    Every folder of the table that holds Parquet files directly -- a partition,
    or the table itself where it is not partitioned -- is rewritten as
    ``ceil(size / target_file_size)`` files if it holds more than that. The
    rows, their values and the partitioning stay as they were; only the number
    of files changes. A folder that already holds few enough files is left
    alone, so the function can be run again at any time and only does work
    where there is some to do.

    A partition is rewritten into a folder beside it, the new files are
    counted against the old ones, and only then is the old folder swapped for
    the new. A partition is therefore never half rewritten. The swap is two
    renames rather than one transaction, so a reader that lists the table at
    that moment may miss the partition; the function is meant to run when the
    pipeline that writes the table does not, and while nothing important reads
    it. A run that dies during a swap is completed by the next run, see
    :func:`_recover`.

    The files of a partition are read with ``mergeSchema``. Without it Spark
    reads a folder with the schema of one of its files, and a column that only
    some of the files carry would be dropped from the rewrite. A partition
    whose files disagree on the type of a column cannot be merged at all; it is
    left as it was, logged with its traceback, and counted in the result, and
    the other partitions are compacted regardless.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    path: str
        The folder of the table, as ``paths.yaml`` names it through the
        module's ``path_*`` functions.
    target_file_size: int
        The size a file is aimed at, in bytes.

    Returns
    -------
    dict
        ``compacted``: the partitions that were rewritten, as paths;
        ``files_before`` and ``files_after``: the number of Parquet files in
        those partitions before and after; ``failed``: the partitions that
        could not be rewritten.

    Raises
    ------
    ValueError
        If ``path`` is not a folder, or is a Delta table, which is compacted
        with ``OPTIMIZE`` instead: rewriting its files behind the transaction
        log would corrupt it.
    """
    if not os.path.isdir(path):
        raise ValueError(f"{path!r} is not a folder, so there is nothing to compact.")
    if os.path.isdir(os.path.join(path, "_delta_log")):
        raise ValueError(
            f"{path!r} is a Delta table. Compact it with OPTIMIZE, which keeps "
            "the transaction log in step with the files it rewrites."
        )
    if target_file_size <= 0:
        raise ValueError("target_file_size must be a positive number of bytes.")

    _recover(path)

    result = {"compacted": [], "files_before": 0, "files_after": 0, "failed": []}
    for folder, files in _data_folders(path):
        size = sum(os.path.getsize(os.path.join(folder, f)) for f in files)
        wanted = max(1, math.ceil(size / target_file_size))
        if len(files) <= wanted:
            continue
        try:
            written = _rewrite(spark, folder, wanted)
        except Exception:
            logger.exception(
                "Could not compact a partition, it was left as it was",
                extra=context(layer="layout", path=folder, files=len(files)),
            )
            result["failed"].append(folder)
            continue
        result["compacted"].append(folder)
        result["files_before"] += len(files)
        result["files_after"] += written
        logger.info(
            "Compacted a partition",
            extra=context(
                layer="layout", path=folder, files=len(files), written=written,
            ),
        )

    logger.info(
        "Compacted the partitions of a table",
        extra=context(
            layer="layout", path=path,
            partitions=len(result["compacted"]),
            files=result["files_before"], written=result["files_after"],
        ),
    )
    if result["failed"]:
        logger.warning(
            "Partitions could not be compacted and keep their files",
            extra=context(layer="layout", path=path, partitions=len(result["failed"])),
        )
    return result


def _parquet_files(folder):
    """The Parquet files directly in ``folder``, hidden ones excepted."""
    return sorted(
        name for name in os.listdir(folder)
        if name.endswith(".parquet")
        and not name.startswith((".", "_"))
        and os.path.isfile(os.path.join(folder, name))
    )


def _data_folders(path):
    """The folders below ``path`` that hold Parquet files, with those files.

    Folders whose name begins with a dot or an underscore are passed over, as
    Spark passes over them, so that neither the work folders of a compaction
    nor metadata such as ``_temporary`` are taken for data.
    """
    for folder, subfolders, _ in os.walk(path):
        subfolders[:] = sorted(
            s for s in subfolders if not s.startswith((".", "_"))
        )
        files = _parquet_files(folder)
        if files:
            yield folder, files


def _rewrite(spark, folder, wanted):
    """Rewrite ``folder`` as ``wanted`` files and swap it in; return the count.

    The folder is read on its own, so its rows come without the partition
    columns, which live in the names of the folders above it, and are written
    back the same way. A folder that also holds partitions below it is not
    rewritten, because reading it would take those in as columns.
    """
    if any(
        "=" in s and os.path.isdir(os.path.join(folder, s)) and not s.startswith((".", "_"))
        for s in os.listdir(folder)
    ):
        raise ValueError(
            f"{folder!r} holds Parquet files beside partitions of its own, "
            "which a rewrite of the folder would fold into it."
        )

    parent, name = os.path.split(folder.rstrip("/" + os.sep))
    staging = os.path.join(parent, STAGING_PREFIX + name)
    retired = os.path.join(parent, RETIRED_PREFIX + name)
    if os.path.exists(staging):
        shutil.rmtree(staging)

    rows = spark.read.option("mergeSchema", "true").parquet(folder)
    expected = rows.count()
    rows.repartition(wanted).write.mode("errorifexists").parquet(staging)

    written = spark.read.option("mergeSchema", "true").parquet(staging).count()
    if written != expected:
        shutil.rmtree(staging)
        raise ValueError(
            f"Rewriting {folder!r} produced {written} rows from {expected}."
        )
    # The marker of a finished write means nothing inside a partition.
    for marker in ("_SUCCESS", "._SUCCESS.crc"):
        if os.path.exists(os.path.join(staging, marker)):
            os.remove(os.path.join(staging, marker))

    os.rename(folder, retired)
    os.rename(staging, folder)
    shutil.rmtree(retired)
    return len(_parquet_files(folder))


def _recover(path):
    """Finish or undo the swaps a compaction left behind when it died.

    The new files are counted before a swap begins, so a staging folder that
    exists beside a retired one is complete and is put in place. A staging
    folder on its own may be incomplete and is removed. A retired folder whose
    partition already stands again is left over from a swap that only lacked
    its clean-up.
    """
    for parent, subfolders, _ in os.walk(path):
        for retired_name in [s for s in subfolders if s.startswith(RETIRED_PREFIX)]:
            name = retired_name[len(RETIRED_PREFIX):]
            folder = os.path.join(parent, name)
            retired = os.path.join(parent, retired_name)
            staging = os.path.join(parent, STAGING_PREFIX + name)
            if os.path.exists(folder):
                shutil.rmtree(retired)
            elif os.path.exists(staging):
                os.rename(staging, folder)
                shutil.rmtree(retired)
            else:
                os.rename(retired, folder)
            logger.warning(
                "Completed a compaction that had been interrupted",
                extra=context(layer="layout", path=folder),
            )
        for staging_name in [s for s in subfolders if s.startswith(STAGING_PREFIX)]:
            staging = os.path.join(parent, staging_name)
            if os.path.exists(staging):
                shutil.rmtree(staging)
        subfolders[:] = [s for s in subfolders if not s.startswith((".", "_"))]
