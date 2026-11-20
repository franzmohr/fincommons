# SPDX-FileCopyrightText: 2026 Franz Mohr
#
# SPDX-License-Identifier: MIT
"""Rebuild the Silver layer of a lake written before the snake_case rename.

The rename of 0.5.0 changed the names of the columns the Silver tables are
written with, so a lake written before it cannot be read by the code after
it. Every table has to be written again from Bronze, which is untouched by
the rename and is the source for all of this.

Run one stage at a time. The workstation this was written on has under 6 GB
of RAM, so the session is single-threaded with a small driver heap; a machine
with more can raise both with --master and --driver-memory.

    python tools/rebuild_silver.py exchangerate
    python tools/rebuild_silver.py esef --clean
    python tools/rebuild_silver.py esef --countries GR,HR,HU
    python tools/rebuild_silver.py sec
    python tools/rebuild_silver.py combined

The order matters only for `combined`, which reads the tables the other
stages write.

Why `esef` needs --clean the first time
---------------------------------------
The facts table is partitioned by country and rebuilt a partition at a time,
and the partition column was renamed with the rest. So the rebuild writes
`country=AT` while the lake already holds `COUNTRY=AT`, and
`replace_silver_partitions` replaces partitions of the column it is given and
knows nothing of the other one. The old directories are left where they are:
the table ends up holding every rebuilt country twice, under two schemas, and
arrow resolves the whole dataset to whichever it reads first. A reader then
gets the pre-rename names back from a table that has just been rebuilt.

--clean removes those pre-rename directories and nothing else. The rebuild
reads Bronze, which the rename did not touch, so they are not a source for
anything: for a country already rebuilt they are a duplicate, and for one
still to come they are a copy no code after the rename can read. The
`filings` table is written whole rather than by partition and needs none of
this. --countries is for resuming an interrupted rebuild and does not clean.
"""
import argparse
import os
import shutil
import sys
import time

CONFIG = os.environ.get(
    "FINCOMMONS_CONFIG", os.path.expanduser("~/data/fincommons/paths.yaml"))


def session(master, driver_memory, local_dir):
    """A Spark session with the settings a small machine needs.

    `spark.local.dir` is set away from /tmp, which is a tmpfs on the
    workstation: the default scratch would spill a large shuffle into memory
    and end the run with "No space left on device" while the disk is empty.
    SPARK_LOCAL_DIRS in the environment is read before this and wins, so it
    is set here as well.
    """
    os.environ.setdefault("SPARK_LOCAL_DIRS", local_dir)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.makedirs(local_dir, exist_ok=True)
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .master(master)
            .appName("fincommons-rebuild")
            .config("spark.driver.memory", driver_memory)
            .config("spark.local.dir", local_dir)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def silver_path(table):
    """Where a table of a module sits, from the configuration file."""
    from fincommons.addressing import load_paths
    config = load_paths(CONFIG)
    module, _, name = table.partition("/")
    return os.path.join(config[module]["silver"], name)


def orphaned_upper_case(table):
    """The pre-rename partition directories of a table, if any are left."""
    root = silver_path(table)
    if not os.path.isdir(root):
        return []
    return sorted(d for d in os.listdir(root)
                  if d.split("=")[0].isupper() and "=" in d)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("stage", choices=("exchangerate", "esef", "sec",
                                          "combined"))
    parser.add_argument("--countries", help="comma-separated, esef only; "
                        "resumes an interrupted rebuild")
    parser.add_argument("--clean", action="store_true",
                        help="esef only: remove the pre-rename COUNTRY= "
                             "partitions of the facts table before writing "
                             "it, which the first rebuild of a pre-rename "
                             "lake needs")
    parser.add_argument("--master", default="local[1]")
    parser.add_argument("--driver-memory", default="1500m")
    parser.add_argument("--local-dir",
                        default=os.path.expanduser("~/data/fincommons/spark-tmp"))
    args = parser.parse_args()

    if args.stage == "esef":
        orphans = orphaned_upper_case("esef/facts")
        if orphans and not (args.clean or args.countries):
            parser.error(
                "The facts table still holds %d pre-rename partitions (%s, "
                "...). Rebuilding over them leaves the table holding each "
                "country twice under two schemas. Pass --clean to remove "
                "them first, or --countries to resume a rebuild that has "
                "already been cleaned." % (len(orphans), ", ".join(orphans[:3])))
        if args.clean:
            root = silver_path("esef/facts")
            for name in orphans:
                print("removing pre-rename partition", name, flush=True)
                shutil.rmtree(os.path.join(root, name))
            if not orphans:
                print("no pre-rename partitions to remove", flush=True)

    spark = session(args.master, args.driver_memory, args.local_dir)
    started = time.time()
    try:
        if args.stage == "exchangerate":
            from fincommons.exchangerate.silver import update_silver_exchangerate
            # No rebase: the table is based on the euro already and asking
            # for one raises.
            out = update_silver_exchangerate(spark, CONFIG)
        elif args.stage == "esef":
            from fincommons.financialreport.esef.silver import update_silver_esef
            countries = args.countries.split(",") if args.countries else None
            out = update_silver_esef(spark, CONFIG, full=True,
                                     countries=countries)
        elif args.stage == "sec":
            from fincommons.financialreport.sec.silver import update_silver_sec
            out = update_silver_sec(spark, CONFIG)
        else:
            from fincommons.financialreport.combined.silver import (
                update_silver_financialreport)
            out = update_silver_financialreport(spark, CONFIG)
    finally:
        spark.stop()
    print("result:", out, flush=True)
    print("elapsed: %.0f s" % (time.time() - started), flush=True)


if __name__ == "__main__":
    main()
