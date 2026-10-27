"""Bring the French Data Library up to date, as a scheduled job.

Run with::

    python -m fincommons.frenchdatalibrary --config /path/to/paths.yaml

This is the command a job scheduler runs. It opens a Spark session, collects
every file of the library that has changed into the Bronze layer and rebuilds
the partitions of the Silver table those files belong to. It takes no
argument saying what to collect: what changed is read from the server and
from what the data lake already holds, so the same command is right on the
first day, when it collects the whole library, and on every day after it.

A file that fails is set aside with its traceback and the run carries on by
design, so one broken download does not cost the other files their update.
The exit code still says so: it is 0 only where every file was brought up to
date, and 1 where a file was set aside or the run did not complete.
"""

from __future__ import annotations

import argparse
import sys

from ..log import add_log_level_argument, configure
from .pipeline import update_frenchdatalibrary


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="path to the yaml file the data lake paths are specified in",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "rebuild every partition of the Silver table from the Bronze "
            "layer, whether its file changed or not. A scheduled run never "
            "needs it; it is how the table follows a new version of the parser"
        ),
    )
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        metavar="NAME",
        help=(
            "collect and rebuild only this file of the library, e.g. "
            "F-F_Research_Data_Factors; may be given more than once "
            "(default: every file)"
        ),
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master (default: local[*])",
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def build_session(master):
    """Open a Spark session with the Delta extensions.

    The Silver table may be Delta, so off-cluster the session carries the
    extensions where the ``delta-spark`` package is installed; where it is
    absent the session is opened without them, which writes parquet just as
    well and is enough on a cluster whose runtime provides Delta. The same
    session as the one of :mod:`fincommons.marketpricedata.__main__`.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(master)
        .appName("fincommons-frenchdatalibrary")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        session = builder.getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def main():
    args = parse_arguments()

    # The log goes to standard error. The command writes nothing to standard
    # output: what it did is the log, and whether it did all of it is the exit
    # code.
    configure(args.log_level)

    spark = build_session(args.master)
    try:
        outcome = update_frenchdatalibrary(spark, args.config, full=args.full,
                                           datasets=args.datasets)
    finally:
        spark.stop()

    # A file set aside is not a failure of the run, which is why the library
    # returns rather than raises for it -- but it is a job that did less than
    # it was scheduled to do, and a scheduler only sees the exit code.
    return 1 if outcome["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
