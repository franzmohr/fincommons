"""Bring the ESEF filings of some countries up to date, as a scheduled job.

Run with::

    python -m fincommons.esef --config /path/to/paths.yaml --country AT

This is the command a job scheduler runs. It opens a Spark session, brings
the indexes of filings.xbrl.org up to date, collects every report of the
countries asked for that is new or was processed anew, and rebuilds the
table of filings and the partitions of the table of facts those reports
belong to. What changed is read from the repository and from what the data
lake already holds, so the same command is right on the first day, when it
collects every report of the countries, and on every day after it.

A report that fails is set aside with its traceback and the run carries on
by design, so one broken download does not cost the other reports their
update. The exit code still says so: it is 0 only where every report was
brought up to date, and 1 where one was set aside or the run did not
complete.
"""

from __future__ import annotations

import argparse
import sys

from ..log import add_log_level_argument, configure
from .pipeline import update_esef


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="path to the yaml file the data lake paths are specified in",
    )
    parser.add_argument(
        "--country",
        action="append",
        dest="countries",
        required=True,
        metavar="CODE",
        help=(
            "collect the reports of this country, as the repository codes it, "
            "e.g. AT; may be given more than once"
        ),
    )
    parser.add_argument(
        "--year",
        action="append",
        dest="years",
        type=int,
        metavar="YEAR",
        help=(
            "collect only the reports of filings whose period ends in this "
            "year; may be given more than once (default: every year)"
        ),
    )
    parser.add_argument(
        "--packages",
        action="store_true",
        help="collect the report packages, as filed, as well",
    )
    parser.add_argument(
        "--contact",
        help=(
            "an e-mail address or web page the user agent names, as the "
            "repository asks of a client that uses it routinely"
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "rebuild every country of the table of facts from the Bronze "
            "layer, whether its reports changed or not. A scheduled run never "
            "needs it; it is how the table follows a new version of the reader"
        ),
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master (default: local[*])",
    )
    parser.add_argument(
        "--driver-memory",
        default=DRIVER_MEMORY,
        help=(
            "memory of the Spark driver, which in local mode runs the "
            f"executors as well (default: {DRIVER_MEMORY}). Ignored where a "
            "session is already running, as on a cluster"
        ),
    )
    add_log_level_argument(parser)
    return parser.parse_args()


#: The memory the session is opened with. Spark's own default is a gigabyte,
#: and a report is read whole before its facts are handed back; in local mode
#: the driver runs the executors too, so this is the memory the whole run has.
DRIVER_MEMORY = "4g"


def build_session(master, driver_memory=DRIVER_MEMORY):
    """Open a Spark session with the Delta extensions.

    The same session as the one of
    :mod:`fincommons.famafrenchdata.__main__`.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(master)
        .appName("fincommons-esef")
        .config("spark.driver.memory", driver_memory)
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

    spark = build_session(args.master, args.driver_memory)
    try:
        outcome = update_esef(spark, args.config, args.countries,
                              years=args.years, full=args.full,
                              packages=args.packages, contact=args.contact)
    finally:
        spark.stop()

    # A report set aside is not a failure of the run, which is why the
    # library returns rather than raises for it -- but it is a job that did
    # less than it was scheduled to do, and a scheduler only sees the exit
    # code.
    return 1 if outcome["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
