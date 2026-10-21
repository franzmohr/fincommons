"""Bring the market price data up to date, as a scheduled job.

Run with::

    python -m fincommons.marketpricedata --config /path/to/paths.yaml

This is the command a job scheduler runs. It opens a Spark session, collects
every source into the Bronze layer and folds the Bronze layer into the Silver
table, and it takes no argument saying what to collect: each source works out
for itself what it is missing, from what the data lake already holds. The same
command is therefore correct on the first morning, when it collects the whole
history, and on every morning after it, when it collects a day.

A publisher that is unreachable is set aside with its traceback and the run
carries on by design, so one source being down does not cost the others their
update. The exit code still says so: it is 0 only where every source was
updated, and 1 where a source was set aside or the run did not complete. The
log on standard error names the sources that were set aside and ends, where
there were any, with a warning that counts them.
"""

from __future__ import annotations

import argparse
import sys

from ..log import add_log_level_argument, configure
from .bronze_yahoo import YAHOO_FIRST_DATE
from .pipeline import update_marketpricedata


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="path to the yaml file the data lake paths are specified in",
    )
    parser.add_argument(
        "--min-date",
        default=YAHOO_FIRST_DATE,
        help=(
            "earliest reference date to collect, YYYY-MM-DD. It applies only "
            "to a source whose Bronze layer is still empty, so it decides "
            "where the history of a new data lake begins and is ignored "
            f"afterwards (default: {YAHOO_FIRST_DATE})"
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "merge every source into the Silver table from the beginning of "
            "its Bronze layer rather than only what is new. A scheduled run "
            "never needs it; it rebuilds the table from the Bronze layer, "
            "which is worth doing after the identifiers a source writes have "
            "changed"
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

    The Silver table is Delta and is maintained by merge, so off-cluster the
    session has to carry the extensions; that needs the ``delta-spark``
    package, which comes with the ``local`` extra. Where it is absent the
    session is opened without them, because on a cluster whose runtime already
    provides Delta the configuration is enough on its own.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(master)
        .appName("fincommons-marketpricedata")
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
        outcome = update_marketpricedata(spark, args.config, args.min_date,
                                         args.full)
    finally:
        spark.stop()

    # A source set aside is not a failure of the run, which is why the library
    # returns rather than raises for it -- but it is a job that did less than
    # it was scheduled to do, and a scheduler only sees the exit code.
    return 1 if outcome["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
