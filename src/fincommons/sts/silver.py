"""STS: the tables of notifications, assertions, tranches, parties and events.

The Bronze layer holds one complete snapshot of the register per run and the
spreadsheets ESMA publishes beside it. This layer reads one snapshot into five
tables -- the notifications, the assertions each public one makes against the
STS criteria, its tranches, its parties, and the events known about a
securitisation -- and the list of synthetic securitisations into a sixth.

**Everything is built on the driver and rebuilt in full.** The register is
small: the snapshot of 20 November 2026 held 1,147 notifications and 4,613
child documents, which become some forty thousand rows across the six tables.
There is nothing here to distribute and nothing to resume from, a snapshot
being a complete statement of the register on one day rather than an increment
on the one before.

**The tables are of one snapshot, and say which.** Every row carries the day
its snapshot was collected, so a lake that rebuilds from an older snapshot
produces tables that say so rather than tables that look current. The one
table that spans snapshots is the table of events, which is where the history
of a securitisation is kept precisely because no single snapshot holds it.

**A departure is found by comparing two snapshots.** The register answers for
the day it is read and ESMA has published no record of a removal since May
2022, so a notification that was in one snapshot and is not in the next leaves
no trace anywhere else. The build therefore reads the previous snapshot as
well, where the lake holds one, and writes what went as an event.
"""

import logging
import os

from ..layout import rebalanced
from ..log import context, pipeline_run
from ..storage import write_silver
from .common import (SHEET_REFERENCES, SOURCE, assertion_schema,
                     collected_snapshots, event_schema, notification_schema,
                     party_schema, path_bronze_sheet, ref_silver,
                     synthetic_schema, tranche_schema)
from .parse import (child_rows, departures, notification_rows,
                    register_events, sheet_events, synthetic_rows,
                    unmapped_fields)

logger = logging.getLogger(__name__)

#: Which spreadsheet witnesses which event, and the columns it states it in.
#: The frozen list of notifications and the list of cancellations are the only
#: published record of what the register no longer holds.
SHEET_EVENTS = {
    "traditional": ("notified", "Notification date to ESMA", None),
    "cancelled": ("cancelled", "Cancellation date to ESMA",
                  "Reasons for cancellation"),
}


def latest_snapshot(config_path):
    """The most recent snapshot the Bronze layer holds.

    Raises
    ------
    RuntimeError
        Where the Bronze layer holds none, which is what a lake that has not
        collected looks like.
    """
    held = collected_snapshots(config_path)
    if not held:
        raise RuntimeError(
            "The Bronze layer of the sts module holds no snapshot of the "
            "register. Collect one first with update_bronze_sts.")
    return held[-1]


def previous_snapshot(config_path, day):
    """The snapshot before a given one, or ``None`` where it is the first."""
    earlier = [d for d in collected_snapshots(config_path) if d < str(day)]
    return earlier[-1] if earlier else None


def build_snapshot(spark, config_path, day):
    """The four tables one snapshot yields, and the rows they were built from.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str
        The day of the snapshot.

    Returns
    -------
    tuple
        The four data frames -- notifications, assertions, tranches and
        parties -- and the rows of the notifications, which the events are
        built from and a comparison with another snapshot needs.
    """
    notifications, assertions = notification_rows(config_path, day)
    if not notifications:
        raise RuntimeError(
            f"The snapshot of {day} holds no notification. It may have been "
            "collected against a register that was answering nothing.")

    identifiers = {row[0]: row[1] for row in notifications}
    tranches, parties = child_rows(config_path, day, identifiers)

    return (
        spark.createDataFrame(notifications, schema=notification_schema),
        spark.createDataFrame(assertions, schema=assertion_schema),
        spark.createDataFrame(tranches, schema=tranche_schema),
        spark.createDataFrame(parties, schema=party_schema),
        notifications,
    )


def build_events(spark, config_path, day, notifications, compare=True):
    """The table of events, from the register, the spreadsheets and a comparison.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str
        The day of the snapshot the register events are taken from.
    notifications: list
        The rows of the table of notifications of that snapshot.
    compare: bool
        Read the previous snapshot as well, where the lake holds one, and
        record what left the register between the two.

    Returns
    -------
    tuple
        The data frame, and the day it was compared with or ``None``.
    """
    events = list(register_events(notifications, day))

    for name, (event, date_column, reason_column) in SHEET_EVENTS.items():
        path = path_bronze_sheet(config_path, day, name)
        if not os.path.exists(path):
            logger.warning(
                "The spreadsheet is not in the snapshot, so the events it "
                "witnesses are missing from the table",
                extra=context(module="sts", layer="silver", source=SOURCE,
                              table="event", day=str(day),
                              file=f"{name}.xlsx"),
            )
            continue
        events.extend(sheet_events(
            path, event, SHEET_REFERENCES.get(name) or name, day,
            date_column, reason_column))

    compared = previous_snapshot(config_path, day) if compare else None
    if compared is not None:
        before, _ = notification_rows(config_path, compared)
        events.extend(departures(before, notifications, day))

    return spark.createDataFrame(events, schema=event_schema), compared


def build_synthetic(spark, config_path, day):
    """The table of synthetic STS securitisations, from ESMA's list.

    Returns ``None`` where the snapshot does not hold the list, which is not
    an error: the four spreadsheets are independent sources and a run that got
    three has three.
    """
    path = path_bronze_sheet(config_path, day, "synthetic")
    if not os.path.exists(path):
        return None
    return spark.createDataFrame(synthetic_rows(path, day),
                                 schema=synthetic_schema)


def update_silver_sts(spark, config_path, day=None, file_format=None,
                      compare=True):
    """Rebuild the tables of the Silver layer from one snapshot.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str, optional
        The snapshot to build from; the most recent the Bronze layer holds by
        default.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    compare: bool
        Record what left the register since the snapshot before, where the
        lake holds one.
    """
    day = str(day) if day is not None else latest_snapshot(config_path)

    with pipeline_run(
        logger,
        "update_silver_sts",
        module="sts",
        layer="silver",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        outcome["snapshot"] = day

        unknown = unmapped_fields(config_path, day)
        if unknown:
            # A field the register has added. It is in the Bronze layer and
            # in no table, which is a thing to decide about rather than to
            # discover when a table is wrong.
            outcome["fields_unmapped"] = len(unknown)
            logger.warning(
                "The register carries fields this layer does not read, so "
                "they are in the snapshot and in no table",
                extra=context(module="sts", layer="silver", source=SOURCE,
                              day=day,
                              fields=", ".join(sorted(unknown)[:10])),
            )

        notifications, assertions, tranches, parties, rows = build_snapshot(
            spark, config_path, day)

        for table, frame in (("notification", notifications),
                             ("assertion", assertions),
                             ("tranche", tranches),
                             ("party", parties)):
            write_silver(rebalanced(frame), ref_silver(config_path, table),
                         file_format=file_format)
            outcome[table] = frame.count()

        events, compared = build_events(spark, config_path, day, rows, compare)
        write_silver(rebalanced(events), ref_silver(config_path, "event"),
                     file_format=file_format)
        outcome["event"] = events.count()
        outcome["compared_with"] = compared or "none"
        if compared is not None:
            outcome["left_register"] = events.where(
                "event = 'left_register'").count()

        synthetic = build_synthetic(spark, config_path, day)
        if synthetic is None:
            outcome["synthetic"] = 0
            logger.warning(
                "The list of synthetic securitisations is not in the "
                "snapshot, so the table was not built. It is the only public "
                "record of a synthetic STS securitisation",
                extra=context(module="sts", layer="silver", source=SOURCE,
                              day=day, file="synthetic.xlsx"),
            )
        else:
            write_silver(rebalanced(synthetic),
                         ref_silver(config_path, "synthetic"),
                         file_format=file_format)
            outcome["synthetic"] = synthetic.count()


#: What this module offers. Building the tables from a snapshot.
__all__ = [
    "SHEET_EVENTS",
    "build_events",
    "build_snapshot",
    "build_synthetic",
    "latest_snapshot",
    "previous_snapshot",
    "update_silver_sts",
]
