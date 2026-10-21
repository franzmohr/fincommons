"""FIRDS: the historical database in the Silver layer.

The Silver layer holds the historical database described in section 4.2 of
ESMA's *Instructions on download and use of full, delta and cancellations
reference data files* (ESMA65-8-5014 rev 3). The algorithm itself lives in
:mod:`fincommons.firds.history`, which expresses it as transformations of data
frames; this module applies it to the Delta table that stores the history and
keeps track of which delta files have already been processed.
"""

from datetime import datetime, timedelta
import logging
import os

import pyspark.sql.functions as func
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import functools

from . import common
from . import history
from .landing import clear_late_day, read_landing_state, record_applied_through
from .utilities import (PARTITION_PREFIX, missing_parts, parse_file_name,
                        unimported_listed_files)
from ..log import context, pipeline_run

logger = logging.getLogger(__name__)


#: The format reference dates are spelled in, both in the names of the Bronze
#: partitions and in the metadata that records how far the history has been
#: built.
REFERENCE_DATE_FORMAT = "%Y-%m-%d"

#: Prefix of the ``userMetadata`` the commits of a daily update carry. The tag
#: of a commit is the prefix, the reference date and the step, e.g.
#: ``firds-silver-main:2026-09-06:append``.
COMMIT_TAG_PREFIX = "firds-silver-main"

#: The session setting Delta takes the ``userMetadata`` of a MERGE from.
USER_METADATA_CONF = "spark.databricks.delta.commitInfo.userMetadata"

#: The operations after which the history of the table starts afresh.
REWRITING_OPERATIONS = ("CREATE TABLE", "REPLACE TABLE", "CREATE OR REPLACE TABLE")


def parse_reference_date(datadt):
    """Return the reference date spelled by ``datadt`` as a date."""
    return datetime.strptime(datadt, REFERENCE_DATE_FORMAT).date()


def format_reference_date(date):
    """Return the spelling of ``date`` used by partitions and metadata."""
    return date.strftime(REFERENCE_DATE_FORMAT)


def list_reference_dates(path):
    """Return the reference dates of the partitions of ``path``, in order.

    The partitions are directories named ``DATADT=<date>``. Their dates are
    parsed before they are sorted, so that the chronological order in which the
    files have to be applied does not rest on the dates being spelled in a way
    that happens to sort as text.
    """
    if not os.path.isdir(path):
        # A data lake need not hold both delta message definitions: one whose
        # history begins after 2022-02-01 never sees auth.036.001.02 at all.
        return []
    dates = [
        parse_reference_date(name[len(PARTITION_PREFIX):])
        for name in os.listdir(path)
        if name.startswith(PARTITION_PREFIX)
    ]
    dates.sort()
    return dates


def dltins_reference_dates(config_path):
    """The reference dates the Bronze layer holds a delta partition for.

    ESMA published the delta file under two message definitions and each has a
    folder of its own, so a date may sit in either. The history cares only
    about the date, so the folders are read as one.
    """
    dates = set()
    for path in common.paths_bronze_dltins(config_path):
        dates.update(list_reference_dates(path))
    return sorted(dates)


def dltins_partitions(config_path, datadt):
    """The delta folders holding a partition for ``datadt``.

    Normally one: a day's delta file is published under one definition. Both
    are returned where both hold the date, so that a day split across the
    change of definition is read whole rather than half.
    """
    return [
        path
        for path in common.paths_bronze_dltins(config_path)
        if os.path.isdir(os.path.join(path, PARTITION_PREFIX + datadt))
    ]


def read_dltins_partition(spark, config_path, datadt, records=()):
    """Read the delta partition of one reference date, whichever folder holds it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    datadt: str
        The reference date.
    records: tuple of str
        The record types to read, as :func:`read_bronze_partition` takes them.
    """
    frames = [
        read_bronze_partition(spark, path, datadt, records=records)
        for path in dltins_partitions(config_path, datadt)
    ]
    if not frames:
        raise RuntimeError(
            f"The Bronze layer holds no delta partition for {datadt} under "
            "either message definition."
        )
    # Both folders are read through the paths of SOURCE_FIELDS and cast to the
    # types named there, so the frames agree whichever definition they came
    # from and can simply be put together.
    return functools.reduce(DataFrame.unionByName, frames)


def commit_tag(datadt, step):
    """The ``userMetadata`` of the commit ``step`` of the update for ``datadt``."""
    return f"{COMMIT_TAG_PREFIX}:{datadt}:{step}"


def tagged_commits(spark, ref):
    """The commits of the table since it was last written from scratch.

    Returned newest first, as pairs of version and tag, where the tag is the
    ``userMetadata`` of the commit and is ``None`` for a commit that carries
    none. The walk stops after the commit that wrote the table from scratch,
    so that a history built again does not inherit the tags of the one it
    replaced.
    """
    commits = (
        common.delta_table(spark, ref).history()
        .select(
            "version",
            "operation",
            func.col("operationParameters").getItem("mode").alias("mode"),
            "userMetadata",
        )
        .collect()
    )
    walked = []
    for commit in sorted(commits, key=lambda c: c["version"], reverse=True):
        walked.append((commit["version"], commit["userMetadata"]))
        rewritten = (
            commit["operation"].startswith(REWRITING_OPERATIONS)
            or (commit["operation"] == "WRITE" and commit["mode"] == "Overwrite")
        )
        if rewritten:
            break
    return walked


def watermark_from_commits(spark, ref):
    """Return the reference date the table has been built up to, from its log.

    The watermark is kept in a table of its own, and a run that dies between
    writing the history and writing the watermark leaves the second behind.
    The commits of the history name the reference date they applied, so the
    watermark can be recovered from them rather than the database having to be
    built again.

    A date whose append was committed has been applied in full. A date of
    which only the merge was committed has not, so the watermark is the day
    before it and the date is applied again, which
    :func:`upsert_from_bronze_daily` resumes rather than starts afresh.

    Returns ``None`` where no commit names a reference date, as for a database
    that was not built by this code.
    """
    for _, tag in tagged_commits(spark, ref):
        if not tag or not tag.startswith(COMMIT_TAG_PREFIX + ":"):
            continue
        _, datadt, step = tag.split(":", 2)
        if step == "merge":
            return format_reference_date(
                parse_reference_date(datadt) - timedelta(days=1)
            )
        return datadt
    return None


def committed_steps(spark, ref, datadt):
    """Return the versions at which the steps of ``datadt`` were committed.

    The update of one reference date is two commits, a merge and an append,
    followed by the watermark in a table of its own. A run that dies between
    them leaves the watermark behind, and the next run starts the same date
    again. What it must then do depends on what was committed, which the tags
    of the commits tell. Only the commits since the table was last written
    from scratch count, so a history built again does not inherit the tags of
    the one it replaced.

    Returns
    -------
    dict
        Maps ``'merge'`` and ``'append'`` to the version they were committed
        at, for each of them that was, and ``'current'`` to the version the
        table is at.
    """
    wanted = {commit_tag(datadt, step): step for step in ("merge", "append")}
    commits = tagged_commits(spark, ref)
    found = {"current": commits[0][0]}
    for version, tag in commits:
        step = wanted.get(tag)
        if step is not None:
            found.setdefault(step, version)
    return found


def finished_files(config_path):
    """The names of the files the Bronze layer has been built from."""
    path = common.path_landing_finished(config_path)
    return os.listdir(path) if os.path.isdir(path) else []


def quarantined_files(config_path):
    """The names of the files that were set aside instead of being imported."""
    path = common.path_landing_error(config_path)
    return os.listdir(path) if os.path.isdir(path) else []


def reference_dates_to_apply(config_path, after):
    """The reference dates that have to be applied after ``after``, in order.

    The dates are taken from the landing zone as well as from the Bronze
    layer. A day whose delta file was quarantined has no Bronze partition at
    all, and taking the dates from the Bronze layer alone would pass over it
    and apply the day after it, which moves the watermark beyond a day whose
    records can then never be applied: the update only ever looks at dates
    after the watermark.
    """
    dates = set(dltins_reference_dates(config_path))
    for name in finished_files(config_path) + quarantined_files(config_path):
        parsed = parse_file_name(name)
        if parsed is not None and parsed["kind"] == "DLTINS":
            dates.add(parsed["date"])
    # The days the landing zone last saw the register list. A day whose files
    # never arrived leaves no trace in the data lake at all, and applying the
    # days after it would move the watermark past it for good.
    dates.update(
        parse_reference_date(day)
        for day in read_landing_state(config_path)["published"]
    )
    return sorted(d for d in dates if d > after)


def _partition_holds_records(config_path, datadt):
    """Whether the Bronze partition of ``datadt`` holds a file.

    The Bronze layer creates the partition of a delta file that carries no
    records as an empty directory, so that the date is known to have been
    imported. Such a date has nothing to apply.
    """
    # dltins_partitions names the folders that hold the date, so the partition
    # itself has to be joined on: walking the folder would find the files of
    # every other date in it and call an empty partition full.
    return any(
        name.endswith(".parquet")
        for folder in dltins_partitions(config_path, datadt)
        for _, _, names in os.walk(os.path.join(folder, PARTITION_PREFIX + datadt))
        for name in names
    )


def update_from_bronze_to_silver_main(spark, config_path):
    """Update the historical FIRDS database from the Bronze layer.

    The function initialises the database from the full file if it does not
    exist yet, and then applies every delta file that is newer than the state
    recorded in the metadata, one reference date at a time and in chronological
    order. The order matters: a delta file states what changed since the
    previous one, so applying them out of order would attribute the wrong
    validity periods to the versions.

    A reference date is only applied once every part of its delta file is in
    the finished folder of the landing zone. The first date that is not
    complete stops the update: the dates before it are applied, it and the
    ones after it are not, and the function raises. Applying past it would move
    the watermark beyond it, and its missing records could then never be
    applied.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    """
    with pipeline_run(
        logger,
        "update_from_bronze_to_silver_main",
        module="firds",
        layer="silver",
        table="MAIN",
        config_path=config_path,
    ) as outcome:
        _update_from_bronze_to_silver_main(spark, config_path, outcome)


def _update_from_bronze_to_silver_main(spark, config_path, outcome):
    """Do the work of :func:`update_from_bronze_to_silver_main`."""

    # Whether the database exists is asked directly rather than inferred from a
    # read failing. Catching every exception here would treat a permission
    # problem or an unreadable transaction log as an empty database, and the
    # next step overwrites the history from the full file.
    if not common.delta_table_exists(spark, common.ref_silver_main(config_path)):
        logger.info(
            "Initialising historical database",
            extra=context(module="firds", layer="silver", table="MAIN"),
        )
        initialise_firds_database(spark, config_path)

    recorded = common.get_metadata_maxdate(spark, config_path, "MAIN")

    if recorded is None:
        # The history is written before the watermark, so a run that died
        # between the two leaves a database whose watermark is missing. The
        # commits of the history name the reference dates they applied, which
        # is enough to put it back.
        recorded = watermark_from_commits(
            spark, common.ref_silver_main(config_path)
        )
        if recorded is not None:
            logger.warning(
                "The watermark was missing and has been recovered from the "
                "commits of the historical database",
                extra=context(
                    module="firds", layer="silver", table="MAIN", datadt=recorded
                ),
            )
            common.update_metadata_maxdate(
                spark, config_path, table="MAIN", maxdate=recorded
            )

    if recorded is None:
        raise RuntimeError(
            "The historical database exists but carries no watermark, so there "
            "is no way to tell which delta files have already been applied. "
            "Record the reference date the database was last built up to with "
            "fincommons.firds.common.update_metadata_maxdate(spark, "
            "config_path, table='MAIN', maxdate=<date>) before updating."
        )

    maxdate = parse_reference_date(recorded)

    # A database that holds the amounts as doubles cannot take the decimals
    # the Bronze layer is read as. Such a database is rewritten with decimals
    # here; one that already holds decimals is left alone.
    migrate_silver_main_to_decimal(spark, config_path)

    # A delta file that ESMA published after its own day had been settled.
    # Where the history has already been built past that day, its records are
    # not in it and applying the days after it would not put them there.
    state = read_landing_state(config_path)
    passed = sorted(
        day for day in state["late"] if parse_reference_date(day) <= maxdate
    )
    if passed:
        outcome["blocked"] = passed[0]
        raise RuntimeError(
            "The delta file of "
            + ", ".join(passed)
            + " was published after the history had been built past that day, "
            "so its records are missing from the history. Rebuild the history "
            "from that day, or accept the gap with "
            "fincommons.firds.clear_late_day(config_path, day) before "
            "updating again."
        )

    periods = reference_dates_to_apply(config_path, after=maxdate)

    if len(periods) == 0:
        outcome["periods"] = 0
        logger.warning(
            "Nothing to update, consider updating the Bronze layer",
            extra=context(
                module="firds", layer="silver", table="MAIN", datadt=recorded
            ),
        )
        return

    finished = finished_files(config_path)
    in_bronze = set(dltins_reference_dates(config_path))
    outcome["periods"] = 0
    outcome["empty"] = 0

    for d in periods:
        datadt = format_reference_date(d)
        missing = missing_parts(finished, "DLTINS", d)
        if missing:
            outcome["blocked"] = datadt
            raise RuntimeError(
                f"The delta file of {datadt} is incomplete, missing "
                f"{', '.join(missing)}. The history has been built up to the "
                f"day before and is not updated further until the missing "
                f"parts have been imported into the Bronze layer."
            )
        if d not in in_bronze:
            outcome["blocked"] = datadt
            raise RuntimeError(
                f"Every part of the delta file of {datadt} has been imported, "
                "but the Bronze layer holds no partition for it, not even an "
                "empty one. A partition has gone missing; the history is not "
                "updated further until it is back, or until the watermark is "
                "moved past this date by hand."
            )
        if not _partition_holds_records(config_path, datadt):
            # The file of that date carried no records. There is nothing to
            # apply, and the date is passed rather than left to be applied
            # again on the next run.
            outcome["empty"] += 1
            logger.warning(
                "The delta file of this date held no records, nothing to apply",
                extra=context(
                    module="firds", layer="silver", table="MAIN", datadt=datadt
                ),
            )
            common.update_metadata_maxdate(
                spark, config_path, table="MAIN", maxdate=datadt
            )
            outcome["datadt"] = datadt
            record_applied_through(config_path, datadt)
            if datadt in state["late"]:
                clear_late_day(config_path, datadt)
            continue

        # One partition at a time, and only once the date is known to be
        # there. Reading the whole folder would put files written with
        # different vintages of the XML schema under one schema, and reading
        # it before the checks above would fail with a complaint about the
        # schema of a folder that holds no file yet, which says nothing about
        # what is actually missing.
        bronze = read_dltins_partition(
            spark, config_path, datadt, records=history.ACTION_TYPES
        )
        upsert_from_bronze_daily(
            spark=spark, bronze=bronze, config_path=config_path, datadt=datadt
        )

        # Cluster the files written since the last run (every Friday, i.e.
        # weekday == 4). On a clustered table this is incremental: it takes
        # the files that have arrived since the previous OPTIMIZE and leaves
        # the ones it has already clustered alone, so the cost follows the
        # week rather than the size of the history.
        if d.weekday() == 4:

            logger.debug(
                "Compacting the historical database",
                extra=context(
                    module="firds", layer="silver", table="MAIN", datadt=datadt
                ),
            )
            dt = common.delta_table(spark, common.ref_silver_main(config_path))
            dt.optimize().executeCompaction()

        outcome["datadt"] = datadt
        outcome["periods"] += 1
        # The landing zone cannot read the watermark of the historical
        # database and needs to know how far it stands, so that it can tell a
        # file for a day still to come from one for a day left behind.
        record_applied_through(config_path, datadt)
        if datadt in state["late"]:
            # It arrived late but still before the history reached its day.
            clear_late_day(config_path, datadt)


def declare_silver_main_layout(spark, config_path):
    """Declare how the historical database lays its files out.

    The table is clustered by the instrument key rather than partitioned by
    trading venue. Partitioning by ``TRADGVN`` puts the table into one
    directory per venue, which is far too fine a grain for its size -- the
    partitions average well under a megabyte -- and it buys no pruning, because
    a delta file touches most venues at once and the merge therefore reads
    every partition anyway. What it does buy is a file per (writing task,
    venue) pair on every append, so the table grows thousands of files of a
    few kilobytes a day and every later merge has to open all of them.

    Clustering keeps the columns of the merge condition together in the same
    files without cutting the table into directories, so the statistics of a
    file can rule it out. The clustering key is :data:`history.KEY_COLUMNS`
    itself, so that it cannot drift away from the condition the merges are
    written with. ``optimizeWrite`` sizes the files of the writes that follow,
    which is what keeps a small delta file from arriving as several hundred of
    them.

    Deletion vectors keep the daily merge from rewriting the history. Without
    them a merge rewrites every file it touches in full, and since a delta file
    touches instruments all over the table, that is every file: applying one
    that changes 172 versions copies all 17 million rows of the history and
    takes 54 seconds. With them the merge marks the changed rows as deleted in
    a small side file and writes only the new versions, which takes 15 seconds
    on the same file, and the cost follows the size of the file rather than that
    of the history. The weekly ``OPTIMIZE`` rewrites the files whose deleted
    rows have accumulated. Enabling them raises the reader version of the
    table to 3, so a reader outside Spark needs deletion-vector support.

    The statements are idempotent, so the function can be called again on a
    database that already carries the layout.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    """
    table = common.sql_table(common.ref_silver_main(config_path))
    clustering = ", ".join(history.KEY_COLUMNS)

    spark.sql(f"ALTER TABLE {table} CLUSTER BY ({clustering})")
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        "('delta.autoOptimize.optimizeWrite' = 'true', "
        "'delta.enableDeletionVectors' = 'true')"
    )


def migrate_silver_main_to_decimal(spark, config_path):
    """Rewrite a historical database that carries its amounts as doubles.

    The money and rate fields of the database are the ``decimal`` types of
    :data:`DECIMAL_COLUMNS`, the types the Bronze layer is read as. Where the
    main table holds any of them as ``double``, the daily update cannot append
    to it, and Delta cannot widen a ``double`` column into a ``decimal`` one in
    place. The table is therefore read, those columns are cast, and the result
    is written back over it once, in one transaction; a reader sees the table
    before the rewrite or after it, never a part of either.

    An overwrite starts the history of the table afresh as far as
    :func:`tagged_commits` is concerned, so the rewrite carries the watermark
    in a tag of its own, from which :func:`watermark_from_commits` recovers it
    as it would from the append of that date. It also replaces what the table
    declares about itself, so the layout is declared again afterwards.

    Nothing is written where a value would not fit its ``decimal``: the cast
    would turn it into a null, or raise under ANSI mode, and the loss of an
    amount is not something to find out about later. Nor is anything written
    while the update of a date stands half-committed, since the rewrite would
    drop the tags it is resumed by.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.

    Returns
    -------
    bool
        Whether the table was rewritten. A database that already carries the
        ``decimal`` types is left alone, so the function can be called on
        every run.
    """
    ref = common.ref_silver_main(config_path)
    table = common.read_delta(spark, ref)
    types = dict(table.dtypes)
    stale = {
        column: wanted
        for column, wanted in DECIMAL_COLUMNS.items()
        if column in types and types[column] != wanted
    }
    if not stale:
        return False

    tags = [
        tag for _, tag in tagged_commits(spark, ref)
        if tag and tag.startswith(COMMIT_TAG_PREFIX + ":")
    ]
    if tags and tags[0].endswith(":merge"):
        raise RuntimeError(
            "The historical database carries its amounts as doubles and has to "
            "be rewritten as decimals, but the update of "
            f"{tags[0].split(':')[1]} stands half-committed. Rewriting now "
            "would drop the tags that update is resumed by; finish it with the "
            "code that started it first."
        )

    watermark = (
        common.get_metadata_maxdate(spark, config_path, "MAIN")
        or watermark_from_commits(spark, ref)
    )

    # try_cast rather than cast: a value that does not fit comes back null
    # under either setting of ANSI mode, so the count below finds it.
    cast = {
        column: func.expr(f"try_cast(`{column}` AS {wanted})")
        for column, wanted in stale.items()
    }
    lost = table.agg(*[
        func.count(func.when(func.col(column).isNotNull() & cast[column].isNull(), 1))
        .alias(column)
        for column in stale
    ]).first().asDict()
    lost = {column: n for column, n in lost.items() if n}
    if lost:
        raise RuntimeError(
            "The historical database was not rewritten as decimals, because "
            "some of its values do not fit the decimal type of their column: "
            + ", ".join(
                f"{n} in {column} ({stale[column]})" for column, n in lost.items()
            )
            + "."
        )

    migrated = table.select(*[
        cast[column].alias(column) if column in cast else func.col(column)
        for column in table.columns
    ])
    options = {"overwriteSchema": "true"}
    if watermark is not None:
        options["userMetadata"] = commit_tag(watermark, "decimal")
    common.write_delta(migrated, ref, options=options)
    declare_silver_main_layout(spark, config_path)

    logger.warning(
        "The historical database has been rewritten with decimal amounts",
        extra=context(
            module="firds",
            layer="silver",
            table="MAIN",
            datadt=watermark,
        ),
    )
    return True


def initialise_firds_database(spark, config_path):
    """Register the full file as the first state of the historical database.

    This is paragraph 27.a of the instructions. The records of the FULINS file
    are registered with the start of their publication period as ``VLDFROM``,
    an empty ``VLDTO`` and ``LATESTRECORD`` set, see
    :func:`fincommons.firds.history.initialise_history`.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    """
    dates = list_reference_dates(common.path_bronze_auth_017_001_02(config_path))

    if len(dates) == 0:
        raise RuntimeError(
            "There is no full file (FULINS) in the Bronze layer at "
            f"{common.path_bronze_auth_017_001_02(config_path)}, so the "
            "historical database cannot be initialised. Download and import "
            "one first."
        )

    # A full file states the reference data in full, so the newest one is the
    # state to start from. An older one holds instruments the newest one no
    # longer carries, and taking them along would register them as in force.
    maxdate = format_reference_date(dates[-1])
    if len(dates) > 1:
        logger.warning(
            "The Bronze layer holds several full files, the newest is used",
            extra=context(
                module="firds",
                layer="bronze",
                datadt=maxdate,
                files=len(dates),
            ),
        )

    # A full file is published in parts, and an instrument missing from it
    # would only reappear in the history once a delta file changed it.
    missing = missing_parts(finished_files(config_path), "FULINS", dates[-1])
    if missing:
        raise RuntimeError(
            f"The full file is incomplete, missing {', '.join(missing)}. The "
            "historical database is not initialised until every part has been "
            "imported into the Bronze layer."
        )

    # The parts that arrived only show what is missing of a letter of the CFI
    # code that sent some. A letter of which no part arrived is noticed only
    # against the parts the register listed, which the landing zone records;
    # without that record the history would lack the asset class until delta
    # files happened to change its instruments.
    listed = read_landing_state(config_path)["full_sample"]
    if listed is not None and listed.get("date") == maxdate:
        unimported = unimported_listed_files(
            finished_files(config_path), listed.get("files", [])
        )
        if unimported:
            raise RuntimeError(
                f"The full file is incomplete, missing {', '.join(unimported)}, "
                "which the register lists. The historical database is not "
                "initialised until every part has been imported into the "
                "Bronze layer."
            )
    else:
        logger.warning(
            "The landing zone holds no list of the full file's parts, so a "
            "letter of the CFI code of which no part arrived cannot be noticed",
            extra=context(module="firds", layer="silver", table="MAIN", datadt=maxdate),
        )

    df = read_bronze_partition(
        spark, common.path_bronze_auth_017_001_02(config_path), maxdate
    )
    df = translate_xml_to_analytics_schema_017_001_02(df)
    df = check_publication_period(df, f"the full file of {maxdate}")
    df = history.initialise_history(df)

    # The commit names the reference date the history starts at, so that the
    # watermark can be recovered from it if this run does not reach it.
    common.write_delta(
        df,
        common.ref_silver_main(config_path),
        options={"userMetadata": commit_tag(maxdate, "init")},
    )
    declare_silver_main_layout(spark, config_path)

    common.initialise_metadata(spark, config_path, maxdate=maxdate)


#: The paths of the XML schema that the translation to the analytics schema
#: reads, with the type the analytics schema gives each of them. The Bronze
#: layer is read through these paths and no others, and every one of them is
#: cast to the type named here.
#:
#: Both of those are what makes the read robust against a change in the XML
#: schema a Bronze file was written with. The schemas in
#: :mod:`fincommons.schemas` are derived from ESMA's XSD files, and deriving
#: them again can change a type. Two vintages of the Bronze layer differ in
#: this way: one holds the money and rate fields as ``double`` and the other as
#: ``decimal``, and one holds ``<CancRcrd>`` as a full reference data record and
#: the other as the reduced one the schema states. The Bronze layer keeps the
#: files it has already written, so it comes to hold files of more than one
#: vintage, and Parquet will not hand a column
#: written as ``double`` to a reader that asks for ``decimal``. Naming the paths
#: keeps a change in a field the pipeline does not use out of sight altogether,
#: and casting them keeps the schema of the historical database the same
#: whichever vintage the file it is fed has.
#:
#: The types are the ones the historical database carries, so that a file of
#: either vintage lands in it unchanged. The money and rate fields are the
#: ``decimal`` types the XML schema derives from the XSD rather than
#: ``double``: an amount of up to eighteen integer digits and five decimals is
#: more than a double holds exactly, and these are figures reported to a
#: supervisor. A file of the older vintage states them as ``double`` and is
#: cast up to them. A database that holds them as ``double`` is rewritten
#: with them as ``decimal`` by :func:`migrate_silver_main_to_decimal`, since
#: Delta cannot widen a ``double`` column into a ``decimal`` one in place.
SOURCE_FIELDS = {
    "FinInstrmGnlAttrbts.Id": "string",
    "FinInstrmGnlAttrbts.FullNm": "string",
    "FinInstrmGnlAttrbts.ClssfctnTp": "string",
    "FinInstrmGnlAttrbts.CmmdtyDerivInd": "boolean",
    "FinInstrmGnlAttrbts.ShrtNm": "string",
    "FinInstrmGnlAttrbts.NtnlCcy": "string",
    "Issr": "string",
    "TradgVnRltdAttrbts.Id": "string",
    "TradgVnRltdAttrbts.IssrReq": "boolean",
    "TradgVnRltdAttrbts.AdmssnApprvlDtByIssr": "timestamp",
    "TradgVnRltdAttrbts.ReqForAdmssnDt": "timestamp",
    "TradgVnRltdAttrbts.FrstTradDt": "timestamp",
    "TradgVnRltdAttrbts.TermntnDt": "timestamp",
    "DebtInstrmAttrbts.TtlIssdNmnlAmt._VALUE": "decimal(23,5)",
    "DebtInstrmAttrbts.TtlIssdNmnlAmt._Ccy": "string",
    "DebtInstrmAttrbts.MtrtyDt": "date",
    "DebtInstrmAttrbts.NmnlValPerUnit._VALUE": "decimal(23,5)",
    "DebtInstrmAttrbts.NmnlValPerUnit._Ccy": "string",
    "DebtInstrmAttrbts.IntrstRate.Fxd": "decimal(21,10)",
    "DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.ISIN": "string",
    "DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.Indx": "string",
    "DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.Nm": "string",
    "DebtInstrmAttrbts.IntrstRate.Fltg.Term.Val": "long",
    "DebtInstrmAttrbts.IntrstRate.Fltg.Term.Unit": "string",
    "DebtInstrmAttrbts.IntrstRate.Fltg.BsisPtSprd": "long",
    "DebtInstrmAttrbts.DebtSnrty": "string",
    "DerivInstrmAttrbts.XpryDt": "date",
    "DerivInstrmAttrbts.PricMltplr": "decimal(35,17)",
    "DerivInstrmAttrbts.OptnTp": "string",
    "DerivInstrmAttrbts.OptnExrcStyle": "string",
    "DerivInstrmAttrbts.DlvryTp": "string",
    # The publication period is a choice, and ESMA uses more than one of its
    # branches in the same file: one delta file of January 2020 states
    # <PblctnPrd><FrDt> for 32,875 of its records and
    # <PblctnPrd><FrDtToDt><FrDt> for 19,630 more. Every branch the schemas
    # define is read here, and the translation coalesces them, because a
    # branch that is not read comes back null rather than failing, and a null
    # VLDFROM cannot be closed: date_sub(NULL, 1) is NULL, so the version it
    # supersedes stays in force and the instrument ends up with two.
    "TechAttrbts.PblctnPrd.Dt": "date",
    "TechAttrbts.PblctnPrd.FrDt": "date",
    "TechAttrbts.PblctnPrd.ToDt": "date",
    "TechAttrbts.PblctnPrd.FrDtToDt.FrDt": "date",
    "TechAttrbts.PblctnPrd.FrDtToDt.ToDt": "date",
    "TechAttrbts.RlvntCmptntAuthrty": "string",
    "TechAttrbts.RlvntTradgVn": "string",
}

#: The columns of the historical database that carry a money or rate field,
#: with the ``decimal`` type :data:`SOURCE_FIELDS` reads it as.
DECIMAL_COLUMNS = {
    "TOTALISSUEDNOMINALAMT": SOURCE_FIELDS["DebtInstrmAttrbts.TtlIssdNmnlAmt._VALUE"],
    "NMNLVALPERUNITAMT": SOURCE_FIELDS["DebtInstrmAttrbts.NmnlValPerUnit._VALUE"],
    "INTRSTRATEFXD": SOURCE_FIELDS["DebtInstrmAttrbts.IntrstRate.Fxd"],
    "PRICMLTPLR": SOURCE_FIELDS["DerivInstrmAttrbts.PricMltplr"],
}

#: The element whose absence says that a record is not of the type the column
#: it sits in stands for. :func:`read_daily_changes` tells the four record
#: types of a delta file apart by it, so the read has to leave it empty where
#: the file leaves it empty; the other elements are rebuilt whether the file
#: states them or not, since the translation only ever reads their leaves.
TECHNICAL = "TechAttrbts"


def _type_at(schema, path):
    """The type at ``path`` of ``schema``, or ``None`` if it has no such path.

    Parameters
    ----------
    schema: pyspark.sql.types.StructType
        The schema to look in.
    path: str
        A path of element names separated by dots, e.g.
        ``'NewRcrd.TechAttrbts.PblctnPrd.FrDt'``.
    """
    current = schema
    for name in path.split("."):
        if not isinstance(current, StructType) or name not in current.fieldNames():
            return None
        current = current[name].dataType
    return current.simpleString()


def _by_first_element(paths):
    """Group ``paths`` by their first element, with the rest of each path."""
    groups = {}
    for path, wanted_as in paths.items():
        first, _, rest = path.partition(".")
        groups.setdefault(first, {})[rest] = wanted_as
    return groups


def _fields_to_read(schema, prefix, paths):
    """The elements of ``prefix`` to read, as ``(name, type)`` pairs.

    The type of a path is the one the file states, not the one the analytics
    schema wants: Parquet hands a column over as it was written and refuses to
    convert it, so the conversion is asked for afterwards, by
    :func:`_projection`. A path the file does not state is asked for with the
    type the analytics schema wants and comes back empty.
    """
    fields = []
    for first, below in _by_first_element(paths).items():
        full = f"{prefix}.{first}" if prefix else first
        if "" in below:
            fields.append((first, _type_at(schema, full) or below[""]))
        else:
            inner = _fields_to_read(schema, full, below)
            fields.append((first, _as_struct(inner)))
    return fields


def _as_struct(fields):
    """The ``(name, type)`` pairs as the spelling of a struct type."""
    return "struct<" + ",".join(f"`{name}`:{type_}" for name, type_ in fields) + ">"


def _projection(prefix, paths):
    """The elements of ``prefix`` the pipeline reads, in one fixed shape.

    Every path is cast to the type the analytics schema wants, so that the
    frame this produces has the same schema whichever vintage of the XML
    schema the file was written with.
    """
    columns = []
    for first, below in _by_first_element(paths).items():
        full = f"{prefix}.{first}" if prefix else first
        if "" in below:
            columns.append(func.col(full).cast(below[""]).alias(first))
            continue
        built = func.struct(*_projection(full, below))
        if first == TECHNICAL:
            # A struct built out of empty fields is itself not empty, so the
            # element that says which record type a row belongs to is kept
            # empty by hand where the file leaves it empty.
            built = func.when(func.col(full).isNotNull(), built)
        columns.append(built.alias(first))
    return columns


def read_bronze_partition(spark, path, datadt, records=()):
    """Read one Bronze partition, through the paths the pipeline reads.

    The files of a partition are grouped by the schema they were written with
    and each group is read on its own, because one read can ask for only one
    type per column and Parquet will not convert a column into another. Within
    a group the read asks for the paths named in :data:`SOURCE_FIELDS` and for
    nothing else, so a field the pipeline does not use can change its type
    without being noticed, and the groups are brought together by
    :func:`_projection`, which casts every path to the type the analytics
    schema wants. What comes back therefore has the same schema whatever the
    partition holds.

    The schema of every file is read, which costs one look at its footer. That
    is nothing next to reading a delta file, of which a partition holds one to
    four, and it is paid once for the forty files of a full one.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    path: str
        The Bronze folder of a schema, e.g. the one named by
        :func:`fincommons.firds.common.paths_bronze_dltins`.
    datadt: str
        The reference date of the partition, spelled ``'YYYY-MM-DD'``.
    records: tuple of str
        The elements the records sit in, i.e.
        :data:`fincommons.firds.history.ACTION_TYPES` for a delta file. A
        full file states its records directly and takes the default.
    """
    partition = os.path.join(path, PARTITION_PREFIX + datadt)
    files = sorted(
        os.path.join(root, name)
        for root, _, names in os.walk(partition)
        for name in names
        if name.endswith(".parquet")
    )
    if not files:
        raise RuntimeError(
            f"The Bronze partition {partition} holds no file, so there is "
            "nothing to read."
        )

    prefixes = records or ("",)
    groups = {}
    for file in files:
        schema = spark.read.parquet(file).schema
        written_as = tuple(
            _type_at(schema, f"{prefix}.{name}" if prefix else name)
            for prefix in prefixes
            for name in SOURCE_FIELDS
        )
        groups.setdefault(written_as, (schema, []))[1].append(file)

    if len(groups) > 1:
        logger.warning(
            "The files of this partition were written with different schemas "
            "and are read one schema at a time",
            extra=context(
                module="firds", layer="bronze", datadt=datadt,
                files=len(files), schemas=len(groups),
            ),
        )

    frames = []
    for schema, group in groups.values():
        if records:
            fields = [
                (record, _as_struct(_fields_to_read(schema, record, SOURCE_FIELDS)))
                for record in records
            ]
            columns = [
                func.struct(*_projection(record, SOURCE_FIELDS)).alias(record)
                for record in records
            ]
        else:
            fields = _fields_to_read(schema, "", SOURCE_FIELDS)
            columns = _projection("", SOURCE_FIELDS)
        asked = ", ".join(f"`{name}` {type_}" for name, type_ in fields)
        frame = spark.read.schema(asked).parquet(*group)
        frames.append(
            frame.select(*columns, func.lit(datadt).cast("date").alias("DATADT"))
        )
    return functools.reduce(DataFrame.unionByName, frames)


#: The element of a delta file that reports a cancellation. It is the one
#: record type of ``auth.036.001.03`` that does not carry a full reference
#: data record, so it is translated on its own.
CANCELLATION = "CancRcrd"


def read_daily_changes(incoming):
    """Flatten the four record types of a delta file into one table.

    A delta file reports a change in one of four elements -- ``<NewRcrd>``,
    ``<ModfdRcrd>``, ``<CancRcrd>`` and ``<TermntdRcrd>`` -- which are separate
    columns of the Bronze layer. The element a record was reported in is what
    determines how it changes the history, so it is carried along in
    ``ACTNTP``.

    Three of the four carry a full reference data record and are translated
    the same way. ``<CancRcrd>`` does not: the schema types it as a reduced
    record that states the instrument and the trading venue whose combination
    is cancelled and nothing else, so it has no ``TechAttrbts`` and is read by
    :func:`read_cancellations` instead. Recognising a record by that element,
    as the other three are, dropped every cancellation before it reached the
    history.
    """
    dfs = []
    for colname in history.ACTION_TYPES:
        if colname == CANCELLATION:
            dfs.append(read_cancellations(incoming))
            continue
        # The reference date comes along so that a termination stating no
        # publication period can fall back on it; see :func:`_period_start`.
        df = incoming.select(f"{colname}.*", "DATADT").filter(
            func.col("TechAttrbts").isNotNull()
        )
        df = translate_xml_to_analytics_schema_036_001_03(df)
        df = df.withColumn("ACTNTP", func.lit(colname))
        dfs.append(df)
    return functools.reduce(DataFrame.unionByName, dfs)


def read_cancellations(incoming):
    """The ``<CancRcrd>`` records of a delta file, in the analytics schema.

    A cancellation says that the combination of an instrument and a trading
    venue is withdrawn. It carries none of the reference data fields, so those
    columns are left empty; they are still produced, and with the types the
    other three record types give them, because the four are unioned by name.

    The reduced record carries no publication period either, and the validity
    of a version is derived from one. The reference date of the file is used
    in its place: it is the day the cancellation was published, which is what
    ``TechAttrbts/PblctnPrd/FrDt`` states for the records that have it.

    Parameters
    ----------
    incoming: pyspark.sql.DataFrame
        One Bronze partition of a delta file, i.e. the schema
        auth.036.001.03 and the reference date in ``DATADT``.
    """
    complete = next(
        name for name in history.ACTION_TYPES if name != CANCELLATION
    )
    typed = dict(
        translate_xml_to_analytics_schema_036_001_03(
            incoming.select(f"{complete}.*")
        ).dtypes
    )
    stated = {
        "ISIN": func.col(f"{CANCELLATION}.FinInstrmGnlAttrbts.Id"),
        "TRADGVN": func.col(f"{CANCELLATION}.TradgVnRltdAttrbts.Id"),
        "TECH_PBLCTNPRD_FRDT": func.to_date(func.col("DATADT")),
    }
    return incoming.select(
        *[
            stated.get(name, func.lit(None)).cast(typed[name]).alias(name)
            for name in ANALYTICS_COLUMNS
        ],
        func.lit(CANCELLATION).alias("ACTNTP"),
    ).filter(func.col("ISIN").isNotNull())


def check_publication_period(records, source):
    """Refuse records whose publication period could not be read.

    The validity of a version is derived from the start of its publication
    period: ``VLDFROM`` is that date, and the version a record supersedes is
    closed on the day before it. Where the date is missing the closing date is
    ``date_sub(NULL, 1)``, which is null as well, so the superseded version is
    never closed and the instrument comes to have two versions in force.

    Nothing fails at that point, which is what makes it worth refusing here.
    The history goes on building, ``LATESTRECORD`` becomes ambiguous, and the
    fault surfaces days later as a merge that matches several source rows
    against one target row -- an error that points at the merge rather than at
    the record that caused it.

    ``TechAttrbts.PblctnPrd`` is a choice, and reading only its ``FrDt``
    branch leaves the records stating ``FrDtToDt`` without a start: in the
    real data that gives 132,977 instruments up to eight versions in force
    each before a merge refuses to carry on. Every branch is therefore read,
    and a termination stating no period at all falls back on the reference
    date of its file; see :func:`_period_start`. What is left for this to
    catch is a record whose period cannot be read for a reason not yet known.

    Parameters
    ----------
    records: pyspark.sql.DataFrame
        Records in the analytics schema, carrying ``TECH_PBLCTNPRD_FRDT``.
    source: str
        What was being read, for the message.

    Returns
    -------
    pyspark.sql.DataFrame
        ``records`` unchanged, so the check can stand in a chain of reads.

    Raises
    ------
    ValueError
        If any record states no publication start date.
    """
    missing = records.filter(func.col("TECH_PBLCTNPRD_FRDT").isNull())
    count = missing.count()
    if not count:
        return records

    columns = [c for c in ("ISIN", "TRADGVN", "ACTNTP") if c in records.columns]
    examples = [tuple(row) for row in missing.select(*columns).limit(3).collect()]
    raise ValueError(
        f"{count} records of {source} state no publication start date. The "
        f"validity of a version is derived from it, so the versions these "
        f"supersede could not be closed and their instruments would come to "
        f"have more than one version in force. Examples, as "
        f"({', '.join(columns)}): {examples}."
    )


def upsert_from_bronze_daily(spark, bronze, config_path: str, datadt: str):
    """Apply the delta file of one reference date to the historical database.

    The records of the file are classified by
    :func:`fincommons.firds.history.plan_changes` into the three operations of
    paragraph 27.b, which are then applied to the Delta table:

    1. the versions that the file supersedes are closed on the day before the
       incoming record starts and lose the latest record flag,
    2. a version terminated in the ordinary way is replaced by the incoming
       record, and
    3. the remaining incoming records are inserted.

    The first two steps read the state of the table before any of them is
    applied, and they concern disjoint sets of instruments, so their order does
    not matter. The insert has to come last, because it would otherwise add a
    second open version for the instruments that step one closes.

    The first two steps are one commit and the third another, and the
    watermark follows in a table of its own, so a run can die with the date
    applied in part. Applying it again from the table as it then stands would
    close the versions the date has just inserted. Each commit is therefore
    tagged with the date, and a date is resumed rather than started again:

    * if its append was committed, it is not applied again, and only the
      watermark is recorded;
    * if only its merge was committed, the plan is made from the table as it
      stood before the merge, and only the append is made.
    """

    logger.info(
        "Applying delta file",
        extra=context(
            module="firds", layer="silver", table="MAIN", datadt=datadt
        ),
    )

    # The reference to the cached frame is kept, because read_daily_changes
    # reads it once per action type and the blocks are of no use afterwards.
    # Rebinding the name would leave them in the cache for the rest of the
    # session, and a backfill applies one file after another.
    daily = bronze.filter(func.col("DATADT") == datadt).cache()
    incoming = check_publication_period(
        read_daily_changes(daily), f"the delta file of {datadt}"
    )

    ref = common.ref_silver_main(config_path)
    committed = committed_steps(spark, ref, datadt)

    if "append" in committed:
        logger.warning(
            "Delta file had already been applied, only the watermark was recorded",
            extra=context(
                module="firds", layer="silver", table="MAIN", datadt=datadt
            ),
        )
        daily.unpersist()
        common.update_metadata_maxdate(spark, config_path, table="MAIN", maxdate=datadt)
        return

    # The plan is made from a fixed version of the table: the one before the
    # merge of this date. The merge does not fill the cache of the plan, so
    # the append computes the plan again, and a read of the table that is not
    # pinned would then see the table as the merge left it. A version the
    # merge has replaced is no longer in force there, and the record that
    # replaced it would be planned as an insert and added a second time.
    if "merge" in committed:
        base_version = committed["merge"] - 1
    else:
        base_version = committed["current"]
    main = common.read_delta_version(spark, ref, base_version)
    columns = main.columns

    planned = history.plan_changes(main, incoming).cache()
    table = common.delta_table(spark, ref)
    # The condition is built from the key the history identifies an
    # instrument by, so that it cannot drift away from it.
    condition = " AND ".join(
        [f"tgt.{name} = src.{name}" for name in history.KEY_COLUMNS]
        + [f"tgt.{history.VALID_TO} IS NULL"]
    )

    # Closing a superseded version and replacing a terminated one are applied
    # together. Both look for the version in force under the same condition,
    # and an instrument is subject to at most one of them, so a second merge
    # would read the table again to reach a disjoint set of rows. The clauses
    # are told apart by the tag records_to_change attaches; should a file ever
    # produce two rows for one instrument the merge fails rather than picking
    # one of them.
    if "merge" in committed:
        logger.warning(
            "Resuming a delta file whose merge had already been committed",
            extra=context(
                module="firds", layer="silver", table="MAIN", datadt=datadt,
                version=committed["merge"],
            ),
        )
    else:
        changing = history.records_to_change(planned, columns)
        tag = func.col(f"src.{history.OPERATION}")
        spark.conf.set(USER_METADATA_CONF, commit_tag(datadt, "merge"))
        try:
            table.alias("tgt").merge(changing.alias("src"), condition).whenMatchedUpdate(
                condition=tag == func.lit(history.CLOSE_AND_INSERT),
                set={"VLDTO": "src.VLDTO", "LATESTRECORD": func.lit(False)},
            ).whenMatchedUpdate(
                condition=tag == func.lit(history.REPLACE),
                set={name: func.col(f"src.`{name}`") for name in columns},
            ).execute()
        finally:
            spark.conf.unset(USER_METADATA_CONF)

    # The file sizing of the append is left to optimizeWrite, which
    # declare_silver_main_layout turns on. The plan carries as many tasks as
    # the window functions of plan_changes left behind, and one day of a delta
    # file would otherwise arrive as a few hundred files of a few kilobytes
    # each, which every later merge has to open again.
    inserting = history.records_to_insert(planned, columns)
    common.write_delta(
        inserting, ref, mode="append",
        options={"userMetadata": commit_tag(datadt, "append")},
    )

    planned.unpersist()
    daily.unpersist()
    common.update_metadata_maxdate(spark, config_path, table="MAIN", maxdate=datadt)


def get_analytical_fields():
    """Return the reference data fields carried by the historical database.

    The columns of :data:`ANALYTICS_COLUMNS` that state something about the
    instrument, i.e. all but the key and the technical attributes.
    """
    return [
        name for name in ANALYTICS_COLUMNS
        if name not in history.KEY_COLUMNS and not name.startswith("TECH_")
    ]


def update_from_silver_main_to_silver_compact(spark, config_path,
                                              file_format = None):
    """Prepares FIRDS data so that there is only one entry per ISIN in the data set.

    The table is rebuilt from the main table in full, so the write replaces it
    rather than adding to it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    file_format: str, optional
        Format the compact table is written in, ``'parquet'`` or ``'delta'``.
        Delta is worth asking for where a reader may be querying the table
        while it is rebuilt, since the rebuild then replaces it in one
        transaction; see :func:`fincommons.firds.write_compact`. It says
        nothing about the main table, which is always Delta.

        Only a compact table addressed by path has a choice: one addressed by
        catalogue name is a Delta table and the argument may only confirm it.
        Left out, the format is :data:`fincommons.firds.DEFAULT_COMPACT_FORMAT`
        for a table addressed by path and Delta for one addressed by name.
    """
    common.compact_format(common.ref_silver_compact(config_path), file_format)
    with pipeline_run(
        logger,
        "update_from_silver_main_to_silver_compact",
        module="firds",
        layer="silver",
        table="COMPACT",
        config_path=config_path,
        file_format=file_format,
    ):
        _update_from_silver_main_to_silver_compact(spark, config_path, file_format)


def _only_value(name):
    """The value of ``name`` if all rows of the group agree on it, else null.

    Several values cannot be joined into one date or number, and choosing one
    of them would state something the venues do not agree on.
    """
    values = func.collect_set(name)
    return func.when(func.size(values) == 1, values[0])


def _update_from_silver_main_to_silver_compact(spark, config_path, file_format):
    """Do the work of :func:`update_from_silver_main_to_silver_compact`.

    The text columns join the different values the trading venues report for
    an ISIN with ``+``. The dates and the price multiplier keep their type --
    for the multiplier the ``decimal`` of :data:`DECIMAL_COLUMNS` -- and hold
    the value the venues agree on, and are empty where they do not.
    """
    compact = common.read_delta(spark, common.ref_silver_main(config_path)) \
        .filter(func.col("LATESTRECORD")) \
        .groupBy("ISIN") \
        .agg(
            func.concat_ws("+", func.collect_set("CFI")).alias("CFI"),
            func.concat_ws("+", func.collect_set("ISSUER")).alias("ISSUER"),
            func.concat_ws("+", func.collect_set("TRADGVN")).alias("TRADGVN"),
            func.concat_ws("+", func.collect_set("SHORTNAME")).alias("SHORTNAME"),
            _only_value("MTRTYDT").alias("MTRTYDT"),
            _only_value("XPRYDT").alias("XPRYDT"),
            _only_value("PRICMLTPLR").alias("PRICMLTPLR"),
        ) \
        .withColumn("MTRTYDT", func.to_date(func.col("MTRTYDT"))) \
        .withColumn("XPRYDT", func.to_date(func.col("XPRYDT"))) \
        .withColumn("PRICMLTPLR", func.col("PRICMLTPLR").cast(DECIMAL_COLUMNS["PRICMLTPLR"]))

    common.write_compact(
        compact, common.ref_silver_compact(config_path), file_format = file_format
    )


#: The columns of the historical database, in the order the table carries
#: them. The full file and the delta files state the same fields, so one
#: translation serves both.
ANALYTICS_COLUMNS = [
    "ISIN", "FULLNAME", "CFI", "COMMDERIVIND",
    "ISSUER", "TRADGVN", "SHORTNAME", "ISSRREQ",
    "APPRVLDTBYISSR", "REQFORADMSSNDT", "FRSTTRADDT", "TERMNTNDT",
    "NTNLCCY", "TOTALISSUEDNOMINALAMT", "TOTALISSUEDNOMINALCCY", "MTRTYDT",
    "NMNLVALPERUNITAMT", "NMNLVALPERUNITCCY", "INTRSTRATEFXD", "INTRSTRATEFLTGIDTP",
    "INTRSTRATEFLTGID", "INTRSTRATEFLTGTERMVAL", "INTRSTRATEFLTGTERMUNIT", "INTRSTRATEFLTGSPRD",
    "DEBTSNRTY", "XPRYDT", "PRICMLTPLR", "OPTNTP",
    "OPTNEXRCSTYLE", "DLVRYTP", "TECH_PBLCTNPRD_FRDT", "TECH_PBLCTNPRD_TODT",
    "TECH_RLVNTCMPTNTAUTHRTY", "TECH_RLVNTTRADGVN",
]


def _period_start(df):
    """The places a record's publication period may begin, in order.

    ``TechAttrbts/PblctnPrd`` is a choice and ESMA uses more than one of its
    branches in the same file: the records reported in ``<ModfdRcrd>`` and
    ``<NewRcrd>`` state ``<FrDt>``, while a termination states the closed
    range ``<FrDtToDt>`` -- the instrument's publication period has ended, so
    the file gives both of its ends.

    A termination may also state no period at all. In one delta file of
    January 2020, 4,477 of 24,107 terminations carried nothing in
    ``TechAttrbts`` but the competent authority and the trading venue. The
    reference date of the file stands in for those: it is the day the
    termination was published, which is what the period states for the
    records that have one, and it is the same substitution
    :func:`read_cancellations` makes for ``<CancRcrd>``, which carries no
    period either.

    The reference date is only offered where the frame carries it, so that a
    caller reading records outside a Bronze partition gets no silent default.
    """
    places = [
        func.col("TechAttrbts.PblctnPrd.FrDt"),
        func.col("TechAttrbts.PblctnPrd.FrDtToDt.FrDt"),
        func.col("TechAttrbts.PblctnPrd.Dt"),
    ]
    if "DATADT" in df.columns:
        places.append(func.to_date(func.col("DATADT")))
    return places


def translate_xml_to_analytics_schema(df):
    """Transform to analytics schema.

    Transforms the data frame so that it follows the analytics schema. The
    fields sit at the same paths in ``auth.017.001.02``, the full file, and in
    a record of ``auth.036.001.03``, a delta file, so the same translation
    applies to both.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        A spark data frame that follows the schema auth.017.001.02, or one
        record type of the schema auth.036.001.03.
    """

    df = df \
    .withColumn("ISIN", func.col("FinInstrmGnlAttrbts.Id")) \
    .withColumn("FULLNAME", func.col("FinInstrmGnlAttrbts.FullNm")) \
    .withColumn("CFI", func.col("FinInstrmGnlAttrbts.ClssfctnTp")) \
    .withColumn("COMMDERIVIND", func.col("FinInstrmGnlAttrbts.CmmdtyDerivInd")) \
    .withColumn("ISSUER", func.col("Issr")) \
    .withColumn("TRADGVN", func.col("TradgVnRltdAttrbts.Id")) \
    .withColumn("SHORTNAME", func.col("FinInstrmGnlAttrbts.ShrtNm")) \
    .withColumn("ISSRREQ", func.col("TradgVnRltdAttrbts.IssrReq")) \
    .withColumn("APPRVLDTBYISSR", func.col("TradgVnRltdAttrbts.AdmssnApprvlDtByIssr")) \
    .withColumn("REQFORADMSSNDT", func.col("TradgVnRltdAttrbts.ReqForAdmssnDt")) \
    .withColumn("FRSTTRADDT", func.col("TradgVnRltdAttrbts.FrstTradDt")) \
    .withColumn("TERMNTNDT", func.col("TradgVnRltdAttrbts.TermntnDt")) \
    .withColumn("NTNLCCY", func.col("FinInstrmGnlAttrbts.NtnlCcy")) \
    .withColumn("TOTALISSUEDNOMINALAMT", func.col("DebtInstrmAttrbts.TtlIssdNmnlAmt._VALUE")) \
    .withColumn("TOTALISSUEDNOMINALCCY", func.col("DebtInstrmAttrbts.TtlIssdNmnlAmt._Ccy")) \
    .withColumn("MTRTYDT", func.col("DebtInstrmAttrbts.MtrtyDt")) \
    .withColumn("NMNLVALPERUNITAMT", func.col("DebtInstrmAttrbts.NmnlValPerUnit._VALUE")) \
    .withColumn("NMNLVALPERUNITCCY", func.col("DebtInstrmAttrbts.NmnlValPerUnit._Ccy")) \
    .withColumn("INTRSTRATEFXD", func.col("DebtInstrmAttrbts.IntrstRate.Fxd")) \
    .withColumn("INTRSTRATEFLTGIDTP", func.when(func.col("DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.ISIN").isNotNull(), "ISIN")
                                          .when(func.col("DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.Indx").isNotNull(), "Index")
                                          .when(func.col("DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.Nm").isNotNull(), "Name")) \
    .withColumn("INTRSTRATEFLTGID", func.when(func.col("INTRSTRATEFLTGIDTP") == "ISIN" ,func.col("DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.ISIN"))
                                           .when(func.col("INTRSTRATEFLTGIDTP") == "Index", func.col("DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.Indx")) \
                                           .when(func.col("INTRSTRATEFLTGIDTP") == "Name", func.col("DebtInstrmAttrbts.IntrstRate.Fltg.RefRate.Nm"))) \
    .withColumn("INTRSTRATEFLTGTERMVAL", func.col("DebtInstrmAttrbts.IntrstRate.Fltg.Term.Val")) \
    .withColumn("INTRSTRATEFLTGTERMUNIT", func.col("DebtInstrmAttrbts.IntrstRate.Fltg.Term.Unit")) \
    .withColumn("INTRSTRATEFLTGSPRD", func.col("DebtInstrmAttrbts.IntrstRate.Fltg.BsisPtSprd")) \
    .withColumn("DEBTSNRTY", func.col("DebtInstrmAttrbts.DebtSnrty")) \
    .withColumn("XPRYDT", func.col("DerivInstrmAttrbts.XpryDt")) \
    .withColumn("PRICMLTPLR", func.col("DerivInstrmAttrbts.PricMltplr")) \
    .withColumn("OPTNTP", func.col("DerivInstrmAttrbts.OptnTp")) \
    .withColumn("OPTNEXRCSTYLE", func.col("DerivInstrmAttrbts.OptnExrcStyle")) \
    .withColumn("DLVRYTP", func.col("DerivInstrmAttrbts.DlvryTp")) \
    .withColumn("TECH_PBLCTNPRD_FRDT", func.coalesce(*_period_start(df))) \
    .withColumn("TECH_PBLCTNPRD_TODT", func.coalesce(
        func.col("TechAttrbts.PblctnPrd.ToDt"),
        func.col("TechAttrbts.PblctnPrd.FrDtToDt.ToDt"),
        func.col("TechAttrbts.PblctnPrd.Dt"))) \
    .withColumn("TECH_RLVNTCMPTNTAUTHRTY", func.col("TechAttrbts.RlvntCmptntAuthrty")) \
    .withColumn("TECH_RLVNTTRADGVN", func.col("TechAttrbts.RlvntTradgVn")) \
    .select(*ANALYTICS_COLUMNS)

    return df


#: The two schemas the translation is applied to. The names are kept because
#: they say which file the frame came from where it is called.
translate_xml_to_analytics_schema_017_001_02 = translate_xml_to_analytics_schema
translate_xml_to_analytics_schema_036_001_03 = translate_xml_to_analytics_schema


#: What this module offers. The historical database and the compact table of the Silver layer.
__all__ = [
    "ANALYTICS_COLUMNS",
    "COMMIT_TAG_PREFIX",
    "DECIMAL_COLUMNS",
    "PARTITION_PREFIX",
    "REFERENCE_DATE_FORMAT",
    "REWRITING_OPERATIONS",
    "SOURCE_FIELDS",
    "USER_METADATA_CONF",
    "check_publication_period",
    "commit_tag",
    "committed_steps",
    "declare_silver_main_layout",
    "dltins_partitions",
    "dltins_reference_dates",
    "finished_files",
    "format_reference_date",
    "get_analytical_fields",
    "initialise_firds_database",
    "list_reference_dates",
    "migrate_silver_main_to_decimal",
    "parse_reference_date",
    "read_bronze_partition",
    "read_cancellations",
    "read_daily_changes",
    "read_dltins_partition",
    "tagged_commits",
    "translate_xml_to_analytics_schema",
    "translate_xml_to_analytics_schema_017_001_02",
    "translate_xml_to_analytics_schema_036_001_03",
    "update_from_bronze_to_silver_main",
    "update_from_silver_main_to_silver_compact",
    "upsert_from_bronze_daily",
    "watermark_from_commits",
]
