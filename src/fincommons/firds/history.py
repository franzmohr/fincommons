"""Construction of the FIRDS historical database.

This module implements section 4.2 of ESMA's *FIRDS Reference Data System --
Instructions on download and use of full, delta and cancellations reference
data files* (ESMA65-8-5014 rev 3, 9 February 2022), which describes how a
historical database is built from one full file (FULINS) and the daily delta
files (DLTINS) that follow it.

The algorithm associates two pieces of bookkeeping with every record:

``VLDFROM`` / ``VLDTO``
    the validity dates of the version, and
``LATESTRECORD``
    whether the version is the most recent one received for its instrument.

An instrument is identified by the pair ``(ISIN, TRADGVN)`` -- the ISIN and the
MIC of the trading venue -- and ``VLDFROM`` is taken from
``TechAttrbts/PblctnPrd/FrDt`` of the record, as the instructions prescribe.

The logic is expressed as transformations of data frames and does not depend on
how the history is stored. That keeps it testable against the worked example
the instructions themselves give, and it lets the storage layer choose between
rewriting the table and applying the changes with a merge.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#: Columns identifying an instrument on a trading venue.
KEY_COLUMNS = ("ISIN", "TRADGVN")

#: Column holding the first day of the validity period of a version.
VALID_FROM = "VLDFROM"

#: Column holding the last day of the validity period, empty while the version
#: is the one in force.
VALID_TO = "VLDTO"

#: Column marking the most recent version received for an instrument.
LATEST_RECORD = "LATESTRECORD"

#: Column holding the kind of record a version originates from.
ACTION_TYPE = "ACTNTP"

#: The element of the delta file a record was reported in. The order is the
#: order in which several records for the same instrument in the same file are
#: applied: an instrument is created before it is modified, and modified before
#: it is terminated.
ACTION_TYPES = ("NewRcrd", "ModfdRcrd", "CancRcrd", "TermntdRcrd")

#: Operations an incoming record can imply for the history.
INSERT = "INSERT"
CLOSE_AND_INSERT = "CLOSE_AND_INSERT"
REPLACE = "REPLACE"

#: Column that :func:`records_to_change` tags its rows with, naming the
#: operation each of them implies for the version in force.
OPERATION = "OPERATION"

_OPERATION = "_OPERATION"
_SEQUENCE = "_SEQUENCE"
_OPEN_VALID_FROM = "_OPEN_VLDFROM"


def initialise_history(full: DataFrame) -> DataFrame:
    """Register the records of a full file as the start of the history.

    This is paragraph 27.a of the instructions: on the day the history is
    started, every record of the FULINS file is registered with its publication
    start date as ``VLDFROM``, an empty ``VLDTO`` and ``LATESTRECORD`` set.

    Parameters
    ----------
    full:
        The records of the full file in the analytics schema, carrying at least
        the key columns and ``TECH_PBLCTNPRD_FRDT``.

    Returns
    -------
    pyspark.sql.DataFrame
        The initial state of the historical database.
    """
    history = (
        full.withColumn(VALID_FROM, F.to_date(F.col("TECH_PBLCTNPRD_FRDT")))
        .withColumn(VALID_TO, F.lit(None).cast("date"))
        .withColumn(LATEST_RECORD, F.lit(True))
        .withColumn(ACTION_TYPE, F.lit("FULINS"))
    )
    # A full file states the version in force once per instrument. Should it
    # nevertheless contain several, the most recent one is the version in
    # force and the others are dropped, because the file carries no
    # information about when the earlier ones ceased to be valid.
    ranking = Window.partitionBy(*KEY_COLUMNS).orderBy(F.col(VALID_FROM).desc())
    return (
        history.withColumn("_rank", F.row_number().over(ranking))
        .where(F.col("_rank") == F.lit(1))
        .drop("_rank")
    )


def prepare_incoming(incoming: DataFrame) -> DataFrame:
    """Attach the validity bookkeeping to the records of one delta file.

    Records for the same instrument that arrive in the same file are chained:
    each of them is valid until the day before the next one begins, and only
    the last of them is the latest record. The instructions describe the
    processing of one record at a time; chaining is what that sequential
    description amounts to when a file happens to carry several changes for the
    same instrument.

    Records that begin on the same day cannot be chained, because the earlier
    of them would end the day before it begins. Of those, only the last in the
    order of :data:`ACTION_TYPES` is kept: it is what the instrument looked
    like at the end of that day. Should two records of the same kind begin on
    the same day, which one is kept is not defined by the file. Exact
    duplicates, as a file imported twice produces, are dropped first.
    """
    incoming = incoming.dropDuplicates().withColumn(
        VALID_FROM, F.to_date(F.col("TECH_PBLCTNPRD_FRDT"))
    )
    same_day = Window.partitionBy(*KEY_COLUMNS, VALID_FROM).orderBy(
        _action_rank(F.col(ACTION_TYPE)).desc()
    )
    incoming = (
        incoming.withColumn("_same_day", F.row_number().over(same_day))
        .where(F.col("_same_day") == F.lit(1))
        .drop("_same_day")
    )
    ordering = Window.partitionBy(*KEY_COLUMNS).orderBy(
        F.col(VALID_FROM),
        _action_rank(F.col(ACTION_TYPE)),
    )
    incoming = incoming.withColumn(_SEQUENCE, F.row_number().over(ordering))
    successor = F.lead(F.col(VALID_FROM)).over(ordering)
    incoming = incoming.withColumn("_successor", successor)
    incoming = incoming.withColumn(
        VALID_TO,
        F.when(
            F.col("_successor").isNotNull(), F.date_sub(F.col("_successor"), 1)
        ).otherwise(F.lit(None).cast("date")),
    )
    incoming = incoming.withColumn(
        LATEST_RECORD, F.col("_successor").isNull()
    )
    return incoming.drop("_successor")


def _action_rank(action: F.Column) -> F.Column:
    """Return the position of an action type in :data:`ACTION_TYPES`."""
    rank = F.lit(len(ACTION_TYPES))
    for position, name in reversed(list(enumerate(ACTION_TYPES))):
        rank = F.when(action == F.lit(name), F.lit(position)).otherwise(rank)
    return rank


def plan_changes(history: DataFrame, incoming: DataFrame) -> DataFrame:
    """Decide what each record of a delta file does to the history.

    Every incoming record is classified into one of three operations, which
    together are paragraph 27.b of the instructions:

    What an incoming record does is decided by the version in force rather
    than by the element it was reported in, because what has to hold
    afterwards is that an instrument has one version in force at a time:

    ``INSERT``
        the record is added and nothing in the history is touched. This is the
        case where no version of the instrument is in force -- a record
        reported in ``<NewRcrd>``, and paragraph 27.b.iii.2, an instrument
        terminated but reported late.
    ``CLOSE_AND_INSERT``
        the version in force is closed on the day before the incoming record
        starts and is no longer the latest record, and the incoming record is
        added. This is the case for ``<ModfdRcrd>`` and ``<CancRcrd>``, i.e.
        paragraph 27.b.ii, and for paragraph 27.b.iii.3, a correction of an
        instrument that was already terminated.
    ``REPLACE``
        the version in force is replaced by the incoming record. This is
        paragraph 27.b.iii.4, an instrument terminated in the ordinary way,
        and it is also what a record starting on the same day as the version
        in force amounts to, whichever element it was reported in: closing the
        version in force on the day before it began would leave a version
        valid for no day at all.

    A record reported in ``<NewRcrd>`` for an instrument that already has a
    version in force is treated the same way, i.e. it supersedes that version
    rather than being added beside it. The instructions do not describe that
    case, since a new instrument is not supposed to exist already; ESMA can
    report one all the same, for an instrument admitted again after it was
    terminated, and a termination stays in force because it carries no
    ``VLDTO``. Inserting such a record would give the instrument two versions
    in force, which makes ``LATESTRECORD`` ambiguous and, on the next delta
    file that touches the instrument, matches the merge against two rows at
    once and stops the update with an error until the table is repaired.

    Parameters
    ----------
    history:
        The historical database as it stands before the file is applied.
    incoming:
        The records of one delta file, in the analytics schema and carrying
        ``ACTNTP`` and ``TECH_PBLCTNPRD_FRDT``.

    Returns
    -------
    pyspark.sql.DataFrame
        The incoming records with their validity bookkeeping and a column
        ``_OPERATION`` holding the operation each of them implies.
    """
    incoming = prepare_incoming(incoming)
    in_force = history.where(F.col(VALID_TO).isNull()).select(
        *KEY_COLUMNS, F.col(VALID_FROM).alias(_OPEN_VALID_FROM)
    )
    planned = incoming.join(in_force, on=list(KEY_COLUMNS), how="left")

    first = F.col(_SEQUENCE) == F.lit(1)
    open_version = F.col(_OPEN_VALID_FROM)
    operation = (
        # Only the first record for an instrument meets the version that was
        # in force before the file; the later ones meet its predecessor in the
        # same file, which prepare_incoming has already closed.
        F.when(~first, F.lit(INSERT))
        # Nothing is in force, so there is nothing to supersede.
        .when(open_version.isNull(), F.lit(INSERT))
        # The version in force began on the day the incoming record begins.
        .when(open_version == F.col(VALID_FROM), F.lit(REPLACE))
        # A version in force that began earlier is closed the day before.
        .otherwise(F.lit(CLOSE_AND_INSERT))
    )
    return planned.withColumn(_OPERATION, operation)


def check_supersession(planned: DataFrame, source: str) -> DataFrame:
    """Refuse a record that begins before the version it would supersede.

    Paragraph 27.b.ii closes the version in force on the day before the
    incoming record begins, which says what to do on the assumption that the
    incoming record begins later. Where it begins earlier the arithmetic still
    goes through and produces nonsense: the superseded version is closed on a
    day before its own ``VLDFROM`` and so is valid on no day at all, while the
    record that closed it becomes the version in force with a start date
    behind the one it replaced. A point-in-time query then answers for every
    day after the fault with the older record.

    Nothing fails at that point, which is what makes it worth refusing here,
    and it is the same reasoning as in
    :func:`fincommons.firds.silver.check_publication_period`: a history that
    goes on building around a fault costs more to repair than a run that
    stops on it.

    Only the first record a file carries for an instrument meets the version
    that was in force before the file; :func:`plan_changes` classifies every
    later one as ``INSERT``, and those are chained among themselves by
    :func:`prepare_incoming`. The check is therefore made on that record
    alone.

    Parameters
    ----------
    planned:
        The output of :func:`plan_changes`.
    source:
        What was being applied, for the message.

    Returns
    -------
    pyspark.sql.DataFrame
        ``planned`` unchanged, so the check can stand in a chain.

    Raises
    ------
    ValueError
        If any record begins before the version in force for its instrument.
    """
    backdated = planned.where(
        (F.col(_SEQUENCE) == F.lit(1))
        & F.col(_OPEN_VALID_FROM).isNotNull()
        & (F.col(_OPEN_VALID_FROM) > F.col(VALID_FROM))
    )
    count = backdated.count()
    if not count:
        return planned

    columns = [
        name for name in (*KEY_COLUMNS, ACTION_TYPE, VALID_FROM, _OPEN_VALID_FROM)
        if name in backdated.columns
    ]
    examples = [tuple(row) for row in backdated.select(*columns).limit(3).collect()]
    raise ValueError(
        f"{count} records of {source} begin before the version in force for "
        f"their instrument. Closing that version on the day before the "
        f"incoming record begins would leave it valid on no day, and the "
        f"history would answer for every later day with the older record. "
        f"Examples, as ({', '.join(columns)}): {examples}."
    )


def records_to_close(planned: DataFrame) -> DataFrame:
    """Return the versions that the file supersedes, with their closing date.

    The columns are the key of the instrument and the ``VLDTO`` its open
    version has to be set to, which is the day before the incoming record
    begins.
    """
    return (
        planned.where(F.col(_OPERATION) == F.lit(CLOSE_AND_INSERT))
        .select(
            *KEY_COLUMNS,
            F.date_sub(F.col(VALID_FROM), 1).alias(VALID_TO),
        )
    )


def records_to_replace(planned: DataFrame, columns) -> DataFrame:
    """Return the records that replace the version in force."""
    return planned.where(F.col(_OPERATION) == F.lit(REPLACE)).select(*columns)


def records_to_insert(planned: DataFrame, columns) -> DataFrame:
    """Return the records that are added to the history."""
    return planned.where(
        F.col(_OPERATION).isin([INSERT, CLOSE_AND_INSERT])
    ).select(*columns)


def records_to_change(planned: DataFrame, columns) -> DataFrame:
    """Return the changes to the versions in force, tagged with the operation.

    The rows of :func:`records_to_close` and :func:`records_to_replace` in one
    frame, carrying every column of ``columns`` and :data:`OPERATION`. For a
    row that closes a version, ``VLDTO`` already holds the day the version is
    closed on, so a caller applies either operation by reading the columns the
    tag names.

    Both operations concern the version in force of an instrument, and an
    instrument has at most one row here: only the first record a file carries
    for an instrument meets the version that was in force before the file, and
    :func:`plan_changes` classifies every later one as ``INSERT``. A storage
    layer can therefore apply both in a single pass over the table, rather than
    one pass per operation.
    """
    return (
        planned.where(F.col(_OPERATION).isin([CLOSE_AND_INSERT, REPLACE]))
        .withColumn(
            VALID_TO,
            F.when(
                F.col(_OPERATION) == F.lit(CLOSE_AND_INSERT),
                F.date_sub(F.col(VALID_FROM), 1),
            ).otherwise(F.col(VALID_TO)),
        )
        .select(*columns, F.col(_OPERATION).alias(OPERATION))
    )


def apply_changes(history: DataFrame, incoming: DataFrame) -> DataFrame:
    """Apply one delta file to the history and return the new history.

    This rewrites the whole history, which is what makes it usable as the
    reference implementation of :func:`plan_changes` and appropriate for
    moderate volumes. The Silver layer applies the same plan with a merge
    instead, so that only the affected rows are written.

    Parameters
    ----------
    history:
        The historical database as it stands before the file is applied.
    incoming:
        The records of one delta file.

    Returns
    -------
    pyspark.sql.DataFrame
        The historical database after the file has been applied.
    """
    columns = history.columns
    planned = plan_changes(history, incoming).cache()
    check_supersession(planned, "the file")

    closing = records_to_close(planned).withColumnRenamed(VALID_TO, "_new_vldto")
    replacing = records_to_replace(planned, columns)
    inserting = records_to_insert(planned, columns)

    # The version in force is the only row a file can change; every other row
    # of the history is carried over untouched.
    open_rows = history.where(F.col(VALID_TO).isNull())
    closed_rows = history.where(F.col(VALID_TO).isNotNull())

    replaced_keys = replacing.select(*KEY_COLUMNS).distinct()
    surviving = open_rows.join(replaced_keys, on=list(KEY_COLUMNS), how="left_anti")

    updated = (
        surviving.join(closing, on=list(KEY_COLUMNS), how="left")
        .withColumn(
            LATEST_RECORD,
            F.when(F.col("_new_vldto").isNotNull(), F.lit(False)).otherwise(
                F.col(LATEST_RECORD)
            ),
        )
        .withColumn(
            VALID_TO, F.coalesce(F.col("_new_vldto"), F.col(VALID_TO))
        )
        .drop("_new_vldto")
        .select(*columns)
    )

    return (
        closed_rows.select(*columns)
        .unionByName(updated)
        .unionByName(replacing)
        .unionByName(inserting)
    )


def latest_records(history: DataFrame) -> DataFrame:
    """Return the most recent version of every instrument.

    This is the first query of section 4.3 of the instructions.
    """
    return history.where(F.col(LATEST_RECORD))


def records_as_of(history: DataFrame, day) -> DataFrame:
    """Return the version of every instrument that was in force on ``day``.

    This is the third query of section 4.3 of the instructions: the reference
    data as it was known for a day in the past.
    """
    reference = F.to_date(F.lit(day))
    return history.where(
        (F.col(VALID_FROM) <= reference)
        & (F.col(VALID_TO).isNull() | (F.col(VALID_TO) >= reference))
    )


__all__ = [
    "ACTION_TYPES",
    "CLOSE_AND_INSERT",
    "INSERT",
    "KEY_COLUMNS",
    "LATEST_RECORD",
    "OPERATION",
    "REPLACE",
    "VALID_FROM",
    "VALID_TO",
    "apply_changes",
    "check_supersession",
    "initialise_history",
    "latest_records",
    "plan_changes",
    "prepare_incoming",
    "records_as_of",
    "records_to_change",
    "records_to_close",
    "records_to_insert",
    "records_to_replace",
]
