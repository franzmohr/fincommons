"""Construction of the FIRDS historical database.

This module implements section 4.2 of ESMA's *FIRDS Reference Data System --
Instructions on download and use of full, delta and cancellations reference
data files* (ESMA65-8-5014 rev 3, 9 February 2022), which describes how a
historical database is built from one full file (FULINS) and the daily delta
files (DLTINS) that follow it.

The algorithm associates two pieces of bookkeeping with every record:

``valid_from`` / ``valid_to``
    the validity dates of the version, and
``latest_record``
    whether the version is the most recent one received for its instrument.

An instrument is identified by the pair ``(ISIN, TRADGVN)`` -- the ISIN and the
MIC of the trading venue -- and ``valid_from`` is taken from
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
KEY_COLUMNS = ("isin", "trading_venue")

#: Column holding the first day of the validity period of a version.
VALID_FROM = "valid_from"

#: Column holding the last day of the validity period, empty while the version
#: is the one in force.
VALID_TO = "valid_to"

#: Column marking the most recent version received for an instrument.
LATEST_RECORD = "latest_record"

#: Column holding the kind of record a version originates from.
ACTION_TYPE = "action_type"

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
OPERATION = "operation"

#: Column naming the reference date of the delta file a record came from,
#: where the records of several files are planned together; see
#: :func:`prepare_incoming` and :func:`check_file_order`. The records of a
#: single file need not carry it.
FILE_DATE = "file_date"

_OPERATION = "_OPERATION"
_SEQUENCE = "_SEQUENCE"
_RANK = "_RANK"
_OPEN_VALID_FROM = "_open_valid_from"


def initialise_history(full: DataFrame) -> DataFrame:
    """Register the records of a full file as the start of the history.

    This is paragraph 27.a of the instructions: on the day the history is
    started, every record of the FULINS file is registered with its publication
    start date as ``valid_from``, an empty ``valid_to`` and ``latest_record`` set.

    Parameters
    ----------
    full:
        The records of the full file in the analytics schema, carrying at least
        the key columns and ``tech_publication_period_from_date``.

    Returns
    -------
    pyspark.sql.DataFrame
        The initial state of the historical database.
    """
    history = (
        full.withColumn(VALID_FROM, F.to_date(F.col("tech_publication_period_from_date")))
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
    duplicates, as a file imported twice produces, are among those records and
    are dropped the same way.

    Both steps are taken on one window, ordered as the records are applied, so
    that a file costs one shuffle rather than three. A record another of the
    same day supersedes is the one whose successor in that order begins on the
    same day as it does, which is what the first step drops; and because an
    exact duplicate begins on the same day as the record it duplicates, the
    step drops all but the last of a run of them as well. A ``dropDuplicates``
    of its own would shuffle every column of every record to find what this
    already removes.

    The order the window reads the records in is a column of its own rather
    than the expression :func:`_action_rank` returns. Written as the
    expression, each use of the window computes it again under a name of its
    own, and Spark cannot then see that the orders two of them ask for are the
    same one: it sorts the records once per use, within the partitions the one
    exchange has laid out. Computed once and named, the sort is shared as
    well, and the chaining reads the records the first step left without
    laying them out or sorting them again.

    Records of several delta files can be prepared together where they carry
    the reference date of their file in :data:`FILE_DATE`. That date is then
    ordered on between the start date and the record type, which is what
    applying the files one after another amounts to: a record of a later file
    that begins on the same day as one of an earlier file replaces it, as
    :func:`plan_changes` has a record replace the version in force that began
    on its own day, whichever element either was reported in. Ordering on the
    start date first is only the order the files were published in where no
    file begins a version before one an earlier file began, which
    :func:`check_file_order` refuses.
    """
    incoming = incoming.withColumn(
        VALID_FROM, F.to_date(F.col("tech_publication_period_from_date"))
    ).withColumn(_RANK, _action_rank(F.col(ACTION_TYPE)))
    order = [F.col(VALID_FROM)]
    if FILE_DATE in incoming.columns:
        order.append(F.col(FILE_DATE))
    order.append(F.col(_RANK))
    ordering = Window.partitionBy(*KEY_COLUMNS).orderBy(*order)
    successor = F.lead(F.col(VALID_FROM)).over(ordering)
    # Null-safely, so that a record stating no start date is kept and left to
    # fincommons.firds.silver.check_publication_period to refuse. Comparing
    # with == would make the comparison null and drop the record here, and a
    # file would be applied with it silently missing.
    incoming = (
        incoming.withColumn("_successor", successor)
        .where(
            F.col("_successor").isNull()
            | ~F.col("_successor").eqNullSafe(F.col(VALID_FROM))
        )
        .drop("_successor")
    )
    # Both in one step, so that they are one reading of the window rather
    # than two.
    incoming = incoming.withColumns(
        {_SEQUENCE: F.row_number().over(ordering), "_successor": successor}
    )
    incoming = incoming.withColumn(
        VALID_TO,
        F.when(
            F.col("_successor").isNotNull(), F.date_sub(F.col("_successor"), 1)
        ).otherwise(F.lit(None).cast("date")),
    )
    incoming = incoming.withColumn(
        LATEST_RECORD, F.col("_successor").isNull()
    )
    return incoming.drop("_successor", _RANK)


def check_file_order(incoming: DataFrame, source: str) -> DataFrame:
    """Refuse delta files planned together where a later one backdates an earlier one.

    Applied one after another, the first record a file carries for an
    instrument meets the version an earlier file left in force, and
    :func:`check_supersession` refuses it where it begins before that version
    does. Planned together, the earlier file's version is not yet in the
    history for the check to meet, and :func:`prepare_incoming` would order the
    two records by their start dates and chain them the other way round. The
    history would come out differently from the one the files build applied in
    turn, and without an error. This makes the same refusal across the files
    of a batch: no file may carry a record for an instrument that begins
    before a record an earlier file of the batch carries for it.

    Parameters
    ----------
    incoming:
        The records of several delta files, in the analytics schema, carrying
        ``tech_publication_period_from_date`` and the reference date of their
        file in :data:`FILE_DATE`.
    source:
        What was being applied, for the message.

    Returns
    -------
    pyspark.sql.DataFrame
        ``incoming`` unchanged, so the check can stand in a chain.

    Raises
    ------
    ValueError
        If a record begins before one an earlier file of the batch carries
        for its instrument.
    """
    starts = F.to_date(F.col("tech_publication_period_from_date"))
    per_file = incoming.groupBy(*KEY_COLUMNS, FILE_DATE).agg(
        F.min(starts).alias("_first"), F.max(starts).alias("_last")
    )
    earlier = (
        Window.partitionBy(*KEY_COLUMNS)
        .orderBy(F.col(FILE_DATE))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    backdated = per_file.withColumn(
        "_earlier", F.max(F.col("_last")).over(earlier)
    ).where(F.col("_first") < F.col("_earlier"))
    count = backdated.count()
    if not count:
        return incoming

    columns = [*KEY_COLUMNS, FILE_DATE, "_first", "_earlier"]
    examples = [tuple(row) for row in backdated.select(*columns).limit(3).collect()]
    raise ValueError(
        f"{count} records of {source} begin before a record an earlier delta "
        f"file of the batch carries for their instrument. Applied one file "
        f"after another, the later record would be refused for beginning "
        f"before the version in force; planned together it would be chained "
        f"in front of it instead. Examples, as ({', '.join(KEY_COLUMNS)}, "
        f"file, begins, earlier file begins): {examples}."
    )


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
    ``valid_to``. Inserting such a record would give the instrument two versions
    in force, which makes ``latest_record`` ambiguous and, on the next delta
    file that touches the instrument, matches the merge against two rows at
    once and stops the update with an error until the table is repaired.

    Parameters
    ----------
    history:
        The historical database as it stands before the file is applied.
    incoming:
        The records of one delta file, in the analytics schema and carrying
        ``action_type`` and ``tech_publication_period_from_date``.

    Returns
    -------
    pyspark.sql.DataFrame
        The incoming records with their validity bookkeeping and a column
        ``_OPERATION`` holding the operation each of them implies.
    """
    # The keys are taken before the records are prepared, so that the window
    # of prepare_incoming is not computed a second time to find them. The
    # preparation only drops records, never changes a key, so the two frames
    # name the same instruments.
    keys = incoming.select(*KEY_COLUMNS).distinct()
    incoming = prepare_incoming(incoming)

    # Only an open version whose instrument the file carries a record for can
    # meet the join below, so the open versions are cut down to those before
    # it. The point is which join Spark is then left with: the whole history
    # is far too large to be the broadcast side of the left join, so without
    # this it is sorted and shuffled in full, every file, to be met by one
    # day's records. The semi-join broadcasts the day's keys instead, which a
    # left semi-join may do, so the history is read once -- three columns of
    # it -- probed against a hash table, and never shuffled. What survives is
    # at most one row per key of the file and is small enough to broadcast in
    # turn, which leaves the plan of a file with no shuffle of the history in
    # it at all.
    in_force = (
        history.where(F.col(VALID_TO).isNull())
        .select(*KEY_COLUMNS, F.col(VALID_FROM).alias(_OPEN_VALID_FROM))
        .join(F.broadcast(keys), on=list(KEY_COLUMNS), how="left_semi")
    )
    planned = incoming.join(
        F.broadcast(in_force), on=list(KEY_COLUMNS), how="left"
    )

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
    day before its own ``valid_from`` and so is valid on no day at all, while the
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

    The columns are the key of the instrument and the ``valid_to`` its open
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
    row that closes a version, ``valid_to`` already holds the day the version is
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
    "operation",
    "REPLACE",
    "VALID_FROM",
    "VALID_TO",
    "FILE_DATE",
    "apply_changes",
    "check_file_order",
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
