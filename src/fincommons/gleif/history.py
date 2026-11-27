"""The history of the GLEIF data: one version of a record per state it held.

The history of a level is a table of versions, as the FIRDS history of
:mod:`fincommons.firds.history` is, and it is read the same way:

``valid_from`` / ``valid_to``
    the first and the last day the version held, ``valid_to`` empty while it
    still does;
``latest_record``
    whether the version is the most recent one known for its key.

A record is identified by its key -- the LEI on Level 1, and on Level 2 the
two LEIs of a relationship and its type -- and a new version begins when
anything the history keeps about it changes. ``record_hash`` is a digest of
those columns, so a change is a difference of two hashes.

**When a version begins.** GLEIF records when a record was last changed,
``Registration.LastUpdateDate``, and that day is where a version begins. It
does not depend on how often the history was fed: a record that changed on the
3rd of a month begins its version on the 3rd whether the change was read the
next morning from a delta file or at the end of the month from a full copy.
Where the date is not after the version it supersedes -- a change GLEIF made
without the issuer touching the record, such as a conformity flag -- the day of
the publication it was first read in is taken instead, and where even that is
not after it, the version in force is replaced rather than a second version
being opened on the same day, as FIRDS does with two records of one day.

**What is taken as the end of a record.** A relationship reported as deleted
by a delta file, or no longer in a full copy, has its version closed on the
day before the deletion, or before the publication that no longer lists it,
and keeps ``latest_record``: it is the last state known, and it ended. GLEIF
does not delete LEIs, but an LEI missing from a full copy is closed the same
way rather than left open in a history that would then say something the
publisher no longer does.

**What a history before its start says.** The history is complete from the
publication it was started from. A record that is older than that starts with
the version it had then, from its last update before the start, and what it
was before that update is not known; an entity created later has no version
before its creation.

The logic is expressed as transformations of data frames, independently of
how the history is stored, which is what makes it testable without a data
lake; :mod:`fincommons.gleif.silver` applies it to a Delta table with a merge.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

#: The columns that identify a record of each level.
KEYS = {
    "level_1": ("lei",),
    "level_2": ("start_lei", "end_lei", "relationship_type"),
}

VALID_FROM = "valid_from"
VALID_TO = "valid_to"
LATEST_RECORD = "latest_record"
RECORD_HASH = "record_hash"

#: The publication the version was read from, ``YYYYMMDD-HHMM``.
PUBLICATION = "publication"

#: The bookkeeping columns every version carries beside what it says.
BOOKKEEPING = (VALID_FROM, VALID_TO, LATEST_RECORD, RECORD_HASH, PUBLICATION)

#: Columns kept on a version but not part of what it says, so that a change of
#: them alone opens no version: the day of the last update moves whenever the
#: issuer's local operating unit touches a record, and where nothing else
#: changed nothing about the entity did. A relationship's deletion is an end
#: and not a state. The conformity flag is GLEIF's own verdict on the record's
#: data quality, which GLEIF recomputes without the record being updated, so
#: that the delta files -- the records updated in the day -- do not carry its
#: changes: a history fed by them was out on 73,335 of 3.5 million flags after
#: a month. It is kept as it was read with the version.
NOT_HASHED = ("last_update_date", "deleted_at", "conformity_flag")

#: What a planned change does to the history.
INSERT = "INSERT"
CLOSE = "CLOSE"
CLOSE_AND_INSERT = "CLOSE_AND_INSERT"
REPLACE = "REPLACE"
SUPERSEDE = "SUPERSEDE"

OPERATION = "_operation"

#: The ``valid_from`` of the version in the history a planned change applies
#: to, which the merge matches it on together with the key; empty for a row
#: that is inserted.
TARGET_VALID_FROM = "_target_valid_from"


def _date(column):
    """The day of a timestamp GLEIF writes as ISO 8601 text."""
    return F.to_date(F.substring(F.col(column), 1, 10))


def _moment(column):
    """A timestamp GLEIF writes as ISO 8601 text, to the second.

    GLEIF writes some with milliseconds and some without, all in UTC.
    """
    return F.to_timestamp(F.substring(F.col(column), 1, 19),
                          "yyyy-MM-dd'T'HH:mm:ss")


def level_1_from_bronze(bronze: DataFrame, names) -> DataFrame:
    """The records of a Level 1 file, in the names of the Silver layer.

    Parameters
    ----------
    bronze:
        The file as the Bronze layer holds it, every column text.
    names:
        :data:`fincommons.gleif.common.LEVEL_1_COLUMNS`.
    """
    return bronze.select(*[F.col(f"`{source}`").alias(target)
                           for source, target in names.items()])


def level_2_from_bronze(bronze: DataFrame, groups: int) -> DataFrame:
    """The relationships of a Level 2 file, in the names of the Silver layer.

    A relationship states up to ``groups`` periods, each typed: the period of
    the relationship itself, the accounting period its consolidation rests on,
    and the period of the filing that documents it. They come in no fixed
    order -- the relationship's own is the first in most records and the second
    or third in others -- so each is found by its type rather than its place.
    A quantifier, where one is stated, is the share held, and the first one is
    kept.
    """
    def typed(part, kind):
        expression = F.lit(None).cast("string")
        for n in range(groups, 0, -1):
            expression = F.when(
                F.col(f"`Relationship.Period.{n}.periodType`") == kind,
                F.col(f"`Relationship.Period.{n}.{part}`"),
            ).otherwise(expression)
        return expression

    return bronze.select(
        F.col("`Relationship.StartNode.NodeID`").alias("start_lei"),
        F.col("`Relationship.EndNode.NodeID`").alias("end_lei"),
        F.col("`Relationship.RelationshipType`").alias("relationship_type"),
        F.col("`Relationship.RelationshipStatus`").alias("relationship_status"),
        F.col("`Relationship.StartNode.NodeIDType`").alias("start_node_id_type"),
        F.col("`Relationship.EndNode.NodeIDType`").alias("end_node_id_type"),
        typed("startDate", "RELATIONSHIP_PERIOD").alias("relationship_start"),
        typed("endDate", "RELATIONSHIP_PERIOD").alias("relationship_end"),
        typed("startDate", "ACCOUNTING_PERIOD").alias("accounting_period_start"),
        typed("endDate", "ACCOUNTING_PERIOD").alias("accounting_period_end"),
        F.col("`Relationship.Quantifiers.1.MeasurementMethod`").alias("measurement_method"),
        F.col("`Relationship.Quantifiers.1.QuantifierAmount`").alias("quantifier_amount"),
        F.col("`Relationship.Quantifiers.1.QuantifierUnits`").alias("quantifier_units"),
        F.col("`Registration.InitialRegistrationDate`").alias("initial_registration_date"),
        F.col("`Registration.LastUpdateDate`").alias("last_update_date"),
        F.col("`Registration.RegistrationStatus`").alias("registration_status"),
        F.col("`Registration.NextRenewalDate`").alias("next_renewal_date"),
        F.col("`Registration.ManagingLOU`").alias("managing_lou"),
        F.col("`Registration.ValidationSources`").alias("validation_sources"),
        F.col("`Registration.ValidationDocuments`").alias("validation_documents"),
        F.col("`DeletedAt`").alias("deleted_at"),
    )


def content_columns(frame: DataFrame, level: str):
    """The columns of a version that say something about the record."""
    return [c for c in frame.columns
            if c not in BOOKKEEPING and c not in (OPERATION, TARGET_VALID_FROM)]


def prepare_incoming(records: DataFrame, level: str, publication: str,
                     published) -> DataFrame:
    """The records of one file, one per key, ready to be set against the history.

    A full copy holds one record per key. A delta file may hold two where a
    record changed twice within its day; the one updated last is kept, and the
    publication's own record order breaks a tie the same way on every run.

    Parameters
    ----------
    records:
        From :func:`level_1_from_bronze` or :func:`level_2_from_bronze`.
    level:
        ``"level_1"`` or ``"level_2"``.
    publication:
        The publication the file belongs to, ``YYYYMMDD-HHMM``.
    published:
        The day of the publication, a :class:`datetime.date`.
    """
    keys = KEYS[level]
    content = content_columns(records, level)
    hashed = [c for c in content if c not in NOT_HASHED]
    from pyspark.sql.window import Window

    order = Window.partitionBy(*keys).orderBy(
        F.col("last_update_date").desc_nulls_last(),
        *[F.col(c).asc_nulls_last() for c in hashed])
    return (
        records
        .where(F.col(keys[0]).isNotNull())
        .withColumn("_rank", F.row_number().over(order))
        .where(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn(RECORD_HASH, F.sha2(F.to_json(F.struct(*hashed)), 256))
        .withColumn(VALID_FROM, F.coalesce(_date("last_update_date"),
                                           F.lit(published)))
        .withColumn("_published", F.lit(published).cast("date"))
        .withColumn(PUBLICATION, F.lit(publication))
    )


def initialise_history(incoming: DataFrame) -> DataFrame:
    """The history begun from one full copy: every record an open version.

    A record reported as deleted is not taken in; there is nothing of it to
    begin a history with.
    """
    deleted = (F.col("deleted_at").isNotNull()
               if "deleted_at" in incoming.columns else F.lit(False))
    return (incoming.where(~deleted)
            .withColumn(VALID_TO, F.lit(None).cast("date"))
            .withColumn(LATEST_RECORD, F.lit(True))
            .drop("_published"))


def plan_changes(latest: DataFrame, incoming: DataFrame, level: str,
                 full: bool) -> DataFrame:
    """What one file changes in the history, as rows a merge can apply.

    Parameters
    ----------
    latest:
        The latest version of every key in the history, open or closed.
    incoming:
        From :func:`prepare_incoming`.
    level:
        ``"level_1"`` or ``"level_2"``.
    full:
        Whether the file is a full copy. A key whose version is open and that
        a full copy no longer lists is closed; a delta file lists only what
        changed, and says nothing of the rest.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per change, with :data:`OPERATION` and
        :data:`TARGET_VALID_FROM`. ``CLOSE`` and ``SUPERSEDE`` rows name the
        version they apply to and carry the ``valid_to`` and the
        ``latest_record`` it is to be given; ``REPLACE`` rows carry the content
        that replaces it; ``INSERT`` rows are new versions.
    """
    keys = list(KEYS[level])
    content = content_columns(incoming.drop("_published"), level)
    deleted_col = "deleted_at" in incoming.columns

    old = latest.select(
        *keys,
        F.col(VALID_FROM).alias("_old_from"),
        F.col(VALID_TO).alias("_old_to"),
        F.col(RECORD_HASH).alias("_old_hash"),
        _moment("last_update_date").alias("_old_update"),
    )
    joined = incoming.join(old, keys, "left")

    deleted = F.col("deleted_at").isNotNull() if deleted_col else F.lit(False)
    known = F.col("_old_from").isNotNull()
    is_open = known & F.col("_old_to").isNull()
    changed = F.col(RECORD_HASH) != F.col("_old_hash")
    reopened = known & F.col("_old_to").isNotNull() & ~deleted

    # The day the new version begins: the day of its last update where the
    # update is later than the one the version it follows was read at, and
    # the day it was published where it is not -- a change GLEIF made without
    # the record being updated. An update on the day the version in force
    # began replaces that version below, rather than opening a second one.
    # A record reported again after it ended begins no earlier than the day
    # after that end, so that the two never overlap.
    advanced = F.col("_old_update").isNull() \
        | (_moment("last_update_date") > F.col("_old_update"))
    start = F.when(known & ~advanced, F.col("_published")) \
        .otherwise(F.col(VALID_FROM))
    start = F.when(reopened, F.greatest(start, F.date_add(F.col("_old_to"), 1))) \
        .otherwise(start)

    planned = joined.withColumn(VALID_FROM, start).withColumn(
        OPERATION,
        F.when(deleted & is_open, F.lit(CLOSE))
        .when(deleted, F.lit(None))
        .when(~known, F.lit(INSERT))
        .when(reopened, F.lit(CLOSE_AND_INSERT))
        .when(~changed, F.lit(None))
        .when(F.col(VALID_FROM) <= F.col("_old_from"), F.lit(REPLACE))
        .otherwise(F.lit(CLOSE_AND_INSERT)),
    ).where(F.col(OPERATION).isNotNull())

    def end_before(day):
        # The last day of a version is the day before the one that ends it,
        # and never before its own first day.
        return F.greatest(F.date_sub(day, 1), F.col("_old_from"))

    deleted_day = _date("deleted_at") if deleted_col else F.lit(None).cast("date")
    closes = planned.where(F.col(OPERATION) == CLOSE).select(
        *keys,
        F.col("_old_from").alias(TARGET_VALID_FROM),
        F.lit(CLOSE).alias(OPERATION),
        end_before(F.coalesce(deleted_day, F.col("_published"))).alias(VALID_TO),
        F.lit(True).alias(LATEST_RECORD),
    )
    # The version a new one follows: closed the day before it begins where it
    # was open, and no longer the latest either way.
    supersedes = planned.where(F.col(OPERATION) == CLOSE_AND_INSERT).select(
        *keys,
        F.col("_old_from").alias(TARGET_VALID_FROM),
        F.lit(SUPERSEDE).alias(OPERATION),
        F.coalesce(F.col("_old_to"), end_before(F.col(VALID_FROM))).alias(VALID_TO),
        F.lit(False).alias(LATEST_RECORD),
    )
    version_columns = [*content, RECORD_HASH, PUBLICATION]
    replaces = planned.where(F.col(OPERATION) == REPLACE).select(
        *version_columns,
        F.col("_old_from").alias(TARGET_VALID_FROM),
        F.lit(REPLACE).alias(OPERATION),
    )
    inserts = planned.where(F.col(OPERATION).isin(INSERT, CLOSE_AND_INSERT)).select(
        *version_columns,
        F.col(VALID_FROM),
        F.lit(None).cast("date").alias(TARGET_VALID_FROM),
        F.lit(INSERT).alias(OPERATION),
        F.lit(None).cast("date").alias(VALID_TO),
        F.lit(True).alias(LATEST_RECORD),
    )

    changes = [closes, supersedes, replaces, inserts]
    if full:
        published = incoming.select(F.max("_published").alias("_day"))
        missing = (latest.where(F.col(VALID_TO).isNull())
                   .join(incoming.select(*keys), keys, "left_anti")
                   .crossJoin(published)
                   .select(
                       *keys,
                       F.col(VALID_FROM).alias(TARGET_VALID_FROM),
                       F.lit(CLOSE).alias(OPERATION),
                       F.greatest(F.date_sub(F.col("_day"), 1),
                                  F.col(VALID_FROM)).alias(VALID_TO),
                       F.lit(True).alias(LATEST_RECORD),
                   ))
        changes.append(missing)

    result = changes[0]
    for frame in changes[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


def apply_changes(history: DataFrame, changes: DataFrame, level: str) -> DataFrame:
    """The history with a plan applied, as a new frame.

    The storage applies the same plan with a merge; this is the definition the
    merge is tested against, and what a history held in memory is updated by.
    """
    keys = list(KEYS[level])
    target = history.alias("tgt")
    updates = changes.where(F.col(OPERATION) != INSERT).alias("src")
    on = [F.col(f"tgt.{k}") == F.col(f"src.{k}") for k in keys]
    on.append(F.col(f"tgt.{VALID_FROM}") == F.col(f"src.{TARGET_VALID_FROM}"))
    condition = on[0]
    for clause in on[1:]:
        condition = condition & clause

    joined = target.join(updates, condition, "left")
    operation = F.col(f"src.{OPERATION}")
    columns = []
    for name in history.columns:
        if name in (VALID_TO, LATEST_RECORD):
            value = F.when(operation.isin(CLOSE, SUPERSEDE), F.col(f"src.{name}")) \
                .otherwise(F.col(f"tgt.{name}"))
        elif name in (*keys, VALID_FROM):
            value = F.col(f"tgt.{name}")
        else:
            value = F.when(operation == REPLACE, F.col(f"src.{name}")) \
                .otherwise(F.col(f"tgt.{name}"))
        columns.append(value.alias(name))
    kept = joined.select(*columns)
    added = changes.where(F.col(OPERATION) == INSERT).select(*history.columns)
    return kept.unionByName(added)


def latest_records(history: DataFrame) -> DataFrame:
    """The most recent version of every key, including those that have ended."""
    return history.where(F.col(LATEST_RECORD))


def records_as_of(history: DataFrame, day) -> DataFrame:
    """The version of every key that held on ``day``.

    A record that had ended by then -- a relationship deleted, an entity whose
    LEI GLEIF no longer lists -- has none.
    """
    reference = F.to_date(F.lit(str(day)))
    return history.where(
        (F.col(VALID_FROM) <= reference)
        & (F.col(VALID_TO).isNull() | (F.col(VALID_TO) >= reference))
    )


__all__ = [
    "BOOKKEEPING",
    "CLOSE",
    "CLOSE_AND_INSERT",
    "INSERT",
    "KEYS",
    "LATEST_RECORD",
    "NOT_HASHED",
    "OPERATION",
    "PUBLICATION",
    "RECORD_HASH",
    "REPLACE",
    "SUPERSEDE",
    "TARGET_VALID_FROM",
    "VALID_FROM",
    "VALID_TO",
    "apply_changes",
    "content_columns",
    "initialise_history",
    "latest_records",
    "level_1_from_bronze",
    "level_2_from_bronze",
    "plan_changes",
    "prepare_incoming",
    "records_as_of",
]
