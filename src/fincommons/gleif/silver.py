"""The history of the GLEIF data in the Silver layer, and the pipeline that keeps it.

Each level has one Delta table of versions, built by the rules of
:mod:`fincommons.gleif.history`. A publication is applied to a level in one
merge, and the merge's commit names the publication in its ``userMetadata``:
``gleif-silver-level_1:20261126-0000:delta``. The commit is atomic, so a level
either holds a publication or does not, and the last tagged commit says how
far the history has been carried. A run that died is resumed by applying the
next publication after it, as the FIRDS history is resumed from its tags.

The two levels are carried forward together, one publication at a time, so
that on any day the entities and the relationships of the history describe the
same publication.
"""

from __future__ import annotations

from datetime import date, datetime

from . import common, history
from .bronze import import_file, read_bronze, bronze_files
from .landing import (FULL_WEEKDAY, download_publication, list_publications,
                      plan_publications)
from ..addressing import delta_table, delta_table_exists, read_delta, write_delta
import logging

from ..log import context, pipeline_run

logger = logging.getLogger(__name__)

#: The prefix of the ``userMetadata`` of a commit that applied a publication.
COMMIT_TAG_PREFIX = "gleif-silver"

#: The conf through which a commit is tagged.
USER_METADATA_CONF = "spark.databricks.delta.commitInfo.userMetadata"

#: How many of the latest commits are searched for the last tag. A level is
#: written by one commit per publication and by nothing else.
TAG_COMMITS = 20


def commit_tag(level, label, kind):
    """The ``userMetadata`` of the commit that applied one publication."""
    return f"{COMMIT_TAG_PREFIX}-{level}:{label}:{kind}"


def applied_publication(spark, config_path, level):
    """The label of the last publication applied to a level, or ``None``."""
    ref = common.ref_silver(config_path, level)
    if not delta_table_exists(spark, ref):
        return None
    commits = (delta_table(spark, ref).history(TAG_COMMITS)
               .select("version", "userMetadata").collect())
    prefix = f"{COMMIT_TAG_PREFIX}-{level}:"
    for commit in sorted(commits, key=lambda c: c["version"], reverse=True):
        tag = commit["userMetadata"]
        if tag and tag.startswith(prefix):
            return tag.split(":")[1]
    return None


class _tagged:
    """Tag the commits written within the block."""

    def __init__(self, spark, tag):
        self.spark, self.tag = spark, tag

    def __enter__(self):
        conf = self.spark.conf
        self.before = conf.get(USER_METADATA_CONF, None)
        conf.set(USER_METADATA_CONF, self.tag)

    def __exit__(self, *_):
        if self.before is None:
            self.spark.conf.unset(USER_METADATA_CONF)
        else:
            self.spark.conf.set(USER_METADATA_CONF, self.before)


def incoming_records(spark, path, level, label):
    """The records of one Bronze table, ready to be set against the history."""
    bronze = read_bronze(spark, path)
    if level == "level_1":
        records = history.level_1_from_bronze(bronze, common.LEVEL_1_COLUMNS)
    else:
        records = history.level_2_from_bronze(bronze, common.RELATIONSHIP_GROUPS)
    published = datetime.strptime(label, "%Y%m%d-%H%M").date()
    return history.prepare_incoming(records, level, label, published)


def apply_publication(spark, config_path, level, label, kind, path):
    """Apply one Bronze table of a level to its history, in one tagged commit.

    The first publication of a level begins its history and has to be a full
    copy; any later one is merged into it.

    Returns
    -------
    dict
        How many versions the publication inserted and how many it changed.
    """
    ref = common.ref_silver(config_path, level)
    incoming = incoming_records(spark, path, level, label)
    tag = commit_tag(level, label, kind)

    if not delta_table_exists(spark, ref):
        if kind != common.FULL:
            raise ValueError(
                f"the history of {level} has to begin with a full copy, and "
                f"{label} is a delta"
            )
        begun = history.initialise_history(incoming)
        with _tagged(spark, tag):
            write_delta(begun, ref, mode="overwrite")
        return {"inserted": read_delta(spark, ref).count(), "changed": 0}

    current = read_delta(spark, ref)
    latest = history.latest_records(current)
    changes = history.plan_changes(latest, incoming, level,
                                   full=kind == common.FULL).cache()
    try:
        counts = {row[history.OPERATION]: row["count"] for row in
                  changes.groupBy(history.OPERATION).count().collect()}
        keys = history.KEYS[level]
        condition = " AND ".join(
            [f"tgt.`{k}` = src.`{k}`" for k in keys]
            + [f"tgt.`{history.VALID_FROM}` = src.`{history.TARGET_VALID_FROM}`"])
        columns = current.columns
        content = [c for c in columns if c not in (*keys, history.VALID_FROM,
                                                   history.VALID_TO,
                                                   history.LATEST_RECORD)]
        source = changes.select(*{*columns, history.OPERATION,
                                  history.TARGET_VALID_FROM})
        with _tagged(spark, tag):
            (delta_table(spark, ref).alias("tgt")
             .merge(source.alias("src"), condition)
             .whenMatchedUpdate(
                 condition=f"src.`{history.OPERATION}` IN "
                           f"('{history.CLOSE}', '{history.SUPERSEDE}')",
                 set={history.VALID_TO: f"src.`{history.VALID_TO}`",
                      history.LATEST_RECORD: f"src.`{history.LATEST_RECORD}`"})
             .whenMatchedUpdate(
                 condition=f"src.`{history.OPERATION}` = '{history.REPLACE}'",
                 set={c: f"src.`{c}`" for c in content})
             .whenNotMatchedInsert(
                 condition=f"src.`{history.OPERATION}` = '{history.INSERT}'",
                 values={c: f"src.`{c}`" for c in columns})
             .execute())
    finally:
        changes.unpersist()
    return {"inserted": counts.get(history.INSERT, 0),
            "changed": sum(v for k, v in counts.items() if k != history.INSERT)}


def update_gleif(spark, config_path, start_date, months=1, until=None,
                 session=None, full_weekday=FULL_WEEKDAY):
    """Carry the history of both levels forward to GLEIF's latest publication.

    The first run begins the history from the first publication on or after
    ``start_date``; every later run carries it from where it stands. See
    :mod:`fincommons.gleif.landing` for which publications are read, as full
    copies or as deltas.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session with Delta Lake.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    start_date: datetime.date or str
        The first day the history covers. It decides where a history begins
        and nothing once it has: a later run carries the history on from the
        publication it stands at.
    months: int
        The step between two full copies before the delta files begin.
    until: datetime.date or str, optional
        Carry the history no further than the publications of this day; the
        latest publication by default.
    session: requests.Session, optional
        The session to read GLEIF's API and files with.
    full_weekday: int or None
        The day of the week on which a full copy is read instead of the
        delta, Sunday by default; see
        :data:`fincommons.gleif.landing.FULL_WEEKDAY`.

    Returns
    -------
    list of str
        The labels of the publications applied.
    """
    start_date = _as_date(start_date)
    until = _as_date(until) if until is not None else None
    common.create_directory_tree(config_path)
    with pipeline_run(logger, "update_gleif", module="gleif", layer="silver",
                      config_path=config_path) as outcome:
        applied = {level: applied_publication(spark, config_path, level)
                   for level in common.LEVELS}
        if len(set(applied.values())) != 1:
            # One level was carried further than the other by a run that died
            # between them; the one behind is brought level first.
            behind = min((v or "", k) for k, v in applied.items())[1]
            raise RuntimeError(
                f"the levels of the GLEIF history stand at different "
                f"publications: {applied}. Apply the missing publication to "
                f"{behind} with apply_publication before updating."
            )
        last = applied["level_1"]
        since = (datetime.strptime(last, "%Y%m%d-%H%M").date()
                 if last else start_date)
        publications = list_publications(since, session=session)
        if until is not None:
            publications = [p for p in publications if p.day <= until]
        plan = plan_publications(publications, start_date, last, months,
                                 full_weekday)
        done = []
        for publication, kind in plan:
            paths = download_publication(config_path, publication, kind,
                                         session=session)
            for level in common.LEVELS:
                bronze = import_file(spark, config_path, paths[level])
                counts = apply_publication(spark, config_path, level,
                                           publication.label, kind, bronze)
                logger.info("Applied", extra=context(
                    module="gleif", layer="silver", level=level,
                    publication=publication.label, kind=kind, **counts))
            done.append(publication.label)
        outcome["applied"] = len(done)
        outcome["publication"] = done[-1] if done else last
    return done


def _as_date(day):
    if isinstance(day, datetime):
        return day.date()
    if isinstance(day, date):
        return day
    return date.fromisoformat(str(day))


def entities_as_of(spark, config_path, day):
    """The entities as GLEIF described them on ``day``, one row per LEI."""
    return history.records_as_of(
        common.connect_to_gleif(spark, config_path, "level_1"), day)


def relationships_as_of(spark, config_path, day):
    """The relationships GLEIF reported as standing on ``day``."""
    return history.records_as_of(
        common.connect_to_gleif(spark, config_path, "level_2"), day)


def latest_entities(spark, config_path):
    """The latest version of every LEI, including those that have ended."""
    return history.latest_records(
        common.connect_to_gleif(spark, config_path, "level_1"))


def latest_relationships(spark, config_path):
    """The latest version of every relationship, including those that have ended."""
    return history.latest_records(
        common.connect_to_gleif(spark, config_path, "level_2"))


__all__ = [
    "COMMIT_TAG_PREFIX",
    "applied_publication",
    "apply_publication",
    "commit_tag",
    "entities_as_of",
    "incoming_records",
    "latest_entities",
    "latest_relationships",
    "relationships_as_of",
    "update_gleif",
]
