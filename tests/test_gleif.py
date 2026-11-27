"""The GLEIF history: which publications it reads, and what each does to it.

The records below are made up, and only in the columns the rules look at; a
full GLEIF record has some forty of the columns the history keeps. The rules
are the same for both levels, so most are shown on Level 1 and the ones only
Level 2 has -- a relationship deleted, a period found by its type -- on it.
"""

from __future__ import annotations

import io
import os
import zipfile
from datetime import date, datetime, timedelta

import pytest
import yaml

from fincommons.gleif import common, history, landing
from fincommons.gleif.landing import Publication, plan_publications


# Which publications are read
# --------------------------------------------------------------------------


def publication(when, deltas=True):
    full = {level: f"https://x/{when:%Y%m%d-%H%M}-{level}-full" for level in common.LEVELS}
    delta = ({level: f"https://x/{when:%Y%m%d-%H%M}-{level}-delta" for level in common.LEVELS}
             if deltas else {})
    return Publication(when, full, delta)


def daily(first, last, hours=(0, 8, 16), deltas=True):
    found = []
    day = first
    while day <= last:
        for hour in hours:
            found.append(publication(datetime.combine(day, datetime.min.time())
                                     + timedelta(hours=hour), deltas))
        day += timedelta(days=1)
    return found


def labels(plan):
    return [(p.label, kind) for p, kind in plan]


def test_a_history_is_begun_from_the_first_full_copy_on_its_start_date():
    publications = daily(date(2026, 11, 1), date(2026, 11, 3))

    plan = plan_publications(publications, date(2026, 11, 2))

    assert labels(plan)[0] == ("20261102-0000", common.FULL)


def test_it_is_carried_forward_by_the_delta_of_each_day_at_its_hour():
    publications = daily(date(2026, 11, 1), date(2026, 11, 4))

    plan = plan_publications(publications, date(2026, 11, 1), full_weekday=None)

    assert labels(plan) == [
        ("20261101-0000", common.FULL),
        ("20261102-0000", common.DELTA),
        ("20261103-0000", common.DELTA),
        ("20261104-0000", common.DELTA),
    ]


def test_before_the_deltas_begin_it_steps_a_month_at_a_time():
    """Delta files are kept for some two months; before that, full copies."""
    old = daily(date(2026, 1, 1), date(2026, 3, 31), hours=(0,), deltas=False)
    recent = daily(date(2026, 4, 1), date(2026, 4, 3), hours=(0,))

    plan = plan_publications(old + recent, date(2026, 1, 15), full_weekday=None)

    assert labels(plan) == [
        ("20260115-0000", common.FULL),
        ("20260215-0000", common.FULL),
        ("20260315-0000", common.FULL),
        ("20260401-0000", common.FULL),
        ("20260402-0000", common.DELTA),
        ("20260403-0000", common.DELTA),
    ]


def test_a_missing_day_is_bridged_by_a_full_copy():
    publications = [p for p in daily(date(2026, 11, 1), date(2026, 11, 5), hours=(0,))
                    if p.day != date(2026, 11, 3)]

    plan = plan_publications(publications, date(2026, 11, 1), full_weekday=None)

    assert labels(plan) == [
        ("20261101-0000", common.FULL),
        ("20261102-0000", common.DELTA),
        ("20261104-0000", common.FULL),
        ("20261105-0000", common.DELTA),
    ]


def test_a_full_copy_is_read_once_a_week_instead_of_the_delta():
    """The deltas miss some deletions; Sunday's full copy closes them."""
    publications = daily(date(2026, 11, 20), date(2026, 11, 24), hours=(0,))

    plan = plan_publications(publications, date(2026, 11, 20))

    assert labels(plan) == [
        ("20261120-0000", common.FULL),
        ("20261121-0000", common.DELTA),
        ("20261122-0000", common.FULL),
        ("20261123-0000", common.DELTA),
        ("20261124-0000", common.DELTA),
    ]


def test_a_history_is_carried_on_from_where_it_stands():
    publications = daily(date(2026, 11, 1), date(2026, 11, 3))

    plan = plan_publications(publications, date(2026, 1, 1), applied="20261102-0000",
                             full_weekday=None)

    assert labels(plan) == [("20261103-0000", common.DELTA)]


def test_it_waits_for_the_publication_of_its_hour():
    """Today's 08:00 and 16:00 are out, tomorrow's 00:00 is not: nothing to do."""
    publications = daily(date(2026, 11, 2), date(2026, 11, 2))

    plan = plan_publications(publications, date(2026, 1, 1), applied="20261102-0000")

    assert plan == []


def test_the_files_are_named_as_gleif_names_them():
    p = publication(datetime(2026, 11, 26, 16))
    name = landing.landing_name(p, "level_2", common.DELTA)

    assert name == "20261126-1600-gleif-goldencopy-rr-last-day.csv.zip"
    assert landing.parse_landing_name(name) == ("20261126-1600", "level_2", common.DELTA)


def test_the_api_list_is_read_back_to_the_day_asked_for():
    def entry(when, deltas=True):
        files = {"full_file": {"csv": {"url": "u"}}}
        if deltas:
            files["delta_files"] = {"LastDay": {"csv": {"url": "d"}}}
        return {"publish_date": when, "lei2": files, "rr": files}

    pages = {
        1: [entry("2026-11-03 00:00:00"), entry("2026-11-02 16:00:00")],
        2: [entry("2026-11-02 08:00:00"), entry("2026-11-01 16:00:00", False)],
        3: [entry("2026-11-01 08:00:00")],
    }

    class Response:
        def __init__(self, page):
            self.page = page

        def raise_for_status(self):
            pass

        def json(self):
            return {"data": pages[self.page],
                    "meta": {"pagination": {"total_pages": 3}}}

    class Session:
        asked = []

        def get(self, url, timeout, params):
            self.asked.append(params["page"])
            return Response(params["page"])

    session = Session()
    found = landing.list_publications(date(2026, 11, 2), session=session)

    assert [p.label for p in found] == ["20261102-0800", "20261102-1600", "20261103-0000"]
    assert session.asked == [1, 2]


# What a publication does to the history
# --------------------------------------------------------------------------

LEVEL_1 = {"LEI": "lei", "Entity.LegalName": "legal_name",
           "Entity.LegalAddress.Country": "legal_address_country",
           "Registration.LastUpdateDate": "last_update_date"}


def records(spark, rows):
    """Level 1 records in the Silver names, from (lei, name, country, update)."""
    return spark.createDataFrame(
        rows, "lei string, legal_name string, legal_address_country string, "
              "last_update_date string")


def prepared(spark, rows, label):
    published = datetime.strptime(label, "%Y%m%d-%H%M").date()
    return history.prepare_incoming(records(spark, rows), "level_1", label, published)


def applied(spark, steps):
    """The history after a list of (label, full, rows)."""
    label, _, rows = steps[0]
    current = history.initialise_history(prepared(spark, rows, label))
    for label, full, rows in steps[1:]:
        incoming = prepared(spark, rows, label)
        changes = history.plan_changes(history.latest_records(current), incoming,
                                       "level_1", full)
        current = history.apply_changes(current, changes, "level_1").cache()
    return current


def versions(frame, key="lei"):
    return sorted(
        (row[key], row["legal_name"], str(row["valid_from"]),
         str(row["valid_to"]) if row["valid_to"] else None, row["latest_record"])
        for row in frame.collect())


START = ("20261101-0000", True, [
    ("A", "Alpha AG", "AT", "2025-03-01T10:00:00Z"),
    ("B", "Beta SE", "DE", "2026-10-20T10:00:00Z"),
])


def test_a_full_copy_begins_every_record_on_its_last_update(spark):
    assert versions(applied(spark, [START])) == [
        ("A", "Alpha AG", "2025-03-01", None, True),
        ("B", "Beta SE", "2026-10-20", None, True),
    ]


def test_a_change_opens_a_version_on_the_day_of_its_update(spark):
    result = applied(spark, [START, ("20261103-0000", False, [
        ("A", "Alpha Holding AG", "AT", "2026-11-02T09:00:00Z")])])

    assert versions(result) == [
        ("A", "Alpha AG", "2025-03-01", "2026-11-01", False),
        ("A", "Alpha Holding AG", "2026-11-02", None, True),
        ("B", "Beta SE", "2026-10-20", None, True),
    ]


def test_a_record_read_again_unchanged_opens_nothing(spark):
    result = applied(spark, [START, ("20261103-0000", False, [
        ("A", "Alpha AG", "AT", "2026-11-02T09:00:00Z")])])

    assert len(versions(result)) == 2


def test_a_change_without_a_new_update_day_begins_on_the_publication(spark):
    """GLEIF changed the record without the issuer touching it."""
    result = applied(spark, [START, ("20261103-0000", False, [
        ("B", "Beta SE", "FR", "2026-10-20T10:00:00Z")])])

    assert ("B", "Beta SE", "2026-11-03", None, True) in versions(result)
    assert ("B", "Beta SE", "2026-10-20", "2026-11-02", False) in versions(result)


def test_two_states_of_one_day_leave_the_last(spark):
    result = applied(spark, [
        START,
        ("20261103-0000", False, [("A", "Alpha 1", "AT", "2026-11-02T09:00:00Z")]),
        ("20261103-0800", False, [("A", "Alpha 2", "AT", "2026-11-02T12:00:00Z")]),
    ])

    assert [v for v in versions(result) if v[0] == "A"] == [
        ("A", "Alpha 2", "2026-11-02", None, True),
        ("A", "Alpha AG", "2025-03-01", "2026-11-01", False),
    ]


def test_a_record_a_full_copy_no_longer_lists_is_closed(spark):
    result = applied(spark, [START, ("20261201-0000", True, [
        ("A", "Alpha AG", "AT", "2025-03-01T10:00:00Z")])])

    assert ("B", "Beta SE", "2026-10-20", "2026-11-30", True) in versions(result)


def test_a_new_record_is_inserted(spark):
    result = applied(spark, [START, ("20261103-0000", False, [
        ("C", "Gamma NV", "NL", "2026-11-02T09:00:00Z")])])

    assert ("C", "Gamma NV", "2026-11-02", None, True) in versions(result)


def test_the_records_of_a_day_are_the_versions_holding_on_it(spark):
    result = applied(spark, [START, ("20261103-0000", False, [
        ("A", "Alpha Holding AG", "AT", "2026-11-02T09:00:00Z")])])

    def names(day):
        return sorted(r["legal_name"] for r in history.records_as_of(result, day).collect())

    assert names("2026-11-01") == ["Alpha AG", "Beta SE"]
    assert names("2026-11-02") == ["Alpha Holding AG", "Beta SE"]
    assert names("2025-01-01") == []


# Level 2: relationships, and their deletion
# --------------------------------------------------------------------------


def relationship_bronze(spark, rows):
    """Level 2 rows as the Bronze layer holds them, from
    (start, end, status, period 1 type, period 1 start, period 2 type,
    period 2 start, update, deleted)."""
    columns = common.LEVEL_2_SOURCE_COLUMNS
    frames = []
    for start, end, status, t1, s1, t2, s2, update, deleted in rows:
        row = dict.fromkeys(columns)
        row.update({
            "Relationship.StartNode.NodeID": start,
            "Relationship.EndNode.NodeID": end,
            "Relationship.RelationshipType": "IS_ULTIMATELY_CONSOLIDATED_BY",
            "Relationship.RelationshipStatus": status,
            "Relationship.Period.1.periodType": t1,
            "Relationship.Period.1.startDate": s1,
            "Relationship.Period.2.periodType": t2,
            "Relationship.Period.2.startDate": s2,
            "Registration.LastUpdateDate": update,
            "DeletedAt": deleted,
        })
        frames.append(tuple(row[c] for c in columns))
    return spark.createDataFrame(frames, ", ".join(f"`{c}` string" for c in columns))


def test_the_period_of_a_relationship_is_found_by_its_type(spark):
    bronze = relationship_bronze(spark, [
        ("A", "P", "ACTIVE", "ACCOUNTING_PERIOD", "2025-01-01",
         "RELATIONSHIP_PERIOD", "2019-06-01", "2026-10-01T00:00:00Z", None)])

    row = history.level_2_from_bronze(bronze, common.RELATIONSHIP_GROUPS).collect()[0]

    assert row["relationship_start"] == "2019-06-01"
    assert row["accounting_period_start"] == "2025-01-01"


def test_a_relationship_deleted_by_a_delta_is_closed_the_day_before(spark):
    def incoming(rows, label):
        published = datetime.strptime(label, "%Y%m%d-%H%M").date()
        return history.prepare_incoming(
            history.level_2_from_bronze(relationship_bronze(spark, rows),
                                        common.RELATIONSHIP_GROUPS),
            "level_2", label, published)

    start = history.initialise_history(incoming([
        ("A", "P", "ACTIVE", "RELATIONSHIP_PERIOD", "2019-06-01", None, None,
         "2026-10-01T00:00:00Z", None)], "20261101-0000"))
    delta = incoming([
        ("A", "P", "ACTIVE", "RELATIONSHIP_PERIOD", "2019-06-01", None, None,
         "2026-11-02T00:00:00Z", "2026-11-02T08:00:00Z")], "20261103-0000")
    changes = history.plan_changes(history.latest_records(start), delta,
                                   "level_2", full=False)
    result = history.apply_changes(start, changes, "level_2").collect()

    assert len(result) == 1
    assert str(result[0]["valid_to"]) == "2026-11-01"
    assert result[0]["latest_record"] is True


# The storage: a Delta table carried forward by tagged merges
# --------------------------------------------------------------------------


@pytest.fixture
def lake(tmp_path):
    base = tmp_path / "lake"
    config = tmp_path / "paths.yaml"
    config.write_text(yaml.safe_dump({"gleif": {
        layer: str(base / layer) for layer in ("landing", "bronze", "silver")}}))
    return str(config)


def level_1_bronze(spark, lake, label, kind, rows):
    """A Bronze table of Level 1 from (lei, name, country, update)."""
    columns = common.SOURCE_COLUMNS["level_1"]
    frame = []
    for lei, name, country, update in rows:
        row = dict.fromkeys(columns)
        row.update({"LEI": lei, "Entity.LegalName": name,
                    "Entity.LegalAddress.Country": country,
                    "Registration.LastUpdateDate": update})
        frame.append(tuple(row[c] for c in columns))
    path = os.path.join(common.path_bronze(lake, "level_1"), f"{label}-{kind}.parquet")
    (spark.createDataFrame(frame, ", ".join(f"`{c}` string" for c in columns))
     .write.parquet(path))
    return path


def test_the_merge_applies_what_the_rules_say(spark, lake):
    from fincommons.gleif import silver

    steps = [
        ("20261101-0000", common.FULL, START[2]),
        ("20261103-0000", common.DELTA, [
            ("A", "Alpha Holding AG", "AT", "2026-11-02T09:00:00Z"),
            ("C", "Gamma NV", "NL", "2026-11-02T09:00:00Z")]),
        ("20261201-0000", common.FULL, [
            ("A", "Alpha Holding AG", "AT", "2026-11-02T09:00:00Z"),
            ("C", "Gamma NV", "NL", "2026-11-02T09:00:00Z")]),
    ]
    for label, kind, rows in steps:
        path = level_1_bronze(spark, lake, label, kind, rows)
        silver.apply_publication(spark, lake, "level_1", label, kind, path)

    stored = common.connect_to_gleif(spark, lake, "level_1")
    assert versions(stored) == [
        ("A", "Alpha AG", "2025-03-01", "2026-11-01", False),
        ("A", "Alpha Holding AG", "2026-11-02", None, True),
        ("B", "Beta SE", "2026-10-20", "2026-11-30", True),
        ("C", "Gamma NV", "2026-11-02", None, True),
    ]
    assert silver.applied_publication(spark, lake, "level_1") == "20261201-0000"


def test_a_history_cannot_begin_with_a_delta(spark, lake):
    from fincommons.gleif import silver

    path = level_1_bronze(spark, lake, "20261103-0000", common.DELTA, START[2])
    with pytest.raises(ValueError, match="full copy"):
        silver.apply_publication(spark, lake, "level_1", "20261103-0000",
                                 common.DELTA, path)


def test_a_downloaded_file_is_imported_with_every_column_it_keeps(spark, lake):
    from fincommons.gleif import bronze

    header = ["LEI", "Entity.LegalName", "Entity.LegalAddress.Country", "Unwanted"]
    text = ",".join(header) + '\nA,"Alpha, ""the"" AG",AT,x\nB,,NA,y\n'
    folder = common.path_landing(lake, "level_1")
    os.makedirs(folder)
    path = os.path.join(folder, "20261101-0000-gleif-goldencopy-lei2-golden-copy.csv.zip")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("x.csv", text)

    target = bronze.import_file(spark, lake, path)
    rows = {r["LEI"]: r for r in spark.read.parquet(target).collect()}

    assert set(spark.read.parquet(target).columns) == set(common.SOURCE_COLUMNS["level_1"])
    assert rows["A"]["Entity.LegalName"] == 'Alpha, "the" AG'
    assert rows["B"]["Entity.LegalName"] is None
    # Namibia is a country and not a missing value.
    assert rows["B"]["Entity.LegalAddress.Country"] == "NA"
    assert bronze.bronze_files(lake, "level_1") == [("20261101-0000", "full", target)]


def test_a_download_is_decoded_and_must_be_a_zip(lake):
    """GLEIF sends its archives gzip-encoded; a file that is still not a zip
    once decoded is refused rather than left for the import to fail on."""
    import gzip

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("x.csv", "LEI\nA\n")

    class Raw(io.BytesIO):
        decode_content = False

        def read(self, *args):
            data = super().read(*args)
            return data if self.decode_content else data

    class Response:
        def __init__(self, body):
            self.raw = Raw(body)

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def raise_for_status(self):
            pass

    class Session:
        def __init__(self, body):
            self.body = body

        def get(self, url, stream, timeout):
            return Response(self.body)

    p = publication(datetime(2026, 11, 26))
    paths = landing.download_publication(lake, p, common.FULL,
                                         session=Session(archive.getvalue()))
    assert all(zipfile.is_zipfile(path) for path in paths.values())

    with pytest.raises(ValueError, match="not a zip"):
        landing.download_publication(lake, publication(datetime(2026, 11, 27)),
                                     common.FULL,
                                     session=Session(gzip.compress(b"junk")))
    assert not [n for n in os.listdir(common.path_landing(lake, "level_1"))
                if n.startswith("20261127")]


# The data contracts
# --------------------------------------------------------------------------


def assert_meets_contract(table, identifier):
    """The table is the table its contract describes: the same columns in
    the same order and of the same types, no empty value where the contract
    says a column is required, and one row per value of the primary key."""
    from pyspark.sql import functions as F
    from fincommons.contracts import load_contract, spark_schema

    contract = load_contract(identifier)
    properties = contract["schema"][0]["properties"]

    assert [(f.name, f.dataType) for f in table.schema] == \
        [(f.name, f.dataType) for f in spark_schema(contract)]
    for prop in properties:
        if prop.get("required"):
            assert table.where(F.col(prop["name"]).isNull()).count() == 0, prop["name"]
    key = [p["name"] for p in sorted((p for p in properties if p.get("primaryKey")),
                                     key=lambda p: p["primaryKeyPosition"])]
    assert table.groupBy(*key).count().where("count > 1").count() == 0
    return key


def test_the_tables_are_the_tables_their_contracts_describe(spark, lake):
    """Both levels carried through a full copy, a delta that changes, adds
    and deletes, and a full copy that drops, so that each table holds closed
    and open versions."""
    from fincommons.gleif import silver

    for label, kind, rows in [
        ("20261101-0000", common.FULL, START[2]),
        ("20261103-0000", common.DELTA, [
            ("A", "Alpha Holding AG", "AT", "2026-11-02T09:00:00Z"),
            ("C", "Gamma NV", "NL", "2026-11-02T09:00:00Z")]),
        ("20261201-0000", common.FULL, [
            ("A", "Alpha Holding AG", "AT", "2026-11-02T09:00:00Z")]),
    ]:
        path = level_1_bronze(spark, lake, label, kind, rows)
        silver.apply_publication(spark, lake, "level_1", label, kind, path)

    period = ("RELATIONSHIP_PERIOD", "2019-06-01", None, None)
    for label, kind, rows in [
        ("20261101-0000", common.FULL, [
            ("A", "P", "ACTIVE", *period, "2026-10-01T00:00:00Z", None),
            ("B", "P", "ACTIVE", *period, "2026-10-01T00:00:00Z", None)]),
        ("20261103-0000", common.DELTA, [
            ("A", "P", "INACTIVE", *period, "2026-11-02T00:00:00Z", None),
            ("B", "P", "ACTIVE", *period, "2026-11-02T00:00:00Z", "2026-11-02T08:00:00Z")]),
        ("20261201-0000", common.FULL, [
            ("A", "P", "INACTIVE", *period, "2026-11-02T00:00:00Z", None)]),
    ]:
        path = os.path.join(common.path_bronze(lake, "level_2"), f"{label}-{kind}.parquet")
        relationship_bronze(spark, rows).write.parquet(path)
        silver.apply_publication(spark, lake, "level_2", label, kind, path)

    entities = common.connect_to_gleif(spark, lake, "level_1")
    relationships = common.connect_to_gleif(spark, lake, "level_2")
    assert entities.where("valid_to IS NOT NULL").count() > 0
    assert relationships.where("valid_to IS NOT NULL").count() > 0
    assert assert_meets_contract(entities, "fincommons.gleif.silver.entities") == \
        list(history.KEYS["level_1"]) + [history.VALID_FROM]
    assert assert_meets_contract(relationships, "fincommons.gleif.silver.relationships") == \
        list(history.KEYS["level_2"]) + [history.VALID_FROM]
