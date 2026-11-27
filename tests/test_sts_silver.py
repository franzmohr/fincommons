"""Building the tables of the STS register from a snapshot.

These tests need the Spark session and are therefore in the slow tier. What
they are for is the part :mod:`tests.test_sts_parse` cannot reach: that the
rows it produces fit the declared schemas, that each table is written where
the configuration file says and read back by name, and that the three things
the build decides -- which snapshot it uses, what it compares with, and what
it does when a spreadsheet is missing -- come out as intended.

The snapshots are written into the lake in the shape a collection leaves them.
"""

from __future__ import annotations

import datetime
import json
import os

import pytest
import yaml

from fincommons.sts import common, silver


@pytest.fixture
def config(tmp_path):
    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"sts": {name: str(base / name) for name in ("bronze", "silver")}}
    ))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


def a_notification(**overrides):
    document = {
        "id": "5882", "entity_type": "stsre", "type_s": "parent",
        "_root_": "5882",
        "stsre_notif_id": "2138001WO21Z3B8Y8K20N20260201",
        "stsre_sec_id": "2138001WO21Z3B8Y8K20N202602",
        "stsre_init_notif_dt": "2026-09-10T00:00:00Z",
        "stsre_notif_dt": "2026-09-10T00:00:00Z",
        "stsre_secclass_code": "NON_ABCP",
        "stsre_secclass_desc": "non-ABCP securitisation",
        "stsre_sectype_code": "PUBLIC", "stsre_sectype_desc": "Public",
        "stsre_compl_stat_desc": "Compliant",
        "stsre_sanc_status": "Not Sanctioned",
        "stsre_undrlexp_desc": "residential mortgages",
        "stsre_sec_name": ["Candide Financing 2026-2"],
        "de_name": "Lloyds Bank GmbH",
        "de_lei_priv": "2138001WO21Z3B8Y8K20",
        "non_abcp_requirements_on_simplicity_Homogeneity_assets_flag": "C",
        "non_abcp_requirements_on_simplicity_Homogeneity_assets_comment": "Yes.",
    }
    document.update(overrides)
    return document


def children_of(parent="5882"):
    return [
        {"type_s": "child", "entity_type": "issins", "_root_": parent,
         "stsre_isin_instrument": "XS3461032990",
         "stsre_isin_name": "CANDIDE FINANCI"},
        {"type_s": "child", "entity_type": "originator", "_root_": parent,
         "originator_lei": ["2138001WO21Z3B8Y8K20"],
         "originator_name": ["LLOYDS BANK GMBH"],
         "originator_country": ["DE"], "originator_country_name": "GERMANY"},
        {"type_s": "child", "entity_type": "sspe", "_root_": parent,
         "sspe_country_code": ["NL"], "sspe_country_name": "NETHERLANDS"},
    ]


def write_snapshot(config_path, day, parents=(), children=()):
    for kind, documents in (("parent", parents), ("child", children)):
        answer = {"responseHeader": {"status": 0},
                  "response": {"numFound": len(documents), "docs": list(documents)}}
        path = common.path_bronze_page(config_path, day, kind, 0)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(answer, handle)


def write_sheet(config_path, day, name, rows, above=3):
    from openpyxl import Workbook

    path = common.path_bronze_sheet(config_path, day, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    book = Workbook()
    sheet = book.active
    for _ in range(above):
        sheet.append(["Disclaimer:"])
    for row in rows:
        sheet.append(row)
    book.save(path)
    return path


# Which snapshot the build uses
# --------------------------------------------------------------------------


def test_the_latest_snapshot_is_the_one_built_from(config):
    for day in ("2026-10-01", "2026-11-20", "2026-11-01"):
        write_snapshot(config, day, [a_notification()])

    assert silver.latest_snapshot(config) == "2026-11-20"
    assert silver.previous_snapshot(config, "2026-11-20") == "2026-11-01"
    assert silver.previous_snapshot(config, "2026-10-01") is None


def test_a_lake_with_no_snapshot_is_told_to_collect(config):
    with pytest.raises(RuntimeError, match="holds no snapshot"):
        silver.latest_snapshot(config)


# The tables
# --------------------------------------------------------------------------


def test_the_six_tables_are_built_and_read_back(spark, config):
    write_snapshot(config, "2026-11-20", [a_notification()], children_of())
    write_sheet(config, "2026-11-20", "synthetic", [
        ["Notification number", "Notification date to ESMA",
         "Securitisation Unique Identifier", "Private or Public",
         "Underlying assets"],
        [1, datetime.datetime(2026, 10, 13), "SYSEC_Private_2026_10_13_01",
         "private", "Large corporates loans"],
    ])

    silver.update_silver_sts(spark, config)

    counts = {table: common.connect_to_sts(spark, config, table).count()
              for table in common.SILVER_TABLES}
    assert counts == {"notification": 1, "assertion": 1, "tranche": 1,
                      "party": 2, "event": 1, "synthetic": 1}


def test_each_table_keeps_its_declared_schema(spark, config):
    write_snapshot(config, "2026-11-20", [a_notification()], children_of())

    silver.update_silver_sts(spark, config)

    for table, schema in (("notification", common.notification_schema),
                          ("assertion", common.assertion_schema),
                          ("tranche", common.tranche_schema),
                          ("party", common.party_schema),
                          ("event", common.event_schema)):
        built = common.connect_to_sts(spark, config, table)
        assert built.columns == schema.names, table


def test_a_row_carries_the_snapshot_it_was_built_from(spark, config):
    write_snapshot(config, "2026-11-20", [a_notification()], children_of())

    silver.update_silver_sts(spark, config)

    row = common.connect_to_sts(spark, config, "notification").first()
    assert row["snapshot"] == datetime.date(2026, 11, 20)
    assert row["securitisation_identifier"] == "2138001WO21Z3B8Y8K20N202602"
    assert row["identifier_scheme"] == "lei"


def test_the_children_carry_the_identifier_of_their_notification(spark, config):
    write_snapshot(config, "2026-11-20", [a_notification()], children_of())

    silver.update_silver_sts(spark, config)

    tranche = common.connect_to_sts(spark, config, "tranche").first()
    assert tranche["securitisation_identifier"] == "2138001WO21Z3B8Y8K20N202602"
    assert tranche["isin"] == "XS3461032990"


def test_an_older_snapshot_can_be_built_from(spark, config):
    write_snapshot(config, "2026-11-01", [a_notification(id="old")])
    write_snapshot(config, "2026-11-20", [a_notification(id="new")])

    silver.update_silver_sts(spark, config, day="2026-11-01")

    row = common.connect_to_sts(spark, config, "notification").first()
    assert row["id"] == "old"
    assert row["snapshot"] == datetime.date(2026, 11, 1)


def test_a_snapshot_with_no_notification_is_refused(spark, config):
    write_snapshot(config, "2026-11-20", [])

    with pytest.raises(RuntimeError, match="holds no notification"):
        silver.update_silver_sts(spark, config)


# What the events are folded from
# --------------------------------------------------------------------------


def test_a_departure_is_recorded_against_the_snapshot_before(spark, config):
    write_snapshot(config, "2026-11-01",
                   [a_notification(id="1"),
                    a_notification(id="2", stsre_sec_id="GONE")])
    write_snapshot(config, "2026-11-20", [a_notification(id="1")])

    silver.update_silver_sts(spark, config)

    events = common.connect_to_sts(spark, config, "event")
    gone = events.where("event = 'left_register'").collect()
    assert [row["securitisation_identifier"] for row in gone] == ["GONE"]
    assert gone[0]["source"] == "snapshot"


def test_the_comparison_can_be_turned_off(spark, config):
    write_snapshot(config, "2026-11-01",
                   [a_notification(id="2", stsre_sec_id="GONE")])
    write_snapshot(config, "2026-11-20", [a_notification(id="1")])

    silver.update_silver_sts(spark, config, compare=False)

    events = common.connect_to_sts(spark, config, "event")
    assert events.where("event = 'left_register'").count() == 0


def test_the_frozen_lists_are_folded_into_the_events(spark, config):
    write_snapshot(config, "2026-11-20", [a_notification()])
    write_sheet(config, "2026-11-20", "cancelled", [
        ["Cancellation date to ESMA", "Unique Securitisation Identifier",
         "Reasons for cancellation"],
        [datetime.datetime(2022, 4, 6), "Private_2020_08_21_01",
         "Teminated STS Transaction"],
    ])

    silver.update_silver_sts(spark, config)

    cancelled = common.connect_to_sts(spark, config, "event") \
        .where("event = 'cancelled'").collect()
    assert len(cancelled) == 1
    assert cancelled[0]["reason"] == "Teminated STS Transaction"
    assert cancelled[0]["source"] == "ESMA33-128-761"


# What a missing source does
# --------------------------------------------------------------------------


def test_a_missing_synthetic_list_leaves_the_table_unbuilt_and_warns(
        spark, config, caplog):
    write_snapshot(config, "2026-11-20", [a_notification()])

    with caplog.at_level("WARNING"):
        silver.update_silver_sts(spark, config)

    assert silver.build_synthetic(spark, config, "2026-11-20") is None
    assert "synthetic" in caplog.text


def test_a_field_the_register_adds_is_warned_about(spark, config, caplog):
    write_snapshot(config, "2026-11-20",
                   [a_notification(stsre_brand_new="something")])

    with caplog.at_level("WARNING"):
        silver.update_silver_sts(spark, config)

    assert "fields this layer does not read" in caplog.text
    # The field is recorded beside the message rather than inside it, as
    # everything a record may have to be filtered by is in this package.
    named = [record for record in caplog.records
             if getattr(record, "ctx", {}).get("fields") == "stsre_brand_new"]
    assert len(named) == 1


# The data contracts
# --------------------------------------------------------------------------


SCHEMAS = {"notification": common.notification_schema,
           "assertion": common.assertion_schema,
           "tranche": common.tranche_schema, "party": common.party_schema,
           "event": common.event_schema, "synthetic": common.synthetic_schema}


@pytest.mark.parametrize("table", common.SILVER_TABLES)
def test_the_declared_schema_is_the_schema_of_the_contract(table):
    """The code writes with the declared schema and a reader is told the
    contract, so the two must agree in names, order, types and in which
    columns may be empty."""
    from fincommons.contracts import load_contract, spark_schema

    assert spark_schema(load_contract(f"fincommons.sts.silver.{table}")) == SCHEMAS[table]


def test_the_tables_are_the_tables_their_contracts_describe(spark, config):
    """The six tables built from a snapshot, a list of cancellations and a
    list of synthetic securitisations are those their contracts describe --
    columns, types and order, no empty value where one is required, one row
    per key -- and meet their quality rules, warnings included."""
    from pyspark.sql import functions as F

    from fincommons.contracts import check_quality, load_contract, spark_schema

    write_snapshot(config, "2026-11-20", [a_notification()], children_of())
    write_sheet(config, "2026-11-20", "cancelled", [
        ["Cancellation date to ESMA", "Unique Securitisation Identifier",
         "Reasons for cancellation"],
        [datetime.datetime(2022, 4, 6), "Private_2020_08_21_01", "Teminated STS Transaction"],
    ])
    write_sheet(config, "2026-11-20", "synthetic", [
        ["Notification number", "Notification date to ESMA",
         "Securitisation Unique Identifier", "Private or Public", "Underlying assets"],
        [1, datetime.datetime(2026, 10, 13), "SYSEC_Private_2026_10_13_01",
         "private", "Large corporates loans"],
    ])
    silver.update_silver_sts(spark, config)

    keys = {"notification": ["id"], "assertion": ["id", "template", "requirement"],
            "tranche": [], "party": [], "event": [],
            "synthetic": ["securitisation_identifier", "notification_number"]}
    for table in common.SILVER_TABLES:
        frame = common.connect_to_sts(spark, config, table)
        contract = load_contract(f"fincommons.sts.silver.{table}")
        properties = contract["schema"][0]["properties"]

        assert frame.count() > 0, table
        assert [(f.name, f.dataType) for f in frame.schema] == \
            [(f.name, f.dataType) for f in spark_schema(contract)], table
        for prop in properties:
            if prop.get("required"):
                assert frame.where(F.col(prop["name"]).isNull()).count() == 0, (table, prop["name"])
        key = keys[table]
        assert [p["name"] for p in sorted((p for p in properties if p.get("primaryKey")),
                                          key=lambda p: p["primaryKeyPosition"])] == key
        if key:
            assert frame.groupBy(*key).count().where("count > 1").count() == 0, table
        assert [r for r in check_quality(frame, contract) if r.passed is False] == [], table
