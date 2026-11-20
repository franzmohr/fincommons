"""Reading a snapshot of the STS register and ESMA's spreadsheets into rows.

Nothing here needs Spark. The documents are written into the test in the shape
the register served them on 20 November 2026 -- a notification as a parent
document and its tranches and parties as children hanging off it by
``_root_`` -- so that every rule by which the register becomes a table is
tested against the thing it was written for.

The cases that matter are the ones the register itself produced: an identifier
that is a single space, a field that is multivalued, a field the register
serves as a number, a template field with no ``_flag`` ending, and a special
purpose entity that has a country and no identifier at all.
"""

from __future__ import annotations

import datetime
import json
import os

import pytest
import yaml

from fincommons.sts import common, parse


@pytest.fixture
def config(tmp_path):
    """A configuration file pointing at an empty data lake."""
    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"sts": {name: str(base / name) for name in ("bronze", "silver")}}
    ))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


def a_notification(**overrides):
    """One public notification, as the register serves it."""
    document = {
        "id": "5882",
        "entity_type": "stsre",
        "type_s": "parent",
        "_root_": "5882",
        "_version_": 1882355611586265088,
        "timestamp": "2026-11-20T05:56:41.108Z",
        "stsre_notif_id": "2138001WO21Z3B8Y8K20N20260201",
        "stsre_sec_id": "2138001WO21Z3B8Y8K20N202602",
        "stsre_init_notif_dt": "2026-09-10T00:00:00Z",
        "stsre_notif_dt": "2026-09-10T00:00:00Z",
        "stsre_secclass_code": "NON_ABCP",
        "stsre_secclass_desc": "non-ABCP securitisation",
        "stsre_sectype_code": "PUBLIC",
        "stsre_sectype_desc": "Public",
        "stsre_compl_stat_desc": "Compliant",
        "stsre_sanc_status": "Not Sanctioned",
        "stsre_undrlexp_desc": "residential mortgages",
        "stsre_sec_name": ["Candide Financing 2026-2"],
        "stsre_sec_repository": ["European DataWarehouse GmbH"],
        "de_name": "Lloyds Bank GmbH",
        "de_lei_priv": "2138001WO21Z3B8Y8K20",
        "de_lei_name": "LLOYDS BANK GMBH",
        "authorised_third_party_name": "STS Verification International GmbH",
        "non_abcp_requirements_on_simplicity_Homogeneity_assets_flag": "C",
        "non_abcp_requirements_on_simplicity_Homogeneity_assets_comment":
            "The pool satisfies the homogeneity conditions.",
        "non_abcp_Riskretention_and_RetainingRiskoption_Firstlosstranche": "Y",
    }
    document.update(overrides)
    return document


def a_private_notification(**overrides):
    """One private notification: the core and nothing else."""
    document = {
        "id": "3415",
        "entity_type": "stsre",
        "type_s": "parent",
        "_root_": "3415",
        "stsre_notif_id": "Private_2020_12_18_01",
        "stsre_sec_id": "Private_2020_12_18_01",
        "stsre_init_notif_dt": "2020-12-18T00:00:00Z",
        "stsre_notif_dt": "2024-11-22T00:00:00Z",
        "stsre_secclass_code": "ABCP",
        "stsre_secclass_desc": "ABCP securitisation",
        "stsre_sectype_code": "PRIVATE",
        "stsre_sectype_desc": "Private",
        "stsre_compl_stat_desc": "Compliant",
        "stsre_sanc_status": "Not Sanctioned",
        "stsre_undrlexp_desc": "trade receivables",
    }
    document.update(overrides)
    return document


def children_of(parent="5882"):
    """One child of each kind, as the register serves them."""
    return [
        {"type_s": "child", "entity_type": "issins", "_root_": parent,
         "stsre_isin_instrument": "XS3461032990",
         "stsre_isin_name": "CANDIDE FINANCI/VARASST BKD 2064"},
        {"type_s": "child", "entity_type": "originator", "_root_": parent,
         "originator_lei": ["2138001WO21Z3B8Y8K20"],
         "originator_name": ["LLOYDS BANK GMBH"],
         "originator_country": ["DE"], "originator_country_name": "GERMANY"},
        {"type_s": "child", "entity_type": "sspe", "_root_": parent,
         "sspe_country_code": ["NL"], "sspe_country_name": "NETHERLANDS"},
        {"type_s": "child", "entity_type": "lender", "_root_": parent,
         "lender_lei": ["2138001WO21Z3B8Y8K20"],
         "lender_name": ["LLOYDS BANK GMBH"],
         "lender_country": ["DE"], "lender_country_name": "GERMANY"},
        {"type_s": "child", "entity_type": "sponsor", "_root_": parent,
         "sponsor_lei": ["549300U4O8B2TGA2EH40"], "sponsor_name": ["AXUS"],
         "sponsor_country": ["BE"], "sponsor_country_name": "BELGIUM"},
    ]


def write_snapshot(config_path, day, parents=(), children=()):
    """Write a snapshot into the Bronze layer, as a collection would."""
    for kind, documents in (("parent", parents), ("child", children)):
        answer = {"responseHeader": {"status": 0},
                  "response": {"numFound": len(documents), "start": 0,
                               "docs": list(documents)}}
        path = common.path_bronze_page(config_path, day, kind, 0)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(answer, handle)
    return day


# The values a field can hold
# --------------------------------------------------------------------------


def test_a_multivalued_field_keeps_every_value():
    assert parse.joined(["one", "two"]) == "one; two"
    assert parse.joined("one") == "one"
    assert parse.joined([]) is None
    assert parse.joined(None) is None


def test_a_number_is_rendered_as_the_register_means_it():
    # The register serves some fields as numbers, and str() would write a
    # prospectus identifier of 169403 as '169403.0'.
    assert parse.text_of(169403.0) == "169403"
    assert parse.text_of(169403) == "169403"
    assert parse.text_of("C-027919") == "C-027919"


def test_an_identifier_of_nothing_but_space_becomes_null():
    assert parse.trimmed(" ") is None
    assert parse.trimmed(" Private_2022_02_02_21") == "Private_2022_02_02_21"
    assert parse.trimmed(None) is None


def test_the_two_date_formats_of_one_notification_both_parse():
    assert parse.as_date("2026-09-10T00:00:00Z") == datetime.date(2026, 9, 10)
    assert parse.as_date("2026-09-10 00:00:00.0") == datetime.date(2026, 9, 10)
    assert parse.as_date("") is None
    assert parse.as_date("not a date") is None


@pytest.mark.parametrize("identifier,scheme", [
    ("2138001WO21Z3B8Y8K20N202602", "lei"),
    ("Private_2020_12_18_01", "private"),
    ("PRIVATE_STS2022_06_20_01", "private"),
    ("SYSEC_Private_2026_10_13_01", "synthetic"),
    (" ", "missing"),
    (None, "missing"),
    ("something else", "other"),
])
def test_the_scheme_of_an_identifier(identifier, scheme):
    assert common.identifier_scheme(identifier) == scheme


# Splitting a template field
# --------------------------------------------------------------------------


@pytest.mark.parametrize("field,expected", [
    ("non_abcp_requirements_on_simplicity_Homogeneity_assets_flag",
     ("non_abcp", "requirements_on_simplicity_Homogeneity_assets", "flag")),
    ("non_abcp_requirements_on_simplicity_Homogeneity_assets_comment",
     ("non_abcp", "requirements_on_simplicity_Homogeneity_assets", "comment")),
    ("abcp_requirementsOnStandardisation_ARE_NO_TRAPPING_CASH_FLAG",
     ("abcp", "requirementsOnStandardisation_ARE_NO_TRAPPING_CASH", "flag")),
    ("abcp_Transactiondocumentation_And_Revolvingsecuritisation_ATD_EARLY_TERMINATION_EXPL",
     ("abcp",
      "Transactiondocumentation_And_Revolvingsecuritisation_ATD_EARLY_TERMINATION",
      "comment")),
    # A field with no ending carries the assertion itself.
    ("non_abcp_Riskretention_and_RetainingRiskoption_Firstlosstranche",
     ("non_abcp", "Riskretention_and_RetainingRiskoption_Firstlosstranche",
      "flag")),
    ("stsre_sec_id", None),
    ("de_lei_priv", None),
])
def test_a_template_field_is_split_into_requirement_and_part(field, expected):
    assert parse.requirement_of(field) == expected


# Reading a snapshot
# --------------------------------------------------------------------------


def test_a_notification_becomes_a_row_of_the_schema(config):
    write_snapshot(config, "2026-11-20", [a_notification()])

    rows, _ = parse.notification_rows(config, "2026-11-20")

    assert len(rows) == 1
    row = dict(zip(common.notification_schema.names, rows[0]))
    assert row["id"] == "5882"
    assert row["securitisation_identifier"] == "2138001WO21Z3B8Y8K20N202602"
    assert row["identifier_scheme"] == "lei"
    assert row["securitisation_type"] == "Public"
    assert row["securitisation_name"] == "Candide Financing 2026-2"
    assert row["reporting_entity_lei"] == "2138001WO21Z3B8Y8K20"
    assert row["reporting_entity_legal_name"] == "LLOYDS BANK GMBH"
    assert row["first_notified"] == datetime.date(2026, 9, 10)
    assert row["snapshot"] == datetime.date(2026, 11, 20)
    assert len(rows[0]) == len(common.notification_schema.fields)


def test_the_assertions_of_a_notification_meet_flag_and_comment_in_one_row(config):
    write_snapshot(config, "2026-11-20", [a_notification()])

    _, assertions = parse.notification_rows(config, "2026-11-20")

    made = {row[3]: row for row in assertions}
    assert len(assertions) == 2
    homogeneity = made["requirements_on_simplicity_Homogeneity_assets"]
    assert homogeneity[2] == "non_abcp"
    assert homogeneity[4] == "C"
    assert homogeneity[5].startswith("The pool satisfies")
    # The one with no ending is a flag and has no comment.
    first_loss = made["Riskretention_and_RetainingRiskoption_Firstlosstranche"]
    assert (first_loss[4], first_loss[5]) == ("Y", None)


def test_a_private_notification_makes_no_assertion(config):
    write_snapshot(config, "2026-11-20", [a_private_notification()])

    rows, assertions = parse.notification_rows(config, "2026-11-20")

    assert len(rows) == 1
    assert assertions == []


def test_a_notification_whose_identifier_is_a_space_carries_none(config):
    write_snapshot(config, "2026-11-20",
                   [a_notification(stsre_sec_id=" ", stsre_notif_id=" ")])

    rows, _ = parse.notification_rows(config, "2026-11-20")

    row = dict(zip(common.notification_schema.names, rows[0]))
    assert row["securitisation_identifier"] is None
    assert row["identifier_scheme"] == "missing"


def test_the_children_become_tranches_and_parties(config):
    write_snapshot(config, "2026-11-20", [a_notification()], children_of())

    tranches, parties = parse.child_rows(
        config, "2026-11-20", {"5882": "2138001WO21Z3B8Y8K20N202602"})

    assert len(tranches) == 1
    tranche = dict(zip(common.tranche_schema.names, tranches[0]))
    assert tranche["isin"] == "XS3461032990"
    assert tranche["securitisation_identifier"] == "2138001WO21Z3B8Y8K20N202602"

    roles = {row[2]: dict(zip(common.party_schema.names, row))
             for row in parties}
    assert set(roles) == {"originator", "sspe", "lender", "sponsor"}
    assert roles["originator"]["lei"] == "2138001WO21Z3B8Y8K20"
    assert roles["sponsor"]["name"] == "AXUS"
    assert roles["originator"]["country"] == "DE"


def test_the_special_purpose_entity_has_a_country_and_nothing_else(config):
    write_snapshot(config, "2026-11-20", [a_notification()], children_of())

    _, parties = parse.child_rows(config, "2026-11-20", {})

    sspe = next(dict(zip(common.party_schema.names, row))
                for row in parties if row[2] == "sspe")
    assert sspe["lei"] is None
    assert sspe["name"] is None
    assert sspe["country"] == "NL"
    assert sspe["country_name"] == "NETHERLANDS"


def test_a_field_the_register_adds_is_named_rather_than_dropped(config):
    write_snapshot(config, "2026-11-20",
                   [a_notification(stsre_brand_new="something")])

    assert parse.unmapped_fields(config, "2026-11-20") == {"stsre_brand_new": 1}


def test_the_fields_of_todays_register_are_all_accounted_for(config):
    write_snapshot(config, "2026-11-20",
                   [a_notification(), a_private_notification()])

    assert parse.unmapped_fields(config, "2026-11-20") == {}


def test_several_pages_of_one_snapshot_are_read_in_order(config):
    for page, document in enumerate([a_notification(id="1"),
                                     a_notification(id="2")]):
        answer = {"responseHeader": {"status": 0},
                  "response": {"numFound": 2, "docs": [document]}}
        path = common.path_bronze_page(config, "2026-11-20", "parent", page)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(answer, handle)

    rows, _ = parse.notification_rows(config, "2026-11-20")

    assert [row[0] for row in rows] == ["1", "2"]


# The events
# --------------------------------------------------------------------------


def test_the_register_witnesses_notification_and_update(config):
    write_snapshot(config, "2026-11-20", [
        a_notification(),
        a_private_notification(),   # notified 2020, moved 2024
    ])
    rows, _ = parse.notification_rows(config, "2026-11-20")

    events = parse.register_events(rows, "2026-11-20")

    kinds = sorted((row[2], row[3].isoformat()) for row in events)
    assert kinds == [("notified", "2020-12-18"), ("notified", "2026-09-10"),
                     ("updated", "2024-11-22")]


def test_a_notification_the_register_marks_non_compliant_is_an_event(config):
    write_snapshot(config, "2026-11-20",
                   [a_notification(stsre_compl_stat_desc="Non - Compliant")])
    rows, _ = parse.notification_rows(config, "2026-11-20")

    events = parse.register_events(rows, "2026-11-20")

    bad = [row for row in events if row[2] == "non_compliant"]
    assert len(bad) == 1
    assert bad[0][4] == "Non - Compliant"
    assert bad[0][3] == datetime.date(2026, 11, 20)


def test_a_notification_with_no_identifier_witnesses_nothing(config):
    write_snapshot(config, "2026-11-20", [a_notification(stsre_sec_id=" ")])
    rows, _ = parse.notification_rows(config, "2026-11-20")

    assert parse.register_events(rows, "2026-11-20") == []


def test_what_one_snapshot_held_and_the_next_does_not(config):
    write_snapshot(config, "2026-11-01",
                   [a_notification(id="1"), a_notification(id="2",
                                                           stsre_sec_id="OTHER")])
    write_snapshot(config, "2026-11-20", [a_notification(id="1")])
    before, _ = parse.notification_rows(config, "2026-11-01")
    after, _ = parse.notification_rows(config, "2026-11-20")

    gone = parse.departures(before, after, "2026-11-20")

    assert len(gone) == 1
    assert gone[0][0] == "OTHER"
    assert gone[0][2] == "left_register"
    assert gone[0][3] == datetime.date(2026, 11, 20)
    assert gone[0][5] == "snapshot"


def test_nothing_departs_where_the_register_only_grew(config):
    write_snapshot(config, "2026-11-01", [a_notification(id="1")])
    write_snapshot(config, "2026-11-20",
                   [a_notification(id="1"), a_notification(id="2")])
    before, _ = parse.notification_rows(config, "2026-11-01")
    after, _ = parse.notification_rows(config, "2026-11-20")

    assert parse.departures(before, after, "2026-11-20") == []


# The spreadsheets
# --------------------------------------------------------------------------


def a_workbook(path, rows, above=3):
    """A spreadsheet in the shape ESMA publishes: a disclaimer, then a header."""
    from openpyxl import Workbook

    book = Workbook()
    sheet = book.active
    for _ in range(above):
        sheet.append(["Disclaimer:"])
    for row in rows:
        sheet.append(row)
    book.save(path)
    return str(path)


def test_the_header_is_found_below_the_disclaimer_block(tmp_path):
    path = a_workbook(tmp_path / "sheet.xlsx", [
        ["Notification number", "Notification date to ESMA",
         "Unique Identifier", "Private or Public"],
        [620, datetime.datetime(2022, 1, 14), "529900F746EVPZ6CFI16N202101",
         "Public"],
    ])

    rows = parse.sheet_rows(path)

    assert len(rows) == 1
    assert rows[0]["Unique Identifier"] == "529900F746EVPZ6CFI16N202101"
    assert rows[0]["Private or Public"] == "Public"


def test_a_note_below_the_table_is_not_a_record(tmp_path):
    path = a_workbook(tmp_path / "sheet.xlsx", [
        ["Notification number", "Unique Identifier"],
        [620, "529900F746EVPZ6CFI16N202101"],
        ["Outstanding STS securitisations: 494"],
    ])

    assert len(parse.sheet_rows(path)) == 1


def test_the_three_lists_name_the_identifier_column_differently():
    assert parse.sheet_identifier({"Unique Identifier": "A"}) == "A"
    assert parse.sheet_identifier(
        {"Unique Securitisation Identifier": "B"}) == "B"
    assert parse.sheet_identifier(
        {"Securitisation Unique Identifier": "C"}) == "C"
    assert parse.sheet_identifier({"Something else": "D"}) is None


def test_a_cancellation_carries_esmas_reason(tmp_path):
    path = a_workbook(tmp_path / "cancelled.xlsx", [
        ["Cancellation date to ESMA", "First notification date",
         "Unique Securitisation Identifier", "Reasons for cancellation"],
        [datetime.datetime(2022, 4, 6), datetime.datetime(2020, 8, 21),
         "Private_2020_08_21_01", "Teminated STS Transaction"],
    ])

    events = parse.sheet_events(path, "cancelled", "ESMA33-128-761",
                                "2026-11-20", "Cancellation date to ESMA",
                                "Reasons for cancellation")

    assert len(events) == 1
    assert events[0][2] == "cancelled"
    assert events[0][3] == datetime.date(2022, 4, 6)
    # ESMA's own spelling is carried rather than corrected.
    assert events[0][4] == "Teminated STS Transaction"
    assert events[0][5] == "ESMA33-128-761"
    assert events[0][6] == datetime.date(2026, 11, 20)


def test_the_synthetic_list_has_columns_of_its_own(tmp_path):
    path = a_workbook(tmp_path / "synthetic.xlsx", [
        ["Notification number", "Notification date to ESMA",
         "Securitisation Unique Identifier", "Private or Public",
         "Funded credit protection or unfunded credit protection",
         "Credit derivatives or Financial guarantees", "Underlying assets",
         "Securitisation Name", "Originator Country",
         "Administrative sanctions present (YES/NO)"],
        [228, datetime.datetime(2026, 10, 13), "SYSEC_Private_2026_10_13_01",
         "private", "Synthetic securitisation with funded credit protection.",
         "Financial guarantees", "4) Large corporates loans", "NA", "NA",
         "NO"],
    ])

    rows = parse.synthetic_rows(path, "2026-11-20")

    assert len(rows) == 1
    row = dict(zip(common.synthetic_schema.names, rows[0]))
    assert row["securitisation_identifier"] == "SYSEC_Private_2026_10_13_01"
    assert row["notification_number"] == 228
    assert row["notified"] == datetime.date(2026, 10, 13)
    assert row["protection"].startswith("Synthetic securitisation with funded")
    assert row["instrument"] == "Financial guarantees"
    assert row["source_as_of"] == datetime.date(2026, 11, 20)
