"""Collecting the STS register and the spreadsheets ESMA publishes beside it.

Nothing here goes to the network. The portal is stood in for at the one seam
the module has for it, :func:`fincommons.sts.bronze.fetch`, which is what lets
the tests cover the answers that decide whether this module is safe: a Solr
header whose status is not zero, a page that does not advance, a register that
serves fewer documents than it says it holds, and the page of HTML that
esma.europa.eu answers with where a spreadsheet has been moved.

The shape of the answers is the shape the live service returned on
20 November 2026: a ``responseHeader`` with a status and a ``response`` with
``numFound`` and ``docs``, the notifications as ``type_s: parent`` and their
tranches and parties as ``type_s: child`` hanging off a parent by ``_root_``.
"""

from __future__ import annotations

import json
import os

import pytest
import yaml

from fincommons.sts import bronze, common


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
    """One notification, in the shape the register answers in."""
    document = {
        "id": "5882",
        "entity_type": "stsre",
        "type_s": "parent",
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
        "de_lei_priv": "2138001WO21Z3B8Y8K20",
        "de_name": "Lloyds Bank GmbH",
    }
    document.update(overrides)
    return document


def a_tranche(**overrides):
    """One ISIN child, as the register answers it."""
    document = {
        "type_s": "child",
        "entity_type": "issins",
        "stsre_isin_instrument": "XS3461032990",
        "stsre_isin_name": "CANDIDE  FINANCI/VARASST  BKD 2064",
        "_root_": "5882",
    }
    document.update(overrides)
    return document


def an_answer(docs, num_found=None, status=0):
    """One page, in the envelope Solr answers in."""
    return {
        "responseHeader": {"status": status, "QTime": 1,
                           "params": {"q": "type_s:parent"}},
        "response": {"numFound": len(docs) if num_found is None else num_found,
                     "start": 0, "numFoundExact": True, "docs": docs},
    }


def serve(monkeypatch, pages, sheets=None):
    """Stand in for the portal.

    ``pages`` maps the kind of document to the list of pages the portal will
    answer with, in order. ``sheets`` maps a URL to the answer for it, as a
    triple of status, body and headers. Every request made is recorded, so a
    test can assert what was asked for.
    """
    asked = []
    served = {kind: list(answers) for kind, answers in pages.items()}

    def fetch(url, params=None, headers=None):
        asked.append({"url": url, "params": params or {},
                      "headers": headers or {}})
        if sheets is not None and url in sheets:
            return sheets[url]
        kind = (params or {}).get("q", "").split(":")[-1]
        queue = served.get(kind)
        if not queue:
            return 200, json.dumps(an_answer([])).encode("utf-8"), {}
        answer = queue.pop(0)
        if isinstance(answer, tuple):
            return answer
        return 200, json.dumps(answer).encode("utf-8"), {}

    monkeypatch.setattr(bronze, "fetch", fetch)
    return asked


def a_spreadsheet(body=None, status=200):
    """An answer that is a spreadsheet, as far as the module can tell."""
    return (status, body if body is not None else common.SHEET_MAGIC + b"rest",
            {"Content-Type": common.SHEET_CONTENT_TYPE,
             "Last-Modified": "Tue, 03 Nov 2026 09:00:00 GMT"})


def a_page_of_html(status=404):
    """What esma.europa.eu answers where a file has been moved."""
    return (status, b"\n<!DOCTYPE html>\n<html><body>Not found</body></html>",
            {"Content-Type": "text/html; charset=utf-8"})


# Collecting the register
# --------------------------------------------------------------------------


def test_a_snapshot_holds_both_kinds_of_document(config, monkeypatch):
    serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                        "child": [an_answer([a_tranche()])]})

    outcome = bronze.collect_register(config, day="2026-11-20")

    assert outcome["outcome"] == "collected"
    assert outcome["documents"] == {"parent": 1, "child": 1}
    assert os.path.exists(common.path_bronze_page(
        config, "2026-11-20", "parent", 0))
    assert os.path.exists(common.path_bronze_page(
        config, "2026-11-20", "child", 0))


def test_the_page_is_stored_as_it_came_back(config, monkeypatch):
    answer = an_answer([a_notification()])
    serve(monkeypatch, {"parent": [answer], "child": [an_answer([])]})

    bronze.collect_register(config, day="2026-11-20")

    stored = common.read_json(
        common.path_bronze_page(config, "2026-11-20", "parent", 0))
    assert stored == answer


def test_every_request_is_ordered_and_asks_for_all_fields(config, monkeypatch):
    asked = serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                                "child": [an_answer([a_tranche()])]})

    bronze.collect_register(config, day="2026-11-20")

    assert asked, "the portal was never asked"
    for request in asked:
        assert request["params"]["sort"] == common.PAGE_SORT
        assert request["params"]["fl"] == "*"
        assert request["params"]["wt"] == "json"


def test_paging_counts_past_what_it_has_collected(config, monkeypatch):
    notifications = [a_notification(id=str(n)) for n in range(5)]
    asked = serve(monkeypatch, {
        "parent": [an_answer(notifications[:2], num_found=5),
                   an_answer(notifications[2:4], num_found=5),
                   an_answer(notifications[4:], num_found=5)],
        "child": [an_answer([])],
    })

    outcome = bronze.collect_register(config, day="2026-11-20", page_size=2)

    assert outcome["documents"]["parent"] == 5
    assert outcome["pages"]["parent"] == 3
    starts = [r["params"]["start"] for r in asked
              if r["params"]["q"] == "type_s:parent"]
    assert starts == [0, 2, 4]


def test_a_register_that_serves_less_than_it_claims_says_so(config,
                                                            monkeypatch,
                                                            caplog):
    serve(monkeypatch, {"parent": [an_answer([a_notification()], num_found=9),
                                   an_answer([], num_found=9)],
                        "child": [an_answer([])]})

    with caplog.at_level("WARNING"):
        outcome = bronze.collect_register(config, day="2026-11-20")

    assert outcome["documents"]["parent"] == 1
    assert outcome["stated_totals"]["parent"] == 9
    assert "Fewer documents were collected" in caplog.text


def test_a_field_that_has_gone_missing_is_reported(config, monkeypatch,
                                                   caplog):
    thin = a_notification()
    del thin["stsre_sec_id"]
    serve(monkeypatch, {"parent": [an_answer([thin])],
                        "child": [an_answer([])]})

    with caplog.at_level("WARNING"):
        outcome = bronze.collect_register(config, day="2026-11-20")

    assert outcome["fields_missing"] == ["stsre_sec_id"]
    assert "no longer carries fields" in caplog.text


def test_the_record_holds_a_census_of_the_fields(config, monkeypatch):
    serve(monkeypatch, {
        "parent": [an_answer([a_notification(id="1"),
                              a_notification(id="2", stsre_new="x")])],
        "child": [an_answer([])],
    })

    outcome = bronze.collect_register(config, day="2026-11-20")

    assert outcome["fields_missing"] == []
    assert outcome["field_counts"]["stsre_sec_id"] == 2
    assert outcome["field_counts"]["stsre_new"] == 1
    assert outcome["fields"] == len(outcome["field_counts"])


def test_a_snapshot_already_held_is_left_alone(config, monkeypatch):
    serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                        "child": [an_answer([a_tranche()])]})
    bronze.collect_register(config, day="2026-11-20")

    asked = serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                                "child": [an_answer([])]})
    outcome = bronze.collect_register(config, day="2026-11-20")

    assert outcome == {"outcome": "held", "day": "2026-11-20"}
    assert asked == []


def test_force_takes_the_snapshot_again(config, monkeypatch):
    serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                        "child": [an_answer([])]})
    bronze.collect_register(config, day="2026-11-20")

    serve(monkeypatch, {"parent": [an_answer([a_notification(id="1"),
                                              a_notification(id="2")])],
                        "child": [an_answer([])]})
    outcome = bronze.collect_register(config, day="2026-11-20", force=True)

    assert outcome["outcome"] == "collected"
    assert outcome["documents"]["parent"] == 2


def test_the_snapshots_held_are_listed_oldest_first(config, monkeypatch):
    for day in ("2026-11-20", "2026-10-01", "2026-11-01"):
        serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                            "child": [an_answer([])]})
        bronze.collect_register(config, day=day)

    assert common.collected_snapshots(config) == [
        "2026-10-01", "2026-11-01", "2026-11-20"]


def test_the_pages_of_a_snapshot_are_listed_in_the_order_fetched(config,
                                                                 monkeypatch):
    notifications = [a_notification(id=str(n)) for n in range(3)]
    serve(monkeypatch, {
        "parent": [an_answer(notifications[:1], num_found=3),
                   an_answer(notifications[1:2], num_found=3),
                   an_answer(notifications[2:], num_found=3)],
        "child": [an_answer([])],
    })
    bronze.collect_register(config, day="2026-11-20", page_size=1)

    pages = common.snapshot_pages(config, "2026-11-20", "parent")
    assert [os.path.basename(p) for p in pages] == [
        "parent-0000.json", "parent-0001.json", "parent-0002.json"]


# What the portal can answer instead
# --------------------------------------------------------------------------


def test_a_solr_status_that_is_not_zero_raises(config, monkeypatch):
    serve(monkeypatch, {"parent": [an_answer([], status=3)]})

    with pytest.raises(RuntimeError, match="Solr status"):
        bronze.collect_register(config, day="2026-11-20")


def test_an_http_error_raises(config, monkeypatch):
    serve(monkeypatch, {"parent": [(503, b"", {})]})

    with pytest.raises(RuntimeError, match="answered 503"):
        bronze.collect_register(config, day="2026-11-20")


def test_an_answer_that_is_not_json_raises(config, monkeypatch):
    serve(monkeypatch, {"parent": [(200, b"<html>maintenance</html>", {})]})

    with pytest.raises(RuntimeError, match="not JSON"):
        bronze.collect_register(config, day="2026-11-20")


def test_an_answer_with_no_result_set_raises(config, monkeypatch):
    body = json.dumps({"responseHeader": {"status": 0}}).encode("utf-8")
    serve(monkeypatch, {"parent": [(200, body, {})]})

    with pytest.raises(RuntimeError, match="no result set"):
        bronze.collect_register(config, day="2026-11-20")


def test_an_unknown_kind_of_document_raises(config):
    with pytest.raises(ValueError, match="not recognised"):
        bronze.fetch_page("grandchild")


def test_a_register_that_never_ends_is_given_up_on(config, monkeypatch):
    monkeypatch.setattr(bronze, "MAX_PAGES", 3)
    endless = an_answer([a_notification()], num_found=10 ** 9)
    serve(monkeypatch, {"parent": [endless] * 10})

    with pytest.raises(RuntimeError, match="did not end after 3 pages"):
        bronze.collect_register(config, day="2026-11-20")


def test_no_record_is_written_where_the_collection_failed(config, monkeypatch):
    serve(monkeypatch, {"parent": [(503, b"", {})]})

    with pytest.raises(RuntimeError):
        bronze.collect_register(config, day="2026-11-20")

    assert common.read_json(
        common.path_register_record(config, "2026-11-20")) is None


# The spreadsheets
# --------------------------------------------------------------------------


def test_a_spreadsheet_is_collected_with_a_record(config, monkeypatch):
    serve(monkeypatch, {}, sheets={common.SHEETS["synthetic"]: a_spreadsheet()})

    assert bronze.collect_sheet(config, "synthetic",
                               day="2026-11-20") == "collected"

    assert os.path.exists(common.path_bronze_sheet(
        config, "2026-11-20", "synthetic"))
    record = common.read_json(
        common.path_sheet_record(config, "2026-11-20", "synthetic"))
    assert record["outcome"] == "collected"
    assert record["reference"] == "ESMA82-402-39"
    assert record["last_modified"] == "Tue, 03 Nov 2026 09:00:00 GMT"


def test_a_spreadsheet_is_filed_under_the_day_it_was_collected(config,
                                                               monkeypatch):
    serve(monkeypatch, {}, sheets={common.SHEETS["synthetic"]: a_spreadsheet()})

    bronze.collect_sheet(config, "synthetic", day="2026-10-01")
    bronze.collect_sheet(config, "synthetic", day="2026-11-20")

    for day in ("2026-10-01", "2026-11-20"):
        assert os.path.exists(
            common.path_bronze_sheet(config, day, "synthetic"))


def test_a_page_of_html_is_not_stored_as_a_spreadsheet(config, monkeypatch,
                                                       caplog):
    serve(monkeypatch, {},
          sheets={common.SHEETS["traditional"]: a_page_of_html()})

    with caplog.at_level("WARNING"):
        assert bronze.collect_sheet(config, "traditional",
                                   day="2026-11-20") == "unavailable"

    assert not os.path.exists(common.path_bronze_sheet(
        config, "2026-11-20", "traditional"))
    record = common.read_json(
        common.path_sheet_record(config, "2026-11-20", "traditional"))
    assert record["outcome"] == "unavailable"
    assert record["status"] == 404
    assert "no longer serves a spreadsheet" in caplog.text


def test_a_status_200_that_is_not_a_spreadsheet_is_refused(config,
                                                           monkeypatch):
    serve(monkeypatch, {},
          sheets={common.SHEETS["cancelled"]: a_page_of_html(status=200)})

    assert bronze.collect_sheet(config, "cancelled",
                               day="2026-11-20") == "unavailable"


def test_a_spreadsheet_already_held_is_left_alone(config, monkeypatch):
    serve(monkeypatch, {}, sheets={common.SHEETS["synthetic"]: a_spreadsheet()})
    bronze.collect_sheet(config, "synthetic", day="2026-11-20")

    asked = serve(monkeypatch, {},
                  sheets={common.SHEETS["synthetic"]: a_spreadsheet()})
    assert bronze.collect_sheet(config, "synthetic",
                               day="2026-11-20") == "held"
    assert asked == []


def test_an_unknown_spreadsheet_raises(config):
    with pytest.raises(ValueError, match="not recognised"):
        bronze.collect_sheet(config, "brexit")


def test_one_spreadsheet_failing_does_not_stop_the_others(config, monkeypatch):
    sheets = {url: a_spreadsheet() for url in common.SHEETS.values()}
    sheets[common.SHEETS["traditional"]] = a_page_of_html()
    serve(monkeypatch, {}, sheets=sheets)

    outcomes = bronze.collect_sheets(config, day="2026-11-20")

    assert outcomes["traditional"] == "unavailable"
    assert outcomes["cancelled"] == "collected"
    assert outcomes["synthetic"] == "collected"
    assert outcomes["repositories"] == "collected"


# The whole thing in one call
# --------------------------------------------------------------------------


def test_the_pipeline_collects_the_register_and_the_spreadsheets(config,
                                                                 monkeypatch):
    sheets = {url: a_spreadsheet() for url in common.SHEETS.values()}
    serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                        "child": [an_answer([a_tranche()])]}, sheets=sheets)

    bronze.update_bronze_sts(config, day="2026-11-20")

    assert common.collected_snapshots(config) == ["2026-11-20"]
    for name in common.SHEETS:
        assert os.path.exists(
            common.path_bronze_sheet(config, "2026-11-20", name))


def test_the_pipeline_can_collect_the_register_alone(config, monkeypatch):
    serve(monkeypatch, {"parent": [an_answer([a_notification()])],
                        "child": [an_answer([])]})

    bronze.update_bronze_sts(config, sheets=False, day="2026-11-20")

    assert common.collected_snapshots(config) == ["2026-11-20"]
    assert not os.path.isdir(os.path.join(
        common.path_bronze_sheets(config), "2026-11-20"))


def test_the_pipeline_raises_where_the_register_could_not_be_collected(
        config, monkeypatch):
    serve(monkeypatch, {"parent": [(503, b"", {})]})

    with pytest.raises(RuntimeError):
        bronze.update_bronze_sts(config, sheets=False, day="2026-11-20")


# The layout of the data lake
# --------------------------------------------------------------------------


def test_the_directory_tree_is_made_where_the_configuration_says(config):
    for folder in (common.path_bronze_base(config),
                   common.path_bronze_register(config),
                   common.path_bronze_sheets(config),
                   common.path_bronze_collections(config),
                   common.path_silver_base(config)):
        assert os.path.isdir(folder)


def test_a_lake_with_no_section_for_the_module_is_named_in_the_error(tmp_path):
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump({"firds": {"bronze": "/data"}}))

    with pytest.raises(KeyError, match="sts"):
        common.path_bronze_base(str(config_path))


def test_the_silver_layer_may_be_a_schema_rather_than_a_directory(tmp_path):
    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"sts": {"bronze": str(base / "bronze"),
                 "silver": {"table": "fincommons.sts"}}}
    ))
    config_path = str(config_path)

    common.create_directory_tree(config_path)

    assert common.path_silver_base(config_path) is None
    assert os.path.isdir(common.path_bronze_register(config_path))


def test_no_snapshots_are_reported_for_an_empty_lake(config):
    assert common.collected_snapshots(config) == []
    assert common.snapshot_pages(config, "2026-11-20") == []
