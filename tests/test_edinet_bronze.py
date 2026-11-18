"""Collecting the indexes, the filings and the list of filers.

Nothing here goes to the network. The agency is stood in for at the one seam
the module has for it, :func:`fincommons.financialreport.edinet.bronze.fetch`,
which is what lets the tests cover the answers that decide whether this
module is safe: an error the agency sends with HTTP status 200, an error it
sends as JSON where an archive was asked for, and a day the retention window
no longer reaches.

The one thing that is read from real data rather than constructed is the
layout of the list of filers, which is built here as the agency builds it --
a zip holding one CSV in the Japanese industrial encoding, with the day it
was produced on the first line and the header on the second.
"""

from __future__ import annotations

import csv
import datetime
import io
import json
import os
import zipfile
from pathlib import Path

import pytest

from fincommons.financialreport.edinet import bronze, common


@pytest.fixture
def config(tmp_path):
    """A configuration file pointing at an empty data lake."""
    import yaml

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"edinet": {name: str(base / name) for name in ("bronze", "silver")}}
    ))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


def an_index(count=1, **overrides):
    """One day's index, in the shape the API answers in."""
    entry = {
        "seqNumber": 1,
        "docID": "S100VWVY",
        "edinetCode": "E02144",
        "secCode": "72030",
        "JCN": "1180301018771",
        "filerName": "トヨタ自動車株式会社",
        "fundCode": None,
        "ordinanceCode": "010",
        "formCode": "030000",
        "docTypeCode": "120",
        "periodStart": "2024-04-01",
        "periodEnd": "2025-03-31",
        "submitDateTime": "2025-06-18 15:30",
        "docDescription": "有価証券報告書－第121期",
        "parentDocID": None,
        "withdrawalStatus": "0",
        "docInfoEditStatus": "0",
        "disclosureStatus": "0",
        "xbrlFlag": "1",
        "pdfFlag": "1",
        "attachDocFlag": "1",
        "englishDocFlag": "0",
        "csvFlag": "1",
        "legalStatus": "1",
    }
    entry.update(overrides)
    return {
        "metadata": {"title": "提出された書類を把握するための API",
                     "parameter": {"date": "2025-06-18", "type": "2"},
                     "resultset": {"count": count},
                     "processDateTime": "2025-06-19 00:00",
                     "status": "200", "message": "OK"},
        "results": [entry] * count,
    }


def answers(monkeypatch, status=200, body=b"", headers=None):
    """Stand in for the agency with one answer."""
    monkeypatch.setattr(bronze, "fetch",
                        lambda url, params=None, headers_=None, **kw:
                        (status, body, headers or {}))


def a_filers_zip():
    """The list of filers, built as the agency builds it."""
    header = ["ＥＤＩＮＥＴコード", "提出者種別", "上場区分", "連結の有無",
              "資本金", "決算日", "提出者名", "提出者名（英字）",
              "提出者名（ヨミ）", "所在地", "提出者業種", "証券コード",
              "提出者法人番号"]
    rows = [
        ["ダウンロード実行日", "2026年11月14日現在", "件数", "2件"],
        header,
        ["E00004", "内国法人・組合", "上場", "有", "1491", "5月31日",
         "カネコ種苗株式会社", "KANEKO SEEDS CO., LTD.",
         "カネコシュビョウカブシキガイシャ", "前橋市古市町一丁目５０番地１２",
         "水産・農林業", "13760", "5070001000715"],
        ["E02144", "内国法人・組合", "上場", "有", "635401", "3月31日",
         "トヨタ自動車株式会社", "TOYOTA MOTOR CORPORATION",
         "トヨタジドウシャカブシキガイシャ", "愛知県豊田市トヨタ町1番地",
         "輸送用機器", "72030", "1180301018771"],
    ]
    text = io.StringIO()
    csv.writer(text, lineterminator="\r\n").writerows(rows)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("EdinetcodeDlInfo.csv",
                         text.getvalue().encode(common.FILERS_ENCODING))
    return buffer.getvalue()


# The error the agency hides in the body of a success
# --------------------------------------------------------------------------


def test_an_error_inside_a_200_is_raised_and_not_stored(monkeypatch, config):
    """The index API answers a bad key with HTTP 200 and the real status in
    the body. A run that trusted the status would store the refusal as the
    index of the day and report success."""
    answers(monkeypatch, 200, json.dumps(
        {"StatusCode": 401,
         "message": "Access denied due to invalid subscription key."}
    ).encode())

    with pytest.raises(RuntimeError, match="401"):
        bronze.collect_index(config, "2026-01-05", key="wrong")

    assert not os.path.exists(common.path_bronze_index(config, "2026-01-05"))


def test_the_refusal_of_a_key_says_where_to_check_it(monkeypatch, config):
    answers(monkeypatch, 200,
            json.dumps({"StatusCode": 401, "message": "Access denied"}).encode())

    with pytest.raises(RuntimeError, match="disclosure2.edinet-fsa.go.jp"):
        bronze.collect_index(config, "2026-01-05", key="wrong")


def test_an_error_in_the_metadata_status_is_raised_too(monkeypatch, config):
    """The index API states its status in the metadata; a document that
    carries one other than 200 there is an error however it arrived."""
    document = an_index()
    document["metadata"]["status"] = "400"
    document["metadata"]["message"] = "リクエスト内容が誤っています。"
    answers(monkeypatch, 200, json.dumps(document).encode())

    with pytest.raises(RuntimeError, match="400"):
        bronze.collect_index(config, "2026-01-05", key="k")


def test_a_body_that_is_not_json_is_refused(monkeypatch, config):
    answers(monkeypatch, 200, b"<html>Sorry</html>")

    with pytest.raises(RuntimeError, match="not JSON"):
        bronze.collect_index(config, "2026-01-05", key="k")


# The retention window
# --------------------------------------------------------------------------


def test_the_window_is_ten_years_and_never_before_the_renewal():
    assert common.RETENTION_YEARS == 10
    assert common.earliest_retrievable(datetime.date(2026, 11, 14)) \
        == datetime.date(2016, 11, 14)
    # Ten years before 2020 is before the system renewal, which the API does
    # not reach whatever the retention would allow.
    assert common.earliest_retrievable(datetime.date(2020, 1, 1)) \
        == common.FIRST_DATE


def test_a_leap_day_does_not_break_the_window():
    assert common.earliest_retrievable(datetime.date(2028, 2, 29)) \
        == datetime.date(2018, 2, 28)


def test_a_day_past_the_window_is_not_asked_for(monkeypatch, config):
    """The API answers for such a day with an empty list rather than an error,
    which a run would store as 'nothing was filed that day'."""
    def refuse(*args, **kwargs):
        raise AssertionError("the agency should not have been asked")

    monkeypatch.setattr(bronze, "fetch", refuse)
    old = (datetime.date.today() - datetime.timedelta(days=366 * 11)).isoformat()

    assert bronze.collect_index(config, old, key="k") == "expired"
    assert not os.path.exists(common.path_bronze_index(config, old))


# Collecting an index
# --------------------------------------------------------------------------


def test_an_index_is_stored_and_the_second_run_asks_again_for_nothing(
        monkeypatch, config):
    answers(monkeypatch, 200, json.dumps(an_index()).encode())

    assert bronze.collect_index(config, "2026-01-05", key="k") == "collected"
    assert os.path.exists(common.path_bronze_index(config, "2026-01-05"))
    assert bronze.collected_indexes(config) == ["2026-01-05"]

    def refuse(*args, **kwargs):
        raise AssertionError("the agency should not have been asked again")

    monkeypatch.setattr(bronze, "fetch", refuse)
    assert bronze.collect_index(config, "2026-01-05", key="k") == "held"


def test_the_record_of_a_collection_keeps_no_key(monkeypatch, config):
    """The key travels as a query parameter, so the address of a request
    carries the secret and the address is what a record would keep."""
    answers(monkeypatch, 200, json.dumps(an_index()).encode())
    bronze.collect_index(config, "2026-01-05", key="super-secret")

    written = Path(common.path_index_record(config, "2026-01-05")) \
        .read_text(encoding="utf-8")

    assert "super-secret" not in written
    assert "***" in written
    assert '"count": 1' in written


# Reading an index into filings
# --------------------------------------------------------------------------


def test_a_filing_is_read_into_the_columns_of_the_table(config):
    rows = bronze.filings_from_index(an_index(), "2025-06-18")
    names = [f.name for f in common.filings_schema.fields]
    row = dict(zip(names, rows[0]))

    assert len(rows) == 1
    assert len(rows[0]) == len(names)
    assert row["filing"] == "S100VWVY"
    assert row["edinet_code"] == "E02144"
    assert row["sec_code"] == "72030"
    assert row["jcn"] == "1180301018771"
    assert row["doc_type"] == "120"
    assert row["period_start"] == datetime.date(2024, 4, 1)
    assert row["period_end"] == datetime.date(2025, 3, 31)
    assert row["submitted"] == datetime.datetime(2025, 6, 18, 15, 30)
    assert row["has_csv"] == "1"
    assert row["index_date"] == datetime.date(2025, 6, 18)


def test_the_year_filed_comes_from_the_submission_and_not_the_index(config):
    """A filing lodged on paper is indexed later than it was submitted, and
    the partition follows the submission."""
    rows = bronze.filings_from_index(
        an_index(submitDateTime="2025-06-18 15:30"), "2025-07-02")
    names = [f.name for f in common.filings_schema.fields]
    row = dict(zip(names, rows[0]))

    assert row["filed_year"] == 2025
    assert row["index_date"] == datetime.date(2025, 7, 2)


def test_an_entry_with_no_document_number_is_passed_over(config):
    rows = bronze.filings_from_index(an_index(docID=None), "2025-06-18")

    assert rows == []


def test_a_day_with_nothing_filed_reads_as_no_rows(config):
    assert bronze.filings_from_index({"results": []}, "2026-01-01") == []
    assert bronze.filings_from_index({}, "2026-01-01") == []


def test_an_index_that_cannot_be_read_is_skipped_rather_than_fatal(config):
    """One unreadable day must not cost the other three thousand."""
    common.write_json(common.path_bronze_index(config, "2026-01-05"),
                      an_index())
    broken = common.path_bronze_index(config, "2026-01-06")
    os.makedirs(os.path.dirname(broken), exist_ok=True)
    Path(broken).write_text("{ not json", encoding="utf-8")

    rows = bronze.read_filings(config)

    assert len(rows) == 1


# Collecting a filing
# --------------------------------------------------------------------------


def test_an_error_sent_as_json_is_not_stored_as_an_archive(monkeypatch, config):
    """The document API signals a failure by the content type: a filing is an
    archive and an error is JSON."""
    answers(monkeypatch, 200,
            json.dumps({"StatusCode": 400, "message": "誤っています"}).encode(),
            {"Content-Type": "application/json; charset=utf-8"})

    with pytest.raises(RuntimeError, match="refused"):
        bronze.collect_document(config, "S100VWVY", key="k")

    assert not os.path.exists(common.path_bronze_document(config, "S100VWVY"))


def test_a_form_the_agency_does_not_hold_is_an_answer_and_not_a_failure(
        monkeypatch, config):
    answers(monkeypatch, 200,
            json.dumps({"StatusCode": 404, "message": "not found"}).encode(),
            {"Content-Type": "application/json"})

    assert bronze.collect_document(config, "S100VWVY", key="k",
                                   kind="english") == "absent"


def test_a_body_that_is_not_an_archive_is_refused(monkeypatch, config):
    answers(monkeypatch, 200, b"not a zip at all",
            {"Content-Type": "application/octet-stream"})

    with pytest.raises(RuntimeError, match="not a readable archive"):
        bronze.collect_document(config, "S100VWVY", key="k")


def test_an_archive_is_stored_and_held(monkeypatch, config):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("jpcrp030000-asr-001.csv", "要素ID\t項目名\n")
    answers(monkeypatch, 200, buffer.getvalue(),
            {"Content-Type": "application/octet-stream"})

    assert bronze.collect_document(config, "S100VWVY", key="k") == "collected"
    assert os.path.exists(common.path_bronze_document(config, "S100VWVY"))
    assert bronze.collect_document(config, "S100VWVY", key="k") == "held"


def test_an_unknown_form_is_refused_before_a_request_is_made(config):
    with pytest.raises(ValueError, match="no form of a filing"):
        bronze.collect_document(config, "S100VWVY", key="k", kind="xbrl")


# The list of filers, which needs no key
# --------------------------------------------------------------------------


def test_the_filers_list_is_read_past_the_line_that_is_not_a_header(
        monkeypatch, config):
    """The agency writes the day the file was produced on the first line, so
    a reader that takes it for the header finds one column and no filers."""
    answers(monkeypatch, 200, a_filers_zip())
    bronze.collect_filers(config)

    rows = bronze.read_filers(config)
    names = [f.name for f in common.filers_schema.fields]

    assert len(rows) == 2
    first = dict(zip(names, rows[0]))
    assert first["edinet_code"] == "E00004"
    assert first["name_en"] == "KANEKO SEEDS CO., LTD."
    assert first["sec_code"] == "13760"
    assert first["jcn"] == "5070001000715"
    # Carried as the agency writes it, there being no year in it.
    assert first["year_end"] == "5月31日"


def test_the_japanese_encoding_survives(monkeypatch, config):
    answers(monkeypatch, 200, a_filers_zip())
    bronze.collect_filers(config)

    rows = bronze.read_filers(config)
    names = [f.name for f in common.filers_schema.fields]
    toyota = dict(zip(names, rows[1]))

    assert toyota["name"] == "トヨタ自動車株式会社"
    assert "�" not in toyota["name"]


def test_a_lake_with_no_filers_list_is_told_it_needs_none_of_a_key(config):
    with pytest.raises(RuntimeError, match="needs no API key"):
        bronze.read_filers(config)


def test_an_answer_that_is_not_a_list_of_filers_is_refused(monkeypatch, config):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("readme.txt", "nothing here")
    answers(monkeypatch, 200, buffer.getvalue())

    with pytest.raises(ValueError, match="holds no CSV"):
        bronze.collect_filers(config)


# The run
# --------------------------------------------------------------------------


def test_a_range_that_ends_before_it_begins_is_refused(config):
    with pytest.raises(ValueError, match="ends before it begins"):
        bronze.update_bronze_edinet(config, start="2026-02-01",
                                    end="2026-01-01", filers=False)


def test_a_run_collects_every_day_of_the_range(monkeypatch, config):
    asked = []

    def one_day(cfg, day, key=None, force=False):
        asked.append(str(day))
        return "collected"

    monkeypatch.setattr(bronze, "collect_index", one_day)
    monkeypatch.setattr(bronze, "collect_filers", lambda *a, **k: 0)
    bronze.update_bronze_edinet(config, start="2026-01-01", end="2026-01-04",
                                key="k")

    assert asked == ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
