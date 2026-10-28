"""Tests of the tables of ESEF filings in the Silver layer.

The Silver layer lists every filing of the indexes in one table and reads
every report of the Bronze layer into a table of facts, rebuilt country by
country for the countries whose reports changed. The tests fill the Bronze
layer with the small indexes and reports in ``tests/data/esef`` and check
what the tables hold, which countries a run rebuilds and what happens to a
report that cannot be read -- in parquet and in Delta.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import shutil
from pathlib import Path

import pandas
import pytest
import requests

import fincommons.esef as esef
from fincommons.esef import bronze as esef_bronze

FIXTURES = Path(__file__).resolve().parent / "data" / "esef"
LEI_AT = "5299000000000000AT01"
LEI_DE = "5299000000000000DE01"
AT = f"{LEI_AT}-2026-03-31-ESEF-AT-0"
AT2 = "5299000000000000AT02-2025-12-31-ESEF-AT-0"
DE = f"{LEI_DE}-2025-12-31-ESEF-DE-0"
NO_JSON = f"{LEI_AT}-2024-03-31-ESEF-AT-0"


def load(name):
    with open(FIXTURES / name, encoding="utf-8") as handle:
        return json.load(handle)


def report_name(filing):
    lei, date = filing[:20], filing[21:31]
    return f"{lei}-{date}-1-{'en' if filing == AT2 else 'de'}.json"


@pytest.fixture
def data_lake(tmp_path):
    """A configuration file pointing at an empty data lake."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "esef:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    esef.create_directory_tree(config_path)
    return config_path


def collect(config_path, filing, document=None):
    """Put the report of a filing into the Bronze layer as a collection would."""
    document = load(report_name(filing)) if document is None else document
    content = json.dumps(document).encode()
    esef.write_bytes(esef.path_bronze_report(config_path, filing[-4:-2], filing),
                     content)
    esef.write_json(esef.path_report_record(config_path, filing), {
        "filing": filing, "country": filing[-4:-2], "json_url": None,
        "processed": None, "sha256": hashlib.sha256(content).hexdigest(),
        "facts": len(document["facts"]), "collected": "2026-10-28T12:00:00"})


@pytest.fixture
def collected(data_lake):
    """A Bronze layer holding the indexes and the reports of Austria and Germany."""
    for year in (2024, 2025, 2026):
        shutil.copy(FIXTURES / f"filings-{year}.json",
                    esef.path_bronze_index(data_lake, year))
    for filing in (AT, AT2, DE):
        collect(data_lake, filing)
    return data_lake


def facts(spark, config_path):
    return esef.connect_to_esef(spark, config_path).toPandas()


def test_the_table_of_filings_lists_every_filing_of_the_indexes(
        spark, data_lake, collected):
    outcome = esef.update_silver_esef(spark, data_lake)

    assert outcome["filings"] == 4
    filings = esef.connect_to_esef_filings(spark, data_lake).toPandas() \
        .set_index("FILING")
    assert sorted(filings.index) == sorted(
        [AT, AT2, DE, NO_JSON])
    assert filings.loc[AT, "NAME"] == "Erste Beispiel AG"
    assert filings.loc[AT, "PERIODEND"] == datetime.date(2026, 3, 31)
    # A null, which pandas 3 hands back as NaN and pandas 2 as None.
    assert pandas.isna(filings.loc[NO_JSON, "JSONURL"])


def test_the_table_of_facts_holds_every_fact_of_every_report(
        spark, data_lake, collected):
    outcome = esef.update_silver_esef(spark, data_lake)

    assert outcome["written"] == 2 and outcome["failed"] == 0
    frame = facts(spark, data_lake)
    assert outcome["rows"] == len(frame) == 9  # three facts of each report
    assert set(frame["COUNTRY"]) == {"AT", "DE"}

    revenue = frame[(frame["FILING"] == AT)
                    & (frame["CONCEPT"] == "ifrs-full:Revenue")].iloc[0]
    assert revenue["LEI"] == LEI_AT
    assert revenue["PERIODEND"] == datetime.date(2026, 3, 31)
    assert revenue["PRDSTART"] == datetime.date(2025, 4, 1)
    assert revenue["PRDEND"] == datetime.date(2026, 3, 31)
    assert revenue["VALUE"] == 430569203.0
    assert revenue["UNIT"] == "iso4217:EUR"

    expected = esef.facts_from_report(load(report_name(AT)))
    mine = frame[frame["FILING"] == AT].sort_values("FACT")
    assert list(mine["FACT"]) == list(expected["FACT"])
    assert list(mine["TEXT"].fillna("")) == list(expected["TEXT"].fillna(""))


def test_only_the_countries_whose_reports_changed_are_rebuilt(
        spark, data_lake, collected):
    esef.update_silver_esef(spark, data_lake)

    assert esef.update_silver_esef(spark, data_lake)["written"] == 0

    # Germany's report is collected anew with another revenue.
    changed = load(report_name(DE))
    changed["facts"]["f-1"]["value"] = "7.0"
    collect(data_lake, DE, changed)

    outcome = esef.update_silver_esef(spark, data_lake)

    assert outcome["written"] == 1
    frame = facts(spark, data_lake)
    revenue = frame[frame["CONCEPT"] == "ifrs-full:Revenue"].set_index("LEI")["VALUE"]
    assert revenue[LEI_DE] == 7.0
    assert revenue[LEI_AT] == 430569203.0
    assert len(frame) == 9


def test_a_full_rebuild_reads_every_country_again(spark, data_lake, collected):
    esef.update_silver_esef(spark, data_lake)

    outcome = esef.update_silver_esef(spark, data_lake, full=True,
                                      countries=["DE"])

    assert outcome["written"] == 1


def test_a_report_that_cannot_be_read_is_set_aside_and_its_country_not_marked(
        spark, data_lake, collected):
    with open(esef.path_bronze_report(data_lake, "AT", AT2), "w",
              encoding="utf-8") as handle:
        handle.write("{ not json")

    outcome = esef.update_silver_esef(spark, data_lake)

    assert outcome["failed_sources"] == [f"silver/{AT2}"]
    frame = facts(spark, data_lake)
    assert set(frame["FILING"]) == {AT, DE}
    # The country is tried again on the next run, Germany is not.
    assert list(esef.pending_countries(data_lake)) == ["AT"]


def test_the_tables_can_be_written_as_delta(spark, data_lake, collected):
    outcome = esef.update_silver_esef(spark, data_lake, file_format="delta")

    assert outcome["written"] == 2
    assert os.path.isdir(os.path.join(esef.ref_silver_facts(data_lake).path,
                                      "_delta_log"))
    assert len(facts(spark, data_lake)) == 9


class Answer:
    def __init__(self, status, body=b"", headers=None):
        self.status_code, self.body, self.headers = status, body, headers or {}

    def iter_content(self, chunk_size=8192):
        yield self.body

    def close(self):
        pass


def serve(url, **kwargs):
    """filings.xbrl.org, serving the fixtures."""
    name = url.rsplit("/", 1)[1]
    if (FIXTURES / name).exists():
        return Answer(200, (FIXTURES / name).read_bytes(),
                      {"Last-Modified": "Mon, 26 Oct 2026 17:06:56 GMT"})
    return Answer(404)


def test_the_pipeline_runs_from_the_repository_to_the_tables(
        spark, data_lake, monkeypatch):
    monkeypatch.setattr(requests, "get", serve)
    monkeypatch.setattr(esef_bronze, "current_year", lambda now=None: 2026)

    outcome = esef.update_esef(spark, data_lake, ["AT"])

    assert outcome["failed"] == 0
    frame = facts(spark, data_lake)
    assert set(frame["COUNTRY"]) == {"AT"}
    assert set(frame["FILING"]) == {AT, AT2}
    filings = esef.connect_to_esef_filings(spark, data_lake).toPandas()
    assert set(filings["COUNTRY"]) == {"AT", "DE"}


def test_the_signature_of_a_country_does_not_depend_on_the_order():
    one = [{"filing": "a", "sha256": "1"}, {"filing": "b", "sha256": "2"}]

    assert esef.signature(one) == esef.signature(list(reversed(one)))
    assert esef.signature(one) != esef.signature(one[:1])


def test_a_report_comes_back_in_chunks(tmp_path):
    from fincommons.esef import silver
    path = tmp_path / "report.json"
    shutil.copy(FIXTURES / report_name(AT), path)

    chunks = list(silver._read_report("AT", "F", datetime.date(2026, 3, 31),
                                      str(path), chunk_rows=2))

    assert [len(c) for c in chunks] == [2, 1]
    assert pandas.concat(chunks)["_ERROR"].isna().all()
