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

import fincommons.financialreport.esef as esef
from fincommons.financialreport.esef import bronze as esef_bronze

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
        .set_index("filing")
    assert sorted(filings.index) == sorted(
        [AT, AT2, DE, NO_JSON])
    assert filings.loc[AT, "name"] == "Erste Beispiel AG"
    assert filings.loc[AT, "period_end"] == datetime.date(2026, 3, 31)
    # A null, which pandas 3 hands back as NaN and pandas 2 as None.
    assert pandas.isna(filings.loc[NO_JSON, "json_url"])


def test_the_table_of_facts_holds_every_fact_of_every_report(
        spark, data_lake, collected):
    outcome = esef.update_silver_esef(spark, data_lake)

    assert outcome["written"] == 2 and outcome["failed"] == 0
    frame = facts(spark, data_lake)
    assert outcome["rows"] == len(frame) == 9  # three facts of each report
    assert set(frame["country"]) == {"AT", "DE"}

    revenue = frame[(frame["filing"] == AT)
                    & (frame["concept"] == "ifrs-full:Revenue")].iloc[0]
    assert revenue["lei"] == LEI_AT
    assert revenue["period_end"] == datetime.date(2026, 3, 31)
    assert revenue["prd_start"] == datetime.date(2025, 4, 1)
    assert revenue["prd_end"] == datetime.date(2026, 3, 31)
    assert revenue["value"] == 430569203.0
    assert revenue["unit"] == "iso4217:EUR"

    expected = esef.facts_from_report(load(report_name(AT)))
    mine = frame[frame["filing"] == AT].sort_values("fact")
    assert list(mine["fact"]) == list(expected["fact"])
    assert list(mine["text"].fillna("")) == list(expected["text"].fillna(""))


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
    revenue = frame[frame["concept"] == "ifrs-full:Revenue"].set_index("lei")["value"]
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
    assert set(frame["filing"]) == {AT, DE}
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
    assert set(frame["country"]) == {"AT"}
    assert set(frame["filing"]) == {AT, AT2}
    filings = esef.connect_to_esef_filings(spark, data_lake).toPandas()
    assert set(filings["country"]) == {"AT", "DE"}


def test_the_signature_of_a_country_does_not_depend_on_the_order():
    one = [{"filing": "a", "sha256": "1"}, {"filing": "b", "sha256": "2"}]

    assert esef.signature(one) == esef.signature(list(reversed(one)))
    assert esef.signature(one) != esef.signature(one[:1])


def test_a_report_comes_back_in_chunks(tmp_path):
    from fincommons.financialreport.esef import silver
    path = tmp_path / "report.json"
    shutil.copy(FIXTURES / report_name(AT), path)

    chunks = list(silver._read_report("AT", "F", datetime.date(2026, 3, 31),
                                      str(path), chunk_rows=2))

    assert [len(c) for c in chunks] == [2, 1]
    assert pandas.concat(chunks)["_ERROR"].isna().all()


# The data contracts
# --------------------------------------------------------------------------


CONTRACTS = {
    "filings": ("fincommons.esef.silver.filings", esef.filings_schema),
    "facts": ("fincommons.esef.silver.facts", esef.facts_schema),
    "issues": ("fincommons.esef.silver.issues", esef.issues_schema),
}


@pytest.mark.parametrize("table", sorted(CONTRACTS))
def test_the_declared_schema_is_the_schema_of_the_contract(table):
    """The code writes with the declared schema and a reader is told the
    contract, so the two must agree in names, order, types and in which
    columns may be empty."""
    from fincommons.contracts import load_contract, spark_schema

    identifier, declared = CONTRACTS[table]

    assert spark_schema(load_contract(identifier)) == declared


def assert_meets_contract(table, identifier):
    """The table is the table its contract describes, and meets its rules:
    the same columns in the same order and of the same types, no empty value
    where the contract says a column is required, one row per value of the
    primary key, and no quality rule failed, warnings included."""
    from pyspark.sql import functions as F

    from fincommons.contracts import check_quality, load_contract, spark_schema

    contract = load_contract(identifier)
    properties = contract["schema"][0]["properties"]

    assert [(f.name, f.dataType) for f in table.schema] == \
        [(f.name, f.dataType) for f in spark_schema(contract)]
    for prop in properties:
        if prop.get("required"):
            assert table.where(F.col(prop["name"]).isNull()).count() == 0, prop["name"]
    key = [p["name"] for p in sorted((p for p in properties if p.get("primaryKey")),
                                     key=lambda p: p["primaryKeyPosition"])]
    if key:
        assert table.groupBy(*key).count().where("count > 1").count() == 0
    failed = [r for r in check_quality(table, contract) if r.passed is False]
    assert failed == []
    return key


def test_the_tables_are_the_tables_their_contracts_describe(spark, data_lake, collected):
    """Built from the indexes and reports of the fixtures, with a filing
    that has no report and so an issue about a filing rather than a fact."""
    esef.update_silver_esef(spark, data_lake)
    esef.update_silver_esef_quality(spark, data_lake)

    issues = esef.connect_to_esef_issues(spark, data_lake)
    assert issues.where("fact IS NULL").count() > 0
    assert assert_meets_contract(esef.connect_to_esef_filings(spark, data_lake),
                                 "fincommons.esef.silver.filings") == ["filing"]
    assert assert_meets_contract(esef.connect_to_esef(spark, data_lake),
                                 "fincommons.esef.silver.facts") == ["filing", "fact"]
    assert assert_meets_contract(issues, "fincommons.esef.silver.issues") == []
