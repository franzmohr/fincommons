"""Building the tables of filings, of facts and of filers.

What this layer does is decide which filings to read and write the three
tables; what a filing becomes is the business of the parse module and is
tested there. So the tests here are mostly about the deciding: that a filing
the agency never converted is not attempted, that one it did convert and
nobody collected is counted rather than passed over in silence, and that an
archive which cannot be read costs its own filing and no other.

The index is the real one of 13 November 2026, cut down to a handful of
documents; the archive is the real conversion of one of them.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import date
from pathlib import Path

import pytest

from fincommons.financialreport.edinet import common, silver

DATA = Path(__file__).parent / "data" / "edinet"
ARCHIVE = DATA / "S100ZH4D.zip"
FILING = "S100ZH4D"

pytestmark = pytest.mark.usefixtures("spark")


def an_entry(doc, **overrides):
    entry = {
        "seqNumber": 1, "docID": doc, "edinetCode": "E03270",
        "secCode": "75130", "jcn": "1070001004815",
        "filerName": "株式会社コジマ", "fundCode": None,
        "ordinanceCode": "010", "formCode": "030000", "docTypeCode": "120",
        "periodStart": "2025-09-01", "periodEnd": "2026-08-31",
        "submitDateTime": "2026-11-13 11:39",
        "docDescription": "有価証券報告書", "parentDocID": None,
        "withdrawalStatus": "0", "docInfoEditStatus": "0",
        "disclosureStatus": "0", "xbrlFlag": "1", "pdfFlag": "1",
        "attachDocFlag": "1", "englishDocFlag": "0", "csvFlag": "1",
        "legalStatus": "1",
    }
    entry.update(overrides)
    return entry


@pytest.fixture
def lake(tmp_path):
    """A lake holding one day's index and the conversion of one filing."""
    import yaml

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"edinet": {name: str(base / name) for name in ("bronze", "silver")}}
    ))
    config_path = str(config_path)
    common.create_directory_tree(config_path)

    index = {"metadata": {"status": "200", "resultset": {"count": 3}},
             "results": [
                 an_entry(FILING),
                 # Converted by the agency, not collected here: a gap a run
                 # can close.
                 an_entry("S100AAAA"),
                 # Never converted, so nothing can ever be read for it.
                 an_entry("S100BBBB", csvFlag="0", xbrlFlag="0"),
             ]}
    common.write_json(common.path_bronze_index(config_path, "2026-11-13"),
                      index)

    target = common.path_bronze_document(config_path, FILING, "csv")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    shutil.copy(ARCHIVE, target)
    return config_path


# The table of filings
# --------------------------------------------------------------------------


def test_the_filings_of_every_index_collected_are_written(spark, lake):
    silver.update_silver_edinet(spark, lake, filers=False)
    filings = common.connect_to_edinet(spark, lake, "filings")

    assert filings.count() == 3
    assert set(filings.columns) == {f.name for f in common.filings_schema.fields}
    row = filings.where(f"filing = '{FILING}'").first()
    assert row["edinet_code"] == "E03270"
    assert row["period_end"] == date(2026, 8, 31)
    assert row["filed_year"] == 2026


def test_a_lake_with_no_index_is_told_so(spark, tmp_path):
    import yaml

    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"edinet": {"bronze": str(tmp_path / "b"), "silver": str(tmp_path / "s")}}))
    common.create_directory_tree(str(config_path))

    with pytest.raises(RuntimeError, match="holds no index"):
        silver.build_filings(spark, str(config_path))


# Which filings are read
# --------------------------------------------------------------------------


def test_only_the_filing_whose_conversion_was_collected_is_read(spark, lake):
    silver.update_silver_edinet(spark, lake, filers=False)
    facts = common.connect_to_edinet(spark, lake, "facts")

    assert {row["filing"] for row in facts.select("filing").distinct().collect()} \
        == {FILING}


def test_a_filing_the_agency_converted_and_nobody_collected_is_counted(spark, lake):
    """A gap a run can close, as against one it never can."""
    filings = silver.build_filings(spark, lake)

    assert silver.outstanding(lake, filings) == ["S100AAAA"]


def test_a_filing_the_agency_never_converted_is_not_counted_as_a_gap(spark, lake):
    filings = silver.build_filings(spark, lake)

    assert "S100BBBB" not in silver.outstanding(lake, filings)


def test_a_lake_with_no_conversion_at_all_is_told_so(spark, lake):
    os.remove(common.path_bronze_document(lake, FILING, "csv"))
    filings = silver.build_filings(spark, lake)

    with pytest.raises(RuntimeError, match="has a conversion in the Bronze"):
        silver.build_facts(spark, lake, filings)


# The table of facts
# --------------------------------------------------------------------------


def test_the_facts_carry_the_columns_of_the_table(spark, lake):
    silver.update_silver_edinet(spark, lake, filers=False)
    facts = common.connect_to_edinet(spark, lake, "facts")

    assert set(facts.columns) == {f.name for f in common.facts_schema.fields}
    assert facts.count() > 0
    # The error column belongs to the reading and never to the table.
    assert "_ERROR" not in facts.columns


def test_the_period_of_a_fact_is_the_filers_own_year(spark, lake):
    silver.update_silver_edinet(spark, lake, filers=False)
    facts = common.connect_to_edinet(spark, lake, "facts")

    sales = facts.where(
        "concept LIKE '%NetSalesSummaryOfBusinessResults' AND value IS NOT NULL"
    ).orderBy("prd_end").collect()

    assert [str(row["prd_end"]) for row in sales][-1] == "2026-08-31"
    assert [str(row["prd_start"]) for row in sales][-1] == "2025-09-01"


def test_the_facts_are_partitioned_by_the_year_filed(spark, lake):
    silver.update_silver_edinet(spark, lake, filers=False)
    facts = common.connect_to_edinet(spark, lake, "facts")

    assert {row["filed_year"] for row in
            facts.select("filed_year").distinct().collect()} == {2026}


def test_an_archive_that_cannot_be_read_costs_only_its_own_filing(spark, lake):
    """An exception on an executor would fail the whole job and with it every
    other filing, so the traceback comes back as a row instead."""
    target = common.path_bronze_document(lake, "S100AAAA", "csv")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    Path(target).write_bytes(b"not an archive")

    filings = silver.build_filings(spark, lake)
    parsed = silver.build_facts(spark, lake, filings).cache()
    try:
        failed = parsed.where("_ERROR IS NOT NULL").collect()
        good = parsed.where("_ERROR IS NULL")

        assert [row["filing"] for row in failed] == ["S100AAAA"]
        assert good.count() > 0
        assert {row["filing"] for row in
                good.select("filing").distinct().collect()} == {FILING}
    finally:
        parsed.unpersist()


# The table of filers
# --------------------------------------------------------------------------


def test_a_lake_with_no_filers_list_writes_no_filers_and_does_not_fail(
        spark, lake):
    silver.update_silver_edinet(spark, lake, filers=True)

    filings = common.connect_to_edinet(spark, lake, "filings")
    assert filings.count() == 3


# The data contracts
# --------------------------------------------------------------------------


def schema_of(table):
    from fincommons.financialreport.edinet import quality

    return {"filings": common.filings_schema, "facts": common.facts_schema,
            "filers": common.filers_schema, "issues": quality.issues_schema}[table]


@pytest.mark.parametrize("table", ["filings", "facts", "filers", "issues"])
def test_the_declared_schema_is_the_schema_of_the_contract(table):
    """The code writes with the declared schema and a reader is told the
    contract, so the two must agree in names, order, types and in which
    columns may be empty."""
    from fincommons.contracts import load_contract, spark_schema

    assert spark_schema(load_contract(f"fincommons.edinet.silver.{table}")) == schema_of(table)


def write_filers(config_path):
    """The agency's list of filers, with a filer that is not listed and has
    no corporate number, whose blanks the list keeps as empty strings."""
    import csv
    import io
    import zipfile

    rows = [
        ["ダウンロード実行日", "2026年11月14日現在", "件数", "2件"],
        ["ＥＤＩＮＥＴコード", "提出者種別", "上場区分", "連結の有無", "資本金", "決算日",
         "提出者名", "提出者名（英字）", "提出者名（ヨミ）", "所在地", "提出者業種",
         "証券コード", "提出者法人番号"],
        ["E03270", "内国法人・組合", "上場", "有", "1491", "8月31日", "株式会社例",
         "EXAMPLE CO., LTD.", "カブシキガイシャレイ", "東京都", "小売業", "99990",
         "5070001000715"],
        ["E99999", "外国法人・組合", "非上場", "無", "", "12月31日", "例外国会社",
         "", "レイガイコクガイシャ", "", "", "", ""],
    ]
    text = io.StringIO()
    csv.writer(text, lineterminator="\r\n").writerows(rows)
    os.makedirs(os.path.dirname(common.path_bronze_filers(config_path)), exist_ok=True)
    with zipfile.ZipFile(common.path_bronze_filers(config_path), "w") as archive:
        archive.writestr("EdinetcodeDlInfo.csv", text.getvalue().encode(common.FILERS_ENCODING))


def test_the_tables_are_the_tables_their_contracts_describe(spark, lake):
    """The four tables built from the fixture index, conversion and list of
    filers are those their contracts describe -- columns, types and order, no
    empty value where one is required, one row per key -- and meet their
    quality rules, warnings included."""
    from pyspark.sql import functions as F

    from fincommons.contracts import check_quality, load_contract, spark_schema
    from fincommons.financialreport.edinet import quality

    write_filers(lake)
    silver.update_silver_edinet(spark, lake, filers=True)
    quality.update_silver_edinet_quality(spark, lake)

    keys = {"filings": ["filing"], "facts": ["filing", "fact"],
            "filers": ["edinet_code"], "issues": []}
    assert common.connect_to_edinet(spark, lake, "filers").count() == 2
    for table, key in keys.items():
        frame = common.connect_to_edinet(spark, lake, table)
        contract = load_contract(f"fincommons.edinet.silver.{table}")
        properties = contract["schema"][0]["properties"]

        assert [(f.name, f.dataType) for f in frame.schema] == \
            [(f.name, f.dataType) for f in spark_schema(contract)], table
        for prop in properties:
            if prop.get("required"):
                assert frame.where(F.col(prop["name"]).isNull()).count() == 0, (table, prop["name"])
        assert [p["name"] for p in sorted((p for p in properties if p.get("primaryKey")),
                                          key=lambda p: p["primaryKeyPosition"])] == key
        if key:
            assert frame.groupBy(*key).count().where("count > 1").count() == 0, table
        assert [r for r in check_quality(frame, contract) if r.passed is False] == [], table
