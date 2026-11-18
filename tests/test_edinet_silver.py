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
