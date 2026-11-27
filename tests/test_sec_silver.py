"""The tables of filings and of facts.

The reading happens on the executors, one company file at a time, so the
tests check both what the tables hold and what happens to a file that cannot
be read: it must come back as a row carrying its traceback rather than fail
the job and take every other company with it.
"""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pytest

from fincommons.financialreport.sec import common, silver

DATA = Path(__file__).parent / "data" / "sec"


@pytest.fixture
def config(tmp_path):
    """A lake holding the two fixture company files."""
    import yaml

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"sec": {name: str(base / name) for name in ("bronze", "silver")}}
    ))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    for fixture in sorted(DATA.glob("CIK*.json")):
        shutil.copy(fixture, common.path_bronze_companies(config_path))
    return config_path


def test_the_filings_of_both_registrants_are_read(spark, config):
    silver.update_silver_sec(spark, config)

    filings = common.connect_to_sec(spark, config, "filings")
    frameworks = {row["cik"]: row["framework"] for row in filings.collect()}

    assert filings.count() == 3
    assert frameworks == {"0001177394": "US-GAAP", "0001114448": "IFRS"}


def test_the_facts_carry_the_filing_and_the_registrant(spark, config):
    silver.update_silver_sec(spark, config)

    facts = common.connect_to_sec(spark, config, "facts")
    rows = facts.collect()

    assert facts.count() > 0
    assert all(row["filing"] and row["cik"] for row in rows)
    assert {row["form"] for row in rows} <= {"10-K", "10-Q", "20-F"}


def test_the_facts_are_partitioned_by_the_year_of_the_filing(spark, config):
    import os

    silver.update_silver_sec(spark, config)
    path = common.path_silver_facts(config)

    assert "filed_year=2026" in os.listdir(path)


def test_a_restated_figure_is_kept_under_both_filings(spark, config):
    silver.update_silver_sec(spark, config)

    facts = common.connect_to_sec(spark, config, "facts")
    assets = facts.where("concept = 'us-gaap:Assets' AND prd_end = '2025-11-30'")

    assert assets.count() == 2
    assert {row["value"] for row in assets.collect()} == {100.0, 101.0}


def test_a_company_file_that_cannot_be_read_does_not_fail_the_run(spark, config):
    """An exception on an executor would fail the whole job and with it every
    other company."""
    broken = Path(common.path_bronze_companies(config)) / "CIK0000000009.json"
    broken.write_text("{ not json", encoding="utf-8")

    silver.update_silver_sec(spark, config)

    filings = common.connect_to_sec(spark, config, "filings")
    assert filings.count() == 3
    assert "0000000009" not in {row["cik"] for row in filings.collect()}


def test_the_table_is_rebuilt_and_does_not_grow_on_a_second_run(spark, config):
    """A company file is republished whole and read again."""
    silver.update_silver_sec(spark, config)
    first = common.connect_to_sec(spark, config, "facts").count()
    silver.update_silver_sec(spark, config)

    assert common.connect_to_sec(spark, config, "facts").count() == first


def test_only_the_registrants_asked_for_are_read(spark, config):
    silver.update_silver_sec(spark, config, ciks=["0001114448"])

    filings = common.connect_to_sec(spark, config, "filings")

    assert {row["cik"] for row in filings.collect()} == {"0001114448"}


def test_building_from_an_empty_bronze_layer_is_refused(spark, tmp_path):
    import yaml

    config_path = tmp_path / "empty.yaml"
    config_path.write_text(yaml.safe_dump(
        {"sec": {name: str(tmp_path / "empty" / name)
                       for name in ("bronze", "silver")}}
    ))
    common.create_directory_tree(str(config_path))

    with pytest.raises(RuntimeError, match="no company files"):
        silver.update_silver_sec(spark, str(config_path))


def test_an_unknown_table_is_rejected(spark, config):
    with pytest.raises(ValueError, match="not recognised"):
        common.connect_to_sec(spark, config, "factz")


# The data contracts
# --------------------------------------------------------------------------


def contracted(table):
    from fincommons.financialreport.sec import quality

    return {"filings": common.filings_schema, "facts": common.facts_schema,
            "issues": quality.issues_schema}[table]


@pytest.mark.parametrize("table", ["filings", "facts", "issues"])
def test_the_declared_schema_is_the_schema_of_the_contract(table):
    """The code writes with the declared schema and a reader is told the
    contract, so the two must agree in names, order, types and in which
    columns may be empty."""
    from fincommons.contracts import load_contract, spark_schema

    assert spark_schema(load_contract(f"fincommons.sec.silver.{table}")) == contracted(table)


def test_the_tables_are_the_tables_their_contracts_describe(spark, config):
    """The tables built from the fixture company files are those their
    contracts describe -- columns, types and order, no empty value where one
    is required, one row per key -- and meet their quality rules, warnings
    included."""
    from pyspark.sql import functions as F

    from fincommons.contracts import check_quality, load_contract, spark_schema
    from fincommons.financialreport.sec import quality

    silver.update_silver_sec(spark, config)
    quality.update_silver_sec_quality(spark, config)

    for table, key in [("filings", ["filing"]), ("facts", ["filing", "fact"]), ("issues", [])]:
        frame = common.connect_to_sec(spark, config, table)
        contract = load_contract(f"fincommons.sec.silver.{table}")
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
