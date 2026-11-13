"""The filings of both regimes in one table.

The combiner's job is to put two tables side by side without taking anything
from either, so the tests are as much about what it does not do as about what
it does: that a lake holding only one source still works, that the entity
columns are carried unjoined because no mapping between them exists, and that
the measure catalogue resolves against a source table on its own, so that an
ESEF-only lake needs none of this.

The source tables are written from constructed rows rather than built by
their modules. What a filing becomes is each source module's business and is
tested there; what this module does is reduce two shapes to one.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

from fincommons.esef import common as esef
from fincommons.financialreport import common, mapping, silver
from fincommons.secfiling import common as sec
from fincommons.storage import write_silver


@pytest.fixture
def config(tmp_path):
    """A configuration file naming all three data sets."""
    import yaml

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump({
        "esef": {"bronze": str(base / "esef" / "bronze"),
                 "silver": str(base / "esef" / "silver")},
        "secfiling": {"bronze": str(base / "sec" / "bronze"),
                      "silver": str(base / "sec" / "silver")},
        "financialreport": {"silver": str(base / "financialreport" / "silver")},
    }))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


LEI = "529900T8BM49AURSDO55"
CIK = "0001177394"


def write_esef(spark, config_path, framework="ifrs-full"):
    """An ESEF report of one Danish issuer, with a dimensional fact in it."""
    filings = spark.createDataFrame([(
        "lei-2025-12-31-0", LEI, "Demo Issuer A/S", "DK", None,
        date(2025, 12, 31), "en", None, None, None, 0, 0, 0,
        "/p.zip", "/r.xhtml", None, "/r.json", "a" * 64,
    )], schema=esef.filings_schema)
    facts = spark.createDataFrame([
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f1",
         f"{framework}:Revenue", date(2025, 1, 1), date(2025, 12, 31), "EUR",
         None, 100.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f2",
         f"{framework}:Revenue", date(2025, 1, 1), date(2025, 12, 31), "EUR",
         "ifrs-full:SegmentsAxis", 40.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f3",
         f"{framework}:DescriptionOfAccountingPolicy", date(2025, 1, 1),
         date(2025, 12, 31), None, None, None, "a policy", None, "en"),
    ], schema=esef.facts_schema)
    write_silver(filings, esef.ref_silver_filings(config_path))
    write_silver(facts, esef.ref_silver_facts(config_path),
                 partition_by=("COUNTRY",))


def write_sec(spark, config_path, taxonomy="us-gaap", framework="US-GAAP"):
    """A quarterly report of one American registrant."""
    filings = spark.createDataFrame([(
        "0001628280-26-000002", CIK, "Demo Registrant Inc.", "10-Q",
        date(2026, 10, 1), date(2026, 8, 31), 2026, "Q3",
        f"dei;{taxonomy}", framework, 2,
    )], schema=sec.filings_schema)
    facts = spark.createDataFrame([
        (CIK, "0001628280-26-000002", 2026, "a", f"{taxonomy}:Revenues",
         date(2025, 12, 1), date(2026, 8, 31), "USD", 200.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "b", f"{taxonomy}:Assets",
         None, date(2026, 8, 31), "USD", 900.0, "10-Q",
         date(2026, 10, 1), "CY2026Q3I"),
    ], schema=sec.facts_schema)
    write_silver(filings, sec.ref_silver_filings(config_path))
    write_silver(facts, sec.ref_silver_facts(config_path),
                 partition_by=("FILEDYEAR",))


# What each source contributes
# --------------------------------------------------------------------------


def test_the_coverage_of_each_source_is_stated():
    """An absent breakdown may mean the issuer reported none or may mean the
    publisher does not give them out, and which it is depends on the source."""
    by_source = {entry["source"]: entry for entry in common.coverage()}

    assert by_source["esef"]["dimensions"] is True
    assert by_source["secfiling"]["dimensions"] is False
    assert by_source["esef"]["entity"] == "LEI"
    assert by_source["secfiling"]["entity"] == "CIK"


def test_neither_source_module_imports_this_one():
    """The dependency runs one way: an ESEF-only lake builds nothing here and
    loses nothing by it."""
    import fincommons.esef
    import fincommons.secfiling

    for module in (fincommons.esef, fincommons.secfiling):
        folder = os.path.dirname(module.__file__)
        for name in os.listdir(folder):
            if name.endswith(".py"):
                text = open(os.path.join(folder, name), encoding="utf-8").read()
                assert "financialreport" not in text, f"{module.__name__}/{name}"


# Which sources a lake combines
# --------------------------------------------------------------------------


def test_both_sources_are_found_where_both_have_tables(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)

    assert sorted(silver.available_sources(spark, config)) == ["esef", "secfiling"]


def test_a_lake_with_only_esef_combines_only_esef(spark, config):
    """The same question answered over the population there is."""
    write_esef(spark, config)

    assert silver.available_sources(spark, config) == ["esef"]

    silver.update_silver_financialreport(spark, config)
    facts = common.connect_to_financialreport(spark, config)

    assert {row["SOURCE"] for row in facts.collect()} == {"esef"}


def test_a_lake_with_only_sec_filings_combines_only_those(spark, config):
    write_sec(spark, config)

    silver.update_silver_financialreport(spark, config)
    facts = common.connect_to_financialreport(spark, config)

    assert {row["SOURCE"] for row in facts.collect()} == {"secfiling"}


def test_a_lake_with_neither_is_told_so(spark, config):
    with pytest.raises(RuntimeError, match="No source has tables"):
        silver.update_silver_financialreport(spark, config)


def test_a_data_set_the_configuration_does_not_name_is_passed_over(spark, tmp_path):
    """A lake says it does not collect from a source by not naming it."""
    import yaml

    config_path = tmp_path / "only-esef.yaml"
    config_path.write_text(yaml.safe_dump({
        "esef": {"bronze": str(tmp_path / "e" / "b"),
                 "silver": str(tmp_path / "e" / "s")},
        "financialreport": {"silver": str(tmp_path / "fr")},
    }))
    write_esef(spark, str(config_path))

    assert silver.available_sources(spark, str(config_path)) == ["esef"]


# The combined facts
# --------------------------------------------------------------------------


def test_the_facts_of_both_regimes_land_in_one_table(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)

    assert facts.count() == 5
    assert sorted({row["SOURCE"] for row in facts.collect()}) == ["esef", "secfiling"]


def test_the_entity_columns_are_carried_and_never_both(spark, config):
    """No authoritative mapping between an LEI and a central index key is
    published by either body, so neither column is derived from the other."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    for row in common.connect_to_financialreport(spark, config).collect():
        assert (row["LEI"] is None) != (row["CIK"] is None)


def test_the_framework_comes_from_the_prefix_the_filing_tagged_with(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    frameworks = {row["SOURCE"]: row["FRAMEWORK"] for row
                  in common.connect_to_financialreport(spark, config).collect()}

    assert frameworks == {"esef": "IFRS", "secfiling": "US-GAAP"}


def test_an_ifrs_filer_with_the_commission_is_recorded_as_ifrs(spark, config):
    """A foreign private issuer reporting under IFRS files a 20-F and tags
    against the IFRS taxonomy. It is comparable with the European reports,
    not with the American ones beside it."""
    write_sec(spark, config, taxonomy="ifrs-full", framework="IFRS")
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)

    assert {row["FRAMEWORK"] for row in facts.collect()} == {"IFRS"}
    assert {row["CIK"] for row in facts.collect()} == {CIK}


def test_the_concepts_are_not_reconciled(spark, config):
    """What the two have in common is a judgement, and the table does not
    make it."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    concepts = {row["CONCEPT"] for row
                in common.connect_to_financialreport(spark, config).collect()}

    assert "ifrs-full:Revenue" in concepts
    assert "us-gaap:Revenues" in concepts


def test_text_and_dimensions_are_empty_for_the_american_rows(spark, config):
    """Not because this module drops them: the company files carry neither."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    american = facts.where("SOURCE = 'secfiling'").collect()
    european = facts.where("SOURCE = 'esef'").collect()

    assert all(row["TEXT"] is None and row["DIMENSIONS"] is None
               for row in american)
    assert any(row["TEXT"] is not None for row in european)
    assert any(row["DIMENSIONS"] is not None for row in european)


def test_the_period_of_the_filing_reaches_the_american_facts(spark, config):
    """It is on that module's table of filings and not on its facts."""
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config).collect()

    assert {row["PERIODEND"] for row in facts} == {date(2026, 8, 31)}
    assert {row["PERIODYEAR"] for row in facts} == {2026}


def test_the_facts_are_partitioned_by_source_then_year(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    path = common.path_silver_fact(config)

    assert "SOURCE=esef" in os.listdir(path)
    assert "PERIODYEAR=2025" in os.listdir(os.path.join(path, "SOURCE=esef"))


# The combined filings
# --------------------------------------------------------------------------


def test_each_regime_fills_the_column_it_states(spark, config):
    """ESEF filings are filed with the mechanism of one member state and name
    no form; a filing with the Commission names a form and no country."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    filings = {row["SOURCE"]: row for row in
               common.connect_to_financialreport(spark, config, "filing").collect()}

    assert filings["esef"]["COUNTRY"] == "DK"
    assert filings["esef"]["FORM"] is None
    assert filings["secfiling"]["FORM"] == "10-Q"
    assert filings["secfiling"]["COUNTRY"] is None


def test_the_count_of_facts_of_an_esef_filing_comes_from_its_facts(spark, config):
    """The index says nothing about what a report tagged or how much."""
    write_esef(spark, config)
    silver.update_silver_financialreport(spark, config)

    filing = common.connect_to_financialreport(spark, config, "filing").collect()[0]

    assert filing["FACTS"] == 3
    assert filing["FRAMEWORK"] == "IFRS"


def test_an_unknown_table_is_rejected(spark, config):
    with pytest.raises(ValueError, match="not recognised"):
        common.connect_to_financialreport(spark, config, "facts")


def test_an_unknown_source_is_rejected(spark, config):
    write_esef(spark, config)

    with pytest.raises(ValueError, match="No source is read as"):
        silver.combine(spark, config, "fact", sources=["edgar"])


# The measures, resolved over either table
# --------------------------------------------------------------------------


def test_a_measure_resolves_over_the_combined_table(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    revenue = mapping.measure_values(facts, "revenue").collect()

    assert {row["SOURCE"] for row in revenue} == {"esef", "secfiling"}
    assert {row["VALUE"] for row in revenue} == {100.0, 200.0}


def test_a_measure_resolves_over_the_esef_table_on_its_own(spark, config):
    """An ESEF-only lake can use the catalogue without building anything
    here, which is what keeps that module independent of this one."""
    write_esef(spark, config)

    from fincommons.storage import read_silver
    facts = read_silver(spark, esef.ref_silver_facts(config))
    revenue = mapping.measure_values(facts, "revenue").collect()

    assert [row["VALUE"] for row in revenue] == [100.0]


def test_a_figure_broken_down_by_an_axis_is_not_the_measure(spark, config):
    """Summing the parts beside the whole is what this exclusion prevents."""
    write_esef(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    revenue = mapping.measure_values(facts, "revenue").collect()

    assert len(revenue) == 1
    assert revenue[0]["VALUE"] == 100.0


def test_a_fact_no_measure_names_is_left_unlabelled(spark, config):
    write_esef(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    labelled = {row["CONCEPT"]: row["MEASURE"]
                for row in mapping.with_measures(facts).collect()}

    assert labelled["ifrs-full:Revenue"] == "revenue"
    assert labelled["ifrs-full:DescriptionOfAccountingPolicy"] is None
