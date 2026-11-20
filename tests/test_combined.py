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
import subprocess
import sys
from datetime import date, datetime

import pytest

from fincommons.financialreport.esef import common as esef
from fincommons.financialreport.combined import common, mapping, silver
from fincommons.financialreport.sec import common as sec
from fincommons.financialreport.edinet import common as edinet
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
        "sec": {"bronze": str(base / "sec" / "bronze"),
                      "silver": str(base / "sec" / "silver")},
        "edinet": {"bronze": str(base / "edinet" / "bronze"),
                   "silver": str(base / "edinet" / "silver")},
        "financialreport": {"silver": str(base / "financialreport" / "silver")},
    }))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


LEI = "529900T8BM49AURSDO55"
CIK = "0001177394"
EDINETCODE = "E03270"
JCN = "2060001001667"


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
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f4",
         f"{framework}:Assets", None, date(2025, 12, 31), "EUR",
         None, 900.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f5",
         f"{framework}:Liabilities", None, date(2025, 12, 31), "EUR",
         None, 400.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f6",
         f"{framework}:CashAndCashEquivalents", None, date(2025, 12, 31),
         "EUR", None, 150.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f7",
         f"{framework}:Equity", None, date(2025, 12, 31), "EUR",
         None, 500.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f8",
         f"{framework}:ProfitLoss", date(2025, 1, 1), date(2025, 12, 31),
         "EUR", None, 30.0, None, 0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f9",
         f"{framework}:ProfitLossAttributableToOwnersOfParent",
         date(2025, 1, 1), date(2025, 12, 31), "EUR", None, 28.0, None,
         0, "en"),
        ("DK", "lei-2025-12-31-0", LEI, date(2025, 12, 31), "f10",
         f"{framework}:EquityAttributableToOwnersOfParent", None,
         date(2025, 12, 31), "EUR", None, 460.0, None, 0, "en"),
    ], schema=esef.facts_schema)
    write_silver(filings, esef.ref_silver_filings(config_path))
    write_silver(facts, esef.ref_silver_facts(config_path),
                 partition_by=("country",))


def write_sec(spark, config_path, taxonomy="us-gaap", framework="US-GAAP"):
    """A quarterly report of one American registrant.

    It states its revenue twice, under the ASC 606 element and under the
    older total, which is what registrants do and what the catalogue ranks
    them for. Only the first of the two is the measure.

    It states no total liabilities, which is also what many registrants do:
    the balance sheet presents liabilities and equity together and tags no
    total. The measure is absent for it, and absent is not zero.

    Its cash is stated twice as well, excluding and including restricted
    amounts. Only the first is the measure; ASU 2016-18 puts restricted cash
    in the cash flow statement total and IAS 7 does not.

    Its equity and its result are each stated twice over too, with and
    without the non-controlling interests, and the pairs are different
    numbers because this registrant has some. Only the totals including
    them are the measures.
    """
    filings = spark.createDataFrame([(
        "0001628280-26-000002", CIK, "Demo Registrant Inc.", "10-Q",
        date(2026, 10, 1), date(2026, 8, 31), 2026, "Q3",
        f"dei;{taxonomy}", framework, 9,
    )], schema=sec.filings_schema)
    facts = spark.createDataFrame([
        (CIK, "0001628280-26-000002", 2026, "a", f"{taxonomy}:Revenues",
         date(2025, 12, 1), date(2026, 8, 31), "USD", 200.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "b", f"{taxonomy}:Assets",
         None, date(2026, 8, 31), "USD", 900.0, "10-Q",
         date(2026, 10, 1), "CY2026Q3I"),
        (CIK, "0001628280-26-000002", 2026, "c",
         f"{taxonomy}:RevenueFromContractWithCustomerExcludingAssessedTax",
         date(2025, 12, 1), date(2026, 8, 31), "USD", 200.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "d",
         f"{taxonomy}:CashAndCashEquivalentsAtCarryingValue",
         None, date(2026, 8, 31), "USD", 150.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "e",
         f"{taxonomy}:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
         None, date(2026, 8, 31), "USD", 175.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "f",
         f"{taxonomy}:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
         None, date(2026, 8, 31), "USD", 700.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "g",
         f"{taxonomy}:StockholdersEquity",
         None, date(2026, 8, 31), "USD", 640.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "h", f"{taxonomy}:ProfitLoss",
         date(2025, 12, 1), date(2026, 8, 31), "USD", 55.0, "10-Q",
         date(2026, 10, 1), None),
        (CIK, "0001628280-26-000002", 2026, "i", f"{taxonomy}:NetIncomeLoss",
         date(2025, 12, 1), date(2026, 8, 31), "USD", 50.0, "10-Q",
         date(2026, 10, 1), None),
    ], schema=sec.facts_schema)
    write_silver(filings, sec.ref_silver_filings(config_path))
    write_silver(facts, sec.ref_silver_facts(config_path),
                 partition_by=("FILEDYEAR",))


def write_edinet(spark, config_path, standard="Japan GAAP"):
    """An annual securities report of one Japanese filer.

    Eight facts and a ninth that sits in no period: the module leaves both
    dates empty for a context whose shape it does not recognise, and the
    combined table of facts does not carry such a row.
    """
    filings = spark.createDataFrame([(
        "S100ZH4D", EDINETCODE, "75130", JCN, "株式会社コジマ", None,
        "010", "030000", "120", date(2025, 9, 1), date(2026, 8, 31),
        datetime(2026, 11, 13, 14, 31), 2026, date(2026, 11, 13),
        "有価証券報告書", None, "0", "0", "0", "1", "1", "1", "0",
    )], schema=edinet.filings_schema)

    def fact(name, concept, member, start, end, unit, value, text):
        context = ("CurrentYearDuration" if start else "CurrentYearInstant")
        if member:
            context = f"{context}_{member}"
        return (EDINETCODE, "S100ZH4D", 2026, name, concept, "label",
                context, "当期", "個別" if member else "その他",
                "期間" if start else "時点", start, end, member, unit,
                value, text, "jpcrp030000-asr")

    facts = spark.createDataFrame([
        fact("f1", "jppfs_cor:NetSales", "NonConsolidatedMember",
             date(2025, 9, 1), date(2026, 8, 31), "JPY", 297632.0, None),
        fact("f2", "jppfs_cor:Assets", "NonConsolidatedMember",
             None, date(2026, 8, 31), "JPY", 128669.0, None),
        fact("f5", "jppfs_cor:Liabilities", "NonConsolidatedMember",
             None, date(2026, 8, 31), "JPY", 65343.0, None),
        # The closing balance of the cash flow statement, and beside it the
        # balance sheet line that is not the same figure.
        fact("f6", "jppfs_cor:CashAndCashEquivalents", "NonConsolidatedMember",
             None, date(2026, 8, 31), "JPY", 18402.0, None),
        fact("f7", "jppfs_cor:CashAndDeposits", "NonConsolidatedMember",
             None, date(2026, 8, 31), "JPY", 21877.0, None),
        fact("f8", "jppfs_cor:NetAssets", "NonConsolidatedMember",
             None, date(2026, 8, 31), "JPY", 63326.0, None),
        # A filer that prepares no consolidated statements has no
        # non-controlling interest and states no attributable line: this
        # one figure is both measures, and the column names it.
        fact("f9", "jppfs_cor:ProfitLoss", "NonConsolidatedMember",
             date(2025, 9, 1), date(2026, 8, 31), "JPY", 8911.0, None),
        fact("f3", "jpdei_cor:AccountingStandardsDEI", None,
             None, date(2026, 11, 13), None, None, standard),
        (EDINETCODE, "S100ZH4D", 2026, "f4", "jppfs_cor:NetSales", "label",
         "SomethingElse", None, "その他", None, None, None, None, "JPY",
         1.0, None, "jpcrp030000-asr"),
    ], schema=edinet.facts_schema)
    write_silver(filings, edinet.ref_silver_filings(config_path),
                 partition_by=("FILEDYEAR",))
    write_silver(facts, edinet.ref_silver_facts(config_path),
                 partition_by=("FILEDYEAR",))


# What each source contributes
# --------------------------------------------------------------------------


def test_the_coverage_of_each_source_is_stated():
    """An absent breakdown may mean the issuer reported none or may mean the
    publisher does not give them out, and which it is depends on the source."""
    by_source = {entry["source"]: entry for entry in common.coverage()}

    assert by_source["esef"]["dimensions"] is True
    assert by_source["sec"]["dimensions"] is False
    assert by_source["edinet"]["dimensions"] is True
    assert by_source["esef"]["entity"] == "lei"
    assert by_source["sec"]["entity"] == "cik"
    assert by_source["edinet"]["entity"] == "edinet_code"


def _source_files(package):
    """Every .py file of one regime, as (name, text)."""
    folder = os.path.dirname(package.__file__)
    return [(name, open(os.path.join(folder, name), encoding="utf-8").read())
            for name in sorted(os.listdir(folder)) if name.endswith(".py")]


def _regimes():
    """The regime packages, imported one at a time."""
    import importlib

    return {name: importlib.import_module(module)
            for name, module in common.SOURCES.items()}


def test_no_regime_imports_the_combining_layer():
    """The dependency runs one way: an ESEF-only lake builds nothing here and
    loses nothing by it.

    The four regimes and this module share a namespace package, so every
    module's own dotted name contains the word ``financialreport`` and the
    test cannot look for that. What it looks for is an import of the
    combining layer, under either the relative or the absolute spelling.
    """
    forbidden = ("from ..combined", "from .combined",
                 "financialreport.combined", "import combined")

    for name, package in _regimes().items():
        for file, text in _source_files(package):
            for spelling in forbidden:
                assert spelling not in text, f"{name}/{file} names {spelling}"


def test_no_regime_imports_another_regime():
    """Each regime stands on its own, so that a lake collects one of them
    without the others being installed or configured."""
    for name, package in _regimes().items():
        others = set(common.SOURCES) - {name}
        for file, text in _source_files(package):
            for other in others:
                for spelling in (f"from ..{other}", f"financialreport.{other} "
                                 f"import", f"import fincommons.financialreport.{other}"):
                    assert spelling not in text, f"{name}/{file} names {other}"


def test_the_namespace_package_imports_none_of_them():
    """``financialreport/__init__.py`` is a namespace and not a facade.

    Were it to import its children, reaching one regime would load the other
    three and the combining layer with it, and the lazy imports that let a
    lake install one regime alone would have nothing left to do.
    """
    import fincommons.financialreport as namespace

    text = open(namespace.__file__, encoding="utf-8").read()
    code = [line for line in text.splitlines()
            if line.startswith(("import ", "from "))]

    assert code == [], f"the namespace imports {code}"

    # And in a session of its own, because importing a submodule anywhere
    # binds it as an attribute of its parent: once another test has reached
    # for one regime, the parent carries it whatever this file does.
    loaded = subprocess.run(
        [sys.executable, "-c",
         "import sys, fincommons.financialreport;"
         "print(' '.join(sorted(m for m in sys.modules"
         " if m.startswith('fincommons.financialreport.'))))"],
        capture_output=True, text=True, check=True,
    ).stdout.split()

    assert loaded == [], f"importing the namespace loaded {loaded}"


# Which sources a lake combines
# --------------------------------------------------------------------------


def test_both_sources_are_found_where_both_have_tables(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)

    assert sorted(silver.available_sources(spark, config)) == ["esef", "sec"]


def test_a_lake_with_only_esef_combines_only_esef(spark, config):
    """The same question answered over the population there is."""
    write_esef(spark, config)

    assert silver.available_sources(spark, config) == ["esef"]

    silver.update_silver_financialreport(spark, config)
    facts = common.connect_to_financialreport(spark, config)

    assert {row["source"] for row in facts.collect()} == {"esef"}


def test_a_lake_with_only_sec_filings_combines_only_those(spark, config):
    write_sec(spark, config)

    silver.update_silver_financialreport(spark, config)
    facts = common.connect_to_financialreport(spark, config)

    assert {row["source"] for row in facts.collect()} == {"sec"}


def test_all_three_sources_are_found_where_all_three_have_tables(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    assert sorted(silver.available_sources(spark, config)) \
        == ["edinet", "esef", "sec"]


def test_a_lake_with_only_edinet_combines_only_those(spark, config):
    write_edinet(spark, config)

    facts = silver.combine(spark, config, "fact")
    assert {row["source"] for row in facts.collect()} == {"edinet"}


def test_a_japanese_row_carries_its_own_identifiers_and_no_others(spark,
                                                                  config):
    """Four identifier columns, one regime's own in each, and a row has the
    column of its source filled and the others empty."""
    write_edinet(spark, config)
    row = silver.combine(spark, config, "filing").collect()[0]

    assert row["edinet_code"] == EDINETCODE
    assert row["jcn"] == JCN
    assert row["lei"] is None and row["cik"] is None


def write_semi_annual(spark, config_path, stated_end="2026-09-30"):
    """A semi-annual report, as the agency lists it: under the financial year
    the half-year belongs to, April 2026 to March 2027."""
    filings = spark.createDataFrame([(
        "S100Z9W4", EDINETCODE, "88920", JCN, "株式会社エスコン", None,
        "010", "043A00", "160", date(2026, 4, 1), date(2027, 3, 31),
        datetime(2026, 10, 27, 15, 33), 2026, date(2026, 10, 27),
        "半期報告書", None, "0", "0", "0", "1", "1", "1", "0",
    )], schema=edinet.filings_schema)

    def fact(name, concept, context, start, end, value, text, member=None):
        return (EDINETCODE, "S100Z9W4", 2026, name, concept, "label", context,
                "当中間期", "連結", "期間" if start else "時点", start, end,
                member, "JPY" if value is not None else None, value, text,
                "jpcrp040300-ssr")

    rows = [
        fact("h1", "jppfs_cor:NetSales", "InterimDuration",
             date(2026, 4, 1), date(2026, 9, 30), 55523.0, None),
        fact("h2", "jppfs_cor:Assets", "InterimInstant",
             None, date(2026, 9, 30), 526232.0, None),
        fact("h3", "jpdei_cor:AccountingStandardsDEI", "FilingDateInstant",
             None, date(2026, 10, 27), None, "Japan GAAP"),
    ]
    if stated_end is not None:
        rows.append(fact("h4", "jpdei_cor:CurrentPeriodEndDateDEI",
                         "FilingDateInstant", None, date(2026, 10, 27), None,
                         stated_end))
    facts = spark.createDataFrame(rows, schema=edinet.facts_schema)
    write_silver(filings, edinet.ref_silver_filings(config_path),
                 partition_by=("FILEDYEAR",))
    write_silver(facts, edinet.ref_silver_facts(config_path),
                 partition_by=("FILEDYEAR",))


def test_a_semi_annual_report_ends_where_its_half_year_ends(spark, config):
    """The agency lists it under the year the half-year belongs to; the
    report states the end of its own period, and that is the period it
    reports on."""
    write_semi_annual(spark, config)
    filing = silver.combine(spark, config, "filing").collect()[0]

    assert filing["period_end"] == date(2026, 9, 30)
    assert filing["period_year"] == 2026


def test_the_half_years_figures_are_the_reports_own(spark, config):
    """Joined to the period the filing reports on, the half-year's figures
    are the ones a query for the report's own period finds."""
    write_semi_annual(spark, config)
    facts = silver.combine(spark, config, "fact").collect()
    own = {row["concept"] for row in facts if row["prd_end"] == row["period_end"]}

    assert {"jppfs_cor:NetSales", "jppfs_cor:Assets"} <= own


def test_a_filing_that_states_no_period_keeps_the_indexs(spark, config):
    write_semi_annual(spark, config, stated_end=None)
    filing = silver.combine(spark, config, "filing").collect()[0]

    assert filing["period_end"] == date(2027, 3, 31)


def test_a_stated_period_that_is_not_a_date_keeps_the_indexs(spark, config):
    """The conversion writes a dash where a date does not apply."""
    write_semi_annual(spark, config, stated_end="－")
    filing = silver.combine(spark, config, "filing").collect()[0]

    assert filing["period_end"] == date(2027, 3, 31)


def test_the_japanese_framework_is_read_from_the_filing(spark, config):
    """A Japanese report states its accounting standard in its own document
    and entity information, and the prefix of a fact does not settle it: a
    group reporting under IFRS tags its front matter against the same
    disclosure taxonomy every other filer uses."""
    write_edinet(spark, config, standard="Japan GAAP")
    assert {row["framework"] for row in
            silver.combine(spark, config, "fact").collect()} == {"JP-GAAP"}


def test_a_filing_no_standard_applies_to_has_no_framework(spark, config):
    """A large-holding report states the conversion's mark for not applicable
    where a company's report states its standard, and that is no framework.
    """
    write_edinet(spark, config, standard="－")
    assert {row["framework"] for row in
            silver.combine(spark, config, "filing").collect()} == {None}


def test_a_japanese_group_reporting_under_ifrs_is_told_from_one_that_is_not(
        spark, config):
    """A filing is one document and takes the framework it states, even
    where its facts do not all take the same one: the report of a Japanese
    group that has adopted IFRS is an IFRS report, and the parent company's
    Japan GAAP statements inside it do not make it something else."""
    write_edinet(spark, config, standard="IFRS")
    assert {row["framework"] for row in
            silver.combine(spark, config, "filing").collect()} == {"IFRS"}


def test_a_fact_that_sits_in_no_period_is_not_carried(spark, config):
    """PRDEND is the one column of the combined table of facts that may not
    be empty, a fact in no period being comparable with nothing. It is not
    lost: it is in the edinet table of facts, and that module's
    PERIOD_UNRESOLVED check counts them."""
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    assert all(row["prd_end"] is not None for row in facts)
    assert {row["fact"] for row in facts} \
        == {"f1", "f2", "f3", "f5", "f6", "f7", "f8", "f9"}


def test_the_japanese_dimensions_and_text_survive(spark, config):
    """Unlike the company files of the Commission, the agency's conversion
    carries the whole of a report."""
    write_edinet(spark, config)
    facts = {row["fact"]: row for row in
             silver.combine(spark, config, "fact").collect()}

    assert facts["f1"]["dimensions"] == "NonConsolidatedMember"
    assert facts["f3"]["text"] == "Japan GAAP"


def test_a_japanese_ifrs_filer_keeps_its_two_frameworks_apart(spark, config):
    """Its consolidated statements are IFRS and its parent company's own are
    Japan GAAP, both in one report, so the framework cannot come from the
    filing alone: Fujitsu states 3,502,971 million of revenue under
    jpigp_cor for the group and 1,692,618 million under jppfs_cor for the
    company, and they are not figures of one framework."""
    write_edinet(spark, config, standard="IFRS")
    by_concept = {row["concept"]: row["framework"] for row in
                  silver.combine(spark, config, "fact").collect()}

    # The taxonomy settles these two whatever the filing says.
    assert by_concept["jppfs_cor:NetSales"] == "JP-GAAP"
    # And what no taxonomy settles takes the filing's answer.
    assert by_concept["jpdei_cor:AccountingStandardsDEI"] == "IFRS"


def test_the_three_regimes_stand_in_one_table(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    facts = silver.combine(spark, config, "fact")
    assert {row["source"] for row in facts.collect()} \
        == {"esef", "sec", "edinet"}
    assert facts.columns == list(common.fact_schema.names)


def test_a_measure_reaches_a_filer_that_does_not_consolidate(spark, config):
    """NonConsolidatedMember names a basis and not a part of an entity.

    A filer that prepares no consolidated statements states its figures
    under that member and under nothing else, so excluding every dimensioned
    fact -- which is right for a segment or a class of share -- would return
    nothing at all for it.
    """
    from fincommons.financialreport.combined import mapping

    write_edinet(spark, config)
    values = mapping.measure_values(silver.combine(spark, config, "fact"),
                                    "revenue").collect()

    assert [row["concept"] for row in values] == ["jppfs_cor:NetSales"]
    assert values[0]["dimensions"] == "NonConsolidatedMember"
    assert values[0]["value"] == 297632.0


def test_a_segment_is_still_not_a_measure(spark, config):
    """The exception is for the consolidation member and for nothing else:
    a figure broken down by segment is a part of the entity, and summing the
    parts beside the whole is what the exclusion exists to prevent."""
    from fincommons.financialreport.combined import mapping

    write_esef(spark, config)
    values = mapping.measure_values(silver.combine(spark, config, "fact"),
                                    "revenue").collect()

    assert [row["value"] for row in values] == [100.0]   # not the 40.0 segment


def test_the_japanese_measures_resolve_under_both_frameworks():
    """The catalogue reaches Japan under both of the frameworks a Japanese
    filer may report under, and the taxonomy is what tells them apart."""
    from fincommons.financialreport.combined import mapping

    assert ("jppfs_cor", "NetSales") in mapping.concepts_for("revenue",
                                                             "JP-GAAP")
    assert ("jpigp_cor", "RevenueIFRS") in mapping.concepts_for("revenue",
                                                                "IFRS")


def test_no_japanese_row_claims_an_equity_attributable_to_the_parent():
    """Japan GAAP has no element for it: it is the shareholders' equity plus
    the valuation and translation adjustments, a sum rather than a line, and
    taking the first of the two alone is short by the second -- 9,191 million
    of a correct 63,326 million for one report this was checked against."""
    from fincommons.financialreport.combined import mapping

    assert mapping.concepts_for("equity_attributable_to_parent",
                                "JP-GAAP") == []


# The identifier a fact carries for what it is
# --------------------------------------------------------------------------


def test_total_assets_of_every_regime_answers_to_one_name(spark, config):
    """The point of the column: one identifier, one figure, three regimes.

    Each regime keeps the concept its filer tagged -- ifrs-full:Assets,
    us-gaap:Assets, jppfs_cor:Assets -- and all three carry MEASURE
    'assets' beside it.
    """
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    assets = (silver.combine(spark, config, "fact")
              .where("measure = 'assets'").collect())

    assert {row["source"] for row in assets} == {"esef", "sec", "edinet"}
    assert {row["concept"] for row in assets} \
        == {"ifrs-full:Assets", "us-gaap:Assets", "jppfs_cor:Assets"}


def test_the_concept_is_never_altered_to_carry_the_identifier(spark, config):
    """What the filer said stays in the table beside what it is taken to
    mean, so neither answer is lost."""
    write_edinet(spark, config)
    row = next(r for r in silver.combine(spark, config, "fact").collect()
               if r["measure"] == "assets")

    assert row["concept"] == "jppfs_cor:Assets"


def test_a_fact_no_measure_covers_carries_no_identifier(spark, config):
    """Most of the table, and on purpose: the identifier is given to the
    concepts that have been agreed and to nothing else."""
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    assert any(row["measure"] is None for row in facts)
    assert {row["measure"] for row in facts} \
        <= {None, "assets", "cash_and_equivalents", "equity",
            "liabilities", "profit_loss", "revenue"}


def test_revenue_of_every_regime_answers_to_one_name(spark, config):
    """The second measure, and the one with alternatives to leave out."""
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    revenue = (silver.combine(spark, config, "fact")
               .where("measure = 'revenue'").collect())

    assert {row["source"] for row in revenue} == {"esef", "sec", "edinet"}
    assert {row["concept"] for row in revenue} == {
        "ifrs-full:Revenue", "jppfs_cor:NetSales",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"}


def test_profit_or_loss_of_every_regime_answers_to_one_name(spark, config):
    """The results including the non-controlling interests. What differs
    between the frameworks is composition rather than definition: a
    Japanese account reaches 当期純利益 through 経常利益 and the
    extraordinary items, and the bottom line is still the bottom line."""
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    result = (silver.combine(spark, config, "fact")
              .where("measure = 'profit_loss'").collect())

    assert {row["source"] for row in result} == {"esef", "sec", "edinet"}
    assert {row["concept"] for row in result} == {
        "ifrs-full:ProfitLoss", "us-gaap:ProfitLoss", "jppfs_cor:ProfitLoss"}


def test_the_two_american_results_are_two_measures(spark, config):
    """us-gaap:NetIncomeLoss is ranked second under profit_loss because it
    excludes the minorities, and first under its own measure because that
    is what it is. Both facts of the registrant are named, and named
    differently."""
    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    by_concept = {row["concept"]: row for row in facts}
    assert by_concept["us-gaap:ProfitLoss"]["measure"] == "profit_loss"
    assert by_concept["us-gaap:NetIncomeLoss"]["measure"] \
        == "profit_loss_attributable_to_parent"
    assert by_concept["us-gaap:NetIncomeLoss"]["value"] \
        != by_concept["us-gaap:ProfitLoss"]["value"]


def test_the_two_regimes_are_not_skewed_alike(spark, config):
    """The same economic fact -- a company with no non-controlling
    interests -- is named in Japan and unnamed in the United States,
    because each regime tags it by its own convention. The Japanese
    population under this measure is close to every filer and the American
    one is conditional, so the counts are not comparable."""
    write_sec(spark, config)
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    named = {row["source"] for row in facts if row["measure"] == "profit_loss"}
    assert named == {"sec", "edinet"}

    # The Japanese filer consolidates nothing, states no attributable line,
    # and is named all the same.
    japanese = [row for row in facts
                if row["source"] == "edinet" and row["measure"] == "profit_loss"]
    assert len(japanese) == 1
    assert japanese[0]["concept"] == "jppfs_cor:ProfitLoss"
    assert not [row for row in facts
                if row["concept"] == "jppfs_cor:ProfitLossAttributableToOwnersOfParent"]


def test_the_pair_inverts_the_skew_rather_than_repairing_it(spark, config):
    """The attributable line exists only where there are minorities to
    attribute away, so a filer preparing no consolidated statements states
    none -- while us-gaap:NetIncomeLoss is the ordinary bottom line every
    registrant tags. The two measures therefore lean opposite ways, and
    each reaches a different set of filers."""
    write_sec(spark, config)
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    named = {measure: {row["source"] for row in facts
                       if row["measure"] == measure}
             for measure in ("profit_loss",
                             "profit_loss_attributable_to_parent")}

    # The Japanese filer consolidates nothing: its one result is the total,
    # and it states no attributable line at all.
    assert named["profit_loss"] == {"sec", "edinet"}
    assert named["profit_loss_attributable_to_parent"] == {"sec"}


def test_the_japanese_filer_states_one_figure_that_is_both_measures(spark, config):
    """And the column can only call it one of them. A filer with no
    non-controlling interest has nothing to attribute away, so its 当期純利益
    is also the result attributable to its shareholders; the column names
    it profit_loss, which is what the concept says, and leaves the other
    measure to measure_values."""
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    results = [row for row in facts if row["concept"] == "jppfs_cor:ProfitLoss"]
    assert len(results) == 1
    assert results[0]["measure"] == "profit_loss"
    assert not [row for row in facts
                if row["measure"] == "profit_loss_attributable_to_parent"]


def test_the_attributable_result_reaches_every_regime():
    """Unlike the equity counterpart, which has no JP-GAAP element at all:
    Japan states 親会社株主に帰属する当期純利益 as a line of its own but has
    nothing for 自己資本."""
    from fincommons.financialreport.combined import mapping

    attributable = dict(mapping.canonical_concepts(
        ("profit_loss_attributable_to_parent",)))

    assert set(attributable) == {
        "ifrs-full:ProfitLossAttributableToOwnersOfParent",
        "jpigp_cor:ProfitLossAttributableToOwnersOfParentIFRS",
        "jppfs_cor:ProfitLossAttributableToOwnersOfParent",
        "us-gaap:NetIncomeLoss"}
    # The concept is still not the total including the minorities.
    assert "us-gaap:NetIncomeLoss" not in dict(
        mapping.canonical_concepts(("profit_loss",)))
    assert not [c for c in dict(mapping.canonical_concepts(
        ("equity_attributable_to_parent",))) if c.startswith("jppfs_cor:")]


def test_equity_of_every_regime_answers_to_one_name(spark, config):
    """The totals including the non-controlling interests. 純資産 is their
    counterpart: 株主資本 and the valuation and translation adjustments and
    the subscription rights and the non-controlling interests together."""
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    equity = (silver.combine(spark, config, "fact")
              .where("measure = 'equity'").collect())

    assert {row["source"] for row in equity} == {"esef", "sec", "edinet"}
    assert {row["concept"] for row in equity} == {
        "ifrs-full:Equity", "jppfs_cor:NetAssets",
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"}


def test_the_two_american_equities_are_two_measures(spark, config):
    """us-gaap:StockholdersEquity is ranked second under equity because it
    excludes the minorities, and first under its own measure because that
    is what it is. Both facts are named, and named differently."""
    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    by_concept = {row["concept"]: row for row in facts}
    including = by_concept[
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
    assert including["measure"] == "equity"
    assert by_concept["us-gaap:StockholdersEquity"]["measure"] \
        == "equity_attributable_to_parent"
    assert by_concept["us-gaap:StockholdersEquity"]["value"] \
        != including["value"]


def test_no_japanese_filing_can_carry_the_attributable_equity(spark, config):
    """The only structural gap in the column. Every other absence depends
    on what a filer tagged; this one depends on nothing about the filer.
    Japan GAAP has no element for 自己資本, which is 株主資本 plus
    評価・換算差額等 -- a sum rather than a line -- so a query filtering on
    this measure excludes Japan entirely and says nothing about it."""
    from fincommons.financialreport.combined import mapping

    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    assert not [row for row in facts
                if row["measure"] == "equity_attributable_to_parent"]
    # Not because this filer tagged nothing: there is no element to tag.
    assert mapping.concepts_for("equity_attributable_to_parent", "JP-GAAP") == []
    assert not [c for c in dict(mapping.canonical_concepts(
        ("equity_attributable_to_parent",))) if c.startswith("jppfs_cor:")]
    # And the sum is not computed to close the gap.
    assert any(row["measure"] == "equity" for row in facts)


def test_the_catalogue_is_now_wholly_materialised():
    from fincommons.financialreport.combined import mapping

    assert set(mapping.MATERIALISED_MEASURES) \
        == set(mapping.measure_catalogue()["measure"])


def test_a_new_catalogue_measure_is_not_materialised_by_being_added(monkeypatch):
    """The gate is the point of the tuple. It is written out rather than
    derived from the catalogue so that writing a row and claiming a figure
    is comparable stay two separate acts."""
    import pandas

    from fincommons.financialreport.combined import mapping

    catalogue = mapping.measure_catalogue()
    invented = pandas.DataFrame(
        [("ebitda", "IFRS", "ifrs-full", "ProfitLossFromOperatingActivities",
          1, "", "", "test")],
        columns=list(mapping.CATALOGUE_COLUMNS))
    monkeypatch.setattr(
        mapping, "measure_catalogue",
        lambda **kw: pandas.concat([catalogue, invented], ignore_index=True))

    named = dict(mapping.canonical_concepts())

    assert "ebitda" not in set(named.values())
    assert "ifrs-full:ProfitLossFromOperatingActivities" not in named


def test_the_equity_excluding_minorities_is_its_own_measure():
    """The element is the second concept of equity and the first of
    equity_attributable_to_parent, so the priority rule gives it the name
    it actually has."""
    from fincommons.financialreport.combined import mapping

    assert "us-gaap:StockholdersEquity" not in dict(
        mapping.canonical_concepts(("equity",)))
    assert dict(mapping.canonical_concepts(("equity_attributable_to_parent",))) \
        ["us-gaap:StockholdersEquity"] == "equity_attributable_to_parent"


def test_no_japanese_concept_is_the_equity_attributable_to_the_parent():
    """Which is why materialising that measure would reach two regimes of
    three by construction. 自己資本 is a sum and not a line."""
    from fincommons.financialreport.combined import mapping

    concepts = dict(mapping.canonical_concepts(
        ("equity_attributable_to_parent",)))

    assert not [c for c in concepts if c.startswith("jppfs_cor:")]
    assert mapping.concepts_for("equity_attributable_to_parent", "JP-GAAP") == []


def test_cash_answers_to_one_name_from_two_different_statements(spark, config):
    """The case the column exists for. IFRS and US GAAP present cash on the
    balance sheet; a Japanese balance sheet has no line that means it, so
    the identifier goes to the closing balance of the cash flow statement.
    One name, one figure, three places in the report."""
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)

    cash = (silver.combine(spark, config, "fact")
            .where("measure = 'cash_and_equivalents'").collect())

    assert {row["source"] for row in cash} == {"esef", "sec", "edinet"}
    assert {row["concept"] for row in cash} == {
        "ifrs-full:CashAndCashEquivalents",
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
        "jppfs_cor:CashAndCashEquivalents"}


def test_the_japanese_balance_sheet_line_is_not_given_the_name(spark, config):
    """現金及び預金 is cash together with deposits, time deposits of more
    than three months among them. The catalogue carries it second so that a
    filer stating no cash flow statement is reached at query time; it must
    not wear the name of a measure it is not."""
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    by_concept = {row["concept"]: row for row in facts}
    assert by_concept["jppfs_cor:CashAndCashEquivalents"]["measure"] \
        == "cash_and_equivalents"
    assert by_concept["jppfs_cor:CashAndDeposits"]["measure"] is None
    # The two are different figures, which is the whole reason for the rule.
    assert by_concept["jppfs_cor:CashAndDeposits"]["value"] \
        != by_concept["jppfs_cor:CashAndCashEquivalents"]["value"]


def test_the_restricted_cash_total_is_not_given_the_name(spark, config):
    """ASU 2016-18 puts restricted cash in the cash flow statement total and
    IAS 7 does not. The IFRS first concept carries a caveat about exactly
    that total -- and the rule leaves the total out, so the caveat is about
    a comparison this column cannot make."""
    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    by_concept = {row["concept"]: row for row in facts}
    assert by_concept["us-gaap:CashAndCashEquivalentsAtCarryingValue"]["measure"] \
        == "cash_and_equivalents"
    restricted = by_concept[
        "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
    assert restricted["measure"] is None
    assert restricted["value"] == 175.0


def test_liabilities_of_both_regimes_that_state_it_answer_to_one_name(spark, config):
    """Four concepts, one per taxonomy, nothing ranked below them -- so
    liabilities passes the rule as easily as assets does."""
    write_esef(spark, config)
    write_edinet(spark, config)

    liabilities = (silver.combine(spark, config, "fact")
                   .where("measure = 'liabilities'").collect())

    assert {row["source"] for row in liabilities} == {"esef", "edinet"}
    assert {row["concept"] for row in liabilities} \
        == {"ifrs-full:Liabilities", "jppfs_cor:Liabilities"}


def test_a_filer_stating_no_total_liabilities_carries_no_identifier(spark, config):
    """The other kind of null, and the one a benchmark has to know about.
    Many filings with the Commission present liabilities and equity
    together and tag no total, so the figure was never stated. It is not
    derived from the balance sheet identity to fill the gap, and it is not
    a zero."""
    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact")

    assert facts.where("measure = 'liabilities'").count() == 0
    # And nothing was invented in its place: the registrant's own concepts
    # are all that is there.
    assert "us-gaap:Liabilities" not in {row["concept"]
                                         for row in facts.collect()}


def test_the_two_kinds_of_null_are_both_nulls_and_neither_is_a_zero(spark, config):
    """A concept no measure names and a measure no concept states both
    come back as null, which is the honest answer to two different
    questions."""
    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact").collect()

    # The older revenue total is stated and deliberately not named.
    unnamed = next(r for r in facts if r["concept"] == "us-gaap:Revenues")
    assert unnamed["measure"] is None and unnamed["value"] == 200.0

    # Liabilities is named by the catalogue and stated by nobody here.
    assert all(row["measure"] != "liabilities" for row in facts)
    assert all(row["value"] != 0.0 for row in facts if row["value"] is not None)


def test_a_filing_stating_revenue_twice_is_labelled_once(spark, config):
    """The registrant tags the ASC 606 element and the older total for the
    same period. Labelling both would put two figures of one filing under
    one name; only the first concept carries the identifier, and the other
    keeps its concept and a null."""
    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact")

    revenue = facts.where("measure = 'revenue'").collect()
    assert len(revenue) == 1
    assert revenue[0]["concept"] \
        == "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"

    older = next(row for row in facts.collect()
                 if row["concept"] == "us-gaap:Revenues")
    assert older["measure"] is None


def test_the_older_total_still_resolves_at_query_time(spark, config):
    """What the column leaves out, resolving still reaches. A caller asking
    for revenue wants the best answer the filing affords; the column is
    held to the stricter bar."""
    from fincommons.financialreport.combined import mapping

    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact")

    resolved = {row["concept"]: row["measure"]
                for row in mapping.with_measures(facts).collect()}

    assert resolved["us-gaap:Revenues"] == "revenue"


def test_the_identifier_never_turns_one_fact_into_two(spark, config):
    """A concept two measures claim would duplicate the row it is on, which
    is what canonical_concepts refuses and this says from the other side."""
    write_esef(spark, config)
    write_sec(spark, config)
    write_edinet(spark, config)
    facts = silver.combine(spark, config, "fact")

    assert facts.count() \
        == facts.select("source", "filing", "fact").distinct().count()


def test_resolving_again_replaces_the_stored_identifier(spark, config):
    """The table stores an identifier for the measures agreed so far;
    resolving asks the wider question, over every measure the catalogue
    defines, so it answers for that query and does not collide with what
    was stored."""
    from fincommons.financialreport.combined import mapping

    write_sec(spark, config)
    facts = silver.combine(spark, config, "fact")
    assert "measure" in facts.columns

    resolved = mapping.with_measures(facts)
    assert resolved.columns.count("measure") == 1
    assert "measure_priority" in resolved.columns
    # And the wider question reaches a measure the column does not carry.
    assert {row["measure"] for row in resolved.collect()} \
        >= {"assets", "revenue"}


def test_only_the_first_concept_of_a_framework_is_written_down():
    """Revenue is the measure that made the rule explicit: the catalogue
    ranks three of its six concepts below the first, and each says in its
    own caveat that it is not the same figure."""
    from fincommons.financialreport.combined import mapping

    concepts = dict(mapping.canonical_concepts(("revenue",)))

    assert set(concepts) == {
        "ifrs-full:Revenue", "jpigp_cor:RevenueIFRS", "jppfs_cor:NetSales",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"}
    # The approximations carry no identifier rather than a misleading one.
    assert "us-gaap:Revenues" not in concepts
    assert "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax" \
        not in concepts
    assert "ifrs-full:RevenueFromContractsWithCustomers" not in concepts


def test_every_concept_left_out_says_it_is_not_the_same_figure():
    """The rule is not imposed on the catalogue; it is read off it. Every
    row ranked below the first carries a caveat of that kind."""
    from fincommons.financialreport.combined import mapping

    catalogue = mapping.measure_catalogue()
    ranked_below = catalogue[catalogue["priority"] != mapping.FIRST_CONCEPT]

    assert len(ranked_below) > 0
    for row in ranked_below.itertuples():
        assert row.caveat.strip(), f"{row.taxonomy}:{row.element}"


def test_the_priority_rule_settles_a_concept_two_measures_name():
    """us-gaap:StockholdersEquity is named by equity and by
    equity_attributable_to_parent, and an earlier reading took that to
    block both. It is the first concept of the one and the second of the
    other, so it is the equity attributable to the parent and nothing
    else."""
    from fincommons.financialreport.combined import mapping

    concepts = dict(mapping.canonical_concepts(
        ("equity", "equity_attributable_to_parent")))

    assert concepts["us-gaap:StockholdersEquity"] \
        == "equity_attributable_to_parent"
    assert concepts["us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"] \
        == "equity"


def test_every_measure_of_the_catalogue_passes_the_rule():
    """Which is a statement about the catalogue and not a licence to
    materialise them: each is still a judgement to be made against real
    filings one at a time."""
    from fincommons.financialreport.combined import mapping

    every = tuple(sorted(set(mapping.measure_catalogue()["measure"])))
    pairs = mapping.canonical_concepts(every)

    assert {measure for _, measure in pairs} == set(every)
    # One concept, one measure, and no measure duplicated within a taxonomy.
    assert len(pairs) == len({concept for concept, _ in pairs})


def _catalogue_of(monkeypatch, rows):
    """Put a small catalogue of made-up rows in front of the real one."""
    import pandas

    from fincommons.financialreport.combined import mapping

    frame = pandas.DataFrame(rows, columns=list(mapping.CATALOGUE_COLUMNS))
    monkeypatch.setattr(mapping, "measure_catalogue", lambda **kw: frame)


def test_a_concept_two_measures_name_first_is_still_refused(monkeypatch):
    """The priority rule settles the collisions the catalogue has today.
    The check stays for the row that collides tomorrow."""
    from fincommons.financialreport.combined import mapping

    _catalogue_of(monkeypatch, [
        ("equity", "US-GAAP", "us-gaap", "Equity", 1, "", "", "test"),
        ("assets", "US-GAAP", "us-gaap", "Equity", 1, "", "", "test"),
    ])

    with pytest.raises(ValueError, match="claimed by both"):
        mapping.canonical_concepts(("equity", "assets"))


def test_two_first_concepts_under_one_taxonomy_are_refused(monkeypatch):
    """A filing tagging both would state the measure twice under one name,
    which is the thing a materialised identifier may not do."""
    from fincommons.financialreport.combined import mapping

    _catalogue_of(monkeypatch, [
        ("revenue", "US-GAAP", "us-gaap", "Revenues", 1, "", "", "test"),
        ("revenue", "US-GAAP", "us-gaap", "SalesRevenueNet", 1, "", "", "test"),
    ])

    with pytest.raises(ValueError, match="two first concepts"):
        mapping.canonical_concepts(("revenue",))


def test_the_measures_written_down_are_ones_the_catalogue_defines():
    from fincommons.financialreport.combined import mapping

    defined = set(mapping.measure_catalogue()["measure"])
    assert set(mapping.MATERIALISED_MEASURES) <= defined
    # And they resolve without collision, which is what lets them be written.
    assert mapping.canonical_concepts()


def test_total_assets_resolves_in_every_framework_the_lake_may_hold():
    """Four taxonomies, one measure, no fallbacks -- which is why total
    assets is the one the combined dataset starts with."""
    from fincommons.financialreport.combined import mapping

    concepts = dict(mapping.canonical_concepts(("assets",)))
    assert set(concepts) == {"ifrs-full:Assets", "us-gaap:Assets",
                             "jppfs_cor:Assets", "jpigp_cor:AssetsIFRS"}
    assert set(concepts.values()) == {"assets"}


def test_an_unknown_measure_is_refused_by_name():
    from fincommons.financialreport.combined import mapping

    with pytest.raises(ValueError, match="no measure of the catalogue"):
        mapping.canonical_concepts(("ebitda",))


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

    assert facts.count() == 19
    assert sorted({row["source"] for row in facts.collect()}) == ["esef", "sec"]


def test_the_entity_columns_are_carried_and_never_both(spark, config):
    """No authoritative mapping between an LEI and a central index key is
    published by either body, so neither column is derived from the other."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    for row in common.connect_to_financialreport(spark, config).collect():
        assert (row["lei"] is None) != (row["cik"] is None)


def test_the_framework_comes_from_the_prefix_the_filing_tagged_with(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    frameworks = {row["source"]: row["framework"] for row
                  in common.connect_to_financialreport(spark, config).collect()}

    assert frameworks == {"esef": "IFRS", "sec": "US-GAAP"}


def test_an_ifrs_filer_with_the_commission_is_recorded_as_ifrs(spark, config):
    """A foreign private issuer reporting under IFRS files a 20-F and tags
    against the IFRS taxonomy. It is comparable with the European reports,
    not with the American ones beside it."""
    write_sec(spark, config, taxonomy="ifrs-full", framework="IFRS")
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)

    assert {row["framework"] for row in facts.collect()} == {"IFRS"}
    assert {row["cik"] for row in facts.collect()} == {CIK}


def test_the_concepts_are_not_reconciled(spark, config):
    """What the two have in common is a judgement, and the table does not
    make it."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    concepts = {row["concept"] for row
                in common.connect_to_financialreport(spark, config).collect()}

    assert "ifrs-full:Revenue" in concepts
    assert "us-gaap:Revenues" in concepts


def test_text_and_dimensions_are_empty_for_the_american_rows(spark, config):
    """Not because this module drops them: the company files carry neither."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    american = facts.where("source = 'sec'").collect()
    european = facts.where("source = 'esef'").collect()

    assert all(row["text"] is None and row["dimensions"] is None
               for row in american)
    assert any(row["text"] is not None for row in european)
    assert any(row["dimensions"] is not None for row in european)


def test_the_period_of_the_filing_reaches_the_american_facts(spark, config):
    """It is on that module's table of filings and not on its facts."""
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config).collect()

    assert {row["period_end"] for row in facts} == {date(2026, 8, 31)}
    assert {row["period_year"] for row in facts} == {2026}


def test_the_facts_are_partitioned_by_source_then_year(spark, config):
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    path = common.path_silver_fact(config)

    assert "source=esef" in os.listdir(path)
    assert "period_year=2025" in os.listdir(os.path.join(path, "source=esef"))


# The combined filings
# --------------------------------------------------------------------------


def test_each_regime_fills_the_column_it_states(spark, config):
    """ESEF filings are filed with the mechanism of one member state and name
    no form; a filing with the Commission names a form and no country."""
    write_esef(spark, config)
    write_sec(spark, config)
    silver.update_silver_financialreport(spark, config)

    filings = {row["source"]: row for row in
               common.connect_to_financialreport(spark, config, "filing").collect()}

    assert filings["esef"]["country"] == "DK"
    assert filings["esef"]["form"] is None
    assert filings["sec"]["form"] == "10-Q"
    assert filings["sec"]["country"] is None


def test_the_count_of_facts_of_an_esef_filing_comes_from_its_facts(spark, config):
    """The index says nothing about what a report tagged or how much."""
    write_esef(spark, config)
    silver.update_silver_financialreport(spark, config)

    filing = common.connect_to_financialreport(spark, config, "filing").collect()[0]

    assert filing["facts"] == 10
    assert filing["framework"] == "IFRS"


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

    assert {row["source"] for row in revenue} == {"esef", "sec"}
    assert {row["value"] for row in revenue} == {100.0, 200.0}


def test_a_measure_resolves_over_the_esef_table_on_its_own(spark, config):
    """An ESEF-only lake can use the catalogue without building anything
    here, which is what keeps that module independent of this one."""
    write_esef(spark, config)

    from fincommons.storage import read_silver
    facts = read_silver(spark, esef.ref_silver_facts(config))
    revenue = mapping.measure_values(facts, "revenue").collect()

    assert [row["value"] for row in revenue] == [100.0]


def test_a_figure_broken_down_by_an_axis_is_not_the_measure(spark, config):
    """Summing the parts beside the whole is what this exclusion prevents."""
    write_esef(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    revenue = mapping.measure_values(facts, "revenue").collect()

    assert len(revenue) == 1
    assert revenue[0]["value"] == 100.0


def test_a_fact_no_measure_names_is_left_unlabelled(spark, config):
    write_esef(spark, config)
    silver.update_silver_financialreport(spark, config)

    facts = common.connect_to_financialreport(spark, config)
    labelled = {row["concept"]: row["measure"]
                for row in mapping.with_measures(facts).collect()}

    assert labelled["ifrs-full:Revenue"] == "revenue"
    assert labelled["ifrs-full:DescriptionOfAccountingPolicy"] is None
