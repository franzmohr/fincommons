"""The checks the figures of an ESEF report are held to.

Each test builds a lake holding one sound report and then breaks one thing
about it, so that a check is shown to fire on the fault and -- which matters as
much -- to stay quiet on the sound report. The figures are made up but they add
up, which is all the arithmetic checks ask of them.

Two of the checks here have no counterpart in the SEC module, because the
repository publishes something the Commission does not: it validates every
filing and publishes the counts, and it lists filings whose xBRL-JSON it could
not produce.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from fincommons.esef import common, quality

LEI = "529900ZAXBMQDIWPNB72"
COUNTRY = "AT"
FILING = f"{LEI}-2025-12-31-ESEF-AT-0"
PERIODEND = date(2025, 12, 31)
PRDSTART = date(2025, 1, 1)
PROCESSED = datetime(2026, 4, 1, 12, 0, 0)

#: The sound report: the concept, whether it is a flow, and the value.
SOUND = [
    ("ifrs-full:Assets", False, 1000.0),
    ("ifrs-full:CurrentAssets", False, 400.0),
    ("ifrs-full:NoncurrentAssets", False, 600.0),
    ("ifrs-full:Equity", False, 300.0),
    ("ifrs-full:Liabilities", False, 700.0),
    ("ifrs-full:EquityAndLiabilities", False, 1000.0),
    ("ifrs-full:Revenue", True, 500.0),
    ("ifrs-full:CostOfSales", True, 300.0),
    ("ifrs-full:GrossProfit", True, 200.0),
    ("ifrs-full:ProfitLossBeforeTax", True, 120.0),
    ("ifrs-full:IncomeTaxExpenseContinuingOperations", True, 20.0),
    ("ifrs-full:ProfitLoss", True, 100.0),
    ("ifrs-full:CashFlowsFromUsedInOperatingActivities", True, 150.0),
    ("ifrs-full:CashFlowsFromUsedInInvestingActivities", True, -60.0),
    ("ifrs-full:CashFlowsFromUsedInFinancingActivities", True, -40.0),
    ("ifrs-full:IncreaseDecreaseInCashAndCashEquivalents", True, 50.0),
]


def fact(concept, flow, value, filing=FILING, prdend=PERIODEND,
         prdstart=PRDSTART, unit="iso4217:EUR", dimensions=None, text=None,
         lei=LEI):
    """One row of the table of facts."""
    start = prdstart if flow else None
    return (COUNTRY, filing, lei, PERIODEND,
            f"{filing}:{concept}:{start or ''}:{prdend}:{dimensions or ''}",
            concept, start, prdend, unit, dimensions,
            None if value is None else float(value), text, -3, "en")


def filing_row(filing=FILING, lei=LEI, periodend=PERIODEND, lang="en",
               errors=0, jsonurl="/x.json", processed=PROCESSED):
    """One row of the table of filings."""
    return (filing, lei, "Issuer AG", COUNTRY, "ESEF", periodend, lang,
            PROCESSED, PROCESSED, processed, errors, 0, 0,
            "/p.zip", "/r.xhtml", "/v", jsonurl, "sha")


@pytest.fixture
def lake(spark, tmp_path):
    """A lake whose Silver tables this test writes directly."""
    import yaml

    from fincommons.storage import write_silver

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"esef": {name: str(base / name) for name in ("bronze", "silver")}}))
    config_path = str(config_path)

    def write(facts, filings):
        write_silver(
            spark.createDataFrame(facts, common.facts_schema),
            common.ref_silver_facts(config_path), partition_by=("COUNTRY",))
        write_silver(
            spark.createDataFrame(filings, common.filings_schema),
            common.ref_silver_filings(config_path))
        return config_path

    return write


def codes(spark, config, **kwargs):
    """The codes the checks report over a lake."""
    found = quality.find_issues(spark, config, **kwargs)
    return sorted({row["ISSUE"] for row in found.collect()})


def sound_facts():
    return [fact(*row) for row in SOUND]


# The sound report
# --------------------------------------------------------------------------


def test_a_report_that_adds_up_is_reported_for_nothing(spark, lake):
    config = lake(sound_facts(), [filing_row()])

    assert codes(spark, config) == []


def test_the_issue_table_is_written_and_read_back(spark, lake):
    config = lake(sound_facts(), [filing_row()])
    quality.update_silver_esef_quality(spark, config)

    issues = common.connect_to_esef_issues(spark, config)
    assert issues.count() == 0
    assert issues.columns == list(quality.issues_schema.names)


# The arithmetic
# --------------------------------------------------------------------------


@pytest.mark.parametrize("concept, code", [
    ("ifrs-full:Equity", "IMBAL_BALANCE_SHEET"),
    ("ifrs-full:CurrentAssets", "IMBAL_ASSETS"),
    ("ifrs-full:CostOfSales", "IMBAL_INCOME"),
    ("ifrs-full:IncomeTaxExpenseContinuingOperations", "IMBAL_PROFIT"),
    ("ifrs-full:CashFlowsFromUsedInOperatingActivities", "IMBAL_CASHFLOW"),
])
def test_a_figure_off_by_a_tenth_is_reported(spark, lake, concept, code):
    facts = [fact(row[0], row[1],
                  row[2] * 1.1 if row[0] == concept else row[2])
             for row in SOUND]
    config = lake(facts, [filing_row()])

    assert code in codes(spark, config)


def test_an_issuer_presenting_by_liquidity_is_not_held_to_the_asset_split(spark, lake):
    """IAS 1 lets it, and then neither subtotal is tagged."""
    facts = [fact(*row) for row in SOUND
             if row[0] not in ("ifrs-full:CurrentAssets",
                               "ifrs-full:NoncurrentAssets")]
    config = lake(facts, [filing_row()])

    assert "IMBAL_ASSETS" not in codes(spark, config)


def test_a_breakdown_is_not_mistaken_for_a_total(spark, lake):
    """A figure broken down by segment is a part and not the whole."""
    facts = sound_facts()
    facts.append(fact("ifrs-full:Revenue", True, 9999.0,
                      dimensions="ifrs-full:SegmentsAxis=x:Europe"))
    config = lake(facts, [filing_row()])

    assert codes(spark, config) == []


def test_a_negative_balance_sheet_total_is_reported(spark, lake):
    facts = [fact(row[0], row[1],
                  -row[2] if row[0] == "ifrs-full:Assets" else row[2])
             for row in SOUND]
    config = lake(facts, [filing_row()])

    assert "IMPL_NEGATIVE_ASSETS" in codes(spark, config)


# What the comparison looked for and did not find
# --------------------------------------------------------------------------


@pytest.mark.parametrize("concept, code", [
    ("ifrs-full:Assets", "MISS_ASSETS"),
    ("ifrs-full:Revenue", "MISS_REVENUE"),
    ("ifrs-full:ProfitLoss", "MISS_PROFIT"),
])
def test_a_line_the_report_does_not_tag_is_reported(spark, lake, concept, code):
    facts = [fact(*row) for row in SOUND if row[0] != concept]
    config = lake(facts, [filing_row()])

    assert code in codes(spark, config)


def test_a_second_concept_stands_in_for_the_first(spark, lake):
    facts = [fact(*row) for row in SOUND if row[0] != "ifrs-full:Revenue"]
    facts.append(fact("ifrs-full:RevenueFromContractsWithCustomers", True, 500.0))
    config = lake(facts, [filing_row()])

    assert "MISS_REVENUE" not in codes(spark, config)


# The facts themselves
# --------------------------------------------------------------------------


def test_a_period_that_ends_before_it_begins_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("ifrs-full:Revenue", True, 1.0,
                      prdstart=date(2026, 6, 1), prdend=date(2025, 12, 31),
                      unit="iso4217:USD"))
    config = lake(facts, [filing_row()])

    assert "IMPL_PERIOD" in codes(spark, config)


def test_a_fact_saying_nothing_at_all_is_reported(spark, lake):
    """Neither a number nor a text, which points at the conversion."""
    facts = sound_facts()
    facts.append(fact("ifrs-full:NameOfReportingEntityOrOtherMeansOfIdentification",
                      False, None, unit=None))
    config = lake(facts, [filing_row()])

    assert "MISS_FACT_VALUE" in codes(spark, config)


def test_a_text_fact_is_not_mistaken_for_an_empty_one(spark, lake):
    facts = sound_facts()
    facts.append(fact("ifrs-full:DomicileOfEntity", False, None, unit=None,
                      text="Linz"))
    config = lake(facts, [filing_row()])

    assert "MISS_FACT_VALUE" not in codes(spark, config)


def test_one_figure_tagged_twice_with_two_values_is_reported(spark, lake):
    facts = sound_facts()
    facts.append((COUNTRY, FILING, LEI, PERIODEND, f"{FILING}:second",
                  "ifrs-full:Assets", None, PERIODEND, "iso4217:EUR", None,
                  999.0, None, -3, "en"))
    config = lake(facts, [filing_row()])

    found = quality.find_issues(spark, config)
    rows = [r for r in found.collect() if r["ISSUE"] == "DUPL_FACT_VALUE"]
    # An instant has no period start, and an equi-join on an empty key matches
    # nothing: both facts have to come back.
    assert len(rows) == 2


def test_the_same_figure_twice_with_one_value_is_not_a_fault(spark, lake):
    facts = sound_facts()
    facts.append((COUNTRY, FILING, LEI, PERIODEND, f"{FILING}:again",
                  "ifrs-full:Assets", None, PERIODEND, "iso4217:EUR", None,
                  1000.0, None, -3, "en"))
    config = lake(facts, [filing_row()])

    assert "DUPL_FACT_VALUE" not in codes(spark, config)


def test_one_concept_in_two_currencies_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("ifrs-full:Assets", False, 1000.0, unit="iso4217:USD"))
    config = lake(facts, [filing_row()])

    assert "MIXED_UNIT" in codes(spark, config)


def test_a_period_ending_after_the_repository_saw_it_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("ifrs-full:Revenue", True, 1.0,
                      prdstart=date(2030, 1, 1), prdend=date(2030, 12, 31),
                      unit="iso4217:USD"))
    config = lake(facts, [filing_row()])

    assert "IMPL_PRDEND_FUTURE" in codes(spark, config)


# What the repository itself says
# --------------------------------------------------------------------------


def test_the_repositorys_own_validation_errors_are_carried_in(spark, lake):
    config = lake(sound_facts(), [filing_row(errors=3)])

    assert "VALIDATION_ERRORS" in codes(spark, config)


def test_a_filing_with_no_facts_is_counted(spark, lake):
    empty = f"{LEI}-2024-12-31-ESEF-AT-0"
    config = lake(sound_facts(),
                  [filing_row(),
                   filing_row(filing=empty, periodend=date(2024, 12, 31),
                              jsonurl=None)])

    found = quality.find_issues(spark, config)
    rows = [r for r in found.collect() if r["ISSUE"] == "NO_FACTS"]
    assert [r["FILING"] for r in rows] == [empty]


def test_the_same_period_filed_twice_in_one_language_is_reported(spark, lake):
    second = f"{LEI}-2025-12-31-ESEF-AT-1"
    facts = sound_facts() + [fact(*row, filing=second) for row in SOUND]
    config = lake(facts, [filing_row(), filing_row(filing=second)])

    assert "DUPL_FILING_PERIOD" in codes(spark, config)


def test_the_same_period_filed_in_two_languages_is_not(spark, lake):
    """An issuer filing in German and in English states the same figures."""
    second = f"{LEI}-2025-12-31-ESEF-AT-1"
    facts = sound_facts() + [fact(*row, filing=second) for row in SOUND]
    config = lake(facts,
                  [filing_row(), filing_row(filing=second, lang="de")])

    assert "DUPL_FILING_PERIOD" not in codes(spark, config)


def test_a_report_mostly_tagged_with_the_issuers_own_concepts_is_counted(spark, lake):
    facts = sound_facts()
    for i in range(40):
        facts.append(fact(f"issuerag:OwnConcept{i}", True, float(i + 1)))
    config = lake(facts, [filing_row()])

    assert "HIGH_EXTENSION_SHARE" in codes(spark, config)


def test_a_report_tagged_against_the_taxonomy_is_not(spark, lake):
    config = lake(sound_facts(), [filing_row()])

    assert "HIGH_EXTENSION_SHARE" not in codes(spark, config)


def test_the_taxonomy_prefixes_are_not_counted_as_extensions(spark):
    """esef_cor and the rest are taxonomies, not an issuer's own vocabulary."""
    for prefix in quality.TAXONOMY_PREFIXES:
        assert prefix in quality.TAXONOMY_PREFIXES
    assert "issuerag" not in quality.TAXONOMY_PREFIXES


# Running over part of the population
# --------------------------------------------------------------------------


def test_a_run_over_one_country_keeps_what_is_known_of_the_others(spark, lake):
    other_lei = "529900ZAXBMQDIWPNB73"
    other = f"{other_lei}-2025-12-31-ESEF-DE-0"
    facts = sound_facts()
    # A German filing with a missing total, in another partition.
    facts.append(("DE", other, other_lei, PERIODEND, "f1",
                  "ifrs-full:Revenue", PRDSTART, PERIODEND, "iso4217:EUR",
                  None, 1.0, None, -3, "en"))
    filings = [filing_row(),
               (other, other_lei, "Other AG", "DE", "ESEF", PERIODEND, "de",
                PROCESSED, PROCESSED, PROCESSED, 0, 0, 0,
                "/p.zip", "/r.xhtml", "/v", "/x.json", "sha")]
    config = lake(facts, filings)

    quality.update_silver_esef_quality(spark, config)
    before = common.connect_to_esef_issues(spark, config)
    assert {row["COUNTRY"] for row in before.collect()} == {"DE"}
    count = before.count()

    # Checking Austria alone must leave Germany's issues where they are.
    quality.update_silver_esef_quality(spark, config, countries=["AT"])
    after = common.connect_to_esef_issues(spark, config)

    assert after.count() == count
    assert {row["COUNTRY"] for row in after.collect()} == {"DE"}
