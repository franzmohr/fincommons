"""The checks the figures of a filing with the Commission are held to.

Each test builds a lake holding one sound annual report and then breaks one
thing about it, so that a check is shown to fire on the fault and -- which
matters as much -- to stay quiet on the sound report. The figures of the sound
report are NVIDIA's for its financial year 2026, rounded to millions, because
they are public and they add up: 215,938 of revenue less 62,475 of cost is
153,463 of gross profit, and 130,387 of operating income plus 11,063 of
non-operating income less 21,383 of tax is 120,067 of net income.
"""

from __future__ import annotations

from datetime import date

import pytest

from fincommons.secfiling import common, quality

CIK = "0001045810"
FILING = "0001045810-26-000021"
PERIODEND = date(2026, 1, 25)
PRDSTART = date(2025, 1, 27)
FILED = date(2026, 2, 25)

M = 1_000_000

#: The sound report: the concept, whether it is a flow, and the value.
SOUND = [
    ("us-gaap:Assets", False, 206_803 * M),
    ("us-gaap:Liabilities", False, 49_510 * M),
    ("us-gaap:StockholdersEquity", False, 157_293 * M),
    ("us-gaap:LiabilitiesAndStockholdersEquity", False, 206_803 * M),
    ("us-gaap:Revenues", True, 215_938 * M),
    ("us-gaap:CostOfRevenue", True, 62_475 * M),
    ("us-gaap:GrossProfit", True, 153_463 * M),
    ("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
     True, 141_450 * M),
    ("us-gaap:IncomeTaxExpenseBenefit", True, 21_383 * M),
    ("us-gaap:NetIncomeLoss", True, 120_067 * M),
    ("us-gaap:NetCashProvidedByUsedInOperatingActivities", True, 102_718 * M),
    ("us-gaap:NetCashProvidedByUsedInInvestingActivities", True, -52_228 * M),
    ("us-gaap:NetCashProvidedByUsedInFinancingActivities", True, -48_474 * M),
    ("us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
     True, 2_016 * M),
]


def fact(concept, flow, value, filing=FILING, prdend=PERIODEND,
         prdstart=PRDSTART, unit="USD", filed=FILED):
    """One row of the table of facts."""
    start = prdstart if flow else None
    return (CIK, filing, filed.year,
            f"{filing}:{concept}:{start or ''}:{prdend}:{unit}",
            concept, start, prdend, unit, float(value), "10-K", filed, None)


def filing_row(filing=FILING, form="10-K", filed=FILED, periodend=PERIODEND,
               fy=2026, framework="US-GAAP", facts=len(SOUND)):
    """One row of the table of filings."""
    return (filing, CIK, "NVIDIA CORP", form, filed, periodend, fy, "FY",
            "dei;us-gaap", framework, facts)


@pytest.fixture
def lake(spark, tmp_path):
    """A lake whose Silver tables this test writes directly.

    The checks read the Silver tables and nothing else, so the Bronze layer
    and the reader are out of the way here; they are tested beside the Silver
    update itself.
    """
    import yaml

    from fincommons.storage import write_silver

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"secfiling": {name: str(base / name) for name in ("bronze", "silver")}}))
    config_path = str(config_path)
    common.create_directory_tree(config_path)

    def write(facts, filings):
        write_silver(
            spark.createDataFrame(facts, common.facts_schema),
            common.ref_silver_facts(config_path), partition_by=("FILEDYEAR",))
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
    quality.update_silver_secfiling_quality(spark, config)

    issues = common.connect_to_secfiling(spark, config, "issues")
    assert issues.count() == 0
    assert issues.columns == list(quality.issues_schema.names)


def test_a_fault_reaches_the_table_with_the_day_it_was_found(spark, lake):
    facts = [row for row in sound_facts()
             if "GrossProfit" not in row[4]]
    facts.append(fact("us-gaap:GrossProfit", True, 99_999 * M))
    config = lake(facts, [filing_row()])

    quality.update_silver_secfiling_quality(spark, config)
    rows = common.connect_to_secfiling(spark, config, "issues").collect()

    assert [row["ISSUE"] for row in rows] == ["IMBAL_INCOME"]
    assert rows[0]["CHECKED"] == date.today()
    # A filing-level fault names no fact: the data cannot say which of the
    # three figures is the wrong one.
    assert rows[0]["FACT"] is None


# The arithmetic
# --------------------------------------------------------------------------


@pytest.mark.parametrize("concept, code", [
    ("us-gaap:Liabilities", "IMBAL_BALANCE_SHEET"),
    ("us-gaap:LiabilitiesAndStockholdersEquity", "IMBAL_TOTALS"),
    ("us-gaap:CostOfRevenue", "IMBAL_INCOME"),
    ("us-gaap:IncomeTaxExpenseBenefit", "IMBAL_PROFIT"),
    ("us-gaap:NetCashProvidedByUsedInOperatingActivities", "IMBAL_CASHFLOW"),
])
def test_a_figure_off_by_a_tenth_is_reported(spark, lake, concept, code):
    facts = []
    for row in SOUND:
        value = row[2] * 1.1 if row[0] == concept else row[2]
        facts.append(fact(row[0], row[1], value))
    config = lake(facts, [filing_row()])

    assert code in codes(spark, config)


def test_rounding_is_not_a_fault(spark, lake):
    """A thousandth of the total is the line between the two."""
    facts = [fact(row[0], row[1],
                  row[2] + 1000 if row[0] == "us-gaap:Liabilities" else row[2])
             for row in SOUND]
    config = lake(facts, [filing_row()])

    assert "IMBAL_BALANCE_SHEET" not in codes(spark, config)


def test_mezzanine_equity_is_part_of_the_balance_sheet(spark, lake):
    """A US balance sheet presents it between the liabilities and the equity.

    Leaving it out reported NVIDIA's own 2016 and 2017 reports as unbalanced
    by exactly the temporary equity they hold, which is what put this concept
    in the check.
    """
    facts = [fact(row[0], row[1],
                  row[2] - 87 * M if row[0] == "us-gaap:StockholdersEquity"
                  else row[2])
             for row in SOUND]
    facts.append(fact("us-gaap:TemporaryEquityValueExcludingAdditionalPaidInCapital",
                      False, 87 * M))
    config = lake(facts, [filing_row()])

    assert "IMBAL_BALANCE_SHEET" not in codes(spark, config)


def test_a_negative_balance_sheet_total_is_reported(spark, lake):
    facts = [fact(row[0], row[1],
                  -row[2] if row[0] == "us-gaap:Assets" else row[2])
             for row in SOUND]
    config = lake(facts, [filing_row()])

    assert "IMPL_NEGATIVE_ASSETS" in codes(spark, config)


# What the comparison looked for and did not find
# --------------------------------------------------------------------------


@pytest.mark.parametrize("concept, code", [
    ("us-gaap:Assets", "MISS_ASSETS"),
    ("us-gaap:Revenues", "MISS_REVENUE"),
    ("us-gaap:NetIncomeLoss", "MISS_PROFIT"),
])
def test_a_line_the_report_does_not_tag_is_reported(spark, lake, concept, code):
    facts = [fact(*row) for row in SOUND if row[0] != concept]
    config = lake(facts, [filing_row()])

    assert code in codes(spark, config)


def test_a_second_concept_stands_in_for_the_first(spark, lake):
    """A registrant that tags revenue the other way is not reported."""
    facts = [fact(*row) for row in SOUND if row[0] != "us-gaap:Revenues"]
    facts.append(fact(
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        True, 215_938 * M))
    config = lake(facts, [filing_row()])

    assert "MISS_REVENUE" not in codes(spark, config)


# The facts themselves
# --------------------------------------------------------------------------


def test_a_period_that_ends_before_it_begins_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("us-gaap:Revenues", True, 1 * M,
                      prdstart=date(2026, 6, 1), prdend=date(2026, 1, 25),
                      unit="EUR"))
    config = lake(facts, [filing_row()])

    found = quality.find_issues(spark, config)
    rows = [row for row in found.collect() if row["ISSUE"] == "IMPL_PERIOD"]
    assert len(rows) == 1
    # A fault of one fact names that fact.
    assert rows[0]["FACT"] is not None


def test_one_figure_tagged_twice_with_two_values_is_reported(spark, lake):
    facts = sound_facts()
    facts.append((CIK, FILING, FILED.year, f"{FILING}:second",
                  "us-gaap:Assets", None, PERIODEND, "USD",
                  float(999 * M), "10-K", FILED, None))
    config = lake(facts, [filing_row()])

    assert "DUPL_FACT_VALUE" in codes(spark, config)


def test_an_instant_tagged_twice_is_reported_too(spark, lake):
    """An equi-join on the empty period of an instant matches nothing, which
    let every balance sheet figure past until the keys were filled first."""
    facts = sound_facts()
    facts.append((CIK, FILING, FILED.year, f"{FILING}:another",
                  "us-gaap:Liabilities", None, PERIODEND, "USD",
                  float(1 * M), "10-K", FILED, None))
    config = lake(facts, [filing_row()])

    found = quality.find_issues(spark, config)
    rows = [row for row in found.collect() if row["ISSUE"] == "DUPL_FACT_VALUE"]
    assert len(rows) == 2


def test_the_same_figure_twice_with_one_value_is_not_a_fault(spark, lake):
    """A figure appears in the statements and again in the notes."""
    facts = sound_facts()
    facts.append((CIK, FILING, FILED.year, f"{FILING}:again",
                  "us-gaap:Assets", None, PERIODEND, "USD",
                  float(206_803 * M), "10-K", FILED, None))
    config = lake(facts, [filing_row()])

    assert "DUPL_FACT_VALUE" not in codes(spark, config)


def test_one_concept_in_two_units_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("us-gaap:Assets", False, 206_803 * M, unit="EUR"))
    config = lake(facts, [filing_row()])

    assert "MIXED_UNIT" in codes(spark, config)


# The filing as a whole
# --------------------------------------------------------------------------


def test_a_filing_made_before_its_period_ended_is_reported(spark, lake):
    config = lake(sound_facts(), [filing_row(filed=date(2025, 12, 1))])

    assert "IMPL_FILED_BEFORE_PERIODEND" in codes(spark, config)


def test_a_cover_page_year_far_from_the_period_is_reported(spark, lake):
    config = lake(sound_facts(), [filing_row(fy=2030)])

    assert "IMPL_FY_PERIODEND" in codes(spark, config)


def test_a_january_year_end_named_by_the_later_year_is_not(spark, lake):
    """NVIDIA's year to 25 January 2026 is its fiscal 2026, and that is fine."""
    config = lake(sound_facts(), [filing_row(fy=2026)])

    assert "IMPL_FY_PERIODEND" not in codes(spark, config)


def test_a_filing_tagging_against_no_reporting_taxonomy_is_reported(spark, lake):
    config = lake(sound_facts(), [filing_row(framework=None)])

    assert "MISS_FRAMEWORK" in codes(spark, config)


def test_a_quarterly_report_is_not_held_to_the_annual_relations(spark, lake):
    """A quarterly report states the year to date beside the quarter, and which
    of those the relations hold over is a question the data does not answer."""
    config = lake(sound_facts(), [filing_row(form="10-Q")])

    assert "MISS_ASSETS" not in codes(spark, config)


# What a later report says about the same year
# --------------------------------------------------------------------------


def test_a_figure_a_later_report_restates_is_reported(spark, lake):
    later = "0001045810-27-000011"
    later_filed = date(2027, 2, 24)
    facts = sound_facts()
    # The later report's account of the same year, with the result moved.
    facts.append(fact("us-gaap:NetIncomeLoss", True, 130_000 * M,
                      filing=later, filed=later_filed))
    filings = [filing_row(),
               filing_row(filing=later, filed=later_filed,
                          periodend=date(2027, 1, 31), fy=2027)]
    config = lake(facts, filings)

    found = quality.find_issues(spark, config)
    rows = [row for row in found.collect()
            if row["ISSUE"] == "RESTATED_MATERIALLY"]
    assert len(rows) == 1
    # The fact named is the one the year's own report stated.
    assert rows[0]["FILING"] == FILING


def test_a_share_split_is_not_a_restatement(spark, lake):
    """A split restates every per-share figure by arithmetic. Reporting those
    filled the check with NVIDIA's own history and said nothing."""
    later = "0001045810-27-000011"
    later_filed = date(2027, 2, 24)
    facts = sound_facts()
    facts.append(fact("us-gaap:EarningsPerShareDiluted", True, 4.90,
                      unit="USD/shares"))
    facts.append(fact("us-gaap:EarningsPerShareDiluted", True, 0.49,
                      filing=later, filed=later_filed, unit="USD/shares"))
    filings = [filing_row(),
               filing_row(filing=later, filed=later_filed,
                          periodend=date(2027, 1, 31), fy=2027)]
    config = lake(facts, filings)

    assert "RESTATED_MATERIALLY" not in codes(spark, config)


def test_a_figure_a_later_report_repeats_is_not_reported(spark, lake):
    later = "0001045810-27-000011"
    later_filed = date(2027, 2, 24)
    facts = sound_facts()
    facts.append(fact("us-gaap:NetIncomeLoss", True, 120_067 * M,
                      filing=later, filed=later_filed))
    filings = [filing_row(),
               filing_row(filing=later, filed=later_filed,
                          periodend=date(2027, 1, 31), fy=2027)]
    config = lake(facts, filings)

    assert "RESTATED_MATERIALLY" not in codes(spark, config)


# Running over part of the population
# --------------------------------------------------------------------------


def test_a_run_over_one_registrant_keeps_what_is_known_of_the_others(spark, lake):
    other = "0001177394"
    facts = sound_facts()
    # A second registrant whose report does not balance, filed in another year.
    facts.append((other, "0001177394-25-000001", 2025, "f1", "us-gaap:Assets",
                  None, date(2025, 11, 30), "USD", 100.0, "10-K",
                  date(2025, 12, 31), None))
    filings = [filing_row(),
               ("0001177394-25-000001", other, "OTHER", "10-K",
                date(2025, 12, 31), date(2025, 11, 30), 2025, "FY",
                "dei;us-gaap", "US-GAAP", 1)]
    config = lake(facts, filings)

    quality.update_silver_secfiling_quality(spark, config)
    before = common.connect_to_secfiling(spark, config, "issues").count()
    assert before > 0

    # Checking NVIDIA alone must not drop the other registrant's issues, which
    # sit in another filed year.
    quality.update_silver_secfiling_quality(spark, config, ciks=[CIK])
    after = common.connect_to_secfiling(spark, config, "issues")

    assert {row["CIK"] for row in after.collect()} == {other}
    assert after.count() == before
