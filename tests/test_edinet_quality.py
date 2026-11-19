"""The checks the figures of a Japanese annual securities report are held to.

Each test builds a lake holding one sound annual report and then breaks one
thing about it, so that a check is shown to fire on the fault and -- which
matters as much -- to stay quiet on the sound report.

The figures of the sound report are Kojima's for its financial year to 31
August 2026, in millions of yen, because they are public and they add up:
297,632 of revenue less 216,014 of cost is 81,618 of gross profit, less
72,995 of selling expenses is 8,622 of operating profit, plus 436 of
non-operating income less 147 of non-operating expenses is 8,911 of ordinary
profit, plus 119 of extraordinary income less 811 of extraordinary loss is
8,219 before tax, less 2,690 of tax is 5,529 for the year.

Kojima prepares no consolidated statements, so the sound report carries every
figure under ``NonConsolidatedMember``, which is where a filer that does not
consolidate puts them. A second fixture states the same figures without a
dimension, as a filer that does consolidate would, so that the checks are
shown to read both.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from fincommons.financialreport.edinet import common, quality

CODE = "E03270"
FILING = "S100ZH4D"
PERIODEND = date(2026, 8, 31)
PRDSTART = date(2025, 9, 1)
SUBMITTED = datetime(2026, 11, 13, 14, 31)
FILEDYEAR = 2026
MEMBER = "NonConsolidatedMember"

M = 1_000_000

#: The sound report: the element, whether it is a flow, and the value.
SOUND = [
    ("jppfs_cor:Assets", False, 128_669 * M),
    ("jppfs_cor:Liabilities", False, 55_075 * M),
    ("jppfs_cor:NetAssets", False, 73_593 * M),
    ("jppfs_cor:LiabilitiesAndNetAssets", False, 128_669 * M),
    ("jppfs_cor:CurrentAssets", False, 92_972 * M),
    ("jppfs_cor:NoncurrentAssets", False, 35_697 * M),
    ("jppfs_cor:CurrentLiabilities", False, 40_027 * M),
    ("jppfs_cor:NoncurrentLiabilities", False, 15_047 * M),
    ("jppfs_cor:NetSales", True, 297_632 * M),
    ("jppfs_cor:CostOfGoodsSold", True, 216_014 * M),
    ("jppfs_cor:GrossProfit", True, 81_618 * M),
    ("jppfs_cor:OperatingIncome", True, 8_622 * M),
    ("jppfs_cor:NonOperatingIncome", True, 436 * M),
    ("jppfs_cor:NonOperatingExpenses", True, 147 * M),
    ("jppfs_cor:OrdinaryIncome", True, 8_911 * M),
    ("jppfs_cor:ExtraordinaryIncome", True, 119 * M),
    ("jppfs_cor:ExtraordinaryLoss", True, 811 * M),
    ("jppfs_cor:IncomeBeforeIncomeTaxes", True, 8_219 * M),
    ("jppfs_cor:IncomeTaxesCurrent", True, 2_299 * M),
    ("jppfs_cor:IncomeTaxesDeferred", True, 390 * M),
    ("jppfs_cor:IncomeTaxes", True, 2_690 * M),
    ("jppfs_cor:ProfitLoss", True, 5_529 * M),
    # The five-year summary of the front matter states the same year again.
    ("jpcrp_cor:NetSalesSummaryOfBusinessResults", True, 297_632 * M),
    ("jpcrp_cor:TotalAssetsSummaryOfBusinessResults", False, 128_669 * M),
]

#: What the filing says about itself, which decides the basis and the periods.
DEI = [
    ("jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI", "false"),
    ("jpdei_cor:AccountingStandardsDEI", "Japan GAAP"),
    ("jpdei_cor:CurrentFiscalYearStartDateDEI", "2025-09-01"),
    ("jpdei_cor:CurrentFiscalYearEndDateDEI", "2026-08-31"),
    ("jpdei_cor:PreviousFiscalYearStartDateDEI", "2024-09-01"),
    ("jpdei_cor:PreviousFiscalYearEndDateDEI", "2025-08-31"),
]


def fact(concept, flow, value, member=MEMBER, prdend=PERIODEND,
         prdstart=PRDSTART, unit="JPY", text=None, context=None, basis=None,
         filing=FILING, label="label", occurrence=0):
    """One row of the table of facts."""
    start = prdstart if flow else None
    context = context or ("CurrentYearDuration" if flow
                          else "CurrentYearInstant")
    if member:
        context = f"{context}_{member}"
    if basis is None:
        basis = "個別" if member == MEMBER else "連結"
    return (CODE, filing, FILEDYEAR,
            f"{filing}:{concept}:{context}:{occurrence}",
            concept, label, context,
            "当期" if flow else "当期末", basis, "期間" if flow else "時点",
            start, prdend, member, unit,
            None if value is None else float(value), text,
            "jpcrp030000-asr")


def dei_fact(concept, text, filing=FILING):
    """One row of the document and entity information, which carries text."""
    return (CODE, filing, FILEDYEAR, f"{filing}:{concept}:FilingDateInstant:0",
            concept, "label", "FilingDateInstant", "提出日時点", "その他",
            "時点", None, date(2026, 11, 13), None, None, None, text,
            "jpcrp030000-asr")


def filing_row(filing=FILING, doctype="120", periodend=PERIODEND,
               periodstart=PRDSTART, submitted=SUBMITTED):
    """One row of the table of filings."""
    return (filing, CODE, "75130", "2060001001667", "株式会社コジマ", None,
            "010", "030000", doctype, periodstart, periodend, submitted,
            FILEDYEAR, date(2026, 11, 13), "有価証券報告書", None,
            "0", "0", "0", "1", "1", "1", "0")


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
        {"edinet": {name: str(base / name) for name in ("bronze", "silver")}}))
    config_path = str(config_path)
    common.create_directory_tree(config_path)

    def write(facts, filings):
        write_silver(
            spark.createDataFrame(facts, common.facts_schema),
            common.ref_silver_facts(config_path), partition_by=("filed_year",))
        write_silver(
            spark.createDataFrame(filings, common.filings_schema),
            common.ref_silver_filings(config_path),
            partition_by=("filed_year",))
        return config_path

    return write


def codes(spark, config, **kwargs):
    """The codes the checks report over a lake."""
    found = quality.find_issues(spark, config, **kwargs)
    return sorted({row["issue"] for row in found.collect()})


def sound_facts(member=MEMBER, consolidated="false"):
    rows = [fact(*row, member=member) for row in SOUND]
    rows += [dei_fact(concept,
                      consolidated if concept.endswith("PreparedDEI") else text)
             for concept, text in DEI]
    return rows


# The sound report
# --------------------------------------------------------------------------


def test_a_report_that_adds_up_is_reported_for_nothing(spark, lake):
    config = lake(sound_facts(), [filing_row()])

    assert codes(spark, config) == []


def test_the_same_report_consolidated_is_reported_for_nothing(spark, lake):
    """The group's figures carry no dimension, and the checks read those."""
    config = lake(sound_facts(member=None, consolidated="true"),
                  [filing_row()])

    assert codes(spark, config) == []


def test_the_issue_table_is_written_and_read_back(spark, lake):
    config = lake(sound_facts(), [filing_row()])
    quality.update_silver_edinet_quality(spark, config)

    issues = common.connect_to_edinet(spark, config, "issues")
    assert issues.count() == 0
    assert issues.columns == list(quality.issues_schema.names)


def test_a_fault_reaches_the_table_with_the_day_it_was_found(spark, lake):
    facts = [row for row in sound_facts()
             if "GrossProfit" not in row[4]]
    facts.append(fact("jppfs_cor:GrossProfit", True, 99_999 * M))
    config = lake(facts, [filing_row()])

    quality.update_silver_edinet_quality(spark, config)
    rows = common.connect_to_edinet(spark, config, "issues").collect()

    assert [row["issue"] for row in rows] == ["IMBAL_INCOME"]
    assert rows[0]["checked"] == date.today()
    # A filing-level fault names no fact: the data cannot say which of the
    # three figures is the wrong one.
    assert rows[0]["fact"] is None


# The arithmetic
# --------------------------------------------------------------------------


@pytest.mark.parametrize("concept, code", [
    ("jppfs_cor:Liabilities", "IMBAL_BALANCE_SHEET"),
    ("jppfs_cor:LiabilitiesAndNetAssets", "IMBAL_TOTALS"),
    ("jppfs_cor:CurrentAssets", "IMBAL_ASSETS"),
    ("jppfs_cor:CurrentLiabilities", "IMBAL_LIABILITIES"),
    ("jppfs_cor:CostOfGoodsSold", "IMBAL_INCOME"),
    ("jppfs_cor:NonOperatingIncome", "IMBAL_ORDINARY"),
    ("jppfs_cor:ExtraordinaryLoss", "IMBAL_PRETAX"),
    ("jppfs_cor:IncomeTaxes", "IMBAL_PROFIT"),
    ("jppfs_cor:IncomeTaxesCurrent", "IMBAL_TAX"),
])
def test_a_figure_moved_by_a_tenth_of_revenue_is_reported(spark, lake,
                                                          concept, code):
    moved = 29_763 * M  # a tenth of revenue, which no tolerance admits
    facts = []
    for row in SOUND:
        value = row[2] + moved if row[0] == concept else row[2]
        facts.append(fact(row[0], row[1], value))
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row()])

    assert code in codes(spark, config)


def test_the_two_lines_only_a_japanese_statement_has_are_checked(spark, lake):
    """経常利益 and the result before tax, neither of which IFRS states.

    The ordinary result is the operating result and the recurring items
    outside it; the result before tax is the ordinary result and the
    extraordinary items. Both relations are stated by the filing on both
    sides and neither can be written for an IFRS or a US GAAP filer.
    """
    facts = []
    for row in SOUND:
        value = row[2]
        if row[0] == "jppfs_cor:OrdinaryIncome":
            value = 1 * M  # too small for either relation to hold
        facts.append(fact(row[0], row[1], value))
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row()])

    found = codes(spark, config)
    assert "IMBAL_ORDINARY" in found
    assert "IMBAL_PRETAX" in found


def test_a_line_the_filing_does_not_state_is_not_read_as_nothing(spark, lake):
    """A missing component makes a check unanswerable, not failed.

    An IFRS statement of financial position subtotals its current liabilities
    and leaves the non-current side to be read as the remainder. A check that
    read the missing total as zero reported Fujitsu's liabilities as short by
    244,695 million; the difference is not computable and the check is silent.
    """
    facts = [fact(*row) for row in SOUND
             if row[0] != "jppfs_cor:NoncurrentLiabilities"]
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row()])

    assert "IMBAL_LIABILITIES" not in codes(spark, config)


def test_a_filer_with_no_extraordinary_items_passes_the_pretax_check(spark,
                                                                     lake):
    """Both extraordinary lines are optional, and a filer that had none in
    the year states neither; its ordinary result is then its pre-tax result."""
    facts = []
    for row in SOUND:
        if row[0] in ("jppfs_cor:ExtraordinaryIncome",
                      "jppfs_cor:ExtraordinaryLoss"):
            continue
        value = 8_911 * M if row[0] == "jppfs_cor:IncomeBeforeIncomeTaxes" \
            else row[2]
        facts.append(fact(row[0], row[1], value))
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row()])

    assert "IMBAL_PRETAX" not in codes(spark, config)


def test_a_split_of_the_result_that_does_not_add_up_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("jppfs_cor:ProfitLossAttributableToOwnersOfParent",
                      True, 4_000 * M))
    facts.append(fact("jppfs_cor:ProfitLossAttributableToNonControllingInterests",
                      True, 100 * M))
    config = lake(facts, [filing_row()])

    assert "IMBAL_PROFIT_SPLIT" in codes(spark, config)


def test_a_filer_that_states_no_split_is_not_reported_for_it(spark, lake):
    """A filer preparing no consolidated statements has no minority."""
    config = lake(sound_facts(), [filing_row()])

    assert "IMBAL_PROFIT_SPLIT" not in codes(spark, config)


def test_negative_total_assets_are_reported(spark, lake):
    facts = []
    for row in SOUND:
        value = -row[2] if row[0] == "jppfs_cor:Assets" else row[2]
        facts.append(fact(row[0], row[1], value))
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row()])

    assert "IMPL_NEGATIVE_ASSETS" in codes(spark, config)


# The checks only a Japanese report affords
# --------------------------------------------------------------------------


def test_the_summary_and_the_statements_are_set_against_each_other(spark,
                                                                   lake):
    """The front matter and the statements state the same year twice.

    This is the only check of the package that tests a filing against another
    part of itself, and it rests on a Japanese report carrying a five-year
    summary of its headline figures in its front matter.
    """
    facts = []
    for row in SOUND:
        value = 200_000 * M \
            if row[0] == "jpcrp_cor:NetSalesSummaryOfBusinessResults" \
            else row[2]
        facts.append(fact(row[0], row[1], value))
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row()])

    assert "SUMMARY_DISAGREES" in codes(spark, config)


def test_a_filer_whose_years_differ_in_length_is_reported(spark, lake):
    """The period of anything older than the comparative is shifted by whole
    years, and a filer on a 52- or 53-week calendar has years that are not a
    whole number of years apart.

    No report read while this check was written trips it -- a leap year moves
    a year end by one day and the slack admits that -- so the case is built
    here: a year of 371 days against one of 364.
    """
    facts = sound_facts()
    facts = [row for row in facts
             if "PreviousFiscalYearStartDate" not in row[4]]
    # A prior year a week longer than the current one.
    facts.append(dei_fact("jpdei_cor:PreviousFiscalYearStartDateDEI",
                          "2024-08-25"))
    # And a fact old enough for the shift to matter.
    facts.append(fact("jppfs_cor:NetSales", True, 282_790 * M,
                      context="Prior2YearDuration",
                      prdstart=date(2023, 9, 1), prdend=date(2024, 8, 31),
                      occurrence=1))
    config = lake(facts, [filing_row()])

    assert "PERIOD_SHIFT_INEXACT" in codes(spark, config)


def test_a_leap_year_alone_is_not_reported(spark, lake):
    """One day of drift is the leap year and not a calendar worth warning
    about; see YEAR_LENGTH_SLACK."""
    facts = [row for row in sound_facts()
             if "PreviousFiscalYearStartDate" not in row[4]]
    facts.append(dei_fact("jpdei_cor:PreviousFiscalYearStartDateDEI",
                          "2024-08-31"))
    facts.append(fact("jppfs_cor:NetSales", True, 282_790 * M,
                      context="Prior2YearDuration",
                      prdstart=date(2023, 9, 1), prdend=date(2024, 8, 31),
                      occurrence=1))
    config = lake(facts, [filing_row()])

    assert "PERIOD_SHIFT_INEXACT" not in codes(spark, config)


def test_a_date_the_filing_states_as_not_applicable_is_no_date(spark, lake):
    """The conversion writes a full-width dash where a date does not apply.

    The period checks read it as no date, as they read any date the filing
    does not state, instead of failing on it.
    """
    facts = [row for row in sound_facts()
             if "PreviousFiscalYearStartDate" not in row[4]
             and "CurrentFiscalYearEndDate" not in row[4]]
    facts.append(dei_fact("jpdei_cor:PreviousFiscalYearStartDateDEI", "－"))
    facts.append(dei_fact("jpdei_cor:CurrentFiscalYearEndDateDEI", "－"))
    config = lake(facts, [filing_row()])

    found = codes(spark, config)
    assert "PERIOD_SHIFT_INEXACT" not in found
    assert "INDEX_PERIOD_DIFFERS" not in found


def test_a_fact_with_no_period_at_all_is_reported(spark, lake):
    """The module refuses to invent dates for a context it does not
    recognise, so the fact can be read and compared with nothing."""
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", True, 1 * M,
                      context="SomethingElse", prdstart=None, prdend=None,
                      occurrence=2))
    config = lake(facts, [filing_row()])

    assert "PERIOD_UNRESOLVED" in codes(spark, config)


def test_the_basis_column_against_the_dimension(spark, lake):
    """連結・個別 and NonConsolidatedMember cannot disagree.

    The correspondence held for every one of 6,752 facts of the reports this
    check was written against.
    """
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", True, 1 * M, basis="連結",
                      occurrence=3))
    config = lake(facts, [filing_row()])

    assert "BASIS_CONTRADICTS_DIMENSION" in codes(spark, config)


def test_an_element_named_for_a_ratio_in_another_unit_is_reported(spark,
                                                                  lake):
    """Fujitsu tags EquityToAssetRatioIFRSSummaryOfBusinessResults with
    1,167.25 in JPYPerShares: its equity per share, under the name of a
    ratio. A reader trusting the name prints an equity ratio of 1,167 per
    cent."""
    facts = sound_facts()
    facts.append(fact("jpcrp_cor:EquityToAssetRatioIFRSSummaryOfBusinessResults",
                      False, 1167.25, unit="JPYPerShares", occurrence=4))
    config = lake(facts, [filing_row()])

    assert "UNIT_NOT_A_RATIO" in codes(spark, config)


def test_a_ratio_that_is_a_pure_number_is_not_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("jpcrp_cor:EquityToAssetRatioSummaryOfBusinessResults",
                      False, 0.571, unit="pure", occurrence=5))
    config = lake(facts, [filing_row()])

    assert "UNIT_NOT_A_RATIO" not in codes(spark, config)


def test_a_filing_that_says_it_consolidates_and_states_nothing_is_reported(
        spark, lake):
    """The flag is set and no figure sits outside a dimension, so every
    figure read on the consolidated basis is missing rather than wrong."""
    facts = sound_facts(consolidated="true")
    config = lake(facts, [filing_row()])

    assert "MISS_CONSOLIDATED" in codes(spark, config)


def test_the_index_and_the_filing_disagreeing_on_the_period_is_reported(
        spark, lake):
    config = lake(sound_facts(),
                  [filing_row(periodend=date(2026, 3, 31),
                              periodstart=date(2025, 4, 1))])

    assert "INDEX_PERIOD_DIFFERS" in codes(spark, config)


# The structural checks
# --------------------------------------------------------------------------


def test_a_period_that_ends_before_it_begins_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", True, 1 * M,
                      prdstart=date(2027, 1, 1), prdend=date(2026, 8, 31),
                      occurrence=6))
    config = lake(facts, [filing_row()])

    assert "IMPL_PERIOD" in codes(spark, config)


def test_a_fact_with_neither_a_number_nor_a_text_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", None, None, occurrence=7))
    config = lake(facts, [filing_row()])

    assert "MISS_FACT_VALUE" in codes(spark, config)


def test_one_figure_tagged_twice_with_two_values_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", True, 1 * M, occurrence=8))
    config = lake(facts, [filing_row()])

    assert "DUPL_FACT_VALUE" in codes(spark, config)


def test_one_figure_tagged_twice_with_one_value_is_not_reported(spark, lake):
    """A figure appears in the statements and again in the notes, and the
    number of employees was tagged three times under one context with one
    value in a report this check was written against."""
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", True, 297_632 * M, occurrence=9))
    config = lake(facts, [filing_row()])

    assert "DUPL_FACT_VALUE" not in codes(spark, config)


def test_one_concept_in_two_units_is_reported(spark, lake):
    facts = sound_facts()
    facts.append(fact("jppfs_cor:NetSales", True, 297_632 * M, unit="USD",
                      occurrence=10))
    config = lake(facts, [filing_row()])

    assert "MIXED_UNIT" in codes(spark, config)


def test_a_report_mostly_in_the_filers_own_concepts_is_counted(spark, lake):
    facts = sound_facts()
    for i in range(len(SOUND) * 2):
        facts.append(fact(f"jpcrp030000-asr_{CODE}-000:Own{i}", True, i + 1,
                          occurrence=100 + i))
    config = lake(facts, [filing_row()])

    assert "HIGH_EXTENSION_SHARE" in codes(spark, config)


def test_an_ordinary_report_is_not_counted_as_extension_heavy(spark, lake):
    """The four reports the threshold was set against run from 1.7 to 7.1
    per cent."""
    config = lake(sound_facts(), [filing_row()])

    assert "HIGH_EXTENSION_SHARE" not in codes(spark, config)


# What is checked, and over which documents
# --------------------------------------------------------------------------


def test_a_quarterly_report_is_not_held_to_the_annual_relations(spark, lake):
    """A quarterly report states a balance sheet of its own but a profit and
    loss account of the period and of the year to date, and which of those
    the annual relations hold over is a question the data does not answer."""
    facts = []
    for row in SOUND:
        value = 1 * M if row[0] == "jppfs_cor:Liabilities" else row[2]
        facts.append(fact(row[0], row[1], value))
    facts += [dei_fact(c, t) for c, t in DEI]
    config = lake(facts, [filing_row(doctype="140")])

    assert "IMBAL_BALANCE_SHEET" not in codes(spark, config)


def test_a_run_over_one_filer_leaves_the_others_alone(spark, lake):
    other = "E99999"
    facts = sound_facts()
    facts += [tuple([other] + list(row[1:3]) + [f"OTHER{row[3]}"] + list(row[4:]))
              for row in sound_facts()[:1]]
    config = lake(facts, [filing_row()])

    found = quality.find_issues(spark, config, codes=[other])
    assert found.count() == 0


def test_the_checks_the_catalogue_defines_are_the_checks_that_run():
    """A code reaching the table that the catalogue does not describe would
    be a string nobody can look up; run_checks refuses one, and this says the
    other way round as well."""
    from fincommons.dataquality import checks_of

    assert sorted(code for code, _ in quality.CHECKS) \
        == sorted(checks_of(quality.DATASET))
