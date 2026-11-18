"""EDINET: the checks the figures are held to, and the issues they find.

The Silver tables say what the agency published, which is the agency's own
conversion of what the filer tagged. Whether what it tagged adds up is a
different question, and this module answers it in a table of its own: one row
per issue found, keyed by the filing and, where the issue is about one fact,
by the fact.

The machinery is :mod:`fincommons.dataquality` -- how a check is described,
how its rows are gathered, what is written where a run looks at part of the
table only. What is here is the checks themselves, because a check knows the
data set it is made of.

What a Japanese report affords that the other two do not
--------------------------------------------------------

The arithmetic checks are the valuable ones, as in the other two regimes, and
they are the relations a statement states twice. Two of them exist only here,
and they are the two lines that make a Japanese profit and loss account what
it is. The ordinary result, 経常利益, is the operating result and the
recurring items outside it, and the filing states all four figures; the
result before tax is the ordinary result and the extraordinary items, and the
filing states those too. Neither relation can be written for an IFRS or a US
GAAP filer, because neither has the lines.

Three more checks have no counterpart anywhere in this package.

**The filer states its own year twice.** The five-year summary of the front
matter and the financial statements both carry the revenue and the total
assets of the year the report is about, in different elements of different
taxonomies. They have to agree, and :func:`check_summary_disagrees` is the
only check in this package that tests a filing against another part of
itself rather than against arithmetic.

**The period of a fact is resolved and not reported.** The conversion carries
no dates, so the module anchors the current year and the comparative on the
dates the filing states about itself and shifts everything further back by
whole years. That shift is exact only where the filer's years are of equal
length -- and the filing says how long its two most recent years were, so
whether the shift can be trusted is a question the data answers.
:func:`check_period_shift_inexact` asks it.

**The conversion says twice whether a figure is the group's.** 連結・個別 is a
column of the conversion and ``NonConsolidatedMember`` is a dimension of the
context, and the two cannot disagree: over the reports read while this module
was written the correspondence held for every one of 6,752 facts.
:func:`check_basis_contradicts_dimension` is what notices if it ever stops.

Nothing is dropped and nothing is corrected. A check writes a row and leaves
the figure where it is: the tables are what the agency published, and a
reader who wants to know what is wrong with a figure before using it joins
the issues on.
"""

import datetime
import logging

import pyspark.sql.functions as func
from pyspark.sql.types import DateType, IntegerType, StringType, StructType

from ...dataquality import (CHECKED_COLUMN, merged_issues, partitioning,
                            run_checks)
from ...layout import rebalanced
from ...log import context, pipeline_run
from ...storage import read_silver, silver_table_exists, write_silver
from .common import (ANNUAL_TYPES, SOURCE, ref_silver_facts,
                     ref_silver_filings, ref_silver_issues)

logger = logging.getLogger(__name__)

#: The name the check catalogue records these checks under.
DATASET = "edinet"

issues_schema = StructType() \
    .add("edinet_code", StringType(), True) \
    .add("filed_year", IntegerType(), False) \
    .add("filing", StringType(), False) \
    .add("fact", StringType(), True) \
    .add("concept", StringType(), True) \
    .add("issue", StringType(), False) \
    .add("checked", DateType(), False)
"""The shape of the table of issues: one row per issue found.

``filed_year`` is the partition column, as it is on the tables of facts and of
filings, so that the issues of a filing sit in the same partition as the
facts they are about.

``fact`` and ``concept`` name the fact an issue is about, and are empty where
the issue is about the filing as a whole: a balance sheet that does not
balance is a fault of the filing and not of any one of the facts that makes
it up, and pointing at one of them would be picking a culprit the data cannot
name.

``issue`` is the code, which
:func:`fincommons.dataquality.describe_check` turns into the sentence that
says what it means. ``checked`` is the day the run was made, not the day the
problem arose: the issues of a partition are replaced whole when it is
checked again.

A filing with nothing wrong with it has no row here. The absence of a code is
what says a record passed.
"""

#: The documents the arithmetic checks are run over: the annual securities
#: report and its correction. A quarterly or half-year report states a
#: balance sheet of its own but a profit and loss account of the period and of
#: the year to date, and which of those the annual relations hold over is a
#: question the data does not answer.
ANNUAL_DOCTYPES = ANNUAL_TYPES

#: A difference this far into the scale of the figure is a tagging fault
#: rather than the rounding of an amount stated in millions.
TOLERANCE = 0.001

#: How far the two most recent financial years of a filer may differ in
#: length before a period shifted by whole years is held to drift. A leap year
#: moves a year end by one day and a filer closing on the last day of February
#: by as much as three; a filer on a 52- or 53-week calendar, or one that has
#: moved its year end, differs by a week or more, and it is that one the check
#: is about.
YEAR_LENGTH_SLACK = 3

#: What share of a filing's facts may be tagged with concepts the filer
#: created before the filing is worth counting as hard to compare. The four
#: reports read while this module was written run from 1.7 to 7.1 per cent.
EXTENSION_SHARE = 0.5

#: The prefixes of the taxonomies. A concept under any other prefix is one the
#: filer created for its own report, the prefix being built from the form and
#: the filer's EDINET code.
TAXONOMY_PREFIXES = ("jppfs_cor", "jpcrp_cor", "jpdei_cor", "jpigp_cor",
                     "jpctl_cor", "jpsps_cor", "jppsp_cor", "jpaud_cor")

#: The dimension the parent company's own figures carry. The consolidated
#: ones carry no dimension at all, which is why a check cannot simply look
#: for the group's member: there is none.
NON_CONSOLIDATED = "NonConsolidatedMember"

#: The elements each figure of the checks is read from, best first, matched on
#: the local name and never on the prefix -- a filer that creates a concept of
#: its own gives it the taxonomy's name under a prefix of its own making. The
#: ``...IFRS`` spellings belong to the jpigp_cor taxonomy, which a filer
#: reporting its group under IFRS uses for its consolidated statements.
CONCEPTS = {
    # Balance sheet
    "assets": ["Assets", "AssetsIFRS"],
    "current_assets": ["CurrentAssets", "CurrentAssetsIFRS"],
    "noncurrent_assets": ["NoncurrentAssets", "NonCurrentAssetsIFRS"],
    "liabilities": ["Liabilities", "LiabilitiesIFRS"],
    "current_liabilities": ["CurrentLiabilities", "TotalCurrentLiabilitiesIFRS"],
    "noncurrent_liabilities": ["NoncurrentLiabilities",
                               "TotalNonCurrentLiabilitiesIFRS"],
    "net_assets": ["NetAssets", "EquityIFRS"],
    "liabilities_and_net_assets": ["LiabilitiesAndNetAssets",
                                   "LiabilitiesAndEquityIFRS"],

    # Profit and loss
    "revenue": ["NetSales", "RevenueIFRS", "OperatingRevenue"],
    # A trading company builds its cost of sales up from the opening stock,
    # the purchases and the closing stock and states no 売上原価 total;
    # 商品売上原価 is the same line of the same statement.
    "cost_of_sales": ["CostOfSales", "CostOfGoodsSold", "CostOfSalesIFRS"],
    "gross_profit": ["GrossProfit", "GrossProfitIFRS"],
    "operating_income": ["OperatingIncome", "OperatingProfitLossIFRS"],
    "nonoperating_income": ["NonOperatingIncome"],
    "nonoperating_expenses": ["NonOperatingExpenses"],
    "ordinary_income": ["OrdinaryIncome"],
    "extraordinary_income": ["ExtraordinaryIncome"],
    "extraordinary_loss": ["ExtraordinaryLoss"],
    "pretax": ["IncomeBeforeIncomeTaxes", "ProfitLossBeforeTaxIFRS"],
    # An IFRS statement presents the result before tax of the continuing
    # operations and the result of the discontinued ones beside it, without
    # adding the two up. Fujitsu states 409,034 million and 146,904 million.
    "pretax_discontinued": ["ProfitLossBeforeTaxFromDiscontinuedOperationsIFRS"],
    "income_taxes": ["IncomeTaxes", "IncomeTaxExpenseIFRS"],
    "income_taxes_current": ["IncomeTaxesCurrent"],
    "income_taxes_deferred": ["IncomeTaxesDeferred"],
    "profit": ["ProfitLoss", "ProfitLossIFRS"],
    "profit_parent": ["ProfitLossAttributableToOwnersOfParent",
                      "ProfitLossAttributableToOwnersOfParentIFRS"],
    "profit_nci": ["ProfitLossAttributableToNonControllingInterests",
                   "ProfitLossAttributableToNonControllingInterestsIFRS"],

    # The five-year summary of the front matter, against which the statements
    # of the year the report is about are set.
    "summary_revenue": ["NetSalesSummaryOfBusinessResults",
                        "RevenueIFRSSummaryOfBusinessResults"],
    "summary_assets": ["TotalAssetsSummaryOfBusinessResults",
                       "TotalAssetsIFRSSummaryOfBusinessResults"],
}

#: What a filing says about itself, in the document and entity information.
DEI = {
    "consolidated": "WhetherConsolidatedFinancialStatementsArePreparedDEI",
    "standards": "AccountingStandardsDEI",
    "current_start": "CurrentFiscalYearStartDateDEI",
    "current_end": "CurrentFiscalYearEndDateDEI",
    "prior_start": "PreviousFiscalYearStartDateDEI",
    "prior_end": "PreviousFiscalYearEndDateDEI",
}

#: The columns that identify the record an issue is about. Every check returns
#: these, with the two that name a fact left null where the issue is about the
#: filing as a whole.
KEYS = ("edinet_code", "filed_year", "filing", "fact", "concept")


def _filing_level(frame):
    """A frame of filing-level issues: the keys, with no fact named."""
    return (frame
            .withColumn("fact", func.lit(None).cast("string"))
            .withColumn("concept", func.lit(None).cast("string"))
            .select(*KEYS))


def _local(column="concept"):
    """The element's local name: what a check matches on."""
    return func.regexp_replace(func.col(column), r"^[^:]*:", "")


def dei_of(facts):
    """One row per filing, one column per thing the filing says about itself.

    The document and entity information is the authority on two questions no
    other part of a filing settles: whether the filer prepared consolidated
    statements, and what its own financial years were. Both decide what the
    rest of the checks may assume.
    """
    # The local name is derived here where the caller has not already done
    # it, so that this reads a table of facts as it stands rather than one a
    # particular caller has prepared.
    if "_LOCAL" not in facts.columns:
        facts = facts.withColumn("_LOCAL", _local())
    wanted = facts.where(func.col("_LOCAL").isin(list(DEI.values())))
    wide = (wanted.groupBy("filing")
            .pivot("_LOCAL", list(DEI.values()))
            .agg(func.first("text")))
    for key, element in DEI.items():
        wide = wide.withColumn(
            key,
            func.col(f"`{element}`") if element in wide.columns
            else func.lit(None).cast("string"))
    return wide.select("filing", *DEI)


def annual_figures(facts, filings):
    """One row per annual report, one column per figure the checks need.

    The figures of a filing are the ones it states for its own period end, on
    the basis the filing itself says it reports on: the consolidated accounts
    where it prepared them, the parent company's own where it did not. That
    choice is the first thing a Japanese report asks to have made, and it is
    made here once so that no check has to make it again.

    A member on top of the consolidation one is a breakdown and not a
    statement line -- the statement of changes in equity tags the net assets
    once per component of equity -- so a fact carrying one is left out.

    Where a filing states several of the elements a figure is read from, the
    first of them wins, which is the order :data:`CONCEPTS` lists.
    """
    if "_LOCAL" not in facts.columns:
        facts = facts.withColumn("_LOCAL", _local())

    annual = (filings
              .where(func.col("doc_type").isin(list(ANNUAL_DOCTYPES)))
              .where(func.col("period_end").isNotNull())
              .select("filing", "period_end"))

    dei = dei_of(facts)

    own = (facts
           .join(annual, "filing")
           .join(dei.select("filing", "consolidated"), "filing")
           .where(func.col("value").isNotNull())
           .where(func.col("prd_end") == func.col("period_end"))
           # The basis the filing says it reports on, and no other.
           .where(func.when(func.col("consolidated") == "true",
                            func.col("dimensions").isNull())
                  .otherwise(func.col("dimensions") == NON_CONSOLIDATED)))

    names = [name for group in CONCEPTS.values() for name in group]
    # One value per filing and element. A figure is often tagged twice in one
    # report, in the statements and again in the notes, with one value.
    chosen = (own.where(func.col("_LOCAL").isin(names))
              .groupBy("filing", "edinet_code", "filed_year", "_LOCAL")
              .agg(func.min("value").alias("value")))

    wide = (chosen.groupBy("filing", "edinet_code", "filed_year")
            .pivot("_LOCAL", names)
            .agg(func.first("value")))

    for key, group in CONCEPTS.items():
        column = func.lit(None).cast("double")
        for name in reversed(group):
            if name in wide.columns:
                column = func.coalesce(func.col(f"`{name}`"), column)
        wide = wide.withColumn(key, column)

    return wide.select("filing", "edinet_code", "filed_year", *CONCEPTS)


def _imbalance(figures, difference, scale):
    """The filings whose two sides differ by more than the tolerance.

    A component of the sum being tested has to be stated. Treating an absent
    line as nothing is what turns a filing that presents its statements
    differently into a filing that fails arithmetic: an IFRS statement of
    financial position subtotals its current liabilities and leaves the
    non-current side to be read as the remainder, and a check that read the
    missing total as zero reported Fujitsu's liabilities as short by 244,695
    million. A difference that cannot be computed is not a difference, and the
    null propagates. Only a line that is genuinely optional is coalesced, and
    each of those is named where it is added.
    """
    return _filing_level(
        figures
        .withColumn("_diff", difference)
        .withColumn("_scale", func.abs(scale))
        .where(func.col("_diff").isNotNull() & func.col("_scale").isNotNull())
        .where(func.col("_scale") > 0)
        .where(func.abs(func.col("_diff")) > func.col("_scale") * TOLERANCE))


def _zero(name):
    """An optional line, which a filing that has none of it does not state."""
    return func.coalesce(func.col(name), func.lit(0.0))


def check_imbalanced_balance_sheet(state):
    """Total assets against the liabilities and the net assets."""
    return _imbalance(
        state["figures"],
        func.col("assets") - func.col("liabilities") - func.col("net_assets"),
        func.col("assets"))


def check_imbalanced_totals(state):
    """The two totals the balance sheet states facing each other."""
    return _imbalance(
        state["figures"],
        func.col("assets") - func.col("liabilities_and_net_assets"),
        func.col("assets"))


def check_imbalanced_assets(state):
    """The current and the non-current assets against the total."""
    return _imbalance(
        state["figures"],
        func.col("current_assets") + func.col("noncurrent_assets")
        - func.col("assets"),
        func.col("assets"))


def check_imbalanced_liabilities(state):
    """The current and the non-current liabilities against the total."""
    return _imbalance(
        state["figures"],
        func.col("current_liabilities") + func.col("noncurrent_liabilities")
        - func.col("liabilities"),
        func.col("assets"))


def check_imbalanced_income(state):
    """Revenue less the cost of sales against the gross profit."""
    return _imbalance(
        state["figures"],
        func.col("revenue") - func.col("cost_of_sales")
        - func.col("gross_profit"),
        func.col("revenue"))


def check_imbalanced_ordinary(state):
    """The operating result and the non-operating items against 経常利益.

    The relation has no counterpart under IFRS or US GAAP, neither of which
    has an ordinary result. A filer that reports its group under IFRS states
    none and is not checked; it states one for its parent company, which
    reports under Japan GAAP, and that is a different set of accounts.

    The two non-operating lines are optional: a filer with no income and no
    expense outside its operations states neither, and its operating result
    and its ordinary result are then the same figure.
    """
    return _imbalance(
        state["figures"],
        func.col("operating_income") + _zero("nonoperating_income")
        - _zero("nonoperating_expenses") - func.col("ordinary_income"),
        func.col("revenue"))


def check_imbalanced_pretax(state):
    """The ordinary result and the extraordinary items against the pre-tax result.

    特別損益, the gains and losses on disposals and the impairments, fall
    between the two lines, and a filer that had none in the year states
    neither -- so both are optional and the relation is then that the two
    results are equal.
    """
    return _imbalance(
        state["figures"],
        func.col("ordinary_income") + _zero("extraordinary_income")
        - _zero("extraordinary_loss") - func.col("pretax"),
        func.col("revenue"))


def check_imbalanced_profit(state):
    """The result before tax less the tax against the result for the year.

    The result before tax of an IFRS statement is of the continuing
    operations, the result of the discontinued ones standing beside it
    unadded, so that figure is added back where the filing states one.
    """
    return _imbalance(
        state["figures"],
        func.col("pretax") + _zero("pretax_discontinued")
        - func.col("income_taxes") - func.col("profit"),
        func.col("revenue"))


def check_imbalanced_tax(state):
    """The current and the deferred tax against the tax charge."""
    return _imbalance(
        state["figures"],
        func.col("income_taxes_current") + func.col("income_taxes_deferred")
        - func.col("income_taxes"),
        func.col("revenue"))


def check_imbalanced_profit_split(state):
    """The result for the year against its split between parent and minorities.

    Only a filing that states the split is checked: a filer that prepares no
    consolidated statements has no non-controlling interest to attribute
    anything to, and its result is simply its result.
    """
    return _imbalance(
        state["figures"],
        func.col("profit_parent") + func.col("profit_nci")
        - func.col("profit"),
        func.col("revenue"))


def check_summary_disagrees(state):
    """The five-year summary and the statements on the year they share.

    The front matter of an annual securities report carries 主要な経営指標等
    の推移, a five-year summary of the headline figures, and the statements
    carry the year the report is about. Both therefore state the revenue and
    the total assets of that year, under different elements of different
    taxonomies, and they have to agree.

    This is the only check in this package that tests a filing against
    another part of itself. The other two regimes afford nothing like it: a
    company file of the Commission holds one account of a period per filing,
    and an ESEF report states its summary nowhere.
    """
    figures = state["figures"]
    revenue = _imbalance(
        figures,
        func.col("summary_revenue") - func.col("revenue"),
        func.col("revenue"))
    assets = _imbalance(
        figures,
        func.col("summary_assets") - func.col("assets"),
        func.col("assets"))
    return revenue.unionByName(assets)


def check_negative_assets(state):
    """A balance sheet that totals to less than nothing."""
    return _filing_level(state["figures"].where(func.col("assets") < 0))


def _missing(state, key):
    """The annual reports that state no figure for one of the keys."""
    return _filing_level(state["figures"].where(func.col(key).isNull()))


def check_missing_assets(state):
    """An annual report with no total assets on the basis it reports on."""
    return _missing(state, "assets")


def check_missing_revenue(state):
    """An annual report with no revenue on the basis it reports on."""
    return _missing(state, "revenue")


def check_missing_profit(state):
    """An annual report with no result for the year."""
    return _missing(state, "profit")


def check_missing_consolidated(state):
    """A filing that says it consolidates and states no consolidated figure.

    The document and entity information carries 連結決算の有無 as a flag, and a
    filing whose flag is set states its group's figures without a dimension.
    One that sets the flag and states none has an index, a cover page and a
    set of accounts that do not agree about what it is, and every figure read
    from it on the consolidated basis is missing rather than wrong.
    """
    annual = (state["filings"]
              .where(func.col("doc_type").isin(list(ANNUAL_DOCTYPES)))
              .select("edinet_code", "filed_year", "filing"))
    saying = (state["dei"].where(func.col("consolidated") == "true")
              .select("filing"))
    stating = (state["facts"]
               .where(func.col("dimensions").isNull())
               .where(func.col("value").isNotNull())
               .where(_local().isin(CONCEPTS["assets"] + CONCEPTS["revenue"]))
               .select("filing").distinct())
    return _filing_level(
        annual.join(saying, "filing").join(stating, "filing", "left_anti"))


def check_period_shift_inexact(state):
    """A filer whose two most recent financial years are of different lengths.

    The conversion carries no dates. The current year and the comparative are
    taken from the dates the filing states about itself; everything further
    back is shifted by whole years from them, which is exact only where the
    filer's years are of equal length. The filing states the first and last
    day of both of its two most recent years, so whether that holds is a
    question the filing itself answers, and where it does not every period
    further back than the comparative is out by the drift.

    A leap year moves a year end by a day and is not what this is about; see
    :data:`YEAR_LENGTH_SLACK`. A filer on a 52- or 53-week calendar, or one
    that has moved its year end, differs by a week or more.
    """
    dei = state["dei"]
    lengths = (dei
               .withColumn("_current",
                           func.datediff(func.to_date("current_end"),
                                         func.to_date("current_start")))
               .withColumn("_prior",
                           func.datediff(func.to_date("prior_end"),
                                         func.to_date("prior_start")))
               .where(func.col("_current").isNotNull()
                      & func.col("_prior").isNotNull())
               .where(func.abs(func.col("_current") - func.col("_prior"))
                      > YEAR_LENGTH_SLACK)
               .select("filing"))
    # Only a filing that actually has a shifted period is named: a report
    # stating two years and no more has nothing to be out by.
    shifted = (state["facts"]
               .where(func.col("context").rlike("Prior[2-9]"))
               .where(func.col("prd_end").isNotNull())
               .select("edinet_code", "filed_year", "filing").distinct())
    return _filing_level(shifted.join(lengths, "filing"))


def check_period_unresolved(state):
    """A fact that says something and sits in no period at all.

    The module refuses to invent dates for a context whose shape it does not
    recognise, so both dates are null and the context is carried verbatim
    beside them. A fact in that state can be read and cannot be compared with
    anything, which is worth counting rather than discovering later.
    """
    return (state["facts"]
            .where(func.col("prd_end").isNull())
            .where(func.col("value").isNotNull()
                   | func.col("text").isNotNull())
            .select(*KEYS))


def check_implausible_period(state):
    """A fact whose period ends before it begins."""
    return (state["facts"]
            .where(func.col("prd_start").isNotNull()
                   & (func.col("prd_start") > func.col("prd_end")))
            .select(*KEYS))


def check_index_period_differs(state):
    """The index and the filing disagree about the period reported on.

    The index of the day a filing was made states the period in its own
    fields, and the filing states it among its facts. They are two accounts of
    one thing and the filing's own is the one this module's periods rest on,
    so a difference says the index cannot be trusted for that filing -- which
    matters to anything selecting filings by period without opening them.
    """
    annual = (state["filings"]
              .where(func.col("doc_type").isin(list(ANNUAL_DOCTYPES)))
              .where(func.col("period_end").isNotNull())
              .select("edinet_code", "filed_year", "filing", "period_end"))
    stated = state["dei"].select(
        "filing", func.to_date("current_end").alias("_stated"))
    return _filing_level(
        annual.join(stated, "filing")
        .where(func.col("_stated").isNotNull())
        .where(func.col("period_end") != func.col("_stated")))


def check_missing_fact_value(state):
    """A fact carrying neither a number nor a text."""
    return (state["facts"]
            .where(func.col("value").isNull() & func.col("text").isNull())
            .select(*KEYS))


def check_basis_contradicts_dimension(state):
    """連結・個別 against the dimension of the context.

    The conversion says twice whether a figure is the group's: the column
    連結・個別 says 連結 or 個別, and the context carries
    ``NonConsolidatedMember`` or does not. The two cannot disagree -- a figure
    the conversion calls consolidated cannot sit under the member that says it
    is not -- and over the reports read while this module was written the
    correspondence held for every one of 6,752 facts. 連結・個別 is also
    その他 for a fact that is neither, which constrains nothing.
    """
    facts = state["facts"]
    member = func.array_contains(
        func.split(func.coalesce(func.col("dimensions"), func.lit("")), "_"),
        NON_CONSOLIDATED)
    return (facts
            .where(((func.col("basis") == "連結") & member)
                   | ((func.col("basis") == "個別") & ~member))
            .select(*KEYS))


def check_duplicate_fact_value(state):
    """One figure tagged twice in one filing with two values.

    Twice with one value is ordinary and is not reported: a figure appears in
    the statements and again in the notes, and the number of employees was
    tagged three times under one context with one value in a report read
    while this module was written. Twice with two values is a fault, and which
    of them a reader should use cannot be decided here, so both facts are
    named.
    """
    keys = ["filing", "concept", "context", "unit"]
    filled = state["facts"].where(func.col("value").isNotNull())
    for key in keys:
        filled = filled.withColumn(
            f"_{key}",
            func.coalesce(func.col(key).cast("string"), func.lit("")))
    grouped = [f"_{key}" for key in keys]

    conflicting = (filled.groupBy(*grouped)
                   .agg(func.countDistinct("value").alias("_values"))
                   .where(func.col("_values") > 1)
                   .select(*grouped))
    return filled.join(conflicting, grouped).select(*KEYS)


def check_mixed_unit(state):
    """One concept in two units in one filing."""
    facts = state["facts"]
    mixed = (facts
             .where(func.col("unit").isNotNull())
             .groupBy("filing", "concept")
             .agg(func.countDistinct("unit").alias("_units"))
             .where(func.col("_units") > 1)
             .drop("_units"))
    return facts.join(mixed, ["filing", "concept"]).select(*KEYS)


def check_unit_is_not_a_ratio(state):
    """An element named for a ratio carrying a unit no ratio can have.

    A ratio is a pure number. An element whose name ends in ``Ratio`` and
    whose unit is an amount, a count of shares or an amount per share is
    being used for something other than what it is named for, and a reader
    taking it for the ratio prints a percentage in the hundreds.

    This is not hypothetical. Fujitsu's five-year summary tags
    ``EquityToAssetRatioIFRSSummaryOfBusinessResults`` with 1,167.25 and the
    unit ``JPYPerShares``: the equity attributable to the owners of the parent
    per share, under the name of a ratio, while the ratio itself is in the
    same report under ``RatioOfOwnersEquityToGrossAssetsIFRS...`` as 0.60.
    """
    return (state["facts"]
            .where(func.col("value").isNotNull())
            .where(func.col("unit").isNotNull() & (func.col("unit") != "pure"))
            .where(_local().rlike(r"Ratio([A-Z][A-Za-z]*)?$"))
            .select(*KEYS))


def check_high_extension_share(state):
    """A filing most of whose figures are tagged with concepts of its own.

    Nothing is wrong with such a filing. It says how far its figures can be
    set beside another filer's, which is the thing an extension-heavy report
    makes hard. The four reports read while this module was written run from
    1.7 to 7.1 per cent, so the threshold is far above what an ordinary
    report reaches.
    """
    facts = state["facts"].where(func.col("value").isNotNull())
    taxonomy = func.regexp_replace(func.col("concept"), r":.*", "")
    share = (facts
             .withColumn("_own",
                         func.when(taxonomy.isin(list(TAXONOMY_PREFIXES)),
                                   func.lit(0)).otherwise(func.lit(1)))
             .groupBy("edinet_code", "filed_year", "filing")
             .agg(func.avg("_own").alias("_share"))
             .where(func.col("_share") > EXTENSION_SHARE)
             .select("edinet_code", "filed_year", "filing"))
    return _filing_level(share)


#: Every check of this data set, by the code its rows carry. The order is the
#: order they are run in, which is the order of the catalogue: the ones that
#: say a figure cannot be right first.
CHECKS = (
    ("IMBAL_BALANCE_SHEET", check_imbalanced_balance_sheet),
    ("IMBAL_TOTALS", check_imbalanced_totals),
    ("IMBAL_ASSETS", check_imbalanced_assets),
    ("IMBAL_LIABILITIES", check_imbalanced_liabilities),
    ("IMBAL_INCOME", check_imbalanced_income),
    ("IMBAL_ORDINARY", check_imbalanced_ordinary),
    ("IMBAL_PRETAX", check_imbalanced_pretax),
    ("IMBAL_PROFIT", check_imbalanced_profit),
    ("IMBAL_TAX", check_imbalanced_tax),
    ("IMBAL_PROFIT_SPLIT", check_imbalanced_profit_split),
    ("IMPL_NEGATIVE_ASSETS", check_negative_assets),
    ("IMPL_PERIOD", check_implausible_period),
    ("MISS_FACT_VALUE", check_missing_fact_value),
    ("BASIS_CONTRADICTS_DIMENSION", check_basis_contradicts_dimension),
    ("SUMMARY_DISAGREES", check_summary_disagrees),
    ("PERIOD_SHIFT_INEXACT", check_period_shift_inexact),
    ("PERIOD_UNRESOLVED", check_period_unresolved),
    ("INDEX_PERIOD_DIFFERS", check_index_period_differs),
    ("UNIT_NOT_A_RATIO", check_unit_is_not_a_ratio),
    ("DUPL_FACT_VALUE", check_duplicate_fact_value),
    ("MIXED_UNIT", check_mixed_unit),
    ("MISS_ASSETS", check_missing_assets),
    ("MISS_REVENUE", check_missing_revenue),
    ("MISS_PROFIT", check_missing_profit),
    ("MISS_CONSOLIDATED", check_missing_consolidated),
    ("HIGH_EXTENSION_SHARE", check_high_extension_share),
)


def find_issues(spark, config_path, codes=None):
    """Run every check over the filers asked for, and return the issues.

    The frames the checks need are built once and handed to all of them, so
    that the figures of the annual reports are derived once rather than
    twenty-six times.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    codes: iterable, optional
        The EDINET codes to check. By default every filer the tables hold.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per issue, in the columns of :data:`issues_schema`.
    """
    facts = read_silver(spark, ref_silver_facts(config_path))
    filings = read_silver(spark, ref_silver_filings(config_path))

    if codes:
        wanted = [str(code).strip().upper() for code in codes]
        facts = facts.where(func.col("edinet_code").isin(wanted))
        filings = filings.where(func.col("edinet_code").isin(wanted))

    # The local name is what every check matches on, so it is derived once
    # rather than in each of them.
    facts = facts.withColumn("_LOCAL", _local())

    dei = dei_of(facts).cache()
    state = {
        "facts": facts,
        "filings": filings,
        "dei": dei,
        "figures": annual_figures(facts, filings).cache(),
    }

    found = run_checks(state, CHECKS, KEYS, catalogue_dataset=DATASET)
    return (found
            .withColumn(CHECKED_COLUMN,
                        func.lit(datetime.date.today()).cast("date"))
            .select(*issues_schema.names))


def update_silver_edinet_quality(spark, config_path, codes=None,
                                 file_format=None):
    """Run the checks and rebuild the table of issues.

    Where ``codes`` names part of the population, the issues of the filed
    years it touches are replaced and every other year is carried forward, so
    that a run over one filer does not throw away what is known about the
    rest.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    codes: iterable, optional
        The EDINET codes to check. By default every filer the tables hold.
    file_format: str, optional
        How to store the table; see :func:`fincommons.storage.silver_format`.
    """
    with pipeline_run(
        logger,
        "update_silver_edinet_quality",
        module="edinet",
        layer="silver",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        ref = ref_silver_issues(config_path)
        found = find_issues(spark, config_path, codes).cache()

        existing = None
        if silver_table_exists(spark, ref):
            existing = read_silver(spark, ref)

        if codes and existing is not None:
            rechecked = [row[0] for row in
                         found.select("filed_year").distinct().collect()]
            to_write = merged_issues(found, existing, "filed_year", rechecked)
        else:
            to_write = found

        write_silver(rebalanced(to_write, "filed_year"), ref,
                     file_format=file_format,
                     partition_by=partitioning(to_write, "filed_year"))
        outcome["issues"] = found.count()
        outcome["checks"] = len(CHECKS)

        logger.info("The checks found %s issues", outcome["issues"],
                    extra=context(module="edinet", layer="silver",
                                  table="issues", rows=outcome["issues"]))


#: What this module offers. The checks, and the table of what they found.
__all__ = [
    "ANNUAL_DOCTYPES",
    "CHECKS",
    "CONCEPTS",
    "DATASET",
    "DEI",
    "EXTENSION_SHARE",
    "KEYS",
    "NON_CONSOLIDATED",
    "TAXONOMY_PREFIXES",
    "TOLERANCE",
    "YEAR_LENGTH_SLACK",
    "annual_figures",
    "dei_of",
    "find_issues",
    "issues_schema",
    "update_silver_edinet_quality",
]
