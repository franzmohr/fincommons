"""SEC filings: the checks the figures are held to, and the issues they find.

The Silver tables say what the Commission published, which is what the
registrant tagged. Whether what it tagged adds up is a different question, and
this module answers it in a table of its own: one row per issue found, keyed by
the filing and, where the issue is about one fact, by the fact.

The machinery is :mod:`fincommons.dataquality` -- how a check is described, how
its rows are gathered, what is written where a run looks at part of the table
only. What is here is the checks themselves, because a check knows the data set
it is made of: which concept a balance sheet totals under, which column holds
the registrant, what a financial year of a 52/53-week filer is allowed to look
like.

What is checked, and what is not
--------------------------------

The arithmetic checks are the valuable ones, and they are the relations a
statement states twice: total assets against the liabilities and the equity,
revenue against the cost of revenue and the gross profit, the result before
tax against the tax and the result, the three cash flows against the movement
in cash. Each of those is stated by the filing on both sides, so a difference
is a tagging fault rather than a difference of opinion -- the scale of a number
is an attribute of the tag and invisible on the page, which is the fault these
checks exist to find.

The rest are structural: a period that ends before it begins, a figure tagged
twice with two values, a concept in two units, a line the comparison looked for
and did not find.

One check has no counterpart in :mod:`fincommons.esef.quality`, and it is the
one the company files make possible. A company file carries every filing's
account of every period, so what a report said about a year can be set beside
what a later report says about the same year, and a figure that moved is
visible. The repository behind the ESEF module publishes each filing on its
own, and the same question there would mean reading two filings and deciding
which is which.

Nothing is dropped and nothing is corrected. A check writes a row and leaves
the figure where it is: the tables are what the Commission published, and a
reader who wants to know what is wrong with a figure before using it joins the
issues on.
"""

import datetime
import logging

import pyspark.sql.functions as func
from pyspark.sql.types import (DateType, IntegerType, StringType, StructType)

from ..dataquality import (CHECKED_COLUMN, merged_issues, partitioning,
                           run_checks)
from ..layout import rebalanced
from ..log import context, pipeline_run
from ..storage import read_silver, silver_table_exists, write_silver
from .common import (SOURCE, ref_silver_facts, ref_silver_filings,
                     ref_silver_issues)

logger = logging.getLogger(__name__)

#: The name the check catalogue records these checks under.
DATASET = "secfiling"

issues_schema = StructType() \
    .add("CIK", StringType(), False) \
    .add("FILEDYEAR", IntegerType(), False) \
    .add("FILING", StringType(), False) \
    .add("FACT", StringType(), True) \
    .add("CONCEPT", StringType(), True) \
    .add("ISSUE", StringType(), False) \
    .add("CHECKED", DateType(), False)
"""The shape of the table of issues: one row per issue found.

``FILEDYEAR`` is the partition column, as it is on the table of facts, so that
the issues of a filing sit in the same partition as the facts they are about.

``FACT`` and ``CONCEPT`` name the fact an issue is about, and are empty where
the issue is about the filing as a whole: a balance sheet that does not balance
is a fault of the filing and not of any one of the facts that makes it up, and
pointing at one of them would be picking a culprit the data cannot name.

``ISSUE`` is the code, which
:func:`fincommons.dataquality.describe_check` turns into the sentence that says
what it means. ``CHECKED`` is the day the run was made, not the day the problem
arose: the issues of a partition are replaced whole when it is checked again.

A filing with nothing wrong with it has no row here. The absence of a code is
what says a record passed, and an issue table that recorded passes as well
would be the table of facts over again.
"""

#: The forms that carry a full set of annual statements, amendment or not.
#: The arithmetic checks are run over these, because a quarterly report states
#: a balance sheet of its own but an income statement of the quarter and of the
#: year to date, and which of those the annual relations hold over is a
#: question the data does not answer.
ANNUAL_FORMS = ("10-K", "20-F", "40-F")

#: The length a financial year may have, in days. A registrant on a 52/53-week
#: calendar does not close on the same date every year; see the factsheet
#: template, where the same window is used and for the same reason.
MIN_DAYS = 340
MAX_DAYS = 380

#: A difference this far into the total is a tagging fault rather than the
#: rounding of a figure stated in millions.
TOLERANCE = 0.001

#: A figure a later report restates by more than this is reported. A smaller
#: move is a reclassification of something immaterial, or rounding.
RESTATEMENT_TOLERANCE = 0.01

#: The units a share split restates by arithmetic, which the restatement check
#: therefore passes over. The factsheet template of this module adjusts for a
#: split rather than ignoring it; a quality check has nothing to say about one.
SPLIT_RESTATED_UNITS = ("shares", "USD/shares")

#: The concepts each figure of the checks is read from, best first. They are
#: the concepts of the US GAAP taxonomy, which is all a company file carries:
#: a line the registrant tagged with a concept of its own is not in the data,
#: which is what the MISS_ checks report.
CONCEPTS = {
    "assets": ["us-gaap:Assets"],
    "liabilities": ["us-gaap:Liabilities"],
    "equity": ["us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
               "us-gaap:StockholdersEquity"],
    "equity_and_liabilities": ["us-gaap:LiabilitiesAndStockholdersEquity"],
    # Mezzanine equity, which a US balance sheet presents between the
    # liabilities and the equity rather than inside either. Leaving it out of
    # the balance sheet identity reports a registrant that has any as
    # unbalanced by exactly the amount it holds.
    "temporary_equity": ["us-gaap:TemporaryEquityCarryingAmountAttributableToParent",
                         "us-gaap:TemporaryEquityValueExcludingAdditionalPaidInCapital",
                         "us-gaap:TemporaryEquityCarryingAmountIncludingPortionAttributableToNoncontrollingInterests"],
    "revenue": ["us-gaap:Revenues",
                "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"],
    "cost_of_revenue": ["us-gaap:CostOfRevenue",
                        "us-gaap:CostOfGoodsAndServicesSold"],
    "gross_profit": ["us-gaap:GrossProfit"],
    "pretax": ["us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
               "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
    "income_tax": ["us-gaap:IncomeTaxExpenseBenefit"],
    "profit": ["us-gaap:NetIncomeLoss", "us-gaap:ProfitLoss"],
    # As in the ESEF module: the result of the discontinued operations falls
    # between the income before tax and the net income, and the identity does
    # not hold without it. NVIDIA has none, so its reports passed either way.
    "profit_discontinued": ["us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax",
                            "us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity"],
    "operating_cash_flow": ["us-gaap:NetCashProvidedByUsedInOperatingActivities",
                            "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
    "investing_cash_flow": ["us-gaap:NetCashProvidedByUsedInInvestingActivities"],
    "financing_cash_flow": ["us-gaap:NetCashProvidedByUsedInFinancingActivities"],
    "cash_change": ["us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
                    "us-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"],
}

#: The columns that identify the record an issue is about. Every check returns
#: these, with the two that name a fact left null where the issue is about the
#: filing as a whole.
KEYS = ("CIK", "FILEDYEAR", "FILING", "FACT", "CONCEPT")

def _filing_level(frame):
    """A frame of filing-level issues: the keys, with no fact named."""
    return (frame
            .withColumn("FACT", func.lit(None).cast("string"))
            .withColumn("CONCEPT", func.lit(None).cast("string"))
            .select(*KEYS))


def annual_figures(facts, filings):
    """One row per annual filing, one column per figure the checks need.

    The figures of a filing are the ones it states for its own period: an
    instant on its period end, or a flow over a period of about a year ending
    on it. The comparative figures of earlier years that every report carries
    as well are left aside, so that a check tests one report against itself.

    Where a filing states several of the concepts a figure is read from, the
    first of them wins, which is the order :data:`CONCEPTS` lists.
    """
    annual = (filings
              .where(func.regexp_replace("FORM", "/.*", "").isin(list(ANNUAL_FORMS)))
              .where(func.col("PERIODEND").isNotNull())
              .select("FILING", "PERIODEND"))

    own = (facts
           .join(annual, "FILING")
           .where(func.col("VALUE").isNotNull())
           .where(func.col("PRDEND") == func.col("PERIODEND"))
           .where(func.col("PRDSTART").isNull()
                  | (func.datediff("PRDEND", "PRDSTART").between(MIN_DAYS, MAX_DAYS))))

    # One value per filing and concept. The same fact is often tagged twice in
    # one report, in the statements and again in the notes, with one value.
    chosen = (own.groupBy("FILING", "CIK", "FILEDYEAR", "CONCEPT")
              .agg(func.min("VALUE").alias("VALUE")))

    wide = chosen.groupBy("FILING", "CIK", "FILEDYEAR").pivot(
        "CONCEPT", [c for names in CONCEPTS.values() for c in names]
    ).agg(func.first("VALUE"))

    for key, names in CONCEPTS.items():
        column = func.lit(None).cast("double")
        for name in reversed(names):
            if name in wide.columns:
                column = func.coalesce(func.col(f"`{name}`"), column)
        wide = wide.withColumn(key, column)

    return wide.select("FILING", "CIK", "FILEDYEAR", *CONCEPTS)


def _imbalance(figures, difference, scale):
    """The filings whose two sides differ by more than the tolerance."""
    return _filing_level(
        figures
        .withColumn("_diff", difference)
        .withColumn("_scale", func.abs(scale))
        .where(func.col("_diff").isNotNull() & func.col("_scale").isNotNull())
        .where(func.abs(func.col("_diff")) > func.col("_scale") * TOLERANCE))


def check_imbalanced_balance_sheet(state):
    """Total assets against the liabilities and the equity."""
    figures = state["figures"]
    return _imbalance(
        figures,
        func.col("assets") - func.col("liabilities") - func.col("equity")
        - func.coalesce(func.col("temporary_equity"), func.lit(0.0)),
        func.col("assets"))


def check_imbalanced_totals(state):
    """The two totals the balance sheet states facing each other."""
    figures = state["figures"]
    return _imbalance(
        figures,
        func.col("assets") - func.col("equity_and_liabilities"),
        func.col("assets"))


def check_imbalanced_income(state):
    """Revenue against the cost of revenue and the gross profit."""
    figures = state["figures"]
    return _imbalance(
        figures,
        func.col("revenue") - func.col("cost_of_revenue") - func.col("gross_profit"),
        func.col("revenue"))


def check_imbalanced_profit(state):
    """The result before tax against the tax and the result."""
    figures = state["figures"]
    return _imbalance(
        figures,
        func.col("pretax") - func.col("income_tax")
        + func.coalesce(func.col("profit_discontinued"), func.lit(0.0))
        - func.col("profit"),
        func.col("revenue"))


def check_imbalanced_cashflow(state):
    """The three cash flows against the movement in cash."""
    figures = state["figures"]
    return _imbalance(
        figures,
        func.col("cash_change") - func.col("operating_cash_flow")
        - func.col("investing_cash_flow") - func.col("financing_cash_flow"),
        func.col("assets"))


def check_negative_assets(state):
    """A balance sheet that totals to less than nothing."""
    return _filing_level(
        state["figures"].where(func.col("assets") < 0))


def _missing(state, key):
    """The annual filings that state no figure for one of the keys."""
    return _filing_level(state["figures"].where(func.col(key).isNull()))


def check_missing_assets(state):
    """An annual report with no total assets."""
    return _missing(state, "assets")


def check_missing_revenue(state):
    """An annual report with no revenue."""
    return _missing(state, "revenue")


def check_missing_profit(state):
    """An annual report with no result for the year."""
    return _missing(state, "profit")


def check_implausible_period(state):
    """A fact whose period ends before it begins."""
    return (state["facts"]
            .where(func.col("PRDSTART").isNotNull()
                   & (func.col("PRDSTART") > func.col("PRDEND")))
            .select(*KEYS))


def check_filed_before_periodend(state):
    """A filing made before the period it reports on had ended."""
    return _filing_level(
        state["filings"]
        .where(func.col("PERIODEND").isNotNull() & func.col("FILED").isNotNull())
        .where(func.col("FILED") < func.col("PERIODEND"))
        .select("CIK", "FILEDYEAR", "FILING"))


def check_implausible_fiscal_year(state):
    """A cover-page fiscal year more than a year from the period reported on.

    A registrant whose financial year ends in January names it by the later
    year -- NVIDIA's year to 25 January 2026 is its fiscal 2026 -- so a gap of
    one is ordinary and only a greater one is reported.
    """
    return _filing_level(
        state["filings"]
        .where(func.col("FY").isNotNull() & func.col("PERIODEND").isNotNull())
        .where(func.abs(func.col("FY") - func.year("PERIODEND")) > 1)
        .select("CIK", "FILEDYEAR", "FILING"))


def check_duplicate_fact_value(state):
    """One figure tagged twice in one filing with two values.

    Twice with one value is ordinary: a figure appears in the statements and
    again in the notes, and the table holds both facts. Twice with two values
    is a fault, and which of them a reader should use cannot be decided here,
    so both facts are named.

    The columns a fact is identified by include ones that are empty for an
    instant or for a figure with no breakdown, and an equi-join on an empty
    key matches nothing -- which would have let every balance sheet figure
    past. They are therefore filled before they are grouped and joined on.
    """
    keys = ["FILING", "CONCEPT", "PRDSTART", "PRDEND", "UNIT"]
    filled = state["facts"].where(func.col("VALUE").isNotNull())
    for key in keys:
        filled = filled.withColumn(
            f"_{key}",
            func.coalesce(func.col(key).cast("string"), func.lit("")))
    grouped = [f"_{key}" for key in keys]

    conflicting = (filled.groupBy(*grouped)
                   .agg(func.countDistinct("VALUE").alias("_values"))
                   .where(func.col("_values") > 1)
                   .select(*grouped))
    return filled.join(conflicting, grouped).select(*KEYS)


def check_mixed_unit(state):
    """One concept in two units in one filing."""
    facts = state["facts"]
    mixed = (facts
             .where(func.col("UNIT").isNotNull())
             .groupBy("FILING", "CONCEPT")
             .agg(func.countDistinct("UNIT").alias("_units"))
             .where(func.col("_units") > 1)
             .drop("_units"))
    return facts.join(mixed, ["FILING", "CONCEPT"]).select(*KEYS)


def check_missing_framework(state):
    """A filing that tags against neither reporting taxonomy."""
    return _filing_level(
        state["filings"]
        .where(func.col("FRAMEWORK").isNull())
        .select("CIK", "FILEDYEAR", "FILING"))


def check_restated_materially(state):
    """A figure a later annual report states differently for the same period.

    The company file carries every filing's account of every period, so the
    figure a year's own report stated can be set beside the figure a later
    report states for that same year. Where the two differ by more than a
    hundredth, the fact of the year's own report is named -- it is the figure a
    reader of that report saw, and the one a later reader would be surprised
    by.
    """
    facts, filings = state["facts"], state["filings"]

    annual = (filings
              .where(func.regexp_replace("FORM", "/.*", "").isin(list(ANNUAL_FORMS)))
              .where(func.col("PERIODEND").isNotNull())
              .select(func.col("FILING"), func.col("CIK").alias("_cik"),
                      func.col("PERIODEND"), func.col("FILED").alias("_filed")))

    # The facts of the annual reports, each carrying the day its own filing was
    # made, so that earlier and later can be told apart.
    pool = (facts
            .where(func.col("VALUE").isNotNull())
            .join(annual.select("FILING", "_filed"), "FILING"))

    # The first report of a period is the one whose own period end it is.
    own = (pool.join(annual.select(func.col("FILING").alias("_own_filing"),
                                   func.col("PERIODEND")),
                     pool.FILING == func.col("_own_filing"))
           .where(func.col("PRDEND") == func.col("PERIODEND"))
           .select(*KEYS, "PRDSTART", "PRDEND", "UNIT", "VALUE", "_filed"))

    later = (pool
             .select(func.col("FILING").alias("_later_filing"),
                     func.col("CONCEPT").alias("_concept"),
                     func.col("PRDSTART").alias("_prdstart"),
                     func.col("PRDEND").alias("_prdend"),
                     func.col("UNIT").alias("_unit"),
                     func.col("VALUE").alias("_value"),
                     func.col("_filed").alias("_later_filed")))

    joined = own.join(
        later,
        (own.CONCEPT == func.col("_concept"))
        & (own.PRDEND == func.col("_prdend"))
        & (own.UNIT.eqNullSafe(func.col("_unit")))
        & (own.PRDSTART.eqNullSafe(func.col("_prdstart")))
        & (func.col("_later_filed") > func.col("_filed")))

    return (joined
            .where(func.col("VALUE") != 0)
            # A share split restates every share count and per-share amount by
            # arithmetic rather than by judgement, and a registrant that has
            # split its stock would otherwise fill this check with figures
            # nothing is wrong with. NVIDIA's two splits accounted for 69 of
            # the 218 rows the check found before these units were left out.
            .where(~func.col("UNIT").isin(list(SPLIT_RESTATED_UNITS)))
            .where(func.abs(func.col("_value") - func.col("VALUE"))
                   > func.abs(func.col("VALUE")) * RESTATEMENT_TOLERANCE)
            .select(*KEYS))


#: Every check of this data set, by the code its rows carry. The order is the
#: order they are run in, which is the order of the catalogue: the ones that
#: say a figure cannot be right first.
CHECKS = (
    ("IMBAL_BALANCE_SHEET", check_imbalanced_balance_sheet),
    ("IMBAL_TOTALS", check_imbalanced_totals),
    ("IMBAL_INCOME", check_imbalanced_income),
    ("IMBAL_PROFIT", check_imbalanced_profit),
    ("IMBAL_CASHFLOW", check_imbalanced_cashflow),
    ("IMPL_NEGATIVE_ASSETS", check_negative_assets),
    ("IMPL_PERIOD", check_implausible_period),
    ("IMPL_FILED_BEFORE_PERIODEND", check_filed_before_periodend),
    ("MISS_ASSETS", check_missing_assets),
    ("MISS_REVENUE", check_missing_revenue),
    ("MISS_PROFIT", check_missing_profit),
    ("DUPL_FACT_VALUE", check_duplicate_fact_value),
    ("MIXED_UNIT", check_mixed_unit),
    ("IMPL_FY_PERIODEND", check_implausible_fiscal_year),
    ("RESTATED_MATERIALLY", check_restated_materially),
    ("MISS_FRAMEWORK", check_missing_framework),
)


def find_issues(spark, config_path, ciks=None):
    """Run every check over the registrants asked for, and return the issues.

    The frames the checks need are built once and handed to all of them, so
    that the figures of the annual reports are derived once rather than
    sixteen times.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    ciks: iterable, optional
        The registrants to check. By default every one the tables hold.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per issue, in the columns of :data:`issues_schema`.
    """
    facts = read_silver(spark, ref_silver_facts(config_path))
    filings = read_silver(spark, ref_silver_filings(config_path))

    if ciks:
        wanted = [str(cik).zfill(10) for cik in ciks]
        facts = facts.where(func.col("CIK").isin(wanted))
        filings = filings.where(func.col("CIK").isin(wanted))

    # The table of filings carries no FILEDYEAR of its own, and the issues are
    # partitioned by it, so it is taken from the day the filing was made --
    # which is what the column means on the table of facts.
    filings = filings.withColumn("FILEDYEAR", func.year("FILED"))

    state = {
        "facts": facts,
        "filings": filings,
        "figures": annual_figures(facts, filings).cache(),
    }

    found = run_checks(state, CHECKS, KEYS, catalogue_dataset=DATASET)
    return (found
            .withColumn(CHECKED_COLUMN,
                        func.lit(datetime.date.today()).cast("date"))
            .select(*issues_schema.names))


def update_silver_secfiling_quality(spark, config_path, ciks=None,
                                    file_format=None):
    """Run the checks and rebuild the table of issues.

    Where ``ciks`` names part of the population, the issues of the filed years
    it touches are replaced and every other year is carried forward, so that a
    run over one registrant does not throw away what is known about the rest.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    ciks: iterable, optional
        The registrants to check. By default every one the tables hold.
    file_format: str, optional
        How to store the table; see :func:`fincommons.storage.silver_format`.
    """
    with pipeline_run(
        logger,
        "update_silver_secfiling_quality",
        module="secfiling",
        layer="silver",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        ref = ref_silver_issues(config_path)
        found = find_issues(spark, config_path, ciks).cache()

        existing = None
        if silver_table_exists(spark, ref):
            existing = read_silver(spark, ref)

        if ciks and existing is not None:
            rechecked = [row[0] for row in
                         found.select("FILEDYEAR").distinct().collect()]
            to_write = merged_issues(found, existing, "FILEDYEAR", rechecked)
        else:
            to_write = found

        write_silver(rebalanced(to_write, "FILEDYEAR"), ref,
                     file_format=file_format,
                     partition_by=partitioning(to_write, "FILEDYEAR"))
        outcome["issues"] = found.count()
        outcome["checks"] = len(CHECKS)

        logger.info("The checks found %s issues", outcome["issues"],
                    extra=context(module="secfiling", layer="silver",
                                  table="issues", rows=outcome["issues"]))


#: What this module offers. The checks, and the table of what they found.
__all__ = [
    "ANNUAL_FORMS",
    "CHECKS",
    "CONCEPTS",
    "DATASET",
    "KEYS",
    "MAX_DAYS",
    "MIN_DAYS",
    "RESTATEMENT_TOLERANCE",
    "SPLIT_RESTATED_UNITS",
    "TOLERANCE",
    "annual_figures",
    "find_issues",
    "issues_schema",
    "update_silver_secfiling_quality",
]
