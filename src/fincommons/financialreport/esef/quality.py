"""ESEF filings: the checks the figures are held to, and the issues they find.

The Silver tables say what the repository published, which is what the issuer
tagged. Whether what it tagged adds up is a different question, and this module
answers it in a table of its own: one row per issue found, keyed by the filing
and, where the issue is about one fact, by the fact.

The machinery is :mod:`fincommons.dataquality` -- how a check is described, how
its rows are gathered, what is written where a run looks at part of the table
only. What is here is the checks themselves, and they are the counterpart of
:mod:`fincommons.financialreport.sec.quality`: the same relations, read from the concepts
of the IFRS taxonomy rather than of US GAAP, and run over the facts of the
reports this module collects.

What is checked, and what is not
--------------------------------

The arithmetic checks are the valuable ones, and they are the relations a
statement states twice: total assets against the current and the non-current
assets and again against the equity and the liabilities, revenue against the
cost of sales and the gross profit, the result before tax against the tax and
the result, the three cash flows against the movement in cash. Each of those is
stated by the filing on both sides, so a difference is a tagging fault rather
than a difference of opinion -- the scale of a number is an attribute of the
tag and invisible on the page, which is the fault these checks exist to find.

They are run over the undimensioned facts only. A figure broken down by segment
or by class is a part of a total and not a total, and summing the parts beside
the whole is the arithmetic the exclusion exists to prevent.

Two things are checked here that have no counterpart in the SEC module, and
both because the repository publishes something the Commission does not. It
validates every filing and publishes the counts, so what its own rules found
can be carried into the issue table rather than left on its site; and it lists
filings whose xBRL-JSON it could not produce, so a filing that contributes no
facts at all can be counted as what it is. Against that, the restatement check
of the SEC module has no counterpart here: the repository publishes each filing
on its own rather than every filing's account of every period.

Two of the checks read the table of filings alone -- what the repository's own
validation found, and whether it published any facts at all -- and that table
covers the whole index rather than the reports a lake has collected. Their rows
therefore name filings whose facts are not in the lake, which is right: both
questions are answered by the index, and a filing nobody downloaded is not
thereby sound.

Nothing is dropped and nothing is corrected. A check writes a row and leaves
the figure where it is: the tables are what the repository published, and a
reader who wants to know what is wrong with a figure before using it joins the
issues on.
"""

import datetime
import logging

import pyspark.sql.functions as func
from pyspark.sql.types import DateType, StringType, StructType

from ...dataquality import (CHECKED_COLUMN, merged_issues, partitioning,
                           run_checks)
from ...layout import rebalanced
from ...log import context, pipeline_run
from ...storage import read_silver, silver_table_exists, write_silver
from .common import ref_silver_facts, ref_silver_filings, ref_silver_issues

logger = logging.getLogger(__name__)

#: The name the check catalogue records these checks under.
DATASET = "esef"

issues_schema = StructType() \
    .add("country", StringType(), False) \
    .add("lei", StringType(), False) \
    .add("filing", StringType(), False) \
    .add("fact", StringType(), True) \
    .add("concept", StringType(), True) \
    .add("issue", StringType(), False) \
    .add("checked", DateType(), False)
"""The shape of the table of issues: one row per issue found.

``country`` is the partition column, as it is on the table of facts, so that
the issues of a filing sit in the same partition as the facts they are about
and a run can check one country at a time as the pipeline collects one at a
time.

``fact`` and ``concept`` name the fact an issue is about, and are empty where
the issue is about the filing as a whole: a balance sheet that does not balance
is a fault of the filing and not of any one of the facts that makes it up, and
pointing at one of them would be picking a culprit the data cannot name.

``issue`` is the code, which
:func:`fincommons.dataquality.describe_check` turns into the sentence that says
what it means. ``checked`` is the day the run was made, not the day the problem
arose: the issues of a country are replaced whole when it is checked again.

A filing with nothing wrong with it has no row here. The absence of a code is
what says a record passed.
"""

#: The length a financial year may have, in days. A window rather than a fixed
#: 365 because an issuer may change its year end, and because a 52/53-week
#: calendar does not close on the same date every year.
MIN_DAYS = 340
MAX_DAYS = 380

#: A difference this far into the total is a tagging fault rather than the
#: rounding of a figure stated in thousands.
TOLERANCE = 0.001

#: The share of numeric facts under concepts of the issuer's own making above
#: which a report is reported as hard to compare. Nothing is wrong with such a
#: report; the code says how far its figures can be set beside another's.
EXTENSION_SHARE = 0.5

#: The prefixes a filing binds to a taxonomy rather than to a vocabulary of
#: its own. A concept under anything else was created by the issuer for its own
#: report, which it is free to rename from one year to the next. The list is
#: the one the coverage report of this module uses, and a lake holding reports
#: tagged against a taxonomy it does not name would count them as extensions.
TAXONOMY_PREFIXES = ("ifrs-full", "ifrs-smes", "esef_cor", "esef_all", "ifrs",
                     "esma")

#: The concepts each figure of the checks is read from, best first. They are
#: concepts of the IFRS taxonomy only: a line the issuer tagged with a concept
#: of its own is its to name, and a check that followed those would be checking
#: a different thing for every issuer. What that costs is what the MISS_ checks
#: report.
CONCEPTS = {
    "assets": ["ifrs-full:Assets"],
    "current_assets": ["ifrs-full:CurrentAssets"],
    "noncurrent_assets": ["ifrs-full:NoncurrentAssets"],
    # IFRS 5 presents the assets held for sale on a line of their own. Most
    # issuers count that line among the current assets, as the taxonomy does;
    # some present it below them, and their two subtotals then fall short of
    # total assets by exactly that line. The line has two concepts, with and
    # without the assets held for distribution to owners, and both are used.
    "held_for_sale": ["ifrs-full:NoncurrentAssetsOrDisposalGroupsClassifiedAsHeldForSaleOrAsHeldForDistributionToOwners",
                      "ifrs-full:NoncurrentAssetsOrDisposalGroupsClassifiedAsHeldForSale"],
    "liabilities": ["ifrs-full:Liabilities"],
    "equity": ["ifrs-full:Equity"],
    "equity_and_liabilities": ["ifrs-full:EquityAndLiabilities"],
    "revenue": ["ifrs-full:Revenue",
                "ifrs-full:RevenueFromContractsWithCustomers"],
    "cost_of_sales": ["ifrs-full:CostOfSales"],
    "gross_profit": ["ifrs-full:GrossProfit"],
    "pretax": ["ifrs-full:ProfitLossBeforeTax"],
    "income_tax": ["ifrs-full:IncomeTaxExpenseContinuingOperations"],
    "profit": ["ifrs-full:ProfitLoss"],
    # The result of the discontinued operations falls between the result
    # before tax and the result for the year, so the identity does not hold
    # without it. voestalpine's reports for 2021/22 and 2022/23 were reported
    # as faulty until it was read: the gaps were its discontinued operations
    # to the cent.
    "profit_discontinued": ["ifrs-full:ProfitLossFromDiscontinuedOperations"],
    "operating_cash_flow": ["ifrs-full:CashFlowsFromUsedInOperatingActivities"],
    "investing_cash_flow": ["ifrs-full:CashFlowsFromUsedInInvestingActivities"],
    "financing_cash_flow": ["ifrs-full:CashFlowsFromUsedInFinancingActivities"],
    # The three cash flows sum to the movement in cash before the effect of
    # exchange rates on the cash held, which IAS 7 presents on a line of its
    # own. The check reads the movement before it where it is tagged and
    # otherwise takes the effect off the movement after it; read against the
    # movement after it, every issuer holding cash in a second currency was out
    # by its exchange differences -- 59 of the 72 reports the check had found
    # faulty among the Austrian and Dutch reports of the lake it was corrected
    # on. Twenty other reports tag the movement before the effect under the
    # concept for the movement after it, and their three cash flows sum to it
    # exactly; that is a tag put on the wrong line, not figures that
    # contradict one another, so the check accepts either reading.
    "cash_change_before_fx": ["ifrs-full:IncreaseDecreaseInCashAndCashEquivalentsBeforeEffectOfExchangeRateChanges"],
    "cash_change": ["ifrs-full:IncreaseDecreaseInCashAndCashEquivalents"],
    "fx_effect_on_cash": ["ifrs-full:EffectOfExchangeRateChangesOnCashAndCashEquivalents"],
}

#: The columns that identify the record an issue is about. Every check returns
#: these, with the two that name a fact left null where the issue is about the
#: filing as a whole.
KEYS = ("country", "lei", "filing", "fact", "concept")


def is_extension(concept):
    """Whether a concept was created by the issuer, as a Spark expression."""
    prefix = func.substring_index(concept, ":", 1)
    return ~prefix.isin(list(TAXONOMY_PREFIXES))


def _filing_level(frame):
    """A frame of filing-level issues: the keys, with no fact named."""
    return (frame
            .withColumn("fact", func.lit(None).cast("string"))
            .withColumn("concept", func.lit(None).cast("string"))
            .select(*KEYS))


def annual_figures(facts, filings):
    """One row per filing, one column per figure the checks need.

    The figures of a filing are the ones it states for its own period: no
    dimensions, an instant on its period end, or a flow over a period of about
    a year ending on it. The comparative figures of the previous year that
    every report carries as well are left aside, so that a check tests one
    report against itself.
    """
    own = (facts
           .where(func.col("value").isNotNull() & func.col("dimensions").isNull())
           .where(func.col("prd_end") == func.col("period_end"))
           .where(func.col("prd_start").isNull()
                  | (func.datediff("prd_end", "prd_start").between(MIN_DAYS, MAX_DAYS))))

    # The same figure is often tagged twice in one report, in the statements
    # and again in the notes, with one value.
    chosen = (own.groupBy("filing", "country", "lei", "concept")
              .agg(func.min("value").alias("value")))

    names = [name for values in CONCEPTS.values() for name in values]
    wide = (chosen.groupBy("filing", "country", "lei")
            .pivot("concept", names).agg(func.first("value")))

    for key, values in CONCEPTS.items():
        column = func.lit(None).cast("double")
        for name in reversed(values):
            if name in wide.columns:
                column = func.coalesce(func.col(f"`{name}`"), column)
        wide = wide.withColumn(key, column)

    return wide.select("filing", "country", "lei", *CONCEPTS)


def _imbalance(figures, difference, scale):
    """The filings whose two sides differ by more than the tolerance."""
    return _filing_level(
        figures
        .withColumn("_diff", difference)
        .withColumn("_scale", func.abs(scale))
        .where(func.col("_diff").isNotNull() & func.col("_scale").isNotNull())
        .where(func.abs(func.col("_diff")) > func.col("_scale") * TOLERANCE))


def check_imbalanced_balance_sheet(state):
    """Total assets against the equity and the liabilities."""
    return _imbalance(
        state["figures"],
        func.col("assets") - func.col("equity") - func.col("liabilities"),
        func.col("assets"))


def check_imbalanced_assets(state):
    """Total assets against the current and the non-current assets.

    IAS 1 lets an issuer present its assets in order of liquidity instead of
    splitting them, in which case it tags neither subtotal and the check
    passes over the filing rather than reporting it. An issuer that presents
    its assets held for sale below the current assets rather than among them
    is short by that line, and the difference is taken as explained where it
    is that line.
    """
    gap = (func.col("assets") - func.col("current_assets")
           - func.col("noncurrent_assets"))
    explained = func.abs(gap - func.col("held_for_sale")) \
        <= func.abs(func.col("assets")) * TOLERANCE
    return _imbalance(
        state["figures"],
        func.when(explained, func.lit(0.0)).otherwise(gap),
        func.col("assets"))


def check_imbalanced_income(state):
    """Revenue against the cost of sales and the gross profit."""
    return _imbalance(
        state["figures"],
        func.col("revenue") - func.col("cost_of_sales")
        - func.col("gross_profit"),
        func.col("revenue"))


def check_imbalanced_profit(state):
    """The result before tax against the tax and the result."""
    return _imbalance(
        state["figures"],
        func.col("pretax") - func.col("income_tax")
        + func.coalesce(func.col("profit_discontinued"), func.lit(0.0))
        - func.col("profit"),
        func.col("revenue"))


def cash_change_before_fx():
    """The movement in cash the three cash flows sum to.

    The movement before the effect of exchange rates where the report tags
    it, and otherwise the movement after it less that effect, which is
    nothing where the report states none.
    """
    return func.coalesce(
        func.col("cash_change_before_fx"),
        func.col("cash_change")
        - func.coalesce(func.col("fx_effect_on_cash"), func.lit(0.0)))


def _smaller(a, b):
    """Whichever of two differences is nearer nothing, and the one that is
    known where only one is."""
    return (func.when(a.isNull(), b).when(b.isNull(), a)
            .when(func.abs(a) <= func.abs(b), a).otherwise(b))


def check_imbalanced_cashflow(state):
    """The three cash flows against the movement in cash.

    The movement is read before the effect of exchange rates, and, where the
    report tags the movement after it, also as the report tags it, since a
    report that puts the movement before the effect under the concept for the
    one after it has tagged a line wrongly but has figures that agree. The
    report is out where neither reading agrees with the three cash flows.
    """
    flows = (func.col("operating_cash_flow") + func.col("investing_cash_flow")
             + func.col("financing_cash_flow"))
    return _imbalance(
        state["figures"],
        _smaller(cash_change_before_fx() - flows,
                 func.col("cash_change") - flows),
        func.col("assets"))


def check_negative_assets(state):
    """A statement of financial position that totals to less than nothing."""
    return _filing_level(state["figures"].where(func.col("assets") < 0))


def _missing(state, key):
    """The filings that state no figure for one of the keys."""
    return _filing_level(state["figures"].where(func.col(key).isNull()))


def check_missing_assets(state):
    """A report with no total assets."""
    return _missing(state, "assets")


def check_missing_revenue(state):
    """A report with no revenue."""
    return _missing(state, "revenue")


def check_missing_profit(state):
    """A report with no result for the year."""
    return _missing(state, "profit")


def check_implausible_period(state):
    """A fact whose period ends before it begins."""
    return (state["facts"]
            .where(func.col("prd_start").isNotNull()
                   & (func.col("prd_start") > func.col("prd_end")))
            .select(*KEYS))


def check_missing_fact_value(state):
    """A fact carrying neither a number nor a text, which says nothing."""
    return (state["facts"]
            .where(func.col("value").isNull() & func.col("text").isNull())
            .select(*KEYS))


def check_period_after_processing(state):
    """A fact reporting on a period the repository had not yet seen end."""
    return (state["facts"]
            .join(state["filings"].select("filing", "processed"), "filing")
            .where(func.col("processed").isNotNull())
            .where(func.col("prd_end") > func.to_date("processed"))
            .select(*KEYS))


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
    keys = ["filing", "concept", "prd_start", "prd_end", "unit",
            "dimensions"]
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


def check_validation_errors(state):
    """A filing the repository's own validation found errors in.

    The count is the repository's and nothing here re-ran its rules. An error
    there does not say a figure is wrong, but it does say the filing does not
    meet the format, which is worth carrying into the table rather than
    leaving on a web page.
    """
    return _filing_level(
        state["filings"]
        .where(func.coalesce(func.col("errors"), func.lit(0)) > 0)
        .select("country", "lei", "filing"))


def check_no_facts(state):
    """A filing the index lists and the repository published no facts for.

    The test is that the repository published no xBRL-JSON for the filing, and
    deliberately not that the table of facts holds none. The table of filings
    covers the whole index, every country and year, while the facts cover the
    reports a lake has actually collected -- so asking the facts would report
    every filing nobody downloaded as one the repository could not convert. On
    one index of 24,180 filings, 192 were the latter and 24,174 would have
    been reported.
    """
    return _filing_level(
        state["filings"].where(func.col("json_url").isNull())
        .select("country", "lei", "filing"))


def check_duplicate_filing_period(state):
    """The same issuer, period and language filed more than once, with facts.

    An issuer filing in two languages is two filings of the same figures and
    is ordinary; two filings in one language for one period is a correction,
    and a reader taking either without looking would be taking one of two.
    """
    filings = state["filings"]
    with_facts = (filings
                  .join(state["facts"].select("filing").distinct(), "filing"))
    repeated = (with_facts
                .groupBy("lei", "period_end",
                         func.coalesce(func.col("lang"), func.lit("")).alias("_lang"))
                .agg(func.countDistinct("filing").alias("_filings"))
                .where(func.col("_filings") > 1)
                .select("lei", "period_end", "_lang"))
    return _filing_level(
        with_facts
        .withColumn("_lang", func.coalesce(func.col("lang"), func.lit("")))
        .join(repeated, ["lei", "period_end", "_lang"])
        .select("country", "lei", "filing"))


def check_high_extension_share(state):
    """A report whose figures mostly rest on concepts of the issuer's own."""
    shares = (state["facts"]
              .where(func.col("value").isNotNull())
              .groupBy("country", "lei", "filing")
              .agg(func.avg(is_extension(func.col("concept")).cast("double"))
                   .alias("_share"))
              .where(func.col("_share") > EXTENSION_SHARE))
    return _filing_level(shares.select("country", "lei", "filing"))


#: Every check of this data set, by the code its rows carry. The order is the
#: order of the catalogue: the ones that say a figure cannot be right first.
CHECKS = (
    ("IMBAL_BALANCE_SHEET", check_imbalanced_balance_sheet),
    ("IMBAL_ASSETS", check_imbalanced_assets),
    ("IMBAL_INCOME", check_imbalanced_income),
    ("IMBAL_PROFIT", check_imbalanced_profit),
    ("IMBAL_CASHFLOW", check_imbalanced_cashflow),
    ("IMPL_NEGATIVE_ASSETS", check_negative_assets),
    ("IMPL_PERIOD", check_implausible_period),
    ("MISS_FACT_VALUE", check_missing_fact_value),
    ("MISS_ASSETS", check_missing_assets),
    ("MISS_REVENUE", check_missing_revenue),
    ("MISS_PROFIT", check_missing_profit),
    ("DUPL_FACT_VALUE", check_duplicate_fact_value),
    ("MIXED_UNIT", check_mixed_unit),
    ("IMPL_PRDEND_FUTURE", check_period_after_processing),
    ("VALIDATION_ERRORS", check_validation_errors),
    ("DUPL_FILING_PERIOD", check_duplicate_filing_period),
    ("NO_FACTS", check_no_facts),
    ("HIGH_EXTENSION_SHARE", check_high_extension_share),
)


def find_issues(spark, config_path, countries=None):
    """Run every check over the countries asked for, and return the issues.

    The frames the checks need are built once and handed to all of them, so
    that the figures of the reports are derived once rather than eighteen
    times.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    countries: iterable of str, optional
        The countries to check. By default every one the tables hold.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per issue, in the columns of :data:`issues_schema`.
    """
    facts = read_silver(spark, ref_silver_facts(config_path))
    filings = read_silver(spark, ref_silver_filings(config_path))

    if countries:
        wanted = [str(country).upper() for country in countries]
        facts = facts.where(func.col("country").isin(wanted))
        filings = filings.where(func.col("country").isin(wanted))

    # The facts carry their filing's period end, which is what lets a check
    # tell a figure of the year from a comparative without a join.
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


def update_silver_esef_quality(spark, config_path, countries=None,
                               file_format=None):
    """Run the checks and rebuild the table of issues.

    Where ``countries`` names part of the population, the issues of those
    countries are replaced and every other country is carried forward, so that
    a run over one country does not throw away what is known about the rest --
    which is how the pipeline of this module collects, one country at a time.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    countries: iterable of str, optional
        The countries to check. By default every one the tables hold.
    file_format: str, optional
        How to store the table; see :func:`fincommons.storage.silver_format`.
    """
    with pipeline_run(
        logger,
        "update_silver_esef_quality",
        module="esef",
        layer="silver",
        config_path=config_path,
    ) as outcome:
        ref = ref_silver_issues(config_path)
        found = find_issues(spark, config_path, countries).cache()

        existing = None
        if silver_table_exists(spark, ref):
            existing = read_silver(spark, ref)

        if countries and existing is not None:
            rechecked = [str(country).upper() for country in countries]
            to_write = merged_issues(found, existing, "country", rechecked)
        else:
            to_write = found

        write_silver(rebalanced(to_write, "country"), ref,
                     file_format=file_format,
                     partition_by=partitioning(to_write, "country"))
        outcome["issues"] = found.count()
        outcome["checks"] = len(CHECKS)

        logger.info("The checks found %s issues", outcome["issues"],
                    extra=context(module="esef", layer="silver",
                                  table="issues", rows=outcome["issues"]))


#: What this module offers. The checks, and the table of what they found.
__all__ = [
    "CHECKS",
    "CONCEPTS",
    "DATASET",
    "EXTENSION_SHARE",
    "KEYS",
    "MAX_DAYS",
    "MIN_DAYS",
    "TAXONOMY_PREFIXES",
    "TOLERANCE",
    "annual_figures",
    "find_issues",
    "is_extension",
    "issues_schema",
    "update_silver_esef_quality",
]
