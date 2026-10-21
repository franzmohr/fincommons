"""Market price data: the ECB series in the Silver layer.

The data flows of the ECB carry many series in one table, so the Silver stage
of this source is mostly a matter of picking the one series that is wanted out
of the flow and reducing it to the three columns of the combined table.

The ``FM`` flow carries the widest mixture of the two. It holds a hundred and
more series at five frequencies at once, and the frequency is what decides most
of what can be taken from it: the Silver table is keyed by a date, so a yearly,
quarterly or monthly observation has no reference date until one is chosen for
it.

Three stages read it. :func:`update_silver_ecb_fm` takes the daily key ECB
interest rates, which need no such choice. :func:`update_silver_ecb_fm_monthly_us`
and :func:`update_silver_ecb_fm_monthly_ea` take the US and the euro area series,
which the flow publishes monthly and at no finer grain, and date each observation
to the last day of its month.

Those monthly series are the only ones in the module that are not daily, and they
say so in their identifier: it ends in ``-M``. The table has no column for the
frequency of a series, so a reader who joins them to the daily sources on the
reference date would otherwise find a value on one day a month with nothing to
explain the others. Nothing else in the row carries that warning, which is why
the suffix is not decoration.

The yearly and quarterly frequencies are still left alone. They duplicate what
the monthly series already carry, at a coarser grain.
"""

import pyspark.sql.functions as func

from .common import *

#: The key ECB interest rates taken from the ``FM`` flow, as the code the flow
#: names them by against the identifier they reach the Silver table under.
#:
#: The three of them are the corridor the euro money market is steered within:
#: the deposit facility is the floor, the marginal lending facility the ceiling,
#: and the main refinancing operations rate the one in between. They are named
#: rather than given an ISIN for the same reason the overnight rates are -- they
#: are rates and not instruments -- and they take a prefix of their own because
#: they are policy rates and not overnight market rates.
#:
#: ``MRR_RT`` is the main refinancing rate and not ``MRR_FR`` or ``MRR_MBR``.
#: Those two are the rate under one tender regime each, so each covers only the
#: years its regime ran in: the minimum bid rate stops in October 2008, when the
#: ECB moved to fixed rate full allotment. ``MRR_RT`` is the series that carries
#: both, which is why it is the only one of the three that spans the whole
#: history.
ECB_KEY_RATES = {
    "DFR": "KR-DFR",
    "MRR_RT": "KR-MRO",
    "MLFR": "KR-MLF",
}

#: The monthly US yield curve series taken from the ``FM`` flow, as the code the
#: flow names them by against the identifier they reach the Silver table under.
#:
#: These are the one part of the module that is not daily, and the identifier
#: says so: every one of them ends in ``-M``. The Silver table has no column for
#: the frequency of a series and a reader joining on the reference date would
#: otherwise find a value on one day a month and nothing on the others, with
#: nothing to say why. The suffix is the warning that cannot be joined away.
#:
#: What each of them is differs in a way the identifier cannot carry, so it is
#: worth stating: ``YLD-US10Y-M`` is an average of the daily observations
#: through the month, while ``YLD-US10YZ-M`` is the zero coupon yield at the end
#: of it. They are close but they are not the same measurement, and neither is a
#: month-end quote of the other.
ECB_MONTHLY_US = {
    "US10YT_RR": "YLD-US10Y-M",
    "USD10YZ_R": "YLD-US10YZ-M",
    "US10YT_US1YT": "SPR-US10Y1Y-M",
}


#: The monthly euro area series taken from the ``FM`` flow, as the code the flow
#: names them by against the identifier they reach the Silver table under.
#:
#: They are dated and suffixed exactly as :data:`ECB_MONTHLY_US` is; see
#: :func:`update_silver_ecb_fm_monthly_us` for why.
#:
#: Two of the twenty-five the flow publishes for the euro area are deliberately
#: left out, because the table already carries them at a daily frequency and a
#: monthly copy of a daily series is not additional data: ``UONSTR``, which is
#: the €STR that arrives daily as ``ON-ESTR``, and ``DJES50I``, which is the EURO
#: STOXX 50 that arrives daily from Yahoo Finance under its ISIN.
#:
#: ``EONIA`` is kept although it stopped in December 2021. It was the euro
#: overnight benchmark until €STR replaced it, nothing else in the table carries
#: it, and a supervisory data set still meets contracts that reference it. Its
#: end shows up in the coverage table like any other series that has stopped.
#:
#: The prefixes say what kind of number each is, because the table has no column
#: that does: ``YLD-`` a bond yield, ``IBOR-`` a term interbank rate, ``ON-`` an
#: overnight rate, ``EQ-`` an equity index level. The first three are per cent
#: per annum and the last is index points, which is the sharpest reason for
#: telling them apart by name.
ECB_MONTHLY_EA = {
    # The government benchmark curve, back to 1970.
    "U2_2Y": "YLD-EA2Y-M",
    "U2_3Y": "YLD-EA3Y-M",
    "U2_5Y": "YLD-EA5Y-M",
    "U2_7Y": "YLD-EA7Y-M",
    "U2_10Y": "YLD-EA10Y-M",
    "R_U2_10Y": "YLD-EA10Y-REAL-M",
    # The money market.
    "EURIBOR1MD_": "IBOR-EUR1M-M",
    "EURIBOR3MD_": "IBOR-EUR3M-M",
    "EURIBOR6MD_": "IBOR-EUR6M-M",
    "EURIBOR1YD_": "IBOR-EUR1Y-M",
    "R_EURIBOR3MD_": "IBOR-EUR3M-REAL-M",
    "EONIA": "ON-EONIA-M",
    # The EURO STOXX index and its sectors.
    "DJEURST": "EQ-ESTOXX-M",
    "S1ESBME": "EQ-ESTOXX-BASICMAT-M",
    "S1ESCGE": "EQ-ESTOXX-CONSTPL-M",
    "S1ESCSE": "EQ-ESTOXX-CONSDIS-M",
    "S1ESFNE": "EQ-ESTOXX-FINANC-M",
    "S1ESG1E": "EQ-ESTOXX-TECH-M",
    "S1ESH1E": "EQ-ESTOXX-HEALTH-M",
    "S1ESIDE": "EQ-ESTOXX-INDUS-M",
    "S1ESO1E": "EQ-ESTOXX-ENERGY-M",
    "S1EST1E": "EQ-ESTOXX-TELECOM-M",
    "S1ESU1E": "EQ-ESTOXX-UTIL-M",
}


def update_silver_ecb(spark, config_path):
    """Import various ECB data sets into Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_ecb(spark, "path/to/configuration/file")
    """
    # ESTR
    update_silver_ecb_estr(spark, config_path)

    # The key ECB interest rates out of the FM flow
    update_silver_ecb_fm(spark, config_path)

    # The monthly US yield curve series out of the same flow
    update_silver_ecb_fm_monthly_us(spark, config_path)

    # The monthly euro area series out of the same flow
    update_silver_ecb_fm_monthly_ea(spark, config_path)


def update_silver_ecb_estr(spark, config_path):
    """Import ECB ESTR data into Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_ecb_estr(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_ecb_estr(config_path), "ECB")
    if bronze is not None:

        bronze = bronze \
            .filter((func.col("FREQ") == "B") & \
                    (func.col("BENCHMARK_ITEM") == "EU000A2X2A25") & \
                    (func.col("DATA_TYPE_EST") == "WT")) \
            .withColumn("INSTRID", func.lit("ON-ESTR")) \
            .withColumnRenamed("TIME_PERIOD", "REFDT") \
            .withColumnRenamed("OBS_VALUE", "VALUE") \
            .withColumn("VALUE", func.col("VALUE")) \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)


def update_silver_ecb_fm(spark, config_path):
    """Import the key ECB interest rates of the ``FM`` flow into the Silver layer.

    The flow carries far more than this. What is taken from it is settled by
    three filters, and each of them leaves something out for a reason worth
    stating.

    ``FREQ = 'D'`` keeps the daily series. The flow publishes the same rate at
    five frequencies at once, so without this filter the same identifier would
    be delivered five times over for the same reference date -- and four of them
    could not be given a reference date at all, because a year, a quarter and a
    month are not dates. The daily series carries a value on every calendar day
    rather than only on the days a rate changed, so the step between two
    decisions is held rather than implied.

    ``DATA_TYPE_FM = 'LEV'`` keeps the level. The flow publishes the change in
    percentage points beside it, under the same series name and the same date,
    and the Silver table holds what a rate is and not what it moved by.

    ``PROVIDER_FM_ID`` keeps the three rates of :data:`ECB_KEY_RATES` and leaves
    the other two the flow publishes beside them, which are the main refinancing
    rate under one tender regime each.

    The rates arrive in per cent per annum, which is the unit the other rates in
    the table are in, so nothing is rescaled. ``TIME_PERIOD`` does have to be
    cast: this flow's schema types it as a string, where the ``EST`` schema
    types it as a date.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_ecb_fm(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_ecb_fm(config_path), "ECB")
    if bronze is not None:

        # The flow names the rate in a dimension of its own, so the identifier is
        # looked up from that rather than parsed out of the series key.
        named = func.create_map(
            [func.lit(part) for item in ECB_KEY_RATES.items() for part in item]
        )

        bronze = bronze \
            .filter((func.col("FREQ") == "D") &
                    (func.col("DATA_TYPE_FM") == "LEV") &
                    func.col("PROVIDER_FM_ID").isin(list(ECB_KEY_RATES))) \
            .withColumn("INSTRID", named[func.col("PROVIDER_FM_ID")]) \
            .withColumn("REFDT", func.to_date(func.col("TIME_PERIOD"), "yyyy-MM-dd")) \
            .withColumnRenamed("OBS_VALUE", "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)


def update_silver_ecb_fm_monthly_us(spark, config_path):
    """Import the monthly US yield curve series of the ``FM`` flow.

    These are the first series in the module that are not daily, and taking them
    means answering the question that kept them out until now: what reference
    date a month belongs to.

    **The last day of the month is used.** The flow labels an observation
    ``2026-08`` and the Silver table is keyed by a date, so one has to be chosen.
    The end of the period is the least wrong of the available answers -- it is
    the date by which the observation is complete, so nothing is dated earlier
    than the last day it draws on, and a reader carrying the last quote forward
    from it is never carrying a figure into days it already covers. Dating it to
    the first of the month would do exactly that.

    **The identifier says the series is monthly.** Every one of them ends in
    ``-M``. The Silver table has no column for the frequency of a series, so a
    reader who joins the daily sources to these on the reference date finds a
    value on one day a month and nothing on the others, and nothing in the row
    explains it. The suffix is the one place that warning can live, and it is
    the reason these series can be in the table at all without misleading.

    ``FREQ = 'M'`` and ``PROVIDER_FM_ID`` are the whole of the filter. The three
    series are published at several frequencies, so without the first the same
    identifier arrives more than once for one date; each of them is uniquely
    named by the second.

    The values arrive in per cent per annum, the unit the rates in the table are
    in, so nothing is rescaled -- including the spread, which is a difference of
    two yields and therefore a number of percentage points rather than a price.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_ecb_fm_monthly_us(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_ecb_fm(config_path), "ECB")
    if bronze is not None:

        named = func.create_map(
            [func.lit(part) for item in ECB_MONTHLY_US.items() for part in item]
        )

        bronze = bronze \
            .filter((func.col("FREQ") == "M") &
                    func.col("PROVIDER_FM_ID").isin(list(ECB_MONTHLY_US))) \
            .withColumn("INSTRID", named[func.col("PROVIDER_FM_ID")]) \
            .withColumn("REFDT", func.last_day(
                func.to_date(func.concat_ws("-", func.col("TIME_PERIOD"),
                                            func.lit("01"))))) \
            .withColumnRenamed("OBS_VALUE", "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)


def update_silver_ecb_fm_monthly_ea(spark, config_path):
    """Import the monthly euro area series of the ``FM`` flow.

    The dating and the ``-M`` suffix are the same as for the US series, and for
    the same reasons; see :func:`update_silver_ecb_fm_monthly_us`.

    What differs is the breadth. This block is a whole government benchmark curve
    -- two, three, five, seven and ten years, back to 1970 -- the four EURIBOR
    tenors, the real ten-year yield and the real three-month rate, and the EURO
    STOXX index with ten of its sectors. Which of them is which is carried by the
    prefix of the identifier and nothing else, so :data:`ECB_MONTHLY_EA` is where
    a reader has to look to know whether a number is a yield, a rate or an index
    level -- and the last of those is in points where the others are in per cent
    per annum.

    ``REF_AREA`` is part of the filter here where it was not for the US series.
    Both blocks are named by ``PROVIDER_FM_ID`` and no code is shared between
    them, but the flow publishes other areas under codes of the same shape, and
    the area is what says this stage is about the euro area rather than merely
    about the codes that happen to be listed.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_ecb_fm_monthly_ea(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_ecb_fm(config_path), "ECB")
    if bronze is not None:

        named = func.create_map(
            [func.lit(part) for item in ECB_MONTHLY_EA.items() for part in item]
        )

        bronze = bronze \
            .filter((func.col("FREQ") == "M") &
                    (func.col("REF_AREA") == "U2") &
                    func.col("PROVIDER_FM_ID").isin(list(ECB_MONTHLY_EA))) \
            .withColumn("INSTRID", named[func.col("PROVIDER_FM_ID")]) \
            .withColumn("REFDT", func.last_day(
                func.to_date(func.concat_ws("-", func.col("TIME_PERIOD"),
                                            func.lit("01"))))) \
            .withColumnRenamed("OBS_VALUE", "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)
