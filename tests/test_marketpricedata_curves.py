"""The zero curves the market price table implies.

The first nine tests came with the module from the analysis the curves were
first built for, and are unchanged apart from their imports. The last two pin
what the move made public without a test of its own: the parallel shift and
the forward rate.
"""

from __future__ import annotations

import datetime
import math

import pytest
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from fincommons.marketpricedata import curves


PRICES = StructType([
    StructField("REFDT", DateType()),
    StructField("INSTRID", StringType()),
    StructField("VALUE", DoubleType()),
])

REFDT = "2025-06-30"


def test_a_tenor_is_measured_on_the_same_day_count_as_a_maturity(spark):
    assert curves.tenor_in_years("ON") == pytest.approx(1 / 360)
    assert curves.tenor_in_years("3M") == pytest.approx(90 / 360)
    assert curves.tenor_in_years("1Y") == pytest.approx(365 / 360)


def test_a_tenor_that_cannot_be_read_says_so(spark):
    with pytest.raises(ValueError, match="1Q"):
        curves.tenor_in_years("1Q")


def test_a_money_market_quote_is_read_as_simple_and_a_swap_quote_as_compounded(spark):
    short = curves.discount_of(0.04, 0.5)
    assert short == pytest.approx(1 / (1 + 0.04 * 0.5))
    long = curves.discount_of(0.04, 5.0)
    assert long == pytest.approx(1.04 ** -5)


def test_one_pillar_gives_a_flat_curve(spark):
    """The usual case at present: only the overnight rate is delivered.

    The single quote is the only information there is about the term structure,
    and carrying its forward rate across the whole curve is the right reading
    of it. It also means the curve is defined beyond the last pillar, which a
    payment due in a year against an overnight quote depends on.
    """
    segments = curves.build_segments({"EUR": {1 / 360: 0.03}})
    assert [segment["T_TO"] for segment in segments] == [1 / 360, float("inf")]
    forwards = {round(segment["FWD"], 12) for segment in segments}
    assert len(forwards) == 1


def test_the_curve_reproduces_the_discount_factor_of_its_own_pillars(spark):
    """The pillars are where the curve is pinned; interpolation is between."""
    pillars = {"EUR": {0.5: 0.04, 5.0: 0.03}}
    segments = curves.segment_frame(spark, curves.build_segments(pillars))

    at_pillars = spark.createDataFrame(
        [("EUR", 0.5), ("EUR", 5.0)], "PMTCCY string, YEARS double")
    got = {row.YEARS: row.DF for row in
           curves.add_discount_factor(at_pillars, segments, "PMTCCY", "YEARS", "DF").collect()}

    assert got[0.5] == pytest.approx(curves.discount_of(0.04, 0.5))
    assert got[5.0] == pytest.approx(curves.discount_of(0.03, 5.0))


def test_the_forward_rate_is_constant_between_two_pillars(spark):
    """Which is what log-linear interpolation in the discount factors means."""
    segments = curves.build_segments({"EUR": {1.0: 0.04, 3.0: 0.04}})
    middle = next(s for s in segments if s["T_FROM"] == 1.0 and s["T_TO"] == 3.0)
    df_at = lambda t: math.exp(middle["LOGDF_FROM"] - middle["FWD"] * (t - middle["T_FROM"]))
    first_half = math.log(df_at(1.5) / df_at(1.0))
    second_half = math.log(df_at(2.5) / df_at(2.0))
    assert first_half == pytest.approx(second_half)


def test_a_payment_in_a_currency_without_a_curve_keeps_its_row(spark):
    """So that it can be reported as uncovered rather than silently omitted."""
    segments = curves.segment_frame(spark, curves.build_segments({"EUR": {1.0: 0.04}}))
    payments = spark.createDataFrame(
        [("EUR", 1.0), ("HUF", 1.0)], "PMTCCY string, YEARS double")
    got = {row.PMTCCY: row.DF for row in
           curves.add_discount_factor(payments, segments, "PMTCCY", "YEARS", "DF").collect()}
    assert got["EUR"] is not None
    assert got["HUF"] is None


def test_the_overnight_rate_of_a_currency_is_picked_out_of_the_price_table(spark):
    prices = spark.createDataFrame([
        (datetime.date(2025, 6, 30), "ON-ESTR", 2.0),
        (datetime.date(2025, 6, 30), "OIS-ESTR-1Y", 2.5),
        (datetime.date(2025, 6, 30), "EUR/USD", 1.17),
        (datetime.date(2025, 6, 30), "IE00B4L5Y983", 120.0),
    ], schema = PRICES)

    pillars = curves.read_pillars(spark, prices, REFDT, ["EUR"])

    # In decimal fractions, not percent: the sources publish percentages and
    # this is the one place the two conventions meet.
    assert pillars["EUR"][curves.tenor_in_years("ON")] == pytest.approx(0.02)
    assert pillars["EUR"][curves.tenor_in_years("1Y")] == pytest.approx(0.025)
    assert set(pillars) == {"EUR"}


def test_the_most_recent_quote_before_the_date_is_used(spark):
    """The table holds nothing for a day a source does not publish on."""
    prices = spark.createDataFrame([
        (datetime.date(2025, 6, 27), "ON-ESTR", 2.0),
        (datetime.date(2025, 7, 15), "ON-ESTR", 9.0),
    ], schema = PRICES)
    pillars = curves.read_pillars(spark, prices, REFDT, ["EUR"])
    assert pillars["EUR"][curves.tenor_in_years("ON")] == pytest.approx(0.02)


def test_a_parallel_shift_multiplies_every_discount_factor_by_exp_minus_d_t(spark):
    """Which is what shifting the continuously compounded zero curve means,
    on both sides of a pillar and beyond the last one."""
    segments = curves.segment_frame(
        spark, curves.build_segments({"EUR": {0.5: 0.04, 5.0: 0.03}}))
    shifts = spark.createDataFrame(
        [("EUR", 0.0, 0.0), ("EUR", 1.0, 0.01)], "CCY string, SHOCK double, SHIFT double")
    shifted = curves.parallel_shift(segments, shifts)

    maturities = [0.25, 2.0, 10.0]
    payments = spark.createDataFrame(
        [("EUR", shock, t) for shock in (0.0, 1.0) for t in maturities],
        "PMTCCY string, SHOCK double, YEARS double")
    got = {(row.SHOCK, row.YEARS): row.DF for row in curves.add_discount_factor(
        payments, shifted, "PMTCCY", "YEARS", "DF", keys = ["SHOCK"]).collect()}

    for t in maturities:
        assert got[(1.0, t)] == pytest.approx(got[(0.0, t)] * math.exp(-0.01 * t))


def test_the_forward_rate_of_a_period_follows_from_its_two_discount_factors(spark):
    frame = spark.createDataFrame(
        [(0.99, 0.97, 0.5), (0.99, 0.97, 0.0)], "DF0 double, DF1 double, TAU double")
    got = [row.FWD for row in
           curves.forward_rate(frame, "DF0", "DF1", "TAU", "FWD").collect()]
    assert got[0] == pytest.approx((0.99 / 0.97 - 1) / 0.5)
    # A period of no length has no forward rate rather than a division by zero.
    assert got[1] is None
