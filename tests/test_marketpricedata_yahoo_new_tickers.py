"""Yahoo Finance tickers added to a Bronze layer that already holds a history.

A run carries on from the partitions of its layer, which every ticker in it
shares, so a ticker added to the mapping table after the first run has to be
asked for its own history or it is only ever asked for the last fortnight.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import fincommons.marketpricedata as mp
from fincommons.marketpricedata import bronze_yahoo


class FakeYahoo:
    """``yfinance.download``, answering with one close a day for every ticker."""

    def __init__(self):
        self.calls = []

    def download(self, tickers, start, end, auto_adjust, actions, progress):
        self.calls.append({"tickers": list(tickers), "start": start})
        days = pd.bdate_range(start, end, inclusive="left")
        columns = pd.MultiIndex.from_product([["Close"], tickers])
        return pd.DataFrame(10.0, index=days, columns=columns)


@pytest.fixture
def data_lake(tmp_path):
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    mp.create_directory_tree(str(config_path))
    return str(config_path)


@pytest.fixture
def yahoo(monkeypatch):
    fake = FakeYahoo()
    monkeypatch.setattr(bronze_yahoo, "_yfinance", lambda: fake)
    monkeypatch.setattr(bronze_yahoo, "YAHOO_BATCH_PAUSE", 0)
    return fake


def mapping(*pairs):
    return pd.DataFrame(pairs, columns=["ISIN", "yahoo"])


def run_on(monkeypatch, day, spark, table, data_lake):
    class Today(date):
        @classmethod
        def today(cls):
            return day
    monkeypatch.setattr(bronze_yahoo, "date", Today)
    bronze_yahoo.download_yahoo_prices(
        spark, table, "ISIN", mp.path_bronze_yahoo_marketpricedata(data_lake),
        "2019-01-01")


def test_the_tickers_are_asked_for_in_batches(spark, data_lake, yahoo, monkeypatch):
    monkeypatch.setattr(bronze_yahoo, "YAHOO_BATCH_SIZE", 2)
    table = mapping(("AT0000743059", "OMV.VI"), ("AT0000746409", "VER.VI"),
                    ("NL0010273215", "ASML.AS"))

    run_on(monkeypatch, date(2026, 9, 1), spark, table, data_lake)

    assert [len(c["tickers"]) for c in yahoo.calls] == [2, 1]
    assert {c["start"] for c in yahoo.calls} == {"2019-01-01"}


def test_a_ticker_added_later_is_asked_for_its_own_history(spark, data_lake, yahoo,
                                                           monkeypatch):
    first = mapping(("AT0000743059", "OMV.VI"))
    run_on(monkeypatch, date(2026, 9, 1), spark, first, data_lake)
    yahoo.calls.clear()

    extended = mapping(("AT0000743059", "OMV.VI"), ("NL0010273215", "ASML.AS"))
    run_on(monkeypatch, date(2026, 9, 8), spark, extended, data_lake)

    starts = {t: c["start"] for c in yahoo.calls for t in c["tickers"]}
    # The ticker already held carries on a fortnight before the last run ...
    assert starts["OMV.VI"] == "2026-08-18"
    # ... and the new one is asked for from the date new tickers start at.
    assert starts["ASML.AS"] == bronze_yahoo.YAHOO_NEW_TICKER_FIRST_DATE

    held = spark.read.parquet(mp.path_bronze_yahoo_marketpricedata(data_lake)) \
        .filter("ISIN = 'NL0010273215'").agg({"REFDT": "min"}).collect()[0][0]
    assert str(held) == "2020-01-01"


def test_a_ticker_added_on_the_day_of_the_run_is_not_left_for_tomorrow(
        spark, data_lake, yahoo, monkeypatch):
    run_on(monkeypatch, date(2026, 9, 8), spark,
           mapping(("AT0000743059", "OMV.VI")), data_lake)
    yahoo.calls.clear()

    # The layer is current for OMV, so only the new ticker is asked for.
    run_on(monkeypatch, date(2026, 9, 8), spark,
           mapping(("AT0000743059", "OMV.VI"), ("NL0010273215", "ASML.AS")), data_lake)
    assert [c["tickers"] for c in yahoo.calls] == [["ASML.AS"]]

    # And with nothing new, nothing is asked at all.
    yahoo.calls.clear()
    run_on(monkeypatch, date(2026, 9, 8), spark,
           mapping(("AT0000743059", "OMV.VI"), ("NL0010273215", "ASML.AS")), data_lake)
    assert yahoo.calls == []


def test_a_row_can_ask_for_more_history_than_the_default():
    table = pd.DataFrame({
        "ISIN": ["NL0010273215", "VN000000VNM8"],
        "yahoo": ["ASML.AS", "VNM.VN"],
        "first_date": [float("nan"), "2010-01-01"],
    })

    starts = bronze_yahoo.new_ticker_starts(table, "2000-01-01", "2026-09-01")

    assert starts == {"ASML.AS": bronze_yahoo.YAHOO_NEW_TICKER_FIRST_DATE,
                      "VNM.VN": "2010-01-01"}
    # Never before the lake's own first date.
    assert bronze_yahoo.new_ticker_starts(table, "2015-01-01", "2026-09-01")["VNM.VN"] \
        == "2015-01-01"
