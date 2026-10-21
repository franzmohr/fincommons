"""The Dhaka Stock Exchange source of the market price data.

The notices and prices below are the exchange's own, cut down to the rows that
matter: City Bank's 12.5% cash and 12.5% stock dividend for 2024, which went
ex on the 24th of May 2025, and Berger Paints' 1-for-17 rights issue of June
2025. The first half of the file needs no Spark; the second runs the Bronze and
Silver stages against a fake of the exchange's JSON service.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import fincommons.marketpricedata as mp
from fincommons.marketpricedata import bronze_dse, dse_events


def notice(id_, code, filed, summary, body):
    return {"ID": id_, "CODE": code, "FILEDAT": filed, "SUMMARY": summary,
            "BODY": body}


CITYBANK_NEWS = [
    notice("psi-1", "CITYBANK", "2025-04-30", "CITYBANK: Dividend Declaration",
           "The Board of Directors has recommended 12.50% Cash and 12.50% Stock "
           "Dividend for the year ended December 31, 2024. Date of AGM: "
           "19.06.2025, Time: 11:00 AM, Venue: Digital Platform."),
    notice("psi-2", "CITYBANK", "2025-04-30", "CITYBANK: Dividend Declaration",
           "(cont. news of CITYBANK): Record Date: 22.05.2025. The Company has "
           "also reported EPS of Tk. 4.36."),
    notice("psi-3", "CITYBANK", "2025-05-21", "CITYBANK: Suspension for Record Date",
           "Trading of the shares of the company will remain suspended on record "
           "date i.e., 22.05.2025."),
    notice("psi-4", "CITYBANK", "2025-05-22", "CITYBANK: Resumption after Record Date",
           "Trading of the shares of the company will resume on 24.05.2025."),
    notice("psi-5", "CITYBANK", "2025-06-24", "CITYBANK: Dividend Disbursement",
           "The company has credited the Bonus Shares and disbursed the Cash "
           "Dividend for the year ended December 31, 2024."),
]


def prices(code, rows):
    return pd.DataFrame([{"REFDT": d, "CODE": code, "CLOSE": c, "TRADES": t}
                         for d, c, t in rows])


CITYBANK_PRICES = prices("CITYBANK", [
    ("2025-05-20", 23.0, 429),
    ("2025-05-21", 22.8, 814),
    ("2025-05-22", 22.8, 0),      # the record date: suspended, close carried
    ("2025-05-24", 19.0, 1166),
    ("2025-05-25", 18.9, 519),
])


# Reading the notices
# --------------------------------------------------------------------------


@pytest.mark.parametrize("text, expected", [
    ("record date i.e., 29.06.2025", date(2025, 6, 29)),
    ("Record Date: 17-May-2026.", date(2026, 5, 17)),
    ("will resume on 07-Sep-2026.", date(2026, 9, 7)),
    ("on 5/1/25", date(2025, 1, 5)),
    ("Record Date: May 06, 2026. The Company", date(2026, 5, 6)),
    ("1 Rights Share against 17 existing shares", None),
])
def test_the_dates_are_read_however_the_exchange_writes_them(text, expected):
    assert dse_events.parse_dse_date(text) == expected


def test_a_record_date_in_a_continuation_belongs_to_its_declaration():
    notices = dse_events.join_continuations(pd.DataFrame(CITYBANK_NEWS))
    declared = dse_events.dividend_declarations(notices)

    assert declared[["CODE", "RECORD", "STOCK_PCT", "CASH_PCT"]] \
        .to_dict("records") == [{"CODE": "CITYBANK", "RECORD": date(2025, 5, 22),
                                 "STOCK_PCT": 12.5, "CASH_PCT": 12.5}]


def test_a_later_notice_for_the_same_record_date_replaces_the_first():
    news = pd.DataFrame([
        notice("psi-1", "DHAKABANK", "2025-04-28", "DHAKABANK: Dividend Declaration",
               "The Board has recommended 7.50% Cash and 7.50% Stock Dividend. "
               "Record Date: 20.07.2025."),
        notice("psi-2", "DHAKABANK", "2025-06-30",
               "DHAKABANK: BSEC's consent for issuance of 5% Stock Dividend",
               "BSEC has accorded its consent for declaring 5% Stock Dividend. "
               "The Record Date for the entitlement of 5% Stock Dividend will be "
               "20.07.2025."),
    ])
    declared = dse_events.dividend_declarations(dse_events.join_continuations(news))

    assert declared["STOCK_PCT"].tolist() == [5.0]


def test_a_dividend_written_word_first_is_read_as_well():
    # Eastern Bank's declaration for 2025, which the first reader missed.
    news = pd.DataFrame([notice(
        "psi-1", "EBL", "2026-04-16", "EBL: Dividend Declaration",
        "The Board of Directors has recommended Cash Dividend @ 25.00% and Stock "
        "Dividend @ 3.00% for the year ended December 31, 2025. Date of AGM: "
        "June 11, 2026. Record Date: May 06, 2026.")])
    declared = dse_events.dividend_declarations(dse_events.join_continuations(news))

    assert declared[["RECORD", "STOCK_PCT", "CASH_PCT"]].to_dict("records") ==         [{"RECORD": date(2026, 5, 6), "STOCK_PCT": 3.0, "CASH_PCT": 25.0}]


# The events
# --------------------------------------------------------------------------


def test_a_bonus_issue_and_a_cash_dividend_go_ex_on_the_day_trading_resumes():
    events = mp.corporate_actions(pd.DataFrame(CITYBANK_NEWS), CITYBANK_PRICES)

    assert events["EXDATE"].unique().tolist() == [date(2025, 5, 24)]
    assert events["EXDATE_FROM"].unique().tolist() == ["notice"]
    factors = dict(zip(events["KIND"], events["FACTOR"]))
    assert factors["bonus"] == pytest.approx(1 / 1.125)
    # 12.5% of the ten taka face value against the close of 22.80 before it.
    assert factors["cash"] == pytest.approx(1 - 1.25 / 22.8)
    # The two together take the close of 22.80 to 19.16; it traded at 19.00.
    assert 22.8 * factors["bonus"] * factors["cash"] == pytest.approx(19.16, abs=0.01)


def test_without_a_resumption_notice_the_ex_date_is_read_off_the_prices():
    news = pd.DataFrame([n for n in CITYBANK_NEWS if "Resumption" not in n["SUMMARY"]])
    events = mp.corporate_actions(news, CITYBANK_PRICES)

    assert events["EXDATE"].unique().tolist() == [date(2025, 5, 24)]
    assert events["EXDATE_FROM"].unique().tolist() == ["prices"]


def test_an_event_whose_share_has_not_traded_ex_yet_is_pending():
    before = CITYBANK_PRICES[CITYBANK_PRICES["REFDT"] <= "2025-05-22"]
    news = pd.DataFrame([n for n in CITYBANK_NEWS if "Resumption" not in n["SUMMARY"]])

    assert mp.corporate_actions(news, before).empty


BERGER_NEWS = [
    notice("psi-10", "BERGERPBL", "2025-05-28",
           "BERGERPBL: BSEC consent for issuance of Rights Share",
           "(1 Rights Share against 17 existing shares of the Company) to all the "
           "shareholders at an issue price of BDT 1,110 per share including "
           "premium of BDT 1,100 for each Right Share."),
    notice("psi-11", "BERGERPBL", "2025-06-26", "BERGERPBL: Suspension for Record Date",
           "Trading of the shares of the company will remain suspended on record "
           "date i.e. 29.06.2025 for entitlement of rights share."),
    notice("psi-12", "BERGERPBL", "2025-06-29",
           "BERGERPBL: Resumption after Record Date",
           "Trading of the shares of the company will resume on 30.06.2025."),
]


def test_a_rights_issue_is_adjusted_to_its_theoretical_ex_rights_price():
    closes = prices("BERGERPBL", [("2025-06-26", 1736.6, 300),
                                  ("2025-06-29", 1736.6, 0),
                                  ("2025-06-30", 1595.5, 800)])
    events = mp.corporate_actions(pd.DataFrame(BERGER_NEWS), closes)

    assert events[["KIND", "EXDATE"]].to_dict("records") == \
        [{"KIND": "rights", "EXDATE": date(2025, 6, 30)}]
    assert events["FACTOR"].iloc[0] == \
        pytest.approx((17 * 1736.6 + 1110) / (18 * 1736.6))


def test_a_right_to_pay_more_than_the_market_asks_is_no_event():
    closes = prices("BERGERPBL", [("2025-06-26", 900.0, 300),
                                  ("2025-06-30", 890.0, 800)])

    assert mp.corporate_actions(pd.DataFrame(BERGER_NEWS), closes).empty


# The download
# --------------------------------------------------------------------------


class FakeService:
    """The JSON service of the DSE website, answering from fixed rows."""

    def __init__(self, summary=(), dayend=None, news=None, total=None,
                 truncated=False):
        self.summary = list(summary)
        self.dayend = dayend or {}
        self.news = news or {}
        self.total = total
        self.truncated = truncated
        self.requests = []

    def get(self, url, params=None, timeout=None, headers=None):
        assert timeout is not None, "a request without a timeout can hang a run"
        self.requests.append((url.rsplit("/api/live/", 1)[1], dict(params or {})))
        endpoint = url.rsplit("/api/live/", 1)[1]
        day = params.get("from")
        if endpoint == "data-archive/market-summary":
            rows = [r for r in self.summary if params["from"] <= r["date"] <= params["to"]]
            payload = {"rows": rows}
        elif endpoint == "data-archive/instruments":
            payload = {"instruments": sorted({r["tradingCode"]
                                              for rows in self.dayend.values()
                                              for r in rows})}
        elif endpoint == "data-archive/day-end":
            rows = [r for d, rows in self.dayend.items()
                    if params["from"] <= d <= params["to"]
                    for r in rows if r["tradingCode"] == params.get("inst")]
            payload = {"rows": rows, "total": self.total or len(rows)}
        else:
            payload = {"rows": self.news.get(day, []), "truncated": self.truncated}
        return FakeAnswer(payload)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeAnswer:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


def dayend_row(day, code, close, trades, ycp=None):
    return {"serial": 1, "date": day, "tradingCode": code, "ltp": close,
            "high": close, "low": close, "openp": close, "closep": close,
            "ycp": ycp if ycp is not None else close, "trade": trades,
            "value": 1.0, "volume": trades * 10}


def news_row(n):
    return {"id": n["ID"], "code": n["CODE"], "name": n["CODE"], "sector": "",
            "type": "Dividend", "date": "", "time": "", "summary": n["SUMMARY"],
            "body": n["BODY"], "filedAt": n["FILEDAT"]}


def test_a_day_the_archive_answers_in_part_is_raised():
    service = FakeService(dayend={"2025-05-21": [dayend_row("2025-05-21", "GP", 1, 1)]},
                          total=412)

    with pytest.raises(RuntimeError, match="412 day-end rows for GP"):
        bronze_dse.fetch_dse_dayend(service, "GP", date(2025, 5, 1),
                                    date(2025, 5, 30))


def test_a_long_history_is_asked_for_a_year_at_a_time():
    service = FakeService()
    bronze_dse.fetch_dse_dayend(service, "GP", date(2024, 9, 27), date(2026, 9, 27))

    asked = [(p["from"], p["to"]) for e, p in service.requests]
    assert asked == [("2024-09-27", "2025-09-26"), ("2025-09-27", "2026-09-26")]


def test_a_day_of_news_the_archive_truncates_is_raised():
    with pytest.raises(RuntimeError, match="truncated"):
        bronze_dse.fetch_dse_news(FakeService(truncated=True), date(2025, 5, 21))


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
def citybank_service(monkeypatch):
    days = ["2025-05-20", "2025-05-21", "2025-05-22", "2025-05-24", "2025-05-25"]
    closes = dict(zip(days, [23.0, 22.8, 22.8, 19.0, 18.9]))
    trades = dict(zip(days, [429, 814, 0, 1166, 519]))
    news = {}
    for n in CITYBANK_NEWS:
        news.setdefault(n["FILEDAT"], []).append(news_row(n))
    service = FakeService(
        summary=[{"date": d, "dsex": 5000.0 + i, "ds30": 2000.0 + i,
                  "totalTrade": 1, "totalValue": 1.0, "totalVolume": 1,
                  "marketCap": 1.0} for i, d in enumerate(days)],
        dayend={d: [dayend_row(d, "CITYBANK", closes[d], trades[d])] for d in days},
        news=news)

    import requests
    monkeypatch.setattr(requests, "Session", lambda: service)
    monkeypatch.setattr(bronze_dse, "_dhaka_today", lambda: date(2025, 5, 26))
    monkeypatch.setattr(bronze_dse, "DSE_PAUSE", 0)
    return service


def test_the_closes_and_indices_reach_the_price_table(spark, data_lake,
                                                      citybank_service):
    mp.update_bronze_dse(spark, data_lake, min_date="2025-05-20")
    mp.update_silver_dse(spark, data_lake)

    table = mp.connect_to_marketpricedata(spark, data_lake).toPandas()
    city = table[table["INSTRID"] == "DSE-CITYBANK"].sort_values("REFDT")
    # The record date, on which it did not trade, is not a price.
    assert [str(d) for d in city["REFDT"]] == \
        ["2025-05-20", "2025-05-21", "2025-05-24", "2025-05-25"]
    assert city["VALUE"].tolist() == [23.0, 22.8, 19.0, 18.9]
    assert set(table["INSTRID"]) == {"DSE-CITYBANK", "IX-DSEX", "IX-DS30"}


def test_the_prices_are_asked_for_one_security_at_a_time(spark, data_lake,
                                                         citybank_service):
    mp.update_bronze_dse(spark, data_lake, min_date="2025-05-20")

    asked = [p for e, p in citybank_service.requests
             if e == "data-archive/day-end"]
    assert asked == [{"from": "2025-05-20", "to": "2025-05-25",
                      "inst": "CITYBANK"}]


def test_the_corporate_actions_are_a_table_of_their_own(spark, data_lake,
                                                        citybank_service):
    # From the day the dividend was declared, three weeks before its record date.
    mp.update_bronze_dse(spark, data_lake, min_date="2025-04-30")
    mp.update_silver_dse_corporate_actions(spark, data_lake)

    events = mp.connect_to_dse_corporate_actions(spark, data_lake).toPandas()
    assert sorted(events["KIND"]) == ["bonus", "cash"]
    assert set(events["INSTRID"]) == {"DSE-CITYBANK"}
    assert [str(d) for d in events["EXDATE"].unique()] == ["2025-05-24"]


def test_a_second_run_carries_on_from_the_first(spark, data_lake,
                                                citybank_service, monkeypatch):
    mp.update_bronze_dse(spark, data_lake, min_date="2025-05-20")
    citybank_service.requests.clear()
    monkeypatch.setattr(bronze_dse, "_dhaka_today", lambda: date(2025, 5, 27))

    mp.update_bronze_dse(spark, data_lake, min_date="2025-05-20")

    summary = [p for e, p in citybank_service.requests
               if e == "data-archive/market-summary"]
    # The last download ended on the 26th, and a week of it is asked again.
    assert summary[0]["from"] == "2025-05-19"
