"""The Hanoi Stock Exchange source of the market price data, from VNDirect.

Run against a fake of VNDirect's market-data service. The prices are its own
for Saigon-Hanoi Securities (SHS) in September 2026, quoted as the service
quotes them, in thousands of dong.
"""

from __future__ import annotations

from datetime import date

import pytest

import fincommons.marketpricedata as mp
from fincommons.marketpricedata import bronze_vndirect


def price(day, close, volume=100000.0, code="SHS"):
    return {"code": code, "date": day, "floor": "HNX", "open": close, "high": close,
            "low": close, "close": close, "adClose": close, "basicPrice": close,
            "nmVolume": volume, "nmValue": volume * close * 1000,
            "ptVolume": 0.0, "ptValue": 0.0}


class FakeVNDirect:
    """VNDirect's service, answering from fixed rows, page by page."""

    def __init__(self, prices, listings=None, indices=None, total=None):
        self.prices = prices
        self.listings = listings if listings is not None else [
            {"code": "SHS", "floor": "HNX", "isin": "VN000000SHS3",
             "companyNameEng": "Saigon - Hanoi Securities JSC", "listedDate": "2009-07-20"}]
        self.indices = indices or {}
        self.total = total
        self.requests = []

    def get(self, url, params=None, timeout=None, headers=None):
        assert timeout is not None, "a request without a timeout can hang a run"
        endpoint = url.rsplit("/v4/", 1)[1]
        self.requests.append((endpoint, dict(params)))
        query = dict(part.split(":", 1) for part in params["q"].split("~")
                     if not part.startswith("date"))
        dates = [part.split(":")[2] for part in params["q"].split("~")
                 if part.startswith("date")]
        lo, hi = (dates + ["0000", "9999"])[:2]
        if endpoint == "stocks":
            rows = [r for r in self.listings if r["floor"] == query["floor"]]
        elif endpoint == "stock_prices":
            rows = [r for r in self.prices
                    if r["code"] == query["code"] and lo <= r["date"] <= hi]
        else:
            rows = [r for r in self.indices.get(query["code"], [])
                    if lo <= r["date"] <= hi]
        size, page = params["size"], params["page"]
        return FakeAnswer({"data": rows[(page - 1) * size: page * size],
                           "totalElements": self.total or len(rows)})

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


SHS = [price("2026-09-21", 14.3), price("2026-09-22", 14.2),
       price("2026-09-23", 14.1, volume=0.0),  # nothing traded
       price("2026-09-24", 13.9), price("2026-09-25", 13.7)]


def test_a_history_is_read_page_by_page(monkeypatch):
    monkeypatch.setattr(bronze_vndirect, "VNDIRECT_PAGE_SIZE", 2)
    monkeypatch.setattr(bronze_vndirect, "VNDIRECT_PAUSE", 0)
    service = FakeVNDirect(SHS)

    frame = bronze_vndirect.fetch_vndirect_prices(service, "SHS", date(2026, 9, 1),
                                                  date(2026, 9, 27))

    assert len(frame) == 5
    assert [p["page"] for e, p in service.requests] == [1, 2, 3]


def test_an_answer_that_ends_short_is_raised(monkeypatch):
    monkeypatch.setattr(bronze_vndirect, "VNDIRECT_PAUSE", 0)
    with pytest.raises(RuntimeError, match="reported 3424 rows for SHS"):
        bronze_vndirect.fetch_vndirect_prices(FakeVNDirect(SHS, total=3424), "SHS",
                                              date(2013, 1, 1), date(2026, 9, 27))


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
def service(monkeypatch):
    levels = {"HNX": (262.1, 260.4), "HNX30": (560.0, 555.2),
              "VNINDEX": (1790.3, 1785.1), "VN30": (1950.0, 1944.8)}
    fake = FakeVNDirect(SHS, indices={
        code: [{"code": code, "date": "2026-09-24", "close": a},
               {"code": code, "date": "2026-09-25", "close": b}]
        for code, (a, b) in levels.items()})
    import requests
    monkeypatch.setattr(requests, "Session", lambda: fake)
    monkeypatch.setattr(bronze_vndirect, "_vietnam_today", lambda: date(2026, 9, 27))
    monkeypatch.setattr(bronze_vndirect, "VNDIRECT_PAUSE", 0)
    return fake


def test_a_hanoi_share_reaches_the_table_by_isin_in_dong(spark, data_lake, service):
    mp.update_bronze_vndirect(spark, data_lake, min_date="2026-09-01")
    mp.update_silver_vndirect(spark, data_lake)

    table = mp.connect_to_marketpricedata(spark, data_lake).toPandas()
    shs = table[table["INSTRID"] == "VN000000SHS3"].sort_values("REFDT")
    # The day nothing traded is not a price.
    assert [str(d) for d in shs["REFDT"]] == \
        ["2026-09-21", "2026-09-22", "2026-09-24", "2026-09-25"]
    # 13.7 thousand dong is 13,700 dong, the scale of Yahoo's HOSE closes.
    assert shs["VALUE"].iloc[-1] == 13700.0
    # Exactly, not 32,299.999999999996: dong prices are whole numbers.
    assert (shs["VALUE"] == shs["VALUE"].round(0)).all()
    hnx = table[table["INSTRID"] == "IX-HNX"]
    assert sorted(hnx["VALUE"]) == [260.4, 262.1]
    assert {"IX-HNX30", "IX-VNINDEX", "IX-VN30"} <= set(table["INSTRID"])


def test_a_second_run_carries_on_and_a_new_share_brings_its_history(
        spark, data_lake, service, monkeypatch):
    mp.update_bronze_vndirect(spark, data_lake, min_date="2026-09-01")
    service.requests.clear()
    service.listings.append({"code": "PVS", "floor": "HNX", "isin": "VN000000PVS3",
                             "companyNameEng": "PetroVietnam Technical Services",
                             "listedDate": "2007-09-20"})
    service.prices.extend([price("2026-09-10", 30.1, code="PVS")])
    monkeypatch.setattr(bronze_vndirect, "_vietnam_today", lambda: date(2026, 9, 28))

    mp.update_bronze_vndirect(spark, data_lake, min_date="2026-09-01")

    asked = {p["q"].split("~")[0]: p["q"] for e, p in service.requests
             if e == "stock_prices"}
    # SHS carries on a week before the last download ended ...
    assert "date:gte:2026-09-20" in asked["code:SHS"]
    # ... and PVS, new to the layer, is asked for from the first date.
    assert "date:gte:2026-09-01" in asked["code:PVS"]


def test_nothing_is_asked_twice_on_the_same_day(spark, data_lake, service):
    mp.update_bronze_vndirect(spark, data_lake, min_date="2026-09-01")
    service.requests.clear()

    mp.update_bronze_vndirect(spark, data_lake, min_date="2026-09-01")

    assert [e for e, p in service.requests] == ["stocks"]
