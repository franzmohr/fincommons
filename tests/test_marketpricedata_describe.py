"""describe_instrument: what an identifier in the market price table stands for.

It needs neither Spark nor a data lake, except for the name of a Hanoi share,
which is read from VNDirect's listings in a lake; that case is given a small
listings layer of its own.
"""

from __future__ import annotations

import pandas as pd
import pytest

import fincommons.marketpricedata as mp


@pytest.mark.parametrize("identifier, kind, source, unit, currency", [
    ("EUR/USD", "exchange rate", "ECB euro foreign exchange reference rates", "USD per EUR", None),
    ("EUR/VND", "exchange rate", "Yahoo Finance", "VND per EUR", None),
    ("ON-ESTR", "overnight rate", "ECB", "per cent per annum", None),
    ("ON-SOFR", "overnight rate", "Federal Reserve Bank of New York", "per cent per annum", None),
    ("KR-DFR", "policy rate", "ECB Statistical Data Warehouse, FM flow", "per cent per annum", None),
    ("YLD-EA10Y-M", "bond yield", "ECB Statistical Data Warehouse, FM flow", "per cent per annum", None),
    ("EQ-ESTOXX-M", "equity index", "ECB Statistical Data Warehouse, FM flow", "index points", None),
    ("EL-DELU", "electricity price",
     "Fraunhofer ISE energy-charts (origin Bundesnetzagentur | SMARD.de)", "EUR per MWh", "EUR"),
    ("CO-GOLD", "precious metal benchmark", "LBMA", "US dollars per troy ounce", "USD"),
    ("CO-BRENT", "commodity future", "Yahoo Finance", "US dollars per contract unit", "USD"),
    ("VOL-VIX", "volatility index", "Yahoo Finance",
     "index points (annualised implied volatility, per cent)", None),
    ("IX-DSEX", "stock index", "Dhaka Stock Exchange", "index points", "BDT"),
    ("IX-VNINDEX", "stock index", "VNDirect (public market-data service)", "index points", "VND"),
    ("DSE-GP", "listed share", "Dhaka Stock Exchange day-end archive", "taka per share", "BDT"),
    ("VN000000VNM8", "listed instrument", "Yahoo Finance", "VND per share", "VND"),
    ("SE0000106270", "listed instrument", "Yahoo Finance", "SEK per share", "SEK"),
    ("IT0003465736", "stock index", "Yahoo Finance", "index points", None),
])
def test_every_kind_of_identifier_is_described(identifier, kind, source, unit, currency):
    described = mp.describe_instrument(identifier)

    assert (described.kind, described.source, described.unit, described.currency) == \
        (kind, source, unit, currency)
    assert described.frequency and described.collected_by


def test_a_listed_share_is_named_and_its_ticker_given():
    described = mp.describe_instrument("se0000106270")  # case does not matter

    assert described.identifier == "SE0000106270"
    assert described.name.startswith("Hennes & Mauritz")
    assert described.source_id == "ticker HM-B.ST on Nasdaq Stockholm"


def test_a_monthly_ecb_series_says_it_is_monthly_and_by_which_code():
    described = mp.describe_instrument("YLD-EA10Y-M")

    assert described.frequency == "monthly"
    assert described.source_id == "series U2_10Y"
    assert any("once a month" in note for note in described.notes)


def test_the_description_prints_as_a_record():
    text = str(mp.describe_instrument("EUR/VND"))

    assert text.splitlines()[0] == "EUR/VND -- Vietnamese dong per euro"
    assert "  source       Yahoo Finance, ticker EURVND=X" in text


def test_a_hanoi_share_is_named_from_the_lake_when_one_is_given(tmp_path):
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8")
    listings = tmp_path / "bronze" / "vndirect" / "listings" / "LASTDT=2026-09-27"
    listings.mkdir(parents=True)
    pd.DataFrame({"CODE": ["SHS"], "FLOOR": ["HNX"], "ISIN": ["VN000000SHS3"],
                  "NAME": ["Saigon - Hanoi Securities JSC"], "LISTED": ["2009-06-25"]}) \
        .to_parquet(listings / "part.parquet")

    unnamed = mp.describe_instrument("VN000000SHS3")
    named = mp.describe_instrument("VN000000SHS3", config_path=str(config_path))

    assert unnamed.source == named.source == "VNDirect (public market-data service)"
    assert "pass config_path" in unnamed.name
    assert named.name == "Saigon - Hanoi Securities JSC"
    assert named.unit == "dong per share"


def test_an_identifier_no_source_delivers_is_an_error():
    with pytest.raises(KeyError, match="not an identifier any public source"):
        mp.describe_instrument("NOT-A-SERIES")
    with pytest.raises(KeyError, match="electricity zones"):
        mp.describe_instrument("EL-XX")
