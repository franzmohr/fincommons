"""Market price data: what an identifier in the Silver table stands for.

The combined table holds a reference date, an identifier and a value, and
nothing else -- no name, no unit, no currency, no publisher. That is what keeps
it one table for every source, and it is also why a reader looking at
``EUR/VND`` or ``YLD-EA10Y-M`` or ``VN000000SHS3`` has to know the conventions
of the module to know what the number is.

:func:`describe_instrument` answers that for one identifier::

    >>> from fincommons import marketpricedata as mp
    >>> print(mp.describe_instrument("EUR/VND"))
    EUR/VND -- Vietnamese dong per euro
      kind       exchange rate
      source     Yahoo Finance, ticker EURVND=X
      ...

It reads the same tables the downloads are driven by -- the Yahoo mapping
tables, the ECB series of :mod:`~fincommons.marketpricedata.silver_ecb`, the
electricity zones, the DSE and VNDirect conventions -- so it describes what the
module collects rather than a copy that could drift from it. It needs no Spark
and no data lake. What it cannot know without the lake is the name of a Hanoi
share, which only VNDirect's listings give; pass ``config_path`` to read them.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import pandas as pd

from .bronze_electricity import ELECTRICITY_ZONES
from .bronze_vndirect import VNDIRECT_INDICES
from .bronze_yahoo import (load_list_of_yahoo_commodities, load_list_of_yahoo_fx,
                           load_list_of_yahoo_indices, load_list_of_yahoo_tickers,
                           load_list_of_yahoo_volatility)
from .silver_ecb import ECB_KEY_RATES, ECB_MONTHLY_EA, ECB_MONTHLY_US
from .silver_dse import DSE_PREFIX

__all__ = ["InstrumentDescription", "describe_instrument"]


@dataclass
class InstrumentDescription:
    """What one identifier of the Silver table stands for.

    ``unit`` is what one unit of ``VALUE`` is; ``currency`` is the currency of
    a price, or None for a rate, a ratio or an index level.
    """

    identifier: str
    name: str | None
    kind: str
    source: str
    source_id: str | None
    unit: str
    currency: str | None
    frequency: str
    collected_by: str
    notes: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        title = f"{self.identifier} -- {self.name}" if self.name else self.identifier
        rows = [("kind", self.kind),
                ("source", self.source + (f", {self.source_id}" if self.source_id else "")),
                ("unit", self.unit),
                ("currency", self.currency or "--"),
                ("frequency", self.frequency),
                ("collected by", self.collected_by)]
        lines = [title] + [f"  {k:<12} {v}" for k, v in rows]
        lines += [f"  note         {n}" for n in self.notes]
        return "\n".join(lines)


#: The exchange and quote currency of a Yahoo Finance suffix.
_YAHOO_EXCHANGES = {
    ".VI": ("Wiener Börse", "EUR"), ".DE": ("Xetra", "EUR"),
    ".F": ("Frankfurt", "EUR"), ".PA": ("Euronext Paris", "EUR"),
    ".AS": ("Euronext Amsterdam", "EUR"), ".BR": ("Euronext Brussels", "EUR"),
    ".MI": ("Borsa Italiana", "EUR"), ".MC": ("Bolsa de Madrid", "EUR"),
    ".LS": ("Euronext Lisbon", "EUR"), ".IR": ("Euronext Dublin", "EUR"),
    ".HE": ("Nasdaq Helsinki", "EUR"), ".AT": ("Athens Exchange", "EUR"),
    ".TL": ("Nasdaq Tallinn", "EUR"), ".RG": ("Nasdaq Riga", "EUR"),
    ".VS": ("Nasdaq Vilnius", "EUR"), ".LJ": ("Ljubljana Stock Exchange", "EUR"),
    ".ST": ("Nasdaq Stockholm", "SEK"), ".CO": ("Nasdaq Copenhagen", "DKK"),
    ".OL": ("Oslo Børs", "NOK"), ".IC": ("Nasdaq Iceland", "ISK"),
    ".WA": ("Warsaw Stock Exchange", "PLN"), ".PR": ("Prague Stock Exchange", "CZK"),
    ".BD": ("Budapest Stock Exchange", "HUF"), ".RO": ("Bucharest Stock Exchange", "RON"),
    ".L": ("London Stock Exchange", "GBP"), ".SW": ("SIX Swiss Exchange", "CHF"),
    ".VN": ("Ho Chi Minh City Stock Exchange (HOSE)", "VND"),
}

_CURRENCY_NAMES = {
    "USD": "US dollar", "GBP": "pound sterling", "CHF": "Swiss franc",
    "JPY": "Japanese yen", "SEK": "Swedish krona", "NOK": "Norwegian krone",
    "DKK": "Danish krone", "PLN": "Polish zloty", "CZK": "Czech koruna",
    "HUF": "Hungarian forint", "RON": "Romanian leu", "BGN": "Bulgarian lev",
    "CNY": "Chinese renminbi", "INR": "Indian rupee", "BDT": "Bangladeshi taka",
    "VND": "Vietnamese dong", "TRY": "Turkish lira", "AUD": "Australian dollar",
    "CAD": "Canadian dollar", "HKD": "Hong Kong dollar", "KRW": "South Korean won",
    "SGD": "Singapore dollar", "ZAR": "South African rand", "BRL": "Brazilian real",
    "MXN": "Mexican peso", "IDR": "Indonesian rupiah", "ILS": "Israeli shekel",
    "ISK": "Icelandic krona", "MYR": "Malaysian ringgit", "NZD": "New Zealand dollar",
    "PHP": "Philippine peso", "THB": "Thai baht", "RUB": "Russian rouble",
}

#: The ECB series of silver_ecb, described. The identifiers are those tables';
#: the words are here.
_ECB_SERIES = {
    "KR-DFR": "ECB deposit facility rate -- the floor of the euro money market corridor",
    "KR-MRO": "ECB main refinancing operations rate",
    "KR-MLF": "ECB marginal lending facility rate -- the ceiling of the corridor",
    "YLD-US10Y-M": "US 10-year Treasury yield, average of the month",
    "YLD-US10YZ-M": "US 10-year zero-coupon yield at the end of the month",
    "SPR-US10Y1Y-M": "US 10-year minus 1-year Treasury spread",
    "YLD-EA2Y-M": "Euro area 2-year government benchmark yield",
    "YLD-EA3Y-M": "Euro area 3-year government benchmark yield",
    "YLD-EA5Y-M": "Euro area 5-year government benchmark yield",
    "YLD-EA7Y-M": "Euro area 7-year government benchmark yield",
    "YLD-EA10Y-M": "Euro area 10-year government benchmark yield",
    "YLD-EA10Y-REAL-M": "Euro area 10-year government benchmark yield, real",
    "IBOR-EUR1M-M": "1-month EURIBOR",
    "IBOR-EUR3M-M": "3-month EURIBOR",
    "IBOR-EUR6M-M": "6-month EURIBOR",
    "IBOR-EUR1Y-M": "12-month EURIBOR",
    "IBOR-EUR3M-REAL-M": "3-month EURIBOR, real",
    "ON-EONIA-M": "EONIA, the euro overnight index average",
    "EQ-ESTOXX-M": "EURO STOXX broad index",
    "EQ-ESTOXX-BASICMAT-M": "EURO STOXX basic materials",
    "EQ-ESTOXX-CONSTPL-M": "EURO STOXX consumer staples",
    "EQ-ESTOXX-CONSDIS-M": "EURO STOXX consumer discretionary",
    "EQ-ESTOXX-FINANC-M": "EURO STOXX financials",
    "EQ-ESTOXX-TECH-M": "EURO STOXX technology",
    "EQ-ESTOXX-HEALTH-M": "EURO STOXX health care",
    "EQ-ESTOXX-INDUS-M": "EURO STOXX industrials",
    "EQ-ESTOXX-ENERGY-M": "EURO STOXX energy",
    "EQ-ESTOXX-TELECOM-M": "EURO STOXX telecommunications",
    "EQ-ESTOXX-UTIL-M": "EURO STOXX utilities",
}

_INDEX_NAMES = {
    "IX-DSEX": "DSEX, the Dhaka Stock Exchange broad index",
    "IX-DS30": "DS30, the Dhaka Stock Exchange blue-chip index",
    "IX-HNX": "HNX-Index, the Hanoi Stock Exchange broad index",
    "IX-HNX30": "HNX30, the Hanoi Stock Exchange blue-chip index",
    "IX-VNINDEX": "VN-Index, the Ho Chi Minh City Stock Exchange broad index",
    "IX-VN30": "VN30, the Ho Chi Minh City Stock Exchange blue-chip index",
}

_OVERNIGHT = {
    "ON-ESTR": ("€STR, the euro short-term rate", "ECB", "EST flow", "ECB_ESTR"),
    "ON-SOFR": ("SOFR, the secured overnight financing rate", "Federal Reserve Bank of New York",
                "SOFR", "NYFED"),
    "ON-SONA": ("SONIA, the sterling overnight index average", "Bank of England",
                "series IUDSOIA", "BOE"),
}


#: The benchmark indices among the hand-chosen rows of isin_yahoo.csv. They
#: are named by their index ISIN like a share, and one of them (FTSE MIB) has
#: a ticker without Yahoo's caret, so neither says it is an index.
_INDEX_ISINS = frozenset({
    "AT0000999982", "EU0009658145", "US78378X1072", "DE0008469008",
    "FR0003500008", "GB0001383545", "CH0009980894", "ES0SI0000005",
    "IT0003465736", "NL0000000107", "EU0009658202",
})


def _yahoo_row(table, column, identifier):
    rows = table[table[column].str.upper() == identifier]
    return rows.iloc[0] if len(rows) else None


def _hanoi_name(identifier, config_path):
    """The name VNDirect's listings give a Hanoi share, or None."""
    from .common import path_bronze_vndirect_listings

    path = path_bronze_vndirect_listings(config_path)
    if not os.path.isdir(path):
        return None
    listings = pd.read_parquet(path)
    named = listings[listings["ISIN"] == identifier]
    if named.empty:
        return None
    return named.sort_values("LASTDT").iloc[-1]["NAME"]


def describe_instrument(identifier, config_path=None):
    """Describe one identifier of the market price table.

    Parameters
    ----------
    identifier: str
        As the Silver table holds it: an ISIN, ``EUR/USD``, ``ON-ESTR``,
        ``CO-GOLD``, ``DSE-GP``, ``IX-HNX`` and so on. Case does not matter.
    config_path: str, optional
        The configuration file of a data lake. Only needed for the name of a
        Hanoi share, which only VNDirect's listings in the lake give.

    Returns
    -------
    InstrumentDescription
        Printable; its fields are also there to be read one by one.

    Raises
    ------
    KeyError
        Where the identifier is none the module's public sources deliver. It
        may still be in a lake as a proprietary quote, which the module takes
        as it comes and cannot describe.

    Examples
    --------
    >>> print(describe_instrument("VN000000VNM8"))
    >>> describe_instrument("YLD-EA10Y-M").unit
    'per cent per annum'
    """
    ident = str(identifier).strip().upper()

    # Exchange rates: Yahoo for the currencies the ECB does not fix, else ECB.
    if len(ident) == 7 and ident[3] == "/":
        base, quote = ident.split("/")
        name = f"{_CURRENCY_NAMES.get(quote, quote)} per {'euro' if base == 'EUR' else base}"
        row = _yahoo_row(load_list_of_yahoo_fx(), "INSTRID", ident)
        if row is not None:
            return InstrumentDescription(
                ident, name, "exchange rate", "Yahoo Finance", f"ticker {row['yahoo']}",
                f"{quote} per {base}", None, "daily, Yahoo's trading days",
                "update_bronze_yahoo_fx / update_silver_yahoo_fx",
                ["A market quote at Yahoo's close, not a fixing. The ECB fixes no "
                 f"reference rate for {quote}, which is why it comes from Yahoo.",
                 "Named and quoted like the ECB rates, so a price is divided by it "
                 "the same way."])
        return InstrumentDescription(
            ident, name, "exchange rate", "ECB euro foreign exchange reference rates",
            f"EXR series D.{quote}.{base}.SP00.A", f"{quote} per {base}", None,
            "daily, TARGET business days", "fincommons.exchangerate, merged by "
            "update_silver_exchangerate",
            ["Fixed at 14:15 CET; a currency the ECB stopped fixing ends where it did."])

    if ident in _OVERNIGHT:
        name, publisher, series, stage = _OVERNIGHT[ident]
        return InstrumentDescription(
            ident, name, "overnight rate", publisher, series, "per cent per annum", None,
            "daily, the publisher's business days", f"update_bronze_{stage.lower()}",
            ["A rate, not a price: it does not compound as a series of levels."])

    if ident in _ECB_SERIES:
        monthly = ident.endswith("-M")
        code = {**{v: k for k, v in ECB_KEY_RATES.items()},
                **{v: k for k, v in ECB_MONTHLY_US.items()},
                **{v: k for k, v in ECB_MONTHLY_EA.items()}}.get(ident)
        equity = ident.startswith("EQ-")
        notes = []
        if monthly:
            notes.append("Monthly: dated the first of its month, so a join on a daily "
                         "reference date finds it once a month.")
        if ident == "ON-EONIA-M":
            notes.append("Discontinued in December 2021; €STR (ON-ESTR) replaced it.")
        kind = ("policy rate" if ident.startswith("KR-") else
                "equity index" if equity else
                "overnight rate" if ident.startswith("ON-") else
                "interbank rate" if ident.startswith("IBOR-") else
                "yield spread" if ident.startswith("SPR-") else "bond yield")
        return InstrumentDescription(
            ident, _ECB_SERIES[ident], kind, "ECB Statistical Data Warehouse, FM flow",
            f"series {code}" if code else None,
            "index points" if equity else "per cent per annum", None,
            "monthly" if monthly else "daily, as decided", "update_bronze_ecb", notes)

    if ident.startswith("EL-"):
        zone = {v: k for k, v in ELECTRICITY_ZONES.items()}.get(ident)
        if zone is None:
            raise KeyError(f"{ident} is not one of the electricity zones collected: "
                           + ", ".join(sorted(ELECTRICITY_ZONES.values())))
        return InstrumentDescription(
            ident, f"Day-ahead electricity price, bidding zone {zone}", "electricity price",
            "Fraunhofer ISE energy-charts (origin Bundesnetzagentur | SMARD.de)",
            f"bidding zone {zone}", "EUR per MWh", "EUR",
            "daily, the average of the market day's delivery periods",
            "update_bronze_electricity",
            ["Published under CC BY 4.0: attribution to Bundesnetzagentur | SMARD.de "
             "is required."])

    if ident in ("CO-GOLD", "CO-SILVER"):
        metal = ident[3:].lower()
        return InstrumentDescription(
            ident, f"LBMA {metal} price", "precious metal benchmark", "LBMA",
            f"LBMA {metal.title()} Price", "US dollars per troy ounce", "USD",
            "daily, London business days", "update_bronze_lbma",
            ["A benchmark set by auction, not a traded close."])
    if ident.startswith("CO-"):
        row = _yahoo_row(load_list_of_yahoo_commodities(), "INSTRID", ident)
        if row is not None:
            return InstrumentDescription(
                ident, f"{ident[3:].title()} front-month future", "commodity future",
                "Yahoo Finance", f"ticker {row['yahoo']}", "US dollars per contract unit",
                "USD", "daily, exchange trading days", "update_bronze_yahoo_commodities",
                ["The nearest contract still trading, so the series steps when one "
                 "contract expires and the next becomes the front month."])

    if ident.startswith("VOL-"):
        row = _yahoo_row(load_list_of_yahoo_volatility(), "INSTRID", ident)
        if row is not None:
            return InstrumentDescription(
                ident, f"{ident[4:]} implied volatility index", "volatility index",
                "Yahoo Finance", f"ticker {row['yahoo']}", "index points "
                "(annualised implied volatility, per cent)", None,
                "daily, exchange trading days", "update_bronze_yahoo_volatility",
                ["Read off option prices: what protection costs, not what prices did."])

    if ident in _INDEX_NAMES:
        dhaka = ident in ("IX-DSEX", "IX-DS30")
        return InstrumentDescription(
            ident, _INDEX_NAMES[ident], "stock index",
            "Dhaka Stock Exchange" if dhaka else "VNDirect (public market-data service)",
            "market summary" if dhaka else
            f"index {({v: k for k, v in VNDIRECT_INDICES.items()})[ident]}",
            "index points", "BDT" if dhaka else "VND", "daily, exchange trading days",
            "update_bronze_dse" if dhaka else "update_bronze_vndirect",
            ["A price index: dividends are not reinvested."]
            + ([] if dhaka else ["History from August 2017."]))
    if ident.startswith("IX-"):
        row = _yahoo_row(load_list_of_yahoo_indices(), "INSTRID", ident)
        if row is not None:
            return InstrumentDescription(
                ident, f"{ident[3:]} stock index", "stock index", "Yahoo Finance",
                f"ticker {row['yahoo']}", "index points", None,
                "daily, exchange trading days", "update_bronze_yahoo_indices")

    if ident.startswith(DSE_PREFIX):
        return InstrumentDescription(
            ident, f"Dhaka Stock Exchange share, trading code {ident[len(DSE_PREFIX):]}",
            "listed share", "Dhaka Stock Exchange day-end archive",
            f"trading code {ident[len(DSE_PREFIX):]}", "taka per share", "BDT",
            "daily, days the share traded (Sunday to Thursday)", "update_bronze_dse",
            ["The close that traded: bonus and rights issues and cash dividends are "
             "in the table connect_to_dse_corporate_actions opens, not in the price.",
             "The exchange serves two years of history; the lake holds what it "
             "collected since."])

    row = _yahoo_row(load_list_of_yahoo_tickers(), "ISIN", ident)
    if row is not None:
        ticker = row["yahoo"]
        index = ticker.startswith("^") or ident in _INDEX_ISINS
        suffix = "." + ticker.rsplit(".", 1)[1] if "." in ticker else ""
        exchange, currency = _YAHOO_EXCHANGES.get(suffix, ("US exchange", "USD"))
        name = row.get("name") if isinstance(row.get("name"), str) else None
        notes = (["A price index: dividends are not reinvested."] if index else
                 ["The close that traded: not adjusted for dividends, and splits "
                  "undone (see traded_closes)."])
        if suffix == ".L":
            notes.append("Yahoo quotes most London shares in pence (GBp), not pounds.")
        first = row.get("first_date")
        return InstrumentDescription(
            ident, name, "stock index" if index else "listed instrument",
            "Yahoo Finance", f"ticker {ticker}" + ("" if index else f" on {exchange}"),
            "index points" if index else f"{currency} per share", None if index else currency,
            "daily, exchange trading days",
            "update_bronze_yahoo" + (f", history from {first}" if isinstance(first, str) else ""),
            notes)

    if ident.startswith("VN") and len(ident) == 12:
        name = _hanoi_name(ident, config_path) if config_path else None
        return InstrumentDescription(
            ident, name or "Hanoi Stock Exchange share (name in VNDirect's listings; "
                           "pass config_path)",
            "listed share", "VNDirect (public market-data service)", "stock_prices",
            "dong per share", "VND", "daily, days the share traded",
            "update_bronze_vndirect",
            ([] if name else
             ["A Vietnamese ISIN not in the Yahoo table is a Hanoi listing if the "
              "lake holds it; with config_path the name confirms it."])
            + ["VNDirect quotes thousands of dong; the table holds dong."])

    raise KeyError(
        f"{ident} is not an identifier any public source of marketpricedata delivers. "
        "Known kinds: ISINs of listed instruments, EUR/XXX exchange rates, ON- and KR- "
        "rates, YLD-/SPR-/IBOR-/EQ- ECB series, EL- electricity zones, CO- commodities, "
        "VOL- volatility indices, IX- stock indices, DSE- Dhaka shares. It may still be "
        "a proprietary quote in a lake's internal source.")
