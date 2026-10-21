"""Market price data: the Hanoi Stock Exchange, from VNDirect.

Yahoo Finance serves the Ho Chi Minh City exchange (HOSE) and not the Hanoi
one (HNX), so the Hanoi listings come from the public market-data service of
VNDirect, a Vietnamese broker, which publishes three things this module
collects:

``stocks``
    the listed shares of an exchange, with the ISIN beside each trading code;
``stock_prices``
    the daily open, high, low and close of a share, as traded and as adjusted
    for corporate actions, and the volume traded on the order book and in
    negotiated deals;
``vnmarket_prices``
    the daily level of an index. The HNX-Index and HNX30 are collected, and
    the VN-Index and VN30 of the Ho Chi Minh City exchange with them, since
    Yahoo carries no history of either.

It is a broker's service rather than the exchange's own, and it is the one
used because the exchange publishes nothing comparable that can be read
without a browser. Its prices are quoted in **thousands of dong** -- Vinamilk's
59,800 is written 59.8 -- which the Silver stage puts right, so that a Hanoi
close and a Ho Chi Minh City close from Yahoo are on the same scale.

The history reaches back to 2013 for most shares and to August 2017 for the
indices. The layers are partitioned by the date of the download and appended
to, as the Yahoo and DSE ones are, and a run carries on from the last
partition less :data:`VNDIRECT_REVISIT_DAYS`. A share or an index the price
layer has never held is asked for its whole history.

An answer is paged, and says how many rows it holds in all. A page is asked
for :data:`VNDIRECT_PAGE_SIZE` rows at a time until that many have arrived,
and an answer that ends short is raised rather than written, so that a
history is never held in part.
"""

from datetime import date, datetime, timedelta, timezone
import logging
import os
import time

import pandas as pd
from pyspark.sql.types import (DateType, DoubleType, StringType, StructField,
                               StructType)

from ..layout import rebalanced
from ..log import context
from .bronze_yahoo import start_date_from_partitions
from .common import *

logger = logging.getLogger(__name__)

#: Where VNDirect's market-data service answers.
VNDIRECT_API = "https://api-finfo.vndirect.com.vn/v4"

#: The exchanges whose shares are collected. Ho Chi Minh City is not among
#: them: its shares come from Yahoo Finance, see ``data/isin_yahoo.csv``.
VNDIRECT_FLOORS = ("HNX",)

#: The indices collected, by VNDirect's code, and the identifier each is
#: given in the Silver table.
VNDIRECT_INDICES = {
    "HNX": "IX-HNX",
    "HNX30": "IX-HNX30",
    "VNINDEX": "IX-VNINDEX",
    "VN30": "IX-VN30",
}

#: How many days before the end of the last download a run asks for again.
VNDIRECT_REVISIT_DAYS = 7

#: How many rows one page asks for. A whole history of one share is some
#: three thousand rows, so it is one page.
VNDIRECT_PAGE_SIZE = 5000

#: How long a run waits between two requests, in seconds.
VNDIRECT_PAUSE = 0.3

vndirect_price_schema = StructType() \
    .add("REFDT", DateType(), False) \
    .add("CODE", StringType(), False) \
    .add("FLOOR", StringType(), True) \
    .add("OPEN", DoubleType(), True) \
    .add("HIGH", DoubleType(), True) \
    .add("LOW", DoubleType(), True) \
    .add("CLOSE", DoubleType(), True) \
    .add("AD_CLOSE", DoubleType(), True) \
    .add("BASIC_PRICE", DoubleType(), True) \
    .add("NM_VOLUME", DoubleType(), True) \
    .add("NM_VALUE", DoubleType(), True) \
    .add("PT_VOLUME", DoubleType(), True) \
    .add("PT_VALUE", DoubleType(), True)
"""One share on one day, as VNDirect reports it, in thousands of dong.

``CLOSE`` is the close that traded and ``AD_CLOSE`` the close adjusted for
the corporate actions since, which VNDirect recomputes whenever a new one
goes ex -- so the adjusted history of a share differs from one download to
the next, and only ``CLOSE`` reaches the Silver table. ``NM_`` is the order
book, ``PT_`` negotiated deals.
"""

vndirect_index_schema = StructType() \
    .add("REFDT", DateType(), False) \
    .add("CODE", StringType(), False) \
    .add("CLOSE", DoubleType(), True)

vndirect_listing_schema = StructType() \
    .add("CODE", StringType(), False) \
    .add("FLOOR", StringType(), True) \
    .add("ISIN", StringType(), True) \
    .add("NAME", StringType(), True) \
    .add("LISTED", StringType(), True)


def _vietnam_today():
    """Today in Hanoi, seven hours ahead of UTC and without summer time."""
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


def _get(session, endpoint, **params):
    """One request to the service, as parsed JSON."""
    response = session.get(f"{VNDIRECT_API}/{endpoint}", params=params,
                           timeout=HTTP_TIMEOUT,
                           headers={"User-Agent": "fincommons"})
    response.raise_for_status()
    time.sleep(VNDIRECT_PAUSE)
    return response.json()


def _all_pages(session, endpoint, what, **params):
    """Every row of a paged answer, or an error if it ends short."""
    rows, page = [], 1
    while True:
        answer = _get(session, endpoint, size=VNDIRECT_PAGE_SIZE, page=page, **params)
        chunk = answer.get("data") or []
        rows.extend(chunk)
        total = answer.get("totalElements", len(rows))
        if len(rows) >= total or not chunk:
            break
        page += 1
    if len(rows) < total:
        raise RuntimeError(
            f"VNDirect reported {total} rows for {what} and returned {len(rows)}. "
            "A history held in part would read as days that did not trade, so "
            "nothing is written."
        )
    return rows


def fetch_vndirect_listings(session, floor):
    """The listed shares of one exchange, with their ISINs."""
    rows = _all_pages(session, "stocks", f"the {floor} listings",
                      q=f"type:STOCK~status:LISTED~floor:{floor}")
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=vndirect_listing_schema.fieldNames())
    frame = frame.rename(columns={"code": "CODE", "floor": "FLOOR", "isin": "ISIN",
                                  "companyNameEng": "NAME", "listedDate": "LISTED"})
    for column in vndirect_listing_schema.fieldNames():
        if column not in frame:
            frame[column] = None
    return frame[vndirect_listing_schema.fieldNames()]


def fetch_vndirect_prices(session, code, start, end):
    """One share's daily prices in ``[start, end)``."""
    last = (end - timedelta(days=1)).isoformat()
    rows = _all_pages(session, "stock_prices", code, sort="date",
                      q=f"code:{code}~date:gte:{start.isoformat()}~date:lte:{last}")
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=vndirect_price_schema.fieldNames())
    frame = frame.rename(columns={
        "date": "REFDT", "code": "CODE", "floor": "FLOOR", "open": "OPEN",
        "high": "HIGH", "low": "LOW", "close": "CLOSE", "adClose": "AD_CLOSE",
        "basicPrice": "BASIC_PRICE", "nmVolume": "NM_VOLUME",
        "nmValue": "NM_VALUE", "ptVolume": "PT_VOLUME", "ptValue": "PT_VALUE"})
    frame["REFDT"] = pd.to_datetime(frame["REFDT"]).dt.date
    for column in vndirect_price_schema.fieldNames():
        if column not in frame:
            frame[column] = None
    return frame[vndirect_price_schema.fieldNames()]


def fetch_vndirect_index(session, code, start, end):
    """One index's daily closes in ``[start, end)``."""
    last = (end - timedelta(days=1)).isoformat()
    rows = _all_pages(session, "vnmarket_prices", code, sort="date",
                      q=f"code:{code}~date:gte:{start.isoformat()}~date:lte:{last}")
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=vndirect_index_schema.fieldNames())
    frame = frame.rename(columns={"date": "REFDT", "code": "CODE", "close": "CLOSE"})
    frame["REFDT"] = pd.to_datetime(frame["REFDT"]).dt.date
    return frame[vndirect_index_schema.fieldNames()]


def _held_codes(spark, parquet_path):
    """The codes a price layer holds at least one row for."""
    if not os.path.isdir(parquet_path) or \
            not any("LASTDT" in k for k in os.listdir(parquet_path)):
        return set()
    return {row["CODE"] for row in
            spark.read.parquet(parquet_path).select("CODE").distinct().collect()}


def _coerce(frame, schema):
    frame = frame.copy()
    for field in schema.fields:
        if isinstance(field.dataType, DoubleType):
            frame[field.name] = pd.to_numeric(frame[field.name], errors="coerce").astype(float)
        elif isinstance(field.dataType, StringType):
            column = frame[field.name]
            frame[field.name] = column.astype(object).where(column.notna(), None)
    return frame[schema.fieldNames()]


def _append(spark, frame, schema, parquet_path, end):
    """Write one download into its layer as a partition of its own."""
    frame = _coerce(frame, schema)
    frame["LASTDT"] = end.isoformat()
    written = StructType(schema.fields + [StructField("LASTDT", StringType(), False)])
    rebalanced(spark.createDataFrame(frame, schema=written), "LASTDT") \
         .write.partitionBy("LASTDT").mode("append").parquet(parquet_path)


def update_bronze_vndirect(spark, config_path, min_date="2000-01-01"):
    """Download the Hanoi listings, their prices and the Vietnamese indices.

    The listings are collected whole every run, since they are what pairs a
    trading code with its ISIN and a share can list or delist on any day. The
    prices of a share already held are asked for from the last download less
    :data:`VNDIRECT_REVISIT_DAYS`; a share or index not held yet is asked for
    from ``min_date``, which the service answers with the history it has.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD.

    Examples
    --------
    >>> update_bronze_vndirect(spark, "path/to/configuration/file")
    """
    import requests

    prices_path = path_bronze_vndirect_prices(config_path)
    end = _vietnam_today()
    first = date.fromisoformat(min_date)
    carry_on = date.fromisoformat(start_date_from_partitions(prices_path, min_date))
    start = max(first, carry_on - timedelta(days=VNDIRECT_REVISIT_DAYS)) \
        if carry_on > first else first
    held = _held_codes(spark, prices_path)
    held_indices = _held_codes(spark, path_bronze_vndirect_indices(config_path))

    with requests.Session() as session:
        listings = pd.concat([fetch_vndirect_listings(session, floor)
                              for floor in VNDIRECT_FLOORS], ignore_index=True)
        if listings.empty:
            raise RuntimeError("VNDirect returned no listed share for "
                               f"{', '.join(VNDIRECT_FLOORS)}; nothing is written.")
        codes = sorted(set(listings["CODE"]))
        if carry_on >= end and set(codes) <= held and set(VNDIRECT_INDICES) <= held_indices:
            logger.info(
                "The Bronze layer is current, nothing to download",
                extra=context(module="marketpricedata", layer="bronze",
                              source="VNDIRECT", end=end))
            return

        prices = [fetch_vndirect_prices(session, code,
                                        start if code in held else first, end)
                  for code in codes if code not in held or carry_on < end]
        indices = [fetch_vndirect_index(session, code,
                                        start if code in held_indices else first, end)
                   for code in VNDIRECT_INDICES
                   if code not in held_indices or carry_on < end]

    prices = [p for p in prices if not p.empty]
    indices = [i for i in indices if not i.empty]
    if not prices:
        logger.warning(
            "VNDirect returned no price for any of %d shares between %s and %s",
            len(codes), start, end,
            extra=context(module="marketpricedata", layer="bronze", source="VNDIRECT"))
        return

    _append(spark, listings, vndirect_listing_schema,
            path_bronze_vndirect_listings(config_path), end)
    if indices:
        _append(spark, pd.concat(indices, ignore_index=True), vndirect_index_schema,
                path_bronze_vndirect_indices(config_path), end)
    # Last, because its partitions are what the next run carries on from.
    _append(spark, pd.concat(prices, ignore_index=True), vndirect_price_schema,
            prices_path, end)
