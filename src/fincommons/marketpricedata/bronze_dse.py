"""Market price data: the day-end archive of the Dhaka Stock Exchange.

The DSE publishes three things this module collects, all from the JSON service
behind the data archive of its website:

``day-end``
    the open, high, low, close, last traded price, the close of the day before,
    and the trades, value and volume of every security, one day at a time;
``market-summary``
    the DSEX broad index and the DS30 blue-chip index, with the market's
    turnover and capitalisation;
``news``
    the notices of listed companies, which is where dividend declarations and
    record dates are published, and from which
    :mod:`fincommons.marketpricedata.dse_events` reads the corporate actions
    the closes have to be adjusted for.

**The archive reaches back two years and no further.** The exchange serves the
last two years and answers an older request with nothing. A history longer
than that exists only where someone kept it, which is what the Bronze layer is
for: every run appends what is new, so the lake holds the whole history from
the first run onwards, and a lake that is not updated for more than two years
loses the days in between for good.

**An answer is capped, and says so.** The service returns at most a few hundred
rows to a request -- five hundred for the day-end prices and the news, four
hundred for the market summary -- and does not page. A trading day has more
securities than that, some six hundred and sixty with the bonds and the funds,
so the prices are asked for one security at a time, a year of it per request,
and an answer that reports more rows than it carries is raised rather than
written, so that a history is never held in part. The news are asked for one
day at a time for the same reason, and the market summary a season at a time.
A first run therefore sends some two thousand requests and takes a quarter of
an hour; a daily run sends one per security.

The layers are partitioned by the date of the download and appended to, as the
Yahoo Finance ones are, and a run carries on from the last partition less
:data:`DSE_REVISIT_DAYS`, so that a day the exchange corrects after the fact is
collected again.
"""

from datetime import date, datetime, timedelta, timezone
import logging
import time

import pandas as pd
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                               StructField, StructType)

from ..layout import rebalanced
from ..log import context
from .bronze_yahoo import start_date_from_partitions
from .common import *

logger = logging.getLogger(__name__)

#: Where the JSON service of the DSE website answers.
DSE_API = "https://www.dsebd.org/api/live"

#: How far back the exchange serves its archive, in days.
DSE_HISTORY_DAYS = 730

#: How many days before the end of the last download a run asks for again.
DSE_REVISIT_DAYS = 7

#: How long a run waits between two requests, in seconds. The archive is a
#: public website, and a first run asks it for two thousand answers.
DSE_PAUSE = 0.3

#: The most calendar days of one security's prices one request is trusted
#: with. A year is some two hundred and forty trading days, half the cap.
DSE_DAYEND_CHUNK_DAYS = 365

#: The most days of market summary one request is trusted with. The service
#: returns at most four hundred rows, and a season of trading days is well
#: below that.
DSE_SUMMARY_CHUNK_DAYS = 120

dse_dayend_schema = StructType() \
    .add("REFDT", DateType(), False) \
    .add("CODE", StringType(), False) \
    .add("OPEN", DoubleType(), True) \
    .add("HIGH", DoubleType(), True) \
    .add("LOW", DoubleType(), True) \
    .add("CLOSE", DoubleType(), True) \
    .add("YCP", DoubleType(), True) \
    .add("LTP", DoubleType(), True) \
    .add("TRADES", LongType(), True) \
    .add("VALUE_MN", DoubleType(), True) \
    .add("VOLUME", LongType(), True)
"""One security on one day, as the archive reports it.

``CLOSE`` is the close the exchange publishes and ``YCP`` its close of the day
before. Neither is adjusted for a corporate action: the day after a bonus issue
the close falls and ``YCP`` is still the close before it. ``VALUE_MN`` is the
turnover in millions of taka. A day the share was suspended -- its record date,
typically -- is reported with no trades and the last close carried.
"""

dse_summary_schema = StructType() \
    .add("REFDT", DateType(), False) \
    .add("DSEX", DoubleType(), True) \
    .add("DS30", DoubleType(), True) \
    .add("TOTAL_TRADES", LongType(), True) \
    .add("TOTAL_VALUE_MN", DoubleType(), True) \
    .add("TOTAL_VOLUME", LongType(), True) \
    .add("MARKET_CAP_MN", DoubleType(), True)

dse_news_schema = StructType() \
    .add("ID", StringType(), False) \
    .add("CODE", StringType(), True) \
    .add("TYPE", StringType(), True) \
    .add("FILEDAT", DateType(), True) \
    .add("SUMMARY", StringType(), True) \
    .add("BODY", StringType(), True)


def _dhaka_today():
    """Today in Dhaka, which is six hours ahead of UTC and keeps no summer time.

    The archive holds a day once the market has closed there, and a run in the
    European evening is already a day later in Dhaka than the date on the clock
    of the machine it runs on.
    """
    return (datetime.now(timezone.utc) + timedelta(hours=6)).date()


def _get(session, endpoint, **params):
    """One request to the service, as parsed JSON."""
    import requests  # noqa: F401 - the session is a requests.Session

    response = session.get(f"{DSE_API}/{endpoint}", params=params,
                           timeout=HTTP_TIMEOUT,
                           headers={"User-Agent": "fincommons"})
    response.raise_for_status()
    time.sleep(DSE_PAUSE)
    return response.json()


def _days(start, end):
    """Every calendar day in ``[start, end)``."""
    day = start
    while day < end:
        yield day
        day += timedelta(days=1)


def fetch_dse_summary(session, start, end):
    """The market summary between two days, ``[start, end)``."""
    rows = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(end, chunk_start + timedelta(days=DSE_SUMMARY_CHUNK_DAYS))
        answer = _get(session, "data-archive/market-summary",
                      **{"from": chunk_start.isoformat(),
                         "to": (chunk_end - timedelta(days=1)).isoformat()})
        rows.extend(answer.get("rows") or [])
        chunk_start = chunk_end

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=dse_summary_schema.fieldNames())
    frame = frame.rename(columns={
        "date": "REFDT", "dsex": "DSEX", "ds30": "DS30",
        "totalTrade": "TOTAL_TRADES", "totalValue": "TOTAL_VALUE_MN",
        "totalVolume": "TOTAL_VOLUME", "marketCap": "MARKET_CAP_MN"})
    frame["REFDT"] = pd.to_datetime(frame["REFDT"]).dt.date
    return frame.drop_duplicates("REFDT")[dse_summary_schema.fieldNames()]


def fetch_dse_instruments(session):
    """The trading codes the archive serves prices for."""
    return sorted(set(_get(session, "data-archive/instruments")
                      .get("instruments") or []))


def fetch_dse_dayend(session, code, start, end):
    """One security's day-end rows between two days, ``[start, end)``."""
    rows = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(end, chunk_start + timedelta(days=DSE_DAYEND_CHUNK_DAYS))
        last = (chunk_end - timedelta(days=1)).isoformat()
        answer = _get(session, "data-archive/day-end",
                      **{"from": chunk_start.isoformat(), "to": last,
                         "inst": code})
        chunk = answer.get("rows") or []
        total = answer.get("total")
        if total is not None and total > len(chunk):
            raise RuntimeError(
                f"The DSE archive reported {total} day-end rows for {code} "
                f"from {chunk_start} to {last} and returned {len(chunk)}. A "
                "history held in part would read as days on which the share "
                "did not trade, so nothing is written."
            )
        rows.extend(chunk)
        chunk_start = chunk_end

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=dse_dayend_schema.fieldNames())
    frame = frame.rename(columns={
        "date": "REFDT", "tradingCode": "CODE", "openp": "OPEN", "high": "HIGH",
        "low": "LOW", "closep": "CLOSE", "ycp": "YCP", "ltp": "LTP",
        "trade": "TRADES", "value": "VALUE_MN", "volume": "VOLUME"})
    frame["REFDT"] = pd.to_datetime(frame["REFDT"]).dt.date
    return frame[dse_dayend_schema.fieldNames()]


def fetch_dse_news(session, day):
    """Every notice filed on one day."""
    answer = _get(session, "news",
                  **{"from": day.isoformat(), "to": day.isoformat()})
    if answer.get("truncated"):
        raise RuntimeError(
            f"The DSE archive truncated the news of {day}. A notice missing "
            "from the layer is a corporate action missing from the prices, so "
            "nothing is written."
        )
    frame = pd.DataFrame(answer.get("rows") or [])
    if frame.empty:
        return pd.DataFrame(columns=dse_news_schema.fieldNames())
    frame = frame.rename(columns={"id": "ID", "code": "CODE", "type": "TYPE",
                                  "filedAt": "FILEDAT", "summary": "SUMMARY",
                                  "body": "BODY"})
    frame["FILEDAT"] = pd.to_datetime(frame["FILEDAT"]).dt.date
    return frame[dse_news_schema.fieldNames()]


def _range_to_collect(parquet_path, min_date):
    """The days ``[start, end)`` a DSE layer still has to be asked for."""
    end = _dhaka_today()
    earliest = end - timedelta(days=DSE_HISTORY_DAYS)
    carry_on = date.fromisoformat(
        start_date_from_partitions(parquet_path, min_date))
    if carry_on > date.fromisoformat(min_date):
        carry_on -= timedelta(days=DSE_REVISIT_DAYS)
    elif carry_on < earliest:
        # A first run asks for what the archive serves, not for what it would
        # answer with nothing, which is a thousand empty requests from 2000.
        carry_on = earliest
    return max(carry_on, earliest), end


def _coerce(frame, schema):
    """A frame whose columns hold what the schema says they hold.

    The service writes a count as a number and a missing count as nothing, and
    pandas reads that column as floats; Spark then refuses it as a long.
    """
    frame = frame.copy()
    for field in schema.fields:
        column = frame[field.name]
        if isinstance(field.dataType, LongType):
            frame[field.name] = pd.to_numeric(column, errors="coerce") \
                                  .fillna(0).astype("int64")
        elif isinstance(field.dataType, DoubleType):
            frame[field.name] = pd.to_numeric(column, errors="coerce").astype(float)
        elif isinstance(field.dataType, StringType):
            frame[field.name] = column.astype(object).where(column.notna(), None)
    return frame[schema.fieldNames()]


def _append(spark, frame, schema, parquet_path, end):
    """Write one download into its layer as a partition of its own."""
    frame = _coerce(frame, schema)
    frame["LASTDT"] = end.isoformat()
    # A copy of the schema, since StructType.add changes the one it is called on.
    written = StructType(schema.fields + [StructField("LASTDT", StringType(), False)])
    rebalanced(spark.createDataFrame(frame, schema=written), "LASTDT") \
         .write.partitionBy("LASTDT").mode("append").parquet(parquet_path)


def update_bronze_dse(spark, config_path, min_date="2000-01-01"):
    """Download the DSE day-end prices, market summary and news into Bronze.

    The market summary is collected first, because the days it holds are the
    days the market traded and every one of them has to come back with prices.
    The prices are asked for one security at a time, and the news on every
    calendar day, since companies file on days the market is closed.

    The three layers are written as one download or not at all: a day-end
    layer that ran ahead of its news would have prices for a bonus issue whose
    declaration the next run never goes back for.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. The
        archive serves two years, so an earlier date is the same as those two
        years; it only applies to the first run.

    Examples
    --------
    >>> update_bronze_dse(spark, "path/to/configuration/file")
    """
    import requests

    start, end = _range_to_collect(path_bronze_dse_dayend(config_path), min_date)
    if start >= end:
        logger.info(
            "The Bronze layer is current, nothing to download",
            extra=context(module="marketpricedata", layer="bronze", source="DSE",
                          start=start, end=end))
        return

    with requests.Session() as session:
        summary = fetch_dse_summary(session, start, end)
        trading_days = sorted(set(summary["REFDT"]))
        dayend = [fetch_dse_dayend(session, code, start, end)
                  for code in fetch_dse_instruments(session)] \
            if trading_days else []
        news = [fetch_dse_news(session, day) for day in _days(start, end)]

    dayend = [frame for frame in dayend if not frame.empty]
    dayend = pd.concat(dayend, ignore_index=True) if dayend else \
        pd.DataFrame(columns=dse_dayend_schema.fieldNames())
    news = pd.concat(news, ignore_index=True).drop_duplicates("ID")

    if summary.empty or dayend.empty:
        # No market day between the start and today is a holiday, not a failed
        # download -- Eid closes the exchange for a week -- so it is logged and
        # the partitions stay where they were for the next run to try again.
        logger.warning(
            "The DSE archive returned no trading day between %s and %s",
            start, end,
            extra=context(module="marketpricedata", layer="bronze", source="DSE"))
        return

    empty = set(trading_days) - set(dayend["REFDT"])
    if empty:
        raise RuntimeError(
            "The DSE archive returned a market summary but no prices for "
            f"{', '.join(sorted(d.isoformat() for d in empty))}. Nothing is "
            "written, so the next run asks for the same days again."
        )

    _append(spark, summary, dse_summary_schema, path_bronze_dse_summary(config_path), end)
    _append(spark, news, dse_news_schema, path_bronze_dse_news(config_path), end)
    # Last, because its partitions are what the next run carries on from.
    _append(spark, dayend, dse_dayend_schema, path_bronze_dse_dayend(config_path), end)
