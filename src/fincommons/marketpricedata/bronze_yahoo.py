"""Market price data: the closing prices published by Yahoo Finance.

The instruments to be downloaded are named by the mapping table shipped with
the package, which pairs an ISIN with the ticker Yahoo knows it under. The
Bronze layer is partitioned by the date of the download rather than overwritten,
because the source is queried for a date range and not for the whole history:
the partitions already present are what tells the next run where to carry on
from, which is what :func:`get_start_date` reads.

**The prices are the closes the instruments traded at.** Yahoo Finance adjusts
its history by default, and it adjusts it for two things: every dividend since
a date lowers the close shown for that date, and every split since it divides
the close by the split ratio. Neither is a price anyone dealt at -- an Allianz
share that closed at 156 euro on the 4th of January 2016 is shown at 92 once a
decade of dividends has been taken off it -- and both keep moving as new
dividends and splits come in, so a history collected in pieces would be adjusted
to a different day in every piece. :func:`traded_closes` takes the history
without the dividend adjustment and undoes the splits.
"""

from datetime import date, timedelta
from importlib import resources
import logging
import os
import time

import pandas as pd
from pyspark.sql.functions import to_date

from ..layout import rebalanced
from ..log import context
from .common import *

logger = logging.getLogger(__name__)


#: The first date a Yahoo Finance source is asked for where its Bronze layer
#: holds nothing yet.
#:
#: It is a default rather than a required argument so that a scheduled run needs
#: to say nothing at all: the value only ever applies to the first run of a
#: source, because every run after it carries on from the partitions. Yahoo
#: answers a request that begins before an instrument existed with whatever it
#: has, so a ticker listed later simply comes back from its own first day.
YAHOO_FIRST_DATE = "2000-01-01"

#: How many days before the end of the last download a run asks for again.
#:
#: Yahoo Finance does not fail a download when it cannot answer for some of the
#: tickers -- when it is rate limiting, typically -- but leaves those tickers
#: out and returns the rest. A run that carried on exactly where the last one
#: stopped would never go back for them, so one bad morning would leave a gap
#: in those series for good. Asking for the last fortnight again closes such a
#: gap on the next run that succeeds, and a fortnight is longer than any market
#: closure, so a ticker that brings back nothing over it is a ticker that
#: failed. The days asked for again are written into the new partition, and
#: the Silver stage keeps the newest delivery of each day.
YAHOO_REVISIT_DAYS = 14

#: The first date a ticker is asked for when it is added to a Bronze layer that
#: already holds a history.
#:
#: A run carries on from the partitions of its layer, which are shared by every
#: ticker in it, so a row added to the mapping table after the first run would
#: otherwise be asked for the last :data:`YAHOO_REVISIT_DAYS` days and never for
#: anything before them. A ticker the layer holds no price for is therefore
#: asked for from this date instead. It is later than :data:`YAHOO_FIRST_DATE`
#: on purpose: the mapping table was extended by some six thousand European
#: listings at once, and a quarter of a century of each would make that run the
#: largest the source ever makes.
YAHOO_NEW_TICKER_FIRST_DATE = "2020-01-01"

#: How many tickers one request to Yahoo Finance asks for. The source answers
#: a request for thousands of tickers slowly and in part, and a request that
#: fails costs only its own batch.
YAHOO_BATCH_SIZE = 200

#: How long a run waits between two batches, in seconds. Yahoo throttles a
#: client that asks too quickly, and a throttled request is not refused but
#: answered with no prices, which reads as a batch of tickers that did not trade.
YAHOO_BATCH_PAUSE = 5


def _yfinance():
    """The ``yfinance`` module, imported where it is used.

    Only this source needs yfinance, and the rest of the module does not: the
    ECB, the FRED and the internal sources all run without it. An import at the
    top of this file would nevertheless make yfinance a condition of importing
    ``fincommons.marketpricedata`` at all, because the package imports this
    module. It is therefore imported by the one function that downloads from
    Yahoo, and its absence
    is reported in terms of what the caller was doing rather than as a bare
    ``ModuleNotFoundError`` from an import line they never wrote.
    """
    try:
        import yfinance
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "Downloading from Yahoo Finance needs yfinance, which is not "
            "installed. Install it with the 'marketprice' extra of the "
            "package, pip install 'fincommons[marketprice]', or on its own."
        ) from exc
    return yfinance


def load_list_of_yahoo_tickers():
    """Read the ISIN to Yahoo ticker mapping shipped with the package.

    The data directory is located relative to this module rather than through a
    hard coded package name, as in :mod:`fincommons.schemas`, so that the
    package works whether it is imported as ``fincommons`` or, from the
    project root, as ``src.fincommons``.
    """
    root = __package__.rpartition(".")[0]
    with resources.as_file(
        resources.files(root).joinpath("data", "isin_yahoo.csv")
    ) as f:
        file = f
    return pd.read_csv(file)


def load_list_of_yahoo_commodities():
    """Read the commodity to Yahoo ticker mapping shipped with the package.

    A commodity future has no ISIN, so this table pairs the identifier the
    commodity reaches the Silver table under with the ticker it is downloaded
    by, where the instrument table pairs an ISIN with a ticker.
    """
    root = __package__.rpartition(".")[0]
    with resources.as_file(
        resources.files(root).joinpath("data", "commodity_yahoo.csv")
    ) as f:
        file = f
    return pd.read_csv(file)


def start_date_from_partitions(path, min_date):
    """The first date a partitioned Bronze layer still has to be queried for.

    See :func:`get_start_date`, which is this function pointed at the listed
    instruments. It is separate because the commodities are downloaded into a
    Bronze layer of their own and carry on from their own partitions.
    """
    if not os.path.isdir(path):
        return min_date

    files_in_bronze = [k for k in os.listdir(path) if 'LASTDT' in k]
    if len(files_in_bronze) == 0:
        start_date = min_date
    else:
        start_date = max(files_in_bronze).replace("LASTDT=", "")
    return start_date


def revisit_start_date(path, carry_on, min_date):
    """The first date a download asks for, the revisited days included.

    Where the Bronze layer holds a download already, that is
    :data:`YAHOO_REVISIT_DAYS` before the day it stopped at, ``carry_on``, but
    never before ``min_date``; where it holds none, ``carry_on`` is the start
    of the history and there is nothing to revisit.
    """
    if not os.path.isdir(path) or \
            not any("LASTDT" in k for k in os.listdir(path)):
        return carry_on

    revisit = str(date.fromisoformat(carry_on)
                  - timedelta(days=YAHOO_REVISIT_DAYS))
    return max(revisit, min_date)


def traded_closes(answer):
    """The closes an answer of ``yfinance.download`` says were traded.

    The answer has to be asked for with ``auto_adjust=False``, which leaves the
    dividends out of the closes, and ``actions=True``, which brings the splits
    with it. What remains is the split adjustment: Yahoo divides every close
    before a split by its ratio, so that NVIDIA, which traded at about 1,200
    dollars the week before its ten-for-one split in June 2024, is shown at
    about 120. Multiplying each close by the product of the splits that took
    effect after its date undoes that, and it needs only the splits within the
    answer, because a split before the first date asked for did not change any
    close the answer holds.

    Returns a frame of closes with one column per ticker, indexed by date.
    """
    if answer is None or answer.empty or \
            "Close" not in answer.columns.get_level_values(0):
        return pd.DataFrame()

    answer = answer.sort_index()
    closes = answer["Close"]

    if "Stock Splits" in answer.columns.get_level_values(0):
        # A day without a split carries a ratio of 0 rather than 1.
        splits = answer["Stock Splits"].reindex(columns=closes.columns) \
                                       .fillna(0.0)
        splits = splits.where(splits > 0, 1.0)
        # For each day, the product of the ratios of the days after it.
        later = splits.iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0)
        closes = closes * later

    closes.index.name = "Date"
    closes.columns.name = "Ticker"
    return closes


def download_yahoo_prices(spark, mapping, identifier, parquet_path, min_date):
    """Download the closing prices of a set of tickers into a Bronze layer.

    The listed instruments and the commodities are downloaded the same way and
    differ only in what names them: a table pairing an ISIN with a ticker in the
    one case and a commodity identifier with a ticker in the other. The download
    is therefore written once and told which column carries the identifier.

    The closes are the traded ones, see :func:`traded_closes`, and the last
    :data:`YAHOO_REVISIT_DAYS` days of what is held are asked for again. A
    ticker that comes back with nothing is named in a warning and the others
    are written; an answer with nothing for any ticker is raised, because it is
    a failed download and not a quiet day.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    mapping: pandas.DataFrame
        A table with a ``yahoo`` column of tickers and one further column naming
        what each ticker is.
    identifier: str
        Name of that further column.
    parquet_path: str
        Path of the partitioned parquet data set that should be appended to.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to the first run.
    """
    carry_on = start_date_from_partitions(parquet_path, min_date)
    end_date = str(date.today())
    start_date = revisit_start_date(parquet_path, carry_on, min_date)

    # A ticker the layer has never delivered a price for is asked for its own
    # history rather than for the days the others are missing -- also on a
    # day the others have nothing left to fetch, or a row added after the
    # day's run would wait for tomorrow's.
    held = held_identifiers(spark, parquet_path, identifier)
    new = set(mapping.loc[~mapping[identifier].isin(held), "yahoo"]) \
        if held else set()
    current = carry_on >= end_date
    if current and not new:
        logger.info(
            "The Bronze layer is current, nothing to download",
            extra=context(module="marketpricedata", layer="bronze",
                          source=os.path.basename(parquet_path.rstrip("/\\")),
                          end=end_date))
        return

    tickers = sorted(set(mapping["yahoo"]) & new) if current \
        else sorted(set(mapping["yahoo"]))
    starts = new_ticker_starts(mapping, min_date, start_date)
    wanted = [(t, starts[t] if t in new else start_date) for t in tickers]
    if new:
        logger.info(
            "%d tickers are new to the layer and are asked for their own "
            "history, from %s", len(new), min(starts[t] for t in new),
            extra=context(module="marketpricedata", layer="bronze",
                          source=os.path.basename(parquet_path.rstrip("/\\"))))

    frames = []
    for number, (first, batch) in enumerate(_batches(wanted)):
        if number:
            time.sleep(YAHOO_BATCH_PAUSE)
        # The query runs to the day after the run, one day further than the
        # range that is kept, so that a split taking effect on the day of the
        # run is among the actions the answer carries: Yahoo has already
        # divided the closes before it by the ratio, and they could not be put
        # right without it.
        answer = _yfinance().download(
            batch, start = first,
            end = str(date.today() + timedelta(days=1)),
            auto_adjust = False, actions = True, progress = False)
        closes = traded_closes(answer)
        if not closes.empty:
            frames.append(closes[closes.index < pd.Timestamp(end_date)])

    yahoo_data = pd.concat(
        [pd.melt(c, ignore_index=False).dropna().reset_index() for c in frames],
        ignore_index=True) if frames else pd.DataFrame()
    yahoo_data = yahoo_data.rename(columns = {"Date": "REFDT",
                                              "Ticker": "yahoo",
                                              "value": "VALUE"})

    returned = set(yahoo_data["yahoo"]) if "yahoo" in yahoo_data else set()
    if not returned:
        raise RuntimeError(
            f"Yahoo Finance returned no price for any of the {len(tickers)} "
            f"tickers between {start_date} and {end_date}. Nothing is written, "
            "so the next run asks for the same days again."
        )
    missing = [t for t in tickers if t not in returned]
    if missing:
        # Written down rather than raised: the others are worth keeping, and
        # the next run asks for these days again. A ticker named here run
        # after run has stopped trading or been renamed at the source.
        logger.warning(
            "Yahoo Finance returned no price for %d of %d tickers: %s",
            len(missing), len(tickers), ", ".join(missing),
            extra=context(module="marketpricedata", layer="bronze",
                          source=os.path.basename(parquet_path.rstrip("/\\"))),
        )

    result = yahoo_data.merge(mapping, on = "yahoo", how = "left")[
        ["REFDT", identifier, "VALUE"]]
    result.loc[:, "LASTDT"] = end_date
    rebalanced(spark.createDataFrame(result)
               .withColumn("REFDT", to_date("REFDT")), "LASTDT") \
        .write.partitionBy("LASTDT").mode("append") \
        .parquet(parquet_path)


def new_ticker_starts(mapping, min_date, start_date):
    """The first date each ticker is asked for if the layer holds none of it.

    That is :data:`YAHOO_NEW_TICKER_FIRST_DATE`, unless the mapping table
    gives the row a ``first_date`` of its own: a small market added whole,
    such as the Ho Chi Minh City exchange, is worth its full history, where
    thousands of European listings added at once are not. It is never
    before ``min_date`` and never after the date the others carry on from.
    """
    default = max(YAHOO_NEW_TICKER_FIRST_DATE, min_date)
    own = mapping["first_date"] if "first_date" in mapping else \
        pd.Series(index=mapping.index, dtype=object)
    starts = {}
    for ticker, first in zip(mapping["yahoo"], own):
        wanted = max(str(first), min_date) if isinstance(first, str) and first else default
        starts[ticker] = min(wanted, start_date)
    return starts


def held_identifiers(spark, parquet_path, identifier):
    """The identifiers a Bronze layer holds at least one price for."""
    if not os.path.isdir(parquet_path) or \
            not any("LASTDT" in k for k in os.listdir(parquet_path)):
        return set()
    held = spark.read.parquet(parquet_path).select(identifier).distinct().collect()
    return {row[identifier] for row in held}


def _batches(wanted):
    """Group ``(ticker, start)`` pairs into downloads of one start each.

    Yields ``(start, tickers)`` with at most :data:`YAHOO_BATCH_SIZE` tickers.
    """
    by_start = {}
    for ticker, start in wanted:
        by_start.setdefault(start, []).append(ticker)
    for start in sorted(by_start):
        tickers = by_start[start]
        for k in range(0, len(tickers), YAHOO_BATCH_SIZE):
            yield start, tickers[k:k + YAHOO_BATCH_SIZE]


def get_start_date(config_path, min_date):
    """The first date the source still has to be queried for.

    A download queries the half-open range ``[start_date, end_date)`` -- the end
    of a Yahoo Finance query is exclusive -- and records ``end_date``, the day it
    was run on, as its partition. A partition is therefore named after the first
    day its download does *not* hold rather than the last day it does, and the
    next run carries on from that day itself.

    The distinction is the whole of this function. The name ``LASTDT`` invites
    the reading that the partition is the last date collected, and a run that
    starts a day after it skips exactly one reference date every time, quietly
    and for good: the range of the next run begins a day beyond where the last
    one stopped, and nothing ever goes back for what fell between them.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        is returned where the Bronze layer holds nothing yet.
    """
    return start_date_from_partitions(
        path_bronze_yahoo_marketpricedata(config_path), min_date)


def update_bronze_yahoo(spark, config_path, min_date=YAHOO_FIRST_DATE):
    """Download closing prices into the Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to the first run, see :func:`get_start_date`.

    Examples
    --------
    >>> update_bronze_yahoo(spark, "path/to/configuration/file", "2024-01-01")
    """
    download_yahoo_prices(spark,
                          load_list_of_yahoo_tickers(),
                          "ISIN",
                          path_bronze_yahoo_marketpricedata(config_path),
                          min_date)


def update_bronze_yahoo_commodities(spark, config_path, min_date=YAHOO_FIRST_DATE):
    """Download commodity closing prices into the Bronze layer.

    These are the energy and industrial commodities, as the front month future
    of each. Gold and silver are not among them: they are benchmarks with a body
    that sets them and are taken from the LBMA, see
    :mod:`fincommons.marketpricedata.bronze_lbma`.

    A front month future is not a spot price. It is the price of the nearest
    contract still trading, so the series steps whenever one contract expires
    and the next becomes the front month, and the step is not a movement in the
    market. It is the closest thing to a daily commodity price that can be read
    without a licence, which is why it is what the module collects.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to the first run.

    Examples
    --------
    >>> update_bronze_yahoo_commodities(spark, "path/to/configuration/file", "2024-01-01")
    """
    download_yahoo_prices(spark,
                          load_list_of_yahoo_commodities(),
                          "INSTRID",
                          path_bronze_yahoo_commodities(config_path),
                          min_date)


def load_list_of_yahoo_volatility():
    """Read the volatility index to ticker mapping shipped with the package."""
    root = __package__.rpartition(".")[0]
    with resources.as_file(
        resources.files(root).joinpath("data", "volatility_yahoo.csv")
    ) as f:
        file = f
    return pd.read_csv(file)


def load_list_of_yahoo_indices():
    """Read the stock index to ticker mapping shipped with the package.

    This is the second place indices are named. Ten of them travel in the
    instrument table under their published index ISIN; these are named directly,
    because an index is not admitted to trading and FIRDS holds no ISIN for one,
    so an index ISIN written here could not be checked against anything.
    Inventing identifiers that cannot be verified is the thing a reference table
    must not do, so the ones that have no well-known ISIN are named instead.
    """
    root = __package__.rpartition(".")[0]
    with resources.as_file(
        resources.files(root).joinpath("data", "index_yahoo.csv")
    ) as f:
        file = f
    return pd.read_csv(file)


def update_bronze_yahoo_volatility(spark, config_path, min_date=YAHOO_FIRST_DATE):
    """Download the volatility indices into the Bronze layer.

    These are implied volatilities read off option prices -- what the market
    pays for protection rather than what a price has done -- so they are a
    forward looking number and not a realised one.

    They are US indices. There is no European equivalent the module can collect:
    VSTOXX is STOXX's and is not served by the source the rest of these come
    from, and neither is VDAX. That is a gap in the data rather than a choice.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to the first run.

    Examples
    --------
    >>> update_bronze_yahoo_volatility(spark, "path/to/configuration/file", "2024-01-01")
    """
    download_yahoo_prices(spark,
                          load_list_of_yahoo_volatility(),
                          "INSTRID",
                          path_bronze_yahoo_volatility(config_path),
                          min_date)


def update_bronze_yahoo_indices(spark, config_path, min_date=YAHOO_FIRST_DATE):
    """Download the stock indices named by ticker into the Bronze layer.

    The ten benchmark indices of the instrument table are not downloaded again
    here: they travel there under their ISIN and are collected with the listed
    instruments.

    Central and eastern Europe is missing from this list, and not by choice. The
    source answers with nothing for the Warsaw WIG20 under any of its tickers,
    and likewise for the Budapest BUX and the Prague PX, so those markets have
    no series here at all. It is the same gap that leaves VSTOXX out of the
    volatility list: the source carries the large western and Asian indices
    well and thins out from there.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to the first run.

    Examples
    --------
    >>> update_bronze_yahoo_indices(spark, "path/to/configuration/file", "2024-01-01")
    """
    download_yahoo_prices(spark,
                          load_list_of_yahoo_indices(),
                          "INSTRID",
                          path_bronze_yahoo_indices(config_path),
                          min_date)


def load_list_of_yahoo_fx():
    """Read the exchange rate to Yahoo ticker mapping shipped with the package.

    It names only the currencies the ECB does not fix a reference rate for.
    Each is named ``EUR/XXX``, units of the currency per euro, which is how the
    ECB rates are named and quoted, so a reader converts a price with either in
    the same way.
    """
    root = __package__.rpartition(".")[0]
    with resources.as_file(
        resources.files(root).joinpath("data", "fx_yahoo.csv")
    ) as f:
        file = f
    return pd.read_csv(file)


def update_bronze_yahoo_fx(spark, config_path, min_date=YAHOO_FIRST_DATE):
    """Download the exchange rates the ECB does not publish into the Bronze layer.

    The Bangladeshi taka is the reason this source exists: the DSE prices are
    quoted in it, and the ECB fixes no reference rate for it, so without one a
    Dhaka share could not be priced in euro at all. Yahoo's rate is a market
    quote at the close of its own day rather than a fixing, and it is noisier
    than an ECB rate for it -- the taka is managed, and its quotes move in
    steps where the central bank lets it move.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to the first run.

    Examples
    --------
    >>> update_bronze_yahoo_fx(spark, "path/to/configuration/file")
    """
    download_yahoo_prices(spark,
                          load_list_of_yahoo_fx(),
                          "INSTRID",
                          path_bronze_yahoo_fx(config_path),
                          min_date)
