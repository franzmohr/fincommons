"""Market price data: the Yahoo Finance prices in the Silver layer.

The Bronze layer of this source is partitioned by the date of the download and
appended to rather than overwritten, so one reference date of one instrument can
have been delivered by more than one download. The partition is therefore not
simply dropped: it is first what decides which of those deliveries is the
current one, and only then dropped, because the combined table is keyed by the
reference date of the price and not by the day it was fetched.
"""

import logging
import os

import pyspark.sql.functions as func
from pyspark.sql.window import Window

from ..log import context
from .common import *

logger = logging.getLogger(__name__)


def deliveries_to_merge(spark, parquet_path, config_path, source, full=False):
    """The Bronze rows of a Yahoo source that have not been merged yet.

    A Bronze layer here is partitioned by ``LASTDT``, the day the download ran,
    and is appended to rather than rewritten: a partition, once written, never
    changes again. Merging one a second time therefore writes the values that
    are already in the Silver table, which is correct and is most of the work
    -- the layer holds every price ever downloaded, and a morning's run adds one
    day to it.

    Only the partitions after the mark of the last successful merge are read.
    Where no mark has been written the whole layer is read, which is what makes
    the first run after this was introduced put the table in the state the mark
    then claims.

    Returns ``(frame, watermark)``, or ``(None, None)`` where there is nothing
    to do. The watermark is returned rather than written here, because it must
    not be recorded until the merge it describes has succeeded.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    parquet_path: str
        Path of the partitioned Bronze layer.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    source: str
        Name the mark is recorded under.
    full: bool
        Read every partition regardless of the mark. It is how a Silver table
        is rebuilt from a Bronze layer that is still intact, for instance after
        the identifiers a source writes have changed.
    """
    bronze = read_bronze(spark, parquet_path, source)
    if bronze is None:
        return None, None

    merged = None if full else read_watermark(config_path, source)
    if merged is not None:
        # The mark is text and the partition column is read back as a date, so
        # the comparison is made between two dates rather than left to a cast.
        # The partition the mark names is read again only if it has grown
        # since it was merged -- a second download on the same day, see
        # Delivery -- which a mark without a count never says.
        seen = read_watermark_files(config_path, source)
        grown = seen is not None and \
            partition_files(parquet_path, merged) > seen
        cut = func.to_date(func.lit(merged))
        bronze = bronze.filter(func.col("LASTDT") >= cut if grown
                               else func.col("LASTDT") > cut)

    deliveries = [row["LASTDT"] for row in
                  bronze.select("LASTDT").distinct().collect()]
    if not deliveries:
        return None, None

    latest = max(deliveries)
    return bronze, Delivery(latest, partition_files(parquet_path, latest))


def partition_files(parquet_path, lastdt):
    """How many data files one ``LASTDT`` partition of a Bronze layer holds."""
    folder = os.path.join(parquet_path, f"LASTDT={lastdt}")
    if not os.path.isdir(folder):
        return 0
    return sum(1 for name in os.listdir(folder) if name.endswith(".parquet"))


def update_silver_yahoo(spark, config_path, full=False):
    """Import Yahoo Finance closing prices into the Silver layer.

    The instrument is identified by its ISIN, which is what the mapping table
    of :func:`load_list_of_yahoo_tickers` pairs with the ticker the price was
    downloaded under. The ticker itself does not reach the Silver table: it
    names the instrument at one venue and the ISIN names the instrument.

    Only the downloads that have not been merged yet are read; see
    :func:`deliveries_to_merge`.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Merge every download again rather than only the new ones.

    Examples
    --------
    >>> update_silver_yahoo(spark, "path/to/configuration/file")
    """
    bronze, watermark = deliveries_to_merge(
        spark, path_bronze_yahoo_marketpricedata(config_path), config_path,
        "YAHOO", full)

    if bronze is None:
        logger.info(
            "Nothing new in the Bronze layer to merge",
            extra=context(module="marketpricedata", layer="silver",
                          source="YAHOO"))
        return

    # A reference date that has been downloaded more than once is held once
    # per download, and the most recent download is the one that is kept.
    # The merge in standard_upsert is what makes this necessary rather than
    # merely tidy: it matches on the instrument and the reference date, and
    # a merge whose source holds the same pair twice fails outright instead
    # of choosing between them.
    downloads = Window.partitionBy("ISIN", "REFDT") \
                      .orderBy(func.col("LASTDT").desc())

    bronze = bronze \
        .withColumn("DELIVERY", func.row_number().over(downloads)) \
        .filter(func.col("DELIVERY") == 1) \
        .withColumnRenamed("ISIN", "INSTRID") \
        .select("REFDT", "INSTRID", "VALUE")

    standard_upsert(spark, config_path, bronze)
    write_watermark(config_path, "YAHOO", watermark)


def _update_silver_named_yahoo(spark, config_path, parquet_path, source,
                               full=False):
    """Bring one Bronze layer of already-named Yahoo prices into the table.

    The commodities, the volatility indices and the stock indices named by
    ticker are all downloaded the same way and all carry their identifier
    already, where the listed instruments carry an ISIN that becomes one. They
    therefore differ only in which Bronze layer they are read from, and the
    reduction to the most recent delivery of each day is written once.
    """
    bronze, watermark = deliveries_to_merge(spark, parquet_path, config_path,
                                            source, full)

    if bronze is None:
        logger.info(
            "Nothing new in the Bronze layer to merge",
            extra=context(module="marketpricedata", layer="silver",
                          source=source))
        return

    # As for the instruments: the Bronze layer is appended to one download
    # at a time, so a reference date delivered twice is held twice, and the
    # merge fails on a source that carries the same key twice.
    downloads = Window.partitionBy("INSTRID", "REFDT") \
                      .orderBy(func.col("LASTDT").desc())

    bronze = bronze \
        .withColumn("DELIVERY", func.row_number().over(downloads)) \
        .filter(func.col("DELIVERY") == 1) \
        .select("REFDT", "INSTRID", "VALUE")

    standard_upsert(spark, config_path, bronze)
    write_watermark(config_path, source, watermark)


def update_silver_yahoo_commodities(spark, config_path, full=False):
    """Import the Yahoo Finance commodity prices into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Merge every download again rather than only the new ones.

    Examples
    --------
    >>> update_silver_yahoo_commodities(spark, "path/to/configuration/file")
    """
    _update_silver_named_yahoo(spark, config_path,
                               path_bronze_yahoo_commodities(config_path),
                               "YAHOO-COMMODITIES", full)


def update_silver_yahoo_volatility(spark, config_path, full=False):
    """Import the Yahoo Finance volatility indices into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Merge every download again rather than only the new ones.

    Examples
    --------
    >>> update_silver_yahoo_volatility(spark, "path/to/configuration/file")
    """
    _update_silver_named_yahoo(spark, config_path,
                               path_bronze_yahoo_volatility(config_path),
                               "YAHOO-VOLATILITY", full)


def update_silver_yahoo_indices(spark, config_path, full=False):
    """Import the Yahoo Finance stock indices into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Merge every download again rather than only the new ones.

    Examples
    --------
    >>> update_silver_yahoo_indices(spark, "path/to/configuration/file")
    """
    _update_silver_named_yahoo(spark, config_path,
                               path_bronze_yahoo_indices(config_path),
                               "YAHOO-INDICES", full)


def update_silver_yahoo_fx(spark, config_path, full=False):
    """Import the Yahoo Finance exchange rates into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Merge every download again rather than only the new ones.

    Examples
    --------
    >>> update_silver_yahoo_fx(spark, "path/to/configuration/file")
    """
    _update_silver_named_yahoo(spark, config_path,
                               path_bronze_yahoo_fx(config_path),
                               "YAHOO-FX", full)
