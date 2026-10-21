"""Market price data: the Hanoi Stock Exchange in the Silver layer.

A Hanoi share reaches the price table under its ISIN, which VNDirect's
listings give beside every trading code -- ``VN000000SHS3`` for Saigon-Hanoi
Securities -- so it is named the way the Ho Chi Minh City shares downloaded
from Yahoo Finance are, and a reader cannot tell from the identifier which of
the two sources it came from, only which country. The indices are
``IX-HNX``, ``IX-HNX30``, ``IX-VNINDEX`` and ``IX-VN30``, named rather than
given an ISIN for the reason the DSE ones are.

The value is the close of a day the share traded, in dong: VNDirect quotes in
thousands, and the close is multiplied by a thousand here, so that it is on
the scale of the Yahoo closes of the Ho Chi Minh City shares. A day with no
volume on the order book or in negotiated deals is left out, as a day a
Yahoo ticker did not trade is. The close is the one that traded, not the one
VNDirect adjusts for corporate actions, like every other price in the table.
"""

import logging

import pyspark.sql.functions as func
from pyspark.sql.window import Window

from ..log import context
from .bronze_vndirect import VNDIRECT_INDICES
from .common import *
from .silver_yahoo import deliveries_to_merge

logger = logging.getLogger(__name__)

#: VNDirect's prices are in thousands of dong; the table holds dong.
VNDIRECT_PRICE_UNIT = 1000.0


def _latest_delivery(bronze, keys):
    """Each row once, as the most recent download delivered it."""
    downloads = Window.partitionBy(*keys).orderBy(func.col("LASTDT").desc())
    return bronze \
        .withColumn("DELIVERY", func.row_number().over(downloads)) \
        .filter(func.col("DELIVERY") == 1) \
        .drop("DELIVERY")


def update_silver_vndirect(spark, config_path, full=False):
    """Import the Hanoi closes and the Vietnamese indices into the Silver layer.

    The ISIN of a share is read from the most recent listing that names its
    code, across every download, so that a share delisted since is still
    named by the ISIN it had. A code no listing names is left out and said so.

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
    >>> update_silver_vndirect(spark, "path/to/configuration/file")
    """
    prices, watermark = deliveries_to_merge(
        spark, path_bronze_vndirect_prices(config_path), config_path,
        "VNDIRECT", full)
    if prices is None:
        logger.info(
            "Nothing new in the Bronze layer to merge",
            extra=context(module="marketpricedata", layer="silver",
                          source="VNDIRECT"))
    else:
        listings = _latest_delivery(
            spark.read.parquet(path_bronze_vndirect_listings(config_path))
                 .filter(func.col("ISIN").isNotNull()),
            ["CODE"]).select("CODE", "ISIN")
        traded = _latest_delivery(prices, ["CODE", "REFDT"]) \
            .filter((func.coalesce(func.col("NM_VOLUME"), func.lit(0.0))
                     + func.coalesce(func.col("PT_VOLUME"), func.lit(0.0)) > 0)
                    & (func.col("CLOSE") > 0))
        named = traded.join(listings, on="CODE", how="left")
        unnamed = [row["CODE"] for row in
                   named.filter(func.col("ISIN").isNull())
                        .select("CODE").distinct().collect()]
        if unnamed:
            logger.warning(
                "No VNDirect listing gives an ISIN for %d codes, left out: %s",
                len(unnamed), ", ".join(sorted(unnamed)),
                extra=context(module="marketpricedata", layer="silver",
                              source="VNDIRECT"))
        closes = named.filter(func.col("ISIN").isNotNull()) \
            .select("REFDT", func.col("ISIN").alias("INSTRID"),
                    # Rounded, since 32.3 thousand in binary floating point
                    # comes back as 32,299.999999999996 dong.
                    func.round(func.col("CLOSE") * VNDIRECT_PRICE_UNIT, 2).alias("VALUE"))
        standard_upsert(spark, config_path, closes)
        write_watermark(config_path, "VNDIRECT", watermark)

    indices, watermark = deliveries_to_merge(
        spark, path_bronze_vndirect_indices(config_path), config_path,
        "VNDIRECT-INDICES", full)
    if indices is None:
        logger.info(
            "Nothing new in the Bronze layer to merge",
            extra=context(module="marketpricedata", layer="silver",
                          source="VNDIRECT-INDICES"))
        return

    names = func.create_map(*[x for code, name in VNDIRECT_INDICES.items()
                              for x in (func.lit(code), func.lit(name))])
    levels = _latest_delivery(indices, ["CODE", "REFDT"]) \
        .filter(func.col("CLOSE") > 0) \
        .select("REFDT", names[func.col("CODE")].alias("INSTRID"),
                func.col("CLOSE").alias("VALUE")) \
        .filter(func.col("INSTRID").isNotNull())
    standard_upsert(spark, config_path, levels)
    write_watermark(config_path, "VNDIRECT-INDICES", watermark)
