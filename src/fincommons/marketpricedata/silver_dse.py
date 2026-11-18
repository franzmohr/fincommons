"""Market price data: the Dhaka Stock Exchange in the Silver layer.

A DSE share reaches the price table as ``DSE-`` followed by its trading code,
``DSE-GP`` for Grameenphone. The code is how the exchange names a security and
the only name its archive gives; the exchange publishes no ISIN beside it, and
an ISIN written here could not be checked against anything, which is the reason
the stock indices of :func:`fincommons.marketpricedata.load_list_of_yahoo_indices`
are named rather than given one. The two DSE indices are ``IX-DSEX`` and
``IX-DS30`` for the same reason.

The value is the close of a day the share traded. A day it did not -- a record
date, a suspension, a share nobody dealt in -- is reported by the archive with
the last close carried and is left out here, as a day a Yahoo ticker did not
trade is left out, so that a reader can tell a price from the absence of one.

The closes are not adjusted for corporate actions, like every other price in
the table. The events they would be adjusted for are a table of their own,
:func:`update_silver_dse_corporate_actions`, because which of them a reader
applies depends on whether the question is about the price or the return.
"""

import logging

import pyspark.sql.functions as func
from pyspark.sql.types import DateType, DoubleType, StringType, StructType
from pyspark.sql.window import Window

from ..addressing import write_delta
from ..log import context
from .common import *
from .dse_events import corporate_actions
from .silver_yahoo import deliveries_to_merge

logger = logging.getLogger(__name__)

#: The prefix a DSE trading code is given in the price table.
DSE_PREFIX = "DSE-"

#: What a column of the corporate action frame that
#: :func:`~fincommons.marketpricedata.dse_events.corporate_actions` builds is
#: called in the Silver table. That function reads and writes the Bronze shape
#: of the DSE deliveries, and this is where its result crosses over.
CORPORATE_ACTION_SILVER_NAMES = {
    "EXDATE": "ex_date", "KIND": "kind", "FACTOR": "factor",
    "RECORD": "record_date", "DETAIL": "detail", "SOURCE": "source",
    "EXDATE_FROM": "ex_date_from",
}

dse_corporate_actions_schema = StructType() \
    .add("instr_id", StringType(), False) \
    .add("ex_date", DateType(), False) \
    .add("kind", StringType(), False) \
    .add("factor", DoubleType(), False) \
    .add("record_date", DateType(), True) \
    .add("detail", StringType(), True) \
    .add("source", StringType(), True) \
    .add("ex_date_from", StringType(), True)


def _latest_delivery(bronze, keys):
    """Each row once, as the most recent download delivered it."""
    downloads = Window.partitionBy(*keys).orderBy(func.col("last_date").desc())
    return bronze \
        .withColumn("_DELIVERY", func.row_number().over(downloads)) \
        .filter(func.col("_DELIVERY") == 1) \
        .drop("_DELIVERY")


def update_silver_dse(spark, config_path, full=False):
    """Import the DSE closes and the DSEX and DS30 indices into the Silver layer.

    Only the downloads that have not been merged yet are read; see
    :func:`fincommons.marketpricedata.deliveries_to_merge`.

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
    >>> update_silver_dse(spark, "path/to/configuration/file")
    """
    dayend, watermark = deliveries_to_merge(
        spark, path_bronze_dse_dayend(config_path), config_path, "DSE", full)
    if dayend is None:
        logger.info(
            "Nothing new in the Bronze layer to merge",
            extra=context(module="marketpricedata", layer="silver",
                          source="DSE"))
    else:
        closes = _latest_delivery(dayend, ["CODE", "ref_date"]) \
            .filter((func.col("TRADES") > 0) & (func.col("CLOSE") > 0)) \
            .select("ref_date",
                    func.concat(func.lit(DSE_PREFIX), func.col("CODE"))
                        .alias("instr_id"),
                    func.col("CLOSE").alias("value"))
        standard_upsert(spark, config_path, closes)
        write_watermark(config_path, "DSE", watermark)

    summary, watermark = deliveries_to_merge(
        spark, path_bronze_dse_summary(config_path), config_path,
        "DSE-INDICES", full)
    if summary is None:
        logger.info(
            "Nothing new in the Bronze layer to merge",
            extra=context(module="marketpricedata", layer="silver",
                          source="DSE-INDICES"))
        return

    summary = _latest_delivery(summary, ["ref_date"])
    indices = summary.select("ref_date", func.lit("IX-DSEX").alias("instr_id"),
                             func.col("DSEX").alias("value")) \
        .unionByName(summary.select("ref_date", func.lit("IX-DS30").alias("instr_id"),
                                    func.col("DS30").alias("value"))) \
        .filter(func.col("value") > 0)
    standard_upsert(spark, config_path, indices)
    write_watermark(config_path, "DSE-INDICES", watermark)


def update_silver_dse_corporate_actions(spark, config_path):
    """Rebuild the table of DSE corporate actions from the Bronze layer.

    The table is rebuilt whole every run rather than merged into. It is small
    -- a few hundred events a year -- and an event can only be placed once the
    notices on both sides of it have arrived, the declaration before its record
    date and the resumption after it, so an event that was pending in one run
    is complete in a later one without any single download bringing it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_dse_corporate_actions(spark, "path/to/configuration/file")
    """
    news = read_bronze(spark, path_bronze_dse_news(config_path),
                       "DSE-CORPORATE-ACTIONS")
    dayend = read_bronze(spark, path_bronze_dse_dayend(config_path),
                         "DSE-CORPORATE-ACTIONS")
    if news is None or dayend is None:
        return

    news = _latest_delivery(news, ["ID"]) \
        .select("ID", "CODE", "FILEDAT", "SUMMARY", "BODY").toPandas()
    # ``corporate_actions`` reads the Bronze shape of the DSE deliveries, so
    # it is given the Bronze names and its result is read into the Silver ones.
    prices = _latest_delivery(dayend, ["CODE", "ref_date"]) \
        .select(func.col("ref_date").alias("REFDT"), "CODE", "CLOSE",
                "TRADES").toPandas()

    events = corporate_actions(news, prices)
    events.insert(0, "instr_id", DSE_PREFIX + events.pop("CODE").astype(str))
    events = events.rename(columns=CORPORATE_ACTION_SILVER_NAMES)
    frame = spark.createDataFrame(
        events[dse_corporate_actions_schema.fieldNames()],
        schema=dse_corporate_actions_schema)
    write_delta(frame, ref_silver_dse_corporate_actions(config_path),
                options={"overwriteSchema": "true"})
    logger.info(
        "Rebuilt the table of DSE corporate actions",
        extra=context(module="marketpricedata", layer="silver",
                      source="DSE-CORPORATE-ACTIONS", rows=len(events)))
