"""Market price data: the electricity prices in the Silver layer.

The Bronze layer already holds one row per zone and day, so this stage only has
to take the baseload average and leave the rest of what was kept -- the lowest
and highest price of the day and the number of delivery periods behind them --
where it is.

The values are euro per megawatt hour. The combined table has no column saying
what unit a value is in, so that is a property of the identifier and is written
down in the module's README.
"""

import logging

import pyspark.sql.functions as func

from ..log import context
from .bronze_electricity import ELECTRICITY_REVISION_DAYS
from .common import *

logger = logging.getLogger(__name__)


def update_silver_electricity(spark, config_path, full=False):
    """Import the day-ahead electricity prices into the Silver layer.

    The zone is already the identifier in the Bronze layer, so nothing is
    renamed. What is taken is the average over the day, the baseload price: the
    Silver table holds one value per identifier and date, and of the four
    numbers the Bronze layer keeps it is the one that answers what electricity
    cost that day.

    A reader who needs to know how far the price moved within the day, or how
    many delivery periods a day's average was computed from, has to go to the
    Bronze layer for it. That is worth knowing here rather than there, because
    electricity moves within a day in a way nothing else in this table does: it
    can run from negative, when there is more wind than demand, to several
    hundred euro between one quarter of an hour and another.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_electricity(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_electricity_dayahead(config_path),
                         "ELECTRICITY")
    if bronze is not None:

        # Only the days the download actually revisited are merged. The Bronze
        # layer here is rewritten whole on every run, so it cannot say which of
        # its rows are new -- but the download does: it asks for the days since
        # the last one it held, less the revision window, and leaves every
        # earlier day exactly as it was. Merging those again would write the
        # values that are already in the table.
        merged = None if full else read_watermark(config_path, "ELECTRICITY")
        if merged is not None:
            first = func.date_sub(func.to_date(func.lit(merged)),
                                  ELECTRICITY_REVISION_DAYS)
            bronze = bronze.filter(func.col("date") >= first)

        bronze = bronze \
            .withColumnRenamed("date", "REFDT") \
            .withColumnRenamed("zone", "INSTRID") \
            .withColumnRenamed("price", "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        latest = bronze.agg(func.max("REFDT").alias("last")).collect()
        watermark = latest[0]["last"] if latest else None
        if watermark is None:
            logger.info(
                "Nothing new in the Bronze layer to merge",
                extra=context(module="marketpricedata", layer="silver",
                              source="ELECTRICITY"))
            return

        standard_upsert(spark, config_path, bronze)
        write_watermark(config_path, "ELECTRICITY", str(watermark))
