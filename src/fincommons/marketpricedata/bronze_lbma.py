"""Market price data: the precious metal benchmarks published by the LBMA.

The LBMA Gold Price and LBMA Silver Price are the benchmarks the market settles
against, and the London Bullion Market Association publishes them itself. They
are taken from there rather than from a data vendor for the same reason SONIA is
taken from the Bank of England: a benchmark has a body that sets it, and a
supervisory data set should be able to name the body that published each of its
figures.

The energy and industrial commodities have no such source that can be read
without a licence or a key, and they are taken from Yahoo Finance instead; see
:mod:`fincommons.marketpricedata.bronze_yahoo`.

Source: https://www.lbma.org.uk/prices-and-data/precious-metal-prices
"""

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (ArrayType, DoubleType, StringType, StructField,
                               StructType)

from .common import *
from .download import downloaded, refuse_empty_answer

#: Where the association serves the benchmark history.
LBMA_PRICES_URL = "https://prices.lbma.org.uk/json/{series}.json"

#: The shape of one entry. ``d`` is the date the benchmark was set on and ``v``
#: holds the same price in three currencies, in the order US dollar, pound
#: sterling, euro. The schema is declared rather than inferred, as everywhere
#: else in the module, so that a change in what the association publishes shows
#: up as a field that does not fit instead of as a table that has quietly
#: changed shape.
LBMA_SCHEMA = StructType([
    StructField("d", StringType(), nullable=False),
    StructField("v", ArrayType(DoubleType()), nullable=True),
])

#: Which of the three currencies is kept. The Silver table has no column saying
#: what currency a value is in, so one has to be chosen and written down: the US
#: dollar, which is the currency these benchmarks are quoted and settled in.
LBMA_CURRENCY = 0


def download_lbma_series(spark, series, value_column, parquet_path):
    """Download one LBMA benchmark and store it as parquet in the data lake.

    As with the other sources, the answer passes through a temporary directory
    on the local disk of the driver and only the parquet data set reaches the
    data lake.

    The association serves JSON rather than CSV, and one array rather than one
    object per line, so Spark is told to read the file whole. A request without
    a user agent is refused, as at the Bank of England, and an answer without
    a single price -- an empty one, or an error page served with status 200 --
    is refused before it is written over the history; see
    :func:`fincommons.marketpricedata.download.refuse_empty_answer`.

    The euro price is null before 1999, which is not a gap in the data but the
    currency not existing yet. Only the dollar price is kept, so those rows are
    not affected -- but a reader who changes :data:`LBMA_CURRENCY` should know
    that thirty years of the history would go with it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    series: str
        Name of the series at the association, e.g. ``"gold_pm"``.
    value_column: str
        Name the value is stored under in the Bronze layer.
    parquet_path: str
        Path of the parquet data set that should be written.
    """
    url = LBMA_PRICES_URL.format(series=series)

    with downloaded(url, "LBMA", file_name="download.json") as file_path:
        answer = spark.read.format("json") \
            .schema(LBMA_SCHEMA) \
            .option("multiLine", "true") \
            .load(file_path) \
            .withColumn("date", to_date(col("d"))) \
            .withColumn(value_column, col("v")[LBMA_CURRENCY]) \
            .select("date", value_column) \
            .dropna() \
            .orderBy("date")

        refuse_empty_answer(answer, "LBMA", url)

        answer.write.mode("overwrite").parquet(parquet_path)


def update_bronze_lbma(spark, config_path):
    """Import the LBMA benchmarks into the Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_lbma(spark, "path/to/configuration/file")
    """
    # Gold
    update_bronze_lbma_gold(spark, config_path)

    # Silver
    update_bronze_lbma_silver(spark, config_path)


def update_bronze_lbma_gold(spark, config_path):
    """Download the LBMA Gold Price into the Bronze layer.

    The afternoon benchmark is taken. Gold is set twice a day, in the morning
    and in the afternoon, and it is the afternoon price that is normally meant
    by "the LBMA Gold Price" and used to value a holding, because it is set
    while both London and New York are trading.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_lbma_gold(spark, "path/to/configuration/file")
    """
    download_lbma_series(spark, "gold_pm", "gold",
                         path_bronze_lbma_gold(config_path))


def update_bronze_lbma_silver(spark, config_path):
    """Download the LBMA Silver Price into the Bronze layer.

    Silver is set once a day, so there is no morning and afternoon to choose
    between as there is for gold.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_lbma_silver(spark, "path/to/configuration/file")
    """
    download_lbma_series(spark, "silver", "silver",
                         path_bronze_lbma_silver(config_path))
