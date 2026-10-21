"""Market price data: the LBMA benchmarks in the Silver layer.

Each benchmark arrives in the Bronze layer as a reference date and a value, so
this stage only has to name the commodity the value belongs to and bring the
columns into the shape of the combined table.

The values are US dollars per troy ounce. The combined table has no column
saying what unit or currency a value is in, so that is a property of the
identifier and is written down in the module's README rather than carried
beside the number.
"""

import pyspark.sql.functions as func

from .common import *


def update_silver_lbma(spark, config_path):
    """Import the LBMA benchmarks into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_lbma(spark, "path/to/configuration/file")
    """
    # Gold
    update_silver_lbma_gold(spark, config_path)

    # Silver
    update_silver_lbma_silver(spark, config_path)


def _update_silver_lbma_series(spark, config_path, parquet_path, value_column,
                               instrid):
    """Bring one LBMA benchmark into the shape of the combined table."""

    bronze = read_bronze(spark, parquet_path, "LBMA")
    if bronze is not None:

        bronze = bronze \
            .withColumn("INSTRID", func.lit(instrid)) \
            .withColumnRenamed("date", "REFDT") \
            .withColumnRenamed(value_column, "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)


def update_silver_lbma_gold(spark, config_path):
    """Import the LBMA Gold Price into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_lbma_gold(spark, "path/to/configuration/file")
    """
    _update_silver_lbma_series(spark, config_path,
                               path_bronze_lbma_gold(config_path),
                               "gold", "CO-GOLD")


def update_silver_lbma_silver(spark, config_path):
    """Import the LBMA Silver Price into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_lbma_silver(spark, "path/to/configuration/file")
    """
    _update_silver_lbma_series(spark, config_path,
                               path_bronze_lbma_silver(config_path),
                               "silver", "CO-SILVER")
