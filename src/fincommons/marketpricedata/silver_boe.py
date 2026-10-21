"""Market price data: the Bank of England series in the Silver layer.

The series arrives in the Bronze layer as a reference date and a value, so this
stage only has to name the rate the value belongs to and bring the columns into
the shape of the combined table.
"""

import pyspark.sql.functions as func

from .common import *


def update_silver_boe(spark, config_path):
    """Import the Bank of England data sets into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_boe(spark, "path/to/configuration/file")
    """
    # SONIA
    update_silver_boe_sonia(spark, config_path)


def update_silver_boe_sonia(spark, config_path):
    """Import SONIA into the Silver layer.

    The rate carries the identifier ``ON-SONA`` whichever source it is read
    from. The identifier names the rate and not the publisher, so a
    change of source is not a change of identifier -- and renaming it would
    leave the rows collected under the present name behind in the table, since
    the upsert replaces a row but never removes one.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_boe_sonia(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_boe_sonia(config_path), "BOE")
    if bronze is not None:

        bronze = bronze \
            .withColumn("INSTRID", func.lit("ON-SONA")) \
            .withColumnRenamed("date", "REFDT") \
            .withColumnRenamed("sonia", "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)
