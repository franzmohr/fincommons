"""Market price data: the New York Fed rates in the Silver layer.

The Bronze layer holds the rate together with the distribution it was drawn from
and the flag saying whether it has been revised. The Silver table has three
columns and takes the rate; the rest stays in the Bronze layer, where a reader
who needs the volume behind a print or wants to know whether a figure was
republished can still find it.
"""

import pyspark.sql.functions as func

from .common import *


def update_silver_nyfed(spark, config_path):
    """Import the New York Fed data sets into the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_nyfed(spark, "path/to/configuration/file")
    """
    # SOFR
    update_silver_nyfed_sofr(spark, config_path)


def update_silver_nyfed_sofr(spark, config_path):
    """Import SOFR into the Silver layer.

    The rate carries the identifier ``ON-SOFR`` whichever source it is read
    from, for the reason SONIA carries ``ON-SONA``: an identifier names the
    rate and not the publisher.

    The administrator and FRED's mirror, compared date by date, agree on every
    one of the 2,112 dates they share, so reading the administrator adds the
    first observation the mirror is missing and alters nothing the mirror
    carries.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_nyfed_sofr(spark, "path/to/configuration/file")
    """

    bronze = read_bronze(spark, path_bronze_nyfed_sofr(config_path), "NYFED")
    if bronze is not None:

        bronze = bronze \
            .withColumn("INSTRID", func.lit("ON-SOFR")) \
            .withColumnRenamed("date", "REFDT") \
            .withColumnRenamed("sofr", "VALUE") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)
