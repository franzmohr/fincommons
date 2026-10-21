"""Market price data: the series published by the ECB.

The ECB publishes its data through an API that answers with CSV, one request
per data flow. The answer is written as parquet into the Bronze layer without
any change to its content, so that the Bronze layer holds what the source
published and nothing else.

Source: https://data.ecb.europa.eu
"""

from pyspark.sql.functions import col

from ..schemas import load_pyspark_schema
from .common import *
from .download import downloaded, refuse_empty_answer


def download_csv(spark, url, schema, parquet_path):
    """Download a CSV file and store it as parquet in the data lake.

    The file is fetched into a temporary directory on the local disk of the
    driver rather than into the data lake itself, see
    :func:`fincommons.marketpricedata.download.downloaded`. Only the parquet
    file reaches the data lake.

    The answer carries a header row, which is skipped rather than parsed: a
    reader that is given a schema does not skip it by itself. Where it is
    parsed it arrives as a row whose typed fields are all null, because neither
    ``TIME_PERIOD`` nor ``OBS_VALUE`` reads as a date or a number from its own
    column name.

    **An answer that is not the data is refused before it replaces anything.**
    The series is downloaded whole and the Bronze layer is overwritten with it,
    so a refusal or an error page read as CSV would replace the history with
    rows of nulls -- and the run would report success, since the Silver stage
    then finds nothing to take and the table simply stops moving. An answer
    with an error status is therefore raised, and so is one that parses to no
    observation at all, and in both cases the Bronze layer keeps what it held;
    see :func:`fincommons.marketpricedata.download.refuse_empty_answer`.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    url: str
        Address of the CSV file.
    schema: pyspark.sql.types.StructType
        Schema the CSV file is parsed with.
    parquet_path: str
        Path of the parquet data set that should be written.
    """
    with downloaded(url, "ECB", file_name="download.csv") as file_path:
        answer = spark.read.format("csv") \
            .schema(schema) \
            .option("header", "true") \
            .load(file_path)

        refuse_empty_answer(answer.filter(col("OBS_VALUE").isNotNull()),
                            "ECB", url)

        answer.write.mode("overwrite").parquet(parquet_path)


def update_bronze_ecb(spark, config_path):
    """Import various ECB data sets into Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_ecb(spark, "path/to/configuration/file")
    """
    # ESTR
    update_bronze_ecb_estr(spark, config_path)

    # FM
    update_bronze_ecb_fm(spark, config_path)


def update_bronze_ecb_estr(spark, config_path):
    """Download ECB ESTR data into Bronze layer.

    Downloads public ESTR time series into the Bronze layer of the data lake.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_ecb_estr(spark, "path/to/configuration/file")
    """
    download_csv(
        spark,
        'https://data-api.ecb.europa.eu/service/data/EST?format=csvdata',
        load_pyspark_schema("marketpricedata_ecb_estr"),
        path_bronze_ecb_estr(config_path),
    )


def update_bronze_ecb_fm(spark, config_path):
    """Download ECB financial market data into Bronze layer.

    Downloads public financial market time series into the Bronze layer of the data lake.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_ecb_fm(spark, "path/to/configuration/file")
    """
    download_csv(
        spark,
        'https://data-api.ecb.europa.eu/service/data/FM?format=csvdata',
        load_pyspark_schema("marketpricedata_ecb_fm"),
        path_bronze_ecb_fm(config_path),
    )
