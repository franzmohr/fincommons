"""Market price data: the series published by the Bank of England.

SONIA is the Bank of England's benchmark and is taken from the Bank directly
rather than from FRED, which mirrors the Bank's series under the Bank's own
code, and which is a convenience rather than a source: a mirror is a day
behind at times, it can stop being maintained without saying so, and a
supervisory data set should be able to name the body that published each of its
figures.

The Bank serves its statistical database from a query string rather than from a
REST path, and it answers a request without a user agent with 403 rather than
with data. See :func:`download_boe_series`.

Source: https://www.bankofengland.co.uk/boeapps/database/
"""

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType

from .common import *
from .download import downloaded, refuse_empty_answer

#: Where the statistical database answers CSV requests.
BOE_DATABASE_URL = \
    "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"

#: The date the series are asked for from. It is earlier than any of them
#: begins, so the answer is the whole history and the request does not have to
#: be told where each series starts.
BOE_FIRST_DATE = "01/Jan/1963"


def download_boe_series(spark, series, value_column, parquet_path):
    """Download one series of the Bank's statistical database as parquet.

    As with the other sources, the file is fetched into a temporary directory
    on the local disk of the driver rather than into the data lake, see
    :func:`fincommons.marketpricedata.download.downloaded`.

    Two things are properties of this source rather than choices.

    The request carries a user agent. The Bank serves the database through a
    content delivery network that answers a request without one with ``403
    Access Denied`` and an HTML page, so a download that does not send one fails
    with something that does not look like a refusal at all: a CSV reader given
    that page returns a table of one column and no rows. The same page arriving
    with status 200, or an empty answer, would therefore replace the history
    with nothing and the run would report success, so an answer without an
    observation is refused before it is written and the Bronze layer keeps what
    it held; see :func:`fincommons.marketpricedata.download.refuse_empty_answer`.

    The dates are parsed with a format. The Bank writes them as ``02 Jan 1997``
    rather than as ``1997-01-02``, so casting them would silently yield null
    for every row of the series. The month is an English abbreviation, which is
    the locale Spark parses in by default.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    series: str
        Code of the series in the Bank's database, e.g. ``"IUDSOIA"``.
    value_column: str
        Name the value is stored under in the Bronze layer.
    parquet_path: str
        Path of the parquet data set that should be written.
    """
    schema = StructType([
        StructField("DATE", StringType(), nullable=False),
        StructField(series, StringType(), nullable=True),
    ])

    parameters = {
        "csv.x": "yes",
        "Datefrom": BOE_FIRST_DATE,
        "Dateto": "now",
        "SeriesCodes": series,
        "CSVF": "TN",
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }

    with downloaded(BOE_DATABASE_URL, "BOE", params=parameters,
                    file_name="download.csv") as file_path:
        answer = spark.read.format("csv") \
            .schema(schema) \
            .option("header", "true") \
            .load(file_path) \
            .withColumn("date", to_date(col("DATE"), "dd MMM yyyy")) \
            .withColumnRenamed(series, value_column) \
            .withColumn(value_column, col(value_column).cast("double")) \
            .select("date", value_column) \
            .dropna() \
            .orderBy("date")

        refuse_empty_answer(answer, "BOE", BOE_DATABASE_URL)

        answer.write.mode("overwrite").parquet(parquet_path)


def update_bronze_boe(spark, config_path):
    """Import the Bank of England data sets into the Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_boe(spark, "path/to/configuration/file")
    """
    # SONIA
    update_bronze_boe_sonia(spark, config_path)


def update_bronze_boe_sonia(spark, config_path):
    """Download SONIA into the Bronze layer.

    ``IUDSOIA`` is the Bank's code for the daily SONIA rate, and it is the code
    FRED mirrored the series under while it was read from there.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_boe_sonia(spark, "path/to/configuration/file")
    """
    download_boe_series(spark, "IUDSOIA", "sonia",
                        path_bronze_boe_sonia(config_path))
