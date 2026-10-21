"""Market price data: the reference rates published by the New York Fed.

SOFR is administered by the Federal Reserve Bank of New York, which publishes it
itself. It is read from the administrator rather than from FRED, which mirrors
it, for the reasons SONIA is read from the Bank of England.

The mirror carries the rate and nothing else. The administrator publishes the
rate together with the distribution it was drawn from -- the first, twenty-fifth,
seventy-fifth and ninety-ninth percentiles and the traded volume -- and with a
flag saying whether an observation has been revised. All of it is kept in the
Bronze layer, which is what a Bronze layer is for: the Silver table has three
columns and can hold the rate alone, and everything else would be lost if it
were not written down here on the way past.

That the revision flag exists at all is reason enough to prefer this source.
The Silver table is maintained by upsert, so a revised figure silently
replaces the one collected before it, and without the flag nothing anywhere
records that a figure ever moved.

Source: https://markets.newyorkfed.org/static/docs/markets-api.html
"""

from pyspark.sql.functions import col, explode, to_date
from pyspark.sql.types import (ArrayType, DoubleType, StringType, StructField,
                               StructType)

from .common import *
from .download import downloaded, refuse_empty_answer

#: Where the markets API answers a request for a range of a reference rate.
NYFED_RATES_URL = "https://markets.newyorkfed.org/api/rates/secured/{rate}/search.json"

#: The date the rates are asked for from. SOFR begins on the 2nd of April 2018;
#: this is earlier than that, so the answer is the whole history and the request
#: does not have to be told where the series starts.
NYFED_FIRST_DATE = "2018-01-01"

#: The last date asked for. The API takes a range rather than a count -- the
#: ``last/n`` form refuses a count large enough to cover the history -- and an
#: end date in the future is answered with everything up to today.
NYFED_LAST_DATE = "2099-12-31"

#: The shape of the answer. The observations arrive inside one ``refRates``
#: array rather than one to a line, so the file is read whole and the array is
#: unpacked. The schema is declared rather than inferred, as everywhere else in
#: the module.
NYFED_SCHEMA = StructType([
    StructField("refRates", ArrayType(StructType([
        StructField("effectiveDate", StringType(), nullable=False),
        StructField("type", StringType(), nullable=True),
        StructField("percentRate", DoubleType(), nullable=True),
        StructField("percentPercentile1", DoubleType(), nullable=True),
        StructField("percentPercentile25", DoubleType(), nullable=True),
        StructField("percentPercentile75", DoubleType(), nullable=True),
        StructField("percentPercentile99", DoubleType(), nullable=True),
        StructField("volumeInBillions", DoubleType(), nullable=True),
        StructField("revisionIndicator", StringType(), nullable=True),
    ])), nullable=True),
])


def download_nyfed_rate(spark, rate, value_column, parquet_path):
    """Download one New York Fed reference rate as parquet into the data lake.

    As with the other sources, the answer passes through a temporary directory
    on the local disk of the driver and only the parquet data set reaches the
    data lake. The request carries a user agent, as at the Bank of England and
    the LBMA. An answer without a single rate -- an empty one, or an error page
    served with status 200 -- is refused before it is written over the history;
    see :func:`fincommons.marketpricedata.download.refuse_empty_answer`.

    Everything the administrator publishes is kept, not only the rate. The
    percentiles and the volume say whether a rate was drawn from a thin or a
    dispersed market, which is the question a supervisor asks of a benchmark,
    and ``revisionIndicator`` says whether the figure has been republished.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    rate: str
        The rate in the API's own terms, e.g. ``"sofr"``.
    value_column: str
        Name the rate itself is stored under in the Bronze layer.
    parquet_path: str
        Path of the parquet data set that should be written.
    """
    url = NYFED_RATES_URL.format(rate=rate)
    parameters = {"startDate": NYFED_FIRST_DATE, "endDate": NYFED_LAST_DATE}

    with downloaded(url, "NYFED", params=parameters,
                    file_name="download.json") as file_path:
        answer = spark.read.format("json") \
            .schema(NYFED_SCHEMA) \
            .option("multiLine", "true") \
            .load(file_path) \
            .select(explode(col("refRates")).alias("observation")) \
            .select(
                to_date(col("observation.effectiveDate")).alias("date"),
                col("observation.percentRate").alias(value_column),
                col("observation.percentPercentile1").alias("percentile1"),
                col("observation.percentPercentile25").alias("percentile25"),
                col("observation.percentPercentile75").alias("percentile75"),
                col("observation.percentPercentile99").alias("percentile99"),
                col("observation.volumeInBillions").alias("volume"),
                col("observation.revisionIndicator").alias("revision"),
            ) \
            .dropna(subset=["date", value_column]) \
            .orderBy("date")

        refuse_empty_answer(answer, "NYFED", url)

        answer.write.mode("overwrite").parquet(parquet_path)


def update_bronze_nyfed(spark, config_path):
    """Import the New York Fed data sets into the Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_nyfed(spark, "path/to/configuration/file")
    """
    # SOFR
    update_bronze_nyfed_sofr(spark, config_path)


def update_bronze_nyfed_sofr(spark, config_path):
    """Download SOFR into the Bronze layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_nyfed_sofr(spark, "path/to/configuration/file")
    """
    download_nyfed_rate(spark, "sofr", "sofr",
                        path_bronze_nyfed_sofr(config_path))
