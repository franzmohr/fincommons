"""Market price data: the exchange rates in the Silver layer.

The exchange rates are not downloaded again here. They are read from the Silver
table of :mod:`fincommons.exchangerate`, which is the one place they are
maintained, and a rate enters the combined table as the price of a currency
pair named ``BASE/QUOTE``.
"""

import logging

from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, concat_ws

from .. import exchangerate as ex
from ..log import context
from .common import *

logger = logging.getLogger(__name__)


def update_silver_exchangerate(spark, config_path):
    """Import exchange rate data into Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_exchangerate(spark, "path/to/configuration/file")
    """

    # The table belongs to another module and may not be there: the
    # configuration may name no exchange rate layer (KeyError), or the layer
    # may hold no table yet (AnalysisException). Either way there is nothing to
    # merge and the run says so. Anything else is a failure of the source and
    # is raised, so that the run sets it aside with its traceback.
    try:
        bronze = ex.connect_to_exchangerate(spark, config_path)
        bronze.schema
    except (KeyError, AnalysisException) as err:
        bronze = None
        logger.warning(
            "The exchange rate table cannot be read, nothing is merged",
            extra=context(module="marketpricedata", layer="silver",
                          source="EXCHANGERATE", error=type(err).__name__))

    if bronze is not None:

        # Bring into the correct format
        bronze = bronze \
            .withColumn("INSTRID", concat_ws("/", col("BASE"), col("QUOTE"))) \
            .withColumnRenamed("REFDATE", "REFDT") \
            .select("REFDT", "INSTRID", "VALUE")

        standard_upsert(spark, config_path, bronze)
