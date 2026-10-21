"""Market price data: the proprietary source in the Silver layer.

The module does not download the proprietary quotes of an organisation, because
where such quotes come from and how they are produced differs from one
organisation to the next. It picks up a file placed in the Bronze layer by
whoever maintains them, as :mod:`fincommons.exchangerate` and
:mod:`fincommons.entitymasterdata` do with their own proprietary sources, and
in the same form: one comma-separated file with a header row, under a name the
module knows, read with the schema declared beside the paths.

The file is the last word on the quotes it carries. It is merged into the
combined table like any other source, so a reference date it delivers replaces
whatever was there for that instrument before -- a public quote of the same
instrument among it.
"""

import logging
import os

import pyspark.sql.functions as func

from ..log import context
from .common import *

logger = logging.getLogger(__name__)


INTERNAL_FILE = "marketpricedata_internal.csv"
"""Name of the file the proprietary quotes are picked up under."""


def update_silver_internal(spark, config_path):
    """Import price data from a proprietary source into the Silver layer.

    The file is picked up from the ``internal`` directory of the Bronze layer,
    see :func:`path_bronze_internal_marketpricedata`, and nothing happens where
    it is not there: the stage is called on every run of the Silver layer,
    including in a data lake that keeps no proprietary quotes at all.

    **A file with a row that cannot be read is refused whole.** The reader
    does not fail on a field it cannot parse; it leaves the field empty, so a
    date written as ``21.09.2026`` or a price written as ``1,5`` would reach
    the table as a row without a date or without a price. The merge drops such
    rows, see :func:`complete_rows`, but a quote that silently goes missing
    from the one source that is the last word on its quotes is no better. The
    file is therefore checked before any of it is merged, and a file that
    fails the check is set aside by the run with the lines that need fixing,
    and the quotes collected earlier stay in the table as they were.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_silver_internal(spark, "path/to/configuration/file")
    """

    internal_file = os.path.join(
        path_bronze_internal_marketpricedata(config_path), INTERNAL_FILE
    )

    if not os.path.exists(internal_file):
        # INFO rather than WARNING: a data lake that keeps no proprietary quotes
        # is the usual case, not a run that did less than it should have.
        logger.info(
            "No proprietary file in the Bronze layer, nothing is merged",
            extra=context(module="marketpricedata", layer="silver",
                          source="INTERNAL", path=internal_file))
        return

    bronze = spark.read \
        .options(header='True', delimiter=',') \
        .schema(internal_schema) \
        .csv(internal_file) \
        .select("REFDT", "INSTRID", "VALUE")

    # Checked here, while the stage can still fail on its own, rather than left
    # to the merge: inside a batch the rows are only read when every source is
    # merged together, and an error then would take the other sources with it.
    unreadable = bronze.filter(
        func.col("REFDT").isNull() | func.col("INSTRID").isNull()
        | func.col("VALUE").isNull()
    ).limit(5).collect()
    if unreadable:
        raise ValueError(
            f"{internal_file} holds rows without a readable reference date, "
            f"instrument or value, for instance {[tuple(r) for r in unreadable]}. "
            "Dates are read as YYYY-MM-DD and values with a decimal point; "
            "the file is not merged until every row reads."
        )

    standard_upsert(spark, config_path, bronze)
