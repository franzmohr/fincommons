"""STS: the whole pipeline in one call."""

import logging

from ..log import pipeline_run
from .bronze import update_bronze_sts
from .common import SOURCE, today
from .silver import update_silver_sts

logger = logging.getLogger(__name__)


def update_sts(spark, config_path, collect=True, sheets=True,
               file_format=None, compare=True):
    """Take a snapshot of the register and rebuild the tables from it.

    Two steps, and they are separable for a reason. A lake that collected this
    morning and wants its tables built again passes ``collect=False``, and a
    lake that wants the record of today kept without rebuilding anything calls
    :func:`fincommons.sts.bronze.update_bronze_sts` on its own. The second is
    the one that matters: the snapshots are the only record there is of what
    the register held, so collecting is worth doing even on a day nothing is
    read.

    The tables are built from the snapshot of today where one was just taken,
    and otherwise from the most recent the Bronze layer holds.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    collect: bool
        Take a snapshot first. Without it the tables are rebuilt from what the
        Bronze layer already holds.
    sheets: bool
        Collect the spreadsheets as well as the register.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    compare: bool
        Record what left the register since the snapshot before, where the
        lake holds one.
    """
    with pipeline_run(
        logger,
        "update_sts",
        module="sts",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        day = None
        if collect:
            day = str(today())
            update_bronze_sts(config_path, sheets=sheets, day=day)
            outcome["snapshot"] = day

        update_silver_sts(spark, config_path, day=day,
                          file_format=file_format, compare=compare)


__all__ = ["update_sts"]
