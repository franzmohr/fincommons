"""SEC filings: the whole pipeline in one call."""

import logging

from ..log import context, pipeline_run
from .bronze import update_bronze_secfiling
from .common import SOURCE
from .parse import PERIODIC_FORMS
from .silver import update_silver_secfiling

logger = logging.getLogger(__name__)


def update_secfiling(spark, config_path, ciks=None, contact=None, bulk=False,
                     forms=PERIODIC_FORMS, file_format=None, force=False):
    """Collect the company files and rebuild the tables.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    ciks: iterable, optional
        The registrants to collect and read. Required unless ``bulk``.
    contact: str
        A contact address; see :func:`fincommons.secfiling.bronze.user_agent`.
    bulk: bool
        Take the archive of every company file rather than one request each.
    forms: iterable of str, optional
        The forms to keep. The periodic reports by default.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    force: bool
        Collect a company file even where the record says it has not changed.
    """
    with pipeline_run(
        logger,
        "update_secfiling",
        module="secfiling",
        source=SOURCE,
        config_path=config_path,
    ):
        update_bronze_secfiling(config_path, ciks=ciks, contact=contact,
                                bulk=bulk, force=force)
        update_silver_secfiling(spark, config_path, ciks=ciks, forms=forms,
                                file_format=file_format)


__all__ = ["update_secfiling"]
