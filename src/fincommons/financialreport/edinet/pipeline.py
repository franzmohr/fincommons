"""EDINET: the whole pipeline in one call."""

import logging

from ...log import pipeline_run
from .bronze import collect_document, update_bronze_edinet
from .common import SOURCE
from .silver import build_filings, outstanding, update_silver_edinet

logger = logging.getLogger(__name__)


def update_edinet(spark, config_path, start=None, end=None, key=None,
                  documents=True, file_format=None):
    """Collect the indexes and the filings, and rebuild the tables.

    Three steps rather than two, because what is collected depends on what the
    indexes say: the indexes come first, the table of filings is built from
    them, and only then is it known which filings the agency has a conversion
    for. A filing it never converted is never asked for.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    start, end: datetime.date or str, optional
        The range of calendar days; see
        :func:`fincommons.financialreport.edinet.bronze.update_bronze_edinet`.
        Mind that a filing leaves EDINET ten years after it was filed.
    key: str, optional
        The API key; see :func:`fincommons.credentials.api_key`.
    documents: bool
        Collect the conversions of the filings the indexes name. Without it
        the run brings the indexes up to date and reads whatever was
        collected before, which is how a lake separates the two.
    file_format: str, optional
        How to store the tables; see :func:`fincommons.storage.silver_format`.
    """
    with pipeline_run(
        logger,
        "update_edinet",
        module="edinet",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        update_bronze_edinet(config_path, start=start, end=end, key=key)

        if documents:
            filings = build_filings(spark, config_path)
            wanted = outstanding(config_path, filings)
            counts = {"collected": 0, "held": 0, "absent": 0}
            for doc in wanted:
                counts[collect_document(config_path, doc, key, "csv")] += 1
            outcome.update(counts)

        update_silver_edinet(spark, config_path, file_format=file_format)


__all__ = ["update_edinet"]
