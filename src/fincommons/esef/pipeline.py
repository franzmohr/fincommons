"""ESEF filings: the whole pipeline in one call."""

import logging

from ..log import context, pipeline_run
from ..storage import silver_format
from .bronze import update_bronze_esef
from .common import create_directory_tree, ref_silver_facts
from .silver import update_silver_esef

logger = logging.getLogger(__name__)


def update_esef(spark, config_path, countries, years=None, full=False,
                file_format=None, packages=False, contact=None):
    """Brings the ESEF filings of some countries up to date, from the repository to the tables.

    This is the call a job scheduler makes. It sets up the directory tree,
    brings the indexes of filings.xbrl.org up to date in the Bronze layer,
    collects the report of every filing of the countries asked for that it
    does not hold yet or that the repository has processed anew, rebuilds the
    table of filings and rebuilds the partitions of the table of facts whose
    country now has other reports. What a run has to do is read from the
    repository and from what the lake already holds, so the same call is
    right on the first day, when it collects every report of the countries,
    and on every day after it.

    A report that fails, in either layer, is set aside with its traceback and
    the run carries on, as in
    :func:`fincommons.famafrenchdata.update_famafrenchdata`. The reports
    set aside are counted in the outcome the function returns and in a
    closing ``WARNING``, and the function returns rather than raises: the
    command line, :mod:`fincommons.esef.__main__`, turns a count above nought
    into an exit code a scheduler sees.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    countries: list of str
        The countries whose reports are collected and read, ``["AT"]``. The
        table of filings lists every country whatever is asked for.
    years: list of int, optional
        Collect only the reports of filings whose period ends in these years.
    full: bool
        Rebuild every country of the table of facts, whether its reports
        changed or not; see :func:`update_silver_esef`.
    file_format: str, optional
        Format of the Silver tables, ``'parquet'`` or ``'delta'``. Where it is
        left out the configuration file and the tables on disk decide.
    packages: bool
        Collect the report packages into the Bronze layer as well.
    contact: str, optional
        A contact the user agent names, as the repository asks of a client
        that uses it routinely.

    Returns
    -------
    dict
        The outcome of the run. ``failed`` is the number of indexes and
        reports set aside in the two layers together, ``failed_sources``
        names them as ``layer/<name>``.

    Examples
    --------
    >>> outcome = update_esef(spark, "path/to/configuration/file", ["AT"])
    >>> outcome["failed"]
    0
    """
    file_format = silver_format(ref_silver_facts(config_path), file_format)
    with pipeline_run(
        logger,
        "update_esef",
        module="esef",
        config_path=config_path,
        file_format=file_format,
    ) as outcome:
        create_directory_tree(config_path)
        stages = (
            update_bronze_esef(config_path, countries, years, packages, contact),
            update_silver_esef(spark, config_path, full, file_format, countries),
        )
        outcome["failed"] = sum(stage["failed"] for stage in stages)
        outcome["failed_sources"] = [
            source for stage in stages for source in stage["failed_sources"]
        ]
        if outcome["failed"]:
            logger.warning(
                "Indexes or reports were set aside, the run did not update them",
                extra=context(module="esef", stage="update_esef",
                              failed=outcome["failed"],
                              sources=",".join(outcome["failed_sources"])),
            )
    return outcome


__all__ = ["update_esef"]
