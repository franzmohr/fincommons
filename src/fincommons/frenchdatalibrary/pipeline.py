"""The Kenneth R. French Data Library: the whole pipeline in one call."""

import logging

from ..log import context, pipeline_run
from ..storage import silver_format
from .bronze import update_bronze_frenchdatalibrary
from .common import LIBRARY_URL, create_directory_tree, ref_silver_frenchdatalibrary
from .silver import update_silver_frenchdatalibrary

logger = logging.getLogger(__name__)


def update_frenchdatalibrary(spark, config_path, full=False, file_format=None,
                             datasets=None, url=LIBRARY_URL):
    """Brings the French Data Library up to date, from the page to the table.

    This is the call a job scheduler makes. It sets up the directory tree,
    collects every file of the library that has changed into the Bronze
    layer, and rebuilds the partitions of the Silver table whose file the
    Bronze layer now holds a newer collection of. It needs no argument beyond
    the configuration: what a run has to do is read from the server and from
    what the lake already holds, so the same call is right on the first day,
    when it collects the whole library, and on every day after it, when it
    usually finds that nothing has changed. Kenneth French updates the
    library once a month.

    A file that fails, in either layer, is set aside with its traceback and
    the run carries on, as in
    :func:`fincommons.marketpricedata.update_marketpricedata`. The files set
    aside are counted in the outcome the function returns and in a closing
    ``WARNING``, and the function returns rather than raises: the command
    line, :mod:`fincommons.frenchdatalibrary.__main__`, turns a count above
    nought into an exit code a scheduler sees.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Rebuild every partition of the Silver table, whether its file changed
        or not; see :func:`update_silver_frenchdatalibrary`.
    file_format: str, optional
        Format of the Silver table, ``'parquet'`` or ``'delta'``. Where it is
        left out the configuration file and the table on disk decide.
    datasets: list of str, optional
        Collect and rebuild these files only, ``["F-F_Research_Data_Factors"]``.
        Every file of the library where left out.
    url: str
        The page of the library.

    Returns
    -------
    dict
        The outcome of the run. ``failed`` is the number of files set aside in
        the two layers together, ``failed_sources`` names them as
        ``layer/<name>``.

    Examples
    --------
    >>> outcome = update_frenchdatalibrary(spark, "path/to/configuration/file")
    >>> outcome["failed"]
    0
    """
    file_format = silver_format(ref_silver_frenchdatalibrary(config_path), file_format)
    with pipeline_run(
        logger,
        "update_frenchdatalibrary",
        module="frenchdatalibrary",
        config_path=config_path,
        file_format=file_format,
    ) as outcome:
        create_directory_tree(config_path)
        stages = (
            update_bronze_frenchdatalibrary(config_path, datasets, url),
            update_silver_frenchdatalibrary(spark, config_path, full,
                                            file_format, datasets),
        )
        outcome["failed"] = sum(stage["failed"] for stage in stages)
        outcome["failed_sources"] = [
            source for stage in stages for source in stage["failed_sources"]
        ]
        if outcome["failed"]:
            logger.warning(
                "Files were set aside, the run did not update them",
                extra=context(module="frenchdatalibrary",
                              stage="update_frenchdatalibrary",
                              failed=outcome["failed"],
                              sources=",".join(outcome["failed_sources"])),
            )
    return outcome


__all__ = ["update_frenchdatalibrary"]
