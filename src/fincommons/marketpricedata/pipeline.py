"""Market price data: the sources of the module in one call.

The module draws on several publishers, and a publisher that is unreachable on
a given morning should not stop the others from being brought up to date. The
functions below therefore run each source in turn and set a source that fails
aside with its traceback, which is the ``ERROR`` level of
:mod:`fincommons.log`: a unit of work failed and the run carried on by design.

**A source set aside is counted, and the count is what a caller gets back.**
Each function returns the outcome of its stage, a mapping whose ``failed``
entry is the number of sources set aside and whose ``failed_sources`` entry
names them as ``layer/SOURCE``, and the closing record of the stage carries
the same two entries. A stage that set a source aside also ends with a
``WARNING`` saying how many. The functions do not raise for it, because
carrying on past one publisher is what they are for and a caller in a notebook
should still get the outcome of the others; it is the command line,
:mod:`fincommons.marketpricedata.__main__`, that turns a count above nought
into an exit code a scheduler sees.

The Yahoo Finance source is part of the Silver layer but not of the Bronze one.
Its download is the only one that has to be told how far back to go and the only
one that needs a package outside the dependencies of fincommons, so
:func:`update_bronze_yahoo` is called on its own; the Silver stage that reads
what it left behind needs neither and runs with the rest.
"""

import logging
from functools import partial

from ..log import context, pipeline_run
from .bronze_boe import *
from .bronze_dse import *
from .bronze_vndirect import *
from .bronze_ecb import *
from .bronze_electricity import *
from .bronze_lbma import *
from .bronze_nyfed import *
from .bronze_yahoo import *
from .common import *
from .silver_boe import *
from .silver_dse import *
from .silver_vndirect import *
from .silver_ecb import *
from .silver_electricity import *
from .silver_lbma import *
from .silver_exchangerate import *
from .silver_nyfed import *
from .silver_internal import *
from .silver_yahoo import *

logger = logging.getLogger(__name__)


def _run_sources(sources, layer, outcome, message):
    """Run each source in turn, setting aside and counting those that fail.

    A source that raises is logged with its traceback at ``ERROR`` and the next
    one runs, which is the design of the module rather than a lapse in it. The
    count is kept in ``outcome`` rather than only in the log, so that the stage
    can report it at its end and a caller can act on it without reading the
    log back.

    Parameters
    ----------
    sources: iterable of tuple
        Pairs of the name of a source and a callable without arguments that
        updates it, raising where it fails.
    layer: str
        The layer the sources are written into, ``bronze`` or ``silver``.
    outcome: dict
        The outcome of the stage, see :func:`fincommons.log.pipeline_run`. Its
        ``failed`` and ``failed_sources`` entries are created and added to.
    message: str
        The constant message a source is announced with.
    """
    outcome.setdefault("failed", 0)
    outcome.setdefault("failed_sources", [])

    for source, update in sources:
        logger.info(message, extra=context(module="marketpricedata",
                                           layer=layer, source=source))
        try:
            update()
        except Exception:
            logger.exception(
                "A source failed and was set aside",
                extra=context(module="marketpricedata", layer=layer,
                              source=source),
            )
            outcome["failed"] += 1
            outcome["failed_sources"].append(f"{layer}/{source}")


def _report_set_aside(outcome, stage, layer=None):
    """Say at the end of a stage how many sources it set aside.

    It is a ``WARNING`` rather than an ``ERROR``: each failure has already been
    recorded with its traceback, and what this adds is that the stage as a
    whole did less than a reader of its closing line would assume.
    """
    if outcome["failed"]:
        logger.warning(
            "Sources were set aside, the run did not update them",
            extra=context(module="marketpricedata", layer=layer, stage=stage,
                          failed=outcome["failed"],
                          sources=",".join(outcome["failed_sources"])),
        )


def update_bronze_layer(spark, config_path):
    """Updates the Bronze layer.

    Each source runs in turn and one that fails is set aside while the others
    carry on; see the module docstring.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    dict
        The outcome of the stage. ``failed`` is the number of sources set
        aside, ``failed_sources`` names them.

    Examples
    --------
    >>> outcome = update_bronze_layer(spark, "path/to/configuration/file")
    >>> outcome["failed"]
    0
    """
    with pipeline_run(
        logger,
        "update_bronze_layer",
        module="marketpricedata",
        layer="bronze",
        config_path=config_path,
    ) as outcome:
        sources = (
            ("ECB", update_bronze_ecb),
            ("NYFED", update_bronze_nyfed),
            ("ELECTRICITY", update_bronze_electricity),
            ("LBMA", update_bronze_lbma),
            ("BOE", update_bronze_boe),
            ("DSE", update_bronze_dse),
            ("VNDIRECT", update_bronze_vndirect),
        )
        _run_sources(
            ((source, partial(update, spark, config_path))
             for source, update in sources),
            "bronze", outcome, "Downloading a source into the Bronze layer")
        _report_set_aside(outcome, "update_bronze_layer", "bronze")

    return outcome


def update_bronze_yahoo_layer(spark, config_path, min_date=YAHOO_FIRST_DATE):
    """Downloads the five Yahoo Finance sources into the Bronze layer.

    They are kept apart from :func:`update_bronze_layer` because they are the
    only sources that need a package outside the dependencies of fincommons,
    so a data lake can be brought up to date without yfinance installed at all.
    :func:`update_marketpricedata` calls both.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to a source whose Bronze layer holds nothing yet.

    Returns
    -------
    dict
        The outcome of the stage, as for :func:`update_bronze_layer`.

    Examples
    --------
    >>> update_bronze_yahoo_layer(spark, "path/to/configuration/file")
    """
    with pipeline_run(
        logger,
        "update_bronze_yahoo_layer",
        module="marketpricedata",
        layer="bronze",
        config_path=config_path,
    ) as outcome:
        sources = (
            ("YAHOO", update_bronze_yahoo),
            ("YAHOO-COMMODITIES", update_bronze_yahoo_commodities),
            ("YAHOO-VOLATILITY", update_bronze_yahoo_volatility),
            ("YAHOO-INDICES", update_bronze_yahoo_indices),
            ("YAHOO-FX", update_bronze_yahoo_fx),
        )
        _run_sources(
            ((source, partial(update, spark, config_path, min_date))
             for source, update in sources),
            "bronze", outcome, "Downloading a source into the Bronze layer")
        _report_set_aside(outcome, "update_bronze_yahoo_layer", "bronze")

    return outcome


def update_marketpricedata(spark, config_path, min_date=YAHOO_FIRST_DATE,
                           full=False):
    """Brings the market price data up to date, from the sources to the table.

    This is the call a job scheduler makes. It runs every source into the Bronze
    layer and then folds the whole Bronze layer into the Silver table, and it
    needs no argument beyond the configuration: what a run has to collect is
    read from what the lake already holds rather than passed in, so the same
    command is correct on the first morning and on every morning after it.

    **What "only new data" means differs by source, and it is worth knowing
    which kind each one is.** Yahoo Finance and the electricity prices are
    collected incrementally: they are asked for the days since the last one
    their Bronze layer holds, because their histories are large enough that
    fetching them again every morning would be the cost of the job. The
    electricity source additionally asks for the last week again, since a
    delivery day can be republished, and Yahoo for the last fortnight, since
    it leaves a ticker it cannot answer for out of a download rather than
    failing it.

    The ECB, the New York Fed, the Bank of England and the LBMA are collected
    in full every run, and that is deliberate rather than unfinished. Each of
    them publishes its whole series as one file and offers no way to ask for
    part of it, and a file is also how a *revision* to an older value arrives:
    SOFR carries a revision marker, and a download that only ever asked for
    days after the last one held would never see a value corrected behind it.
    The Silver merge replaces a row it has seen before, so collecting the file
    again is what keeps the table current rather than merely longer.

    **The merge is incremental on the same terms.** The Yahoo Bronze layers are
    appended to one download at a time and a partition never changes once it is
    written, so only the partitions after the mark of the last merge are read;
    the electricity stage merges the days its download revisited and leaves the
    rest. The mark is written after the merge has succeeded, so a run that
    fails folds the same deliveries in again next time rather than skipping
    them. The small sources are merged whole, for the reason they are collected
    whole.

    A source that fails is set aside with its traceback and the run carries on,
    as in :func:`update_bronze_layer`, so one unreachable publisher does not
    stop the morning's update. The sources set aside in the three stages are
    added up in the outcome the function returns and in a closing ``WARNING``,
    and the function returns rather than raises: a caller decides what a
    source set aside means to it. The command line,
    :mod:`fincommons.marketpricedata.__main__`, exits with 1 for it, so a
    scheduler sees a run that did not update every source as a failed job.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest reference date to be collected, in the format YYYY-MM-DD. It
        only applies to a source whose Bronze layer holds nothing yet, so it is
        what the history of a new data lake begins at and is ignored afterwards.
    full: bool
        Merge every source into the Silver table from the beginning rather than
        only what is new. A scheduled run never needs it; it is how the table
        is rebuilt from the Bronze layer, which is worth doing after the
        identifiers a source writes have changed, since the rows written under
        the old ones stay where they are.

    Returns
    -------
    dict
        The outcome of the run. ``failed`` is the number of sources set aside
        in all three stages together, ``failed_sources`` names them as
        ``layer/SOURCE``.

    Examples
    --------
    >>> outcome = update_marketpricedata(spark, "path/to/configuration/file")
    >>> outcome["failed"]
    0
    """
    with pipeline_run(
        logger,
        "update_marketpricedata",
        module="marketpricedata",
        config_path=config_path,
    ) as outcome:
        create_directory_tree(config_path=config_path)
        stages = (
            update_bronze_layer(spark, config_path),
            update_bronze_yahoo_layer(spark, config_path, min_date),
            update_silver_layer(spark, config_path, full),
        )
        outcome["failed"] = sum(stage["failed"] for stage in stages)
        outcome["failed_sources"] = [
            source for stage in stages for source in stage["failed_sources"]
        ]
        _report_set_aside(outcome, "update_marketpricedata")

    return outcome


def update_silver_layer(spark, config_path, full=False):
    """Updates the Silver layer for market price data.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    full: bool
        Merge every source from the beginning of its Bronze layer rather than
        only what has arrived since the last run. It is how the table is
        rebuilt from a Bronze layer that is still intact.

    Returns
    -------
    dict
        The outcome of the stage, as for :func:`update_bronze_layer`.

    Examples
    --------
    >>> update_silver_layer(spark, "path/to/configuration/file")
    """
    with pipeline_run(
        logger,
        "update_silver_layer",
        module="marketpricedata",
        layer="silver",
        config_path=config_path,
    ) as outcome:
        # The third entry says whether the stage merges only what is new. Those
        # that do take a ``full`` argument to merge everything again; the rest
        # read one small file holding a whole series and merge all of it every
        # run, which is both cheap and how a revision to an older value
        # arrives. See :func:`update_marketpricedata`.
        sources = (
            ("ECB", update_silver_ecb, False),
            ("EXCHANGERATE", update_silver_exchangerate, False),
            ("NYFED", update_silver_nyfed, False),
            ("BOE", update_silver_boe, False),
            ("LBMA", update_silver_lbma, False),
            ("YAHOO-COMMODITIES", update_silver_yahoo_commodities, True),
            ("YAHOO-VOLATILITY", update_silver_yahoo_volatility, True),
            ("YAHOO-INDICES", update_silver_yahoo_indices, True),
            ("YAHOO-FX", update_silver_yahoo_fx, True),
            ("DSE", update_silver_dse, True),
            ("DSE-CORPORATE-ACTIONS", update_silver_dse_corporate_actions, False),
            ("VNDIRECT", update_silver_vndirect, True),
            ("ELECTRICITY", update_silver_electricity, True),
            ("YAHOO", update_silver_yahoo, True),
            ("INTERNAL", update_silver_internal, False),
        )

        # One merge for the whole run rather than one per source. The sources
        # are collected in the order below and the last delivery of an
        # instrument and a reference date is the one that is kept, as if they
        # were merged one after another -- so the proprietary source runs
        # last, and overrides what the public sources deliver.
        with collected_upserts(spark, config_path):
            _run_sources(
                ((source, partial(update, spark, config_path, full)
                  if incremental else partial(update, spark, config_path))
                 for source, update, incremental in sources),
                "silver", outcome, "Merging a source into the Silver layer")
        _report_set_aside(outcome, "update_silver_layer", "silver")

    return outcome
