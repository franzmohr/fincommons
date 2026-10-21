"""Logging for the pipelines of the package.

The pipelines of the package run unattended, on a workstation during
development and as a job on Databricks in production, and in both places the
log is the only record of what a run did. The module gives that record one
shape, so that a line can be read without knowing which pipeline wrote it.

Three rules hold everywhere in the package:

1. Library code takes its logger with ``logging.getLogger(__name__)`` and never
   configures logging. Attaching a handler or setting a level on import would
   fight with whatever the notebook or the job has already set up, so that
   decision is left to the caller, who makes it by calling :func:`configure`.
2. The level says what the reader has to do about a record, and can be relied
   on without reading the message. ``INFO`` is the audit record of a run,
   ``WARNING`` marks work that was skipped or degraded while the result stayed
   valid, and ``ERROR`` marks a unit of work that failed and was set aside
   while the run carried on by design. ``CRITICAL`` is not used: a problem that
   invalidates the run raises, it does not log. Logging is never the mechanism
   by which a pipeline decides to carry on.
3. Anything a record is filtered or grouped by later travels as context rather
   than inside the sentence, see :func:`context`.

``print`` keeps a job next to this. A log record is a statement that is still
useful in a log file three weeks after the run; result tables that a script
writes for someone watching the console are the output of the program and
stay on standard output.
"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager

#: The logger every logger of the package descends from. :func:`configure`
#: works on this one rather than on the root logger.
LOGGER_NAME = "fincommons"

#: The default layout of a record. The context follows the message, see
#: :class:`ContextFormatter`.
DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s  %(message)s"

#: The default layout of the timestamp. Seconds are enough: the pipelines are
#: measured in minutes, and a shorter stamp leaves more of the line for the
#: message.
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _render(value) -> str:
    """Render one context value for the log line.

    Values that carry a space are quoted, so that the ``key=value`` pairs of a
    record stay separable by whitespace. Paths are the usual case.
    """
    text = str(value)
    if text == "" or any(character.isspace() for character in text):
        return f'"{text}"'
    return text


class ContextFormatter(logging.Formatter):
    """Formatter that appends the context of a record as ``key=value`` pairs.

    The pairs are appended to the message and therefore appear before a
    traceback rather than after it, which keeps the traceback the last thing in
    the record and readable as one block.
    """

    def formatMessage(self, record: logging.LogRecord) -> str:
        text = super().formatMessage(record)
        values = getattr(record, "ctx", None)
        if not values:
            return text
        pairs = " ".join(f"{key}={_render(value)}" for key, value in values.items())
        return f"{text}  {pairs}" if pairs else text


def context(**values) -> dict:
    """Build the ``extra`` argument of a log call.

    Whatever a record may have to be filtered or grouped by later belongs here
    rather than in the message, so that the message stays a constant string and
    the same key means the same thing in every module. The vocabulary the
    pipelines use is ``module``, ``layer``, ``source``, ``table``, ``datadt``,
    ``refdt``, ``file``, ``rows`` and ``elapsed``.

    Keys whose value is ``None`` are dropped, so that a caller can pass an
    optional value without deciding whether to pass it at all.

    Parameters
    ----------
    **values:
        The context of the record.

    Returns
    -------
    dict
        A mapping to be passed as the ``extra`` argument of a log call.

    Examples
    --------
    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> logger.info(
    ...     "Applied delta file", extra=context(datadt="2026-09-04", rows=18432)
    ... )
    """
    return {"ctx": {key: value for key, value in values.items() if value is not None}}


def configure(
    level="INFO", stream=None, force: bool = False, logger_name: str = LOGGER_NAME
) -> logging.Logger:
    """Send the log of the package to a stream.

    This is meant for the entry points -- a script, a notebook, a job -- and is
    the only place in the package where logging is configured. It is not called
    on import, so a caller who has already set logging up keeps their setup.

    The handler is attached to the ``fincommons`` logger rather than to the
    root logger, and propagation is switched off. That is what makes a local
    run and a run on Databricks produce the same log: the Databricks driver
    already carries a handler on the root logger, so configuring the root
    logger there would write every record twice, while a handler of our own
    writing to standard error is still picked up by the driver log.

    The call is idempotent. Calling it a second time adjusts the level and
    leaves the handler alone, unless ``force`` is set.

    Parameters
    ----------
    level:
        The level below which records are dropped, as a name or a number.
        ``"INFO"`` gives the record of what a run did, ``"DEBUG"`` adds the
        detail needed to reproduce it.
    stream:
        Where the records are written. Standard error by default, which is
        where a log belongs and which keeps it apart from the result tables the
        demonstration scripts write to standard output.
    force:
        Replace a handler that a previous call attached. Useful in a notebook
        that is re-run against a different stream.
    logger_name:
        The logger to configure. A package that builds on this one and logs
        under a tree of its own configures that tree
        with the same call.

    Returns
    -------
    logging.Logger
        The logger of the package.
    """
    logger = logging.getLogger(logger_name)

    if force:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    attached = any(
        not isinstance(handler, logging.NullHandler) for handler in logger.handlers
    )
    if not attached:
        handler = logging.StreamHandler(sys.stderr if stream is None else stream)
        handler.setFormatter(ContextFormatter(DEFAULT_FORMAT, DEFAULT_DATE_FORMAT))
        logger.addHandler(handler)
        logger.propagate = False

    logger.setLevel(level)

    # py4j reports every command it sends to the JVM at DEBUG, which is by a
    # wide margin the loudest thing in a log of this package once the level is
    # lowered. Spark's own log4j is left alone: it is configured on the
    # cluster, and a session built locally sets it through setLogLevel.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    return logger


def add_log_level_argument(parser) -> None:
    """Add ``--log-level`` to the argument parser of an entry point.

    Parameters
    ----------
    parser: argparse.ArgumentParser
        The parser of a script that calls :func:`configure` with the result.
    """
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="level below which log records are dropped",
    )


@contextmanager
def pipeline_run(logger: logging.Logger, stage: str, **values):
    """Record the boundaries of one pipeline stage.

    The context manager writes a line when the stage starts and one when it
    finishes, both carrying the context it was given, and the closing line also
    carries how long the stage took. Where the stage raises, the failure is
    recorded with its traceback and the exception is re-raised: the manager
    reports what happened, it never decides that a run may carry on.

    The object it yields is a mapping the body adds the outcome of the stage
    to -- the rows it wrote, the files it processed -- which is merged into the
    closing line. That keeps the counts out of the body's own log calls and
    gives every stage a closing record of the same shape.

    Parameters
    ----------
    logger:
        The logger of the module the stage lives in.
    stage:
        The name of the stage, usually the name of the function.
    **values:
        The context of the stage, see :func:`context`.

    Yields
    ------
    dict
        The outcome of the stage, to be filled in by the body.

    Examples
    --------
    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> with pipeline_run(logger, "update_silver", module="firds") as outcome:
    ...     outcome["rows"] = 18432
    """
    outcome: dict = {}
    started = time.monotonic()
    logger.info("%s started", stage, extra=context(stage=stage, **values))

    def closing() -> dict:
        # The outcome is merged rather than passed as a second set of keyword
        # arguments, because a stage is free to record a key the stage context
        # already carries and that would be a duplicate argument.
        merged = {"stage": stage, **values, **outcome}
        merged["elapsed"] = f"{time.monotonic() - started:.1f}s"
        return context(**merged)

    try:
        yield outcome
    except Exception:
        logger.exception("%s failed", stage, extra=closing())
        raise

    logger.info("%s finished", stage, extra=closing())
