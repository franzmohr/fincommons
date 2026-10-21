"""Tests of the logging the pipelines share.

What is tested here is the contract the pipelines rely on: the context of a
record survives into the line, a stage that fails is recorded with its
traceback and the exception is re-raised rather than swallowed, and configuring
the log twice does not attach the handler twice, which on a cluster would write
every record more than once.
"""

from __future__ import annotations

import io
import logging

import pytest

from fincommons import log


@pytest.fixture
def package_logger():
    """The package logger, restored to its previous state afterwards.

    ``configure`` attaches a handler and switches propagation off, which would
    otherwise stay in effect for the rest of the session and hide the records
    of every other test from ``caplog``.
    """
    logger = logging.getLogger(log.LOGGER_NAME)
    handlers = list(logger.handlers)
    level = logger.level
    propagate = logger.propagate
    yield logger
    logger.handlers = handlers
    logger.setLevel(level)
    logger.propagate = propagate


@pytest.fixture
def stream(package_logger):
    """A configured package log, written into a buffer."""
    buffer = io.StringIO()
    log.configure("DEBUG", stream=buffer, force=True)
    return buffer


# The context of a record
# --------------------------------------------------------------------------


def test_the_context_reaches_the_line_as_key_value_pairs(stream):
    logging.getLogger("fincommons.firds.silver").info(
        "Applied delta file", extra=log.context(datadt="2026-09-04", rows=18432)
    )

    assert "datadt=2026-09-04 rows=18432" in stream.getvalue()


def test_a_value_that_was_not_given_is_dropped():
    # A caller passes an optional part of the context without having to decide
    # whether to pass it at all, so a key that is None must not reach the line
    # as the word "None".
    assert log.context(source="ECB", bis=None) == {"ctx": {"source": "ECB"}}


def test_a_value_with_a_space_is_quoted(stream):
    # The pairs are separated by whitespace, so a path with a space in it would
    # otherwise be read as two of them.
    logging.getLogger("fincommons.firds").warning(
        "Nothing to update", extra=log.context(path="/a folder/paths.yaml")
    )

    assert 'path="/a folder/paths.yaml"' in stream.getvalue()


def test_a_record_without_context_is_formatted_as_usual(stream):
    logging.getLogger("fincommons.firds").info("Plain")

    assert stream.getvalue().rstrip().endswith("Plain")


# The boundaries of a stage
# --------------------------------------------------------------------------


def test_a_stage_is_recorded_when_it_starts_and_when_it_finishes(stream):
    logger = logging.getLogger("fincommons.firds.silver")

    with log.pipeline_run(logger, "update_silver", module="firds"):
        pass

    written = stream.getvalue()
    assert "update_silver started" in written
    assert "update_silver finished" in written
    assert "module=firds" in written


def test_the_outcome_of_a_stage_reaches_the_closing_record(stream):
    logger = logging.getLogger("fincommons.firds.silver")

    with log.pipeline_run(logger, "update_silver", module="firds") as outcome:
        outcome["rows"] = 18432

    finished = [
        line for line in stream.getvalue().splitlines() if "finished" in line
    ]
    assert "rows=18432" in finished[0]
    assert "elapsed=" in finished[0]


def test_an_outcome_may_carry_a_key_the_stage_context_already_has(stream):
    # The two are merged rather than passed as two sets of keyword arguments,
    # so a stage that reports the reference date it reached does not fail with
    # a duplicate argument.
    logger = logging.getLogger("fincommons.firds.silver")

    with log.pipeline_run(logger, "update_silver", datadt="2026-09-01") as outcome:
        outcome["datadt"] = "2026-09-04"

    finished = [
        line for line in stream.getvalue().splitlines() if "finished" in line
    ]
    assert "datadt=2026-09-04" in finished[0]


def test_a_failing_stage_is_recorded_with_its_traceback_and_raises(stream):
    """The manager reports what happened; it never decides that a run may
    carry on. A pipeline that cannot do its work has to stop."""
    logger = logging.getLogger("fincommons.firds.silver")

    with pytest.raises(ValueError, match="no watermark"):
        with log.pipeline_run(logger, "update_silver", module="firds"):
            raise ValueError("no watermark")

    written = stream.getvalue()
    assert "update_silver failed" in written
    assert "ValueError: no watermark" in written
    assert "Traceback" in written


def test_the_context_precedes_the_traceback(stream):
    # The traceback is the last thing in the record, so that it reads as one
    # block rather than with the context wedged into its end.
    logger = logging.getLogger("fincommons.firds.silver")

    with pytest.raises(ValueError):
        with log.pipeline_run(logger, "update_silver", module="firds"):
            raise ValueError("boom")

    written = stream.getvalue()
    assert written.index("module=firds") < written.index("Traceback")


# Configuring the log
# --------------------------------------------------------------------------


def test_the_handler_is_attached_only_once(package_logger):
    """On Databricks the root logger already carries a handler, so the package
    attaches its own and stops propagating. Attaching a second one would write
    every record twice into the driver log."""
    log.configure("INFO", stream=io.StringIO(), force=True)
    before = len(package_logger.handlers)

    log.configure("DEBUG", stream=io.StringIO())

    assert len(package_logger.handlers) == before
    assert package_logger.level == logging.DEBUG
    assert package_logger.propagate is False


def test_an_unconfigured_package_does_not_write_anything(package_logger, capsys):
    # Library code that logs in a process which never configured logging must
    # stay silent, and must not have logging complain that no handler was
    # found. That is what the NullHandler in the package is for.
    logging.getLogger("fincommons.firds").info("Applied delta file")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
