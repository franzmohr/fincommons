"""Tests of how a run of the market price pipeline reports a source it set aside.

A source that fails is set aside so that the others are still brought up to
date, and that is by design. What these tests pin is that it is not silent: the
stage counts it, says so at its end, and the command line a scheduler runs exits
with a code that is not 0.

Every source is replaced by a stub, so the tests need neither a Spark session
nor the network: what is under test is the bookkeeping around the sources, not
the sources themselves, which ``tests/test_marketpricedata.py`` covers.
"""

from __future__ import annotations

import logging
import sys
from contextlib import nullcontext

import pytest

from fincommons.marketpricedata import __main__ as cli
from fincommons.marketpricedata import pipeline

#: The functions that run a whole stage. They are what is under test, so they
#: are the ones that are not replaced.
STAGES = {"update_bronze_layer", "update_bronze_yahoo_layer",
          "update_silver_layer"}


@pytest.fixture
def sources(monkeypatch):
    """Replace every source the pipeline runs with a stub that records its call.

    The returned mapping says which sources ran, and a test makes one fail by
    naming it in ``failing``. The merge that collects the Silver stages and the
    creation of the directory tree are replaced as well, since both need a data
    lake and neither is under test.
    """
    state = {"ran": [], "failing": set()}

    def stub(name):
        def update(*args):
            state["ran"].append(name)
            if name in state["failing"]:
                raise RuntimeError(f"{name} did not answer")
        return update

    for name in dir(pipeline):
        if (name.startswith("update_bronze_") or
                name.startswith("update_silver_")) and name not in STAGES:
            monkeypatch.setattr(pipeline, name, stub(name))

    monkeypatch.setattr(pipeline, "collected_upserts",
                        lambda spark, config_path: nullcontext())
    monkeypatch.setattr(pipeline, "create_directory_tree",
                        lambda config_path: None)
    return state


def test_a_stage_counts_the_source_it_set_aside(sources, caplog):
    """The failure is recorded with its traceback, the sources after it still
    run, and the stage ends with a warning that counts what it set aside."""
    sources["failing"].add("update_bronze_lbma")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        outcome = pipeline.update_bronze_layer(None, "paths.yaml")

    assert outcome["failed"] == 1
    assert outcome["failed_sources"] == ["bronze/LBMA"]
    assert "update_bronze_boe" in sources["ran"]

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "update_bronze_lbma did not answer" in errors[0].exc_text
    assert errors[0].ctx["source"] == "LBMA"

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].ctx["failed"] == 1


def test_a_stage_without_a_failure_reports_none(sources, caplog):
    with caplog.at_level(logging.INFO, logger="fincommons"):
        outcome = pipeline.update_silver_layer(None, "paths.yaml")

    assert outcome["failed"] == 0
    assert outcome["failed_sources"] == []
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_the_run_adds_up_what_its_stages_set_aside(sources, caplog):
    """A source can fail in the Bronze stage and again in the Silver one, and
    each is a source the run did not update."""
    sources["failing"].update({"update_bronze_ecb", "update_bronze_yahoo_fx",
                               "update_silver_ecb"})

    with caplog.at_level(logging.INFO, logger="fincommons"):
        outcome = pipeline.update_marketpricedata(None, "paths.yaml")

    assert outcome["failed"] == 3
    assert outcome["failed_sources"] == [
        "bronze/ECB", "bronze/YAHOO-FX", "silver/ECB"]

    closing = [r for r in caplog.records
               if r.getMessage() == "update_marketpricedata finished"]
    assert closing[0].ctx["failed"] == 3

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING
                and r.ctx.get("stage") == "update_marketpricedata"]
    assert warnings[0].ctx["failed"] == 3


class FakeSession:
    """The part of a Spark session the command line uses."""

    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


@pytest.fixture
def command_line(monkeypatch):
    """Run the command line against the stubbed sources and a fake session.

    Logging is left as the test harness set it up: the command configures the
    ``fincommons`` logger to write to standard error and stop propagating,
    which would hide the records of every later test from ``caplog``.
    """
    session = FakeSession()
    monkeypatch.setattr(cli, "build_session", lambda master: session)
    monkeypatch.setattr(cli, "configure", lambda level: None)
    monkeypatch.setattr(sys, "argv", ["fincommons.marketpricedata",
                                      "--config", "paths.yaml"])
    return session


def test_the_command_fails_where_a_source_was_set_aside(
        sources, command_line, caplog):
    """A scheduler only sees the exit code, so a run that did not update every
    source has to end with one that is not 0 -- while the other sources were
    still brought up to date."""
    sources["failing"].add("update_bronze_boe")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        code = cli.main()

    assert code != 0
    assert "update_silver_boe" in sources["ran"]
    assert command_line.stopped
    assert any(r.levelno == logging.ERROR and r.ctx.get("source") == "BOE"
               for r in caplog.records)


def test_the_command_succeeds_where_every_source_was_updated(
        sources, command_line):
    assert cli.main() == 0
    assert command_line.stopped


def test_the_library_writes_nothing_to_standard_output():
    """A record belongs in the log, where it carries a level and the context of
    the run and can still be read three weeks later; standard output is left to
    whoever calls the library. The call itself is looked for rather than the
    word, so that an example in a docstring does not count."""
    import ast
    from pathlib import Path

    import fincommons.marketpricedata as mp

    package = Path(mp.__file__).parent
    calls = [
        f"{path.name}:{node.lineno}"
        for path in sorted(package.glob("*.py"))
        if path.name != "__main__.py"
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "print"
    ]
    assert calls == []
