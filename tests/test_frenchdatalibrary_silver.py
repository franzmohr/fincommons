"""Tests of the table of the French Data Library in the Silver layer.

The Silver layer reads every file of the Bronze layer into one long table and
rebuilds it file by file, for the files whose collection has changed. The
tests fill the Bronze layer with the small files in
``tests/data/frenchdatalibrary`` and check what the table holds, which files
a run rebuilds, what happens to a file that cannot be read, and that the
table is the one the reader in plain Python gives -- in parquet and in Delta.

The last tests are the vignette of the module's README and of the handbook:
the HML and SMB factors are rebuilt from the six portfolios on size and
book-to-market and compared with the factors the library publishes. Here the
files are synthetic, made so that the published factors are the rebuilt
ones rounded to two decimals; the same comparison against the library itself
runs where ``FINCOMMONS_NETWORK=1`` is set.
"""

from __future__ import annotations

import datetime
import hashlib
import logging
import os
import sys
from pathlib import Path

import pandas
import pytest
import requests

import fincommons.frenchdatalibrary as fdl
from fincommons.frenchdatalibrary import bronze as fdl_bronze

FIXTURES = Path(__file__).resolve().parent / "data" / "frenchdatalibrary"
NAMES = ["F-F_Research_Data_Factors", "6_Portfolios_2x3", "ME_Breakpoints",
         "BE-ME_Breakpoints", "F-F_Research_Data_Factors_weekly",
         "F-F_Research_Data_Factors_daily", "F-F_Momentum_Factor"]


@pytest.fixture
def data_lake(tmp_path):
    """A configuration file pointing at an empty data lake."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "frenchdatalibrary:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    fdl.create_directory_tree(config_path)
    return config_path


def fixture_text(name):
    return (FIXTURES / f"{name}.csv").read_text(encoding="utf-8")


def collect(config_path, name, text=None):
    """Put a file into the Bronze layer as a collection of it would."""
    text = fixture_text(name) if text is None else text
    now = datetime.datetime(2026, 10, 27, 15, 19)
    fdl_bronze.write_dataset(config_path, name, fdl.split_lines(text),
                             "Tue, 27 Oct 2026 15:19:14 GMT", now)
    fdl.write_json(
        os.path.join(fdl.path_bronze_collections(config_path), f"{name}.json"),
        {"dataset": name, "url": f"ftp/{name}_CSV.zip", "etag": None,
         "last_modified": None, "lines": 0,
         "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
         "collected": now.isoformat()},
    )


def table(spark, config_path):
    frame = fdl.connect_to_frenchdatalibrary(spark, config_path).toPandas()
    frame["REFDT"] = pandas.to_datetime(frame["REFDT"])
    return frame


def expected(name):
    frame = fdl.parse_lines(name, fdl.split_lines(fixture_text(name)))
    frame["DATASET"] = name
    return frame


KEY = ["DATASET", "TABLE", "FREQUENCY", "SERIES", "REFDT"]


def same_rows(left, right):
    columns = KEY + ["VALUE", "TITLE"]
    left = left[columns].sort_values(KEY).reset_index(drop=True)
    right = right[columns].sort_values(KEY).reset_index(drop=True)
    pandas.testing.assert_frame_equal(left, right, check_dtype=False)


# Building the table
# --------------------------------------------------------------------------


def test_the_table_holds_what_the_reader_gives_for_every_file(spark, data_lake):
    for name in NAMES:
        collect(data_lake, name)

    outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake)

    assert outcome["written"] == len(NAMES) and outcome["failed"] == 0
    frame = table(spark, data_lake)
    same_rows(frame, pandas.concat([expected(n) for n in NAMES]))
    assert outcome["rows"] == len(frame)


def test_a_file_that_comes_back_in_many_chunks_is_written_whole(
        spark, data_lake, monkeypatch):
    """A file's values travel back in chunks, because a daily file of a
    hundred portfolios returned as one frame is more than a gigabyte and
    exhausted the direct memory of the first collection into a real lake.
    Chunks of seven rows split every file of the fixtures many times over."""
    from fincommons.frenchdatalibrary import silver
    monkeypatch.setattr(silver, "CHUNK_ROWS", 7)
    for name in NAMES:
        collect(data_lake, name)

    outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake)

    assert outcome["written"] == len(NAMES) and outcome["failed"] == 0
    same_rows(table(spark, data_lake), pandas.concat([expected(n) for n in NAMES]))


def test_the_table_has_the_declared_columns_and_no_null(spark, data_lake):
    collect(data_lake, "6_Portfolios_2x3")
    fdl.update_silver_frenchdatalibrary(spark, data_lake)

    df = fdl.connect_to_frenchdatalibrary(spark, data_lake)
    types = dict(df.dtypes)
    assert types == {"REFDT": "date", "DATASET": "string", "TABLE": "string",
                     "FREQUENCY": "string", "SERIES": "string",
                     "VALUE": "double", "TITLE": "string"}
    for column in df.columns:
        assert df.filter(df[column].isNull()).count() == 0, column
    # Partitioned by the file.
    assert os.path.isdir(os.path.join(fdl.path_silver_frenchdatalibrary(data_lake),
                                      "DATASET=6_Portfolios_2x3"))


def test_a_run_with_nothing_new_writes_nothing(spark, data_lake, caplog):
    collect(data_lake, "F-F_Research_Data_Factors")
    fdl.update_silver_frenchdatalibrary(spark, data_lake)

    with caplog.at_level(logging.INFO, logger="fincommons"):
        outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake)

    assert outcome["written"] == 0
    assert any("nothing to do" in r.getMessage() for r in caplog.records)


def test_only_the_file_that_changed_is_rebuilt(spark, data_lake):
    for name in ["F-F_Research_Data_Factors", "ME_Breakpoints"]:
        collect(data_lake, name)
    fdl.update_silver_frenchdatalibrary(spark, data_lake)
    text = fixture_text("ME_Breakpoints").replace("202409,", "202410,")
    collect(data_lake, "ME_Breakpoints", text)

    outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake)

    assert outcome["written"] == 1
    frame = table(spark, data_lake)
    breakpoints = frame[frame.DATASET == "ME_Breakpoints"]
    assert breakpoints.REFDT.max() == pandas.Timestamp("2024-10-31")
    assert pandas.Timestamp("2024-09-30") not in set(breakpoints.REFDT)
    same_rows(frame[frame.DATASET == "F-F_Research_Data_Factors"],
              expected("F-F_Research_Data_Factors"))


def test_full_rebuilds_every_file(spark, data_lake):
    for name in ["F-F_Research_Data_Factors", "ME_Breakpoints"]:
        collect(data_lake, name)
    fdl.update_silver_frenchdatalibrary(spark, data_lake)

    outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake, full=True)

    assert outcome["written"] == 2
    assert len(table(spark, data_lake)) == sum(len(expected(n)) for n in
                                               ["F-F_Research_Data_Factors",
                                                "ME_Breakpoints"])


def test_a_file_that_cannot_be_read_is_set_aside_and_keeps_its_partition(
        spark, data_lake, caplog):
    for name in ["F-F_Research_Data_Factors", "ME_Breakpoints"]:
        collect(data_lake, name)
    fdl.update_silver_frenchdatalibrary(spark, data_lake)
    mark = fdl.read_watermark(data_lake, "ME_Breakpoints")
    collect(data_lake, "ME_Breakpoints",
            fixture_text("ME_Breakpoints") + "202410, 1, 2, 3\n")
    collect(data_lake, "F-F_Research_Data_Factors",
            fixture_text("F-F_Research_Data_Factors").replace("0.40", "0.41"))

    with caplog.at_level(logging.INFO, logger="fincommons"):
        outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake)

    assert outcome["failed"] == 1
    assert outcome["failed_sources"] == ["silver/ME_Breakpoints"]
    assert outcome["written"] == 1
    frame = table(spark, data_lake)
    same_rows(frame[frame.DATASET == "ME_Breakpoints"], expected("ME_Breakpoints"))
    assert set(frame[(frame.DATASET == "F-F_Research_Data_Factors")
                     & (frame.SERIES == "RF") & (frame.FREQUENCY == "M")].VALUE) == {0.41}
    # The mark stays where it was, so the next run tries the file again.
    assert fdl.read_watermark(data_lake, "ME_Breakpoints") == mark
    error = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert error.ctx["source"] == "ME_Breakpoints"
    assert "percentiles" in error.ctx["error"]


def test_a_table_under_a_code_of_its_own_making_is_reported(spark, data_lake, caplog):
    collect(data_lake, "Example",
            "Preamble.\n\n  Median Dividend Yield -- Monthly\n,Lo,Hi\n202401, 1, 2\n")

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        fdl.update_silver_frenchdatalibrary(spark, data_lake)

    record = next(r for r in caplog.records
                  if "not in the vocabulary" in r.getMessage())
    assert record.ctx["table"] == "MEDIAN_DIVIDEND_YIELD_MONTHLY"
    assert record.ctx["source"] == "Example"


def test_the_table_is_rebuilt_a_partition_at_a_time_as_delta(spark, data_lake):
    from delta.tables import DeltaTable

    for name in ["F-F_Research_Data_Factors", "ME_Breakpoints"]:
        collect(data_lake, name)
    fdl.update_silver_frenchdatalibrary(spark, data_lake, file_format="delta")
    collect(data_lake, "ME_Breakpoints",
            fixture_text("ME_Breakpoints").replace("202409,", "202410,"))

    outcome = fdl.update_silver_frenchdatalibrary(spark, data_lake)

    path = fdl.path_silver_frenchdatalibrary(data_lake)
    assert DeltaTable.isDeltaTable(spark, path)
    assert outcome["written"] == 1
    frame = table(spark, data_lake)
    assert frame[frame.DATASET == "ME_Breakpoints"].REFDT.max() == \
        pandas.Timestamp("2024-10-31")
    same_rows(frame[frame.DATASET == "F-F_Research_Data_Factors"],
              expected("F-F_Research_Data_Factors"))


def test_the_marks_need_a_path_beside_a_catalogue_name(tmp_path):
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "frenchdatalibrary:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        "  silver:\n"
        "    table: fincommons.frenchdatalibrary\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="addressed by catalogue name alone"):
        fdl.path_silver_watermarks(str(config_path))


# Reading the table
# --------------------------------------------------------------------------


def test_a_table_of_a_file_is_turned_back_into_its_published_shape(spark, data_lake):
    collect(data_lake, "6_Portfolios_2x3")
    fdl.update_silver_frenchdatalibrary(spark, data_lake)
    df = fdl.connect_to_frenchdatalibrary(spark, data_lake)

    wide = fdl.series(df, "6_Portfolios_2x3", "RET_VW", "M",
                      columns=["SMALL LoBM", "BIG HiBM"])

    assert list(wide.columns) == ["SMALL LoBM", "BIG HiBM"]
    assert len(wide) == 24
    assert wide.index[0] == pandas.Timestamp("2023-01-31")
    assert wide.index.is_monotonic_increasing
    assert wide.loc["2023-01-31", "SMALL LoBM"] == -1.3149
    # The same from rows already in pandas.
    pandas.testing.assert_frame_equal(
        fdl.series(df.toPandas(), "6_Portfolios_2x3", "RET_VW", "M",
                   columns=["SMALL LoBM", "BIG HiBM"]), wide)


def test_a_missing_value_is_a_gap_in_the_wide_frame(spark, data_lake):
    collect(data_lake, "6_Portfolios_2x3")
    fdl.update_silver_frenchdatalibrary(spark, data_lake)
    df = fdl.connect_to_frenchdatalibrary(spark, data_lake)

    wide = fdl.series(df, "6_Portfolios_2x3", "RET_EW", "M")

    assert wide.isna().sum().sum() == 1
    assert list(wide.columns) == sorted(wide.columns)


def test_a_misspelt_name_is_an_error_rather_than_an_empty_frame(spark, data_lake):
    collect(data_lake, "6_Portfolios_2x3")
    fdl.update_silver_frenchdatalibrary(spark, data_lake)
    df = fdl.connect_to_frenchdatalibrary(spark, data_lake)

    with pytest.raises(LookupError, match="no row"):
        fdl.series(df, "6_Portfolios_2x3", "RET_VW", "Q")
    with pytest.raises(LookupError, match="no series"):
        fdl.series(df.toPandas(), "6_Portfolios_2x3", "RET_VW", "M",
                   columns=["SMALL LoBM", "MEDIUM"])


# The whole pipeline
# --------------------------------------------------------------------------


class _Response:
    def __init__(self, status, body=b"", headers=None):
        self.status_code, self.body, self.headers = status, body, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size=8192):
        yield self.body

    def close(self):
        pass


@pytest.fixture
def library(monkeypatch):
    """The library's server, offering the two files of the vignette."""
    import io
    import zipfile

    names = ["F-F_Research_Data_Factors", "6_Portfolios_2x3"]
    base = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"

    def get(url, **kwargs):
        if url.endswith("data_library.html"):
            links = "".join(f'<a href="ftp/{n}_CSV.zip">CSV</a>' for n in names)
            return _Response(200, links.encode())
        for name in names:
            if url == f"{base}ftp/{name}_CSV.zip":
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, "w") as handle:
                    handle.writestr(f"{name}.csv", fixture_text(name))
                return _Response(200, buffer.getvalue(), {"ETag": f'"{name}"'})
        return _Response(404)

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests, "head", lambda url, **kwargs: _Response(
        200, b"", {"ETag": '"' + url.rsplit("/", 1)[1][:-len("_CSV.zip")] + '"'}))
    return names


def test_the_pipeline_runs_from_the_page_to_the_table(spark, data_lake, library):
    outcome = fdl.update_frenchdatalibrary(spark, data_lake)

    assert outcome["failed"] == 0
    frame = table(spark, data_lake)
    assert set(frame.DATASET) == set(library)

    # A second run finds the files unchanged and writes nothing.
    again = fdl.update_silver_frenchdatalibrary(spark, data_lake)
    assert again["written"] == 0


def test_the_command_fails_where_a_file_was_set_aside(monkeypatch):
    import fincommons.frenchdatalibrary.__main__ as cli

    class Session:
        def stop(self):
            pass

    outcome = {"failed": 1, "failed_sources": ["bronze/x"]}
    monkeypatch.setattr(cli, "build_session",
                        lambda master, driver_memory=None: Session())
    monkeypatch.setattr(cli, "configure", lambda level: None)
    monkeypatch.setattr(cli, "update_frenchdatalibrary",
                        lambda spark, config, full, datasets: outcome)
    monkeypatch.setattr(sys, "argv", ["fincommons.frenchdatalibrary",
                                      "--config", "paths.yaml"])

    assert cli.main() == 1
    outcome["failed"] = 0
    assert cli.main() == 0


# The vignette: reproducing a Fama-French factor
# --------------------------------------------------------------------------


def rebuilt_factors(df, frequency="M"):
    """HML and SMB from the six portfolios, beside the published ones.

    The code of the vignette in the README and the handbook.
    """
    portfolios = fdl.series(df, "6_Portfolios_2x3", "RET_VW", frequency)
    factors = fdl.series(df, "F-F_Research_Data_Factors", "FACTORS", frequency)

    hml = (portfolios["SMALL HiBM"] + portfolios["BIG HiBM"]) / 2 \
        - (portfolios["SMALL LoBM"] + portfolios["BIG LoBM"]) / 2
    smb = (portfolios["SMALL LoBM"] + portfolios["ME1 BM2"]
           + portfolios["SMALL HiBM"]) / 3 \
        - (portfolios["BIG LoBM"] + portfolios["ME2 BM2"]
           + portfolios["BIG HiBM"]) / 3
    return pandas.DataFrame({
        "HML": factors["HML"], "HML_REBUILT": hml,
        "SMB": factors["SMB"], "SMB_REBUILT": smb,
    }).dropna()


def assert_the_factors_are_reproduced(comparison, periods):
    assert len(comparison) == periods
    for factor in ("HML", "SMB"):
        gap = (comparison[factor] - comparison[f"{factor}_REBUILT"]).abs()
        # The published factors have two decimals: rounding is all there is.
        assert gap.max() <= 0.005 + 1e-9, factor
        assert comparison[factor].corr(comparison[f"{factor}_REBUILT"]) > 0.9999


def test_the_published_factors_are_rebuilt_from_the_six_portfolios(spark, data_lake):
    for name in ["F-F_Research_Data_Factors", "6_Portfolios_2x3"]:
        collect(data_lake, name)
    fdl.update_silver_frenchdatalibrary(spark, data_lake)
    df = fdl.connect_to_frenchdatalibrary(spark, data_lake)

    assert_the_factors_are_reproduced(rebuilt_factors(df, "M"), 24)
    assert_the_factors_are_reproduced(rebuilt_factors(df, "A"), 2)


@pytest.mark.skipif(os.environ.get("FINCOMMONS_NETWORK") != "1",
                    reason="reaches the French Data Library; set FINCOMMONS_NETWORK=1")
def test_the_published_factors_are_rebuilt_from_the_library_itself(spark, data_lake):
    outcome = fdl.update_frenchdatalibrary(
        spark, data_lake, datasets=["F-F_Research_Data_Factors", "6_Portfolios_2x3"])
    assert outcome["failed"] == 0
    df = fdl.connect_to_frenchdatalibrary(spark, data_lake)

    comparison = rebuilt_factors(df, "M")

    # Every month since July 1926, 1,203 of them in the 202609 edition.
    assert comparison.index[0] == pandas.Timestamp("1926-07-31")
    assert_the_factors_are_reproduced(comparison, len(comparison))


def test_a_batch_is_bounded_by_its_lines_as_well_as_its_files():
    """Fifty daily files of a hundred portfolios in one batch ran the session
    out of heap, so a batch closes before the next file takes it over its
    budget of lines, and a file larger than the budget is a batch alone."""
    from fincommons.frenchdatalibrary import silver
    pending = {"a": {"lines": 40}, "b": {"lines": 40}, "c": {"lines": 150},
               "d": {"lines": 10}, "e": {}, "f": {"lines": 10}}

    assert list(silver.batches(pending, size=50, lines=100)) == [
        ["a", "b"], ["c"], ["d", "e", "f"]]
    assert list(silver.batches(pending, size=2, lines=1000)) == [
        ["a", "b"], ["c", "d"], ["e", "f"]]
