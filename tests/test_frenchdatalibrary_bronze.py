"""Tests of collecting the French Data Library into the Bronze layer.

The collection reads the page of the library for the files it links and
downloads each file that has changed since the last run. The tests put a fake
server in place of the library -- a page, zip archives and the headers that go
with them -- so that what is asserted is what the module does with an answer:
which files it downloads, which it skips, what it keeps, and what it does with
an answer that is not a file of the library. Nothing reaches the network and
nothing needs Spark.
"""

from __future__ import annotations

import io
import json
import logging
import os
import zipfile
from pathlib import Path

import pyarrow.parquet
import pytest
import requests

import fincommons.frenchdatalibrary as fdl

FIXTURES = Path(__file__).resolve().parent / "data" / "frenchdatalibrary"
BASE = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"
PAGE = BASE + "data_library.html"


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


def archive(text, name="file.csv"):
    """A zip archive holding one file, as the library packs them."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as handle:
        handle.writestr(name, text)
    return buffer.getvalue()


def page(*names):
    """The page of the library linking the CSV and the text archive of each file."""
    links = "".join(
        f'<a href="ftp/{n}_TXT.zip">TXT</a> <a href="ftp/{n}_CSV.zip">CSV</a>\n'
        for n in names)
    return f"<html><body>{links}</body></html>".encode()


class FakeResponse:
    def __init__(self, status, body=b"", headers=None):
        self.status_code = status
        self.body = body
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def iter_content(self, chunk_size=8192):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        pass


class FakeLibrary:
    """The server of the library: a page, files and their headers.

    ``files`` maps the name of a file to its text; ``tags`` to the entity tag
    the server gives it. ``head`` can be switched off, which is a server that
    does not answer a HEAD request. Every request is recorded.
    """

    def __init__(self, files):
        self.files = dict(files)
        self.tags = {name: f'"{i}"' for i, name in enumerate(self.files)}
        self.broken = {}
        self.head = True
        self.requests = []

    def url(self, name):
        return f"{BASE}ftp/{name}_CSV.zip"

    def _find(self, url):
        for name in self.files:
            if url == self.url(name):
                return name
        return None

    def get(self, url, **kwargs):
        assert kwargs["headers"]["User-Agent"]
        assert kwargs["timeout"]
        self.requests.append(("GET", url))
        if url == PAGE:
            return FakeResponse(200, page(*self.files))
        name = self._find(url)
        if name is None:
            return FakeResponse(404)
        body = self.broken.get(name) or archive(self.files[name], f"{name}.csv")
        return FakeResponse(200, body, self._headers(name))

    def head_request(self, url, **kwargs):
        assert kwargs["headers"]["User-Agent"]
        self.requests.append(("HEAD", url))
        if not self.head:
            return FakeResponse(405)
        name = self._find(url)
        if name is None:
            return FakeResponse(404)
        return FakeResponse(200, b"", self._headers(name))

    def _headers(self, name):
        return {"ETag": self.tags[name],
                "Last-Modified": "Tue, 27 Oct 2026 15:19:14 GMT"}

    def downloads(self):
        return [url for method, url in self.requests
                if method == "GET" and url != PAGE]


@pytest.fixture
def library(monkeypatch):
    names = ["F-F_Research_Data_Factors", "6_Portfolios_2x3", "ME_Breakpoints"]
    server = FakeLibrary({
        n: (FIXTURES / f"{n}.csv").read_text(encoding="utf-8") for n in names})
    monkeypatch.setattr(requests, "get", server.get)
    monkeypatch.setattr(requests, "head", server.head_request)
    return server


def bronze_lines(config_path, dataset):
    path = os.path.join(fdl.path_bronze_dataset(config_path, dataset),
                        "part-00000.parquet")
    return pyarrow.parquet.ParquetFile(path).read().to_pandas()


# The page of the library
# --------------------------------------------------------------------------


def test_the_page_is_read_for_the_csv_archives_only():
    html = page("F-F_Research_Data_Factors", "6_Portfolios_2x3").decode()

    files = fdl.parse_index(html, PAGE)

    assert files == {
        "6_Portfolios_2x3": BASE + "ftp/6_Portfolios_2x3_CSV.zip",
        "F-F_Research_Data_Factors": BASE + "ftp/F-F_Research_Data_Factors_CSV.zip",
    }


def test_a_page_without_a_file_is_refused():
    with pytest.raises(ValueError, match="links no zipped CSV file"):
        fdl.parse_index("<html>Service unavailable</html>", PAGE)


def test_a_link_that_is_not_a_plain_name_or_leaves_the_server_is_left_out(caplog):
    html = ('<a href="ftp/..%2F..%2Fetc_CSV.zip">x</a>'
            '<a href="https://elsewhere.example/ftp/Other_CSV.zip">y</a>'
            '<a href="ftp/Good_CSV.zip">z</a>')
    with caplog.at_level(logging.WARNING, logger="fincommons"):
        files = fdl.parse_index(html, PAGE)

    assert list(files) == ["Good"]
    assert len(caplog.records) == 2


def test_a_name_with_parentheses_is_a_plain_name(caplog):
    """Twelve files of the library name investment ``INV(TA)``; refusing the
    parentheses set all twelve aside on the first collection into a lake."""
    name = "Europe_32_Portfolios_ME_INV(TA)_OP_2x4x4"
    with caplog.at_level(logging.WARNING, logger="fincommons"):
        files = fdl.parse_index(f'<a href="ftp/{name}_CSV.zip">x</a>', PAGE)

    assert files == {name: BASE + f"ftp/{name}_CSV.zip"}
    assert not caplog.records


# Collecting a file
# --------------------------------------------------------------------------


def test_a_file_is_kept_line_by_line_with_its_preamble(data_lake, library):
    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    assert outcome["collected"] == 3 and outcome["failed"] == 0
    lines = bronze_lines(data_lake, "F-F_Research_Data_Factors")
    text = (FIXTURES / "F-F_Research_Data_Factors.csv").read_text(encoding="utf-8")
    assert lines.LINE.tolist() == text.split("\n")[:-1]
    assert lines.LINENO.tolist() == list(range(1, len(lines) + 1))
    assert lines.LINE.iloc[0].startswith("This file was created using")
    assert str(lines.LASTMODIFIED.iloc[0]) == "2026-10-27 15:19:14"
    # The name of the file is the partition, not a column of the file.
    path = os.path.join(fdl.path_bronze_dataset(data_lake, "6_Portfolios_2x3"),
                        "part-00000.parquet")
    assert "DATASET" not in pyarrow.parquet.ParquetFile(path).schema_arrow.names


def test_the_record_of_a_collection_says_what_the_server_said(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)

    record = fdl.read_collection(data_lake, "6_Portfolios_2x3")
    assert record["etag"] == library.tags["6_Portfolios_2x3"]
    assert record["last_modified"] == "Tue, 27 Oct 2026 15:19:14 GMT"
    assert record["url"] == library.url("6_Portfolios_2x3")
    assert len(record["sha256"]) == 64
    assert set(fdl.read_collections(data_lake)) == set(library.files)


def test_a_file_whose_tag_has_not_changed_is_not_downloaded_again(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)
    library.requests.clear()

    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    assert outcome["unchanged"] == 3 and outcome["collected"] == 0
    assert library.downloads() == []


def test_a_file_whose_tag_has_changed_is_downloaded_again(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)
    library.requests.clear()
    library.tags["ME_Breakpoints"] = '"new"'
    library.files["ME_Breakpoints"] += "A note added.\n"

    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    assert outcome["collected"] == 1 and outcome["unchanged"] == 2
    assert library.downloads() == [library.url("ME_Breakpoints")]
    assert bronze_lines(data_lake, "ME_Breakpoints").LINE.iloc[-1] == "A note added."


def test_the_same_text_under_a_new_tag_is_not_written_again(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)
    before = fdl.read_collection(data_lake, "ME_Breakpoints")
    partition = os.path.join(fdl.path_bronze_dataset(data_lake, "ME_Breakpoints"),
                             "part-00000.parquet")
    written = os.stat(partition).st_mtime_ns
    library.tags["ME_Breakpoints"] = '"republished"'

    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    after = fdl.read_collection(data_lake, "ME_Breakpoints")
    assert outcome["unchanged"] == 3
    assert os.stat(partition).st_mtime_ns == written
    # The record follows the new tag, so the next run skips the download, and
    # keeps the collection the Silver layer was built from.
    assert after["etag"] == '"republished"'
    assert after["sha256"] == before["sha256"]
    assert after["collected"] == before["collected"]


def test_a_server_that_does_not_answer_head_is_compared_by_text(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)
    library.head = False
    library.requests.clear()

    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    # Every file is downloaded, and none is written, since none has changed.
    assert len(library.downloads()) == 3
    assert outcome["unchanged"] == 3 and outcome["collected"] == 0


def test_a_file_removed_from_the_bronze_layer_is_collected_again(data_lake, library):
    import shutil

    fdl.update_bronze_frenchdatalibrary(data_lake)
    shutil.rmtree(fdl.path_bronze_dataset(data_lake, "ME_Breakpoints"))

    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    assert outcome["collected"] == 1


def test_only_the_files_asked_for_are_collected(data_lake, library):
    outcome = fdl.update_bronze_frenchdatalibrary(
        data_lake, datasets=["F-F_Research_Data_Factors"])

    assert outcome["collected"] == 1
    assert list(fdl.read_collections(data_lake)) == ["F-F_Research_Data_Factors"]


# What fails
# --------------------------------------------------------------------------


def test_a_file_that_fails_is_set_aside_and_the_others_are_collected(
        data_lake, library, caplog):
    library.broken["6_Portfolios_2x3"] = b"<html>Error</html>"

    with caplog.at_level(logging.INFO, logger="fincommons"):
        outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    assert outcome["failed"] == 1
    assert outcome["failed_sources"] == ["bronze/6_Portfolios_2x3"]
    assert outcome["collected"] == 2
    assert fdl.read_collection(data_lake, "6_Portfolios_2x3") is None
    error = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert error.ctx["source"] == "6_Portfolios_2x3" and error.exc_info


def test_a_file_that_fails_keeps_the_text_collected_before(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)
    before = bronze_lines(data_lake, "6_Portfolios_2x3")
    library.tags["6_Portfolios_2x3"] = '"new"'
    library.broken["6_Portfolios_2x3"] = archive("")

    outcome = fdl.update_bronze_frenchdatalibrary(data_lake)

    assert outcome["failed"] == 1
    assert bronze_lines(data_lake, "6_Portfolios_2x3").equals(before)


def test_an_archive_of_more_than_one_file_is_refused(tmp_path):
    path = tmp_path / "two.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as handle:
        handle.writestr("a.csv", "x")
        handle.writestr("b.csv", "y")
    path.write_bytes(buffer.getvalue())

    with pytest.raises(ValueError, match="holds 2 CSV files"):
        fdl.read_archive(str(path), "Example")


def test_a_file_the_page_does_not_link_is_counted_as_failed(data_lake, library):
    outcome = fdl.update_bronze_frenchdatalibrary(data_lake, datasets=["Missing"])

    assert outcome["failed"] == 1 and outcome["failed_sources"] == ["bronze/Missing"]


def test_a_page_that_is_not_the_library_fails_the_stage(data_lake, monkeypatch):
    monkeypatch.setattr(requests, "get",
                        lambda url, **kwargs: FakeResponse(200, b"<html></html>"))

    with pytest.raises(ValueError, match="links no zipped CSV file"):
        fdl.update_bronze_frenchdatalibrary(data_lake)


def test_the_records_are_json(data_lake, library):
    fdl.update_bronze_frenchdatalibrary(data_lake)

    folder = fdl.path_bronze_collections(data_lake)
    for name in os.listdir(folder):
        with open(os.path.join(folder, name), encoding="utf-8") as handle:
            assert json.load(handle)["dataset"] + ".json" == name
