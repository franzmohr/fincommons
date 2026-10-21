"""Tests of the landing zone of the FIRDS pipeline.

The landing zone asks ESMA's register which files were published, skips the
ones that are already there and downloads the rest. Nothing here is tested
against the register itself: the response is supplied by the tests, so that the
assertions are about what the module does with an answer rather than about what
ESMA happens to be publishing today.
"""

from __future__ import annotations

import datetime as dtmodule
import hashlib
import logging
import io
import os
import urllib.parse
import zipfile

import polars as pl
import pytest
import yaml

from fincommons.firds import common, landing


@pytest.fixture
def config(tmp_path):
    """A configuration file pointing at an empty data lake."""
    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"firds": {name: str(base / name)
                       for name in ("metadata", "landing", "bronze", "silver")}}
        )
    )
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


def docs(*rows):
    """A frame shaped like the output of get_downloadable_files."""
    return pl.DataFrame(
        [
            {"file_type": t, "publication_date": d, "id": n,
             "file_name": n, "download_link": "https://example.invalid/" + n}
            for t, d, n in rows
        ],
        schema=["file_type", "publication_date", "id", "file_name", "download_link"],
    )


class FakeResponse:
    """The part of a requests response the module uses."""

    def __init__(self, payload=b"", json_body=None, status=200, fail_after=None):
        self.payload = payload
        self.json_body = json_body
        self.status = status
        self.fail_after = fail_after

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise landing.requests.HTTPError(f"{self.status} error")

    def iter_content(self, chunk_size):
        for start in range(0, len(self.payload), chunk_size):
            if self.fail_after is not None and start >= self.fail_after:
                raise landing.requests.ConnectionError("connection reset")
            yield self.payload[start:start + chunk_size]
        if self.fail_after is not None:
            raise landing.requests.ConnectionError("connection reset")

    def json(self):
        return self.json_body


def archive(text):
    """The bytes of a zip archive holding one member."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as handle:
        handle.writestr("member.xml", text)
    return buffer.getvalue()


# The query put to the register
# --------------------------------------------------------------------------


def test_the_query_covers_the_whole_of_both_end_days():
    url = landing.url_for_available_files("2024-01-01", "2024-01-31")

    assert "2024-01-01T00:00:00Z" in url
    assert "2024-01-31T23:59:59Z" in url


def test_the_answer_is_sorted_by_publication_date(monkeypatch):
    body = {"response": {"docs": [
        {"file_type": "DLTINS", "publication_date": "2024-01-03T00:00:00Z",
         "id": "b", "file_name": "b.zip", "download_link": "https://example.invalid/b"},
        {"file_type": "FULINS", "publication_date": "2024-01-01T00:00:00Z",
         "id": "a", "file_name": "a.zip", "download_link": "https://example.invalid/a"},
    ]}}
    monkeypatch.setattr(landing.requests, "get", lambda url, timeout: FakeResponse(json_body=body))

    frame = landing.get_downloadable_files("2024-01-01", "2024-01-31")

    assert frame["publication_date"].to_list() == ["2024-01-01", "2024-01-03"]
    assert frame["file_type"].to_list() == ["FULINS", "DLTINS"]


def test_an_empty_answer_is_an_empty_list(monkeypatch):
    body = {"response": {"numFound": 0, "docs": []}}
    monkeypatch.setattr(landing.requests, "get", lambda url, timeout: FakeResponse(json_body=body))

    frame = landing.get_downloadable_files("2024-01-01", "2024-01-01")

    assert len(frame) == 0
    assert "publication_date" in frame.columns


class FakeRegister:
    """A register that answers page by page, as the real one does.

    ``found`` is what it claims to hold, which a register that loses files
    between two pages states wrongly.
    """

    def __init__(self, count, found=None):
        self.docs = [
            {"file_type": "DLTINS", "publication_date": "2024-01-03T00:00:00Z",
             "id": str(i), "file_name": f"f{i:05d}.zip",
             "download_link": f"https://example.invalid/f{i:05d}.zip"}
            for i in range(count)
        ]
        self.found = count if found is None else found
        self.requests = []

    def __call__(self, url, timeout):
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        start, rows = int(query["start"][0]), int(query["rows"][0])
        self.requests.append((start, rows))
        page = self.docs[start:start + rows]
        return FakeResponse(json_body={"response": {"numFound": self.found, "docs": page}})


def test_the_answer_is_read_page_by_page(monkeypatch):
    """The register answers with at most one page at a time. A single request
    for ten thousand rows returns an arbitrary part of the list once the
    window holds more files than that, so the list must be read page by
    page."""
    register = FakeRegister(2500)
    monkeypatch.setattr(landing.requests, "get", register)

    frame = landing.get_downloadable_files("2024-01-01", "2024-01-31")

    assert len(frame) == 2500
    assert frame["file_name"].n_unique() == 2500
    assert [start for start, _ in register.requests] == [0, 1000, 2000]


def test_a_truncated_answer_is_refused(monkeypatch):
    """Deciding what is missing from a list that is itself incomplete would
    pass over whatever the register left out."""
    monkeypatch.setattr(landing.requests, "get", FakeRegister(1500, found=1600))

    with pytest.raises(RuntimeError, match="incomplete"):
        landing.get_downloadable_files("2024-01-01", "2024-01-31")


def test_the_pages_are_sorted_so_that_they_do_not_overlap():
    query = urllib.parse.parse_qs(urllib.parse.urlparse(
        landing.url_for_available_files("2024-01-01", "2024-01-31", start=1000)
    ).query)

    assert query["sort"] == ["publication_date asc,id asc"]
    assert query["start"] == ["1000"]


def test_an_error_answer_from_the_register_is_raised(monkeypatch):
    monkeypatch.setattr(
        landing.requests, "get", lambda url, timeout: FakeResponse(status=503)
    )

    with pytest.raises(landing.requests.HTTPError):
        landing.get_downloadable_files("2024-01-01", "2024-01-31")


# What is already there
# --------------------------------------------------------------------------


def test_an_import_folder_that_does_not_exist_holds_nothing(tmp_path):
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        yaml.safe_dump({"firds": {name: str(tmp_path / "absent" / name)
                                  for name in ("metadata", "landing", "bronze", "silver")}})
    )

    assert landing.files_in_landing_import(str(config_path)) == []


def test_a_file_already_in_the_import_folder_is_not_downloaded_again(config):
    open(os.path.join(common.path_landing_import(config), "b.zip"), "w").close()
    available = docs(("DLTINS", "2024-01-02", "a.zip"), ("DLTINS", "2024-01-03", "b.zip"))

    remaining = landing.drop_imported_files(config, available)

    assert remaining["file_name"].to_list() == ["a.zip"]


def test_a_file_already_imported_is_not_downloaded_again(config):
    open(os.path.join(common.path_landing_finished(config), "b.zip"), "w").close()
    available = docs(("DLTINS", "2024-01-02", "a.zip"), ("DLTINS", "2024-01-03", "b.zip"))

    remaining = landing.drop_imported_files(config, available)

    assert remaining["file_name"].to_list() == ["a.zip"]


def test_a_quarantined_file_is_downloaded_again(config):
    """A file in the error folder failed; fetching it again is the repair."""
    open(os.path.join(common.path_landing_error(config), "a.zip"), "w").close()
    available = docs(("DLTINS", "2024-01-02", "a.zip"))

    assert landing.drop_imported_files(config, available)["file_name"].to_list() == ["a.zip"]


def test_nothing_is_dropped_when_the_import_folder_is_empty(config):
    available = docs(("DLTINS", "2024-01-02", "a.zip"))

    assert landing.drop_imported_files(config, available)["file_name"].to_list() == ["a.zip"]


def test_an_empty_list_of_candidates_survives_the_filter(config):
    open(os.path.join(common.path_landing_import(config), "b.zip"), "w").close()

    assert len(landing.drop_imported_files(config, docs())) == 0


# Downloading
# --------------------------------------------------------------------------


def test_every_candidate_is_written_into_the_import_folder(monkeypatch, config):
    monkeypatch.setattr(
        landing.requests, "get",
        lambda url, stream, timeout: FakeResponse(payload=archive(url)),
    )

    landing.download_files(config, docs(("DLTINS", "2024-01-02", "a.zip"),
                                        ("DLTINS", "2024-01-03", "b.zip")))

    folder = common.path_landing_import(config)
    assert sorted(os.listdir(folder)) == ["a.zip", "b.zip"]
    with zipfile.ZipFile(os.path.join(folder, "a.zip")) as handle:
        assert handle.read("member.xml") == b"https://example.invalid/a.zip"


def with_checksum(frame, checksum):
    return frame.with_columns(landing.pl.lit(checksum).alias("checksum"))


def test_a_file_matching_its_checksum_is_kept(monkeypatch, config):
    payload = archive("contents")
    monkeypatch.setattr(
        landing.requests, "get", lambda url, stream, timeout: FakeResponse(payload=payload)
    )

    landing.download_files(config, with_checksum(
        docs(("DLTINS", "2024-01-02", "a.zip")), hashlib.md5(payload).hexdigest()
    ))

    assert os.listdir(common.path_landing_import(config)) == ["a.zip"]


def test_a_file_not_matching_its_checksum_is_refused(monkeypatch, config):
    """ESMA states the MD5 digest of every file. A file that differs from it is
    damaged, even where it still opens as an archive."""
    monkeypatch.setattr(
        landing.requests, "get",
        lambda url, stream, timeout: FakeResponse(payload=archive("damaged")),
    )

    with pytest.raises(RuntimeError, match="a.zip"):
        landing.download_files(config, with_checksum(
            docs(("DLTINS", "2024-01-02", "a.zip")), hashlib.md5(b"other").hexdigest()
        ))

    assert os.listdir(common.path_landing_import(config)) == []


def test_an_interrupted_download_leaves_nothing_behind(monkeypatch, config):
    """A file left under its own name would count as held, and the next run
    would not fetch it again."""
    payload = archive("x" * 100_000)
    monkeypatch.setattr(
        landing.requests, "get",
        lambda url, stream, timeout: FakeResponse(payload=payload, fail_after=1),
    )

    with pytest.raises(RuntimeError, match="a.zip"):
        landing.download_files(config, docs(("DLTINS", "2024-01-02", "a.zip")))

    assert os.listdir(common.path_landing_import(config)) == []
    remaining = landing.drop_imported_files(config, docs(("DLTINS", "2024-01-02", "a.zip")))
    assert len(remaining) == 1


def test_an_error_answer_is_not_stored_as_the_file(monkeypatch, config):
    monkeypatch.setattr(
        landing.requests, "get",
        lambda url, stream, timeout: FakeResponse(payload=b"<html>busy</html>", status=503),
    )

    with pytest.raises(RuntimeError):
        landing.download_files(config, docs(("DLTINS", "2024-01-02", "a.zip")))

    assert os.listdir(common.path_landing_import(config)) == []


def test_an_answer_that_is_not_an_archive_is_refused(monkeypatch, config):
    monkeypatch.setattr(
        landing.requests, "get",
        lambda url, stream, timeout: FakeResponse(payload=b"<html>maintenance</html>"),
    )

    with pytest.raises(RuntimeError):
        landing.download_files(config, docs(("DLTINS", "2024-01-02", "a.zip")))

    assert os.listdir(common.path_landing_import(config)) == []


def test_one_failed_download_does_not_stop_the_others(monkeypatch, config):
    def get(url, stream, timeout):
        if url.endswith("a.zip"):
            return FakeResponse(status=500)
        return FakeResponse(payload=archive(url))

    monkeypatch.setattr(landing.requests, "get", get)

    with pytest.raises(RuntimeError, match="1 of 2"):
        landing.download_files(config, docs(("DLTINS", "2024-01-02", "a.zip"),
                                            ("DLTINS", "2024-01-03", "b.zip")))

    assert os.listdir(common.path_landing_import(config)) == ["b.zip"]


def test_nothing_is_downloaded_when_there_is_nothing_to_download(monkeypatch, config):
    def refuse(*args, **kwargs):
        raise AssertionError("no request should be made")

    monkeypatch.setattr(landing.requests, "get", refuse)

    landing.download_files(config, docs())

    assert os.listdir(common.path_landing_import(config)) == []


# Where an update starts from
# --------------------------------------------------------------------------


def finished(config_path, *names):
    for name in names:
        open(os.path.join(common.path_landing_finished(config_path), name), "w").close()


def record_downloads(monkeypatch, available):
    """Capture what update_landing_import decides to download."""
    monkeypatch.setattr(
        landing, "get_downloadable_files", lambda date_from, date_to: available
    )
    requested = []
    monkeypatch.setattr(
        landing, "download_files",
        lambda config_path, docs: requested.extend(docs["file_name"].to_list()),
    )
    return requested


class FrozenDatetime(dtmodule.datetime):
    """A ``datetime`` whose ``now`` is fixed, to pin the end of the window.

    It subclasses the real one rather than standing in for it, because the
    module also parses dates with ``strptime`` and would lose that.
    """

    fixed = None

    @classmethod
    def now(cls, tz=None):
        return cls.fixed.astimezone(tz) if tz is not None else cls.fixed


def freeze(monkeypatch, moment):
    FrozenDatetime.fixed = moment
    monkeypatch.setattr(landing, "datetime", FrozenDatetime)


def record_window(monkeypatch, available):
    """Capture the window update_landing_import asks the register for."""
    window = {}

    def remember(date_from, date_to):
        window["from"], window["to"] = date_from, date_to
        return available

    monkeypatch.setattr(landing, "get_downloadable_files", remember)
    monkeypatch.setattr(landing, "download_files", lambda config_path, docs: None)
    return window


def test_the_most_recent_cancellations_file_is_fetched(monkeypatch, config):
    """A cancellations file is the consolidated list of every record cancelled
    so far, not the change of one day, so the newest one says everything the
    older ones do and only it is worth fetching."""
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip")
    available = docs(("FULCAN", "2024-01-02", "FULCAN_20240102_01of01.zip"),
                     ("FULCAN", "2024-01-03", "FULCAN_20240103_01of01.zip"),
                     ("FULCAN", "2024-01-03", "FULCAN_20240103_02of02.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    # Every part of the newest publication, and nothing of the older one.
    assert sorted(requested) == ["FULCAN_20240103_01of01.zip",
                                 "FULCAN_20240103_02of02.zip"]


def test_a_cancellations_file_already_held_is_not_fetched_again(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip",
             "FULCAN_20240103_01of01.zip")
    available = docs(("FULCAN", "2024-01-03", "FULCAN_20240103_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == []


def test_the_delta_files_and_the_cancellations_file_are_fetched_together(
    monkeypatch, config
):
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip")
    available = docs(("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"),
                     ("FULCAN", "2024-01-03", "FULCAN_20240103_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert sorted(requested) == ["DLTINS_20240103_01of01.zip",
                                 "FULCAN_20240103_01of01.zip"]


def test_the_window_ends_yesterday(monkeypatch, config):
    """Today is left out, because ESMA publishes through the day and the set
    of a day still running may be incomplete."""
    finished(config, "FULINS_E_20240101_01of01.zip")
    window = record_window(monkeypatch, docs())
    freeze(monkeypatch, dtmodule.datetime(2024, 1, 10, 12, 0, tzinfo=dtmodule.timezone.utc))

    landing.update_landing_import(config, "2024-01-01")

    assert window["to"] == "2024-01-09"


def test_the_window_is_measured_in_utc_and_not_on_the_local_clock(monkeypatch, config):
    """A run started shortly after local midnight east of Greenwich is still
    on the previous day in UTC. Taking the day off the local clock would move
    the end of the window a further day back, and the run would report that
    there was nothing to download on a day when there was."""
    finished(config, "FULINS_E_20240101_01of01.zip")
    window = record_window(monkeypatch, docs())
    # 00:30 in Vienna on 11 January is 23:30 UTC on 10 January.
    vienna = dtmodule.timezone(dtmodule.timedelta(hours=2))
    freeze(monkeypatch, dtmodule.datetime(2024, 1, 11, 0, 30, tzinfo=vienna))

    landing.update_landing_import(config, "2024-01-01")

    # Yesterday in UTC is the 9th, not the 10th that the local date implies.
    assert window["to"] == "2024-01-09"


def test_the_update_starts_at_the_full_file_when_no_delta_file_was_imported(
    monkeypatch, config
):
    """This branch runs only once, between the first full file and the first
    delta file, so nothing else exercises it: it must start from the full
    file and fetch the delta files published after it."""
    finished(config, "FULINS_E_20240101_01of01.zip")
    available = docs(("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"),
                     ("DLTINS", "2023-12-31", "DLTINS_20231231_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    # Only the delta files published after the full file are wanted.
    assert requested == ["DLTINS_20240102_01of01.zip"]


def test_the_update_continues_after_the_last_delta_file_that_was_imported(
    monkeypatch, config
):
    finished(config, "FULINS_E_20240101_01of01.zip",
             "DLTINS_20240102_01of01.zip", "DLTINS_20240103_01of01.zip")
    available = docs(("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"),
                     ("DLTINS", "2024-01-04", "DLTINS_20240104_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["DLTINS_20240104_01of01.zip"]


def test_a_delta_file_that_failed_is_fetched_although_a_later_one_was_imported(
    monkeypatch, config
):
    """Resuming after the newest file imported would pass over the one that
    failed, and the history would lack its records for good."""
    finished(config, "FULINS_E_20240101_01of01.zip",
             "DLTINS_20240102_01of01.zip", "DLTINS_20240104_01of01.zip")
    available = docs(("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"),
                     ("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"),
                     ("DLTINS", "2024-01-04", "DLTINS_20240104_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["DLTINS_20240103_01of01.zip"]


def test_a_missing_part_of_a_day_is_fetched(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of02.zip")
    available = docs(("DLTINS", "2024-01-02", "DLTINS_20240102_01of02.zip"),
                     ("DLTINS", "2024-01-02", "DLTINS_20240102_02of02.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["DLTINS_20240102_02of02.zip"]


def test_a_missing_part_of_the_full_file_is_fetched(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip")
    available = docs(("FULINS", "2024-01-01", "FULINS_C_20240101_01of01.zip"),
                     ("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"),
                     ("FULINS", "2024-01-08", "FULINS_C_20240108_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["FULINS_C_20240101_01of01.zip"]


def test_a_full_file_that_failed_is_fetched_although_delta_files_were_imported(
    monkeypatch, config
):
    """A finished folder that is not empty but holds no full file must not be
    taken for one that holds a full file: the full file is fetched rather than
    looked up among files that do not include it."""
    finished(config, "DLTINS_20240102_01of01.zip")
    available = docs(("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"),
                     ("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["FULINS_E_20240101_01of01.zip"]


def test_no_full_file_to_start_from_is_reported_as_such(monkeypatch, config):
    available = docs(("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"))
    record_downloads(monkeypatch, available)

    with pytest.raises(RuntimeError, match="No full file"):
        landing.update_landing_import(config, "2024-01-01")


def test_the_full_file_is_downloaded_when_nothing_has_been_imported(monkeypatch, config):
    available = docs(("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"),
                     ("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip"]


# How far back the register is asked
# --------------------------------------------------------------------------


def test_the_register_is_asked_from_the_full_file_at_first(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip")
    window = record_window(monkeypatch, docs())

    landing.update_landing_import(config, "2023-06-01")

    assert window["from"] == "2024-01-01"


def test_the_register_is_asked_from_the_start_date_without_a_full_file(monkeypatch, config):
    available = docs(("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"))
    window = record_window(monkeypatch, available)

    landing.update_landing_import(config, "2023-06-01")

    assert window["from"] == "2023-06-01"


def test_the_watermark_moves_to_the_last_day_everything_is_imported(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip",
             "DLTINS_20240102_01of01.zip", "DLTINS_20240104_01of01.zip")
    available = docs(("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"),
                     ("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"),
                     ("DLTINS", "2024-01-03", "DLTINS_20240103_01of02.zip"),
                     ("DLTINS", "2024-01-03", "DLTINS_20240103_02of02.zip"),
                     ("DLTINS", "2024-01-04", "DLTINS_20240104_01of01.zip"))
    finished(config, "DLTINS_20240103_01of02.zip")
    record_window(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    # The 3rd still lacks a part, so the 4th does not count yet.
    assert landing.read_landing_watermark(config) == "2024-01-02"


def test_the_next_run_asks_from_the_watermark(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip",
             "DLTINS_20240102_01of01.zip", "DLTINS_20240103_01of01.zip")
    available = docs(("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"),
                     ("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"),
                     ("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"))
    record_window(monkeypatch, available)
    landing.update_landing_import(config, "2024-01-01")

    window = record_window(monkeypatch, docs(
        ("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"),
        ("DLTINS", "2024-01-04", "DLTINS_20240104_01of01.zip"),
    ))
    requested = []
    monkeypatch.setattr(
        landing, "download_files",
        lambda config_path, docs: requested.extend(docs["file_name"].to_list()),
    )
    landing.update_landing_import(config, "2024-01-01")

    # The window begins at the full file here, because the watermark is
    # younger than the days the run looks back over.
    assert window["from"] == "2024-01-01"
    assert requested == ["DLTINS_20240104_01of01.zip"]


def test_the_window_begins_before_the_watermark(monkeypatch, config):
    """A file published late is not in the answer the watermark was computed
    from, so the days just before it are asked about again."""
    finished(config, "FULINS_E_20230101_01of01.zip")
    landing.write_landing_watermark(config, "2024-03-20")
    window = record_window(monkeypatch, docs())

    landing.update_landing_import(config, "2023-01-01")

    assert window["from"] == str(
        dtmodule.date(2024, 3, 20) - dtmodule.timedelta(days=landing.LOOKBACK_DAYS)
    )


def test_a_file_published_late_is_still_fetched(monkeypatch, config):
    """The day was settled when the register did not list this part yet."""
    finished(config, "FULINS_E_20230101_01of01.zip",
             "DLTINS_20240318_01of01.zip", "DLTINS_20240320_01of01.zip")
    landing.write_landing_watermark(config, "2024-03-20")
    available = docs(
        ("DLTINS", "2024-03-18", "DLTINS_20240318_01of01.zip"),
        ("DLTINS", "2024-03-19", "DLTINS_20240319_01of01.zip"),
        ("DLTINS", "2024-03-20", "DLTINS_20240320_01of01.zip"),
    )
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2023-01-01")

    assert requested == ["DLTINS_20240319_01of01.zip"]
    # The day that was missed is not settled, so the watermark moves back to
    # the day before it and the update stops there until it is imported.
    assert landing.read_landing_watermark(config) == "2024-03-20"


def test_the_watermark_is_not_moved_back(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip")
    landing.write_landing_watermark(config, "2024-01-05")
    record_window(monkeypatch, docs(("DLTINS", "2024-01-06", "DLTINS_20240106_01of01.zip")))

    landing.update_landing_import(config, "2024-01-01")

    assert landing.read_landing_watermark(config) == "2024-01-05"


def test_the_watermark_is_ignored_without_a_full_file(monkeypatch, config):
    """A landing zone emptied to start again must not resume from the old
    watermark."""
    landing.write_landing_watermark(config, "2024-01-05")
    window = record_window(monkeypatch, docs(
        ("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip")))

    landing.update_landing_import(config, "2024-01-01")

    assert window["from"] == "2024-01-01"


def test_the_watermark_is_recorded_although_a_download_fails(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip")
    monkeypatch.setattr(landing, "get_downloadable_files", lambda date_from, date_to: docs(
        ("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"),
        ("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"),
    ))

    def fail(config_path, docs):
        raise RuntimeError("1 of 1 downloads failed")

    monkeypatch.setattr(landing, "download_files", fail)

    with pytest.raises(RuntimeError):
        landing.update_landing_import(config, "2024-01-01")

    assert landing.read_landing_watermark(config) == "2024-01-02"


# A file published after its day was settled
# --------------------------------------------------------------------------


def test_a_file_for_a_settled_day_is_recorded_as_late(monkeypatch, config, caplog):
    """ESMA published it after the day it is dated, by which time the history
    may have been built past that day."""
    finished(config, "FULINS_E_20240101_01of01.zip",
             "DLTINS_20240102_01of01.zip", "DLTINS_20240103_01of01.zip")
    landing.write_landing_watermark(config, "2024-01-03")
    available = docs(("DLTINS", "2024-01-02", "DLTINS_20240102_01of01.zip"),
                     ("DLTINS", "2024-01-02", "DLTINS_20240102_02of02.zip"),
                     ("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    with caplog.at_level(logging.ERROR, logger="fincommons"):
        landing.update_landing_import(config, "2024-01-01")

    assert requested == ["DLTINS_20240102_02of02.zip"]
    assert landing.read_landing_state(config)["late"] == ["2024-01-02"]
    assert any("left behind" in r.msg for r in caplog.records)


def test_a_file_for_a_day_that_is_not_settled_is_not_late(monkeypatch, config):
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip")
    landing.write_landing_watermark(config, "2024-01-02")
    available = docs(("DLTINS", "2024-01-03", "DLTINS_20240103_01of01.zip"))
    record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert landing.read_landing_state(config)["late"] == []


def test_a_day_can_be_taken_off_the_late_list(config):
    landing.write_landing_watermark(config, "2024-01-03", late=["2024-01-02"])

    landing.clear_late_day(config, "2024-01-02")

    assert landing.read_landing_state(config)["late"] == []
    assert landing.read_landing_watermark(config) == "2024-01-03"


def test_a_record_that_cannot_be_read_is_taken_as_empty(config):
    with open(common.path_landing_watermark(config), "w") as handle:
        handle.write("{ truncated")

    assert landing.read_landing_state(config) == {
        "complete_through": None, "applied_through": None,
        "published": [], "late": [], "full_sample": None,
    }


def test_a_file_for_a_day_the_history_has_passed_is_recorded_as_late(monkeypatch, config):
    """The Silver layer can be further along than this zone, which records what
    it has imported before it downloads anything. Comparing against its own
    record alone would let such a file through unremarked."""
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip")
    landing.write_landing_watermark(config, "2024-01-02")
    landing.record_applied_through(config, "2024-01-05")
    available = docs(("DLTINS", "2024-01-04", "DLTINS_20240104_01of01.zip"))
    requested = record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert requested == ["DLTINS_20240104_01of01.zip"]
    assert landing.read_landing_state(config)["late"] == ["2024-01-04"]


def test_the_day_the_history_stands_at_is_only_ever_moved_forward(config):
    landing.record_applied_through(config, "2024-01-05")
    landing.record_applied_through(config, "2024-01-03")

    assert landing.read_landing_state(config)["applied_through"] == "2024-01-05"


# The full file as the register lists it
# --------------------------------------------------------------------------


def test_the_parts_the_register_lists_for_the_full_file_are_recorded(monkeypatch, config):
    """A full file is published in parts per letter of the CFI code, and a
    letter of which no part arrived leaves no trace among the parts that did.
    The register's list is what the Silver layer checks them against."""
    available = docs(("FULINS", "2024-01-01", "FULINS_E_20240101_01of01.zip"),
                     ("FULINS", "2024-01-01", "FULINS_C_20240101_01of01.zip"),
                     ("FULINS", "2024-01-08", "FULINS_C_20240108_01of01.zip"))
    record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert landing.read_landing_state(config)["full_sample"] == {
        "date": "2024-01-01",
        "files": ["FULINS_C_20240101_01of01.zip", "FULINS_E_20240101_01of01.zip"],
    }


def test_a_run_that_did_not_see_the_full_file_keeps_its_list(monkeypatch, config):
    listed = {"date": "2024-01-01", "files": ["FULINS_E_20240101_01of01.zip"]}
    finished(config, "FULINS_E_20240101_01of01.zip", "DLTINS_20240102_01of01.zip")
    landing.write_landing_watermark(config, "2024-01-20", full_sample=listed)
    available = docs(("DLTINS", "2024-01-21", "DLTINS_20240121_01of01.zip"))
    record_downloads(monkeypatch, available)

    landing.update_landing_import(config, "2024-01-01")

    assert landing.read_landing_state(config)["full_sample"] == listed


def test_the_list_of_the_full_file_survives_the_other_records(config):
    listed = {"date": "2024-01-01", "files": ["FULINS_E_20240101_01of01.zip"]}
    landing.write_landing_watermark(config, "2024-01-03", late=["2024-01-02"],
                                    full_sample=listed)

    landing.record_applied_through(config, "2024-01-05")
    landing.clear_late_day(config, "2024-01-02")

    assert landing.read_landing_state(config)["full_sample"] == listed
