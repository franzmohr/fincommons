"""Tests of collecting filings.xbrl.org into the Bronze layer.

The collection brings the index files of the repository up to date and
downloads the report of each filing of the countries asked for that is new
or was processed anew. The tests put a fake repository in place of the real
one -- index files, reports, packages and the headers that go with them -- so
that what is asserted is what the module does with an answer: which files it
downloads, which it skips, what it keeps, and what it does with an answer
that is not what it asked for. Nothing reaches the network and nothing needs
Spark.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
from pathlib import Path

import pytest
import requests

import fincommons.esef as esef
from fincommons.esef import bronze as esef_bronze

FIXTURES = Path(__file__).resolve().parent / "data" / "esef"
LAST_MODIFIED = "Mon, 26 Oct 2026 17:06:56 GMT"


def load(name):
    with open(FIXTURES / name, encoding="utf-8") as handle:
        return json.load(handle)


@pytest.fixture
def data_lake(tmp_path):
    """A configuration file pointing at an empty data lake."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "esef:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    esef.create_directory_tree(config_path)
    return config_path


class FakeResponse:
    def __init__(self, status, body=b"", headers=None):
        self.status_code = status
        self.body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size=8192):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        pass


class FakeRepository:
    """filings.xbrl.org: an index per year, and the files of each filing.

    ``indexes`` maps a year to its index document, ``files`` an address to
    the bytes served at it. An index answers ``304`` to a request whose
    ``If-Modified-Since`` is its ``Last-Modified``. Every request is
    recorded with its headers.
    """

    def __init__(self, indexes, files):
        self.indexes = dict(indexes)
        self.files = dict(files)
        self.requests = []

    def get(self, url, **kwargs):
        headers = kwargs["headers"]
        assert headers["User-Agent"]
        assert kwargs["timeout"]
        self.requests.append((url, headers))
        prefix = esef.BASE_URL + "/indexes/filings-"
        if url.startswith(prefix):
            year = int(url[len(prefix):-len(".json")])
            if year not in self.indexes:
                return FakeResponse(404)
            if headers.get("If-Modified-Since") == LAST_MODIFIED:
                return FakeResponse(304)
            body = json.dumps(self.indexes[year]).encode()
            return FakeResponse(200, body, {"Last-Modified": LAST_MODIFIED})
        path = url[len(esef.BASE_URL):]
        if path not in self.files:
            return FakeResponse(404)
        return FakeResponse(200, self.files[path])

    def downloads(self):
        """The files of filings that were asked for, indexes left out."""
        return [url[len(esef.BASE_URL):] for url, _ in self.requests
                if "/indexes/" not in url]


INDEXES = {year: load(f"filings-{year}.json") for year in (2024, 2025, 2026)}
FILINGS = {f["attributes"]["fxo_id"]: f
           for document in INDEXES.values() for f in document["data"]}
AT = FILINGS["5299000000000000AT01-2026-03-31-ESEF-AT-0"]
AT2 = FILINGS["5299000000000000AT02-2025-12-31-ESEF-AT-0"]
DE = FILINGS["5299000000000000DE01-2025-12-31-ESEF-DE-0"]
NO_JSON = FILINGS["5299000000000000AT01-2024-03-31-ESEF-AT-0"]


def report_of(document):
    """The fixture report of a filing, by the name of its file."""
    return load(document["attributes"]["json_url"].rsplit("/", 1)[1])


def body(document):
    return json.dumps(document).encode()


def reprocessed(document, processed="2026-10-01 08:00:00"):
    """A filing the repository has processed anew."""
    document = json.loads(json.dumps(document))
    document["attributes"]["processed"] = processed
    return document


def with_filings(year, *filings):
    """The index of a year with other filings, its entities kept."""
    document = json.loads(json.dumps(INDEXES[year]))
    document["data"] = list(filings)
    return document


@pytest.fixture
def repository(monkeypatch):
    server = FakeRepository(
        {year: json.loads(json.dumps(document))
         for year, document in INDEXES.items()},
        {f["attributes"]["json_url"]: body(report_of(f))
         for f in (AT, AT2, DE)},
    )
    monkeypatch.setattr(requests, "get", server.get)
    monkeypatch.setattr(esef_bronze, "current_year", lambda now=None: 2026)
    return server


def name(document):
    return document["attributes"]["fxo_id"]


# The indexes
# --------------------------------------------------------------------------


def test_every_index_is_kept_as_published_and_a_missing_year_is_passed_over(
        data_lake, repository):
    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["indexes"] == 3 and outcome["failed"] == 0
    for year, document in repository.indexes.items():
        with open(esef.path_bronze_index(data_lake, year), encoding="utf-8") as handle:
            assert json.load(handle) == document
    assert not os.path.exists(esef.path_bronze_index(data_lake, 2023))
    record = esef.read_json(esef.path_index_record(data_lake, 2026))
    assert record["last_modified"] == LAST_MODIFIED
    assert record["filings"] == 1


def test_an_index_is_asked_for_with_the_date_it_was_last_modified(
        data_lake, repository):
    esef.update_bronze_esef(data_lake, ["AT"])
    repository.requests.clear()

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["indexes"] == 0
    asked = {url: headers for url, headers in repository.requests
             if "/indexes/filings-2026" in url}
    assert list(asked.values())[0]["If-Modified-Since"] == LAST_MODIFIED


def test_an_index_that_is_not_one_is_refused_and_the_one_held_is_kept(
        data_lake, repository):
    esef.update_bronze_esef(data_lake, ["AT"])
    os.remove(esef.path_index_record(data_lake, 2026))
    repository.indexes[2026] = {"errors": [{"status": "503"}]}

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["failed_sources"] == ["bronze/index/2026"]
    with open(esef.path_bronze_index(data_lake, 2026), encoding="utf-8") as handle:
        assert json.load(handle) == INDEXES[2026]


def test_the_filings_are_read_from_every_index_the_layer_holds(
        data_lake, repository):
    esef.update_bronze_esef(data_lake, ["AT"])

    filings = esef.read_filings(data_lake)

    assert list(filings) == sorted([name(AT), name(AT2), name(DE), name(NO_JSON)])
    assert filings[name(DE)]["NAME"] == "Beispiel SE"
    assert list(esef.read_filings(data_lake, years=[2025])) == sorted(
        [name(AT2), name(DE)])


# The reports
# --------------------------------------------------------------------------


def test_the_reports_of_the_countries_asked_for_are_collected(data_lake, repository):
    outcome = esef.update_bronze_esef(data_lake, ["at"])

    assert outcome["collected"] == 2
    assert outcome["unavailable"] == 1  # the filing without xBRL-JSON
    assert sorted(repository.downloads()) == sorted(
        [AT["attributes"]["json_url"], AT2["attributes"]["json_url"]])
    path = esef.path_bronze_report(data_lake, "AT", name(AT))
    with open(path, encoding="utf-8") as handle:
        assert json.load(handle) == report_of(AT)
    record = esef.read_report_records(data_lake)[name(AT)]
    assert record["country"] == "AT"
    assert record["facts"] == 4
    assert record["processed"] == "2026-08-04T16:48:35.245129"
    assert record["sha256"] == hashlib.sha256(
        repository.files[AT["attributes"]["json_url"]]).hexdigest()


def test_a_report_is_not_downloaded_again_until_it_is_processed_anew(
        data_lake, repository):
    esef.update_bronze_esef(data_lake, ["AT"])
    repository.requests.clear()

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["collected"] == 0 and outcome["unchanged"] == 2
    assert repository.downloads() == []

    # The repository converts the filing anew, and its index says so.
    repository.indexes[2026] = with_filings(2026, reprocessed(AT))
    changed = report_of(AT)
    changed["facts"]["f-1"]["value"] = "1.0"
    repository.files[AT["attributes"]["json_url"]] = body(changed)
    os.remove(esef.path_index_record(data_lake, 2026))

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["collected"] == 1 and outcome["unchanged"] == 1
    assert repository.downloads() == [AT["attributes"]["json_url"]]


def test_only_the_years_asked_for_are_collected(data_lake, repository):
    outcome = esef.update_bronze_esef(data_lake, ["AT", "DE"], years=[2025])

    assert outcome["collected"] == 2
    assert sorted(repository.downloads()) == sorted(
        [AT2["attributes"]["json_url"], DE["attributes"]["json_url"]])


def test_a_report_that_fails_is_set_aside_and_the_others_are_collected(
        data_lake, repository, caplog):
    repository.files[AT["attributes"]["json_url"]] = b"<html>Not found</html>"
    del repository.files[AT2["attributes"]["json_url"]]

    with caplog.at_level(logging.ERROR, logger="fincommons"):
        outcome = esef.update_bronze_esef(data_lake, ["AT", "DE"])

    assert outcome["collected"] == 1
    assert outcome["failed"] == 2
    assert sorted(outcome["failed_sources"]) == sorted(
        [f"bronze/{name(AT)}", f"bronze/{name(AT2)}"])
    assert not os.path.exists(esef.path_bronze_report(data_lake, "AT", name(AT)))
    assert set(esef.read_report_records(data_lake)) == {name(DE)}


def test_an_answer_that_is_not_a_report_does_not_replace_the_one_held(
        data_lake, repository):
    esef.update_bronze_esef(data_lake, ["AT"])
    repository.files[AT["attributes"]["json_url"]] = body({"errors": []})
    record = esef.path_report_record(data_lake, name(AT))
    os.remove(record)

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["failed_sources"] == [f"bronze/{name(AT)}"]
    with open(esef.path_bronze_report(data_lake, "AT", name(AT)),
              encoding="utf-8") as handle:
        assert json.load(handle) == report_of(AT)


def test_a_request_turned_away_for_the_moment_is_made_again(
        data_lake, repository, monkeypatch):
    """The repository limits the rate of requests, and turned the first
    collection into a real lake away for six of its nine indexes."""
    waits = []
    monkeypatch.setattr(esef_bronze.time, "sleep", waits.append)
    answer = repository.get
    busy = {"left": 2}

    def get(url, **kwargs):
        if busy["left"]:
            busy["left"] -= 1
            return FakeResponse(429, headers={"Retry-After": "3"})
        return answer(url, **kwargs)

    monkeypatch.setattr(requests, "get", get)

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["failed"] == 0 and outcome["indexes"] == 3
    assert waits == [3.0, 3.0]


def test_a_request_turned_away_every_time_fails_after_a_bounded_wait(
        data_lake, repository, monkeypatch):
    waits = []
    monkeypatch.setattr(esef_bronze.time, "sleep", waits.append)
    monkeypatch.setattr(esef_bronze, "RETRIES", 2)
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: FakeResponse(
        429, headers={"Retry-After": "86400"}))

    outcome = esef.update_bronze_esef(data_lake, ["AT"])

    assert outcome["indexes"] == 0
    assert outcome["failed"] == 2026 - esef.FIRST_YEAR + 1
    assert set(waits) == {esef_bronze.MAX_WAIT}
    assert len(waits) == 2 * outcome["failed"]


def test_an_address_that_leaves_the_repository_is_refused():
    assert esef.file_url("/x/y.json") == esef.BASE_URL + "/x/y.json"
    with pytest.raises(ValueError, match="is not one of"):
        esef.file_url("https://elsewhere.example/y.json")


def test_the_user_agent_names_the_contact_where_one_is_given(data_lake, repository):
    esef.update_bronze_esef(data_lake, ["AT"], contact="data@example.org")

    agents = {headers["User-Agent"] for _, headers in repository.requests}
    assert agents == {"fincommons (data@example.org)"}


# The packages
# --------------------------------------------------------------------------


def test_a_package_is_collected_where_asked_for_and_checked_against_the_index(
        data_lake, repository):
    package = b"PK\x03\x04 package of the filing"
    digest = hashlib.sha256(package).hexdigest()
    signed = json.loads(json.dumps(AT))
    signed["attributes"]["sha256"] = digest
    repository.indexes = {2026: with_filings(2026, signed)}
    repository.files[signed["attributes"]["package_url"]] = package

    outcome = esef.update_bronze_esef(data_lake, ["AT"], packages=True)

    assert outcome["packages"] == 1 and outcome["failed"] == 0
    target = os.path.join(esef.path_bronze_packages(data_lake), "COUNTRY=AT",
                          f"{name(signed)}.zip")
    with open(target, "rb") as handle:
        assert handle.read() == package

    # The package the layer holds has the SHA-256 the index gives.
    repository.requests.clear()
    assert esef.update_bronze_esef(data_lake, ["AT"], packages=True)["packages"] == 0
    assert repository.downloads() == []

    # A package that is not the one the index describes is refused.
    os.remove(target)
    repository.files[signed["attributes"]["package_url"]] = b"something else"
    outcome = esef.update_bronze_esef(data_lake, ["AT"], packages=True)
    assert outcome["failed_sources"] == [f"bronze/{name(signed)}"]
    assert not os.path.exists(target)


def test_the_directory_tree_can_be_set_up_twice(data_lake):
    esef.create_directory_tree(data_lake)

    assert os.path.isdir(esef.path_bronze_reports(data_lake))
    assert os.path.isdir(esef.path_bronze_indexes(data_lake))


def test_a_collection_records_the_time_it_was_made(data_lake, repository):
    filings = {r["FILING"]: r for r in esef.filings_from_index(INDEXES[2026])}
    now = datetime.datetime(2026, 10, 28, 12, 0)

    esef.collect_report(data_lake, filings[name(AT)], now=now)

    record = esef.read_report_records(data_lake)[name(AT)]
    assert record["collected"] == "2026-10-28T12:00:00"
