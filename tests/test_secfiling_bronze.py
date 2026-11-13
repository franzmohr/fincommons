"""Collecting the company files.

Nothing here goes to the network. The Commission is stood in for at the one
seam the module has for it, :func:`fincommons.secfiling.bronze.fetch`, which
is what lets the tests cover the answers that are awkward to arrange against
the real thing: a file that has not changed, a registrant that has tagged
nothing, and an error page returned with a success status.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from fincommons.secfiling import bronze, common

DATA = Path(__file__).parent / "data" / "secfiling"
COMPANY = json.loads((DATA / "CIK0001177394.json").read_text(encoding="utf-8"))
CIK = "0001177394"


@pytest.fixture
def config(tmp_path):
    """A configuration file pointing at an empty data lake."""
    import yaml

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"secfiling": {name: str(base / name) for name in ("bronze", "silver")}}
    ))
    config_path = str(config_path)
    common.create_directory_tree(config_path)
    return config_path


def answer(monkeypatch, status=200, body=None, headers=None):
    """Stand in for the Commission with one answer."""
    content = json.dumps(body if body is not None else COMPANY).encode()
    monkeypatch.setattr(bronze, "fetch",
                        lambda url, contact, hdrs=None: (status, content,
                                                         headers or {}))


# Identifying the client
# --------------------------------------------------------------------------


def test_a_request_without_a_contact_is_refused_before_it_is_made():
    """The Commission answers an anonymous request with 403 and no file, so
    a run without a contact collects nothing; it is better told here."""
    with pytest.raises(ValueError, match="identify itself"):
        bronze.user_agent(None)
    with pytest.raises(ValueError):
        bronze.user_agent("   ")


def test_the_contact_is_carried_in_the_user_agent():
    assert "tests@example.org" in bronze.user_agent("tests@example.org")


# Collecting one company
# --------------------------------------------------------------------------


def test_a_company_file_is_stored_as_published(monkeypatch, config):
    answer(monkeypatch, headers={"Last-Modified": "Wed, 01 Oct 2026 10:00:00 GMT"})

    assert bronze.collect_company(config, CIK, "t@example.org") == "collected"

    stored = json.loads(Path(common.path_bronze_company(config, CIK)).read_text())
    assert stored["cik"] == COMPANY["cik"]
    assert bronze.collected_companies(config) == [CIK]


def test_what_the_server_said_is_recorded(monkeypatch, config):
    answer(monkeypatch, headers={"Last-Modified": "Wed, 01 Oct 2026 10:00:00 GMT"})
    bronze.collect_company(config, CIK, "t@example.org")

    record = common.read_json(common.path_company_record(config, CIK))

    assert record["last_modified"] == "Wed, 01 Oct 2026 10:00:00 GMT"
    assert record["status"] == 200


def test_a_file_that_has_not_changed_is_not_downloaded_again(monkeypatch, config):
    answer(monkeypatch, headers={"Last-Modified": "Wed, 01 Oct 2026 10:00:00 GMT"})
    bronze.collect_company(config, CIK, "t@example.org")

    asked = {}

    def unchanged(url, contact, hdrs=None):
        asked.update(hdrs or {})
        return 304, b"", {}

    monkeypatch.setattr(bronze, "fetch", unchanged)

    assert bronze.collect_company(config, CIK, "t@example.org") == "unchanged"
    assert asked["If-Modified-Since"] == "Wed, 01 Oct 2026 10:00:00 GMT"


def test_the_file_is_asked_for_again_where_the_run_insists(monkeypatch, config):
    answer(monkeypatch, headers={"Last-Modified": "Wed, 01 Oct 2026 10:00:00 GMT"})
    bronze.collect_company(config, CIK, "t@example.org")

    asked = {}
    monkeypatch.setattr(bronze, "fetch", lambda url, contact, hdrs=None:
                        (asked.update(hdrs or {}) or
                         (200, json.dumps(COMPANY).encode(), {})))
    bronze.collect_company(config, CIK, "t@example.org", force=True)

    assert "If-Modified-Since" not in asked


def test_a_registrant_that_has_tagged_nothing_is_not_a_failure(monkeypatch, config):
    """Not every registrant has tagged a filing, and the Commission says so
    with 404, which is an answer."""
    monkeypatch.setattr(bronze, "fetch",
                        lambda url, contact, hdrs=None: (404, b"", {}))

    assert bronze.collect_company(config, CIK, "t@example.org") == "absent"
    assert not os.path.exists(common.path_bronze_company(config, CIK))


def test_an_answer_that_is_not_a_company_file_is_refused(monkeypatch, config):
    """A server error can come back with status 200, and a run that stored
    it would have a company file that is an error page."""
    answer(monkeypatch, body={"error": "something went wrong"})

    with pytest.raises(ValueError, match="not a company file"):
        bronze.collect_company(config, CIK, "t@example.org")

    assert not os.path.exists(common.path_bronze_company(config, CIK))


def test_an_error_status_is_raised(monkeypatch, config):
    monkeypatch.setattr(bronze, "fetch",
                        lambda url, contact, hdrs=None: (500, b"", {}))

    with pytest.raises(RuntimeError, match="500"):
        bronze.collect_company(config, CIK, "t@example.org")


# Collecting many
# --------------------------------------------------------------------------


def test_a_run_naming_no_registrant_is_refused(config):
    """Collecting every registrant one request at a time is what the bulk
    archive exists to prevent, so it is asked for and not stumbled into."""
    with pytest.raises(ValueError, match="bulk"):
        bronze.update_bronze_secfiling(config, contact="t@example.org")


def test_a_company_that_fails_does_not_stop_the_others(monkeypatch, config):
    def flaky(url, contact, hdrs=None):
        if "0000000002" in url:
            return 500, b"", {}
        return 200, json.dumps(COMPANY).encode(), {}

    monkeypatch.setattr(bronze, "fetch", flaky)

    with pytest.raises(RuntimeError, match="could not be collected"):
        bronze.update_bronze_secfiling(config, ciks=[1, 2, 3],
                                       contact="t@example.org")

    assert bronze.collected_companies(config) == ["0000000001", "0000000003"]


def test_the_bulk_archive_is_unpacked_into_the_lake(monkeypatch, config):
    import io, zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CIK0001177394.json", json.dumps(COMPANY))
        archive.writestr("CIK0001114448.json", json.dumps(COMPANY))
        archive.writestr("README.txt", "not a company file")
    monkeypatch.setattr(bronze, "fetch",
                        lambda url, contact, hdrs=None: (200, buffer.getvalue(), {}))

    assert bronze.collect_bulk(config, "t@example.org") == 2
    assert bronze.collected_companies(config) == ["0001114448", "0001177394"]


def test_the_archive_can_be_narrowed_to_the_registrants_wanted(monkeypatch, config):
    import io, zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CIK0001177394.json", json.dumps(COMPANY))
        archive.writestr("CIK0001114448.json", json.dumps(COMPANY))
    monkeypatch.setattr(bronze, "fetch",
                        lambda url, contact, hdrs=None: (200, buffer.getvalue(), {}))

    assert bronze.collect_bulk(config, "t@example.org", ciks=[1177394]) == 1
    assert bronze.collected_companies(config) == ["0001177394"]


# Waiting when asked to
# --------------------------------------------------------------------------


class _Answer:
    def __init__(self, headers):
        self.headers = headers


def test_the_wait_is_what_the_server_asks_for():
    assert bronze._wait(_Answer({"Retry-After": "7"}), 0) == 7.0


def test_the_wait_grows_where_the_server_says_nothing():
    assert bronze._wait(_Answer({}), 0) < bronze._wait(_Answer({}), 3)


def test_the_wait_is_bounded():
    assert bronze._wait(_Answer({"Retry-After": "100000"}), 0) == bronze.MAX_WAIT
