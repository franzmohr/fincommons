"""Tests of the entity master data module.

The module joins five registers on the LEI, so the tests are about the rules
that govern those joins: which register describes an entity that appears in
more than one of them, which relationship of the GLEIF Level 2 file becomes the
parent and which the manager, and what a proprietary list displaces. The
landing zone fixtures are written inline, in the shapes the publishers use, so
that every assertion refers to an entity that is known by construction.

Nothing here reaches GLEIF or GitHub. The two downloads are given a response of
their own, so what is asserted is what the module does with an answer rather
than what those two happen to be serving that day.
"""

from __future__ import annotations

import io
import logging
import os
import uuid
import zipfile

import pandas
import pytest
import requests

import fincommons.entitymasterdata as em


@pytest.fixture
def data_lake(tmp_path):
    """A configuration file pointing at an empty entity master data lake."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "entitymasterdata:\n"
        f"  landing: {tmp_path / 'landing'}\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    em.create_directory_tree(config_path)
    return config_path


# Writing the landing zone
# --------------------------------------------------------------------------


def zipped_csv(name, frame):
    """A GLEIF golden copy file: one csv file in a zip archive."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, frame.to_csv(index=False))
    return buffer.getvalue()


def write_gleif_level_1(config_path, lei, legal_name, country,
                        file_name="20260724-0000-lei2.zip"):
    """Write a Level 1 golden copy, with a column the import does not read."""
    frame = pandas.DataFrame(
        {
            "LEI": list(lei),
            "Entity.LegalName": list(legal_name),
            "Entity.LegalAddress.Country": list(country),
            "Entity.LegalForm.EntityLegalFormCode": ["8888"] * len(lei),
        }
    )
    path = os.path.join(em.path_landing_gleif_level_1(config_path), file_name)
    with open(path, "wb") as f:
        f.write(zipped_csv("lei2.csv", frame))
    return path


def write_gleif_level_2(config_path, relationships, file_name="20260724-0000-rr.zip"):
    """Write a Level 2 golden copy from ``(start, end, type)`` triples.

    A fourth element states the status of the relationship, for the tests that
    turn on it. Where it is left out the relationship is a current one, which
    is what the rest of the suite is about.
    """
    frame = pandas.DataFrame(
        {
            "Relationship.StartNode.NodeID": [r[0] for r in relationships],
            "Relationship.StartNode.NodeIDType": ["LEI"] * len(relationships),
            "Relationship.EndNode.NodeID": [r[1] for r in relationships],
            "Relationship.EndNode.NodeIDType": ["LEI"] * len(relationships),
            "Relationship.RelationshipType": [r[2] for r in relationships],
            "Relationship.RelationshipStatus": [
                r[3] if len(r) > 3 else "ACTIVE" for r in relationships
            ],
        }
    )
    path = os.path.join(em.path_landing_gleif_level_2(config_path), file_name)
    with open(path, "wb") as f:
        f.write(zipped_csv("rr.csv", frame))
    return path


def write_ecb_investment_fund(config_path, leis, file_name="IF_2026_03.xlsx",
                              ucits="UCITS compliant"):
    """Write the ECB list of investment funds.

    ``ucits`` is what the register says about compliance, which is what the
    sector of a fund is read off: a UCITS becomes ``UCIT``, an AIF ``AIFD``,
    and anything else ``INVF``. It can be given per fund or once for all of
    them.
    """
    compliance = list(ucits) if isinstance(ucits, (list, tuple)) else [ucits] * len(leis)
    frame = pandas.DataFrame(
        {
            "LEI": list(leis),
            "Investment policy": ["Bond funds"] * len(leis),
            "Type of investment fund": ["Exchange traded fund (ETF)"] * len(leis),
            "UCITS compliance": compliance,
        }
    )
    path = os.path.join(em.path_landing_ecb_investment_fund(config_path), file_name)
    frame.to_excel(path, index=False)
    return path


def write_ecb_mfi(config_path, rows, file_name="20260724.xlsx", postal=None):
    """Write an MFI register from ``(lei, category)`` pairs."""
    frame = pandas.DataFrame(
        {
            "LEI": [r[0] for r in rows],
            "CATEGORY": [r[1] for r in rows],
            "HEAD_LEI": [None] * len(rows),
            "BOX": [None] * len(rows),
            "POSTAL": postal if postal is not None else ["1010"] * len(rows),
        }
    )
    path = os.path.join(em.path_landing_ecb_mfi(config_path), file_name)
    frame.to_excel(path, index=False)
    return path


def write_eiopa(config_path, leis, file_name="20260724.xlsx"):
    frame = pandas.DataFrame(
        {
            "Identification code": [f"IC{i}" for i in range(len(leis))],
            "LEI": list(leis),
            "Street Name": ["Wipplingerstrasse"] * len(leis),
            "Street Number": ["7"] * len(leis),
            "Post Code": ["1010"] * len(leis),
            "Registration start date": ["01/01/2020"] * len(leis),
            "Registration end date": ["31/12/2030"] * len(leis),
            "Operation Start Date": ["01/01/2020"] * len(leis),
            "Operation End Date": ["31/12/2030"] * len(leis),
        }
    )
    path = os.path.join(em.path_landing_eiopa_insurance_undertaking(config_path), file_name)
    frame.to_excel(path, index=False)
    return path


def write_iorp(config_path, leis, file_name="20260724.xlsx"):
    """Write EIOPA's register of institutions for occupational retirement provision.

    The columns are the ones the Bronze import settles the type of, so that a
    change to that list is caught here rather than against the real export.
    """
    frame = pandas.DataFrame(
        {
            "Name of NCA": ["FMA"] * len(leis),
            "Official name of the entity": [f"IORP {lei}" for lei in leis],
            "Home Country": ["AT"] * len(leis),
            "EU Country where the entity operates": ["AT"] * len(leis),
            "Type of IORP": ["Pensionskasse"] * len(leis),
            "Street name": ["Wipplingerstrasse"] * len(leis),
            "Street number": ["7"] * len(leis),
            "Post Code": ["1010"] * len(leis),
            "City": ["Wien"] * len(leis),
            "Legal name of the managing entity": [None] * len(leis),
            "Address of the managing entity": [None] * len(leis),
            "Cross border status": ["National"] * len(leis),
            "Cross border activity": [None] * len(leis),
            "LEI": list(leis),
            "Identification Code": [f"IORP{i}" for i in range(len(leis))],
            "Website address": [None] * len(leis),
            "Registration Start Date": ["01/01/2020"] * len(leis),
            "Registration End Date": ["31/12/2030"] * len(leis),
        }
    )
    path = os.path.join(em.path_landing_eiopa_iorp(config_path), file_name)
    frame.to_excel(path, index=False)
    return path


def write_ccp(config_path, rows):
    """Write the list of CCPs from ``(lei, sector, subsector)`` triples."""
    frame = pandas.DataFrame(
        {
            "LEI": [r[0] for r in rows],
            "SECTOR": [r[1] for r in rows],
            "SUBSECTOR": [r[2] for r in rows],
        }
    )
    path = os.path.join(em.path_landing_esma_ccp(config_path), "ccp_list.csv")
    frame.to_csv(path, index=False)
    return path


def write_internal(config_path, frame):
    """Write the optional proprietary list as the CSV the module expects."""
    path = os.path.join(
        em.path_bronze_internal_entitymasterdata(config_path),
        "entitymasterdata_internal.csv",
    )
    frame.to_csv(path, index=False)
    return path


def populated(config_path, leis=("LEI1", "LEI2"), ccp=True):
    """A landing zone holding one file of every register, imported."""
    write_gleif_level_1(
        config_path,
        lei=leis,
        legal_name=[f"Entity {lei}" for lei in leis],
        country=["AT"] * len(leis),
    )
    write_gleif_level_2(config_path, [])
    write_ecb_investment_fund(config_path, [])
    write_ecb_mfi(config_path, [])
    write_eiopa(config_path, [])
    write_iorp(config_path, [])
    if ccp:
        write_ccp(config_path, [])
    em.update_from_landing_to_bronze(config_path)


def main(spark, config_path, catch_all=False):
    """Read the entity master data set back as a pandas frame.

    The catch-all entities every table carries are left out unless they are
    asked for, since most tests are about the entities they put there.
    """
    frame = em.connect_to_entitymasterdata(spark, config_path).toPandas()
    if catch_all:
        return frame
    return frame[~frame.LEI.isin(list(em.CATCH_ALL_ENTITIES))].reset_index(drop=True)


def public(spark, config_path):
    return em.connect_to_entitymasterdata(spark, config_path, "public").toPandas()


def entity(frame, lei):
    """The single row describing one LEI."""
    rows = frame[frame.LEI == lei]
    assert len(rows) == 1, f"expected one row for {lei}, got {len(rows)}"
    return rows.iloc[0]


# The data lake
# --------------------------------------------------------------------------


def test_the_directory_tree_is_created(data_lake):
    for folder in [
        em.path_landing_ecb_investment_fund(data_lake),
        em.path_landing_ecb_mfi(data_lake),
        em.path_landing_eiopa_insurance_undertaking(data_lake),
        em.path_landing_eiopa_iorp(data_lake),
        em.path_landing_esma_ccp(data_lake),
        em.path_landing_gleif_level_1(data_lake),
        em.path_landing_gleif_level_2(data_lake),
        em.path_bronze_ecb_investment_fund(data_lake),
        em.path_bronze_ecb_mfi(data_lake),
        em.path_bronze_eiopa_insurance_undertaking(data_lake),
        em.path_bronze_eiopa_iorp(data_lake),
        em.path_bronze_esma_ccp(data_lake),
        em.path_bronze_gleif_level_1(data_lake),
        em.path_bronze_gleif_level_2(data_lake),
        em.path_bronze_internal_entitymasterdata(data_lake),
        em.path_silver_base(data_lake),
    ]:
        assert os.path.isdir(folder)


def test_creating_the_tree_twice_is_harmless(data_lake):
    em.create_directory_tree(data_lake)


def test_the_tree_is_created_below_parents_that_do_not_exist(tmp_path):
    """The configuration may put the layers below a directory that has not been
    created yet, and setting the tree up should not require creating the path
    to it by hand first."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "entitymasterdata:\n"
        f"  landing: {tmp_path / 'volume' / 'entitymasterdata' / 'landing'}\n"
        f"  bronze: {tmp_path / 'volume' / 'entitymasterdata' / 'bronze'}\n"
        f"  silver: {tmp_path / 'volume' / 'entitymasterdata' / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)

    em.create_directory_tree(config_path)

    assert os.path.isdir(em.path_landing_gleif_level_1(config_path))
    assert os.path.isdir(em.path_silver_base(config_path))


def test_an_unknown_table_is_reported_as_such(spark, data_lake):
    with pytest.raises(ValueError, match="not recognised"):
        em.connect_to_entitymasterdata(spark, data_lake, "gold")


def test_a_table_that_has_never_been_built_is_reported_as_such(spark, data_lake):
    """Spark's own answer is that the path does not exist, which is true and
    silent about the update that would create it."""
    with pytest.raises(FileNotFoundError, match="Silver layer has to be updated"):
        em.connect_to_entitymasterdata(spark, data_lake)


def test_the_module_does_not_re_export_what_it_imported(data_lake):
    """The package is assembled from star imports, so a submodule without an
    ``__all__`` puts the modules it imported on the package as well. That would
    make ``entitymasterdata.pandas`` part of the surface, and
    ``entitymasterdata.logger`` whichever submodule was imported last."""
    for name in ["os", "yaml", "requests", "pandas", "pyarrow", "zipfile",
                 "logging", "func", "Window", "logger", "context", "pipeline_run"]:
        assert not hasattr(em, name), f"{name} is re-exported by the package"


# The downloads
# --------------------------------------------------------------------------


#: A readable zip archive, for the downloads whose target is one.
ARCHIVE = zipped_csv("lei2.csv", pandas.DataFrame({"LEI": ["LEI1"]}))


class FakeResponse:
    """The part of a streamed ``requests`` response the module uses.

    The module reads the answer through a ``with`` block, so the double is a
    context manager and records having been closed on the way out. ``status``
    is what :meth:`raise_for_status` goes on, and an answer that fails half way
    through is expressed by ``fail_after``.
    """

    def __init__(self, payload, status=200, fail_after=None):
        self.payload = payload
        self.status_code = status
        self.fail_after = fail_after
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} for url")

    def iter_content(self, chunk_size):
        for written, start in enumerate(range(0, len(self.payload), chunk_size)):
            if self.fail_after is not None and written == self.fail_after:
                raise requests.ConnectionError("the connection was dropped")
            yield self.payload[start:start + chunk_size]

    def close(self):
        self.closed = True


def serve(monkeypatch, response):
    """Answer every request of the module with ``response``.

    The keyword arguments are taken and discarded rather than spelled out, so
    that the double does not have to be revisited when the module passes the
    request one more option.
    """
    calls = []

    def get(url, **options):
        calls.append((url, options))
        return response

    monkeypatch.setattr("fincommons.entitymasterdata.landing.requests.get", get)
    return calls


def test_a_gleif_file_keeps_the_name_it_was_published_under(monkeypatch, data_lake):
    """The Bronze import reads the reference date off the first eight
    characters of the name, so the name GLEIF gave the file has to survive the
    download."""
    serve(monkeypatch, FakeResponse(ARCHIVE))

    em.download_gleif_level_1(
        data_lake,
        "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/"
        "1255025/20260724-0000-gleif-goldencopy-lei2-golden-copy.csv.zip#",
    )

    assert os.listdir(em.path_landing_gleif_level_1(data_lake)) == [
        "20260724-0000-gleif-goldencopy-lei2-golden-copy.csv.zip"
    ]


def test_the_gleif_file_name_survives_a_query_string():
    name = em.gleif_file_name(
        "https://example.org/a/b/20260724-0000-rr-golden-copy.csv.zip?token=1"
    )
    assert name == "20260724-0000-rr-golden-copy.csv.zip"


def test_a_link_to_an_archive_spelled_in_upper_case_is_accepted():
    name = em.gleif_file_name("https://example.org/a/20260724-0000-LEI2.CSV.ZIP")
    assert name == "20260724-0000-LEI2.CSV.ZIP"


def test_the_list_of_ccps_is_read_from_a_fixed_commit():
    """A branch would import whatever was last pushed to it; a commit is the
    version that was reviewed."""
    commit = em.ESMA_CCP_URL.split("/")[-3]
    assert len(commit) == 40 and all(c in "0123456789abcdef" for c in commit)


def test_the_update_carries_on_when_the_list_of_ccps_cannot_be_downloaded(
    monkeypatch, data_lake, caplog
):
    """The list is downloaded without being asked for, so a cluster without
    internet access must still import the registers and build the tables."""
    from fincommons.entitymasterdata import silver

    def unreachable(config_path):
        raise requests.ConnectionError("no route to host")

    stages = []
    monkeypatch.setattr(silver, "download_latest_gleif",
                        lambda config_path, data_sets: unreachable(config_path))
    monkeypatch.setattr(silver, "download_esma_ccp", unreachable)
    monkeypatch.setattr(silver, "download_eiopa_insurance_undertaking", unreachable)
    monkeypatch.setattr(silver, "download_eiopa_iorp", unreachable)
    monkeypatch.setattr(silver, "download_ecb_mfi", unreachable)
    monkeypatch.setattr(silver, "download_ecb_investment_fund", unreachable)
    monkeypatch.setattr(silver, "download_ecb_fvc", unreachable)
    monkeypatch.setattr(silver, "download_ecb_psri", unreachable)
    monkeypatch.setattr(silver, "update_from_landing_to_bronze",
                        lambda config_path: stages.append("bronze"))
    monkeypatch.setattr(silver, "update_silver_entitymasterdata_public",
                        lambda **kwargs: stages.append("public"))
    monkeypatch.setattr(silver, "update_silver_entitymasterdata",
                        lambda **kwargs: stages.append("main"))

    with caplog.at_level(logging.ERROR, logger="fincommons"):
        silver._update_entitymasterdata(None, data_lake, None, None, None)

    assert stages == ["bronze", "public", "main"]
    failed = [r.getMessage() for r in caplog.records
              if "could not be downloaded" in r.getMessage()]
    assert len(failed) == 8


def test_a_link_that_is_not_a_zip_file_is_refused():
    """Passing the page rather than the file is the mistake worth catching, and
    catching it here beats an unreadable archive in the landing zone."""
    with pytest.raises(ValueError, match="zip file"):
        em.gleif_file_name("https://www.gleif.org/en/lei-data/gleif-golden-copy")


def test_the_response_is_closed_when_the_download_fails(monkeypatch, data_lake):
    response = FakeResponse(ARCHIVE)
    serve(monkeypatch, response)

    with pytest.raises(FileNotFoundError):
        em.landing.download_file("https://example.org/x.zip", "/no/such/folder/x.zip")

    assert response.closed


def test_every_request_is_given_a_timeout(monkeypatch, data_lake):
    """A stalled connection would otherwise hold the run indefinitely."""
    calls = serve(monkeypatch, FakeResponse(ARCHIVE))

    em.download_gleif_level_1(data_lake, "https://example.org/20260724-lei2.csv.zip")

    assert [options["timeout"] for _, options in calls] == [em.landing.TIMEOUT]


def test_an_error_answer_is_not_stored_as_the_register(monkeypatch, data_lake):
    """The failure to expect. GLEIF changes the address of the golden copy with
    every release, so a link taken from the download page some time ago is
    stale, and the answer is an error page. Stored under the name of the
    register it would be read as the register being there, and every later run
    of the Bronze import would break on it until it was removed by hand."""
    serve(monkeypatch, FakeResponse(b"<html>404 Not Found</html>", status=404))

    with pytest.raises(requests.HTTPError):
        em.download_gleif_level_1(
            data_lake, "https://example.org/20260724-0000-lei2-golden-copy.csv.zip"
        )

    assert os.listdir(em.path_landing_gleif_level_1(data_lake)) == []


def test_an_answer_that_is_not_an_archive_is_not_stored_as_the_register(monkeypatch, data_lake):
    """The same page served with a success status, which a proxy or a login
    wall does."""
    serve(monkeypatch, FakeResponse(b"<html>sign in to continue</html>"))

    with pytest.raises(zipfile.BadZipFile):
        em.download_gleif_level_2(
            data_lake, "https://example.org/20260724-0000-rr-golden-copy.csv.zip"
        )

    assert os.listdir(em.path_landing_gleif_level_2(data_lake)) == []


def test_an_interrupted_download_leaves_nothing_behind(monkeypatch, data_lake):
    """Not even under the name that marks it incomplete, so that the next
    attempt starts from an empty landing zone. The chunks are small so that the
    connection drops after some of the file has already been written, which is
    the case the partial name is there for."""
    serve(monkeypatch, FakeResponse(ARCHIVE, fail_after=3))
    target = os.path.join(em.path_landing_gleif_level_1(data_lake), "20260724.zip")

    with pytest.raises(requests.ConnectionError):
        em.landing.download_file("https://example.org/x.zip", target, chunk_size=16)

    assert os.listdir(em.path_landing_gleif_level_1(data_lake)) == []


def test_the_list_of_ccps_is_downloaded_into_the_landing_zone(monkeypatch, data_lake):
    serve(monkeypatch, FakeResponse(b"LEI,SECTOR,SUBSECTOR\nLEI1,CCP,EUCCP\n"))

    em.download_esma_ccp(data_lake)

    assert os.listdir(em.path_landing_esma_ccp(data_lake)) == ["ccp_list.csv"]


def test_an_error_answer_does_not_replace_the_list_of_ccps(monkeypatch, data_lake):
    """The list is not named after a reference date, so a failed download would
    overwrite the copy already held rather than sit beside it."""
    serve(monkeypatch, FakeResponse(b"LEI,SECTOR,SUBSECTOR\nLEI1,CCP,EUCCP\n"))
    em.download_esma_ccp(data_lake)

    serve(monkeypatch, FakeResponse(b"<html>502 Bad Gateway</html>", status=502))
    with pytest.raises(requests.HTTPError):
        em.download_esma_ccp(data_lake)

    path = os.path.join(em.path_landing_esma_ccp(data_lake), "ccp_list.csv")
    assert os.listdir(em.path_landing_esma_ccp(data_lake)) == ["ccp_list.csv"]
    assert "LEI1,CCP,EUCCP" in io.open(path, encoding="utf-8").read()


# GLEIF's latest golden copy
# --------------------------------------------------------------------------

GLEIF_FILES = "https://goldencopy.gleif.org/storage/golden-copy-files/2026/10/28/"
LATEST = {"data": {
    "publish_date": "2026-10-28 08:00:00",
    "lei2": {"full_file": {"csv": {"url": GLEIF_FILES
             + "1294040/20261028-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"}}},
    "rr": {"full_file": {"csv": {"url": GLEIF_FILES
           + "1294085/20261028-0800-gleif-goldencopy-rr-golden-copy.csv.zip"}}},
}}


class FakeJson(FakeResponse):
    def __init__(self, document):
        super().__init__(b"")
        self.document = document

    def json(self):
        return self.document


def serve_gleif(monkeypatch, latest=LATEST):
    """GLEIF: the Golden Copy API, and an archive for every file."""
    calls = []

    def get(url, **options):
        calls.append(url)
        if url == em.GLEIF_LATEST_URL:
            return FakeJson(latest)
        return FakeResponse(ARCHIVE)

    monkeypatch.setattr("fincommons.entitymasterdata.landing.requests.get", get)
    return calls


def test_the_latest_golden_copy_is_found_through_the_api(monkeypatch, data_lake):
    calls = serve_gleif(monkeypatch)

    stored = em.download_latest_gleif(data_lake)

    assert stored == ["20261028-0800-gleif-goldencopy-lei2-golden-copy.csv.zip",
                      "20261028-0800-gleif-goldencopy-rr-golden-copy.csv.zip"]
    assert calls[0] == em.GLEIF_LATEST_URL
    assert os.listdir(em.path_landing_gleif_level_1(data_lake)) == [stored[0]]
    assert os.listdir(em.path_landing_gleif_level_2(data_lake)) == [stored[1]]


def test_a_golden_copy_of_a_day_the_bronze_layer_holds_is_not_downloaded(
        monkeypatch, data_lake):
    """Level 1 is half a gigabyte, and GLEIF publishes three times a day."""
    calls = serve_gleif(monkeypatch)
    bronze = em.path_bronze_gleif_level_1(data_lake)
    os.makedirs(bronze, exist_ok=True)
    open(os.path.join(bronze, "20261028.parquet"), "wb").close()

    stored = em.download_latest_gleif(data_lake)

    assert stored == ["20261028-0800-gleif-goldencopy-rr-golden-copy.csv.zip"]
    assert calls == [em.GLEIF_LATEST_URL, LATEST["data"]["rr"]["full_file"]["csv"]["url"]]


def test_only_the_data_sets_asked_for_are_downloaded(monkeypatch, data_lake):
    calls = serve_gleif(monkeypatch)

    assert em.download_latest_gleif(data_lake, ("rr",)) == [
        "20261028-0800-gleif-goldencopy-rr-golden-copy.csv.zip"]
    assert em.download_latest_gleif(data_lake, ()) == []
    assert calls.count(em.GLEIF_LATEST_URL) == 1


def test_an_api_that_names_no_file_is_refused(monkeypatch, data_lake):
    serve_gleif(monkeypatch, {"data": {"lei2": {}}})

    with pytest.raises(ValueError, match="names no csv file"):
        em.download_latest_gleif(data_lake)

    assert os.listdir(em.path_landing_gleif_level_1(data_lake)) == []


def test_a_gleif_link_given_to_the_update_is_used_rather_than_the_latest(
        monkeypatch, data_lake):
    from fincommons.entitymasterdata import silver
    asked = []
    monkeypatch.setattr(silver, "download_gleif_level_1",
                        lambda config_path, url: asked.append(("L1", url)))
    monkeypatch.setattr(silver, "download_latest_gleif",
                        lambda config_path, data_sets: asked.append(("latest", data_sets)))
    for name in ("download_esma_ccp", "download_ecb_mfi", "download_ecb_investment_fund",
                 "download_ecb_fvc", "download_ecb_psri",
                 "download_eiopa_insurance_undertaking", "download_eiopa_iorp"):
        monkeypatch.setattr(silver, name, lambda config_path: None)
    for name in ("update_from_landing_to_bronze",):
        monkeypatch.setattr(silver, name, lambda config_path: None)
    monkeypatch.setattr(silver, "update_silver_entitymasterdata_public", lambda **kw: None)
    monkeypatch.setattr(silver, "update_silver_entitymasterdata", lambda **kw: None)

    silver._update_entitymasterdata(None, data_lake, "https://x/20261027-l1.csv.zip",
                                    None, None)

    assert asked == [("L1", "https://x/20261027-l1.csv.zip"), ("latest", ("rr",))]


# EIOPA's registers
# --------------------------------------------------------------------------

#: A register page of EIOPA as far as the export reads it: the form, its
#: hidden fields, and the "CSV" and "Excel" links that post it back.
EIOPA_PAGE = """
<form method="post" action="./register-of-iorps?TermStoreId=a&amp;TermId=b" id="aspnetForm">
<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="state&amp;more" />
<input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="valid" />
<input type="text" name="search" value="not sent" />
Export IORPs Data to
<a href="javascript:WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions(&quot;ctl00$ctl34$g_1$lkbtnExport&quot;, &quot;&quot;, true))">CSV</a> |
<a href="javascript:WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions(&quot;ctl00$ctl34$g_1$lkbtnExcelExport&quot;, &quot;&quot;, true))">Excel</a>
</form>
"""

#: A register as EIOPA's CSV export writes it: quoted, separated by
#: semicolons, every line ending in a separator.
REGISTER = ('"Official name of the entity";"LEI";"Registration Start Date";'
            '"Registration End Date";\r\n'
            '"Österreichische Pensionskasse";"31570010000000066734";'
            '"14/11/2013 00:00:00";"";\r\n'
            '"Fund B";"";"1990-12-10T00:00:00Z";"20/12/2023 00:00:00";\r\n'
            ).encode("utf-8")


class FakePage(FakeResponse):
    """An answer read whole: a page as ``text``, a workbook as ``content``."""

    def __init__(self, payload, status=200):
        if isinstance(payload, str):
            payload = payload.encode()
        super().__init__(payload, status)
        self.text = payload.decode("utf-8", "replace")
        self.content = payload
        self.cookies = {"ApplicationGatewayAffinity": "backend-1"}


def serve_register(monkeypatch, page=EIOPA_PAGE, answer=None):
    """EIOPA's register: the page for a GET, the workbook for the form."""
    calls = []

    def get(url, **options):
        calls.append(("GET", url, options))
        return FakePage(page)

    def post(url, **options):
        calls.append(("POST", url, options))
        return answer or FakePage(REGISTER)

    monkeypatch.setattr("fincommons.entitymasterdata.landing.requests.get", get)
    monkeypatch.setattr("fincommons.entitymasterdata.landing.requests.post", post)
    # An export turned away is waited for; the tests do not wait.
    monkeypatch.setattr("fincommons.entitymasterdata.landing.time.sleep",
                        lambda seconds: None)
    return calls


def test_the_export_form_is_read_off_the_page():
    url = "https://register.eiopa.europa.eu/registers/register-of-iorps"

    action, fields = em.export_form(EIOPA_PAGE, url)

    assert action == url + "?TermStoreId=a&TermId=b"
    assert fields == {
        "__VIEWSTATE": "state&more",
        "__EVENTVALIDATION": "valid",
        "__EVENTTARGET": "ctl00$ctl34$g_1$lkbtnExport",
        "__EVENTARGUMENT": "",
    }


def test_a_page_without_the_export_is_refused():
    with pytest.raises(ValueError, match="no form with the export"):
        em.export_form("<html>Service unavailable</html>", em.EIOPA_IORP_URL)


@pytest.mark.parametrize("download, folder", [
    ("download_eiopa_iorp", "path_landing_eiopa_iorp"),
    ("download_eiopa_insurance_undertaking",
     "path_landing_eiopa_insurance_undertaking"),
])
def test_a_register_of_eiopa_is_downloaded_under_the_day(
        monkeypatch, data_lake, download, folder):
    import datetime
    calls = serve_register(monkeypatch)

    getattr(em, download)(data_lake, today=datetime.date(2026, 10, 28))

    landing = getattr(em, folder)(data_lake)
    assert os.listdir(landing) == ["20261028.csv"]
    with open(os.path.join(landing, "20261028.csv"), "rb") as handle:
        assert handle.read() == REGISTER
    (_, _, get), (method, _, post) = calls
    assert method == "POST"
    # The CSV export: the workbook stores an LEI of digits as a number.
    assert post["data"]["__EVENTTARGET"].endswith("$lkbtnExport")
    # The export goes to the server that issued the form's view state.
    assert post["cookies"] == {"ApplicationGatewayAffinity": "backend-1"}
    assert get["timeout"] and post["timeout"]
    assert get["headers"]["User-Agent"]


def test_an_export_that_is_not_the_register_is_not_stored(monkeypatch, data_lake):
    """A form the server no longer accepts is answered with the page again."""
    serve_register(monkeypatch, answer=FakePage(EIOPA_PAGE))

    with pytest.raises(ValueError, match="did not answer the export with the register"):
        em.download_eiopa_iorp(data_lake)

    assert os.listdir(em.path_landing_eiopa_iorp(data_lake)) == []


def serve_pages(monkeypatch, answers):
    """The ECB: each address answered from ``answers``, a 404 otherwise."""
    calls = []

    def get(url, **options):
        calls.append(url)
        body = answers.get(url.split("?")[0])
        return FakePage(body) if body is not None else FakePage(b"", status=404)

    monkeypatch.setattr("fincommons.entitymasterdata.landing.requests.get", get)
    return calls


def lists_archive(names):
    """An archive of the lists of investment funds, packed as the ECB packs it."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in names:
            archive.writestr(f"ecb.ifd_overview_2026.en/IF_overview_2026/{name}",
                             b"workbook " + name.encode())
        archive.writestr("ecb.ifd_overview_2026.en/IF_overview_2026/Read.me.rtf", b"x")
        archive.writestr("ecb.ifd_overview_2026.en/IF_overview_2026/~$ead.me.rtf", b"x")
    return buffer.getvalue()


ECB = "https://www.ecb.europa.eu"
LISTS_PAGE = """
<a href="/stats/pdf/money/ecb.ifd_overview_2026.en.zip?b3d2">IFs Overview 2026</a>
<a href="/stats/pdf/money/ecb.ifd_overview_2025.en.zip?c255">IFs Overview 2025</a>
<a href="/stats/pdf/money/ecb.ifd_overview_2019-2024.en.zip?6c81">IFs Overview 2019-2024</a>
<a href="/stats/pdf/money/fvc/ecb.fvcd_overview_2026.en.zip">FVCs Overview 2026</a>
"""


def test_the_lists_of_investment_funds_the_bronze_layer_lacks_are_unpacked(
        monkeypatch, data_lake):
    archive = lists_archive(["IF_2026_01.xlsx", "IF_2026_02.xlsx", "IF_2025_Q4.xlsx"])
    calls = serve_pages(monkeypatch, {
        em.ECB_LISTS_URL: LISTS_PAGE,
        ECB + "/stats/pdf/money/ecb.ifd_overview_2026.en.zip": archive,
    })
    bronze = em.path_bronze_ecb_investment_fund(data_lake)
    os.makedirs(bronze, exist_ok=True)
    open(os.path.join(bronze, "2026-01-31.parquet"), "wb").close()

    stored = em.download_ecb_investment_fund(data_lake)

    # The archive of the latest single year, not the one of several years.
    assert calls[1] == ECB + "/stats/pdf/money/ecb.ifd_overview_2026.en.zip"
    assert stored == ["IF_2025_Q4.xlsx", "IF_2026_02.xlsx"]
    landing = em.path_landing_ecb_investment_fund(data_lake)
    assert sorted(os.listdir(landing)) == ["IF_2025_Q4.xlsx", "IF_2026_02.xlsx"]
    with open(os.path.join(landing, "IF_2026_02.xlsx"), "rb") as handle:
        assert handle.read() == b"workbook IF_2026_02.xlsx"


@pytest.mark.parametrize("name, prefix, end", [
    ("FVC_2026_Q3.xlsx", "FVC", "2026-09-30"),
    ("FVC_2024_Q1.xlsx", "FVC", "2024-03-31"),
    ("PSRIs_2021.xlsx", "PSRIs", "2021-12-31"),
    ("PSRIs_2026_H1.xlsx", "PSRIs", "2026-06-30"),
    ("PSRIs_2025_H2.xlsx", "PSRIs", "2025-12-31"),
])
def test_a_list_of_fvcs_or_psris_is_dated_at_the_end_of_its_period(name, prefix, end):
    from fincommons.entitymasterdata import bronze
    assert bronze.end_of_period(name, prefix) == end


def test_a_list_named_otherwise_is_refused():
    from fincommons.entitymasterdata import bronze
    with pytest.raises(ValueError, match="does not spell out the period"):
        bronze.end_of_period("FVC list Q3.xlsx", "FVC")


def test_the_lists_of_psris_are_unpacked_and_imported_as_text(monkeypatch, data_lake):
    from fincommons.entitymasterdata import bronze
    lists = {}
    for name, postal in [("PSRIs_2021.xlsx", 1070), ("PSRIs_2026_H1.xlsx", "1012 AB")]:
        buffer = io.BytesIO()
        pandas.DataFrame({"ID": ["BE0205764318"], "LEI": ["549300DYPOFMXOR7XM56"],
                          "Postal Code": [postal]}).to_excel(buffer, index=False)
        lists[name] = buffer.getvalue()
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as handle:
        for name, content in lists.items():
            handle.writestr(f"ecb.psrid_overview.en/{name}", content)
        handle.writestr("ecb.psrid_overview.en/Read.me.rtf", b"x")
    page = '<a href="/stats/pdf/money/psri/ecb.psrid_overview.en.zip?2125">PSRIs</a>'
    serve_pages(monkeypatch, {
        em.ECB_LISTS_URL: page,
        ECB + "/stats/pdf/money/psri/ecb.psrid_overview.en.zip": archive.getvalue(),
    })

    assert em.download_ecb_psri(data_lake) == ["PSRIs_2021.xlsx", "PSRIs_2026_H1.xlsx"]
    assert bronze.update_bronze_ecb_psri(data_lake) == 2

    folder = em.path_bronze_ecb_psri(data_lake)
    assert sorted(os.listdir(folder)) == ["2021-12-31.parquet", "2026-06-30.parquet"]
    frame = pandas.read_parquet(os.path.join(folder, "2021-12-31.parquet"))
    assert frame["Postal Code"].tolist() == ["1070"]
    assert frame["REFDATE"].tolist() == ["2021-12-31"]
    # Held in the Bronze layer, the lists are not unpacked again.
    assert em.download_ecb_psri(data_lake) == []


def test_a_page_without_an_archive_of_investment_funds_is_refused(
        monkeypatch, data_lake):
    serve_pages(monkeypatch, {em.ECB_LISTS_URL: "<html>moved</html>"})

    with pytest.raises(ValueError, match="links no archive"):
        em.download_ecb_investment_fund(data_lake)


MFI_PAGE = """
<a href="/stats/money/mfi/general/html/dla/mfi_MID/mfi_csv_261027.csv.gz">gz</a>
<a href="/stats/money/mfi/general/html/dla/mfi_MID/mfi_csv_261027.csv">csv</a>
<a href="/stats/money/mfi/general/html/dla/mfi_MID/mfi_csv_update_261027.csv">update</a>
"""
MFI_URL = ECB + "/stats/money/mfi/general/html/dla/mfi_MID/mfi_csv_261027.csv"
MFI_LIST = ("RIAD_CODE\tLEI\tPOSTAL\tCATEGORY\r\n"
            "DE05833\t391200DPSK6G0VJNFN83\t00187\tCredit Institution\r\n"
            "DE05843\t\t1012 AB\tCredit Institution\r\n").encode("utf-16")


def test_the_list_of_mfis_of_the_day_is_downloaded_and_read_as_text(
        monkeypatch, data_lake):
    from fincommons.entitymasterdata import bronze
    serve_pages(monkeypatch, {em.ECB_MFI_URL: MFI_PAGE, MFI_URL: MFI_LIST})

    assert em.download_ecb_mfi(data_lake) == "20261027.csv"

    landing = em.path_landing_ecb_mfi(data_lake)
    assert os.listdir(landing) == ["20261027.csv"]
    assert bronze.update_bronze_ecb_mfi(data_lake) == 1
    frame = pandas.read_parquet(os.path.join(em.path_bronze_ecb_mfi(data_lake),
                                             "20261027.parquet"))
    assert list(frame["POSTAL"]) == ["00187", "1012 AB"]
    assert frame["LEI"].isna().tolist() == [False, True]
    assert set(frame["REFDATE"]) == {"2026-10-27"}


def test_the_list_of_mfis_the_bronze_layer_holds_is_not_downloaded_again(
        monkeypatch, data_lake):
    calls = serve_pages(monkeypatch, {em.ECB_MFI_URL: MFI_PAGE, MFI_URL: MFI_LIST})
    bronze = em.path_bronze_ecb_mfi(data_lake)
    os.makedirs(bronze, exist_ok=True)
    open(os.path.join(bronze, "20261027.parquet"), "wb").close()

    assert em.download_ecb_mfi(data_lake) is None
    assert calls == [em.ECB_MFI_URL]


def test_an_answer_that_is_not_the_list_of_mfis_is_not_stored(monkeypatch, data_lake):
    serve_pages(monkeypatch, {em.ECB_MFI_URL: MFI_PAGE,
                              MFI_URL: b"<html>Service unavailable</html>"})

    with pytest.raises(ValueError, match="did not answer with the list of MFIs"):
        em.download_ecb_mfi(data_lake)

    assert os.listdir(em.path_landing_ecb_mfi(data_lake)) == []


def test_an_export_turned_away_is_asked_for_again_from_a_fresh_page(
        monkeypatch, data_lake):
    """EIOPA's gateway answered the first live download with 403 and served
    the same export a moment later."""
    from fincommons.entitymasterdata import landing
    answers = [FakePage("<html>403</html>", status=403), FakePage(REGISTER)]
    calls = serve_register(monkeypatch)
    waits = []
    monkeypatch.setattr(landing.time, "sleep", waits.append)
    post = landing.requests.post

    def turned_away_once(url, **options):
        post(url, **options)
        return answers.pop(0)

    monkeypatch.setattr("fincommons.entitymasterdata.landing.requests.post",
                        turned_away_once)

    em.download_eiopa_iorp(data_lake)

    assert [method for method, _, _ in calls] == ["GET", "POST", "GET", "POST"]
    assert waits == [landing.EIOPA_WAIT]
    assert len(os.listdir(em.path_landing_eiopa_iorp(data_lake))) == 1


def test_the_csv_export_is_imported_as_text_with_its_dates_read(data_lake):
    from fincommons.entitymasterdata import bronze
    landing = em.path_landing_eiopa_iorp(data_lake)
    with open(os.path.join(landing, "20261028.csv"), "wb") as handle:
        handle.write(REGISTER)
    raw = bronze.read_eiopa_register(os.path.join(landing, "20261028.csv"),
                                     "EIOPA_IORP")

    # The LEI of digits is kept as it is, the empty column at the end dropped,
    # and both spellings of a date read.
    assert list(raw.columns) == ["Official name of the entity", "LEI",
                                 "Registration Start Date", "Registration End Date"]
    assert raw["LEI"].tolist()[0] == "31570010000000066734"
    assert raw["Registration Start Date"].tolist() == ["2013-11-14", "1990-12-10"]
    assert raw["Registration End Date"].isna().tolist() == [True, False]
    assert raw["Official name of the entity"][0] == "Österreichische Pensionskasse"


def test_a_workbook_with_mixed_cells_is_imported_as_text(data_lake, tmp_path):
    """EIOPA's workbook holds a commercial name that is a date and street
    numbers that are numbers; pyarrow refuses a column mixing them."""
    import datetime
    from fincommons.entitymasterdata import bronze
    path = str(tmp_path / "20261028.xlsx")
    pandas.DataFrame({
        "Local/Commercial name": ["Name AG", datetime.datetime(2024, 9, 30)],
        "Street Number": [1, "12a"],
        "LEI": ["529900T8BM49AURSDO55", None],
        "Registration start date": [datetime.datetime(2013, 11, 14), "29/04/2004"],
        "Registration end date": [None, None],
        "Operation Start Date": [None, None],
        "Operation End Date": [None, None],
    }).to_excel(path, index=False)

    raw = bronze.read_eiopa_register(path, "EIOPA_IC")

    assert all(str(t) == "string" for t in raw.dtypes)
    assert raw["Street Number"].tolist() == ["1", "12a"]
    assert raw["Registration start date"].tolist() == ["2013-11-14", "2004-04-29"]
    bronze.write_parquet(raw, str(tmp_path / "out.parquet"))


def test_a_date_that_is_not_one_is_refused():
    from fincommons.entitymasterdata import bronze
    with pytest.raises(ValueError, match="not dates"):
        bronze.eiopa_dates(pandas.Series(["14/11/2013 00:00:00", "soon"]))


def test_an_error_answer_to_the_export_is_not_stored(monkeypatch, data_lake):
    serve_register(monkeypatch, answer=FakePage("<html>500</html>", status=500))

    with pytest.raises(requests.HTTPError):
        em.download_eiopa_insurance_undertaking(data_lake)

    assert os.listdir(em.path_landing_eiopa_insurance_undertaking(data_lake)) == []


# The Bronze layer
# --------------------------------------------------------------------------


def test_a_gleif_file_is_imported_under_its_reference_date(data_lake):
    write_gleif_level_1(data_lake, ["LEI1"], ["Entity one"], ["AT"])

    em.update_bronze_gleif_level_1(data_lake)

    folder = em.path_bronze_gleif_level_1(data_lake)
    assert os.listdir(folder) == ["20260724.parquet"]
    written = pandas.read_parquet(os.path.join(folder, "20260724.parquet"))
    assert list(written.columns) == ["LEI", "NAME", "CTRY", "REFDATE"]
    assert written["REFDATE"].iloc[0] == "2026-07-24"


def test_an_imported_file_leaves_the_landing_zone(data_lake):
    write_gleif_level_1(data_lake, ["LEI1"], ["Entity one"], ["AT"])

    em.update_bronze_gleif_level_1(data_lake)

    assert os.listdir(em.path_landing_gleif_level_1(data_lake)) == []


def test_a_file_that_cannot_be_read_stays_in_the_landing_zone(data_lake):
    """The run stops with the file still in place. Reporting the error and
    carrying on would leave a gap in the Bronze layer that nothing points at."""
    path = os.path.join(em.path_landing_gleif_level_1(data_lake), "20260724-lei2.zip")
    with open(path, "wb") as f:
        f.write(zipped_csv("lei2.csv", pandas.DataFrame({"SOMETHING": ["else"]})))

    with pytest.raises(ValueError):
        em.update_bronze_gleif_level_1(data_lake)

    assert os.path.isfile(path)


def test_a_value_pandas_would_take_for_missing_is_kept_as_written(data_lake):
    """'NA' is the country code of Namibia, and a legal name may well read
    'NULL'. Only an empty field is the absence of a value."""
    write_gleif_level_1(data_lake, ["LEI1", "LEI2"], ["Namib Bank", "NULL"], ["NA", "AT"])

    em.update_bronze_gleif_level_1(data_lake)

    written = pandas.read_parquet(
        os.path.join(em.path_bronze_gleif_level_1(data_lake), "20260724.parquet")
    )
    assert list(written["CTRY"]) == ["NA", "AT"]
    assert list(written["NAME"]) == ["Namib Bank", "NULL"]


def test_an_import_that_dies_while_writing_leaves_no_bronze_file(monkeypatch, data_lake):
    """A truncated file under the name of a reference date would be taken by
    the Silver layer as the most recent register."""
    landing = write_gleif_level_1(data_lake, ["LEI1"], ["Entity one"], ["AT"])

    def die(table, path):
        with open(path, "wb") as f:
            f.write(b"PAR1 truncated")
        raise OSError("disk full")

    monkeypatch.setattr("fincommons.entitymasterdata.bronze.pyarrow.parquet.write_table", die)

    with pytest.raises(OSError, match="disk full"):
        em.update_bronze_gleif_level_1(data_lake)

    assert os.listdir(em.path_bronze_gleif_level_1(data_lake)) == []
    assert os.path.isfile(landing)


def test_two_lists_of_funds_ending_on_the_same_day_are_refused(data_lake):
    """A monthly and a quarterly list that end on the same day would be stored
    under the same name, the second replacing the first without a word."""
    monthly = write_ecb_investment_fund(data_lake, ["LEI1"], file_name="IF_2026_03.xlsx")
    quarterly = write_ecb_investment_fund(data_lake, ["LEI2"], file_name="IF_2026_Q1.xlsx")

    with pytest.raises(ValueError, match="replace one another"):
        em.update_bronze_ecb_investment_fund(data_lake)

    assert os.listdir(em.path_bronze_ecb_investment_fund(data_lake)) == []
    assert os.path.isfile(monthly) and os.path.isfile(quarterly)


def test_files_of_another_kind_are_left_alone(data_lake):
    note = os.path.join(em.path_landing_gleif_level_1(data_lake), "readme.txt")
    with open(note, "w") as f:
        f.write("where these files come from")

    em.update_bronze_gleif_level_1(data_lake)

    assert os.path.isfile(note)
    assert os.listdir(em.path_bronze_gleif_level_1(data_lake)) == []


def test_a_file_of_another_kind_is_named_in_a_warning(data_lake, caplog):
    """The registers are placed in the landing zone by hand, so a file of the
    wrong kind is as likely to be the register saved in the wrong format as it
    is to be a note. Leaving it alone is right; doing so in silence is not."""
    stray = os.path.join(em.path_landing_ecb_mfi(data_lake), "20260724.xls")
    with open(stray, "wb") as f:
        f.write(b"a workbook in the format the module does not read")

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        em.update_bronze_ecb_mfi(data_lake)

    assert os.path.isfile(stray)
    assert any(
        r.ctx.get("files") == "20260724.xls"
        for r in caplog.records if r.levelno == logging.WARNING
    ), [getattr(r, "ctx", None) for r in caplog.records]


def test_a_workbook_the_module_reads_is_not_named_in_a_warning(data_lake, caplog):
    write_ecb_mfi(data_lake, [("LEI1", "Credit Institution")])

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        em.update_bronze_ecb_mfi(data_lake)

    assert not [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "files" in getattr(r, "ctx", {})
    ]


def test_the_list_of_ccps_leaves_the_landing_zone_when_it_is_imported(data_lake):
    """The list is the one register whose name carries no reference date, so a
    copy left behind is indistinguishable from one just downloaded, and a run
    whose download failed would import it and report it as the register."""
    write_ccp(data_lake, [("LEI1", "CCP", "EUCCP")])

    assert em.update_bronze_esma_ccp(data_lake) == 1
    assert os.listdir(em.path_landing_esma_ccp(data_lake)) == []
    assert os.listdir(em.path_bronze_esma_ccp(data_lake)) == ["ccp_list.parquet"]


def test_a_quarterly_list_of_investment_funds_is_dated_at_the_end_of_the_quarter(data_lake):
    """The ECB publishes the list per month or per quarter. The quarter used to
    be looked for in the list the period sat in rather than in the period
    itself, which never found it and sent the file into the parser for
    months."""
    write_ecb_investment_fund(data_lake, ["LEI1"], file_name="IF_2026_Q1.xlsx")

    em.update_bronze_ecb_investment_fund(data_lake)

    assert os.listdir(em.path_bronze_ecb_investment_fund(data_lake)) == [
        "2026-03-31.parquet"
    ]


def test_a_monthly_list_of_investment_funds_is_dated_at_the_end_of_the_month(data_lake):
    write_ecb_investment_fund(data_lake, ["LEI1"], file_name="IF_2026_02.xlsx")

    em.update_bronze_ecb_investment_fund(data_lake)

    assert os.listdir(em.path_bronze_ecb_investment_fund(data_lake)) == [
        "2026-02-28.parquet"
    ]


def test_an_mfi_without_an_lei_keeps_a_missing_one(data_lake):
    """Reading the column as text turns a missing LEI into the string 'nan',
    which then travels into the joins of the Silver layer as though it were
    one."""
    write_ecb_mfi(data_lake, [(None, "Credit Institution")])

    em.update_bronze_ecb_mfi(data_lake)

    written = pandas.read_parquet(
        os.path.join(em.path_bronze_ecb_mfi(data_lake), "20260724.parquet")
    )
    assert written["LEI"].isna().all()


def test_an_mfi_postcode_is_kept_as_the_register_writes_it(data_lake):
    """Taking the postcodes for numbers dropped the leading zero of an Italian
    one and turned a Dutch one into a missing value."""
    write_ecb_mfi(
        data_lake,
        [("LEI1", "Credit Institution"), ("LEI2", "Credit Institution"),
         ("LEI3", "Credit Institution")],
        postal=["00187", "1012 AB", 1010],
    )

    em.update_bronze_ecb_mfi(data_lake)

    written = pandas.read_parquet(
        os.path.join(em.path_bronze_ecb_mfi(data_lake), "20260724.parquet")
    )
    assert written["POSTAL"].tolist() == ["00187", "1012 AB", "1010"]


def test_the_registers_are_imported_in_one_call(data_lake):
    write_gleif_level_1(data_lake, ["LEI1"], ["Entity one"], ["AT"])
    write_gleif_level_2(data_lake, [("LEI1", "LEI2", "IS_ULTIMATELY_CONSOLIDATED_BY")])
    write_ecb_investment_fund(data_lake, ["LEI1"])
    write_ecb_mfi(data_lake, [("LEI2", "Credit Institution")])
    write_eiopa(data_lake, ["LEI3"])
    write_iorp(data_lake, [])
    write_ccp(data_lake, [("LEI4", "CCP", "EUCCP")])

    em.update_from_landing_to_bronze(data_lake)

    for folder in [
        em.path_bronze_gleif_level_1(data_lake),
        em.path_bronze_gleif_level_2(data_lake),
        em.path_bronze_ecb_investment_fund(data_lake),
        em.path_bronze_ecb_mfi(data_lake),
        em.path_bronze_eiopa_insurance_undertaking(data_lake),
        em.path_bronze_esma_ccp(data_lake),
    ]:
        assert len(os.listdir(folder)) == 1


def test_the_import_carries_on_when_the_list_of_ccps_was_not_downloaded(data_lake):
    """The list is fetched over the network rather than placed by hand, so a
    landing zone without it means the download has not been run. The registers
    that are there are still imported."""
    write_gleif_level_1(data_lake, ["LEI1"], ["Entity one"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])

    em.update_from_landing_to_bronze(data_lake)

    assert os.listdir(em.path_bronze_esma_ccp(data_lake)) == []
    assert len(os.listdir(em.path_bronze_gleif_level_1(data_lake))) == 1


# The public entity master data
# --------------------------------------------------------------------------


def test_a_register_that_was_never_imported_is_reported_as_such(spark, data_lake):
    """The message that a register is missing beats the one an empty folder
    would otherwise produce somewhere further down."""
    with pytest.raises(FileNotFoundError, match="Bronze layer"):
        em.update_silver_entitymasterdata_public(spark, data_lake)


def test_a_register_the_bronze_layer_never_received_is_reported_as_such(spark, data_lake):
    """The list of CCPs is the one register the Bronze layer holds under a
    fixed name rather than one per reference date, and it has to say the same
    thing as the others when it is missing."""
    populated(data_lake, leis=["LEI1"], ccp=False)

    with pytest.raises(FileNotFoundError, match="Bronze layer"):
        em.update_silver_entitymasterdata_public(spark, data_lake)


def test_gleif_supplies_the_name_and_the_country(spark, data_lake):
    populated(data_lake, leis=["LEI1"])

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").NAME == "Entity LEI1"
    assert entity(frame, "LEI1").CTRY == "AT"


def test_an_entity_no_register_covers_has_no_sector(spark, data_lake):
    populated(data_lake, leis=["LEI1"])

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert pandas.isna(entity(frame, "LEI1").SECTOR)


def test_each_register_contributes_its_sector(spark, data_lake):
    write_gleif_level_1(
        data_lake,
        ["LEI1", "LEI2", "LEI3", "LEI4", "LEI5"],
        ["One", "Two", "Three", "Four", "Five"],
        ["AT"] * 5,
    )
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [("LEI1", "CCP", "EUCCP")])
    write_eiopa(data_lake, ["LEI2"])
    write_iorp(data_lake, ["LEI5"])
    write_ecb_mfi(data_lake, [("LEI3", "Credit Institution")])
    write_ecb_investment_fund(data_lake, ["LEI4"])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == "CCP"
    assert entity(frame, "LEI2").SECTOR == "INUN"
    assert entity(frame, "LEI3").SECTOR == "CDTI"
    assert entity(frame, "LEI4").SECTOR == "UCIT"
    assert entity(frame, "LEI5").SECTOR == "ORPI"
    assert entity(frame, "LEI4").IF_TYPE == "ETF"
    assert entity(frame, "LEI4").IF_UCITS_COMPL == "UCITS"


def test_an_mfi_category_the_module_does_not_know_still_yields_a_sector(spark, data_lake):
    """The MFI register is converted through a spreadsheet program by hand, so
    a category can reach the Bronze layer spelled in a way the mapping does not
    cover. Leaving the sector empty would be the worse answer: the list of MFIs
    takes precedence over the list of investment funds, so the empty sector
    would win and take the fund description down with it."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    # The same category the register uses, relabelled in its second word.
    write_ecb_mfi(data_lake, [("LEI1", "Credit institution")])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == em.MFI_SECTOR_OTHER


def test_an_unknown_mfi_category_is_logged_with_the_spelling_it_carried(spark, data_lake, caplog):
    """The result stays valid but is coarser than the register would allow,
    which is what a warning naming the category is for."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [("LEI1", "Credit institution")])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        em.update_silver_entitymasterdata_public(spark, data_lake)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        r.ctx.get("categories") == "Credit institution" for r in warnings
    ), [getattr(r, "ctx", None) for r in warnings]


def test_a_known_mfi_category_is_not_logged_as_unknown(spark, data_lake, caplog):
    populated(data_lake, leis=["LEI1"])
    write_ecb_mfi(data_lake, [("LEI1", "Credit Institution")])
    em.update_from_landing_to_bronze(data_lake)

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        em.update_silver_entitymasterdata_public(spark, data_lake)

    assert not [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "categories" in getattr(r, "ctx", {})
    ]


def test_an_institution_on_the_mfi_register_is_never_read_as_uncovered(spark, data_lake):
    """The distinction the fallback preserves. An empty sector is what an entity
    no register covers carries, so an unknown category must not leave it on an
    institution that is on the register: that would make it indistinguishable
    from one that is on none of them -- including from the entity on the fund
    register next to it, which the precedence of the registers would then have
    nothing to yield to."""
    write_gleif_level_1(data_lake, ["LEI1", "LEI2"], ["One", "Two"], ["AT", "AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [("LEI1", "Money market fund")])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == em.MFI_SECTOR_OTHER
    assert pandas.isna(entity(frame, "LEI2").SECTOR)


@pytest.mark.parametrize(
    "order",
    [
        [("LEI1", "CCP", "T2CCP"), ("LEI1", "CCP", "EUCCP")],
        [("LEI1", "CCP", "EUCCP"), ("LEI1", "CCP", "T2CCP")],
    ],
    ids=["the-later-one-first", "the-earlier-one-first"],
)
def test_a_register_carrying_an_lei_twice_answers_the_same_way_either_way(spark, data_lake, order):
    """A central counterparty authorised under more than one heading stands in
    the list of CCPs once per heading. The rank settles which register
    describes an entity and nothing about which of a register's own rows is
    taken, so the order has to run on past it.

    The two cases are the same rows in the two orders the file can hold them
    in. Asserting one pick would pass just as well against an order that
    stopped at the sector, since Spark reads a small test file in one
    partition; what makes the test bite is that both orders have to agree."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, order)
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    row = entity(public(spark, data_lake), "LEI1")

    assert row.SECTOR == "CCP"
    assert row.SUBSECTOR == "EUCCP"


def test_a_row_that_states_a_subsector_beats_one_that_does_not(spark, data_lake):
    """Nulls sort last, which is the opposite of what ``orderBy`` does on its
    own. A null is the absence of a statement, so the row that says something
    is the one to keep."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [("LEI1", "CCP", None), ("LEI1", "CCP", "EUCCP")])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)

    assert entity(public(spark, data_lake), "LEI1").SUBSECTOR == "EUCCP"


def test_a_money_market_fund_keeps_what_the_fund_register_says_about_it(spark, data_lake):
    """A money market fund stands on the list of MFIs by definition, and that
    register wins the sector. The fund characteristics are not part of that
    contest -- no other register carries them -- so they must not be lost with
    the row the sector step discards."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [("LEI1", "Money Market Fund")])
    write_ecb_investment_fund(data_lake, ["LEI1"])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    row = entity(public(spark, data_lake), "LEI1")

    assert row.SECTOR == "MMF"
    assert row.IF_UCITS_COMPL == "UCITS"
    assert row.IF_TYPE == "ETF"
    assert row.IF_POLICY == "Bond funds"


def test_the_subsector_stays_the_statement_of_the_register_that_won(spark, data_lake):
    """The decision the previous test rests on. SECTOR and SUBSECTOR are one
    statement from one register, so SUBSECTOR is not filled in from the fund
    register once another register has taken the sector -- that would pair a
    sector from one taxonomy with a refinement from another. Nothing is lost by
    it: the investment policy the fund register would have contributed as
    SUBSECTOR is what IF_POLICY carries."""
    write_gleif_level_1(data_lake, ["LEI1", "LEI2"], ["One", "Two"], ["AT", "AT"])
    write_gleif_level_2(data_lake, [])
    # LEI2 is a central counterparty as well as a fund, and the CCP register
    # has a subsector of its own.
    write_ccp(data_lake, [("LEI2", "CCP", "EUCCP")])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [("LEI1", "Money Market Fund")])
    write_ecb_investment_fund(data_lake, ["LEI1", "LEI2"])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    # The MFI register has no subsector, and the fund register does not supply
    # one in its place.
    assert pandas.isna(entity(frame, "LEI1").SUBSECTOR)
    assert entity(frame, "LEI1").IF_POLICY == "Bond funds"
    # The CCP register does have one, and it is the one that stands.
    assert entity(frame, "LEI2").SUBSECTOR == "EUCCP"
    assert entity(frame, "LEI2").IF_POLICY == "Bond funds"


def test_a_fund_on_no_other_register_is_described_by_the_fund_register_alone(spark, data_lake):
    """The unchanged case: where the fund register wins the sector, it supplies
    the subsector as well."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, ["LEI1"])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    row = entity(public(spark, data_lake), "LEI1")

    assert row.SECTOR == "UCIT"
    assert row.SUBSECTOR == "Bond funds"
    assert row.IF_POLICY == "Bond funds"


def test_an_entity_on_no_fund_register_has_no_fund_characteristics(spark, data_lake):
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [("LEI1", "Credit Institution")])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    row = entity(public(spark, data_lake), "LEI1")

    assert row.SECTOR == "CDTI"
    assert pandas.isna(row.IF_POLICY)
    assert pandas.isna(row.IF_TYPE)
    assert pandas.isna(row.IF_UCITS_COMPL)


def test_an_iorp_is_described_as_one_rather_than_as_the_fund_it_also_stands_in(spark, data_lake):
    """An institution for occupational retirement provision can be listed as an
    investment fund as well. EIOPA names the specific kind of undertaking and
    the ECB list only says that it is a fund, so the EIOPA description wins."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, ["LEI1"])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, ["LEI1"])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == "ORPI"


def test_an_insurer_running_a_pension_scheme_is_described_by_its_authorisation(spark, data_lake):
    """A handful of entities stand in both EIOPA registers: insurers that also
    run occupational pension business, the Pensionskassen constituted as
    insurers among them. The insurance register wins, because it states the
    authorisation the entity holds while the IORP register states an activity
    carried on under it."""
    write_gleif_level_1(data_lake, ["LEI1"], ["Pensionsversicherung"], ["DE"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, ["LEI1"])
    write_iorp(data_lake, ["LEI1"])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == "INUN"


def test_a_fund_is_described_by_its_ucits_status(spark, data_lake):
    """The ECB list of investment funds does not yield one sector but three.
    A fund compliant with the UCITS directive is described as such, one
    reported as an alternative investment fund as an AIF, and a fund the
    register says neither about keeps the generic description."""
    write_gleif_level_1(
        data_lake,
        ["LEI1", "LEI2", "LEI3"],
        ["A UCITS", "An AIF", "Neither"],
        ["AT"] * 3,
    )
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(
        data_lake,
        ["LEI1", "LEI2", "LEI3"],
        ucits=["UCITS compliant", "Non-UCITS compliant, AIF", "Not reported"],
    )
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == "UCIT"
    assert entity(frame, "LEI2").SECTOR == "AIFD"
    assert entity(frame, "LEI3").SECTOR == "INVF"
    # The compliance column itself keeps its own vocabulary, which is not the
    # sector: the sector says what the entity is, the column what the register
    # reported about it.
    assert entity(frame, "LEI1").IF_UCITS_COMPL == "UCITS"
    assert entity(frame, "LEI2").IF_UCITS_COMPL == "AIF"


def test_the_narrower_register_wins_where_two_describe_the_same_entity(spark, data_lake):
    """A central counterparty is licensed as a credit institution as well, so
    it stands in both registers. Which of the two wins must hold by
    construction and not by the order in which the rows of the union happen to
    arrive."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(data_lake, [])
    write_ccp(data_lake, [("LEI1", "CCP", "EUCCP")])
    write_ecb_mfi(data_lake, [("LEI1", "Credit Institution")])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").SECTOR == "CCP"
    assert entity(frame, "LEI1").SUBSECTOR == "EUCCP"


def test_the_ultimate_parent_and_the_manager_come_from_different_relationships(spark, data_lake):
    """Both relationships point away from the same entity, so telling them
    apart is the whole of the Level 2 step."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(
        data_lake,
        [
            ("LEI1", "PARENT", "IS_ULTIMATELY_CONSOLIDATED_BY"),
            ("LEI1", "MANAGER", "IS_FUND-MANAGED_BY"),
            ("LEI1", "DIRECT", "IS_DIRECTLY_CONSOLIDATED_BY"),
        ],
    )
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").ULTPAR_LEI == "PARENT"
    assert entity(frame, "LEI1").AMC == "MANAGER"


def test_an_entity_without_relationships_keeps_them_empty(spark, data_lake):
    populated(data_lake, leis=["LEI1"])

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert pandas.isna(entity(frame, "LEI1").ULTPAR_LEI)
    assert pandas.isna(entity(frame, "LEI1").AMC)


def test_a_relationship_that_has_ended_is_not_the_current_one(spark, data_lake):
    """The golden copy keeps a relationship after it has ended, so an entity
    whose parent changed stands in two records of the same type. The parents
    are named so that the lapsed one sorts first: ordering the rows is what
    made the choice repeatable, and it is not what makes it right."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(
        data_lake,
        [
            ("LEI1", "AAOLDPARENT", "IS_ULTIMATELY_CONSOLIDATED_BY", "INACTIVE"),
            ("LEI1", "ZZNEWPARENT", "IS_ULTIMATELY_CONSOLIDATED_BY", "ACTIVE"),
            ("LEI1", "AAOLDMANAGER", "IS_FUND-MANAGED_BY", "INACTIVE"),
            ("LEI1", "ZZNEWMANAGER", "IS_FUND-MANAGED_BY", "ACTIVE"),
        ],
    )
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").ULTPAR_LEI == "ZZNEWPARENT"
    assert entity(frame, "LEI1").AMC == "ZZNEWMANAGER"


def test_an_entity_whose_only_relationship_has_ended_has_no_parent(spark, data_lake):
    """An entity that is no longer consolidated by anyone is not consolidated
    by the entity that used to consolidate it."""
    write_gleif_level_1(data_lake, ["LEI1"], ["One"], ["AT"])
    write_gleif_level_2(
        data_lake,
        [("LEI1", "FORMER", "IS_ULTIMATELY_CONSOLIDATED_BY", "INACTIVE")],
    )
    write_ccp(data_lake, [])
    write_eiopa(data_lake, [])
    write_iorp(data_lake, [])
    write_ecb_mfi(data_lake, [])
    write_ecb_investment_fund(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert pandas.isna(entity(frame, "LEI1").ULTPAR_LEI)


def test_the_most_recent_register_is_the_one_that_is_used(spark, data_lake):
    populated(data_lake, leis=["LEI1"])
    # A second GLEIF file, published a week later, renames the entity.
    write_gleif_level_1(
        data_lake,
        ["LEI1"],
        ["Entity renamed"],
        ["AT"],
        file_name="20260801-0000-lei2.zip",
    )
    em.update_bronze_gleif_level_1(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)
    frame = public(spark, data_lake)

    assert entity(frame, "LEI1").NAME == "Entity renamed"


# The entity master data set
# --------------------------------------------------------------------------


@pytest.fixture
def built(spark, data_lake):
    """A Silver layer built from the public sources alone."""
    populated(data_lake, leis=["LEI1", "LEI2"])
    em.update_silver_entitymasterdata_public(spark, data_lake)
    return data_lake


def test_the_public_data_passes_through_where_there_is_no_internal_list(spark, built):
    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    assert sorted(frame.LEI) == ["LEI1", "LEI2"]
    assert frame["INTERNALID_1"].isna().all()


def test_the_entities_are_numbered_from_one(spark, built):
    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    assert sorted(frame.ID) == [1, 2]


@pytest.mark.parametrize("names", [["Beta", "Alpha"], ["Alpha", "Beta"]])
def test_entities_without_an_lei_are_numbered_the_same_way_whatever_the_file_order(
    spark, built, names
):
    """Rows without an LEI all tie on it, so the number has to be settled by
    what they say rather than by the order Spark happens to read them in."""
    write_internal(built, pandas.DataFrame({"LEI": [None, None], "NAME": names}))

    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    without_lei = frame[frame.LEI.isna()].sort_values("ID")
    assert list(without_lei.NAME) == ["Alpha", "Beta"]


def test_the_columns_are_the_ones_the_module_promises(spark, built):
    em.update_silver_entitymasterdata(spark, built)

    assert list(main(spark, built).columns) == em.silver.COLUMNS


def test_the_internal_list_replaces_the_public_description(spark, built):
    write_internal(
        built,
        pandas.DataFrame(
            {
                "LEI": ["LEI1"],
                "INTERNALID_1": ["A1"],
                "NAME": ["As we call it"],
                "CTRY": ["DE"],
                "SECTOR": ["CDTI"],
            }
        ),
    )

    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    assert entity(frame, "LEI1").NAME == "As we call it"
    assert entity(frame, "LEI1").SECTOR == "CDTI"
    assert entity(frame, "LEI1").INTERNALID_1 == "A1"
    # The entity the internal list is silent about keeps its public description.
    assert entity(frame, "LEI2").NAME == "Entity LEI2"


def test_entities_the_internal_list_does_not_cover_are_kept(spark, built):
    write_internal(
        built, pandas.DataFrame({"LEI": ["LEI1"], "INTERNALID_1": ["A1"], "NAME": ["One"]})
    )

    em.update_silver_entitymasterdata(spark, built)

    assert sorted(main(spark, built).LEI) == ["LEI1", "LEI2"]


def test_internal_entities_without_an_lei_are_all_kept(spark, built):
    """There is nothing to match them on, so none of them is a duplicate of
    another. Deduplicating them along with the rest would collapse them into
    a single row, because they share a missing key."""
    write_internal(
        built,
        pandas.DataFrame(
            {
                "LEI": [None, None],
                "INTERNALID_1": ["A1", "A2"],
                "NAME": ["One", "Two"],
            }
        ),
    )

    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    assert sorted(frame[frame.LEI.isna()].INTERNALID_1) == ["A1", "A2"]
    assert len(frame) == 4


def test_an_internal_identifier_keeps_its_leading_zeros(spark, built):
    """The identifiers are codes rather than numbers, and a code that happens
    to be all digits must not be read as one."""
    write_internal(
        built, pandas.DataFrame({"LEI": ["LEI1"], "INTERNALID_1": ["007"], "NAME": ["One"]})
    )

    em.update_silver_entitymasterdata(spark, built)

    assert entity(main(spark, built), "LEI1").INTERNALID_1 == "007"


def test_the_silver_tables_are_parquet_unless_delta_is_asked_for(spark, built):
    """Parquet is the default, because it is what every engine reads without a
    further dependency."""
    em.update_silver_entitymasterdata(spark, built)

    for path in [em.path_silver_public(built), em.path_silver_main(built)]:
        assert not os.path.isdir(os.path.join(path, "_delta_log"))
        assert any(name.endswith(".parquet") for name in os.listdir(path))


def test_the_silver_tables_are_delta_tables_where_that_is_asked_for(spark, data_lake):
    """A Delta table replaces itself in one transaction, so a reader never sees
    the half-written file that overwriting a parquet file in place would leave
    behind."""
    populated(data_lake, leis=["LEI1", "LEI2"])
    em.update_silver_entitymasterdata_public(spark, data_lake, file_format="delta")
    em.update_silver_entitymasterdata(spark, data_lake, file_format="delta")

    assert os.path.isdir(os.path.join(em.path_silver_public(data_lake), "_delta_log"))
    assert os.path.isdir(os.path.join(em.path_silver_main(data_lake), "_delta_log"))


def test_a_table_is_read_back_in_the_format_it_was_written_in(spark, data_lake):
    """The reader takes the format from the table rather than from the caller.
    Reading a Delta table as plain parquet would not fail; it would return the
    superseded files of earlier versions along with the current ones."""
    populated(data_lake, leis=["LEI1", "LEI2"])
    em.update_silver_entitymasterdata_public(spark, data_lake, file_format="delta")
    em.update_silver_entitymasterdata(spark, data_lake, file_format="delta")
    # A second run leaves the first version of the table behind on disk.
    em.update_silver_entitymasterdata(spark, data_lake, file_format="delta")

    assert sorted(main(spark, data_lake).LEI) == ["LEI1", "LEI2"]


def test_the_two_tables_need_not_be_stored_in_the_same_format(spark, data_lake):
    populated(data_lake, leis=["LEI1", "LEI2"])
    em.update_silver_entitymasterdata_public(spark, data_lake, file_format="delta")
    em.update_silver_entitymasterdata(spark, data_lake, file_format="parquet")

    assert os.path.isdir(os.path.join(em.path_silver_public(data_lake), "_delta_log"))
    assert not os.path.isdir(os.path.join(em.path_silver_main(data_lake), "_delta_log"))
    assert sorted(main(spark, data_lake).LEI) == ["LEI1", "LEI2"]


@pytest.mark.parametrize(
    "update",
    ["update_silver_entitymasterdata_public", "update_silver_entitymasterdata"],
)
def test_an_unknown_file_format_is_reported_as_such(data_lake, update):
    # The format is checked before the session is put to work, which is why
    # there is no session here to put to work.
    with pytest.raises(ValueError, match="csv"):
        getattr(em, update)(None, data_lake, file_format="csv")


def test_a_second_update_replaces_the_table_rather_than_adding_to_it(spark, built):
    em.update_silver_entitymasterdata(spark, built)
    em.update_silver_entitymasterdata(spark, built)

    assert len(main(spark, built)) == 2


# A Silver layer in the catalogue
# --------------------------------------------------------------------------


def catalogued(tmp_path, schema):
    """A configuration file whose Silver layer is a schema in the catalogue."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "entitymasterdata:\n"
        f"  landing: {tmp_path / 'landing'}\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        "  silver:\n"
        f"    table: {schema}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    em.create_directory_tree(config_path)
    return config_path


def test_a_silver_layer_in_the_catalogue_is_addressed_by_name(tmp_path):
    """A catalogue entry is not a directory, so a layer indexed as one would
    make the module fail before it reached the catalogue."""
    config_path = catalogued(tmp_path, "lake")

    assert em.ref_silver_public(config_path).name == "lake.public"
    assert em.ref_silver_main(config_path).name == "lake.main"
    assert em.path_silver_base(config_path) is None
    assert em.path_silver_main(config_path) is None
    assert em.silver_format(em.ref_silver_main(config_path)) == "delta"
    # The landing zone and the Bronze layer are still set up as directories.
    assert os.path.isdir(em.path_bronze_gleif_level_1(config_path))


def test_the_folders_can_be_renamed_beside_a_silver_layer_in_the_catalogue(tmp_path):
    config_path = catalogued(tmp_path, "lake")

    assert em.migrate_folder_names(config_path) == []


@pytest.mark.parametrize(
    "update",
    ["update_silver_entitymasterdata_public", "update_silver_entitymasterdata",
     "update_entitymasterdata"],
)
def test_parquet_is_refused_for_a_silver_layer_in_the_catalogue(tmp_path, update):
    # Refused before the session is put to work, or anything downloaded.
    config_path = catalogued(tmp_path, "lake")

    with pytest.raises(ValueError, match="cannot be written as 'parquet'"):
        getattr(em, update)(None, config_path, file_format="parquet")


@pytest.fixture
def schema(spark, tmp_path):
    """A schema in the session catalogue whose tables live below ``tmp_path``.

    It stands in for a Unity Catalog schema: the tables in it are addressed by
    name, and the catalogue decides where their files go.
    """
    name = f"entitymasterdata_{uuid.uuid4().hex[:12]}"
    spark.sql(f"CREATE SCHEMA {name} LOCATION '{(tmp_path / 'warehouse').as_posix()}'")
    yield name
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def test_the_silver_layer_is_written_into_the_catalogue_where_the_configuration_says_so(
    spark, tmp_path, schema
):
    """A change of the configuration file is all it takes: the updates are
    called exactly as they are against a directory."""
    config_path = catalogued(tmp_path, schema)
    populated(config_path, leis=["LEI1", "LEI2"])

    em.update_silver_entitymasterdata_public(spark, config_path)
    em.update_silver_entitymasterdata(spark, config_path)
    # A second run replaces the tables rather than adding to them.
    em.update_silver_entitymasterdata(spark, config_path)

    for table in ["public", "main"]:
        name = f"{schema}.{table}"
        assert spark.catalog.tableExists(name)
        assert spark.sql(f"DESCRIBE DETAIL {name}").first()["format"] == "delta"
    assert sorted(main(spark, config_path).LEI) == ["LEI1", "LEI2"]
    assert sorted(public(spark, config_path).LEI) == ["LEI1", "LEI2"]


def test_a_table_missing_from_the_catalogue_is_reported_as_such(spark, tmp_path, schema):
    config_path = catalogued(tmp_path, schema)

    with pytest.raises(LookupError, match="Silver layer has to be updated"):
        em.connect_to_entitymasterdata(spark, config_path)


# How an LEI is written
# --------------------------------------------------------------------------

#: Two LEIs of the form GLEIF gives them, so that nothing about them is
#: malformed but the slip a test puts in.
KDPW = "2594000K576D5CQXI987"
FUND = "815600EABF185F5AF852"


def registers(config_path, leis, ccp=(), iorp=(), mfi=()):
    """A landing zone of GLEIF entities and the three registers named, imported."""
    write_gleif_level_1(config_path, leis, [f"Entity {lei}" for lei in leis],
                        ["PL"] * len(leis))
    write_gleif_level_2(config_path, [])
    write_ccp(config_path, list(ccp))
    write_iorp(config_path, list(iorp))
    write_ecb_mfi(config_path, list(mfi))
    write_eiopa(config_path, [])
    write_ecb_investment_fund(config_path, [])
    em.update_from_landing_to_bronze(config_path)


def test_an_lei_with_a_trailing_space_still_finds_its_entity(spark, data_lake):
    """The list of CCPs of September 2026 carried six LEIs with a trailing
    space, and two central counterparties were left without a sector."""
    registers(data_lake, [KDPW], ccp=[(KDPW + " ", "CCP", "EUCCP")])

    em.update_silver_entitymasterdata_public(spark, data_lake)

    assert entity(public(spark, data_lake), KDPW).SECTOR == "CCP"


def test_an_lei_in_lower_case_still_finds_its_entity(spark, data_lake):
    """The IORP register of the same month carried three in lower case."""
    registers(data_lake, [FUND], iorp=[FUND.lower()])

    em.update_silver_entitymasterdata_public(spark, data_lake)

    assert entity(public(spark, data_lake), FUND).SECTOR == "ORPI"


def test_a_non_breaking_space_is_removed_as_well(spark, data_lake):
    """What a spreadsheet leaves behind, and what trim does not remove."""
    registers(data_lake, [KDPW], mfi=[(" " + KDPW, "Credit Institution")])

    em.update_silver_entitymasterdata_public(spark, data_lake)

    assert entity(public(spark, data_lake), KDPW).SECTOR == "CDTI"


def test_a_parent_written_differently_is_written_as_gleif_writes_it(spark, data_lake):
    write_gleif_level_1(data_lake, [KDPW], ["KDPW_CCP"], ["PL"])
    write_gleif_level_2(data_lake, [(KDPW.lower(), FUND + " ", "IS_ULTIMATELY_CONSOLIDATED_BY")])
    for write in (write_ccp, write_iorp, write_ecb_mfi, write_eiopa, write_ecb_investment_fund):
        write(data_lake, [])
    em.update_from_landing_to_bronze(data_lake)

    em.update_silver_entitymasterdata_public(spark, data_lake)

    assert entity(public(spark, data_lake), KDPW).ULTPAR_LEI == FUND


def test_an_lei_that_is_malformed_even_so_is_named_with_its_register(spark, data_lake, caplog):
    """It is kept, but it will not match GLEIF, and the run says so."""
    registers(data_lake, [KDPW], iorp=["81560OEABF185F5AF85"])

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        em.update_silver_entitymasterdata_public(spark, data_lake)

    warnings = [getattr(r, "ctx", {}) for r in caplog.records if r.levelno == logging.WARNING]
    assert {"source": "EIOPA_IORP", "leis": "81560OEABF185F5AF85"}.items() \
        <= next(w for w in warnings if w.get("source") == "EIOPA_IORP").items()
    assert not [w for w in warnings if w.get("source") in ("GLEIF_L1", "ESMA_CCP")]


# What a proprietary list may say
# --------------------------------------------------------------------------


def test_an_internal_lei_with_a_trailing_space_displaces_the_public_row(spark, built):
    """Written differently, it still has to displace the public row, or the
    entity would stand in the table twice, once under either spelling."""
    write_internal(built, pandas.DataFrame(
        {"LEI": ["lei1 "], "INTERNALID_1": ["A1"], "NAME": ["As we call it"]}))

    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    assert entity(frame, "LEI1").NAME == "As we call it"
    assert len(frame) == 2


def test_an_internal_list_describing_an_lei_twice_is_refused(spark, built):
    """Where the two rows agree on INTERNALID_1 the one kept would be whichever
    Spark read first. Written differently, they are still the same LEI."""
    write_internal(built, pandas.DataFrame(
        {"LEI": ["LEI1", "lei1"], "INTERNALID_1": ["A1", "A1"], "NAME": ["One", "Uno"]}))

    with pytest.raises(ValueError, match="LEIs described more than once.*LEI1"):
        em.update_silver_entitymasterdata(spark, built)


def test_an_internal_column_the_table_does_not_have_is_refused(spark, built):
    """Dropped when the table is written, it would take everything the list
    says under it with it, and nothing would say so."""
    write_internal(built, pandas.DataFrame(
        {"LEI": ["LEI1"], "NAME": ["One"], "Sector": ["CDTI"]}))

    with pytest.raises(ValueError, match="does not have.*'Sector'"):
        em.update_silver_entitymasterdata(spark, built)


def test_an_internal_list_without_an_lei_column_is_refused(spark, built):
    write_internal(built, pandas.DataFrame({"NAME": ["One"], "SECTOR": ["CDTI"]}))

    with pytest.raises(ValueError, match="no LEI column"):
        em.update_silver_entitymasterdata(spark, built)


def test_internal_entities_without_an_lei_are_not_duplicates_of_one_another(spark, built):
    """The duplicate check is about LEIs, and a row without one has none."""
    write_internal(built, pandas.DataFrame(
        {"LEI": [None, " "], "INTERNALID_1": ["A1", "A2"]}))

    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built)

    assert sorted(frame[frame.LEI.isna()].INTERNALID_1) == ["A1", "A2"]


# The catch-all entities
# --------------------------------------------------------------------------


def lei_checksum_holds(lei):
    """ISO 7064 MOD 97-10, the check ISO 17442 puts on the last two digits."""
    return int("".join(str(int(character, 36)) for character in lei)) % 97 == 1


@pytest.mark.parametrize("lei", [em.LEI_OTHER, em.LEI_UNKNOWN])
def test_a_catch_all_lei_has_the_form_of_an_lei(lei):
    """Placeholders such as XOTHERX and XUNKNOWNX would pass no check an LEI
    is put to, the module's own warning among them."""
    import re

    assert re.match(em.LEI_PATTERN, lei)
    assert lei_checksum_holds(lei)


def test_the_catch_all_entities_are_in_the_table(spark, built):
    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built, catch_all=True)

    assert entity(frame, em.LEI_OTHER).NAME == "Other"
    assert entity(frame, em.LEI_UNKNOWN).NAME == "Unknown"
    assert len(frame) == 4


def test_a_catch_all_entity_the_internal_list_describes_is_described_by_it(spark, built):
    write_internal(built, pandas.DataFrame(
        {"LEI": [em.LEI_OTHER], "NAME": ["Rest of the market"]}))

    em.update_silver_entitymasterdata(spark, built)
    frame = main(spark, built, catch_all=True)

    assert entity(frame, em.LEI_OTHER).NAME == "Rest of the market"


def test_the_catch_all_entities_are_not_named_as_malformed(spark, built, caplog):
    write_internal(built, pandas.DataFrame({"LEI": [em.LEI_UNKNOWN], "NAME": ["Unknown"]}))

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        em.update_silver_entitymasterdata(spark, built)

    assert not [r for r in caplog.records
                if getattr(r, "ctx", {}).get("source") == "INTERNAL"]
