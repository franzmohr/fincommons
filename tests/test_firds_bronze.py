"""Tests of the import from the landing zone into the Bronze layer.

The import decides what a file is from the schema named in its head, reads it
with the reader for that schema and then moves it out of the import folder:
into ``finished`` when it was read, into ``error`` when it was not. That routing
is what the tests below are about. The readers themselves are replaced, because
what is at stake here is where a file ends up rather than how FIRDS XML is
parsed -- the parsing is covered by the Silver tests, which work on the frames
the readers produce.
"""

from __future__ import annotations

import logging
import os
import zipfile

import pytest
import yaml

from fincommons.firds import bronze, common


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


def document(schema):
    """The head of a FIRDS file, as far as the import reads it."""
    return f"<?xml version='1.0'?><BizData><Hdr><AppHdr><MsgDefIdr>{schema}</MsgDefIdr>" \
           f"</AppHdr></Hdr><Pyld/></BizData>"


def put_xml(config_path, name, schema):
    path = os.path.join(common.path_landing_import(config_path), name)
    with open(path, "w") as handle:
        handle.write(document(schema))
    return path


def put_zip(config_path, name, member, schema):
    path = os.path.join(common.path_landing_import(config_path), name)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, document(schema))
    return path


@pytest.fixture
def readers(monkeypatch, spark):
    """Replace the XML readers with one that returns a single row."""
    def read(session, file):
        return spark.createDataFrame([("ISIN1",)], "ISIN string")

    monkeypatch.setattr(bronze, "read_xml_017_001_02", read)
    monkeypatch.setattr(bronze, "read_xml_036_001_02", read)
    monkeypatch.setattr(bronze, "read_xml_036_001_03", read)
    monkeypatch.setattr(bronze, "read_xml_102_001_01", read)
    return read


def landing_folders(config_path):
    return (
        sorted(os.listdir(common.path_landing_import(config_path))),
        sorted(os.listdir(common.path_landing_finished(config_path))),
        sorted(os.listdir(common.path_landing_error(config_path))),
    )


# Files that are read
# --------------------------------------------------------------------------


def test_a_cancellations_file_is_imported_and_moved_to_finished(spark, config, readers):
    """The cancellations file carries its own schema, auth.102.001.01, and so
    reaches the Bronze layer through a reader and a folder of its own rather
    than being mistaken for a delta file."""
    put_xml(config, "FULCAN_20240103_01of01.xml", "auth.102.001.01")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["FULCAN_20240103_01of01.xml"]
    assert errors == []


def test_a_cancellations_file_is_written_under_its_publication_date(
    spark, config, readers
):
    put_xml(config, "FULCAN_20240103_01of01.xml", "auth.102.001.01")

    bronze.update_from_landing_to_bronze(spark, config)

    written = os.listdir(common.path_bronze_auth_102_001_01(config))
    assert "DATADT=2024-01-03" in written


def test_the_parts_of_one_cancellations_file_are_kept_together(spark, config, readers):
    """A cancellations file is published in several parts that share a date.
    Each part is appended, because a part that replaced the partition would
    leave the Bronze layer holding the last part alone."""
    put_xml(config, "FULCAN_20240103_01of02.xml", "auth.102.001.01")
    put_xml(config, "FULCAN_20240103_02of02.xml", "auth.102.001.01")

    bronze.update_from_landing_to_bronze(spark, config)

    frame = spark.read.parquet(common.path_bronze_auth_102_001_01(config))
    assert frame.count() == 2


def test_a_full_file_is_imported_and_moved_to_finished(spark, config, readers):
    put_xml(config, "FULINS_E_20240101_01of01.xml", "auth.017.001.02")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["FULINS_E_20240101_01of01.xml"]
    assert errors == []


def test_a_delta_file_is_imported_and_moved_to_finished(spark, config, readers):
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["DLTINS_20240102_01of01.xml"]
    assert errors == []


def test_the_file_is_partitioned_by_the_date_in_its_name(spark, config, readers):
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    partitions = os.listdir(common.path_bronze_auth_036_001_03(config))
    assert "DATADT=2024-01-02" in partitions


def test_a_delta_file_of_the_older_definition_is_imported(spark, config, readers):
    """ESMA published the delta file as auth.036.001.02 before revising it, so
    a history reaching back far enough is built from files of both."""
    put_xml(config, "DLTINS_20200105_01of01.xml", "auth.036.001.02")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["DLTINS_20200105_01of01.xml"]
    assert errors == []


def test_each_delta_definition_goes_to_its_own_folder(spark, config, readers):
    """Every folder of the Bronze layer is named after the message definition
    it holds, so the layer says which message a file was published under."""
    put_xml(config, "DLTINS_20200105_01of01.xml", "auth.036.001.02")
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    older = os.listdir(common.path_bronze_auth_036_001_02(config))
    newer = os.listdir(common.path_bronze_auth_036_001_03(config))
    assert "DATADT=2020-01-05" in older and "DATADT=2024-01-02" not in older
    assert "DATADT=2024-01-02" in newer and "DATADT=2020-01-05" not in newer


def test_the_silver_layer_sees_a_date_in_either_delta_folder(spark, config, readers):
    """The history is built from both definitions without caring which, so
    every part of it that looks for a reference date looks in both folders."""
    put_xml(config, "DLTINS_20200105_01of01.xml", "auth.036.001.02")
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    from fincommons.firds import silver
    assert [str(d) for d in silver.dltins_reference_dates(config)] == [
        "2020-01-05", "2024-01-02"
    ]


def test_the_full_and_delta_files_go_to_their_own_folders(spark, config, readers):
    put_xml(config, "FULINS_E_20240101_01of01.xml", "auth.017.001.02")
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    assert "DATADT=2024-01-01" in os.listdir(common.path_bronze_auth_017_001_02(config))
    assert "DATADT=2024-01-02" in os.listdir(common.path_bronze_auth_036_001_03(config))


# Archives
# --------------------------------------------------------------------------


def test_an_archive_is_unpacked_and_only_the_archive_is_kept(spark, config, readers):
    """The extracted XML is removed and the archive is what moves on, so that
    the landing zone keeps what was published rather than what was unpacked."""
    put_zip(config, "DLTINS_20240102_01of01.zip", "DLTINS_20240102_01of01.xml",
            "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["DLTINS_20240102_01of01.zip"]
    assert errors == []


# Files that are not read
# --------------------------------------------------------------------------


def test_a_file_of_an_unknown_schema_is_moved_to_error(spark, config, readers):
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.999.001.01")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == []
    assert errors == ["DLTINS_20240102_01of01.xml"]


def test_a_file_that_names_no_schema_is_moved_to_error(spark, config, readers):
    path = os.path.join(common.path_landing_import(config), "DLTINS_20240102_01of01.xml")
    with open(path, "w") as handle:
        handle.write("<BizData><Hdr/></BizData>")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert errors == ["DLTINS_20240102_01of01.xml"]


def test_a_file_the_reader_cannot_read_is_moved_to_error(monkeypatch, spark, config, readers):
    def refuse(session, file):
        raise ValueError("the file is not readable")

    monkeypatch.setattr(bronze, "read_xml_036_001_03", refuse)
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == []
    assert errors == ["DLTINS_20240102_01of01.xml"]


def test_one_unreadable_file_does_not_stop_the_others(monkeypatch, spark, config, readers):
    """The import runs unattended, so a single bad file has to be set aside
    rather than abandon the files behind it in the folder."""
    calls = {"n": 0}

    def refuse_the_first(session, file):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("the file is not readable")
        return spark.createDataFrame([("ISIN1",)], "ISIN string")

    monkeypatch.setattr(bronze, "read_xml_036_001_03", refuse_the_first)
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")
    put_xml(config, "DLTINS_20240103_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert errors == ["DLTINS_20240102_01of01.xml"]
    assert finished == ["DLTINS_20240103_01of01.xml"]


def test_a_corrupt_archive_is_quarantined(spark, config, readers):
    """ESMA's files arrive as ZIP archives and a download can be truncated.
    zipfile raises BadZipFile, which does not derive from ValueError, so a
    handler narrower than that lets it end the run."""
    path = os.path.join(common.path_landing_import(config), "DLTINS_20240102_01of01.zip")
    with open(path, "w") as handle:
        handle.write("this is not a zip file")

    bronze.update_from_landing_to_bronze(spark, config)

    assert landing_folders(config) == ([], [], ["DLTINS_20240102_01of01.zip"])


def test_a_file_that_cannot_be_decoded_is_quarantined(spark, config, readers):
    """The header is read as UTF-8 text. A file that is not text at all --
    a truncated download, a stray binary -- has to be set aside like any
    other rather than end the run before the loop can quarantine it."""
    path = os.path.join(common.path_landing_import(config), "DLTINS_20240102_01of01.xml")
    with open(path, "wb") as handle:
        handle.write(b"\xff\xfe\x00\x00 not utf-8 \xc3\x28")

    bronze.update_from_landing_to_bronze(spark, config)

    assert landing_folders(config) == ([], [], ["DLTINS_20240102_01of01.xml"])


def test_a_file_that_cannot_be_decoded_does_not_strand_the_rest(spark, config, readers):
    path = os.path.join(common.path_landing_import(config), "AAA_bad.xml")
    with open(path, "wb") as handle:
        handle.write(b"\xff\xfe\x00\x00 not utf-8 \xc3\x28")
    put_xml(config, "DLTINS_20240103_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["DLTINS_20240103_01of01.xml"]
    assert errors == ["AAA_bad.xml"]


# Nothing to do
# --------------------------------------------------------------------------


def test_an_empty_import_folder_is_not_an_error(spark, config, readers):
    bronze.update_from_landing_to_bronze(spark, config)

    assert landing_folders(config) == ([], [], [])


# What the run leaves behind in the log
# --------------------------------------------------------------------------

# A file that cannot be read is set aside and the import carries on, which is
# what a bulk import of files delivered by a third party has to do. The log is
# then the only record that anything was set aside at all, so it has to carry
# the traceback -- a malformed file and a failing write to the Bronze layer are
# otherwise indistinguishable afterwards -- and a count, because an import that
# quarantines everything looks from the outside like a clean run.


def test_a_quarantined_file_is_recorded_with_its_traceback(
    monkeypatch, spark, config, readers, caplog
):
    def refuse(session, file):
        raise ValueError("the file is not readable")

    monkeypatch.setattr(bronze, "read_xml_036_001_03", refuse)
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        bronze.update_from_landing_to_bronze(spark, config)

    failures = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(failures) == 1
    assert failures[0].ctx["file"] == "DLTINS_20240102_01of01.xml"
    assert "the file is not readable" in caplog.text
    assert failures[0].exc_info is not None


def test_the_closing_record_counts_what_was_imported_and_what_was_set_aside(
    monkeypatch, spark, config, readers, caplog
):
    calls = {"n": 0}

    def refuse_the_first(session, file):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("the file is not readable")
        return spark.createDataFrame([("ISIN1",)], "ISIN string")

    monkeypatch.setattr(bronze, "read_xml_036_001_03", refuse_the_first)
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")
    put_xml(config, "DLTINS_20240103_01of01.xml", "auth.036.001.03")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        bronze.update_from_landing_to_bronze(spark, config)

    finished = [r for r in caplog.records if r.msg == "%s finished"]
    assert finished[-1].ctx["candidates"] == 2
    assert finished[-1].ctx["imported"] == 1
    assert finished[-1].ctx["quarantined"] == 1


def test_an_unrecognised_schema_is_recorded_as_a_warning(
    spark, config, readers, caplog
):
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.999.001.01")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        bronze.update_from_landing_to_bronze(spark, config)

    warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "Schema" in r.msg
    ]
    assert len(warnings) == 1
    assert warnings[0].ctx["schema"] == "auth.999.001.01"


def test_a_zip_of_an_unrecognised_schema_is_recorded_too(
    spark, config, readers, caplog
):
    """The archive was quarantined without a word before, because the message
    sat in the branch that handles a plain file."""
    put_zip(config, "DLTINS_20240102_01of01.zip", "DLTINS_20240102_01of01.xml",
            "auth.999.001.01")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        bronze.update_from_landing_to_bronze(spark, config)

    warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "Schema" in r.msg
    ]
    assert len(warnings) == 1
    assert warnings[0].ctx["file"] == "DLTINS_20240102_01of01.zip"


def test_a_clean_run_records_no_warning(spark, config, readers, caplog):
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        bronze.update_from_landing_to_bronze(spark, config)

    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


# A file imported before
# --------------------------------------------------------------------------


def test_a_file_already_imported_is_not_imported_again(spark, config, readers, caplog):
    """The Bronze layer is only ever appended to, so a second import would put
    every record of the file into it twice."""
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")
    bronze.update_from_landing_to_bronze(spark, config)
    put_zip(config, "DLTINS_20240102_01of01.zip", "DLTINS_20240102_01of01.xml",
            "auth.036.001.03")

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        bronze.update_from_landing_to_bronze(spark, config)

    frame = spark.read.parquet(common.path_bronze_auth_036_001_03(config))
    assert frame.count() == 1
    imports, finished, errors = landing_folders(config)
    assert imports == []
    assert finished == ["DLTINS_20240102_01of01.xml"]
    assert errors == ["DLTINS_20240102_01of01.zip"]
    assert any("already imported" in r.msg for r in caplog.records)


def test_a_quarantined_copy_replaces_an_older_one(spark, config, readers):
    open(os.path.join(common.path_landing_error(config), "AAA_bad.xml"), "w").close()
    put_xml(config, "AAA_bad.xml", "auth.999")

    bronze.update_from_landing_to_bronze(spark, config)

    assert landing_folders(config) == ([], [], ["AAA_bad.xml"])


# A file that carries no records
# --------------------------------------------------------------------------


def test_a_file_without_records_still_marks_its_reference_date(spark, config, monkeypatch):
    """Writing nothing would leave the date without a partition, and the Silver
    layer cannot tell that from a partition that has gone missing."""
    monkeypatch.setattr(
        bronze, "read_xml_036_001_03",
        lambda session, file: spark.createDataFrame([], "ISIN string"),
    )
    put_xml(config, "DLTINS_20240102_01of01.xml", "auth.036.001.03")

    bronze.update_from_landing_to_bronze(spark, config)

    partition = os.path.join(common.path_bronze_auth_036_001_03(config),
                             "DATADT=2024-01-02")
    assert os.path.isdir(partition)
    assert os.listdir(partition) == []
    imports, finished, errors = landing_folders(config)
    assert finished == ["DLTINS_20240102_01of01.xml"]
    assert errors == []
