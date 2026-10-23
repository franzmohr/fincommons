"""Tests of the helpers of the FIRDS landing zone.

The helpers decide which files the import picks up, what reference date a file
carries and which of two copies of the same file is the redundant one. They act
on the file system, so getting them wrong loses or duplicates a delta file
rather than raising, which is why they are pinned down here.
"""

from __future__ import annotations

from datetime import date
import os
import zipfile

import pytest
import yaml

from fincommons.firds import common, utilities


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


def imported(config_path, *names, content="x"):
    """Put files into the import folder of the landing zone."""
    folder = common.path_landing_import(config_path)
    for name in names:
        with open(os.path.join(folder, name), "w") as handle:
            handle.write(content)
    return folder


# Choosing the files to import
# --------------------------------------------------------------------------


def test_only_xml_and_zip_files_are_candidates(config):
    imported(config, "DLTINS_20240102_01of01.xml", "FULINS_E_20240101_01of01.zip",
             "notes.txt", "checksum.md5")

    candidates = utilities.get_candidates_for_import(config)

    assert sorted(candidates) == [
        "DLTINS_20240102_01of01.xml",
        "FULINS_E_20240101_01of01.zip",
    ]


def test_an_empty_import_folder_offers_no_candidates(config):
    assert utilities.get_candidates_for_import(config) == []


# Removing a file that is present twice
# --------------------------------------------------------------------------


def test_the_extracted_xml_is_removed_when_its_archive_is_also_there(config):
    """A zip that was already extracted leaves both files in the folder, and
    importing both would read the same data twice."""
    folder = imported(config, "DLTINS_20240102_01of01.zip", "DLTINS_20240102_01of01.xml")

    utilities.remove_duplicates_from_landing_import_folder(config)

    assert os.path.exists(os.path.join(folder, "DLTINS_20240102_01of01.zip"))
    assert not os.path.exists(os.path.join(folder, "DLTINS_20240102_01of01.xml"))


def test_a_file_that_is_present_once_is_kept(config):
    folder = imported(config, "DLTINS_20240102_01of01.xml", "DLTINS_20240103_01of01.zip")

    utilities.remove_duplicates_from_landing_import_folder(config)

    assert sorted(os.listdir(folder)) == [
        "DLTINS_20240102_01of01.xml",
        "DLTINS_20240103_01of01.zip",
    ]


def test_directories_in_the_import_folder_are_not_taken_for_files(config):
    folder = imported(config, "DLTINS_20240102_01of01.xml")
    os.mkdir(os.path.join(folder, "DLTINS_20240102_01of01"))

    utilities.remove_duplicates_from_landing_import_folder(config)

    assert os.path.exists(os.path.join(folder, "DLTINS_20240102_01of01.xml"))


# The reference date a file carries
# --------------------------------------------------------------------------


def test_the_reference_date_of_a_delta_file_is_read_from_its_name(config):
    assert utilities.get_dltins_data_date("DLTINS_20240102_01of01.xml") == "2024-01-02"


def test_the_reference_date_of_a_full_file_is_read_from_its_name(config):
    assert utilities.get_fulins_data_date("FULINS_E_20240101_01of02.xml") == "2024-01-01"


def test_the_date_is_read_from_the_name_and_not_from_the_path(config):
    """The functions are given a full path by the import, so the directories
    above the file must not shift the characters the date is cut from."""
    assert utilities.get_dltins_data_date(
        "/Volumes/cat/schema/vol/landing/import/DLTINS_20240102_01of01.xml"
    ) == "2024-01-02"


# Unpacking
# --------------------------------------------------------------------------


def test_unzipping_extracts_the_file_and_returns_its_name(config):
    folder = common.path_landing_import(config)
    with zipfile.ZipFile(os.path.join(folder, "DLTINS_20240102_01of01.zip"), "w") as archive:
        archive.writestr("DLTINS_20240102_01of01.xml", "<xml/>")

    name = utilities.unzip_and_get_names(config, "DLTINS_20240102_01of01.zip")

    assert name == "DLTINS_20240102_01of01.xml"
    assert os.path.exists(os.path.join(folder, name))


# Reading the head of a file
# --------------------------------------------------------------------------


def test_only_the_first_characters_of_a_file_are_read(config):
    """The XML files run to gigabytes, so the schema is identified from the
    head of the file rather than by parsing it."""
    folder = imported(config, "big.xml", content="0123456789" * 100)

    assert utilities.read_first_n_chars(os.path.join(folder, "big.xml"), 5) == "01234"


def test_the_text_between_two_tags_is_returned():
    text = "<Document><MsgDefIdr>auth.036.001.03</MsgDefIdr><Rest/></Document>"

    assert utilities.extract_between_keywords(text, "MsgDefIdr") == "auth.036.001.03"


def test_a_tag_that_is_absent_yields_nothing():
    assert utilities.extract_between_keywords("<Document/>", "MsgDefIdr") is None


def test_a_tag_that_is_not_closed_yields_nothing():
    """Only the first characters of the file are read, so the header may have
    been cut in half. The remainder of the text is not what the file states
    about the node."""
    text = "<Document><MsgDefIdr>auth.036.001"

    assert utilities.extract_between_keywords(text, "MsgDefIdr") is None


def test_the_publication_date_of_a_cancellations_file_is_read_off_its_name():
    """FULCAN_<date>_<n>of<m>.zip puts the date at the same offset as a delta
    file does, but the two are read out separately so that a change to one
    naming convention cannot silently move the other."""
    assert utilities.get_fulcan_data_date("FULCAN_20240103_01of02.zip") == "2024-01-03"
    assert utilities.get_fulcan_data_date("/a/b/FULCAN_20260905_02of02.xml") == "2026-09-05"


# The names ESMA gives its files
# --------------------------------------------------------------------------


def test_the_parts_of_a_name_are_read_off_it():
    parsed = utilities.parse_file_name("/x/FULINS_E_20260905_03of04.zip")

    assert parsed == {"kind": "FULINS", "cfi": "E", "date": date(2026, 9, 5),
                      "part": 3, "parts": 4}
    assert utilities.parse_file_name("DLTINS_20260906_01of02.xml")["cfi"] is None
    assert utilities.parse_file_name("notes.txt") is None


def test_a_day_with_every_part_is_complete():
    names = ["DLTINS_20260906_01of02.zip", "DLTINS_20260906_02of02.zip"]

    assert utilities.missing_parts(names, "DLTINS", date(2026, 9, 6)) == []


def test_a_missing_part_is_named():
    names = ["DLTINS_20260906_02of03.zip", "DLTINS_20260907_01of01.zip"]

    assert utilities.missing_parts(names, "DLTINS", date(2026, 9, 6)) == [
        "DLTINS_20260906_01of03", "DLTINS_20260906_03of03",
    ]


def test_a_day_without_any_part_is_incomplete():
    names = ["DLTINS_20260907_01of01.zip", "FULCAN_20260906_01of01.zip"]

    assert utilities.missing_parts(names, "DLTINS", date(2026, 9, 6)) != []


def test_the_parts_of_a_full_file_are_counted_per_cfi_letter():
    names = ["FULINS_C_20260905_01of01.zip",
             "FULINS_E_20260905_01of02.zip"]

    assert utilities.missing_parts(names, "FULINS", date(2026, 9, 5)) == [
        "FULINS_E_20260905_02of02",
    ]


def test_a_cfi_letter_without_any_part_is_found_against_the_register_s_list():
    names = ["FULINS_E_20260905_01of01.zip"]
    listed = ["FULINS_C_20260905_01of01.zip", "FULINS_E_20260905_01of01.zip"]

    assert utilities.missing_parts(names, "FULINS", date(2026, 9, 5)) == []
    assert utilities.unimported_listed_files(names, listed) == [
        "FULINS_C_20260905_01of01",
    ]


def test_an_extracted_file_counts_as_its_archive():
    names = ["FULINS_C_20260905_01of01.xml", "fulins_e_20260905_01of01.zip"]
    listed = ["FULINS_C_20260905_01of01.zip", "FULINS_E_20260905_01of01.zip"]

    assert utilities.unimported_listed_files(names, listed) == []


# What a download that was interrupted leaves behind
# --------------------------------------------------------------------------


def test_a_partial_download_is_not_a_candidate_for_import(config):
    """A download is written as <file>.zip.part and renamed once it is
    complete. A hard stop leaves that file behind, and it must not be taken
    for an archive merely because the extension appears somewhere in its
    name."""
    imported(config, "DLTINS_20240102_01of01.zip.part",
             "DLTINS_20240103_01of01.zip")

    assert utilities.get_candidates_for_import(config) == [
        "DLTINS_20240103_01of01.zip",
    ]


def test_a_partial_download_beside_its_archive_removes_nothing(config):
    """Both carry the same name up to the first dot, which must not count as a
    duplicate: the removal would then fail on an XML file that is not there."""
    folder = imported(config, "DLTINS_20240102_01of01.zip",
                      "DLTINS_20240102_01of01.zip.part")

    utilities.remove_duplicates_from_landing_import_folder(config)

    assert sorted(os.listdir(folder)) == [
        "DLTINS_20240102_01of01.zip",
        "DLTINS_20240102_01of01.zip.part",
    ]


def test_two_files_that_are_not_an_archive_and_its_xml_are_kept(config):
    folder = imported(config, "DLTINS_20240102_01of01.xml", "DLTINS_20240102_01of01.md5")

    utilities.remove_duplicates_from_landing_import_folder(config)

    assert len(os.listdir(folder)) == 2
