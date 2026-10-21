"""Tests of the rename of a data lake's folders to snake_case.

Folder names travel into a SQL catalogue, where a hyphen has to be quoted, so
the folders of the lake are spelled in snake_case. A lake whose tables stand
under hyphenated names is moved to the snake_case ones the modules read by
rename_folders_to_snake_case. It touches the lake itself, so what it must
leave alone matters as much as what it renames.
"""

from __future__ import annotations

import pytest

from fincommons.addressing import rename_files, rename_folders_to_snake_case


def tree(root, *paths):
    """Create the given directories below ``root``, and a file in each leaf."""
    for path in paths:
        folder = root / path
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "part-00000.parquet").write_text("x")
    return root


def folders(root):
    return sorted(str(path.relative_to(root)).replace("\\", "/")
                  for path in root.rglob("*") if path.is_dir())


def test_a_hyphenated_folder_is_renamed(tmp_path):
    root = tree(tmp_path, "bronze/auth-079-001-02")

    rename_folders_to_snake_case(str(tmp_path / "bronze"))

    assert folders(tmp_path) == ["bronze", "bronze/auth_079_001_02"]


def test_the_data_moves_with_the_folder(tmp_path):
    tree(tmp_path, "silver/end-of-day-2020/main")

    rename_folders_to_snake_case(str(tmp_path / "silver"))

    assert (tmp_path / "silver/end_of_day_2020/main/part-00000.parquet").is_file()


def test_nested_folders_are_renamed_at_every_level(tmp_path):
    tree(tmp_path, "gold/risk-analysis/market-overview")

    renamed = rename_folders_to_snake_case(str(tmp_path / "gold"))

    assert folders(tmp_path) == ["gold", "gold/risk_analysis",
                                 "gold/risk_analysis/market_overview"]
    assert len(renamed) == 2


def test_a_partition_keeps_the_hyphens_of_its_value(tmp_path):
    """DATADT=2024-06-03 is a name Spark writes and reads back; renamed it
    would not be a partition of that date."""
    tree(tmp_path, "bronze/auth-079-001-02/DATADT=2024-06-03")

    rename_folders_to_snake_case(str(tmp_path / "bronze"))

    assert "bronze/auth_079_001_02/DATADT=2024-06-03" in folders(tmp_path)


def test_a_folder_named_by_a_date_is_left_alone(tmp_path):
    """Not a name the pipeline chose, and whatever reads it expects the date."""
    tree(tmp_path, "landing/level-1/2024-06-03")

    rename_folders_to_snake_case(str(tmp_path / "landing"))

    assert "landing/level_1/2024-06-03" in folders(tmp_path)


def test_a_file_is_not_renamed(tmp_path):
    tree(tmp_path, "gold/single-files")
    (tmp_path / "gold/single-files/market-overview.parquet").write_text("x")

    rename_folders_to_snake_case(str(tmp_path / "gold"))

    assert (tmp_path / "gold/single_files/market-overview.parquet").is_file()


def test_a_lake_in_snake_case_is_left_alone(tmp_path):
    tree(tmp_path, "bronze/auth_079_001_02/DATADT=2024-06-03")

    assert rename_folders_to_snake_case(str(tmp_path / "bronze")) == []


def test_running_it_twice_renames_nothing_the_second_time(tmp_path):
    tree(tmp_path, "bronze/auth-079-001-02")

    rename_folders_to_snake_case(str(tmp_path / "bronze"))

    assert rename_folders_to_snake_case(str(tmp_path / "bronze")) == []


def test_a_folder_whose_new_name_is_taken_is_not_merged(tmp_path):
    """Which of the two holds the data to keep is not something to guess."""
    tree(tmp_path, "bronze/auth-079-001-02", "bronze/auth_079_001_02")

    with pytest.raises(FileExistsError, match="auth_079_001_02"):
        rename_folders_to_snake_case(str(tmp_path / "bronze"))

    assert folders(tmp_path) == ["bronze", "bronze/auth-079-001-02",
                                 "bronze/auth_079_001_02"]


def test_a_root_that_does_not_exist_is_skipped(tmp_path):
    assert rename_folders_to_snake_case(str(tmp_path / "gold")) == []


# Files that stand under another name than the one a module reads
# --------------------------------------------------------------------------


def test_a_file_is_renamed_to_its_new_name(tmp_path):
    (tmp_path / "ecb-exchangerate.parquet").write_text("x")

    renamed = rename_files([(str(tmp_path / "ecb-exchangerate.parquet"),
                             str(tmp_path / "ecb_exchangerate.parquet"))])

    assert (tmp_path / "ecb_exchangerate.parquet").read_text() == "x"
    assert len(renamed) == 1


def test_a_file_that_is_not_there_is_skipped(tmp_path):
    assert rename_files([(str(tmp_path / "old.csv"), str(tmp_path / "new.csv"))]) == []


def test_a_file_whose_new_name_is_taken_is_not_overwritten(tmp_path):
    (tmp_path / "old.csv").write_text("old")
    (tmp_path / "new.csv").write_text("new")

    with pytest.raises(FileExistsError, match="new.csv"):
        rename_files([(str(tmp_path / "old.csv"), str(tmp_path / "new.csv"))])

    assert (tmp_path / "new.csv").read_text() == "new"
