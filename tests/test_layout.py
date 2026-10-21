"""Tests of the file sizing of partitioned Parquet tables.

What is pinned here is what a caller of :mod:`fincommons.layout` relies on: a
rebalanced write produces about one file per partition rather than one per
partition and task; a compaction changes the number of files and nothing
else, leaves a folder that is already compact alone, keeps every column even
where the files of a partition disagree on which they carry, sets aside a
partition it cannot merge without touching it, refuses a Delta table, and
completes a swap that an earlier run left half done.
"""

from __future__ import annotations

import datetime
import os

import pyspark.sql.functions as func
import pytest

from fincommons import layout


def parquet_files(folder):
    return sorted(f for f in os.listdir(folder) if f.endswith(".parquet"))


def frame(spark, days=2, rows=400):
    """``rows`` records per reference date, spread over many tasks."""
    records = [
        (datetime.date(2026, 1, 1 + d), i, f"name-{i}")
        for d in range(days)
        for i in range(rows)
    ]
    return spark.createDataFrame(records, "DATADT date, ID int, NAME string")


def write_scattered(df, path, tasks=8):
    """Write ``df`` the way a pipeline without the hint does."""
    df.repartition(tasks).write.partitionBy("DATADT").mode("append").parquet(path)


def listing(path):
    """The names in ``path``, without the checksums the local file system adds."""
    return sorted(name for name in os.listdir(path) if not name.endswith(".crc"))


def snapshot(spark, path):
    return sorted(spark.read.parquet(path).collect())


def test_a_rebalanced_write_writes_about_one_file_per_partition(spark, tmp_path):
    scattered = str(tmp_path / "scattered")
    tidy = str(tmp_path / "tidy")
    df = frame(spark)

    write_scattered(df, scattered)
    layout.rebalanced(df.repartition(8), "DATADT") \
        .write.partitionBy("DATADT").mode("append").parquet(tidy)

    partition = "DATADT=2026-01-01"
    assert len(parquet_files(os.path.join(scattered, partition))) == 8
    assert len(parquet_files(os.path.join(tidy, partition))) == 1
    assert snapshot(spark, tidy) == snapshot(spark, scattered)


def test_compaction_changes_the_files_and_not_the_rows(spark, tmp_path):
    path = str(tmp_path / "table")
    write_scattered(frame(spark), path)
    before = snapshot(spark, path)

    result = layout.compact_partitions(spark, path)

    assert len(result["compacted"]) == 2
    assert result["files_before"] == 16
    assert result["files_after"] == 2
    assert result["failed"] == []
    for partition in ("DATADT=2026-01-01", "DATADT=2026-01-02"):
        folder = os.path.join(path, partition)
        assert len(parquet_files(folder)) == 1
        assert not os.path.exists(os.path.join(folder, "_SUCCESS"))
    assert snapshot(spark, path) == before
    # Nothing is left beside the partitions.
    assert listing(path) == ["DATADT=2026-01-01", "DATADT=2026-01-02", "_SUCCESS"]


def test_the_number_of_files_follows_the_size_of_the_partition(spark, tmp_path):
    path = str(tmp_path / "table")
    write_scattered(frame(spark, days=1), path)
    folder = os.path.join(path, "DATADT=2026-01-01")
    size = sum(os.path.getsize(os.path.join(folder, f)) for f in parquet_files(folder))

    # A little more than a third of the partition per file: three files.
    layout.compact_partitions(spark, path, target_file_size=size // 3 + 1)

    assert len(parquet_files(folder)) == 3


def test_a_compact_partition_is_left_alone(spark, tmp_path):
    path = str(tmp_path / "table")
    layout.rebalanced(frame(spark), "DATADT") \
        .write.partitionBy("DATADT").parquet(path)
    folder = os.path.join(path, "DATADT=2026-01-01")
    files = parquet_files(folder)

    result = layout.compact_partitions(spark, path)

    assert result["compacted"] == []
    assert parquet_files(folder) == files


def test_an_empty_partition_is_left_alone(spark, tmp_path):
    # The FIRDS Bronze layer records a file without records as an empty
    # partition, and the Silver layer relies on finding it.
    path = str(tmp_path / "table")
    write_scattered(frame(spark, days=1), path)
    empty = os.path.join(path, "DATADT=2026-01-05")
    os.makedirs(empty)

    layout.compact_partitions(spark, path)

    assert os.path.isdir(empty)
    assert listing(empty) == []


def test_nested_partitions_keep_their_columns(spark, tmp_path):
    path = str(tmp_path / "table")
    df = frame(spark).withColumn("VENUE", (func.col("ID") % 2).cast("string"))
    df.repartition(8).write.partitionBy("DATADT", "VENUE").parquet(path)
    before = snapshot(spark, path)

    result = layout.compact_partitions(spark, path)

    assert len(result["compacted"]) == 4
    assert snapshot(spark, path) == before
    assert spark.read.parquet(path).columns == ["ID", "NAME", "DATADT", "VENUE"]


def test_a_column_only_some_files_carry_survives_the_compaction(spark, tmp_path):
    path = str(tmp_path / "table")
    folder = os.path.join(path, "DATADT=2026-01-01")
    spark.createDataFrame([(1,), (2,)], "ID int").write.mode("append").parquet(folder)
    spark.createDataFrame([(3, "x"), (4, "y")], "ID int, NAME string") \
        .write.mode("append").parquet(folder)

    layout.compact_partitions(spark, path)

    rows = spark.read.parquet(folder).orderBy("ID").collect()
    assert [(r.ID, r.NAME) for r in rows] == [(1, None), (2, None), (3, "x"), (4, "y")]


def test_a_partition_that_cannot_be_merged_is_left_as_it_was(spark, tmp_path, caplog):
    path = str(tmp_path / "table")
    broken = os.path.join(path, "DATADT=2026-01-01")
    spark.createDataFrame([(1, 1.5)], "ID int, VALUE double").write.mode("append").parquet(broken)
    spark.createDataFrame([(2, "a")], "ID int, VALUE string").write.mode("append").parquet(broken)
    write_scattered(
        frame(spark, days=1).withColumn("DATADT", func.date_add("DATADT", 1)), path
    )
    files = parquet_files(broken)

    with caplog.at_level("INFO", logger="fincommons.layout"):
        result = layout.compact_partitions(spark, path)

    assert result["failed"] == [broken]
    assert result["compacted"] == [os.path.join(path, "DATADT=2026-01-02")]
    assert parquet_files(broken) == files
    assert not any(name.startswith(".compact") for name in os.listdir(path))
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert [r.getMessage() for r in errors] == [
        "Could not compact a partition, it was left as it was"
    ]
    assert errors[0].exc_info is not None
    assert "Partitions could not be compacted and keep their files" in caplog.text


def test_a_delta_table_is_refused(spark, tmp_path):
    path = str(tmp_path / "table")
    os.makedirs(os.path.join(path, "_delta_log"))

    with pytest.raises(ValueError, match="OPTIMIZE"):
        layout.compact_partitions(spark, path)


def test_a_missing_folder_is_refused(spark, tmp_path):
    with pytest.raises(ValueError, match="not a folder"):
        layout.compact_partitions(spark, str(tmp_path / "nowhere"))


def interrupted(spark, tmp_path):
    """A table, its scattered partition and a verified rewrite of it."""
    path = str(tmp_path / "table")
    write_scattered(frame(spark, days=1), path)
    before = snapshot(spark, path)
    name = "DATADT=2026-01-01"
    folder = os.path.join(path, name)
    staging = os.path.join(path, layout.STAGING_PREFIX + name)
    retired = os.path.join(path, layout.RETIRED_PREFIX + name)
    spark.read.parquet(folder).repartition(1).write.parquet(staging)
    return path, folder, staging, retired, before


def test_a_swap_that_died_between_the_renames_is_completed(spark, tmp_path):
    path, folder, staging, retired, before = interrupted(spark, tmp_path)
    os.rename(folder, retired)

    layout.compact_partitions(spark, path)

    assert listing(path) == ["DATADT=2026-01-01", "_SUCCESS"]
    assert len(parquet_files(folder)) == 1
    assert snapshot(spark, path) == before


def test_a_swap_that_died_before_its_clean_up_is_cleaned_up(spark, tmp_path):
    path, folder, staging, retired, before = interrupted(spark, tmp_path)
    os.rename(folder, retired)
    os.rename(staging, folder)

    layout.compact_partitions(spark, path)

    assert listing(path) == ["DATADT=2026-01-01", "_SUCCESS"]
    assert snapshot(spark, path) == before


def test_a_rewrite_that_died_before_the_swap_is_discarded(spark, tmp_path):
    path, folder, staging, retired, before = interrupted(spark, tmp_path)

    layout.compact_partitions(spark, path)

    assert listing(path) == ["DATADT=2026-01-01", "_SUCCESS"]
    assert len(parquet_files(folder)) == 1
    assert snapshot(spark, path) == before


def test_a_reader_never_takes_the_work_folders_for_partitions(spark, tmp_path):
    path, folder, staging, retired, before = interrupted(spark, tmp_path)

    assert snapshot(spark, path) == before
