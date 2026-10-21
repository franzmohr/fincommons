"""Tests of the storage shared by the Silver tables that are rebuilt in full.

The module turns two facts -- how a table is addressed and how it is stored --
into a read and a write, so the tests are about the combinations: a table at a
path in either format, a table in the catalogue, and a format asked for that
the addressing or the table on disk does not allow. The tests that decide a
format need no Spark session, because the decision is made from the
configuration and the file system before a session is put to work.
"""

from __future__ import annotations

import os
import uuid

import pyspark.sql.functions as F
import pytest

from fincommons import storage
from fincommons.addressing import TableRef


# Deciding the format
# --------------------------------------------------------------------------


def test_a_new_table_at_a_path_is_parquet_unless_delta_is_asked_for(tmp_path):
    ref = TableRef(path=str(tmp_path / "table"))

    assert storage.silver_format(ref) == "parquet"
    assert storage.silver_format(ref, "delta") == "delta"


def test_a_table_in_the_catalogue_is_delta_without_being_asked(tmp_path):
    """This is what lets a change of the configuration file alone move a
    table into the catalogue."""
    ref = TableRef(name="lake.silver.table")

    assert storage.silver_format(ref) == "delta"
    assert storage.silver_format(ref, "delta") == "delta"


def test_parquet_is_refused_for_a_table_in_the_catalogue():
    with pytest.raises(ValueError, match="cannot be written as 'parquet'"):
        storage.silver_format(TableRef(name="lake.silver.table"), "parquet")


def test_the_name_wins_where_a_layer_carries_both():
    ref = TableRef(path="/nowhere", name="lake.silver.table")

    with pytest.raises(ValueError, match="catalogue name"):
        storage.silver_format(ref, "parquet")


def test_an_unknown_format_is_reported_as_such(tmp_path):
    with pytest.raises(ValueError, match="'csv' not recognised"):
        storage.silver_format(TableRef(path=str(tmp_path)), "csv")
    with pytest.raises(ValueError, match="'csv' not recognised"):
        storage.silver_format(TableRef(name="lake.silver.table"), "csv")


def delta_on_disk(path):
    """A directory that looks like a Delta table to the format check."""
    os.makedirs(os.path.join(path, "_delta_log"))
    return path


def parquet_on_disk(path):
    """A directory that looks like a parquet table to the format check."""
    os.makedirs(path)
    with open(os.path.join(path, "part-00000.parquet"), "wb") as f:
        f.write(b"PAR1")
    return path


def test_a_table_on_disk_keeps_its_format_where_none_is_asked_for(tmp_path):
    delta = delta_on_disk(str(tmp_path / "delta"))
    parquet = parquet_on_disk(str(tmp_path / "parquet"))

    assert storage.silver_format(delta) == "delta"
    assert storage.silver_format(parquet) == "parquet"
    assert storage.file_format_on_disk(parquet, default="delta") == "parquet"


def test_parquet_is_refused_over_a_delta_table(tmp_path):
    """Writing parquet over a Delta table would delete its log, and with it the
    history the table was made Delta for."""
    path = delta_on_disk(str(tmp_path / "table"))

    with pytest.raises(ValueError, match="stored as 'delta'"):
        storage.silver_format(TableRef(path=path), "parquet")


def test_delta_is_refused_over_a_parquet_table(tmp_path):
    """Delta will not adopt files it has no log for."""
    path = parquet_on_disk(str(tmp_path / "table"))

    with pytest.raises(ValueError, match="stored as 'parquet'"):
        storage.silver_format(TableRef(path=path), "delta")


def test_an_empty_directory_holds_no_table(tmp_path):
    """It is what setting up the directory tree leaves behind."""
    path = tmp_path / "table"
    path.mkdir()

    assert storage.silver_format(str(path), "delta") == "delta"
    assert storage.file_format_on_disk(str(path)) == storage.DEFAULT_SILVER_FORMAT


def test_a_table_that_has_never_been_written_is_reported_as_such(tmp_path):
    # A table at a path is looked for on the file system, so no session is
    # asked.
    with pytest.raises(FileNotFoundError, match="Silver layer has to be updated"):
        storage.read_silver(None, TableRef(path=str(tmp_path / "table")))


@pytest.mark.parametrize(
    "path",
    ["abfss://lake@account.dfs.core.windows.net/silver/main",
     "s3://bucket/silver/main", "dbfs:/silver/main"],
)
def test_a_uri_is_refused_rather_than_taken_for_an_empty_location(path):
    """The file system cannot see a table there, so a Delta table would look
    absent and a parquet overwrite would throw its history away."""
    with pytest.raises(ValueError, match="is a URI"):
        storage.silver_format(path, "parquet")
    with pytest.raises(ValueError, match="is a URI"):
        storage.read_silver(None, path)


def test_a_windows_drive_letter_is_not_taken_for_a_uri():
    assert storage.silver_format(r"C:\lake\silver\main") == storage.DEFAULT_SILVER_FORMAT


def test_an_address_that_is_neither_a_reference_nor_a_path_is_refused():
    with pytest.raises(TypeError, match="TableRef or a path"):
        storage.silver_format(42)


# Writing and reading
# --------------------------------------------------------------------------


def frame(spark, values):
    return spark.createDataFrame([(v,) for v in values], "VALUE int")


def values(df):
    return sorted(row.VALUE for row in df.collect())


def test_a_parquet_table_at_a_path_is_replaced_by_a_second_write(spark, tmp_path):
    ref = TableRef(path=str(tmp_path / "table"))

    storage.write_silver(frame(spark, [1, 2]), ref)
    storage.write_silver(frame(spark, [3]), ref)

    assert not os.path.isdir(os.path.join(ref.path, "_delta_log"))
    assert values(storage.read_silver(spark, ref)) == [3]


def test_a_parquet_write_replaces_the_whole_table_under_dynamic_overwrite(spark, tmp_path):
    """A session configured for dynamic partition overwrite would otherwise
    keep the partitions the new frame does not carry."""
    ref = TableRef(path=str(tmp_path / "table"))
    key = "spark.sql.sources.partitionOverwriteMode"
    previous = spark.conf.get(key)
    spark.conf.set(key, "dynamic")
    # The two writes fall into different partitions, so a dynamic overwrite
    # would leave the first one behind; partitioning by every column of the
    # frame is refused by Spark, hence the column of its own.
    first = frame(spark, [1, 2]).withColumn("PART", F.lit("a"))
    second = frame(spark, [3]).withColumn("PART", F.lit("b"))
    try:
        storage.write_silver(first, ref, partition_by=["PART"])
        storage.write_silver(second, ref, partition_by=["PART"])
        assert spark.conf.get(key) == "dynamic"
    finally:
        spark.conf.set(key, previous)

    assert values(storage.read_silver(spark, ref)) == [3]


def test_a_delta_table_at_a_path_is_replaced_in_one_transaction(spark, tmp_path):
    from delta.tables import DeltaTable

    ref = TableRef(path=str(tmp_path / "table"))

    storage.write_silver(frame(spark, [1, 2]), ref, file_format="delta")
    # The second write is not asked for Delta: the table keeps its format.
    storage.write_silver(frame(spark, [3]), ref)

    # Read as Delta, not as the parquet files it is made of, which would
    # return the superseded version as well.
    assert values(storage.read_silver(spark, ref)) == [3]
    table = DeltaTable.forPath(spark, ref.path)
    operations = [row.operation for row in table.history().collect()]
    assert operations.count("WRITE") == 2
    properties = table.detail().first()["properties"]
    assert properties[storage.OPTIMIZE_WRITE] == "true"


def test_a_delta_write_takes_a_new_schema(spark, tmp_path):
    ref = TableRef(path=str(tmp_path / "table"))

    storage.write_silver(frame(spark, [1]), ref, file_format="delta")
    storage.write_silver(spark.createDataFrame([("a",)], "OTHER string"), ref)

    assert storage.read_silver(spark, ref).columns == ["OTHER"]


@pytest.fixture
def schema(spark, tmp_path):
    """A schema in the session catalogue whose tables live below ``tmp_path``.

    It stands in for a Unity Catalog schema: the tables in it are addressed by
    name, and the catalogue decides where their files go.
    """
    name = f"storage_{uuid.uuid4().hex[:12]}"
    location = (tmp_path / "warehouse").as_posix()
    spark.sql(f"CREATE SCHEMA {name} LOCATION '{location}'")
    yield name
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def test_a_table_in_the_catalogue_is_written_and_replaced_as_delta(spark, schema):
    ref = TableRef(name=f"{schema}.table")

    storage.write_silver(frame(spark, [1, 2]), ref)
    storage.write_silver(frame(spark, [3]), ref)

    assert spark.catalog.tableExists(ref.name)
    assert values(storage.read_silver(spark, ref)) == [3]
    detail = spark.sql(f"DESCRIBE DETAIL {ref.name}").first()
    assert detail["format"] == "delta"
    assert detail["properties"][storage.OPTIMIZE_WRITE] == "true"


def test_parquet_is_refused_before_a_table_in_the_catalogue_is_touched(spark, schema):
    ref = TableRef(name=f"{schema}.table")

    with pytest.raises(ValueError, match="cannot be written as 'parquet'"):
        storage.write_silver(frame(spark, [1]), ref, file_format="parquet")

    assert not spark.catalog.tableExists(ref.name)


def test_a_table_missing_from_the_catalogue_is_reported_as_such(spark, schema):
    with pytest.raises(LookupError, match="Silver layer has to be updated"):
        storage.read_silver(spark, TableRef(name=f"{schema}.table"))


def test_a_format_that_conflicts_with_the_table_on_disk_is_refused_before_writing(spark, tmp_path):
    ref = TableRef(path=str(tmp_path / "table"))
    storage.write_silver(frame(spark, [1]), ref, file_format="delta")

    with pytest.raises(ValueError, match="stored as 'delta'"):
        storage.write_silver(frame(spark, [2]), ref, file_format="parquet")

    assert values(storage.read_silver(spark, ref)) == [1]


def test_a_path_given_as_text_is_written_and_returned_as_text(spark, tmp_path):
    """The modules took their Silver tables as paths before they could be
    addressed by name, and their helpers still accept one."""
    path = str(tmp_path / "table")

    assert storage.write_silver(frame(spark, [1]), path) == path
    assert values(storage.read_silver(spark, path)) == [1]
