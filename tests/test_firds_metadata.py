"""The watermark of the historical FIRDS database.

The watermark decides which delta files
:func:`fincommons.firds.silver.update_from_bronze_to_silver_main` still has to
apply. Applying one twice would attribute the wrong validity periods to the
versions, so the tests below pin down that the bookkeeping survives being read
back, updated in place and extended with a second table.

It is stored in a Delta table addressed by path, which is the one form that
behaves the same on a workstation and on Databricks.
"""

import pytest

from fincommons.firds import common, metadata


@pytest.fixture
def config(tmp_path):
    """A configuration file pointing at an empty metadata directory."""
    import yaml

    base = tmp_path / "lake"
    for name in ("metadata", "landing", "bronze", "silver"):
        (base / name).mkdir(parents=True)

    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"firds": {name: str(base / name)
                       for name in ("metadata", "landing", "bronze", "silver")}}
        )
    )
    return str(config_path)


def test_absent_table_is_reported_as_absent(spark, config):
    assert metadata.metadata_exists(spark, config) is False


def test_absent_table_has_no_watermark(spark, config):
    """A missing table reads as no watermark rather than as an error.

    This is what tells a database that has never been initialised apart from
    one that has, so the caller can initialise instead of failing.
    """
    assert metadata.get_metadata_maxdate(spark, config, "MAIN") is None


def test_initialise_records_the_date(spark, config):
    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")

    assert metadata.metadata_exists(spark, config) is True
    assert metadata.get_metadata_maxdate(spark, config, "MAIN") == "2024-01-05"


def test_update_moves_the_watermark_without_adding_a_row(spark, config):
    """The per-delta-file case: the same row is moved forward, not appended."""
    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")
    metadata.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2024-01-08")

    assert metadata.get_metadata_maxdate(spark, config, "MAIN") == "2024-01-08"
    assert metadata.read_metadata(spark, config).count() == 1


def test_a_second_table_gets_its_own_row(spark, config):
    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")
    metadata.update_metadata_maxdate(
        spark, config, table="COMPACT", maxdate="2024-01-08"
    )

    assert metadata.get_metadata_maxdate(spark, config, "COMPACT") == "2024-01-08"
    assert metadata.get_metadata_maxdate(spark, config, "MAIN") == "2024-01-05"
    assert metadata.read_metadata(spark, config).count() == 2


def test_unknown_table_has_no_watermark(spark, config):
    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")

    assert metadata.get_metadata_maxdate(spark, config, "NOSUCHTABLE") is None


def test_update_creates_the_table_when_it_is_missing(spark, config):
    """Setting a watermark by hand works on a database that has none.

    This is the path a database takes whose history was brought in from
    another store: the history exists, the watermark table does not, and the
    date is recorded once by hand.
    """
    metadata.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2024-02-01")

    assert metadata.get_metadata_maxdate(spark, config, "MAIN") == "2024-02-01"


def test_initialise_resets_an_existing_table(spark, config):
    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")
    metadata.update_metadata_maxdate(
        spark, config, table="COMPACT", maxdate="2024-01-08"
    )

    metadata.initialise_metadata(spark, config, maxdate="2023-12-31")

    assert metadata.read_metadata(spark, config).count() == 1
    assert metadata.get_metadata_maxdate(spark, config, "MAIN") == "2023-12-31"


def test_update_is_timestamped(spark, config):
    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")

    assert metadata.read_metadata(spark, config).collect()[0]["UPDATED"] is not None


def test_the_store_is_a_delta_table(spark, config):
    """The store is Delta, not a file the driver writes with its own filesystem.

    This is what lets the same code run against a local directory and against a
    volume or object storage on Databricks.
    """
    import os

    metadata.initialise_metadata(spark, config, maxdate="2024-01-05")

    assert os.path.isdir(os.path.join(common.path_metadata(config), "_delta_log"))
