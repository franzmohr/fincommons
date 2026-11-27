"""Tests of the Silver layer of the FIRDS pipeline.

The delta files have to be applied in the order in which they were published,
because each of them states what changed since the previous one. The order is
taken from the names of the Bronze partitions, which is what
:func:`fincommons.firds.silver.list_reference_dates` reads.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import functools
import logging
import os
import shutil

import pytest
import yaml

from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql import types

from fincommons.firds import common, history, landing, silver
from fincommons.firds.silver import format_reference_date, list_reference_dates
from fincommons.schemas import load_pyspark_schema


def partitions(root, *names):
    """Create the given entries below ``root`` and return the path to it."""
    for name in names:
        (root / name).mkdir()
    return str(root)


def test_dates_are_returned_in_chronological_order(tmp_path):
    path = partitions(
        tmp_path,
        "DATADT=2025-01-10",
        "DATADT=2024-12-31",
        "DATADT=2025-02-01",
        "DATADT=2025-01-02",
    )

    assert list_reference_dates(path) == [
        date(2024, 12, 31),
        date(2025, 1, 2),
        date(2025, 1, 10),
        date(2025, 2, 1),
    ]


def test_dates_are_compared_as_dates_and_not_as_text(tmp_path):
    # The Bronze layer pads the components of the date, so that the names of
    # the partitions happen to sort as text in the same order as the dates
    # they spell. The order the files are applied in must not rest on that.
    path = partitions(tmp_path, "DATADT=2025-01-10", "DATADT=2025-1-9")

    assert list_reference_dates(path) == [date(2025, 1, 9), date(2025, 1, 10)]


def test_entries_that_are_not_partitions_are_ignored(tmp_path):
    path = partitions(
        tmp_path, "DATADT=2025-01-02", "_SUCCESS", "_temporary", "OTHER=2025-01-03"
    )

    assert list_reference_dates(path) == [date(2025, 1, 2)]


def test_a_path_without_partitions_has_no_dates(tmp_path):
    # This is the state the historical database is initialised from: no FULINS
    # file has been loaded into the Bronze layer yet.
    assert list_reference_dates(partitions(tmp_path)) == []


def test_the_dates_address_the_partitions_they_were_read_from(tmp_path):
    # The caller turns the dates back into the strings that select a partition
    # of the Bronze table, so the round trip has to be faithful.
    names = ["DATADT=2024-12-31", "DATADT=2025-01-02"]
    path = partitions(tmp_path, *names)

    spelled = [
        "DATADT=" + format_reference_date(d) for d in list_reference_dates(path)
    ]

    assert sorted(spelled) == sorted(names)


# Deciding whether the database has to be initialised
# --------------------------------------------------------------------------


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
    return str(config_path)


def test_a_failure_to_read_the_table_is_not_taken_for_an_empty_database(
    monkeypatch, spark, config
):
    """Inferring that the database is empty from a read that failed would treat
    a permission problem or an unreadable transaction log as a reason to build
    the history again from the full file, which overwrites what is there."""
    def unreachable(session, path):
        raise PermissionError("the storage is unreachable")

    monkeypatch.setattr("delta.tables.DeltaTable.isDeltaTable", staticmethod(unreachable))

    initialised = []
    monkeypatch.setattr(
        silver, "initialise_firds_database",
        lambda *args, **kwargs: initialised.append(True),
    )

    with pytest.raises(PermissionError):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert initialised == []


# The compact table
# --------------------------------------------------------------------------


def history_row(isin, tradgvn, cfi="ESVUFR", issuer="ISSUER1", actntp="ModfdRcrd",
                full_name="Full name", notional_currency="EUR",
                option_type="CALL", option_exercise_style="EURO",
                delivery_type="PHYS"):
    return (isin, tradgvn, cfi, issuer, "name", "2030-01-01", "2030-01-01", "1.0",
            full_name, notional_currency, option_type, option_exercise_style,
            delivery_type, True, actntp)


HISTORY_SCHEMA = (
    "isin string, trading_venue string, cfi string, issuer string, short_name string, "
    "maturity_date string, expiry_date string, price_multiplier string, "
    "full_name string, notional_currency string, option_type string, "
    "option_exercise_style string, delivery_type string, "
    "latest_record boolean, action_type string"
)


def build_main(spark, config, rows):
    """Write a minimal historical database for the compact step to read."""
    common.create_directory_tree(config)
    spark.createDataFrame(rows, HISTORY_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).save(common.path_silver_main(config))


def test_the_compact_table_holds_one_row_per_isin(spark, config):
    build_main(spark, config, [
        history_row("ISIN1", "MIC1"),
        history_row("ISIN1", "MIC2"),
        history_row("ISIN2", "MIC1"),
    ])

    silver.update_from_silver_main_to_silver_compact(spark, config)

    compact = common.connect_to_firds(spark, config, "compact")
    assert compact.count() == 2
    trading_venues = {r["isin"]: r["trading_venue"] for r in compact.collect()}
    assert sorted(trading_venues["ISIN1"].split("+")) == ["MIC1", "MIC2"]


TYPED_HISTORY_SCHEMA = (
    "isin string, trading_venue string, cfi string, issuer string, short_name string, "
    "maturity_date date, expiry_date date, price_multiplier decimal(35,17), "
    "full_name string, notional_currency string, option_type string, "
    "option_exercise_style string, delivery_type string, "
    "latest_record boolean, action_type string"
)


def test_venues_that_disagree_leave_the_typed_columns_empty(spark, config):
    """The values of venues that disagree must not be joined with '+' and the
    result parsed: Spark 4 raises on a string it cannot parse, which would
    fail the whole rebuild."""
    common.create_directory_tree(config)
    spark.createDataFrame([
        ("ISIN1", "MIC1", "D", "L", "N", date(2027, 1, 1), date(2027, 1, 1), Decimal("1"),
         "Name one", "EUR", "CALL", "EURO", "PHYS", True, "ModfdRcrd"),
        ("ISIN1", "MIC2", "D", "L", "N", date(2028, 1, 1), date(2027, 1, 1), Decimal("10"),
         "Name two", "USD", "PUTO", "AMER", "CASH", True, "ModfdRcrd"),
        ("ISIN2", "MIC1", "D", "L", "N", date(2029, 1, 1), None, Decimal("5"),
         "Agreed", "EUR", "CALL", "EURO", "PHYS", True, "ModfdRcrd"),
        ("ISIN2", "MIC2", "D", "L", "N", date(2029, 1, 1), None, Decimal("5"),
         "Agreed", "EUR", "CALL", "EURO", "PHYS", True, "ModfdRcrd"),
    ], TYPED_HISTORY_SCHEMA).write.format("delta").save(common.path_silver_main(config))

    silver.update_from_silver_main_to_silver_compact(spark, config)

    rows = {r["isin"]: r for r in common.connect_to_firds(spark, config, "compact").collect()}
    assert rows["ISIN1"]["maturity_date"] is None
    assert rows["ISIN1"]["expiry_date"] == date(2027, 1, 1)
    assert rows["ISIN1"]["price_multiplier"] is None
    assert rows["ISIN2"]["maturity_date"] == date(2029, 1, 1)
    assert rows["ISIN2"]["expiry_date"] is None
    assert rows["ISIN2"]["price_multiplier"] == Decimal("5")
    assert dict(common.connect_to_firds(spark, config, "compact").dtypes)[
        "price_multiplier"] == "decimal(35,17)"


def test_the_instrument_fields_a_report_states_are_empty_where_venues_disagree(
        spark, config):
    """The reference data of RTS 22 fields 42 to 56 says what the instrument
    is, not what a venue thinks it is, so two venues that disagree leave it
    empty rather than joined. A name joined with a '+' is no name, and
    CALL+PUTO is not an option type a transaction report could state."""
    common.create_directory_tree(config)
    spark.createDataFrame([
        ("ISIN1", "MIC1", "D", "L", "N", date(2027, 1, 1), date(2027, 1, 1), Decimal("1"),
         "Name one", "EUR", "CALL", "EURO", "PHYS", True, "ModfdRcrd"),
        ("ISIN1", "MIC2", "D", "L", "N", date(2027, 1, 1), date(2027, 1, 1), Decimal("1"),
         "Name two", "USD", "PUTO", "AMER", "CASH", True, "ModfdRcrd"),
        ("ISIN2", "MIC1", "D", "L", "N", date(2029, 1, 1), None, Decimal("5"),
         "Agreed", "EUR", "CALL", "EURO", "PHYS", True, "ModfdRcrd"),
        ("ISIN2", "MIC2", "D", "L", "N", date(2029, 1, 1), None, Decimal("5"),
         "Agreed", "EUR", "CALL", "EURO", "PHYS", True, "ModfdRcrd"),
    ], TYPED_HISTORY_SCHEMA).write.format("delta").save(common.path_silver_main(config))

    silver.update_from_silver_main_to_silver_compact(spark, config)

    rows = {r["isin"]: r for r in
            common.connect_to_firds(spark, config, "compact").collect()}
    for column in ("full_name", "notional_currency", "option_type",
                   "option_exercise_style", "delivery_type"):
        assert rows["ISIN1"][column] is None, column
    assert rows["ISIN2"]["full_name"] == "Agreed"
    assert rows["ISIN2"]["notional_currency"] == "EUR"
    assert rows["ISIN2"]["option_type"] == "CALL"
    assert rows["ISIN2"]["option_exercise_style"] == "EURO"
    assert rows["ISIN2"]["delivery_type"] == "PHYS"


def test_a_cancelled_record_is_left_out_of_the_compact_table(spark, config):
    """The history carries a cancellation as the version in force of its
    instrument and venue, as the Guide prescribes, but what it says is that the
    instrument was never admitted to that venue. Counted, it would list the
    venue among the ones the instrument trades on."""
    build_main(spark, config, [
        history_row("ISIN1", "MIC_LIVE"),
        history_row("ISIN1", "MIC_CXL", cfi=None, issuer=None, actntp="CancRcrd"),
        history_row("ISIN2", "MIC_CXL", cfi=None, issuer=None, actntp="CancRcrd"),
    ])

    silver.update_from_silver_main_to_silver_compact(spark, config)

    rows = {r["isin"]: r for r in common.connect_to_firds(spark, config, "compact").collect()}
    assert rows["ISIN1"]["trading_venue"] == "MIC_LIVE"
    # Every venue of ISIN2 was cancelled, so it trades nowhere and leaves the
    # table rather than standing in it with no venue.
    assert "ISIN2" not in rows


def test_the_venues_of_an_isin_are_joined_in_order(spark, config):
    """collect_set defines no order. The compact table is rebuilt in full on
    every run, so without sorting a venue added to an ISIN can land anywhere in
    the string and two lakes holding the same rows need not spell it alike."""
    venues = [f"MIC{i:02d}" for i in range(1, 9)]
    build_main(spark, config, [history_row("ISIN1", mic) for mic in venues])

    silver.update_from_silver_main_to_silver_compact(spark, config)

    row = common.connect_to_firds(spark, config, "compact").first()
    assert row["trading_venue"] == "+".join(venues)


def test_a_field_no_venue_states_is_empty_rather_than_blank(spark, config):
    """concat_ws answers the empty string for an empty array. The typed columns
    are empty in that situation, and a reader should not have to tell NULL and
    '' apart to ask the same question of two columns of one table."""
    build_main(spark, config, [
        history_row("ISIN1", "MIC1", cfi=None, issuer=None),
        history_row("ISIN1", "MIC2", cfi=None, issuer=None),
    ])

    silver.update_from_silver_main_to_silver_compact(spark, config)

    row = common.connect_to_firds(spark, config, "compact").first()
    assert row["cfi"] is None
    assert row["issuer"] is None
    assert row["trading_venue"] == "MIC1+MIC2"


def test_a_value_one_venue_states_alone_is_kept(spark, config):
    """A venue that states nothing is not a venue that disagrees."""
    build_main(spark, config, [
        history_row("ISIN1", "MIC1", cfi="ESVUFR"),
        history_row("ISIN1", "MIC2", cfi=None),
    ])

    silver.update_from_silver_main_to_silver_compact(spark, config)

    assert common.connect_to_firds(spark, config, "compact").first()["cfi"] == "ESVUFR"


def test_the_compact_table_keeps_the_format_it_is_stored_in(spark, config):
    """Overwriting a Delta table as parquet deletes its transaction log along
    with the superseded files, which is not something to do as a side effect of
    naming a format; see fincommons.storage.silver_format."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])
    silver.update_from_silver_main_to_silver_compact(spark, config, file_format="delta")

    with pytest.raises(ValueError, match="keeps its format"):
        silver.update_from_silver_main_to_silver_compact(
            spark, config, file_format="parquet"
        )


def test_the_compact_table_is_parquet_unless_delta_is_asked_for(spark, config):
    """Parquet is the default, because it is what every engine reads without a
    further dependency."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])

    silver.update_from_silver_main_to_silver_compact(spark, config)

    path = common.path_silver_compact(config)
    assert not os.path.isdir(os.path.join(path, "_delta_log"))
    assert any(name.endswith(".parquet") for name in os.listdir(path))


def test_the_compact_table_is_a_delta_table_where_that_is_asked_for(spark, config):
    """A rebuild then replaces the table in one transaction. Overwriting
    parquet deletes it first, so a reader can find it missing."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])

    silver.update_from_silver_main_to_silver_compact(spark, config, file_format="delta")

    assert os.path.isdir(common.path_silver_compact(config) + "/_delta_log")


def test_the_compact_table_is_read_back_in_the_format_it_was_written_in(spark, config):
    """The reader takes the format from the table rather than from the caller.
    Reading a Delta table as plain parquet would not fail; it would return the
    superseded files of earlier versions along with the current ones."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])
    silver.update_from_silver_main_to_silver_compact(spark, config, file_format="delta")

    # A second run leaves the first version of the table behind on disk.
    build_main(spark, config, [history_row("ISIN2", "MIC1")])
    silver.update_from_silver_main_to_silver_compact(spark, config, file_format="delta")

    compact = common.connect_to_firds(spark, config, "compact")
    assert [r["isin"] for r in compact.collect()] == ["ISIN2"]


def test_an_unknown_file_format_is_reported_as_such(config):
    # The format is checked before the session is put to work, which is why
    # there is no session here to put to work.
    with pytest.raises(ValueError, match="csv"):
        silver.update_from_silver_main_to_silver_compact(None, config, file_format="csv")


def test_rebuilding_the_compact_table_replaces_it(spark, config):
    build_main(spark, config, [history_row("ISIN1", "MIC1"), history_row("ISIN2", "MIC1")])
    silver.update_from_silver_main_to_silver_compact(spark, config)

    build_main(spark, config, [history_row("ISIN3", "MIC1")])
    silver.update_from_silver_main_to_silver_compact(spark, config)

    compact = common.connect_to_firds(spark, config, "compact")
    assert [r["isin"] for r in compact.collect()] == ["ISIN3"]


# What the run leaves behind in the log
# --------------------------------------------------------------------------


def test_a_run_with_nothing_to_apply_says_so(spark, config, caplog):
    """An update that finds no new delta file returns without touching the
    database. Nothing about the data lake afterwards distinguishes that from a
    run that applied every file it found, so the log has to."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])
    common.initialise_metadata(spark, config, maxdate="2024-01-02")

    with caplog.at_level(logging.INFO, logger="fincommons"):
        silver.update_from_bronze_to_silver_main(spark, config)

    skipped = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "Nothing to update" in r.msg
    ]
    assert len(skipped) == 1
    assert skipped[0].ctx["datadt"] == "2024-01-02"

    finished = [r for r in caplog.records if r.msg == "%s finished"]
    assert finished[-1].ctx["periods"] == 0


def test_a_failing_update_is_recorded_and_still_raises(monkeypatch, spark, config, caplog):
    """A run that cannot tell which files it has already applied has to stop.
    The log records the failure, it does not stand in for it."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])
    monkeypatch.setattr(
        common, "get_metadata_maxdate", lambda *args, **kwargs: None
    )

    with caplog.at_level(logging.INFO, logger="fincommons"):
        with pytest.raises(RuntimeError, match="no watermark"):
            silver.update_from_bronze_to_silver_main(spark, config)

    failures = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(failures) == 1
    assert failures[0].exc_info is not None


# Applying a delta file to the historical database
# --------------------------------------------------------------------------
#
# The Silver layer applies a delta file with a MERGE against the Delta table,
# while fincommons.firds.history.apply_changes expresses the same algorithm by
# rewriting the history. The second is the reference implementation: it is the
# one the tests of the algorithm itself check against the worked example of
# ESMA's Guide. The cases below therefore apply the same delta files both ways
# and require the two to agree, which is what keeps the storage layer honest
# when the way it writes the changes is altered.


#: The Bronze layer of the delta files, i.e. the schema the XML is read with
#: and the partition column the reference date is kept in.
BRONZE_SCHEMA = types.StructType(
    list(load_pyspark_schema("firds_schema_036_001_03").fields)
    + [types.StructField("DATADT", types.StringType())]
)

#: The columns of the historical database in these tests. The real table
#: carries the whole analytics schema; the algorithm touches the key, the
#: publication date it derives the validity dates from, and the flags.
FULL_SCHEMA = (
    "isin string, trading_venue string, full_name string, tech_publication_period_from_date date"
)


def bronze_record(isin, mic, name, frdt, action):
    """One record of a delta file, reported in the element ``action``.

    The other three elements stay empty, which is how a delta file states that
    the record is of one kind and not of the others.

    A ``<CancRcrd>`` is the reduced record the schema types it as: it names
    the instrument and the trading venue and carries neither a name nor a
    publication period, so the name and ``frdt`` given here do not reach it.
    Its version begins on the reference date of the file instead, which the
    cases below state as the same day.
    """
    if action == silver.CANCELLATION:
        return {
            action: {
                "FinInstrmGnlAttrbts": {"Id": isin},
                "TradgVnRltdAttrbts": {"Id": mic},
            }
        }
    return {
        action: {
            "FinInstrmGnlAttrbts": {"Id": isin, "FullNm": name},
            "TradgVnRltdAttrbts": {"Id": mic},
            "TechAttrbts": {"PblctnPrd": {"FrDt": frdt}},
        }
    }


def delta_file(spark, datadt, records):
    """The Bronze partition of one delta file."""
    rows = [dict(bronze_record(*r), DATADT=format_reference_date(datadt))
            for r in records]
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


def full_file(spark, records):
    """The records of the full file the history starts from."""
    return spark.createDataFrame(records, FULL_SCHEMA)


def through_storage(spark, config, full, files):
    """Build the history the way the Silver layer does, with a MERGE."""
    common.create_directory_tree(config)
    path = common.path_silver_main(config)

    history.initialise_history(full).write.format("delta").mode("overwrite").save(path)
    silver.declare_silver_main_layout(spark, config)
    common.initialise_metadata(spark, config, maxdate="2000-01-01")

    for datadt, records in files:
        bronze = delta_file(spark, datadt, records)
        silver.upsert_from_bronze_daily(
            spark=spark,
            bronze=bronze,
            config_path=config,
            datadt=format_reference_date(datadt),
        )

    return spark.read.format("delta").load(path)


def through_reference(spark, full, files):
    """Build the history the way history.apply_changes does, by rewriting it.

    The result of every file is materialised before the next one is applied.
    Each step unions the four record types of the file onto the unions of the
    step before it, so the query plan of a run over several files grows deep
    enough to exhaust the driver while it is still being planned. The storage
    layer does not have this problem, because it reads the table back from disk
    for every file and starts afresh from it.
    """
    built = history.initialise_history(full)
    for datadt, records in files:
        incoming = silver.read_daily_changes(delta_file(spark, datadt, records))
        built = history.apply_changes(built, incoming)
        built = spark.createDataFrame(built.collect(), built.schema)
    return built


def versions(built):
    """The history as comparable rows, in a fixed order."""
    columns = ["isin", "trading_venue", "full_name", "valid_from", "valid_to",
               "latest_record", "action_type"]
    return sorted(
        tuple(str(row[name]) for name in columns)
        for row in built.select(*columns).collect()
    )


#: The cases the two implementations are compared on: the worked example of the
#: Guide, each of the record types the algorithm distinguishes, and the
#: combinations in which one delta file carries several of them.
MERGE_CASES = {
    "the worked example": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "Company X", date(2016, 9, 4), "ModfdRcrd")]),
         (date(2016, 11, 8), [("ISIN1", "MIC1", "X Corporation", date(2016, 11, 8), "ModfdRcrd")]),
         (date(2016, 12, 2), [("ISIN1", "MIC1", "X Corporation", date(2016, 12, 2), "ModfdRcrd")])],
    ),
    "an ordinary termination replaces": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 2), [("ISIN1", "MIC1", "Company X", date(2016, 9, 2), "TermntdRcrd")])],
    ),
    "a termination reported late is inserted": (
        [("ISIN9", "MIC9", "Other", date(2016, 9, 2))],
        [(date(2016, 9, 5), [("ISIN1", "MIC1", "Company X", date(2016, 9, 5), "TermntdRcrd")])],
    ),
    "a correction after termination closes and inserts": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 6), [("ISIN1", "MIC1", "Company X", date(2016, 9, 6), "TermntdRcrd")])],
    ),
    "a cancellation closes the version in force": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "Company X", date(2016, 9, 4), "CancRcrd")])],
    ),
    "a new record is inserted": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN2", "MIC1", "Company Y", date(2016, 9, 4), "NewRcrd")])],
    ),
    "several changes for one instrument in one file": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 8), [("ISIN1", "MIC1", "A", date(2016, 9, 4), "ModfdRcrd"),
                             ("ISIN1", "MIC1", "B", date(2016, 9, 6), "ModfdRcrd"),
                             ("ISIN1", "MIC1", "B", date(2016, 9, 8), "TermntdRcrd")])],
    ),
    "a close and a replace in one file": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2)),
         ("ISIN2", "MIC1", "Y", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "X", date(2016, 9, 4), "ModfdRcrd"),
                             ("ISIN2", "MIC1", "Y", date(2016, 9, 2), "TermntdRcrd")])],
    ),
    "a new record changed on the day it begins": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN2", "MIC1", "new", date(2016, 9, 4), "NewRcrd"),
                             ("ISIN2", "MIC1", "changed", date(2016, 9, 4), "ModfdRcrd")])],
    ),
    "a change beginning with the version in force": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 3), [("ISIN1", "MIC1", "Company Y", date(2016, 9, 2), "ModfdRcrd")])],
    ),
    # The append must not plan this file again from the table the merge has
    # already changed, or it inserts the termination a second time.
    "a termination replaced and changed again in one file": (
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))],
        [(date(2016, 9, 6), [("ISIN1", "MIC1", "terminated", date(2016, 9, 2), "TermntdRcrd"),
                             ("ISIN1", "MIC1", "corrected", date(2016, 9, 6), "ModfdRcrd")])],
    ),
    "a mixed batch over two files": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2)),
         ("ISIN2", "MIC2", "Y", date(2016, 9, 2)),
         ("ISIN3", "MIC1", "Z", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "X2", date(2016, 9, 4), "ModfdRcrd"),
                             ("ISIN2", "MIC2", "Y", date(2016, 9, 2), "TermntdRcrd"),
                             ("ISIN4", "MIC1", "W", date(2016, 9, 4), "NewRcrd")]),
         (date(2016, 9, 6), [("ISIN3", "MIC1", "Z2", date(2016, 9, 6), "CancRcrd"),
                             ("ISIN1", "MIC1", "X3", date(2016, 9, 6), "ModfdRcrd")])],
    ),
}


def test_a_cancellation_of_a_delta_file_reaches_the_history(spark):
    """A ``<CancRcrd>`` has to survive the flattening of a delta file.

    The schema types a cancellation as a reduced record: it names the
    instrument and the trading venue whose combination is withdrawn and
    carries no ``TechAttrbts``. Recognising a record by that element, which is
    how the other three record types are recognised, would drop every
    cancellation before it reached the historical database.

    This goes through :func:`read_daily_changes` from a Bronze partition
    rather than from a frame in the analytics schema, because that is the step
    the cancellations would be lost in.
    """
    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    bronze = delta_file(
        spark,
        date(2016, 9, 4),
        [("ISIN1", "MIC1", "Company X", date(2016, 9, 4), "CancRcrd")],
    )

    incoming = silver.read_daily_changes(bronze)

    assert incoming.count() == 1, "the cancellation did not reach the history"
    cancellation = incoming.first()
    assert cancellation["action_type"] == "CancRcrd"
    assert (cancellation["isin"], cancellation["trading_venue"]) == ("ISIN1", "MIC1")
    # The reduced record carries no publication period, so the version begins
    # on the reference date of the file that cancels the combination.
    assert cancellation["tech_publication_period_from_date"] == date(2016, 9, 4)
    # It states nothing about the instrument itself.
    assert cancellation["full_name"] is None

    built = history.apply_changes(history.initialise_history(full), incoming)
    rows = sorted(built.collect(), key=lambda row: row["valid_from"])

    assert [row["valid_to"] for row in rows] == [date(2016, 9, 3), None]
    assert [row["latest_record"] for row in rows] == [False, True]
    assert rows[1]["action_type"] == "CancRcrd"


@pytest.mark.parametrize(
    "case", list(MERGE_CASES), ids=[name.replace(" ", "-") for name in MERGE_CASES]
)
def test_the_merge_agrees_with_the_reference_implementation(spark, config, case):
    """The MERGE the Silver layer applies has to leave the same history behind
    as rewriting it does. The merge reaches only the rows a file affects, which
    is what makes it worth using, and that is also what makes it the place
    where a version can be left open, closed twice or lost."""
    records, files = MERGE_CASES[case]
    full = full_file(spark, records)

    assert versions(through_storage(spark, config, full, files)) == versions(
        through_reference(spark, full, files)
    )


def test_the_watermark_follows_the_file_that_was_applied(spark, config):
    """The watermark is what keeps a delta file from being applied twice, so
    the upsert has to record the reference date it has just processed."""
    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    files = [(date(2016, 9, 4),
              [("ISIN1", "MIC1", "Company X", date(2016, 9, 4), "ModfdRcrd")])]

    through_storage(spark, config, full, files)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-04"


def start_history(spark, config, full):
    """Write the initial state of the history, as the Silver layer does."""
    common.create_directory_tree(config)
    path = common.path_silver_main(config)
    history.initialise_history(full).write.format("delta").mode("overwrite").save(path)
    silver.declare_silver_main_layout(spark, config)
    common.initialise_metadata(spark, config, maxdate="2000-01-01")
    return path


def apply_day(spark, config, datadt, records):
    silver.upsert_from_bronze_daily(
        spark=spark,
        bronze=delta_file(spark, datadt, records),
        config_path=config,
        datadt=format_reference_date(datadt),
    )


def fail_once(monkeypatch, module, name):
    """Make ``module.name`` raise the first time it is called."""
    original = getattr(module, name)
    calls = []

    def failing(*args, **kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError("the run died here")
        return original(*args, **kwargs)

    monkeypatch.setattr(module, name, failing)


#: A delta file whose first record replaces the version in force and whose
#: second closes the record that replaced it. Planned again from the table the
#: merge has already changed, the first record would find no version in force
#: and be inserted a second time.
RESUMED_FULL = [("ISIN1", "MIC1", "Company X", date(2016, 9, 2)),
                ("ISIN2", "MIC1", "Company Y", date(2016, 9, 2))]
RESUMED_FILE = (date(2016, 9, 6), [
    ("ISIN1", "MIC1", "terminated", date(2016, 9, 2), "TermntdRcrd"),
    ("ISIN1", "MIC1", "corrected", date(2016, 9, 6), "ModfdRcrd"),
    ("ISIN2", "MIC1", "Company Y2", date(2016, 9, 6), "ModfdRcrd"),
    ("ISIN3", "MIC1", "Company Z", date(2016, 9, 6), "NewRcrd"),
])


def test_reapplying_a_file_leaves_the_history_unchanged(spark, config):
    """The watermark is written after the records are, so a run that dies
    between the two leaves the file applied and the watermark behind it, and
    the next run applies the same file again. That second application must
    not change anything."""
    full = full_file(spark, RESUMED_FULL)
    path = start_history(spark, config, full)

    apply_day(spark, config, *RESUMED_FILE)
    once = versions(spark.read.format("delta").load(path))
    apply_day(spark, config, *RESUMED_FILE)
    built = spark.read.format("delta").load(path)

    assert versions(built) == once
    assert built.where(func.col("valid_to") < func.col("valid_from")).count() == 0
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-06"


def test_a_run_that_died_before_the_watermark_is_resumed(monkeypatch, spark, config):
    full = full_file(spark, RESUMED_FULL)
    path = start_history(spark, config, full)
    fail_once(monkeypatch, common, "update_metadata_maxdate")

    with pytest.raises(RuntimeError, match="died"):
        apply_day(spark, config, *RESUMED_FILE)
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2000-01-01"
    apply_day(spark, config, *RESUMED_FILE)

    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, [RESUMED_FILE])
    )
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-06"


def test_a_run_that_died_between_merge_and_append_is_resumed(monkeypatch, spark, config):
    """The merge has already replaced and closed the versions in force, so the
    plan has to be made from the table as it stood before it."""
    full = full_file(spark, RESUMED_FULL)
    path = start_history(spark, config, full)
    fail_once(monkeypatch, history, "records_to_insert")

    with pytest.raises(RuntimeError, match="died"):
        apply_day(spark, config, *RESUMED_FILE)
    apply_day(spark, config, *RESUMED_FILE)

    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, [RESUMED_FILE])
    )


def test_the_tags_a_date_is_resumed_by_are_at_the_end_of_the_log(spark, config):
    """What the window of committed_steps rests on. The update writes nothing
    to the table between one date and the next, so the tags of the date the
    table was written by last are its newest commits, and a window of the log
    reaches them without the whole of it having to be read."""
    full = full_file(spark, RESUMED_FULL)
    start_history(spark, config, full)
    apply_day(spark, config, *RESUMED_FILE)
    ref = common.ref_silver_main(config)

    windowed = silver.tagged_commits(spark, ref, limit=2)
    assert len(windowed) == 2
    assert windowed == silver.tagged_commits(spark, ref)[:2]
    assert [tag for _, tag in windowed] == [
        silver.commit_tag("2016-09-06", "append"),
        silver.commit_tag("2016-09-06", "merge"),
    ]
    assert silver.committed_steps(spark, ref, "2016-09-06").keys() == {
        "current", "merge", "append",
    }


def test_a_history_built_again_does_not_inherit_the_tags_of_the_old_one(spark, config):
    """Writing the history from scratch starts its commits afresh; a date
    applied to the old history is applied to the new one again."""
    full = full_file(spark, RESUMED_FULL)
    start_history(spark, config, full)
    apply_day(spark, config, *RESUMED_FILE)

    path = start_history(spark, config, full)
    apply_day(spark, config, *RESUMED_FILE)

    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, [RESUMED_FILE])
    )


# A history that holds the amounts as doubles
# --------------------------------------------------------------------------

#: Amounts as a history of doubles holds them, chosen so that each has an
#: exact decimal of its column's type.
LEGACY_AMOUNTS = {
    "total_issued_nominal_amount": 1500000.25,
    "nominal_value_per_unit_amount": 1000.0,
    "interest_rate_fixed": 0.0375,
    "price_multiplier": 100.5,
}


def legacy_history(spark, config, amounts=LEGACY_AMOUNTS, maxdate="2016-09-02"):
    """A history whose money and rate columns are ``double`` rather than
    ``decimal``, with the tag and the watermark the Silver layer gives it."""
    common.create_directory_tree(config)
    path = common.path_silver_main(config)
    frame = history.initialise_history(full_file(spark, RESUMED_FULL))
    for column, value in amounts.items():
        frame = frame.withColumn(column, func.lit(value).cast("double"))
    frame.write.format("delta").mode("overwrite").option(
        "userMetadata", silver.commit_tag(maxdate, "init")
    ).save(path)
    silver.declare_silver_main_layout(spark, config)
    common.initialise_metadata(spark, config, maxdate=maxdate)
    return path


def test_the_amounts_are_read_as_the_decimals_of_the_xml_schema():
    """The types the history carries are the decimals the XSD states, not
    doubles."""
    assert silver.DECIMAL_COLUMNS == {
        "total_issued_nominal_amount": "decimal(23,5)",
        "nominal_value_per_unit_amount": "decimal(23,5)",
        "interest_rate_fixed": "decimal(21,10)",
        "price_multiplier": "decimal(35,17)",
    }


def test_a_history_of_doubles_is_rewritten_as_decimals(spark, config):
    path = legacy_history(spark, config)

    assert silver.migrate_silver_main_to_decimal(spark, config) is True

    built = spark.read.format("delta").load(path)
    assert {c: t for c, t in built.dtypes if c in silver.DECIMAL_COLUMNS} == \
        silver.DECIMAL_COLUMNS
    assert built.count() == len(RESUMED_FULL)
    row = built.first()
    assert row["total_issued_nominal_amount"] == Decimal("1500000.25")
    assert row["nominal_value_per_unit_amount"] == Decimal("1000")
    assert row["interest_rate_fixed"] == Decimal("0.0375")
    assert row["price_multiplier"] == Decimal("100.5")


def test_the_rewrite_keeps_the_watermark_and_the_layout(spark, config):
    """An overwrite starts the commits of the table afresh and replaces what it
    declares about itself, so both have to be carried across it."""
    path = legacy_history(spark, config)

    silver.migrate_silver_main_to_decimal(spark, config)

    ref = common.ref_silver_main(config)
    assert silver.watermark_from_commits(spark, ref) == "2016-09-02"
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    assert list(detail["clusteringColumns"]) == list(history.KEY_COLUMNS)
    assert detail["properties"]["delta.enableDeletionVectors"] == "true"


def test_a_history_of_decimals_is_left_alone(spark, config):
    legacy_history(spark, config)
    silver.migrate_silver_main_to_decimal(spark, config)
    ref = common.ref_silver_main(config)
    version = silver.tagged_commits(spark, ref)[0][0]

    assert silver.migrate_silver_main_to_decimal(spark, config) is False
    assert silver.tagged_commits(spark, ref)[0][0] == version


def test_a_value_that_does_not_fit_its_decimal_stops_the_rewrite(spark, config):
    """The cast would make it a null, and an amount lost that way is found
    much later if at all."""
    path = legacy_history(spark, config, dict(LEGACY_AMOUNTS, total_issued_nominal_amount=1e30))

    with pytest.raises(RuntimeError, match=r"2 in total_issued_nominal_amount \(decimal\(23,5\)\)"):
        silver.migrate_silver_main_to_decimal(spark, config)

    assert dict(spark.read.format("delta").load(path).dtypes)[
        "total_issued_nominal_amount"] == "double"


def test_a_half_committed_update_stops_the_rewrite(monkeypatch, spark, config):
    """The rewrite would drop the tag the update is resumed by."""
    legacy_history(spark, config)
    monkeypatch.setattr(silver, "tagged_commits", lambda spark, ref: [
        (3, silver.commit_tag("2016-09-06", "merge")),
        (0, silver.commit_tag("2016-09-02", "init")),
    ])

    with pytest.raises(RuntimeError, match="2016-09-06 stands half-committed"):
        silver.migrate_silver_main_to_decimal(spark, config)


def test_the_daily_update_rewrites_a_history_of_doubles_first(spark, config):
    """Without the rewrite the day's decimals could not be appended to the
    doubles of the table."""
    path = legacy_history(spark, config)
    day = date(2016, 9, 6)
    write_bronze(spark, config, [(day, [("ISIN3", "MIC1", "Company Z", day, "NewRcrd")])])
    finished(config, "DLTINS_20160906_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    built = spark.read.format("delta").load(path)
    assert {c: t for c, t in built.dtypes if c in silver.DECIMAL_COLUMNS} == \
        silver.DECIMAL_COLUMNS
    assert sorted(r["isin"] for r in built.collect()) == ["ISIN1", "ISIN2", "ISIN3"]
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-06"


# Applying only what has arrived in full
# --------------------------------------------------------------------------


def finished(config, *names):
    for name in names:
        open(os.path.join(common.path_landing_finished(config), name), "w").close()


def write_bronze(spark, config, files):
    for datadt, records in files:
        delta_file(spark, datadt, records).write.partitionBy("DATADT").mode(
            "append"
        ).parquet(common.path_bronze_auth_036_001_03(config))


def test_the_update_stops_at_the_first_incomplete_day(spark, config):
    """Applying past a day with a missing part would move the watermark beyond
    it, and the records of that part could never be applied."""
    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    path = start_history(spark, config, full)
    files = [
        (date(2016, 9, 4), [("ISIN1", "MIC1", "A", date(2016, 9, 4), "ModfdRcrd")]),
        (date(2016, 9, 5), [("ISIN1", "MIC1", "B", date(2016, 9, 5), "ModfdRcrd")]),
        (date(2016, 9, 6), [("ISIN1", "MIC1", "C", date(2016, 9, 6), "ModfdRcrd")]),
    ]
    write_bronze(spark, config, files)
    finished(config, "DLTINS_20160904_01of01.zip", "DLTINS_20160905_02of02.zip",
             "DLTINS_20160906_01of01.zip")

    with pytest.raises(RuntimeError, match="DLTINS_20160905_01of02"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-04"
    built = spark.read.format("delta").load(path)
    assert sorted(r["full_name"] for r in built.collect()) == ["A", "Company X"]

    # Once the part has arrived, the update carries on from there.
    finished(config, "DLTINS_20160905_01of02.zip")
    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-06"
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files)
    )


def write_full_bronze(spark, config, datadt, isin="ISIN1", folder=None):
    spark.createDataFrame(
        [{"FinInstrmGnlAttrbts": {"Id": isin}, "TradgVnRltdAttrbts": {"Id": "MIC1"},
          "TechAttrbts": {"PblctnPrd": {"FrDt": datadt}}, "DATADT": format_reference_date(datadt)}],
        schema=types.StructType(
            list(load_pyspark_schema("firds_schema_017_001_02").fields)
            + [types.StructField("DATADT", types.StringType())]
        ),
    ).write.partitionBy("DATADT").mode("append").parquet(
        (folder or common.path_bronze_auth_017_001_02)(config)
    )


def test_the_history_is_not_started_from_an_incomplete_full_file(spark, config):
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_02of02.zip")

    with pytest.raises(RuntimeError, match="FULINS_E_20160902_01of02"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert not os.path.isdir(os.path.join(common.path_silver_main(config), "_delta_log"))


def test_the_history_is_started_from_a_complete_full_file(spark, config):
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.connect_to_firds(spark, config, "main").count() == 1
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"


def test_the_history_is_started_from_a_full_file_of_the_draft(spark, config):
    """The last full file of 2018 was published under the draft
    DRAFT13auth.017.001.01, and a history can start from it."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2018, 12, 29),
                      folder=common.path_bronze_draft13auth_017_001_01)
    finished(config, "FULINS_E_20181229_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.connect_to_firds(spark, config, "main").count() == 1
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2018-12-29"


def test_the_newest_full_file_is_taken_whichever_definition_it_has(spark, config):
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2018, 12, 29), isin="OLD",
                      folder=common.path_bronze_draft13auth_017_001_01)
    write_full_bronze(spark, config, date(2019, 9, 28), isin="NEW")
    finished(config, "FULINS_E_20181229_01of01.zip", "FULINS_E_20190928_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    rows = common.connect_to_firds(spark, config, "main").collect()
    assert [row["isin"] for row in rows] == ["NEW"]
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2019-09-28"


def test_the_history_is_not_started_without_a_letter_the_register_lists(spark, config):
    """Every part that arrived is complete, so only the register's list can
    tell that the letter C sent none at all."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")
    landing.write_landing_watermark(config, None, full_sample={
        "date": "2016-09-02",
        "files": ["FULINS_C_20160902_01of01.zip", "FULINS_E_20160902_01of01.zip"],
    })

    with pytest.raises(RuntimeError, match="FULINS_C_20160902_01of01"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert not os.path.isdir(os.path.join(common.path_silver_main(config), "_delta_log"))


def test_the_history_is_started_once_every_listed_part_is_imported(spark, config, caplog):
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_C_20160902_01of01.zip", "FULINS_E_20160902_01of01.zip")
    landing.write_landing_watermark(config, None, full_sample={
        "date": "2016-09-02",
        "files": ["FULINS_C_20160902_01of01.zip", "FULINS_E_20160902_01of01.zip"],
    })

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"
    assert not [r for r in caplog.records if "list of the full file" in r.getMessage()]


def test_a_full_file_without_the_register_s_list_is_started_with_a_warning(
    spark, config, caplog
):
    """A data lake filled by hand has no list to check against. The history is
    started from the parts that arrived, and the log says what could not be
    checked."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")

    with caplog.at_level(logging.WARNING, logger="fincommons"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"
    assert [r for r in caplog.records if "list of the full file" in r.getMessage()]


def test_a_missing_full_file_is_reported_as_such(spark, config):
    common.create_directory_tree(config)

    with pytest.raises(RuntimeError, match="no full file"):
        silver.update_from_bronze_to_silver_main(spark, config)


# How the daily merge writes
# --------------------------------------------------------------------------


def test_the_daily_merge_does_not_copy_the_versions_it_leaves_alone(spark, config):
    """Without deletion vectors a merge rewrites every file it touches in full,
    and a delta file touches files all over the history, so every day copied
    the whole of it."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 2)),
                             ("ISIN2", "MIC1", "Y", date(2016, 9, 2)),
                             ("ISIN3", "MIC1", "Z", date(2016, 9, 2))])
    path = start_history(spark, config, full)
    spark.read.format("delta").load(path).coalesce(1).write.format("delta").mode(
        "overwrite").save(path)

    apply_day(spark, config, date(2016, 9, 4),
              [("ISIN1", "MIC1", "X2", date(2016, 9, 4), "ModfdRcrd")])

    from delta.tables import DeltaTable
    table = DeltaTable.forPath(spark, path)
    merge = table.history().where(func.col("operation") == "MERGE").first()
    assert merge["operationMetrics"]["numTargetRowsCopied"] == "0"
    properties = table.detail().first()["properties"]
    assert properties["delta.enableDeletionVectors"] == "true"


# Starting the history, and a watermark that was not written
# --------------------------------------------------------------------------


def test_the_history_starts_from_the_newest_full_file(spark, config):
    """A full file states the reference data in full. An older one holds
    instruments the newest one no longer carries, and taking them along would
    register them as being in force."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2), isin="OLD")
    write_full_bronze(spark, config, date(2016, 9, 9), isin="NEW")
    finished(config, "FULINS_E_20160902_01of01.zip", "FULINS_E_20160909_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    built = common.connect_to_firds(spark, config, "main")
    assert [r["isin"] for r in built.collect()] == ["NEW"]
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-09"


def test_the_history_starts_from_the_full_file_the_landing_zone_records(spark, config):
    """The landing zone fetches the delta files after the full file it
    records, which after the history was moved back is not the newest one;
    starting from the newest would pass over the delta files between."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2), isin="OLD")
    write_full_bronze(spark, config, date(2016, 9, 9), isin="NEW")
    finished(config, "FULINS_E_20160902_01of01.zip", "FULINS_E_20160909_01of01.zip")
    landing.write_landing_watermark(config, None, full_sample={
        "date": "2016-09-02", "files": ["FULINS_E_20160902_01of01.zip"],
    })

    silver.update_from_bronze_to_silver_main(spark, config)

    built = common.connect_to_firds(spark, config, "main")
    assert [r["isin"] for r in built.collect()] == ["OLD"]
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"


def test_a_recorded_full_file_not_in_bronze_is_reported(spark, config):
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 9))
    finished(config, "FULINS_E_20160909_01of01.zip")
    landing.write_landing_watermark(config, None, full_sample={
        "date": "2016-09-02", "files": ["FULINS_E_20160902_01of01.zip"],
    })

    with pytest.raises(RuntimeError, match="2016-09-02"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert not os.path.isdir(os.path.join(common.path_silver_main(config), "_delta_log"))


# The size the weekly compaction writes
# --------------------------------------------------------------------------


def test_the_compaction_is_held_at_the_target_file_size(spark, config):
    """Open-source Delta takes the size from the session, since the property
    that would record it on the table is one it refuses."""
    with silver.compaction_file_size(spark):
        inside = [spark.conf.get(key) for key in silver.OPTIMIZE_SIZE_CONFS]

    assert inside == [str(silver.OPTIMIZE_FILE_SIZE)] * 2
    assert silver.OPTIMIZE_FILE_SIZE == 128 * 1024 * 1024


def test_both_ends_of_the_size_are_set(spark, config):
    """Left at Delta's gigabyte, the minimum makes every file below a
    gigabyte eligible and each run rewrites what it has already compacted."""
    assert silver.OPTIMIZE_SIZE_CONFS == (
        "spark.databricks.delta.optimize.maxFileSize",
        "spark.databricks.delta.optimize.minFileSize",
    )


def test_a_session_shared_with_other_tables_is_left_as_it_was(spark, config):
    """The settings are the session's, so this table's layout must not stay
    behind on it."""
    key = silver.OPTIMIZE_SIZE_CONFS[0]
    spark.conf.set(key, "999999")
    try:
        with silver.compaction_file_size(spark):
            pass
        assert spark.conf.get(key) == "999999"
    finally:
        spark.conf.unset(key)


def test_a_setting_that_was_not_set_is_unset_again(spark, config):
    """Asked with a default, the session reports only what it carries itself.
    A bare read would answer with Delta's own default instead, once a Delta
    write has initialised it, so it cannot say whether the setting is there."""
    key = silver.OPTIMIZE_SIZE_CONFS[1]
    spark.conf.unset(key)

    with silver.compaction_file_size(spark):
        assert spark.conf.get(key) == str(silver.OPTIMIZE_FILE_SIZE)

    assert spark.conf.get(key, None) is None


def test_the_settings_are_restored_although_the_compaction_fails(spark, config):
    """A compaction that raises must not leave the session behind it."""
    key = silver.OPTIMIZE_SIZE_CONFS[0]
    spark.conf.unset(key)

    with pytest.raises(RuntimeError, match="compaction failed"):
        with silver.compaction_file_size(spark):
            raise RuntimeError("the compaction failed")

    assert spark.conf.get(key, None) is None


def test_delta_s_own_default_is_not_taken_for_a_setting_of_the_session(
    spark, config
):
    """Delta registers a default for these settings, which a session that has
    written a Delta table reports as though it had asked for it. Restoring
    that would pin the value of the Delta in use into the session."""
    key = silver.OPTIMIZE_SIZE_CONFS[0]
    spark.conf.unset(key)
    # A Delta write, so that Delta's configuration is initialised and its
    # default is what a bare read answers with.
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")
    silver.update_from_bronze_to_silver_main(spark, config)

    assert spark.conf.get(key) == str(1024 * 1024 * 1024)

    with silver.compaction_file_size(spark):
        pass

    assert spark.conf.get(key, None) is None


# The size of a row group
# --------------------------------------------------------------------------


def test_the_row_groups_are_held_at_a_quarter_of_parquet_s_default(spark, config):
    """Parquet's 128 megabytes left the compaction writing groups whose
    largest column alone came to 136 megabytes read."""
    with silver.row_group_size(spark):
        inside = spark.conf.get(silver.ROW_GROUP_SIZE_CONF)

    assert inside == str(silver.ROW_GROUP_SIZE)
    assert silver.ROW_GROUP_SIZE == 32 * 1024 * 1024
    assert silver.ROW_GROUP_SIZE_CONF == "parquet.block.size"


def test_the_row_group_size_is_unset_again_afterwards(spark, config):
    """A session shared with other tables is left as it was found."""
    spark.conf.unset(silver.ROW_GROUP_SIZE_CONF)

    with pytest.raises(RuntimeError, match="write failed"):
        with silver.row_group_size(spark):
            raise RuntimeError("the write failed")

    assert spark.conf.get(silver.ROW_GROUP_SIZE_CONF, None) is None


def test_a_row_group_size_the_session_carried_is_kept(spark, config):
    spark.conf.set(silver.ROW_GROUP_SIZE_CONF, "999999")
    try:
        with silver.row_group_size(spark):
            pass
        assert spark.conf.get(silver.ROW_GROUP_SIZE_CONF) == "999999"
    finally:
        spark.conf.unset(silver.ROW_GROUP_SIZE_CONF)


def test_the_row_group_size_reaches_a_delta_write(spark, tmp_path):
    """Spark copies the settings of the session into the Hadoop configuration
    a write is made with, which is where Parquet looks for the size."""
    import pyarrow.parquet as pq

    path = str(tmp_path / "table")
    frame = spark.range(20000).selectExpr(
        "id", "sha2(cast(id as string), 512) as text"
    ).coalesce(1)
    with silver.row_group_size(spark, 64 * 1024):
        frame.write.format("delta").save(path)

    files = [name for name in os.listdir(path) if name.endswith(".parquet")]
    groups = [
        pq.ParquetFile(os.path.join(path, name)).metadata.num_row_groups
        for name in files
    ]
    assert len(files) == 1
    assert groups[0] > 1


def test_every_write_to_the_history_holds_the_row_group_size(
    spark, config, monkeypatch
):
    """The initialising write and the append of a delta file both write the
    history, so both are held at the size."""
    seen = []
    write_delta = common.write_delta

    def recording(df, ref, *args, **kwargs):
        if ref == common.ref_silver_main(config):
            seen.append(spark.conf.get(silver.ROW_GROUP_SIZE_CONF, None))
        return write_delta(df, ref, *args, **kwargs)

    monkeypatch.setattr(common, "write_delta", recording)
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")
    silver.update_from_bronze_to_silver_main(spark, config)

    assert seen == [str(silver.ROW_GROUP_SIZE)]
    assert spark.conf.get(silver.ROW_GROUP_SIZE_CONF, None) is None


def test_the_compaction_holds_the_row_group_size(spark, config, monkeypatch):
    """OPTIMIZE rewrites most of the history, so it is where the size
    matters most."""
    seen = []

    class Table:
        def optimize(self):
            return self

        def executeCompaction(self):
            seen.append(spark.conf.get(silver.ROW_GROUP_SIZE_CONF, None))

    monkeypatch.setattr(common, "delta_table", lambda spark, ref: Table())
    silver.compact_silver_main(spark, config)

    assert seen == [str(silver.ROW_GROUP_SIZE)]


# Vacuuming the history
# --------------------------------------------------------------------------


def history_written_twice(spark, config, tag=None):
    """A historical database of two versions, the second replacing the files
    of the first, as a merge or a compaction leaves it."""
    common.create_directory_tree(config)
    ref = common.ref_silver_main(config)
    common.write_delta(spark.range(10).toDF("value"), ref)
    options = {"userMetadata": tag} if tag else None
    common.write_delta(spark.range(20).toDF("value"), ref, options=options)
    return ref


def parquet_files(ref):
    return sorted(name for name in os.listdir(ref.path) if name.endswith(".parquet"))


def test_the_default_vacuum_keeps_a_week_of_the_history(spark, config):
    """Files replaced hours ago are still read by the versions before."""
    ref = history_written_twice(spark, config)
    before = parquet_files(ref)

    silver.vacuum_silver_main(spark, config)

    assert parquet_files(ref) == before
    assert len(common.read_delta_version(spark, ref, 0).collect()) == 10


def test_a_vacuum_without_retention_keeps_the_current_version_alone(spark, config):
    """What a backfill wants: every file it replaced is only hours old."""
    ref = history_written_twice(spark, config)
    before = parquet_files(ref)

    silver.vacuum_silver_main(spark, config, retention_hours=0)

    assert len(parquet_files(ref)) < len(before)
    assert common.read_delta(spark, ref).count() == 20
    # Read the rows rather than count them: Delta answers a count from the
    # statistics of its log without opening a file.
    with pytest.raises(Exception):
        common.read_delta_version(spark, ref, 0).collect()


def test_the_retention_check_is_lifted_for_the_vacuum_alone(spark, config):
    """Delta refuses a retention below the table's, and a session shared with
    other tables keeps that refusal."""
    history_written_twice(spark, config)
    spark.conf.unset(silver.RETENTION_CHECK_CONF)

    silver.vacuum_silver_main(spark, config, retention_hours=0)

    assert spark.conf.get(silver.RETENTION_CHECK_CONF, None) is None
    assert silver.RETENTION_CHECK_CONF == (
        "spark.databricks.delta.retentionDurationCheck.enabled"
    )


def test_a_batch_whose_append_is_missing_is_not_vacuumed_away(spark, config):
    """The batch is resumed from the version before its merge, which a short
    retention would remove."""
    tag = silver.commit_tag(silver.span_label(["2026-01-01", "2026-01-02"]), "merge")
    ref = history_written_twice(spark, config, tag=tag)
    before = parquet_files(ref)

    with pytest.raises(RuntimeError, match="2026-01-01..2026-01-02"):
        silver.vacuum_silver_main(spark, config, retention_hours=0)

    assert parquet_files(ref) == before


def test_a_batch_that_was_appended_can_be_vacuumed(spark, config):
    tag = silver.commit_tag(silver.span_label(["2026-01-01", "2026-01-02"]), "append")
    ref = history_written_twice(spark, config, tag=tag)
    before = parquet_files(ref)

    silver.vacuum_silver_main(spark, config, retention_hours=0)

    assert len(parquet_files(ref)) < len(before)


@pytest.mark.parametrize("retention", [True, "0", None, -1])
def test_a_retention_that_is_not_a_number_of_hours_is_refused(
    spark, config, retention
):
    with pytest.raises((TypeError, ValueError), match="retention_hours"):
        silver.vacuum_silver_main(spark, config, retention_hours=retention)


# A history whose start was moved back
# --------------------------------------------------------------------------


def test_two_dates_in_the_landing_record_say_the_start_was_moved_back():
    assert silver.moved_back_to({
        "full_sample": {"date": "2023-12-30"}, "history_from": "2024-01-06",
    }) == "2023-12-30"


def test_one_date_in_the_landing_record_says_nothing_about_the_start():
    """A data lake filled by hand records no full sample, and one whose
    history was built before the start was recorded records no start."""
    assert silver.moved_back_to(
        {"full_sample": None, "history_from": "2024-01-06"}
    ) is None
    assert silver.moved_back_to(
        {"full_sample": {"date": "2023-12-30"}, "history_from": None}
    ) is None


def test_a_full_sample_the_history_reaches_back_past_is_not_a_move_back():
    """A history reaching further back than the sample the landing zone last
    listed is the ordinary state: the delta files between the two have been
    applied to it already."""
    assert silver.moved_back_to(
        {"full_sample": {"date": "2024-01-06"}, "history_from": "2024-01-06"}
    ) is None
    assert silver.moved_back_to(
        {"full_sample": {"date": "2024-01-20"}, "history_from": "2024-01-06"}
    ) is None


def test_the_full_file_the_history_was_built_from_is_recorded(spark, config):
    """The landing zone cannot read it off the historical database, and needs
    it to tell that a run has moved the start of the history back."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    assert landing.read_landing_state(config)["history_from"] == "2016-09-02"


def test_a_history_whose_start_was_moved_back_is_built_again(spark, config):
    """The earlier full file and the delta files between it and the one the
    history stands on are all older than the watermark, so the update would
    pass over every one of them and report that there was nothing to do."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 9), isin="NEW")
    finished(config, "FULINS_E_20160909_01of01.zip")
    landing.write_landing_watermark(config, "2016-09-09", full_sample={
        "date": "2016-09-09", "files": ["FULINS_E_20160909_01of01.zip"],
    })
    silver.update_from_bronze_to_silver_main(spark, config)
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-09"

    # A run of the landing zone was asked to start the history earlier and
    # fetched the earlier full file with the delta file published between the
    # two; the Bronze layer has imported both.
    write_full_bronze(spark, config, date(2016, 9, 2), isin="OLD")
    write_bronze(spark, config, [
        (date(2016, 9, 5), [("ISIN9", "MIC1", "Between", date(2016, 9, 5), "NewRcrd")]),
    ])
    finished(config, "FULINS_E_20160902_01of01.zip", "DLTINS_20160905_01of01.zip")
    state = landing.read_landing_state(config)
    landing.write_landing_watermark(
        config, None,
        applied_through=state["applied_through"],
        history_from=state["history_from"],
        full_sample={"date": "2016-09-02",
                     "files": ["FULINS_E_20160902_01of01.zip"]},
    )

    silver.update_from_bronze_to_silver_main(spark, config)

    built = common.connect_to_firds(spark, config, "main")
    assert sorted(r["isin"] for r in built.collect()) == ["ISIN9", "OLD"]
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-05"
    assert landing.read_landing_state(config)["history_from"] == "2016-09-02"


def test_a_history_built_again_does_not_inherit_the_tags_of_the_one_before(
    spark, config
):
    """The rebuild replaces the database, so the commits of the history it
    replaced must not be read as the state of the one that stands."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 9), isin="NEW")
    finished(config, "FULINS_E_20160909_01of01.zip")
    landing.write_landing_watermark(config, "2016-09-09", full_sample={
        "date": "2016-09-09", "files": ["FULINS_E_20160909_01of01.zip"],
    })
    silver.update_from_bronze_to_silver_main(spark, config)

    write_full_bronze(spark, config, date(2016, 9, 2), isin="OLD")
    finished(config, "FULINS_E_20160902_01of01.zip")
    landing.write_landing_watermark(
        config, None, history_from="2016-09-09",
        full_sample={"date": "2016-09-02",
                     "files": ["FULINS_E_20160902_01of01.zip"]},
    )

    silver.update_from_bronze_to_silver_main(spark, config)

    assert silver.watermark_from_commits(
        spark, common.ref_silver_main(config)
    ) == "2016-09-02"


def test_an_unrecorded_start_of_the_history_is_reported(spark, config, caplog):
    """A data lake whose history was built before the start was recorded. The
    log of the historical database is no substitute: the commit naming the
    full file is cleaned out once it is older than the log retention."""
    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    start_history(spark, config, full)
    common.update_metadata_maxdate(
        spark, config, table="MAIN", maxdate="2016-09-02"
    )
    landing.write_landing_watermark(config, "2016-09-02", full_sample={
        "date": "2016-09-02", "files": ["FULINS_E_20160902_01of01.zip"],
    })

    with caplog.at_level(logging.WARNING):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert any("is not recorded" in record.message for record in caplog.records)
    # The history is left as it stands: rebuilding it on a guess would
    # discard a history that may be perfectly good.
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"


def test_a_watermark_lost_after_the_history_was_written_is_recovered(spark, config):
    """The history is written before the watermark, so a run that died between
    the two left a database that could not be updated at all."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")
    silver.update_from_bronze_to_silver_main(spark, config)
    shutil.rmtree(common.path_metadata(config))

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"


def test_a_watermark_lost_after_a_delta_file_is_recovered(spark, config):
    full = full_file(spark, RESUMED_FULL)
    path = start_history(spark, config, full)
    apply_day(spark, config, *RESUMED_FILE)
    built = versions(spark.read.format("delta").load(path))
    shutil.rmtree(common.path_metadata(config))

    recovered = silver.watermark_from_commits(spark, common.ref_silver_main(config))
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate=recovered)
    apply_day(spark, config, *RESUMED_FILE)

    assert recovered == "2016-09-06"
    assert versions(spark.read.format("delta").load(path)) == built


def test_a_watermark_lost_after_a_merge_puts_the_date_back_in_the_queue(
    monkeypatch, spark, config
):
    """Only the merge of the date was committed, so the date has to be applied
    again and the watermark is the day before it."""
    full = full_file(spark, RESUMED_FULL)
    path = start_history(spark, config, full)
    fail_once(monkeypatch, history, "records_to_insert")
    with pytest.raises(RuntimeError, match="died"):
        apply_day(spark, config, *RESUMED_FILE)

    assert silver.watermark_from_commits(
        spark, common.ref_silver_main(config)
    ) == "2016-09-05"


def test_a_history_that_names_no_reference_date_is_still_refused(spark, config):
    """A database that was not built by this code carries no such commits, and
    there is no way to tell which delta files it has already seen."""
    build_main(spark, config, [history_row("ISIN1", "MIC1")])

    with pytest.raises(RuntimeError, match="no watermark"):
        silver.update_from_bronze_to_silver_main(spark, config)


def test_the_full_and_the_delta_files_are_translated_by_one_function(spark):
    """The two translations are identical, line for line. Keeping them apart
    would invite one of them to be corrected and the other left behind."""
    assert (silver.translate_xml_to_analytics_schema_017_001_02
            is silver.translate_xml_to_analytics_schema_036_001_03)
    assert set(silver.get_analytical_fields()) < set(silver.ANALYTICS_COLUMNS)


# A day that never reached the Bronze layer
# --------------------------------------------------------------------------


def quarantined(config, *names):
    for name in names:
        open(os.path.join(common.path_landing_error(config), name), "w").close()


def test_a_day_whose_file_was_quarantined_stops_the_update(spark, config):
    """The day has no Bronze partition at all. Taking the dates from the Bronze
    layer alone passed over it and applied the day after it, which moved the
    watermark beyond a day whose records could then never be applied."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    path = start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    later = (date(2016, 9, 3),
             [("ISIN1", "MIC1", "from the third", date(2016, 9, 3), "ModfdRcrd")])
    write_bronze(spark, config, [later])
    finished(config, "DLTINS_20160903_01of01.zip")
    quarantined(config, "DLTINS_20160902_01of01.zip")

    with pytest.raises(RuntimeError, match="DLTINS of 20160902"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-01"

    # The day is imported after all, and both days are then applied in order.
    earlier = (date(2016, 9, 2),
               [("ISIN1", "MIC1", "from the second", date(2016, 9, 2), "ModfdRcrd")])
    write_bronze(spark, config, [earlier])
    os.remove(os.path.join(common.path_landing_error(config),
                           "DLTINS_20160902_01of01.zip"))
    finished(config, "DLTINS_20160902_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-03"
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, [earlier, later])
    )


def test_a_day_that_was_published_but_never_arrived_stops_the_update(spark, config):
    """A day the landing zone never fetched leaves no trace in the data lake,
    so only what the landing zone recorded of the register can say that a later
    day is not safe to apply."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    write_bronze(spark, config, [
        (date(2016, 9, 3), [("ISIN1", "MIC1", "third", date(2016, 9, 3), "ModfdRcrd")])])
    finished(config, "DLTINS_20160903_01of01.zip")
    landing.write_landing_watermark(
        config, "2016-09-01", published=["2016-09-02", "2016-09-03"]
    )

    with pytest.raises(RuntimeError, match="DLTINS of 20160902"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-01"

    # Once that day has been imported as well, both are applied.
    write_bronze(spark, config, [
        (date(2016, 9, 2), [("ISIN1", "MIC1", "second", date(2016, 9, 2), "ModfdRcrd")])])
    finished(config, "DLTINS_20160902_01of01.zip")
    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-03"


def test_a_day_downloaded_and_imported_in_the_same_run_is_applied(spark, config):
    """The landing zone records what it knows before it downloads, so its
    watermark still names the day before. The day must not be held back for
    that reason."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    write_bronze(spark, config, [
        (date(2016, 9, 2), [("ISIN1", "MIC1", "second", date(2016, 9, 2), "ModfdRcrd")])])
    finished(config, "DLTINS_20160902_01of01.zip")
    landing.write_landing_watermark(config, "2016-09-01", published=["2016-09-02"])

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"


# A delta file that carried no records
# --------------------------------------------------------------------------


def empty_partition(config, day):
    """What the Bronze layer leaves for a file that held no records."""
    os.makedirs(os.path.join(common.path_bronze_auth_036_001_03(config),
                             "DATADT=" + format_reference_date(day)))


def test_a_day_that_held_no_records_is_passed(spark, config):
    """The file was imported and had nothing in it, so there is nothing to
    apply and the watermark moves on rather than stopping on it for ever."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    path = start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    empty_partition(config, date(2016, 9, 2))
    later = (date(2016, 9, 3),
             [("ISIN1", "MIC1", "third", date(2016, 9, 3), "ModfdRcrd")])
    write_bronze(spark, config, [later])
    finished(config, "DLTINS_20160902_01of01.zip", "DLTINS_20160903_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-03"
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, [later])
    )


def test_a_partition_that_has_gone_missing_stops_the_update(spark, config):
    """Every part imported and not even an empty partition: something removed
    it, which is not the same as a day that held no records."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    write_bronze(spark, config, [
        (date(2016, 9, 3), [("ISIN1", "MIC1", "third", date(2016, 9, 3), "ModfdRcrd")])])
    finished(config, "DLTINS_20160902_01of01.zip", "DLTINS_20160903_01of01.zip")

    with pytest.raises(RuntimeError, match="gone missing"):
        silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-01"


# A delta file that arrived after its day had been applied
# --------------------------------------------------------------------------


def test_a_day_that_arrived_after_the_history_passed_it_stops_the_update(spark, config):
    """Its records are not in the history, and applying the days after it
    would not put them there."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-05")
    landing.write_landing_watermark(config, "2016-09-05", late=["2016-09-03"])

    with pytest.raises(RuntimeError, match="2016-09-03"):
        silver.update_from_bronze_to_silver_main(spark, config)

    # The gap can be accepted, and the update then carries on.
    landing.clear_late_day(config, "2016-09-03")
    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-05"


def test_a_day_that_arrived_late_but_in_time_is_applied_and_forgotten(spark, config):
    """The landing zone had settled the day, but the history had not reached
    it yet, so there is nothing wrong once it has been applied."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    path = start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    day = (date(2016, 9, 2),
           [("ISIN1", "MIC1", "second", date(2016, 9, 2), "ModfdRcrd")])
    write_bronze(spark, config, [day])
    finished(config, "DLTINS_20160902_01of01.zip")
    landing.write_landing_watermark(config, "2016-09-02", late=["2016-09-02"])

    silver.update_from_bronze_to_silver_main(spark, config)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"
    assert landing.read_landing_state(config)["late"] == []
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, [day])
    )


# The Bronze layer keeps the files it has written, and the XML schema the
# import reads with is derived from ESMA's XSD files and can be derived again
# with other types -- one vintage holds the money and rate fields as
# ``double``, another as ``decimal``. A partition therefore comes to hold files
# of more than one vintage, which Parquet will not hand over under one schema.
# The tests below are about the read staying whole across such a change.

RECORD_SCHEMA = load_pyspark_schema("firds_schema_036_001_03")

#: A path the pipeline reads and one it does not, both typed ``decimal`` by
#: the schema as it stands.
USED = "NewRcrd.DebtInstrmAttrbts.IntrstRate.Fxd"
UNUSED = ("NewRcrd.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.Intrst"
          ".FrstLegIntrstRate.Fxd")


def retyped(schema, path, data_type):
    """A copy of ``schema`` in which ``path`` carries ``data_type``."""
    first, _, rest = path.partition(".")
    fields = []
    for field in schema.fields:
        if field.name != first:
            fields.append(field)
        elif rest:
            fields.append(types.StructField(
                first, retyped(field.dataType, rest, data_type), field.nullable
            ))
        else:
            fields.append(types.StructField(first, data_type, field.nullable))
    return types.StructType(fields)


def write_partition(spark, config, datadt, rows, schema):
    """Write one file into the Bronze partition of ``datadt``."""
    partition = os.path.join(
        common.path_bronze_auth_036_001_03(config),
        silver.PARTITION_PREFIX + format_reference_date(datadt),
    )
    spark.createDataFrame(rows, schema).write.mode("append").parquet(partition)


def new_record(isin, rate=None, frdt=date(2016, 9, 4)):
    """A ``<NewRcrd>`` that states a fixed interest rate."""
    return {
        "NewRcrd": {
            "FinInstrmGnlAttrbts": {"Id": isin, "FullNm": isin},
            "TradgVnRltdAttrbts": {"Id": "MIC1"},
            "DebtInstrmAttrbts": {"IntrstRate": {"Fxd": rate}},
            "TechAttrbts": {"PblctnPrd": {"FrDt": frdt}},
        }
    }


def read_partition(spark, config, datadt):
    return silver.read_bronze_partition(
        spark,
        common.path_bronze_auth_036_001_03(config),
        format_reference_date(datadt),
        records=history.ACTION_TYPES,
    )


def test_a_partition_written_with_two_schemas_is_read_as_one(spark, config, caplog):
    """A field the pipeline reads was written as a double in one file and as a
    decimal in another. Reading them together would ask Parquet to convert one
    into the other, which it refuses to do, so they are read apart."""
    common.create_directory_tree(config)
    day = date(2016, 9, 4)
    write_partition(spark, config, day, [new_record("ISIN1", Decimal("1.5"))],
                    RECORD_SCHEMA)
    write_partition(spark, config, day, [new_record("ISIN2", 2.5)],
                    retyped(RECORD_SCHEMA, USED, types.DoubleType()))

    with caplog.at_level(logging.WARNING):
        read = read_partition(spark, config, day)
    rates = {r["isin"]: r["interest_rate_fixed"] for r in
             silver.read_daily_changes(read).collect()}

    assert rates == {"ISIN1": 1.5, "ISIN2": 2.5}
    assert "written with different schemas" in caplog.text


def test_a_field_the_pipeline_does_not_use_may_change_its_type(spark, config):
    """The read asks for the paths the translation reads and for nothing else,
    so a field that is never used can change its type without being noticed --
    the two files here are read together, under one schema."""
    common.create_directory_tree(config)
    day = date(2016, 9, 4)
    write_partition(spark, config, day, [new_record("ISIN1", Decimal("1.5"))],
                    RECORD_SCHEMA)
    write_partition(spark, config, day, [new_record("ISIN2", Decimal("2.5"))],
                    retyped(RECORD_SCHEMA, UNUSED, types.LongType()))

    read = read_partition(spark, config, day)

    assert sorted(r["isin"] for r in silver.read_daily_changes(read).collect()) == [
        "ISIN1", "ISIN2"
    ]


def test_a_field_the_file_does_not_state_is_read_as_empty(spark, config):
    """An older vintage of the schema may not carry a path at all. It is asked
    for with the type the analytics schema wants and comes back empty, rather
    than making the read fail."""
    common.create_directory_tree(config)
    day = date(2016, 9, 4)
    reduced = types.StructType([
        types.StructField("NewRcrd", types.StructType([
            types.StructField("FinInstrmGnlAttrbts", types.StructType([
                types.StructField("Id", types.StringType()),
            ])),
            types.StructField("TradgVnRltdAttrbts", types.StructType([
                types.StructField("Id", types.StringType()),
            ])),
            types.StructField("TechAttrbts", types.StructType([
                types.StructField("PblctnPrd", types.StructType([
                    types.StructField("FrDt", types.DateType()),
                ])),
            ])),
        ])),
    ])
    write_partition(spark, config, day, [{"NewRcrd": {
        "FinInstrmGnlAttrbts": {"Id": "ISIN1"},
        "TradgVnRltdAttrbts": {"Id": "MIC1"},
        "TechAttrbts": {"PblctnPrd": {"FrDt": day}},
    }}], reduced)

    rows = silver.read_daily_changes(read_partition(spark, config, day)).collect()

    assert [(r["isin"], r["interest_rate_fixed"], r["full_name"]) for r in rows] == [
        ("ISIN1", None, None)
    ]


def test_a_publication_period_stated_as_a_range_is_read(spark, config):
    """``TechAttrbts/PblctnPrd`` is a choice, and ESMA states it as
    ``<FrDt>`` for some records and ``<FrDtToDt><FrDt>`` for others in the
    same file. Reading only the first branch left the second with no validity
    date at all, and a version with no VLDFROM can never be closed."""
    common.create_directory_tree(config)
    day = date(2016, 9, 4)
    ranged = types.StructType([
        types.StructField("NewRcrd", types.StructType([
            types.StructField("FinInstrmGnlAttrbts", types.StructType([
                types.StructField("Id", types.StringType()),
            ])),
            types.StructField("TradgVnRltdAttrbts", types.StructType([
                types.StructField("Id", types.StringType()),
            ])),
            types.StructField("TechAttrbts", types.StructType([
                types.StructField("PblctnPrd", types.StructType([
                    types.StructField("FrDtToDt", types.StructType([
                        types.StructField("FrDt", types.DateType()),
                        types.StructField("ToDt", types.DateType()),
                    ])),
                ])),
            ])),
        ])),
    ])
    write_partition(spark, config, day, [{"NewRcrd": {
        "FinInstrmGnlAttrbts": {"Id": "ISIN1"},
        "TradgVnRltdAttrbts": {"Id": "MIC1"},
        "TechAttrbts": {"PblctnPrd": {"FrDtToDt": {"FrDt": day, "ToDt": day}}},
    }}], ranged)

    rows = silver.read_daily_changes(read_partition(spark, config, day)).collect()

    assert [(r["isin"], r["tech_publication_period_from_date"], r["tech_publication_period_to_date"])
            for r in rows] == [("ISIN1", day, day)]


def test_a_termination_without_a_publication_period_takes_the_file_date(spark, config):
    """A termination may state no publication period at all -- 4,477 of the
    24,107 in one delta file of January 2020 carried nothing in TechAttrbts
    but the competent authority and the venue. The reference date of the file
    stands in, as it does for a cancellation."""
    common.create_directory_tree(config)
    day = date(2016, 9, 4)
    periodless = types.StructType([
        types.StructField("TermntdRcrd", types.StructType([
            types.StructField("FinInstrmGnlAttrbts", types.StructType([
                types.StructField("Id", types.StringType()),
            ])),
            types.StructField("TradgVnRltdAttrbts", types.StructType([
                types.StructField("Id", types.StringType()),
            ])),
            types.StructField("TechAttrbts", types.StructType([
                types.StructField("RlvntCmptntAuthrty", types.StringType()),
            ])),
        ])),
    ])
    write_partition(spark, config, day, [{"TermntdRcrd": {
        "FinInstrmGnlAttrbts": {"Id": "ISIN1"},
        "TradgVnRltdAttrbts": {"Id": "MIC1"},
        "TechAttrbts": {"RlvntCmptntAuthrty": "DE"},
    }}], periodless)

    rows = silver.read_daily_changes(read_partition(spark, config, day)).collect()

    assert [(r["isin"], r["action_type"], r["tech_publication_period_from_date"]) for r in rows] == [
        ("ISIN1", "TermntdRcrd", day)
    ]


def test_a_record_without_a_publication_period_is_refused(spark, config):
    """A version whose VLDFROM is empty cannot be closed: the closing date is
    the day before it, and that is empty as well, so the version it supersedes
    stays in force. Nothing fails at the time; the fault surfaces days later as
    a merge matching several rows at once."""
    records = spark.createDataFrame(
        [("ISIN1", "MIC1", None, "NewRcrd"), ("ISIN2", "MIC1", date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    with pytest.raises(ValueError, match="state no publication start date"):
        silver.check_publication_period(records, "the delta file of 2016-09-04")


def test_a_record_without_an_instrument_key_is_refused(spark, config):
    """Every comparison on (isin, trading_venue) is an equality, which no null meets,
    so such a record is never found to supersede anything and is inserted again
    by every file that carries it."""
    records = spark.createDataFrame(
        [("ISIN1", None, date(2016, 9, 4), "NewRcrd"),
         ("ISIN2", "MIC1", date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    with pytest.raises(ValueError, match="state no isin or trading_venue"):
        silver.check_instrument_key(records, "the delta file of 2016-09-04")


def test_both_checks_of_a_file_are_made_in_one_pass(spark, config):
    """What each of them refuses on its own, refused together."""
    records = spark.createDataFrame(
        [("ISIN1", "MIC1", None, "NewRcrd"),
         ("ISIN2", "MIC1", date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    with pytest.raises(ValueError, match="state no publication start date"):
        silver.check_incoming_records(records, "the delta file of 2016-09-04")

    records = spark.createDataFrame(
        [("ISIN1", None, date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    with pytest.raises(ValueError, match="state no isin or trading_venue"):
        silver.check_incoming_records(records, "the delta file of 2016-09-04")


def test_a_file_that_fails_both_checks_is_refused_for_the_key(spark, config):
    """The order the checks were made in separately, so that the message of a
    file refused by both does not turn on how they are now counted."""
    records = spark.createDataFrame(
        [("ISIN1", None, None, "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    with pytest.raises(ValueError, match="state no isin or trading_venue"):
        silver.check_incoming_records(records, "the delta file of 2016-09-04")


def test_records_both_checks_accept_are_passed_on(spark, config):
    records = spark.createDataFrame(
        [("ISIN1", "MIC1", date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    assert silver.check_incoming_records(records, "a file") is records


def test_records_that_all_state_an_instrument_key_are_passed_on(spark, config):
    records = spark.createDataFrame(
        [("ISIN1", "MIC1", date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    assert silver.check_instrument_key(records, "the full file of 2016-09-04") is records


def test_records_that_all_state_a_publication_period_are_passed_on(spark, config):
    records = spark.createDataFrame(
        [("ISIN1", "MIC1", date(2016, 9, 4), "NewRcrd")],
        "isin string, trading_venue string, tech_publication_period_from_date date, action_type string",
    )

    assert silver.check_publication_period(records, "a file").count() == 1


def test_an_element_a_file_leaves_empty_is_not_read_as_a_record(spark, config):
    """The four record types are told apart by which element a row states, and
    the read rebuilds those elements. A rebuilt element made of empty fields is
    itself not empty, so leaving it empty is done by hand -- otherwise every
    row would count as a record of all four types."""
    common.create_directory_tree(config)
    day = date(2016, 9, 4)
    write_partition(spark, config, day, [{"CancRcrd": {
        "FinInstrmGnlAttrbts": {"Id": "ISIN1"},
        "TradgVnRltdAttrbts": {"Id": "MIC1"},
    }}], RECORD_SCHEMA)

    rows = silver.read_daily_changes(read_partition(spark, config, day)).collect()

    assert [(r["isin"], r["action_type"]) for r in rows] == [("ISIN1", "CancRcrd")]


def test_a_day_whose_partition_holds_two_vintages_is_applied(spark, config):
    """The whole daily path over a partition that mixes them."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 1))])
    path = start_history(spark, config, full)
    common.update_metadata_maxdate(spark, config, table="MAIN", maxdate="2016-09-01")
    day = date(2016, 9, 2)
    write_partition(spark, config, day, [new_record("ISIN1", Decimal("1.5"), day)],
                    RECORD_SCHEMA)
    write_partition(spark, config, day, [new_record("ISIN2", 2.5, day)],
                    retyped(RECORD_SCHEMA, USED, types.DoubleType()))
    finished(config, "DLTINS_20160902_01of02.zip", "DLTINS_20160902_02of02.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    # The history the tests start from carries the columns of FULL_SCHEMA, so
    # the rate the two files disagreed on is not among the ones the merge
    # keeps. That the records of both files arrived is what this shows; the
    # rate itself is read back in the test of the read above.
    built = spark.read.format("delta").load(path)
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-02"
    assert {(r["isin"], r["full_name"]) for r in
            built.filter(func.col("latest_record")).collect()} == {
        ("ISIN1", "ISIN1"), ("ISIN2", "ISIN2")
    }


def test_the_full_file_is_read_through_the_same_paths(spark, config):
    """The history starts from a full file, which is read the same way."""
    common.create_directory_tree(config)
    day = date(2016, 9, 2)
    full_schema = load_pyspark_schema("firds_schema_017_001_02")
    record = {
        "FinInstrmGnlAttrbts": {"Id": "ISIN1"},
        "TradgVnRltdAttrbts": {"Id": "MIC1"},
        "TechAttrbts": {"PblctnPrd": {"FrDt": day}},
    }
    partition = os.path.join(
        common.path_bronze_auth_017_001_02(config),
        silver.PARTITION_PREFIX + format_reference_date(day),
    )
    spark.createDataFrame([record], full_schema).write.mode("append").parquet(partition)
    spark.createDataFrame(
        [dict(record, FinInstrmGnlAttrbts={"Id": "ISIN2"})],
        retyped(full_schema, "DebtInstrmAttrbts.IntrstRate.Fxd", types.DoubleType()),
    ).write.mode("append").parquet(partition)
    finished(config, "FULINS_E_20160902_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config)

    assert sorted(r["isin"] for r in
                  common.connect_to_firds(spark, config, "main").collect()) == [
        "ISIN1", "ISIN2"
    ]


def test_a_partition_without_a_file_is_reported_as_such(spark, config):
    common.create_directory_tree(config)
    os.makedirs(os.path.join(common.path_bronze_auth_036_001_03(config),
                             silver.PARTITION_PREFIX + "2016-09-04"))

    with pytest.raises(RuntimeError, match="holds no file"):
        read_partition(spark, config, date(2016, 9, 4))


# How often the history is compacted
# --------------------------------------------------------------------------


def compaction_log(monkeypatch):
    """Record the compactions a run makes instead of making them.

    What these tests are about is when the compaction is run, and an OPTIMIZE
    of a history of a few rows says nothing about that while being the slowest
    thing in them. The watermark is written before the compaction is reached,
    so recording it names the date the run had just applied.
    """
    compacted = []

    def record(spark, config_path):
        compacted.append(common.get_metadata_maxdate(spark, config_path, "MAIN"))

    monkeypatch.setattr(silver, "compact_silver_main", record)
    return compacted


#: Days of a fortnight whose delta files each change one instrument. The
#: history starts on Friday 2 September 2016, so the Fridays among them are the
#: 9th and the 16th.
FORTNIGHT = [date(2016, 9, day) for day in range(5, 19)]


def changes_over(spark, config, days):
    """A history and one delta file per day of ``days``."""
    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    path = start_history(spark, config, full)
    files = [
        (day, [("ISIN1", "MIC1", f"Name {day.day}", day, "ModfdRcrd")])
        for day in days
    ]
    write_bronze(spark, config, files)
    finished(config, *[f"DLTINS_{day:%Y%m%d}_01of01.zip" for day in days])
    return path, files


def test_the_default_compacts_on_the_delta_file_of_each_friday(
    monkeypatch, spark, config
):
    """What the daily job has always done, and what a run that names no
    cadence still gets."""
    compacted = compaction_log(monkeypatch)
    changes_over(spark, config, FORTNIGHT)

    silver.update_from_bronze_to_silver_main(spark, config)

    assert compacted == ["2016-09-09", "2016-09-16"]


def test_a_backfill_can_ask_for_no_compaction_at_all(monkeypatch, spark, config):
    """The cadence of the daily job rewrites a growing history once per week of
    the period being built, which is what a backfill is asking to be spared."""
    compacted = compaction_log(monkeypatch)
    changes_over(spark, config, FORTNIGHT)

    silver.update_from_bronze_to_silver_main(
        spark, config, compact=silver.COMPACT_NEVER
    )

    assert compacted == []
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-18"


def test_a_cadence_counts_the_dates_the_run_applied(monkeypatch, spark, config):
    compacted = compaction_log(monkeypatch)
    changes_over(spark, config, FORTNIGHT)

    silver.update_from_bronze_to_silver_main(spark, config, compact=5)

    assert compacted == ["2016-09-09", "2016-09-14"]


def test_a_date_that_held_no_records_does_not_count_towards_the_cadence(
    monkeypatch, spark, config
):
    """Such a date writes nothing, so there is nothing it could leave to
    compact; counting it would compact over an unchanged history."""
    compacted = compaction_log(monkeypatch)
    changes_over(spark, config, [date(2016, 9, 6), date(2016, 9, 7)])
    os.makedirs(os.path.join(common.path_bronze_auth_036_001_03(config),
                             silver.PARTITION_PREFIX + "2016-09-05"))
    finished(config, "DLTINS_20160905_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config, compact=2)

    assert compacted == ["2016-09-07"]


@pytest.mark.parametrize(
    "cadence", [True, False, 0, -1, 2.5, None, "sometimes"]
)
def test_a_cadence_that_names_nothing_is_refused(monkeypatch, spark, config,
                                                 cadence):
    """``True`` is among them although it is an int: it reads as "yes,
    compact" and would mean compacting after every single date."""
    compacted = compaction_log(monkeypatch)
    changes_over(spark, config, [date(2016, 9, 6)])

    with pytest.raises(ValueError, match="compact is"):
        silver.update_from_bronze_to_silver_main(spark, config, compact=cadence)

    # Refused before anything was applied, rather than after some of it.
    assert compacted == []
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2000-01-01"


def test_the_cadence_does_not_change_the_history_that_is_built(spark, config):
    """The argument says when the files are rewritten and nothing about what
    is in them."""
    days = [date(2016, 9, 8), date(2016, 9, 9), date(2016, 9, 12)]
    path, files = changes_over(spark, config, days)

    silver.update_from_bronze_to_silver_main(
        spark, config, compact=silver.COMPACT_NEVER
    )

    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files)
    )


def test_the_compaction_leaves_the_history_as_it_found_it(spark, config):
    """The one test that runs a real OPTIMIZE: it is what a backfill calls
    when its run is done, and it must be safe to call at any time."""
    path, _ = changes_over(spark, config, [date(2016, 9, 6), date(2016, 9, 7)])
    silver.update_from_bronze_to_silver_main(
        spark, config, compact=silver.COMPACT_NEVER
    )
    before = versions(spark.read.format("delta").load(path))

    silver.compact_silver_main(spark, config)

    assert versions(spark.read.format("delta").load(path)) == before
    # And again on a history that has just been compacted.
    silver.compact_silver_main(spark, config)
    assert versions(spark.read.format("delta").load(path)) == before


# Several dates applied in one merge
# --------------------------------------------------------------------------


def through_batches(spark, config, full, files, size):
    """Build the history with ``size`` delta files planned and merged at a time."""
    path = start_history(spark, config, full)
    for start in range(0, len(files), size):
        chunk = files[start:start + size]
        bronze = functools.reduce(
            DataFrame.unionByName,
            [delta_file(spark, datadt, records) for datadt, records in chunk],
        )
        silver.upsert_from_bronze(
            spark, bronze, config, [format_reference_date(d) for d, _ in chunk]
        )
    return spark.read.format("delta").load(path)


#: Cases in which the files of a batch meet the same instrument, which is
#: where planning them together could differ from applying them in turn.
BATCH_CASES = {
    "changed on one day and again on the next": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "A", date(2016, 9, 4), "ModfdRcrd")]),
         (date(2016, 9, 5), [("ISIN1", "MIC1", "B", date(2016, 9, 5), "ModfdRcrd")])],
    ),
    # Applied in turn, the second replaces the version the first left in
    # force, because both begin on the same day.
    "a later file restating the same start replaces": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "A", date(2016, 9, 4), "ModfdRcrd")]),
         (date(2016, 9, 5), [("ISIN1", "MIC1", "B", date(2016, 9, 4), "ModfdRcrd")])],
    ),
    # Within one file a termination outranks a new record of the same day;
    # across files the later file wins whatever its element.
    "a later file outranks the record type": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "ended", date(2016, 9, 4), "TermntdRcrd")]),
         (date(2016, 9, 5), [("ISIN1", "MIC1", "again", date(2016, 9, 4), "NewRcrd")])],
    ),
    "new on one day and terminated on a later one": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN2", "MIC1", "Y", date(2016, 9, 4), "NewRcrd")]),
         (date(2016, 9, 6), [("ISIN2", "MIC1", "Y", date(2016, 9, 6), "TermntdRcrd")])],
    ),
    "terminated in the ordinary way and corrected later": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "X", date(2016, 9, 2), "TermntdRcrd")]),
         (date(2016, 9, 6), [("ISIN1", "MIC1", "corrected", date(2016, 9, 6), "ModfdRcrd")])],
    ),
    "cancelled and admitted again": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "X", date(2016, 9, 4), "CancRcrd")]),
         (date(2016, 9, 7), [("ISIN1", "MIC1", "X again", date(2016, 9, 7), "NewRcrd")])],
    ),
    "a file between that leaves the instrument alone": (
        [("ISIN1", "MIC1", "X", date(2016, 9, 2))],
        [(date(2016, 9, 4), [("ISIN1", "MIC1", "A", date(2016, 9, 4), "ModfdRcrd")]),
         (date(2016, 9, 5), [("ISIN2", "MIC1", "Y", date(2016, 9, 5), "NewRcrd")]),
         (date(2016, 9, 6), [("ISIN1", "MIC1", "B", date(2016, 9, 6), "ModfdRcrd"),
                             ("ISIN2", "MIC1", "Y2", date(2016, 9, 6), "ModfdRcrd")])],
    ),
    "the worked example": MERGE_CASES["the worked example"],
    "a mixed batch over two files": MERGE_CASES["a mixed batch over two files"],
}


@pytest.mark.parametrize(
    "case", list(BATCH_CASES), ids=[name.replace(" ", "-") for name in BATCH_CASES]
)
def test_a_batch_builds_the_history_its_files_build_in_turn(spark, config, case):
    """Planning several files as one is only worth having if the history it
    leaves is the one the files leave applied one after another."""
    records, files = BATCH_CASES[case]
    full = full_file(spark, records)

    assert versions(
        through_batches(spark, config, full, files, size=len(files))
    ) == versions(through_reference(spark, full, files))


def test_a_file_that_backdates_an_earlier_one_of_its_batch_is_refused(spark, config):
    """Applied in turn, the second file meets the version the first left in
    force and is refused for beginning before it. Planned together, it would
    be chained in front of that version instead, and nothing would fail."""
    full = full_file(spark, [("ISIN1", "MIC1", "X", date(2016, 9, 2))])
    files = [
        (date(2016, 9, 4), [("ISIN1", "MIC1", "A", date(2016, 9, 6), "ModfdRcrd")]),
        (date(2016, 9, 5), [("ISIN1", "MIC1", "B", date(2016, 9, 5), "ModfdRcrd")]),
    ]

    with pytest.raises(ValueError, match="begin before the version in force"):
        through_reference(spark, full, files)
    with pytest.raises(ValueError, match="earlier delta file of the batch"):
        through_batches(spark, config, full, files, size=2)

    # Refused before anything was written.
    built = spark.read.format("delta").load(common.path_silver_main(config))
    assert versions(built) == versions(history.initialise_history(full))


def tags_of(spark, config):
    """The tags of the update's commits, oldest first."""
    return [
        tag for _, tag in reversed(
            silver.tagged_commits(spark, common.ref_silver_main(config))
        )
        if tag and tag.startswith(silver.COMMIT_TAG_PREFIX + ":")
        and not tag.endswith(":init")
    ]


def tag(label, step):
    return silver.commit_tag(label, step)


def test_a_batched_run_builds_the_history_a_daily_run_builds(spark, config):
    days = FORTNIGHT[:5]
    path, files = changes_over(spark, config, days)

    silver.update_from_bronze_to_silver_main(spark, config, batch=2)

    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files)
    )
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-09"
    assert tags_of(spark, config) == [
        tag("2016-09-05..2016-09-06", "merge"), tag("2016-09-05..2016-09-06", "append"),
        tag("2016-09-07..2016-09-08", "merge"), tag("2016-09-07..2016-09-08", "append"),
        tag("2016-09-09", "merge"), tag("2016-09-09", "append"),
    ]


def test_a_batch_ends_before_a_date_whose_file_held_no_records(spark, config):
    """The empty date is passed as it is when the dates are applied in turn,
    and the dates on either side of it are not merged across it."""
    days = [date(2016, 9, 5), date(2016, 9, 7), date(2016, 9, 8)]
    path, files = changes_over(spark, config, days)
    empty_partition(config, date(2016, 9, 6))
    finished(config, "DLTINS_20160906_01of01.zip")

    silver.update_from_bronze_to_silver_main(spark, config, batch=5)

    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files)
    )
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-08"
    assert tags_of(spark, config) == [
        tag("2016-09-05", "merge"), tag("2016-09-05", "append"),
        tag("2016-09-07..2016-09-08", "merge"), tag("2016-09-07..2016-09-08", "append"),
    ]


def test_a_batch_stops_at_an_incomplete_day_with_the_dates_before_it_applied(
    spark, config
):
    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    path = start_history(spark, config, full)
    files = [
        (day, [("ISIN1", "MIC1", f"Name {day.day}", day, "ModfdRcrd")])
        for day in (date(2016, 9, 5), date(2016, 9, 6), date(2016, 9, 7))
    ]
    write_bronze(spark, config, files)
    finished(config, "DLTINS_20160905_01of01.zip", "DLTINS_20160906_01of01.zip",
             "DLTINS_20160907_02of02.zip")

    with pytest.raises(RuntimeError, match="DLTINS_20160907_01of02"):
        silver.update_from_bronze_to_silver_main(spark, config, batch=5)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-06"
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files[:2])
    )


def test_a_batch_that_died_between_merge_and_append_is_resumed_as_that_batch(
    monkeypatch, spark, config
):
    """The merge has closed and replaced versions for every date of the
    batch. Resumed as batches of another size, the first would find no tag of
    its own, plan from the table the merge left and insert again what the
    merge had replaced."""
    days = [date(2016, 9, 5), date(2016, 9, 6), date(2016, 9, 7)]
    path, files = changes_over(spark, config, days)
    fail_once(monkeypatch, history, "records_to_insert")

    with pytest.raises(RuntimeError, match="died"):
        silver.update_from_bronze_to_silver_main(spark, config, batch=3)
    ref = common.ref_silver_main(config)
    assert silver.watermark_from_commits(spark, ref) == "2016-09-04"
    assert silver.span_in_flight(spark, ref, date(2000, 1, 1)) == (days[0], days[-1])

    silver.update_from_bronze_to_silver_main(spark, config, batch=1)

    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files)
    )
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-07"
    assert silver.watermark_from_commits(spark, ref) == "2016-09-07"


def test_a_batch_that_died_before_its_watermark_is_not_applied_twice(
    monkeypatch, spark, config
):
    days = [date(2016, 9, 5), date(2016, 9, 6), date(2016, 9, 7)]
    path, files = changes_over(spark, config, days)
    fail_once(monkeypatch, common, "update_metadata_maxdate")

    with pytest.raises(RuntimeError, match="died"):
        silver.update_from_bronze_to_silver_main(spark, config, batch=3)
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2000-01-01"

    silver.update_from_bronze_to_silver_main(spark, config, batch=2)

    full = full_file(spark, [("ISIN1", "MIC1", "Company X", date(2016, 9, 2))])
    assert versions(spark.read.format("delta").load(path)) == versions(
        through_reference(spark, full, files)
    )
    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2016-09-07"


def test_a_batch_is_compacted_after_where_the_cadence_falls_within_it(
    monkeypatch, spark, config
):
    """The Fridays of the fortnight are the 9th and the 16th, which fall in
    the second and the third batch of four; each is compacted after once."""
    compacted = compaction_log(monkeypatch)
    changes_over(spark, config, FORTNIGHT)

    silver.update_from_bronze_to_silver_main(spark, config, batch=4)

    assert compacted == ["2016-09-12", "2016-09-16"]


@pytest.mark.parametrize("size", [True, False, 0, -1, 2.5, None, "3"])
def test_a_batch_size_that_names_nothing_is_refused(spark, config, size):
    changes_over(spark, config, [date(2016, 9, 6)])

    with pytest.raises(ValueError, match="batch is"):
        silver.update_from_bronze_to_silver_main(spark, config, batch=size)

    assert common.get_metadata_maxdate(spark, config, "MAIN") == "2000-01-01"


# The data contracts
# --------------------------------------------------------------------------


def assert_meets_contract(table, identifier):
    """The table is the table its contract describes: the same columns in
    the same order and of the same types, no empty value where the contract
    says a column is required, and one row per value of the primary key."""
    from fincommons.contracts import load_contract, spark_schema

    contract = load_contract(identifier)
    promised = spark_schema(contract)
    properties = contract["schema"][0]["properties"]

    assert [(f.name, f.dataType) for f in table.schema] == \
        [(f.name, f.dataType) for f in promised]
    for prop in properties:
        if prop.get("required"):
            assert table.filter(func.col(prop["name"]).isNull()).count() == 0, prop["name"]
    key = [p["name"] for p in sorted((p for p in properties if p.get("primaryKey")),
                                     key=lambda p: p["primaryKeyPosition"])]
    assert table.groupBy(*key).count().filter("count > 1").count() == 0
    return key


def test_the_tables_are_the_tables_their_contracts_describe(spark, config):
    """Built from a full file and a delta file that modifies, adds and
    cancels, so that the history holds closed and open versions, records of
    every kind, and an instrument on two venues for the compact table."""
    common.create_directory_tree(config)
    write_full_bronze(spark, config, date(2016, 9, 2))
    finished(config, "FULINS_E_20160902_01of01.zip")
    silver.update_from_bronze_to_silver_main(spark, config)
    day = date(2016, 9, 4)
    write_bronze(spark, config, [(day, [
        ("ISIN1", "MIC1", "Company X", day, "ModfdRcrd"),
        ("ISIN1", "MIC2", "Company X", day, "NewRcrd"),
        ("ISIN2", "MIC1", "Company Y", day, "NewRcrd"),
        ("ISIN3", "MIC1", "Company Z", day, silver.CANCELLATION),
    ])])
    finished(config, "DLTINS_20160904_01of01.zip")
    silver.update_from_bronze_to_silver_main(spark, config)
    silver.update_from_silver_main_to_silver_compact(spark, config)

    main = common.connect_to_firds(spark, config, "main")
    assert main.filter("valid_to IS NOT NULL").count() > 0
    assert assert_meets_contract(main, "fincommons.firds.silver.main") == \
        list(history.KEY_COLUMNS) + [history.VALID_FROM]
    assert assert_meets_contract(common.connect_to_firds(spark, config, "compact"),
                                 "fincommons.firds.silver.compact") == ["isin"]
