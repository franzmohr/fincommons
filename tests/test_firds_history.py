"""Tests of the FIRDS historical database.

The tests check the implementation of section 4.2 of ESMA65-8-5014 rev 3
against the worked example the same document gives in section 4.1, and against
each of the cases the algorithm distinguishes.
"""

from __future__ import annotations

from datetime import date

import pytest

from pyspark.sql import functions as F

from fincommons.firds.history import (
    apply_changes,
    initialise_history,
    latest_records,
    plan_changes,
    records_as_of,
)

#: The columns a record of the historical database carries in these tests. The
#: real table carries the whole analytics schema; the algorithm only touches
#: the key, the validity dates and the flags.
COLUMNS = "ISIN string, TRADGVN string, FULLNAME string, TECH_PBLCTNPRD_FRDT string"


def records(spark, rows):
    """Build records in the analytics schema from (isin, mic, name, frdt)."""
    return spark.createDataFrame(rows, COLUMNS)


def delta(spark, rows):
    """Build delta records from (isin, mic, name, frdt, action)."""
    return spark.createDataFrame(
        rows, COLUMNS + ", ACTNTP string"
    )


def history_of(history, isin="ISIN1", mic="MIC1"):
    """Return the versions of one instrument, oldest first."""
    return (
        history.where((F.col("ISIN") == isin) & (F.col("TRADGVN") == mic))
        .orderBy("VLDFROM")
        .collect()
    )


# --------------------------------------------------------------------------
# The full file
# --------------------------------------------------------------------------


def test_initialisation_registers_the_full_file(spark):
    full = records(spark, [("ISIN1", "MIC1", "Company X", "2016-09-02")])
    history = initialise_history(full).collect()
    assert len(history) == 1
    assert history[0]["VLDFROM"] == date(2016, 9, 2)
    assert history[0]["VLDTO"] is None
    assert history[0]["LATESTRECORD"] is True
    assert history[0]["ACTNTP"] == "FULINS"


# --------------------------------------------------------------------------
# The worked example of section 4.1
# --------------------------------------------------------------------------


def test_the_worked_example_of_the_instructions(spark):
    """Reproduce the table of section 4.1 of ESMA65-8-5014.

    The document tracks one (ISIN, MIC) through four versions and states, for
    each of them, the period during which it is the version in force. The
    versions are received on four dates and published from 02/09, 04/09, 08/11
    and 02/12 respectively, so the first three have to be closed on 03/09,
    07/11 and 01/12.
    """
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "Company X", "2016-09-02")])
    )
    for name, frdt in [
        ("Company X", "2016-09-04"),
        ("X Corporation", "2016-11-08"),
        ("X Corporation", "2016-12-02"),
    ]:
        history = apply_changes(
            history,
            delta(spark, [("ISIN1", "MIC1", name, frdt, "ModfdRcrd")]),
        )

    versions = history_of(history)
    assert [r["VLDFROM"] for r in versions] == [
        date(2016, 9, 2),
        date(2016, 9, 4),
        date(2016, 11, 8),
        date(2016, 12, 2),
    ]
    assert [r["VLDTO"] for r in versions] == [
        date(2016, 9, 3),
        date(2016, 11, 7),
        date(2016, 12, 1),
        None,
    ]
    assert [r["LATESTRECORD"] for r in versions] == [False, False, False, True]
    assert [r["FULLNAME"] for r in versions] == [
        "Company X",
        "Company X",
        "X Corporation",
        "X Corporation",
    ]


# --------------------------------------------------------------------------
# The individual record types
# --------------------------------------------------------------------------


def test_new_records_are_inserted(spark):
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history, delta(spark, [("ISIN2", "MIC1", "B", "2016-09-03", "NewRcrd")])
    )
    assert updated.count() == 2
    added = history_of(updated, isin="ISIN2")
    assert added[0]["VLDTO"] is None
    assert added[0]["LATESTRECORD"] is True


def test_cancelled_records_close_the_version_in_force(spark):
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history, delta(spark, [("ISIN1", "MIC1", "A", "2016-09-10", "CancRcrd")])
    )
    versions = history_of(updated)
    assert [r["VLDTO"] for r in versions] == [date(2016, 9, 9), None]
    assert [r["LATESTRECORD"] for r in versions] == [False, True]
    assert versions[1]["ACTNTP"] == "CancRcrd"


def test_a_termination_reported_late_is_inserted(spark):
    """Paragraph 27.b.iii.2: no version of the instrument is in force."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history,
        delta(spark, [("ISIN9", "MIC1", "Z", "2016-09-05", "TermntdRcrd")]),
    )
    added = history_of(updated, isin="ISIN9")
    assert len(added) == 1
    assert added[0]["VLDTO"] is None
    assert added[0]["LATESTRECORD"] is True
    # the unrelated instrument is untouched
    assert history_of(updated)[0]["LATESTRECORD"] is True


def test_a_correction_after_termination_closes_and_inserts(spark):
    """Paragraph 27.b.iii.3: the validity dates differ."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history,
        delta(spark, [("ISIN1", "MIC1", "A corrected", "2016-09-20", "TermntdRcrd")]),
    )
    versions = history_of(updated)
    assert len(versions) == 2
    assert [r["VLDTO"] for r in versions] == [date(2016, 9, 19), None]
    assert [r["LATESTRECORD"] for r in versions] == [False, True]


def test_an_ordinary_termination_replaces_the_version_in_force(spark):
    """Paragraph 27.b.iii.4: the validity dates are equal."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history,
        delta(spark, [("ISIN1", "MIC1", "A terminated", "2016-09-02", "TermntdRcrd")]),
    )
    versions = history_of(updated)
    assert len(versions) == 1, "the version in force is replaced, not added to"
    assert versions[0]["FULLNAME"] == "A terminated"
    assert versions[0]["ACTNTP"] == "TermntdRcrd"
    assert versions[0]["VLDTO"] is None
    assert versions[0]["LATESTRECORD"] is True


# --------------------------------------------------------------------------
# Several changes in one file
# --------------------------------------------------------------------------


def test_several_changes_for_one_instrument_in_one_file_are_chained(spark):
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history,
        delta(
            spark,
            [
                ("ISIN1", "MIC1", "B", "2016-09-05", "ModfdRcrd"),
                ("ISIN1", "MIC1", "C", "2016-09-08", "ModfdRcrd"),
            ],
        ),
    )
    versions = history_of(updated)
    assert [r["VLDFROM"] for r in versions] == [
        date(2016, 9, 2),
        date(2016, 9, 5),
        date(2016, 9, 8),
    ]
    assert [r["VLDTO"] for r in versions] == [
        date(2016, 9, 4),
        date(2016, 9, 7),
        None,
    ]
    assert [r["LATESTRECORD"] for r in versions] == [False, False, True]


def test_validity_periods_do_not_overlap(spark):
    """Whatever the sequence of changes, one version is in force at a time."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    for name, frdt, action in [
        ("B", "2016-09-05", "ModfdRcrd"),
        ("C", "2016-09-09", "CancRcrd"),
        ("D", "2016-09-20", "TermntdRcrd"),
    ]:
        history = apply_changes(
            history, delta(spark, [("ISIN1", "MIC1", name, frdt, action)])
        )
    versions = history_of(history)
    assert sum(1 for r in versions if r["VLDTO"] is None) == 1
    assert sum(1 for r in versions if r["LATESTRECORD"]) == 1
    for earlier, later in zip(versions, versions[1:]):
        assert earlier["VLDTO"] < later["VLDFROM"]


# --------------------------------------------------------------------------
# Querying, section 4.3
# --------------------------------------------------------------------------


def test_point_in_time_query(spark):
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    history = apply_changes(
        history, delta(spark, [("ISIN1", "MIC1", "B", "2016-09-05", "ModfdRcrd")])
    )
    assert [r["FULLNAME"] for r in latest_records(history).collect()] == ["B"]
    as_of = records_as_of(history, "2016-09-03").collect()
    assert [r["FULLNAME"] for r in as_of] == ["A"]
    assert records_as_of(history, "2016-09-01").count() == 0


def test_the_plan_classifies_every_record(spark):
    """What a record does is decided by the version in force, not by the
    element it was reported in: ISIN1 is terminated on the day its version
    began and replaces it, ISIN4 is modified and closes the version that began
    earlier, and ISIN2 and ISIN3 have nothing in force to supersede."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02"),
                        ("ISIN4", "MIC1", "D", "2016-09-01")])
    )
    planned = plan_changes(
        history,
        delta(
            spark,
            [
                ("ISIN1", "MIC1", "A", "2016-09-02", "TermntdRcrd"),
                ("ISIN2", "MIC1", "B", "2016-09-02", "NewRcrd"),
                ("ISIN3", "MIC1", "C", "2016-09-02", "ModfdRcrd"),
                ("ISIN4", "MIC1", "D", "2016-09-02", "ModfdRcrd"),
            ],
        ),
    )
    operations = {
        r["ISIN"]: r["_OPERATION"] for r in planned.select("ISIN", "_OPERATION").collect()
    }
    assert operations == {
        "ISIN1": "REPLACE",
        "ISIN2": "INSERT",
        "ISIN3": "INSERT",
        "ISIN4": "CLOSE_AND_INSERT",
    }


# --------------------------------------------------------------------------
# Records that begin on the same day
# --------------------------------------------------------------------------


def assert_no_empty_period(history):
    assert history.where(F.col("VLDTO") < F.col("VLDFROM")).count() == 0


def test_two_records_beginning_on_the_same_day_leave_one_version(spark):
    """Chaining them would close the first on the day before it begins."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history,
        delta(
            spark,
            [
                ("ISIN2", "MIC1", "new", "2016-09-05", "NewRcrd"),
                ("ISIN2", "MIC1", "modified", "2016-09-05", "ModfdRcrd"),
            ],
        ),
    )
    assert_no_empty_period(updated)
    added = history_of(updated, isin="ISIN2")
    assert [r["FULLNAME"] for r in added] == ["modified"]
    assert added[0]["VLDTO"] is None
    assert added[0]["LATESTRECORD"] is True


def test_a_record_delivered_twice_is_applied_once(spark):
    """This is what a delta file imported twice into the Bronze layer looks like."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    change = ("ISIN1", "MIC1", "B", "2016-09-05", "ModfdRcrd")
    updated = apply_changes(history, delta(spark, [change, change]))

    assert_no_empty_period(updated)
    versions = history_of(updated)
    assert [r["FULLNAME"] for r in versions] == ["A", "B"]
    assert [r["VLDTO"] for r in versions] == [date(2016, 9, 4), None]


@pytest.mark.parametrize("action", ["ModfdRcrd", "CancRcrd"])
def test_a_change_beginning_with_the_version_in_force_replaces_it(spark, action):
    """Closing the version in force on the day before it began would leave a
    version valid for no day at all."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history, delta(spark, [("ISIN1", "MIC1", "A2", "2016-09-02", action)])
    )

    assert_no_empty_period(updated)
    versions = history_of(updated)
    assert len(versions) == 1
    assert versions[0]["FULLNAME"] == "A2"
    assert versions[0]["ACTNTP"] == action
    assert versions[0]["VLDTO"] is None
    assert versions[0]["LATESTRECORD"] is True


# An instrument reported as new although it is already there
# --------------------------------------------------------------------------


def test_a_new_record_for_a_live_instrument_supersedes_it(spark):
    """Adding it beside the version in force would give the instrument two,
    which makes LATESTRECORD ambiguous and matches the merge of the next delta
    file against both rows at once."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    updated = apply_changes(
        history, delta(spark, [("ISIN1", "MIC1", "again", "2016-09-05", "NewRcrd")])
    )

    assert_no_empty_period(updated)
    versions = history_of(updated)
    assert [r["VLDTO"] for r in versions] == [date(2016, 9, 4), None]
    assert [r["LATESTRECORD"] for r in versions] == [False, True]
    assert sum(1 for r in versions if r["VLDTO"] is None) == 1


def test_an_instrument_admitted_again_after_termination_has_one_version_in_force(spark):
    """A termination carries no VLDTO, so it stays in force; ESMA reporting the
    instrument as new again must not add a second version in force beside it."""
    history = initialise_history(
        records(spark, [("ISIN1", "MIC1", "A", "2016-09-02")])
    )
    history = apply_changes(
        history, delta(spark, [("ISIN1", "MIC1", "gone", "2016-09-02", "TermntdRcrd")])
    )
    history = apply_changes(
        history, delta(spark, [("ISIN1", "MIC1", "back", "2016-09-20", "NewRcrd")])
    )

    assert_no_empty_period(history)
    versions = history_of(history)
    assert [r["FULLNAME"] for r in versions] == ["gone", "back"]
    assert sum(1 for r in versions if r["VLDTO"] is None) == 1
    assert sum(1 for r in versions if r["LATESTRECORD"]) == 1
