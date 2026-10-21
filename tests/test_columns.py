"""The column catalogue and describe_column.

The first test is the one that keeps the catalogue honest: it reads the
column names out of the modules' code and checks the catalogue has each of
them, and nothing else, so a column added without a description fails here.
"""

from __future__ import annotations

import pytest

import fincommons
from fincommons.columns import (CATALOGUED_MODULES, column_catalogue,
                                 defined_columns, describe_column)

KINDS = {"identifier", "code", "text", "date", "datetime", "amount", "currency",
         "rate", "quantity", "flag", "count", "weight", "other"}


def test_every_column_the_code_defines_is_catalogued_and_no_other():
    defined = defined_columns()
    catalogue = column_catalogue()

    in_code = set(zip(defined["module"], defined["column"]))
    in_catalogue = set(zip(catalogue["module"], catalogue["column"]))
    assert not in_code - in_catalogue, \
        f"columns without a catalogue entry: {sorted(in_code - in_catalogue)[:20]}"
    assert not in_catalogue - in_code, \
        f"catalogue entries no module defines: {sorted(in_catalogue - in_code)[:20]}"


def test_the_catalogue_names_the_tables_the_code_writes_a_column_in():
    defined = defined_columns().set_index(["module", "column"])["tables"]
    catalogue = column_catalogue().set_index(["module", "column"])["tables"]

    assert (catalogue.reindex(defined.index) == defined).all()


def test_every_entry_is_described_with_a_known_kind():
    catalogue = column_catalogue()

    assert (catalogue["description"].str.len() > 10).all()
    assert set(catalogue["kind"]) <= KINDS
    assert (catalogue["source"] != "").all()
    assert not catalogue.duplicated(["module", "column"]).any()


def test_a_column_is_described_for_the_data_sets_of_this_package_alone():
    described = describe_column("tradgvn")  # case does not matter

    # Data sets of packages built on fincommons may carry the column too, but
    # those packages ship their own catalogue; fincommons describes its own.
    assert [d.dataset for d in described] == ["firds"]
    firds = described[0]
    assert firds.column == "TRADGVN"
    assert firds.tables == ["firds.silver"]
    assert "TradgVnRltdAttrbts.Id" in firds.source


def test_a_data_set_narrows_the_answer_to_one():
    described = describe_column("ACTNTP", dataset="FIRDS")

    assert len(described) == 1
    assert described[0].dataset == "firds"


def test_the_description_prints_as_a_record():
    text = str(fincommons.describe_column("TRADGVN", dataset="firds"))

    assert text.splitlines()[0] == "TRADGVN in firds -- tables firds.silver"
    assert "  description  " in text


def test_an_unknown_column_is_an_error_that_suggests_close_names():
    with pytest.raises(KeyError, match="Similar: .*TRADGVN"):
        describe_column("TRADGVNX")
    with pytest.raises(KeyError, match="not a catalogued data set"):
        column_catalogue("marketpricedata")
    with pytest.raises(KeyError, match="not a catalogued data set"):
        column_catalogue("emir")


def test_the_catalogue_covers_the_catalogued_modules():
    assert set(column_catalogue()["module"]) == set(CATALOGUED_MODULES)
