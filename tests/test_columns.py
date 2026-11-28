"""The column catalogue and describe_column.

The first test is the one that keeps the catalogue honest: it reads the
column names out of the modules' code and checks the catalogue has each of
them, and nothing else, so a column added without a description fails here.
"""

from __future__ import annotations

import pytest

import fincommons
from fincommons.columns import (CATALOGUED_MODULES, OUTPUT_COLUMNS,
                                 column_catalogue, defined_columns,
                                 describe_column)
from fincommons.contracts import catalogue_rows

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
    contracted = {(module, column) for module, column, *_ in catalogue_rows()}
    listed = defined[[key not in contracted for key in defined.index]]

    assert (catalogue.reindex(listed.index) == listed).all()


def test_a_contract_names_the_stage_that_writes_its_columns():
    # A contract describes a table, not the files of the code, so it names
    # the one stage that writes the table rather than every file that names
    # the column -- the schema of the proprietary file declares the columns of
    # the exchange rates as well. That stage has to be among the files, or a
    # file of rules the stage applies: FIRDS keeps the rules of its history,
    # and with them the names of the version columns, in firds.history, and
    # GLEIF those and the columns it reads in gleif.history and gleif.common.
    # ESEF, SEC, EDINET, STS and Fama/French declare the schemas of their tables in their common
    # modules and, for their issues, in their quality modules; the combined
    # financial reports declare theirs in financialreport.combined.common.
    applied_by = {"firds.silver": {"firds.silver", "firds.history"},
                  "gleif.silver": {"gleif.silver", "gleif.history", "gleif.common"},
                  "esef.silver": {"financialreport.esef.common",
                                  "financialreport.esef.quality"},
                  "sec.silver": {"financialreport.sec.common",
                                 "financialreport.sec.quality"},
                  "edinet.silver": {"financialreport.edinet.common",
                                    "financialreport.edinet.quality"},
                  "sts.silver": {"sts.common"},
                  "famafrenchdata.silver": {"famafrenchdata.common"},
                  "financialreport.silver": {"financialreport.combined.common",
                                             "financialreport.combined.silver"}}
    defined = defined_columns().set_index(["module", "column"])["tables"]
    for module, column, stage, *_ in catalogue_rows():
        if module not in CATALOGUED_MODULES:
            continue  # marketpricedata: contracted, not catalogued
        writers = applied_by.get(stage, {stage})
        assert writers & set(defined[(module, column)].split(";")), (module, column)


def test_every_entry_is_described_with_a_known_kind():
    catalogue = column_catalogue()

    assert (catalogue["description"].str.len() > 10).all()
    assert set(catalogue["kind"]) <= KINDS
    assert (catalogue["source"] != "").all()
    assert not catalogue.duplicated(["module", "column"]).any()


def test_a_column_is_described_for_the_data_sets_of_this_package_alone():
    described = describe_column("TRADING_VENUE")  # case does not matter

    # Data sets of packages built on fincommons may carry the column too, but
    # those packages ship their own catalogue; fincommons describes its own.
    assert [d.dataset for d in described] == ["firds"]
    firds = described[0]
    assert firds.column == "trading_venue"
    assert firds.tables == ["firds.silver"]
    assert "TradgVnRltdAttrbts.Id" in firds.source


def test_a_data_set_narrows_the_answer_to_one():
    described = describe_column("action_type", dataset="FIRDS")

    assert len(described) == 1
    assert described[0].dataset == "firds"


def test_the_description_prints_as_a_record():
    text = str(fincommons.describe_column("trading_venue", dataset="firds"))

    assert text.splitlines()[0] == "trading_venue in firds -- tables firds.silver"
    assert "  description  " in text


def test_an_unknown_column_is_an_error_that_suggests_close_names():
    with pytest.raises(KeyError, match="Similar: .*trading_venue"):
        describe_column("trading_venuex")
    with pytest.raises(KeyError, match="not a catalogued data set"):
        column_catalogue("marketpricedata")
    with pytest.raises(KeyError, match="not a catalogued data set"):
        column_catalogue("emir")


def test_the_catalogue_covers_the_catalogued_modules():
    assert set(column_catalogue()["module"]) == set(CATALOGUED_MODULES)


def test_a_column_a_function_returns_is_described_by_the_function():
    import importlib

    every = defined_columns(outputs={})
    defined = set(zip(every["module"], every["column"]))
    for (module, column), function in OUTPUT_COLUMNS.items():
        assert (module, column) in defined, (module, column)
        path, name = function.rsplit(".", 1)
        doc = getattr(importlib.import_module(path), name).__doc__
        assert f"``{column}``" in doc, function


def test_the_package_ships_no_file_of_descriptions():
    from importlib import resources

    assert not resources.files("fincommons").joinpath(
        "data", "column_catalogue.csv").is_file()


def test_a_bronze_column_in_upper_case_is_not_held_to_a_contract():
    defined = defined_columns()
    assert not defined[defined["module"] == "firds"]["column"].eq("DATADT").any()
    everything = defined_columns(contracted=())
    assert everything[everything["module"] == "firds"]["column"].eq("DATADT").any()


def test_a_column_is_found_however_its_struct_field_is_named(tmp_path):
    """``T.StructField``, a ``StructField`` imported by name, one written with
    keywords, and one named through a constant of another module of the
    package all define a column."""
    import ast

    from fincommons.columns import _module_constants, _named_column

    package = tmp_path / "pkg"
    (package / "mod").mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "mod" / "__init__.py").write_text("")
    (package / "mod" / "common.py").write_text('VALID_FROM = "valid_from"\n')
    source = package / "mod" / "stage.py"
    source.write_text(
        "from pyspark.sql.types import StructField, StringType\n"
        "import pyspark.sql.types as T\n"
        "from .common import VALID_FROM\n"
        "from pkg.mod.common import VALID_FROM as SINCE\n"
        "a = T.StructField('by_attribute', T.StringType())\n"
        "b = StructField('by_name', StringType())\n"
        "c = StructField(name='by_keyword', dataType=StringType())\n"
        "d = StructField(VALID_FROM, StringType())\n"
        "e = StructField(SINCE, StringType())\n")
    tree = ast.parse(source.read_text())
    constants = _module_constants(tree, str(source), str(package), "pkg")

    found = {name for node in ast.walk(tree)
             if (name := _named_column(node, "mod", (), constants, {"mod"}))}

    assert found == {"by_attribute", "by_name", "by_keyword", "valid_from"}
