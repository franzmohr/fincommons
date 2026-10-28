"""The part of fincommons that other packages build on.

Other packages depend on fincommons, but fincommons cannot see them: they are
not distributed with it. This test is how fincommons knows what it must not
break. It lists every name those packages -- their modules and their tests --
take from fincommons, and checks that each still exists.

The list was generated from the import statements of the packages built on
fincommons when they were split from it, and is kept by hand from then on.
Removing or renaming a name on it breaks a package that depends on fincommons
and calls for a new major version; adding one, when such a package starts to
use a name, is an ordinary change.
"""

from __future__ import annotations

import importlib

import pytest

PUBLIC_API = {
    "fincommons.addressing": [
        "TableRef", "load_paths", "rename_files", "rename_folders_to_snake_case",
        "resolve",
    ],
    "fincommons.columns": [
        "CATALOGUED_MODULES", "ColumnDescription", "SCHEMA_DECLARING_MODULES",
        "column_catalogue", "defined_columns", "describe_column",
    ],
    "fincommons.entitymasterdata": [
        "LEI_OTHER", "connect_to_entitymasterdata",
    ],
    "fincommons.entitymasterdata.common": ["path_silver_main"],
    "fincommons.esef": [
        "connect_to_esef", "connect_to_esef_filings", "update_esef",
    ],
    "fincommons.exchangerate": ["connect_to_exchangerate"],
    "fincommons.exchangerate.common": ["path_silver_exchangerate_table"],
    "fincommons.firds": ["connect_to_firds"],
    "fincommons.firds.common": ["path_silver_compact"],
    "fincommons.frenchdatalibrary": [
        "connect_to_frenchdatalibrary", "series", "update_frenchdatalibrary",
    ],
    "fincommons.layout": ["rebalanced"],
    "fincommons.log": [
        "ContextFormatter", "DEFAULT_DATE_FORMAT", "DEFAULT_FORMAT", "LOGGER_NAME",
        "add_log_level_argument", "configure", "context", "pipeline_run",
    ],
    "fincommons.marketpricedata": ["connect_to_marketpricedata", "curves"],
    "fincommons.marketpricedata.curves": [
        "OVERNIGHT_INSTRUMENT", "add_discount_factor", "build_curves",
        "build_segments", "discount_of", "parallel_shift", "read_pillars",
        "segment_frame", "tenor_in_years",
    ],
    "fincommons.schemas": ["load_pyspark_schema", "load_replacement_table"],
    "fincommons.storage": ["check_local_path"],
}


@pytest.mark.parametrize("module", sorted(PUBLIC_API))
def test_every_name_other_packages_use_exists(module):
    imported = importlib.import_module(module)
    missing = [name for name in PUBLIC_API[module] if not hasattr(imported, name)]
    assert missing == []


def test_the_loaders_read_the_data_of_another_package():
    # A package built on fincommons reads its own schemas through these
    # loaders by naming its package; the parameter is part of what it relies on.
    import inspect

    from fincommons import columns, log, schemas

    assert "package" in inspect.signature(schemas.load_pyspark_schema).parameters
    assert "package" in inspect.signature(schemas.load_replacement_table).parameters
    assert "catalogues" in inspect.signature(columns.column_catalogue).parameters
    assert "logger_name" in inspect.signature(log.configure).parameters
