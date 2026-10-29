"""Tests of the shared parts of the FIRDS module.

The paths of the data lake and the entry point that opens the Silver tables
live in :mod:`fincommons.firds.common`, and the other modules of the package
reach the data lake through it. The tests below cover that layer, and one of
them checks the package against it: every path helper the other modules call
has to exist, which is the kind of mistake that otherwise only shows up on the
one branch that calls it.
"""

from __future__ import annotations

import ast
import os
import pathlib

import pytest

from fincommons.firds import bronze, common, landing, metadata, silver, utilities


@pytest.fixture
def config(tmp_path):
    """A configuration file pointing at an empty data lake."""
    import yaml

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"firds": {name: str(base / name)
                       for name in ("metadata", "landing", "bronze", "silver")}}
        )
    )
    return str(config_path)


# The layout of the data lake
# --------------------------------------------------------------------------


def test_the_directory_tree_is_created(config):
    common.create_directory_tree(config)

    for folder in (
        common.path_landing_error(config),
        common.path_landing_finished(config),
        common.path_landing_import(config),
        common.path_bronze_draft13auth_017_001_01(config),
        common.path_bronze_auth_017_001_02(config),
        common.path_bronze_draft6auth_036_001_01(config),
        common.path_bronze_auth_036_001_02(config),
        common.path_bronze_auth_036_001_03(config),
        common.path_bronze_auth_102_001_01(config),
        common.path_silver_main(config),
        common.path_silver_compact(config),
    ):
        assert os.path.isdir(folder)


def test_the_tree_is_created_below_parents_that_do_not_exist(config):
    """The configuration puts the four layers below a directory that has not
    been created yet, and setting the tree up should not require creating the
    path to it by hand first."""
    common.create_directory_tree(config)

    assert os.path.isdir(common.path_landing_import(config))


def test_creating_the_tree_twice_is_harmless(config):
    common.create_directory_tree(config)
    common.create_directory_tree(config)

    assert os.path.isdir(common.path_landing_import(config))


# Opening the Silver tables
# --------------------------------------------------------------------------


def test_an_unknown_table_is_rejected(config):
    """Returning None instead would surface as an AttributeError somewhere
    else, far from the call that misspelled the name."""
    with pytest.raises(ValueError, match="not recognised"):
        common.connect_to_firds(None, config, "man")


def test_the_error_names_the_tables_that_do_exist(config):
    with pytest.raises(ValueError, match="main"):
        common.connect_to_firds(None, config, "")


# The package against the paths it uses
# --------------------------------------------------------------------------


def attributes_of_common_used_by(module):
    """Return the names the module reads off the ``common`` module."""
    tree = ast.parse(pathlib.Path(module.__file__).read_text(encoding="utf-8"))
    return {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "common"
    }


@pytest.mark.parametrize("module", [landing, metadata, silver, utilities])
def test_every_helper_the_module_calls_on_common_exists(module):
    """A path helper that does not exist raises only on the branch that reaches
    it, which can be a branch that runs once a year."""
    missing = sorted(
        name for name in attributes_of_common_used_by(module)
        if not hasattr(common, name)
    )

    assert missing == []


# What the package offers
# --------------------------------------------------------------------------


#: The names the README puts in front of a caller. They reach the package
#: through the star imports of its __init__, which is what __all__ governs.
DOCUMENTED = [
    "create_directory_tree", "connect_to_firds", "update_landing_import",
    "update_from_landing_to_bronze", "update_from_bronze_to_silver_main",
    "update_from_silver_main_to_silver_compact", "declare_silver_main_layout",
    "read_metadata", "get_metadata_maxdate", "update_metadata_maxdate",
    "write_silver", "file_format_on_disk", "SILVER_FORMATS", "history",
]


@pytest.mark.parametrize("name", DOCUMENTED)
def test_the_package_offers_what_is_documented(name):
    import fincommons.firds as firds

    assert hasattr(firds, name)


def test_the_package_does_not_offer_what_its_modules_import():
    """The modules are re-exported with star imports, which without __all__
    also carry everything they imported into the package."""
    import fincommons.firds as firds

    borrowed = [
        name for name in
        ("os", "yaml", "requests", "pl", "shutil", "func", "zipfile", "ntpath",
         "json", "hashlib", "re", "functools", "itertools", "logging", "types")
        if hasattr(firds, name)
    ]

    assert borrowed == []
