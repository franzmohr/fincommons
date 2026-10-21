"""Tests of the licensing of fincommons.

fincommons is open source under the EUPL 1.2 as a whole, and other packages
build on it, some of which are not distributed openly. fincommons must never
import such a package: if it did, it could not be published or used on its
own, and anyone receiving it under the EUPL would get code they cannot run
without code they may not have. The check therefore admits only the standard
library, fincommons itself and the open source packages it declares as
dependencies. It reads the import statements rather than running them, so it
costs nothing and needs no Spark.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PACKAGE = ROOT / "src" / "fincommons"


def python_files():
    files = [p for p in PACKAGE.rglob("*.py") if "__pycache__" not in p.parts]
    return files + sorted((ROOT / "tests").glob("*.py"))


def imported_modules(path):
    """The absolute module names the file at ``path`` imports."""
    found = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            found.add(node.module)
    return found


def test_the_licence_is_present():
    assert (ROOT / "LICENSES" / "EUPL-1.2.txt").is_file()
    assert "EUROPEAN UNION PUBLIC LICENCE v. 1.2" in (
        ROOT / "LICENSES" / "EUPL-1.2.txt").read_text(encoding="utf-8")


# The top-level modules of the open source packages fincommons may import, by
# the name they are imported under. A new dependency is declared in
# pyproject.toml and added here; anything else is refused.
OPEN_DEPENDENCIES = {
    "delta",      # delta-spark
    "numpy",
    "openpyxl",
    "pandas",
    "polars",
    "pyarrow",
    "pyspark",
    "pytest",     # the test suite and fincommons.testing
    "requests",
    "yaml",       # pyyaml
    "yfinance",
}


def test_fincommons_imports_only_open_dependencies():
    allowed = set(sys.stdlib_module_names) | OPEN_DEPENDENCIES | {"fincommons"}
    violations = {
        path.relative_to(ROOT).as_posix(): sorted(
            m for m in imported_modules(path) if m.split(".")[0] not in allowed
        )
        for path in python_files()
    }
    assert {path: found for path, found in violations.items() if found} == {}


def test_the_import_check_sees_imports():
    # The check above passes vacuously if it finds no imports at all.
    assert "pyspark.sql.functions" in imported_modules(PACKAGE / "firds" / "silver.py")


def test_the_import_check_refuses_an_undeclared_package(tmp_path):
    # The allow list above passes vacuously if every name is admitted.
    module = tmp_path / "module.py"
    module.write_text("import closed_package.pipelines\n", encoding="utf-8")
    allowed = set(sys.stdlib_module_names) | OPEN_DEPENDENCIES | {"fincommons"}
    assert [m for m in imported_modules(module) if m.split(".")[0] not in allowed] \
        == ["closed_package.pipelines"]
