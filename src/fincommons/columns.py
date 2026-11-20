"""What a column of the data sets holds.

The Silver and Gold tables name their columns in the compressed style of
snake_case -- ``trading_venue``, ``notional_currency`` -- and each
module builds them from its own sources, so what a name means is spread
across the translation code of the modules. :func:`describe_column` answers
it for one name::

    >>> import fincommons
    >>> print(fincommons.describe_column("trading_venue"))
    trading_venue in firds -- tables firds.silver
      ...

A name can mean different things in different data sets -- ``action_type`` is the
action type of a FIRDS record and of a report under a reporting regime, each
read from its own XML path -- so every data set that has the column is
returned, unless ``dataset`` narrows it to one.

Every package shares the mechanism and ships its own catalogue. A package
that builds on this one passes ``catalogues`` naming
both, so that its own ``describe_column`` answers for the data sets of both.

The descriptions are the catalogue shipped as ``data/column_catalogue.csv``:
one row per data set and column, with the tables (the modules' Silver and
Gold stages) that write it, the field of the report it is read from or
"derived" where it is computed, a kind (identifier, code, amount, date, ...)
and a description. A description that could not be established from the
code and the reports alone ends in "(inferred)". :func:`defined_columns`
reads the column names out of the modules' code, and the test suite checks
the two against each other, so a column added to a module without a
catalogue entry fails a test rather than going undocumented.
"""

from __future__ import annotations

import ast
import importlib
import os
from dataclasses import dataclass
from importlib import resources

import pandas as pd

__all__ = ["ColumnDescription", "describe_column", "column_catalogue",
           "defined_columns", "CATALOGUED_MODULES", "CATALOGUES",
           "SNAKE_CASE_MODULES", "NOT_COLUMNS",
           "MODULE_FOLDERS"]

#: The modules whose data sets the catalogue of this package covers.
#: marketpricedata is not among them: its one table has three columns, and
#: what matters there is the identifier in it, see
#: ``marketpricedata.describe_instrument``.
CATALOGUED_MODULES = ("firds", "entitymasterdata", "exchangerate",
                      "famafrenchdata", "esef", "sec", "edinet",
                      "financialreport")

#: Lower-case names a module aliases that are not columns of any table, and
#: that the leading underscore cannot be used to mark. ``src`` and ``tgt`` are
#: the two sides of a Delta MERGE, named by the condition the merge is given,
#: and :func:`_named_column` cannot tell a data frame's alias from a column's.
#: ``checksum`` belongs to the landing register of ``firds``, which is neither
#: a Bronze nor a Silver table and is read with Polars rather than Spark.
NOT_COLUMNS = frozenset({"src", "tgt", "checksum"})

#: The modules whose tables name their columns in snake_case. The tables are
#: being moved to it from the compressed upper case of ISO 20022 one module at
#: a time, and this is how far that has come; see :func:`_is_column`. A module
#: named here has to mark every intermediate column with a leading underscore,
#: since upper case no longer does it.
SNAKE_CASE_MODULES = ("exchangerate", "entitymasterdata", "famafrenchdata",
                      "esef", "sec", "edinet", "financialreport", "firds")

#: The modules that declare their schemas with ``StructType().add`` rather
#: than building their columns with ``withColumn``; see :func:`defined_columns`.
SCHEMA_DECLARING_MODULES = ("exchangerate", "entitymasterdata",
                            "famafrenchdata", "esef", "sec", "edinet",
                            "financialreport")

#: Where a catalogued data set's code lives, for the data sets whose folder
#: is not simply their name. The regimes that require a financial report
#: tagged, and the layer that combines them, sit under one namespace package,
#: while the name of the data set stays what the tables and the configuration
#: file call it: ``esef`` is addressed as ``esef`` and its code is in
#: ``financialreport/esef``. A data set absent from this mapping lives in a
#: folder of its own name.
MODULE_FOLDERS = {
    "esef": "financialreport/esef",
    "sec": "financialreport/sec",
    "edinet": "financialreport/edinet",
    "financialreport": "financialreport/combined",
}

#: The catalogues read by default: the package that ships a catalogue, and
#: the modules it covers.
CATALOGUES = {"fincommons": CATALOGUED_MODULES}


@dataclass
class ColumnDescription:
    """One column of one data set."""

    column: str
    dataset: str
    tables: list[str]
    source: str
    kind: str
    description: str

    def __str__(self) -> str:
        return "\n".join([
            f"{self.column} in {self.dataset} -- tables {', '.join(self.tables)}",
            f"  kind         {self.kind}",
            f"  source       {self.source}",
            f"  description  {self.description}",
        ])


class ColumnDescriptions(list):
    """The descriptions of one column name, one per data set that has it."""

    def __str__(self) -> str:
        return "\n\n".join(str(d) for d in self)


def _modules(catalogues):
    return tuple(m for modules in catalogues.values() for m in modules)


def _root(package):
    """The importable name of ``package``; this one as it was imported.

    The first segment of ``__package__`` and not the whole of it, nor its
    parent: the data directory belongs to the distribution, while the module
    asking for it may sit at any depth below the top -- ``mapping`` is three
    levels down -- and counting levels is how a file that moves stops finding
    its own data.
    """
    return __package__.split(".")[0] if package == "fincommons" else package


def column_catalogue(dataset=None, catalogues=None):
    """The catalogue as a data frame, or the part of it for one data set.

    Columns: ``module``, ``column``, ``tables`` (semicolon-separated),
    ``source``, ``kind``, ``description``.

    ``catalogues`` maps each package whose catalogue is read to the modules
    it covers; by default :data:`CATALOGUES`, i.e. this package alone.
    """
    catalogues = CATALOGUES if catalogues is None else catalogues
    parts = []
    for package in catalogues:
        with resources.files(_root(package)).joinpath("data", "column_catalogue.csv") \
                .open(encoding="utf-8") as handle:
            parts.append(pd.read_csv(handle, dtype=str, keep_default_na=False))
    # In the order of a single catalogue, whichever packages contributed.
    catalogue = pd.concat(parts, ignore_index=True) \
        .sort_values(["module", "column"], kind="stable")
    if dataset is not None:
        dataset = dataset.lower()
        known = _modules(catalogues)
        if dataset not in known:
            raise KeyError(f"{dataset!r} is not a catalogued data set; known: "
                           + ", ".join(known))
        catalogue = catalogue[catalogue["module"] == dataset]
    return catalogue.reset_index(drop=True)


def describe_column(name, dataset=None, catalogues=None):
    """Describe one column of the catalogued data sets.

    Parameters
    ----------
    name: str
        The column name as the tables hold it, e.g. ``"trading_venue"``. Case does
        not matter.
    dataset: str, optional
        The module whose data set is meant, e.g. ``"firds"``. Without it every
        data set that has the column is described.
    catalogues: dict, optional
        The catalogues to look in; see :func:`column_catalogue`.

    Returns
    -------
    ColumnDescriptions
        A list of :class:`ColumnDescription`, one per data set; printable.

    Raises
    ------
    KeyError
        Where no catalogued data set has a column of that name.

    Examples
    --------
    >>> print(describe_column("action_type"))
    >>> describe_column("trading_venue", dataset="firds")[0].source
    """
    # Folded rather than upper-cased: the tables name their columns in
    # snake_case, so a name asked for in any case has to find the row it
    # belongs to, and upper-casing it would find none.
    name = str(name).strip()
    folded = name.casefold()
    catalogue = column_catalogue(dataset, catalogues)
    rows = catalogue[catalogue["column"].str.casefold() == folded]
    if rows.empty:
        where = f"the {dataset} data set" if dataset else "any catalogued data set"
        close = sorted(c for c in set(catalogue["column"])
                       if folded[:4] and c.casefold().startswith(folded[:4]))[:8]
        raise KeyError(f"{name} is not a column of {where}."
                       + (f" Similar: {', '.join(close)}." if close else ""))
    return ColumnDescriptions(
        ColumnDescription(row.column, row.module,
                          [t for t in row.tables.split(";") if t], row.source,
                          row.kind, row.description)
        for row in rows.itertuples())


def _is_column(name, snake_case=False):
    # A leading underscore is how the modules mark a helper column they drop
    # before writing, so it is not a column of any table.
    #
    # Upper case is what marks a column in a module that has not yet been
    # migrated: there, a lower-case name is an intermediate, a data frame
    # alias for a MERGE, or a field of somebody else's metadata, and counting
    # it would hold the catalogue to names no table carries. A migrated
    # module has no upper case left to mark a column with, so there the
    # leading underscore carries the distinction on its own and a lower-case
    # name counts. See :data:`SNAKE_CASE_MODULES`.
    if name.startswith("_") or not name.replace("_", "").isalnum() \
            or not any(c.isalpha() for c in name):
        return False
    if name in NOT_COLUMNS:
        return False
    if snake_case:
        # The module keeps the upper-case names of a Bronze input it reads --
        # the Bronze files of ``famafrenchdata`` are one -- beside the
        # snake_case names of its own tables, so both count.
        return name in (name.upper(), name.lower())
    return name == name.upper()


def defined_columns(catalogues=None, schema_declaring=SCHEMA_DECLARING_MODULES,
                    snake_case=SNAKE_CASE_MODULES):
    """The columns the catalogued modules' code defines, by module.

    Read from the source rather than from data: every ``withColumn``,
    ``withColumnRenamed``, ``alias`` and ``StructField`` that names an
    upper-case column not starting with an underscore, and every
    ``StructType().add`` in the modules that declare their schemas.
    Returns a data frame with ``module``, ``column`` and ``tables``, the
    tables named as ``module.file`` without the extension.

    A column named through a module-level constant counts as named. The
    history of the FIRDS database writes ``valid_from``, ``valid_to`` and
    ``latest_record`` as ``withColumn(VALID_FROM, ...)`` and so on, so that the
    merge condition cannot drift away from the columns it matches on, and
    reading literals alone left the three columns the historical database
    exists for out of the catalogue -- and out of what this function can hold
    the catalogue to.

    ``snake_case`` names the modules whose tables have been converted to
    snake_case, :data:`SNAKE_CASE_MODULES` by default. A package that builds
    on this one passes its own, since how far its modules have been converted
    is its own business and not this package's.

    ``catalogues`` names the packages and modules to read, as for
    :func:`column_catalogue`; ``schema_declaring`` the modules whose
    ``StructType().add`` calls count as defining a column. Where a data set's
    code does not live in a folder of its own name, :data:`MODULE_FOLDERS`
    says where it does, and the table is named by the path rather than by the
    data set: a column of ``esef`` is written in ``financialreport.esef.common``.
    """
    catalogues = CATALOGUES if catalogues is None else catalogues
    snake_case = frozenset(snake_case or ())
    found = {}
    for owner, module in [(p, m) for p, ms in catalogues.items() for m in ms]:
        package = os.path.dirname(os.path.abspath(
            importlib.import_module(_root(owner)).__file__))
        folder = os.path.join(package, *MODULE_FOLDERS.get(module, module)
                              .split("/"))
        for directory, _, files in os.walk(folder):
            for file in sorted(files):
                if not file.endswith(".py"):
                    continue
                path = os.path.join(directory, file)
                table = os.path.relpath(path, package)[:-3].replace(os.sep, ".")
                with open(path, encoding="utf-8") as handle:
                    tree = ast.parse(handle.read())
                constants = _module_constants(tree)
                for node in ast.walk(tree):
                    name = _named_column(node, module, schema_declaring,
                                         constants, snake_case)
                    if name:
                        tables = found.setdefault((module, name), [])
                        if table not in tables:
                            tables.append(table)
    return pd.DataFrame(
        [(m, c, ";".join(t)) for (m, c), t in sorted(found.items())],
        columns=["module", "column", "tables"])


def _module_constants(tree):
    """The module-level ``NAME = "text"`` assignments of a parsed file."""
    found = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target, value = node.targets[0], node.value
        if (isinstance(target, ast.Name)
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)):
            found[target.id] = value.value
    return found


def _named_column(node, module, schema_declaring=SCHEMA_DECLARING_MODULES,
                  constants=None, snake_case=SNAKE_CASE_MODULES):
    """The column a call names, if it is one that defines a column.

    ``constants`` maps the module-level string constants of the file to their
    values, so that a column named through one of them counts as named; see
    :func:`defined_columns`.
    """
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    attr, args = node.func.attr, node.args

    def text(arg):
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
        if isinstance(arg, ast.Name):
            return (constants or {}).get(arg.id)
        return None

    if attr == "withColumn" and len(args) >= 2:
        name = text(args[0])
    elif attr == "withColumnRenamed" and len(args) >= 2:
        name = text(args[1])
    elif attr == "alias" and len(args) == 1:
        name = text(args[0])
    elif attr == "StructField" and len(args) >= 2:
        name = text(args[0])
    elif attr == "add" and len(args) >= 2 and module in schema_declaring:
        name = text(args[0])
    else:
        return None
    return (name if name and _is_column(name, module in snake_case)
            else None)
