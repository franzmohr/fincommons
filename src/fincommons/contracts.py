"""Data contracts: what a table of the data lake promises its readers.

A contract states, in a form a machine can read, what a Silver or Gold table
holds and what a consumer of it may rely on: the name, type and meaning of
every column, which columns identify a row, and how the values of a column
may be combined. The framework writes its contracts in the Open Data Contract
Standard (ODCS) and describes each module as a data product in the Open Data
Product Standard (ODPS), whose output ports name the contracts of the module's
tables. Both standards are pinned, :data:`ODCS_VERSION` and
:data:`ODPS_VERSION`, and their JSON schemas ship in the data directory, so
that :func:`validate_contract` and :func:`validate_product` hold a file to the
standard and not to whatever it happens to state. The handbook describes the
design in the section on data contracts and the semantic layer.

A contract is the single source of truth
-----------------------------------------

For a table that has a contract, the contract is where its columns are
described, and nothing else describes them. :func:`catalogue_rows` turns the
properties of the contracts into rows of the shape of the column catalogue,
which :func:`fincommons.columns.column_catalogue` adds to the rows of the
modules that have no contract yet. The test suite holds the contracts against
the code in both directions, as it holds the catalogue: a column a module
writes must be a property of its contract, and a property must be a column
the module writes. A column a module computes and drops again before writing
is not in any contract, because a contract states what is written, and the
module marks it with a leading underscore.

Templates, not deployments
--------------------------

The contracts that ship with a package are the same wherever the pipeline
runs, so they state nothing that belongs to a deployment: no server, no
service level, no team, no support channel, no price. Where a table lives is
stated by the configuration file, as for every other table.
:func:`validate_contract` refuses a shipped contract that names any of them.

What the standard leaves out
----------------------------

ODCS marks a property as a dimension or a measure, but has no words for how a
measure may be aggregated beyond one expression. The framework adds them as
custom properties whose ``vendor`` is ``fincommons``; :data:`CUSTOM_PROPERTIES`
lists them and :func:`validate_contract` checks their values. ``kind`` and
``source`` carry what the column catalogue has always said about a column:
its kind (identifier, code, amount, ...) and whether it was declared or
derived.

Where the files are
-------------------

A package ships its contracts in its own ``data`` directory, one file per
table, named ``contract_<module>_<layer>_<table>.yaml``, and one product per
module, named ``product_<module>.yaml``. The identifier of a contract is
``<package>.<module>.<layer>.<table>``, the name of the logger of the module
followed by the table, so that a reader can find the code from the contract.
A package that builds on this one passes ``packages`` naming both, as it does
for the catalogue of data quality checks.
"""

from __future__ import annotations

import copy
import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources

import yaml

__all__ = ["ODCS_VERSION", "ODPS_VERSION", "VENDOR", "CUSTOM_PROPERTIES",
           "AGGREGATIONS", "DEPLOYMENT_SECTIONS", "DRAFT_STATUSES", "ContractError",
           "contracts", "products", "load_contract", "validate_contract",
           "validate_product", "custom_property", "catalogue_rows",
           "spark_schema", "QUALITY_SEVERITIES", "QualityResult",
           "check_quality", "quality_failures"]

#: The version of ODCS the contracts are written against. A new version of
#: the standard is adopted by changing this, the shipped JSON schema and the
#: contracts together, not by whatever a contract states.
ODCS_VERSION = "v3.2.0"

#: The version of ODPS the products are written against.
ODPS_VERSION = "v1.1.0"

#: The ``vendor`` of the custom properties the framework defines.
VENDOR = "fincommons"

#: How a measure may be aggregated. ``additive`` may be summed over every
#: dimension. ``semiAdditive`` may be summed over every dimension but time:
#: over a period it is read at the last reference date, which
#: ``timeProperty`` names -- the outstanding notional of a portfolio, the
#: total assets of a balance sheet. ``nonAdditive`` may not be summed at all
#: and is only averaged, and only within the groups that ``aggregateWithin``
#: names -- an exchange rate, within one currency pair. An additive or
#: semi-additive measure may name aggregateWithin too, where it may be
#: summed only within one value of those columns -- a percentage of the
#: capital of one issuer.
AGGREGATIONS = ("additive", "semiAdditive", "nonAdditive")

#: The custom properties of a schema property that the framework defines,
#: each with the check its value has to pass. ``kind`` and ``source`` are
#: required on every property; the others belong to measures.
CUSTOM_PROPERTIES = ("kind", "source", "aggregation", "aggregateWithin",
                     "timeProperty", "currencyProperty")

#: The kinds of column, as the column catalogue has always named them.
KINDS = ("identifier", "code", "text", "date", "datetime", "amount",
         "currency", "rate", "flag", "count", "quantity", "number", "weight",
         "other")

#: The states of a contract whose table no code writes yet. Such a contract
#: is the specification of a table still to be built: it is validated like any
#: other, but it is no description of a column a reader can find, so
#: :func:`catalogue_rows` leaves it out until it becomes active.
DRAFT_STATUSES = ("proposed", "draft")

#: The sections of a contract that describe a deployment rather than a
#: table, and which a shipped contract therefore leaves out.
DEPLOYMENT_SECTIONS = ("servers", "slaProperties", "slaDefaultElement",
                       "team", "support", "price", "roles")

_ODCS_SCHEMA_FILE = "odcs_json_schema_v3_2_0.json"
_ODPS_SCHEMA_FILE = "odps_json_schema_v1_1_0.json"
_CONTRACT_FILE = re.compile(r"^contract_([a-z0-9_]+)\.yaml$")
_PRODUCT_FILE = re.compile(r"^product_([a-z0-9_]+)\.yaml$")
_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


class ContractError(ValueError):
    """A contract or a product does not hold what the framework relies on.

    The message lists every problem found, not only the first, so that a
    contract can be corrected in one pass.
    """


def _root(package):
    # As in fincommons.columns: the data directory belongs to the
    # distribution, so this package is found by the first segment of the
    # name it was imported under.
    return __package__.split(".")[0] if package == "fincommons" else package


def _data_files(package, pattern):
    folder = resources.files(_root(package)).joinpath("data")
    return sorted((entry for entry in folder.iterdir()
                   if pattern.match(entry.name)), key=lambda e: e.name)


def _read_yaml(entry):
    """The parsed file, a copy of its own for every caller.

    A file on disk is parsed once per version of it, keyed by its path and
    the time it was last changed: :func:`load_contract` reads every shipped
    contract to find one, and parsing the largest of them over and over made
    generating the semantic layer of a product take a minute. Each caller is
    handed a deep copy, so that one that changes what it was given changes
    nothing for the next.
    """
    try:
        path = os.fspath(entry)
        stamp = os.stat(path).st_mtime_ns
    except (TypeError, OSError):
        with entry.open(encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    return copy.deepcopy(_parse_yaml(path, stamp))


@lru_cache(maxsize=None)
def _parse_yaml(path, stamp):
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def contracts(packages=("fincommons",)):
    """Every contract the packages ship, keyed by identifier.

    Read from the files named ``contract_*.yaml`` in each package's data
    directory, so that a contract is added by adding its file. Two contracts
    with one identifier are an error rather than one silently replacing the
    other.
    """
    found = {}
    for package in packages:
        for entry in _data_files(package, _CONTRACT_FILE):
            contract = _read_yaml(entry)
            identifier = contract.get("id")
            if identifier in found:
                raise ContractError(f"Two contracts have the identifier {identifier!r}.")
            found[identifier] = contract
    return found


def products(packages=("fincommons",)):
    """Every data product the packages ship, keyed by identifier."""
    found = {}
    for package in packages:
        for entry in _data_files(package, _PRODUCT_FILE):
            product = _read_yaml(entry)
            found[product.get("id")] = product
    return found


def load_contract(identifier, packages=("fincommons",)):
    """The contract with the identifier, e.g. ``fincommons.exchangerate.silver.exchangerate``.

    Raises
    ------
    KeyError
        Where no package ships a contract of that identifier; the message
        names those that exist.
    """
    shipped = contracts(packages)
    if identifier not in shipped:
        raise KeyError(f"No contract {identifier!r}; shipped: "
                       + ", ".join(sorted(shipped)))
    return shipped[identifier]


def custom_property(element, name, default=None):
    """The value of a custom property the framework defines on ``element``.

    Only properties whose ``vendor`` is :data:`VENDOR` count, so that a
    property another tool added under the same name is not mistaken for one
    of the framework's.
    """
    for item in element.get("customProperties") or []:
        if item.get("vendor") == VENDOR and item.get("property") == name:
            return item.get("value")
    return default


def _identifier_parts(identifier):
    parts = str(identifier).split(".")
    return parts if len(parts) == 4 else None


def _schema_errors(document, schema_file):
    """The problems the JSON schema of the standard finds in ``document``.

    ``jsonschema`` is a dependency of the test suite only, so it is imported
    here rather than at the top: reading a contract needs nothing beyond the
    YAML reader, validating one against the standard is a check run before a
    contract is shipped.
    """
    try:
        import jsonschema
    except ImportError as error:  # pragma: no cover - depends on the extras
        raise ImportError("Validating a contract against the standard needs "
                          "jsonschema, which the dev extra installs.") from error
    with resources.files(_root("fincommons")).joinpath("data", schema_file) \
            .open(encoding="utf-8") as handle:
        schema = json.load(handle)
    validator = jsonschema.Draft201909Validator(schema)
    return [f"{'/'.join(str(p) for p in error.absolute_path) or '(top)'}: {error.message}"
            for error in sorted(validator.iter_errors(document),
                                key=lambda e: list(e.absolute_path))]


def _property_errors(table, prop, names):
    where = f"{table}.{prop.get('name')}"
    errors = []
    name = prop.get("name", "")
    if not _SNAKE_CASE.match(name):
        errors.append(f"{where}: a column is named in snake_case and does not "
                      "start with an underscore")
    for field in ("description", "logicalType", "physicalType"):
        if not prop.get(field):
            errors.append(f"{where}: has no {field}")
    for item in prop.get("customProperties") or []:
        if item.get("vendor") == VENDOR and item.get("property") not in CUSTOM_PROPERTIES:
            errors.append(f"{where}: {item.get('property')!r} is not a custom "
                          f"property of {VENDOR}")
    kind = custom_property(prop, "kind")
    if kind not in KINDS:
        errors.append(f"{where}: kind {kind!r} is not one of {', '.join(KINDS)}")
    if custom_property(prop, "source") is None:
        errors.append(f"{where}: has no source")

    aggregation = custom_property(prop, "aggregation")
    if prop.get("semanticType") == "measure":
        if aggregation not in AGGREGATIONS:
            errors.append(f"{where}: a measure states its aggregation, one of "
                          + ", ".join(AGGREGATIONS))
        if not prop.get("transformLogic"):
            errors.append(f"{where}: a measure states its expression in transformLogic")
    elif aggregation is not None:
        errors.append(f"{where}: only a measure has an aggregation")
    if aggregation == "semiAdditive" and custom_property(prop, "timeProperty") not in names:
        errors.append(f"{where}: a semi-additive measure names the column of "
                      "its reference date in timeProperty")
    within = custom_property(prop, "aggregateWithin")
    if aggregation == "nonAdditive" and not within:
        errors.append(f"{where}: a non-additive measure names, in "
                      "aggregateWithin, the columns it may only be "
                      "aggregated within")
    if within is not None and (prop.get("semanticType") != "measure"
                               or not isinstance(within, list)
                               or not set(within) <= names):
        errors.append(f"{where}: aggregateWithin names columns of the table "
                      "a measure may only be aggregated within")
    currency = custom_property(prop, "currencyProperty")
    if currency is not None and currency not in names:
        errors.append(f"{where}: currencyProperty {currency!r} is not a column "
                      "of the table")
    if kind == "amount" and prop.get("semanticType") == "measure" and currency is None:
        errors.append(f"{where}: an amount names the column of its currency "
                      "in currencyProperty")
    return errors


def validate_contract(contract, file_name=None):
    """Check a contract against the standard and against the framework's rules.

    The standard is checked with its own JSON schema. The framework's rules
    are those a shipped contract has to meet beyond it: the pinned version of
    the standard, an identifier that names the package, module, layer and
    table (and agrees with the file name, where ``file_name`` is given), one
    schema object named like the table, no section that describes a
    deployment, and on every column a description, a logical and a physical
    type, a kind and a source, and the custom properties a measure needs.
    A quality rule the framework runs states what :func:`check_quality`
    needs to run it; see :func:`_quality_errors`.

    Raises
    ------
    ContractError
        Listing every problem found.
    """
    errors = _schema_errors(contract, _ODCS_SCHEMA_FILE)
    if contract.get("apiVersion") != ODCS_VERSION:
        errors.append(f"apiVersion is {contract.get('apiVersion')!r}, the "
                      f"framework is written against {ODCS_VERSION}")
    parts = _identifier_parts(contract.get("id"))
    if parts is None:
        errors.append(f"id {contract.get('id')!r} is not "
                      "<package>.<module>.<layer>.<table>")
    elif file_name is not None and file_name != f"contract_{'_'.join(parts[1:])}.yaml":
        errors.append(f"the file {file_name} does not hold the contract "
                      f"{contract.get('id')}; it would be named "
                      f"contract_{'_'.join(parts[1:])}.yaml")
    for section in DEPLOYMENT_SECTIONS:
        if section in contract:
            errors.append(f"{section} describes a deployment, which a shipped "
                          "contract leaves to the configuration of each one")
    objects = contract.get("schema") or []
    if len(objects) != 1:
        errors.append("a contract describes exactly one table")
    for table in objects:
        if parts is not None and table.get("name") != parts[3]:
            errors.append(f"the table is named {table.get('name')!r}, the "
                          f"identifier names {parts[3]!r}")
        properties = table.get("properties") or []
        names = {p.get("name") for p in properties}
        if len(names) != len(properties):
            errors.append(f"{table.get('name')}: a column is named twice")
        for prop in properties:
            errors.extend(_property_errors(table.get("name"), prop, names))
        errors.extend(_quality_errors(table, names))
    if errors:
        raise ContractError(f"The contract {contract.get('id')} is not valid:\n  "
                            + "\n  ".join(errors))


def _version(text):
    parts = str(text).split(".")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        return None
    return tuple(int(p) for p in parts)


def _accepts(kind, named, shipped):
    """Whether a port that names version ``named`` is served by ``shipped``."""
    if kind == "outputPorts":
        return named == shipped
    named, shipped = _version(named), _version(shipped)
    return (named is not None and shipped is not None
            and shipped[0] == named[0] and shipped >= named)


def validate_product(product, packages=("fincommons",)):
    """Check a data product against the standard and against the contracts shipped.

    Every output port names, by ``contractId`` and ``version``, a contract
    that one of ``packages`` ships in exactly that version: a module states
    the version of the tables it writes. Every input port names a contract
    that is shipped in a version the port accepts -- the same major version,
    and a minor and patch version no older than the port's -- since a minor
    version adds to a contract and a patch corrects its wording, and neither
    breaks a consumer written against the version before. That is what makes
    the dependency of one module on the tables of another a statement a test
    can check rather than one visible only in the code.

    Raises
    ------
    ContractError
        Listing every problem found.
    """
    errors = _schema_errors(product, _ODPS_SCHEMA_FILE)
    if product.get("apiVersion") != ODPS_VERSION:
        errors.append(f"apiVersion is {product.get('apiVersion')!r}, the "
                      f"framework is written against {ODPS_VERSION}")
    shipped = contracts(packages)
    if not product.get("outputPorts"):
        errors.append("a product has at least one output port")
    for kind in ("inputPorts", "outputPorts"):
        for port in product.get(kind) or []:
            contract = shipped.get(port.get("contractId"))
            if contract is None:
                errors.append(f"{kind} {port.get('name')}: no package ships the "
                              f"contract {port.get('contractId')!r}")
            elif not _accepts(kind, port.get("version"), contract.get("version")):
                errors.append(f"{kind} {port.get('name')}: the contract "
                              f"{port.get('contractId')} is shipped in version "
                              f"{contract.get('version')}, the port names "
                              f"{port.get('version')}")
    if errors:
        raise ContractError(f"The product {product.get('id')} is not valid:\n  "
                            + "\n  ".join(errors))


def catalogue_rows(packages=("fincommons",)):
    """The columns of the contracts, as rows of the column catalogue.

    One tuple per column: ``module``, ``column``, ``tables``, ``source``,
    ``kind``, ``description``, in the order of the catalogue. ``tables``
    names the stage that writes the table, ``<module>.<layer>``, as the rows
    of the catalogue do.

    The catalogue has one row per column of a module, and a module may write
    a column into more than one table -- the ISIN is a column of both tables
    of FIRDS. Such a column takes its row from the table its product lists
    first among its output ports, the table the others are derived from, so
    that the catalogue describes the column where it originates. The contract
    of each table still describes the column as that table holds it.

    A contract in one of the :data:`DRAFT_STATUSES` describes a table no code
    writes yet, and its columns are not rows of the catalogue.
    """
    shipped = contracts(packages)
    port_order = {}
    for product in products(packages).values():
        for position, port in enumerate(product.get("outputPorts") or []):
            port_order.setdefault(port.get("contractId"), position)
    rows, seen = [], set()
    for identifier in sorted(shipped, key=lambda i: (port_order.get(i, len(port_order)), i)):
        if shipped[identifier].get("status") in DRAFT_STATUSES:
            continue
        parts = _identifier_parts(identifier)
        if parts is None:
            raise ContractError(f"id {identifier!r} is not "
                                "<package>.<module>.<layer>.<table>")
        _, module, layer, _ = parts
        for table in shipped[identifier].get("schema") or []:
            for prop in table.get("properties") or []:
                if (module, prop["name"]) in seen:
                    continue
                seen.add((module, prop["name"]))
                rows.append((module, prop["name"], f"{module}.{layer}",
                             custom_property(prop, "source", ""),
                             custom_property(prop, "kind", ""),
                             prop.get("description", "").strip()))
    return rows


#: The Spark type of each physical type a contract may state.
_SPARK_TYPES = {
    "string": "StringType", "date": "DateType", "timestamp": "TimestampType",
    "boolean": "BooleanType", "double": "DoubleType", "float": "FloatType",
    "int": "IntegerType", "bigint": "LongType",
}


def spark_schema(contract):
    """The schema of the table a contract describes, as a Spark ``StructType``.

    The columns are in the order the contract lists them. A column is
    nullable unless the contract marks it ``required``. A physical type is
    either one of the simple types in lower case (``string``, ``date``,
    ``double``, ...), ``decimal(p,s)``, or ``array<t>`` of a simple type.

    Raises
    ------
    ContractError
        Where a physical type is not one the framework knows how to map.
    """
    from pyspark.sql import types

    fields = []
    for table in contract.get("schema") or []:
        for prop in table.get("properties") or []:
            physical = str(prop.get("physicalType", "")).strip().lower()
            decimal = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", physical)
            array = re.fullmatch(r"array<\s*(\w+)\s*>", physical)
            if decimal:
                spark_type = types.DecimalType(int(decimal.group(1)), int(decimal.group(2)))
            elif array and array.group(1) in _SPARK_TYPES:
                spark_type = types.ArrayType(getattr(types, _SPARK_TYPES[array.group(1)])())
            elif physical in _SPARK_TYPES:
                spark_type = getattr(types, _SPARK_TYPES[physical])()
            else:
                raise ContractError(f"{prop.get('name')}: the physical type "
                                    f"{physical!r} has no Spark type")
            fields.append(types.StructField(prop["name"], spark_type,
                                            not prop.get("required", False)))
    return types.StructType(fields)


# Quality rules
# --------------------------------------------------------------------------
#
# ODCS lets a contract state, on the table and on each column, the rules its
# data is to meet: a predefined metric of the standard compared with a
# threshold (type "library"), a SQL query whose result is compared the same
# way ("sql"), a rule for a named engine ("custom"), or a sentence ("text").
# The framework runs the first two itself, with Spark, so that the rules of a
# contract are checks that are executed rather than promises. A rule of
# another engine, or one in words, is left to whoever reads it.
#
# These are rules about a table as a whole -- that no key is repeated, that
# no rate is negative -- and they answer with one number per rule. The
# checks of fincommons.dataquality answer with one row per record they find
# at fault, which is what a reader of a financial report needs; the two are
# complementary, and neither replaces the other.

#: How severe a failed rule is, as the examples of ODCS name them.
QUALITY_SEVERITIES = ("error", "warning", "info")

#: The predefined metrics of ODCS, by where they are stated: on a column, or
#: on the table. ``duplicateValues`` is stated on a column, or on the table
#: with the columns it counts in ``arguments.properties``.
_COLUMN_METRICS = ("nullValues", "missingValues", "invalidValues", "duplicateValues")
_TABLE_METRICS = ("rowCount", "duplicateValues")

#: The comparisons of ODCS, each with the test a measured value has to pass.
_OPERATORS = {
    "mustBe": lambda v, x: v == x,
    "mustNotBe": lambda v, x: v != x,
    "mustBeGreaterThan": lambda v, x: v > x,
    "mustBeGreaterOrEqualTo": lambda v, x: v >= x,
    "mustBeLessThan": lambda v, x: v < x,
    "mustBeLessOrEqualTo": lambda v, x: v <= x,
    "mustBeBetween": lambda v, x: x[0] <= v <= x[1],
    "mustNotBeBetween": lambda v, x: not (x[0] <= v <= x[1]),
}

#: What a query names the table and the column by; ODCS writes them so.
_TABLE_PLACEHOLDER = "${table}"
_COLUMN_PLACEHOLDER = "${column}"


def _rules(table):
    """Every quality rule of a table: ``(column, rule)``, the column ``None`` for the table's own."""
    found = [(None, rule) for rule in table.get("quality") or []]
    for prop in table.get("properties") or []:
        found += [(prop.get("name"), rule) for rule in prop.get("quality") or []]
    return found


def _runs(rule):
    return rule.get("type", "library") in ("library", "sql")


def _quality_errors(table, names):
    """The problems of the quality rules of a table that the framework runs.

    A rule the framework runs has a name unique in its contract, a dimension
    and a severity of :data:`QUALITY_SEVERITIES`, so that a failure can be
    named, counted and weighed; one comparison; and what its type needs: a
    metric that belongs where it is stated, with the arguments it takes, or a
    query that reads the table.
    """
    errors, seen = [], set()
    for column, rule in _rules(table):
        where = f"{table.get('name')}{'.' + column if column else ''} rule {rule.get('name')!r}"
        if not _runs(rule):
            continue
        name = rule.get("name")
        if not name:
            errors.append(f"{where}: a rule the framework runs has a name")
        elif name in seen:
            errors.append(f"{where}: two rules have the name {name!r}")
        seen.add(name)
        if not rule.get("dimension"):
            errors.append(f"{where}: has no dimension")
        if rule.get("severity") not in QUALITY_SEVERITIES:
            errors.append(f"{where}: severity is one of {', '.join(QUALITY_SEVERITIES)}")
        operators = [o for o in _OPERATORS if o in rule]
        if len(operators) != 1:
            errors.append(f"{where}: states one comparison, e.g. mustBe")
        if rule.get("type", "library") == "sql":
            if _TABLE_PLACEHOLDER not in rule.get("query", ""):
                errors.append(f"{where}: the query reads the table as {_TABLE_PLACEHOLDER}")
            if _COLUMN_PLACEHOLDER in rule.get("query", "") and column is None:
                errors.append(f"{where}: a rule of the table has no {_COLUMN_PLACEHOLDER}")
            continue
        metric = rule.get("metric")
        arguments = rule.get("arguments") or {}
        allowed = _COLUMN_METRICS if column else _TABLE_METRICS
        if metric not in allowed:
            errors.append(f"{where}: the metric {metric!r} is not one of a "
                          f"{'column' if column else 'table'}: {', '.join(allowed)}")
        if metric == "invalidValues" and not ("validValues" in arguments or "pattern" in arguments):
            errors.append(f"{where}: invalidValues names its validValues or its pattern")
        if metric == "duplicateValues" and column is None:
            counted = arguments.get("properties") or []
            if not counted or not set(counted) <= names:
                errors.append(f"{where}: a duplicate key of the table names its "
                              "columns in arguments.properties")
    return errors


@dataclass(frozen=True)
class QualityResult:
    """The outcome of one quality rule of a contract on one table.

    ``value`` is what was measured -- a count of rows, or a percentage where
    the rule's unit is ``percent`` -- and ``passed`` whether it meets the
    rule's comparison. A rule the framework does not run has neither.
    """

    contract: str
    rule: str
    column: str | None
    dimension: str | None
    severity: str | None
    description: str
    value: float | None
    passed: bool | None


def _measure(frame, view, column, rule, rows):
    from pyspark.sql import functions as F

    if rule.get("type", "library") == "sql":
        query = rule["query"].replace(_TABLE_PLACEHOLDER, view)
        if column is not None:
            query = query.replace(_COLUMN_PLACEHOLDER, f"`{column}`")
        return frame.sparkSession.sql(query).first()[0]
    metric = rule["metric"]
    arguments = rule.get("arguments") or {}
    if metric == "rowCount":
        return rows
    if metric == "duplicateValues":
        # A column counts the values that repeat another, its empty rows
        # aside. A key of several columns counts the rows that repeat another
        # in all of them, an empty value being a value like any other: an
        # issue about a filing as a whole has no fact, and two such issues of
        # one check are a duplicate all the same.
        if column:
            present = frame.where(F.col(f"`{column}`").isNotNull())
            return present.count() - present.select(f"`{column}`").distinct().count()
        counted = [f"`{c}`" for c in arguments["properties"]]
        return rows - frame.select(*counted).distinct().count()
    value = F.col(f"`{column}`")
    if metric == "nullValues":
        return frame.where(value.isNull()).count()
    if metric == "missingValues":
        missing = [m for m in arguments.get("missingValues") or [] if m is not None]
        return frame.where(value.isNull() | value.isin(missing)).count()
    if metric == "invalidValues":
        valid = value.isNotNull()
        if "validValues" in arguments:
            bad = valid & ~value.isin(list(arguments["validValues"]))
        else:
            bad = valid & ~value.cast("string").rlike(arguments["pattern"])
        return frame.where(bad).count()
    raise ContractError(f"the metric {metric!r} is not one the framework runs")


def check_quality(frame, contract):
    """Run the quality rules of a contract on a table.

    Parameters
    ----------
    frame: pyspark.sql.DataFrame
        The table the contract describes, as read.
    contract: dict
        Its contract, from :func:`load_contract`.

    Returns
    -------
    list of QualityResult
        One per rule, the rules of the table first and then those of each
        column, in the order the contract states them. A rule of type
        ``text`` or ``custom`` is listed with no value and no verdict.
    """
    import uuid

    view = f"_contract_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    try:
        rows = frame.count()
        results = []
        for table in contract.get("schema") or []:
            for column, rule in _rules(table):
                common = dict(contract=contract.get("id"), rule=rule.get("name") or "",
                              column=column, dimension=rule.get("dimension"),
                              severity=rule.get("severity"),
                              description=" ".join(str(rule.get("description", "")).split()))
                if not _runs(rule):
                    results.append(QualityResult(value=None, passed=None, **common))
                    continue
                value = _measure(frame, view, column, rule, rows)
                # A query may measure nothing -- a SUM over no rows is NULL --
                # and a rule that measured nothing fails rather than raising.
                if (value is not None and rule.get("unit") == "percent"
                        and rule.get("metric") != "rowCount"):
                    value = 100.0 * value / rows if rows else 0.0
                operator = next(o for o in _OPERATORS if o in rule)
                passed = value is not None and bool(_OPERATORS[operator](value, rule[operator]))
                results.append(QualityResult(value=value, passed=passed, **common))
        return results
    finally:
        frame.sparkSession.catalog.dropTempView(view)


def quality_failures(results, severities=("error",)):
    """The rules among ``results`` that failed with one of ``severities``."""
    return [r for r in results if r.passed is False and r.severity in severities]

