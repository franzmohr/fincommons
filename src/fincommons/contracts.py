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

import json
import re
from importlib import resources

import yaml

__all__ = ["ODCS_VERSION", "ODPS_VERSION", "VENDOR", "CUSTOM_PROPERTIES",
           "AGGREGATIONS", "DEPLOYMENT_SECTIONS", "ContractError",
           "contracts", "products", "load_contract", "validate_contract",
           "validate_product", "custom_property", "catalogue_rows",
           "spark_schema"]

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
#: names -- an exchange rate, within one currency pair.
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
    with entry.open(encoding="utf-8") as handle:
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
    if aggregation == "nonAdditive":
        within = custom_property(prop, "aggregateWithin") or []
        if not within or not set(within) <= names:
            errors.append(f"{where}: a non-additive measure names, in "
                          "aggregateWithin, the columns it may only be "
                          "aggregated within")
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
    if errors:
        raise ContractError(f"The contract {contract.get('id')} is not valid:\n  "
                            + "\n  ".join(errors))


def validate_product(product, packages=("fincommons",)):
    """Check a data product against the standard and against the contracts shipped.

    Every output port names, by ``contractId`` and ``version``, a contract
    that one of ``packages`` ships in exactly that version, and so does
    every input port. That is what makes the dependency of one module on the
    tables of another a statement a test can check rather than one visible
    only in the code.

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
            elif contract.get("version") != port.get("version"):
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
    """
    rows = []
    for identifier, contract in contracts(packages).items():
        parts = _identifier_parts(identifier)
        if parts is None:
            raise ContractError(f"id {identifier!r} is not "
                                "<package>.<module>.<layer>.<table>")
        _, module, layer, _ = parts
        for table in contract.get("schema") or []:
            for prop in table.get("properties") or []:
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
    ``double``, ...) or ``decimal(p,s)``.

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
            if decimal:
                spark_type = types.DecimalType(int(decimal.group(1)), int(decimal.group(2)))
            elif physical in _SPARK_TYPES:
                spark_type = getattr(types, _SPARK_TYPES[physical])()
            else:
                raise ContractError(f"{prop.get('name')}: the physical type "
                                    f"{physical!r} has no Spark type")
            fields.append(types.StructField(prop["name"], spark_type,
                                            not prop.get("required", False)))
    return types.StructType(fields)
