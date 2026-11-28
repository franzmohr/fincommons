"""The semantic layer: the contracts, in the dialect of the tool that reads the tables.

A contract states what a table holds; the semantic layer states how its values
may be combined -- which columns are dimensions to group by, which are
measures to aggregate, and how. The framework derives the second from the
first (see :mod:`fincommons.contracts` and the handbook section on data
contracts and the semantic layer), so that what a column means and how it may
be aggregated are stated in one place.

The semantic layer writes no table. What it produces is text that describes
the existing tables to a tool, and the generation is a function of the
package that returns that text, so that it runs and is tested on a
workstation without a connection to any platform. Applying the text is a
separate step, run against the platform.

Databricks
----------

The first tool is the platform of the reference deployment. For each output
port of a data product, :func:`databricks` returns

* the statements that put the description of the table and of each of its
  columns on the table in Unity Catalog, :func:`comment_statements`;
* the statement that creates a metric view over the table,
  :func:`create_metric_view`, from the definition :func:`metric_view`
  writes in version 1.1 of the metric view specification;

and, once for the product, the instructions for an assistant such as Genie,
:func:`assistant_instructions`. :func:`apply_databricks` runs the statements
in a Spark session on the platform.

A metric view describes a table in a catalogue, so a layer served this way is
addressed by catalogue name in the configuration file; :func:`databricks`
refuses a layer addressed by path alone. A package whose modules write their
tables by path, in directories of their own layout, passes ``locate``: it
names, for each contract, the directory the table is written to and the name
it is to carry in the catalogue, and the table is registered under that name
over its files before it is described.

How the rules of aggregation become measures
--------------------------------------------

A dimension of the contract becomes a field of the metric view, under the
name of its column, with the business name as its display name and the
synonyms of the contract as its synonyms. A measure becomes a measure whose
expression is the ``transformLogic`` of the contract, and the custom
properties of the framework decide what more it needs:

``additive``
    the expression as it stands.
``semiAdditive``
    a window ordered by the column ``timeProperty`` names, which reads the
    measure at the last date of a period rather than summing it over the
    dates.
``nonAdditive``
    the expression only where every column ``aggregateWithin`` names holds a
    single value in the group, and empty otherwise: an exchange rate averaged
    within one currency pair is a rate, and one averaged over several pairs
    is a number that means nothing. A tool that is not told otherwise asks
    for the second as readily as for the first; the metric view answers it
    with an empty value rather than a wrong one.

An additive or semi-additive measure may name ``aggregateWithin`` as well,
where it may be summed only within one value of those columns -- a
percentage of the capital of one issuer, a margin within one haircut basis
-- and is guarded the same way; a semi-additive one is read at the last date
of a period besides. A measure that names its currency in
``currencyProperty`` is guarded by its currency, so that amounts in
different currencies are never added.

A guard asks that a column hold at most one distinct value in the group, so
that a group in which the column is empty throughout -- a price quoted in
percent, which has no currency -- is read rather than left empty.
"""

from __future__ import annotations

from dataclasses import dataclass

import yaml

from . import contracts as _contracts
from .addressing import resolve

__all__ = ["METRIC_VIEW_VERSION", "metric_view", "create_metric_view",
           "comment_statements", "assistant_instructions", "Location", "FORMATS",
           "register_statements",
           "databricks",
           "apply_databricks"]

#: The version of the metric view specification the definitions are written
#: in. Version 1.1 is the first with comments, display names and synonyms.
METRIC_VIEW_VERSION = "1.1"

#: The limits the metric view specification sets on the agent metadata.
_MAX_SYNONYMS = 10
_MAX_METADATA = 255


def _table(contract):
    objects = contract.get("schema") or []
    if len(objects) != 1:
        raise _contracts.ContractError(
            f"the contract {contract.get('id')} describes {len(objects)} tables, "
            "the semantic layer describes one per contract")
    return objects[0]


def _text(value):
    return " ".join(str(value).split())


#: The source of a synonym that records a former name of the column in the
#: package rather than a name a reader of the data uses, and which an
#: assistant is therefore not given.
_FORMER_NAMES = "legacy-name"


def _synonyms(prop):
    found = []
    for item in prop.get("synonyms") or []:
        if isinstance(item, dict) and item.get("source") == _FORMER_NAMES:
            continue
        word = _text(item.get("synonym", "")) if isinstance(item, dict) else _text(item)
        if word and len(word) <= _MAX_METADATA and word.lower() not in \
                {w.lower() for w in found} and word != prop["name"]:
            found.append(word)
    return found[:_MAX_SYNONYMS]


def _guards(prop):
    """The columns that must hold one value in a group for the measure to be read."""
    guards = list(_contracts.custom_property(prop, "aggregateWithin") or [])
    currency = _contracts.custom_property(prop, "currencyProperty")
    if currency is not None and currency not in guards:
        guards.append(currency)
    return guards


def _measure_expression(prop):
    expression = _text(prop["transformLogic"])
    guards = _guards(prop)
    if not guards:
        return expression
    # At most one distinct value rather than MIN = MAX, which is empty where
    # the column is empty throughout the group and would leave the measure
    # empty with it.
    condition = " AND ".join(f"COUNT(DISTINCT {c}) <= 1" for c in guards)
    return f"CASE WHEN {condition} THEN {expression} END"


def _measure_name(prop, columns):
    """The name of the measure: its business name in snake case.

    A measure is named after what it is rather than after its column, since
    a measure named like a column of its source is read as that column: the
    rate of the exchange rate table is ``exchange_rate``, not ``value``.
    Where the business name is the name of a column all the same, the
    aggregation is put in front: ``avg_total_issued_nominal_amount``.
    """
    words = "".join(ch if ch.isalnum() else " " for ch in
                    (prop.get("businessName") or prop["name"]).lower()).split()
    name = "_".join(words) or prop["name"]
    if name not in columns:
        return name
    function = _text(prop["transformLogic"]).split("(")[0].strip().lower()
    return f"{function or 'measure'}_{name}"


def _entry(prop, name=None):
    entry = {"name": name or prop["name"]}
    display = _text(prop.get("businessName") or "")
    if display:
        entry["display_name"] = display[:_MAX_METADATA]
    description = _text(prop.get("description") or "")
    if description:
        entry["comment"] = description
    synonyms = _synonyms(prop)
    if synonyms:
        entry["synonyms"] = synonyms
    return entry


def metric_view(contract, source):
    """The definition of a metric view over the table a contract describes.

    Parameters
    ----------
    contract: dict
        A contract, from :func:`fincommons.contracts.load_contract`.
    source: str
        The name of the table in the catalogue, e.g.
        ``main.fincommons_exchangerate.exchangerate``.

    Returns
    -------
    str
        The YAML of the definition, in version :data:`METRIC_VIEW_VERSION`.
    """
    table = _table(contract)
    fields, measures = [], []
    columns = {p["name"] for p in table.get("properties") or []}
    for prop in table.get("properties") or []:
        if prop.get("semanticType") == "measure":
            entry = _entry(prop, _measure_name(prop, columns))
            entry["expr"] = _measure_expression(prop)
            if _contracts.custom_property(prop, "aggregation") == "semiAdditive":
                entry["window"] = [{
                    "order": _contracts.custom_property(prop, "timeProperty"),
                    "range": "current",
                    "semiadditive": "last",
                }]
            measures.append(entry)
        else:
            entry = _entry(prop)
            entry["expr"] = prop["name"]
            fields.append(entry)
    definition = {"version": METRIC_VIEW_VERSION,
                  "comment": _text(table.get("description") or ""),
                  "source": source,
                  "fields": fields}
    if measures:
        definition["measures"] = measures
    # The keys in the order a reader expects them: name, then what it is.
    order = ("name", "expr", "display_name", "comment", "synonyms", "window")
    for entry in fields + measures:
        ordered = {k: entry[k] for k in order if k in entry}
        entry.clear()
        entry.update(ordered)
    return yaml.safe_dump(definition, sort_keys=False, allow_unicode=True,
                          width=100)


def create_metric_view(contract, source, view):
    """The statement that creates, or replaces, the metric view ``view``."""
    return (f"CREATE OR REPLACE VIEW {view}\n"
            f"WITH METRICS LANGUAGE YAML AS $$\n"
            f"{metric_view(contract, source)}$$")


def _literal(text):
    """A SQL string literal. Spark reads a backslash as an escape, so both
    the backslash and the quote are escaped rather than the quote doubled,
    which Spark would read as two adjacent literals."""
    return "'" + _text(text).replace("\\", "\\\\").replace("'", "\\'") + "'"


def comment_statements(contract, table_name):
    """The statements that put the descriptions of a contract on its table.

    The description of the table and of every column become their comments in
    the catalogue, where Catalog Explorer, a dashboard and an assistant show
    them.

    Returns
    -------
    list of str
        One statement each, without a closing semicolon, so that each can be
        handed to ``spark.sql`` as it stands.
    """
    table = _table(contract)
    statements = [f"COMMENT ON TABLE {table_name} IS {_literal(table.get('description', ''))}"]
    for prop in table.get("properties") or []:
        statements.append(f"ALTER TABLE {table_name} ALTER COLUMN {prop['name']} "
                          f"COMMENT {_literal(prop.get('description', ''))}")
    return statements


def _rules(contract):
    """What a reader must not do with the measures of a contract, in words."""
    rules = []
    for prop in _table(contract).get("properties") or []:
        if prop.get("semanticType") != "measure":
            continue
        name = prop["name"]
        aggregation = _contracts.custom_property(prop, "aggregation")
        within = " and ".join(_contracts.custom_property(prop, "aggregateWithin") or [])
        logic = _text(prop.get("transformLogic", ""))
        if aggregation == "nonAdditive":
            # Said as the measure aggregates: most are averaged, a factor
            # that compounds is multiplied, as EXP(SUM(LN(x))) does, and one
            # that is summed is summed within its groups and nowhere else.
            upper = logic.upper()
            if upper.startswith("AVG("):
                rules.append(f"Never sum {name}. Average it only within one value of "
                             f"{within}.")
            elif upper.startswith("EXP(SUM(LN("):
                rules.append(f"Never sum {name}. Multiply it, and only within one "
                             f"value of {within}.")
            elif upper.startswith("SUM("):
                rules.append(f"Sum {name} only within one value of {within}, as "
                             f"{logic}; across them it does not add up.")
            else:
                rules.append(f"Do not sum {name}. Aggregate it only as {logic}, "
                             f"and only within one value of {within}.")
        elif within:
            rules.append(f"Sum {name} only within one value of {within}.")
        if aggregation == "semiAdditive":
            time = _contracts.custom_property(prop, "timeProperty")
            rules.append(f"Do not sum {name} over {time}: over a period, read it at "
                         f"the last {time} of the period.")
        currency = _contracts.custom_property(prop, "currencyProperty")
        if currency is not None:
            rules.append(f"Add {name} only within one value of {currency}.")
    return rules


def assistant_instructions(product, packages=("fincommons",)):
    """Instructions for an assistant that answers questions about a product's tables.

    Taken from the ``context`` the product states in ODPS -- its instructions,
    its constraints and its sample questions -- and from the contracts of its
    output ports: what each table holds, what identifies a row, and the rules
    of aggregation of its measures, stated as constraints.
    """
    shipped = _contracts.contracts(packages)
    context = product.get("context") or {}
    out = [f"# {product.get('name')}", ""]
    purpose = (product.get("description") or {}).get("purpose")
    if purpose:
        out += [_text(purpose), ""]
    if context.get("instructions"):
        out += [_text(context["instructions"]), ""]
    constraints = [_text(c["constraint"]) for c in context.get("constraints") or []]
    for port in product.get("outputPorts") or []:
        contract = shipped[port["contractId"]]
        if contract.get("status") in _contracts.DRAFT_STATUSES:
            # A draft describes a table no code writes yet.
            continue
        table = _table(contract)
        key = [p["name"] for p in sorted(
            (p for p in table.get("properties") or [] if p.get("primaryKey")),
            key=lambda p: p.get("primaryKeyPosition", 0))]
        out += [f"## {table['name']}", "", _text(table.get("description", ""))]
        if key:
            out.append(f"A row is identified by {', '.join(key)}.")
        usage = (contract.get("description") or {}).get("usage")
        if usage:
            out.append(_text(usage))
        out.append("")
        constraints += _rules(contract)
    if constraints:
        out += ["## Never", ""] + [f"- {c}" for c in constraints] + [""]
    questions = [_text(s["question"]) for s in context.get("verifiedStatements") or []]
    if questions:
        out += ["## Example questions", ""] + [f"- {q}" for q in questions] + [""]
    return "\n".join(out)


def _view_name(name, suffix):
    """The name of the view beside a table, inside the quotes of a quoted name."""
    if name.endswith("`"):
        return name[:-1] + suffix + "`"
    return name + suffix


#: The formats a table written by path may be registered in.
FORMATS = ("delta", "parquet")


@dataclass(frozen=True)
class Location:
    """Where a table written by path lives, and what it is called in the catalogue.

    Attributes
    ----------
    name:
        The qualified name the table is to carry, e.g. ``main.emir_silver.contract_2024``.
    path:
        The directory its files are written to.
    format:
        ``"delta"`` or ``"parquet"``, as the module writes it.
    partitioned:
        Whether a Parquet table is partitioned into directories, whose
        partitions the catalogue has to be told of.
    """

    name: str
    path: str
    format: str = "delta"
    partitioned: bool = False

    def __post_init__(self):
        if self.format not in FORMATS:
            raise ValueError(f"a table is registered as {' or '.join(FORMATS)}, "
                             f"not {self.format!r}")


def register_statements(location):
    """The statements that give the files of a table a name in the catalogue.

    For a table written by path: it becomes an external table over its
    directory, which is where the metric view and the comments then find it.
    A table that already has the name is left as it is. A partitioned Parquet
    table keeps no log of its partitions, so they are recovered from its
    directories, which a later run repeats to pick up new ones.
    """
    path = location.path.replace("\\", "/").replace("'", "\\'")
    statements = [f"CREATE TABLE IF NOT EXISTS {location.name} "
                  f"USING {location.format.upper()} LOCATION '{path}'"]
    if location.format == "parquet" and location.partitioned:
        statements.append(f"MSCK REPAIR TABLE {location.name}")
    return statements


def databricks(config_path, product_id, packages=("fincommons",), suffix="_metrics",
               locate=None):
    """The semantic layer of one data product, for Databricks.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where the layers of the product's
        tables are addressed. Each has to be addressed by catalogue name.
    product_id: str
        The product, e.g. ``"fincommons.exchangerate"``.
    packages: tuple of str
        The packages whose contracts and products are read.
    suffix: str
        Appended to the name of a table to name its metric view, which lives
        beside it.
    locate: callable, optional
        ``locate(contract_id)`` returns the :class:`Location` of a table that
        is written by path in a layout of its own -- the directory it is
        written to and the name it is to have in the catalogue -- or ``None``
        for a table the configuration file addresses as it stands. A table so
        located is registered under its name, over its directory, before it
        is described.

    Returns
    -------
    dict
        ``"statements"``: the statements to run, in order, without closing
        semicolons -- the comments and the metric view of every output port
        whose contract is not a draft (:data:`fincommons.contracts.DRAFT_STATUSES`);
        ``"assistant"``: the instructions for an assistant, as Markdown.

    Raises
    ------
    KeyError
        Where no package ships the product.
    ValueError
        Where a table of the product is addressed by path, which no metric
        view can read.
    """
    products = _contracts.products(packages)
    if product_id not in products:
        raise KeyError(f"No product {product_id!r}; shipped: " + ", ".join(sorted(products)))
    product = products[product_id]
    statements = []
    for port in product.get("outputPorts") or []:
        contract = _contracts.load_contract(port["contractId"], packages)
        if contract.get("status") in _contracts.DRAFT_STATUSES:
            # A draft describes a table no code writes yet, so there is
            # nothing for a comment or a metric view to describe.
            continue
        _, module, layer, table = port["contractId"].split(".")
        located = locate(port["contractId"]) if locate is not None else None
        if located is not None:
            name = located.name
            statements += register_statements(located)
            view = _view_name(name, suffix)
        else:
            ref = resolve(config_path, module, layer, table)
            if not ref.is_named:
                raise ValueError(
                    f"the {layer} layer of {module} is addressed by path in "
                    f"{config_path}; a metric view describes a table in a catalogue, "
                    "so the layer has to be given a table prefix there")
            name = ref.name
            # Beside the table, under the same prefix, quoted as the table is.
            view = resolve(config_path, module, layer, table + suffix).name
        statements += comment_statements(contract, name)
        statements.append(create_metric_view(contract, name, view))
    return {"statements": statements,
            "assistant": assistant_instructions(product, packages)}


def apply_databricks(spark, config_path, product_id, packages=("fincommons",),
                     suffix="_metrics", locate=None):
    """Run the statements of :func:`databricks` in a Spark session on Databricks.

    The one part of the semantic layer that needs the platform: the comments
    are set and the metric views created or replaced, one statement at a
    time, so that a statement the platform refuses is named by the error.
    The instructions for an assistant are returned rather than applied,
    since an assistant such as a Genie space is configured in its own
    interface.

    Returns
    -------
    str
        The instructions for an assistant, as Markdown.
    """
    generated = databricks(config_path, product_id, packages, suffix, locate)
    for statement in generated["statements"]:
        spark.sql(statement)
    return generated["assistant"]
