"""The data contracts and products the package ships.

The first two tests are the ones that keep the shipped files honest: every
contract and every product is valid against the JSON schema of its standard
and against the rules of the framework. The rest pin those rules, one by one,
on contracts written into the test, so that a rule that stops being enforced
fails here and not in some later reader of a contract.
"""

from __future__ import annotations

import copy
from importlib import resources

import pytest

from fincommons import contracts as c
from fincommons.columns import describe_column

EXCHANGERATE = "fincommons.exchangerate.silver.exchangerate"


def shipped_files(prefix):
    folder = resources.files("fincommons").joinpath("data")
    return sorted(e.name for e in folder.iterdir()
                  if e.name.startswith(prefix) and e.name.endswith(".yaml"))


@pytest.mark.parametrize("name", shipped_files("contract_"))
def test_every_shipped_contract_is_valid(name):
    entry = resources.files("fincommons").joinpath("data", name)
    contract = c._read_yaml(entry)
    c.validate_contract(contract, file_name=name)


@pytest.mark.parametrize("name", shipped_files("product_"))
def test_every_shipped_product_is_valid(name):
    entry = resources.files("fincommons").joinpath("data", name)
    c.validate_product(c._read_yaml(entry))


def test_the_exchange_rates_are_the_first_data_set_with_a_contract():
    assert EXCHANGERATE in c.contracts()
    assert "fincommons.exchangerate" in c.products()


def a_contract():
    """A copy of the exchange rate contract, to be broken by a test."""
    return copy.deepcopy(c.load_contract(EXCHANGERATE))


def column(contract, name):
    return next(p for p in contract["schema"][0]["properties"] if p["name"] == name)


def problems(contract):
    with pytest.raises(c.ContractError) as raised:
        c.validate_contract(contract)
    return str(raised.value)


def test_the_standard_itself_is_checked():
    contract = a_contract()
    column(contract, "value")["logicalType"] = "money"

    assert "'money' is not one of" in problems(contract)


def test_a_contract_is_written_against_the_pinned_version_of_the_standard():
    contract = a_contract()
    contract["apiVersion"] = "v3.1.0"

    assert "written against v3.2.0" in problems(contract)


def test_a_shipped_contract_describes_no_deployment():
    contract = a_contract()
    contract["servers"] = [{"server": "production", "type": "databricks"}]

    assert "servers describes a deployment" in problems(contract)


def test_the_identifier_names_the_package_module_layer_and_table():
    contract = a_contract()
    contract["id"] = "exchangerate"

    assert "is not <package>.<module>.<layer>.<table>" in problems(contract)


def test_the_file_is_named_after_the_contract_it_holds():
    with pytest.raises(c.ContractError, match="would be named"):
        c.validate_contract(a_contract(), file_name="contract_exchangerate.yaml")


def test_a_column_is_named_in_snake_case_without_a_leading_underscore():
    contract = a_contract()
    column(contract, "value")["name"] = "_value"

    assert "snake_case" in problems(contract)


def test_every_column_has_a_kind_the_catalogue_knows():
    contract = a_contract()
    for item in column(contract, "base")["customProperties"]:
        if item["property"] == "kind":
            item["value"] = "colour"

    assert "kind 'colour'" in problems(contract)


def test_a_custom_property_of_the_framework_has_to_be_one_it_defines():
    contract = a_contract()
    column(contract, "base")["customProperties"].append(
        {"property": "unit", "value": "EUR", "vendor": "fincommons"})

    assert "'unit' is not a custom property of fincommons" in problems(contract)


def test_a_measure_states_how_it_may_be_aggregated():
    contract = a_contract()
    value = column(contract, "value")
    value["customProperties"] = [p for p in value["customProperties"]
                                 if p["property"] != "aggregation"]

    assert "a measure states its aggregation" in problems(contract)


def test_a_non_additive_measure_is_aggregated_within_columns_of_its_table():
    contract = a_contract()
    for item in column(contract, "value")["customProperties"]:
        if item["property"] == "aggregateWithin":
            item["value"] = ["base", "currency_pair"]

    assert "aggregateWithin" in problems(contract)


def test_a_semi_additive_measure_names_its_reference_date():
    contract = a_contract()
    value = column(contract, "value")
    for item in value["customProperties"]:
        if item["property"] == "aggregation":
            item["value"] = "semiAdditive"

    assert "timeProperty" in problems(contract)


def test_an_amount_names_the_column_of_its_currency():
    contract = a_contract()
    value = column(contract, "value")
    for item in value["customProperties"]:
        if item["property"] == "kind":
            item["value"] = "amount"

    assert "currencyProperty" in problems(contract)


def test_only_a_measure_has_an_aggregation():
    contract = a_contract()
    column(contract, "base")["customProperties"].append(
        {"property": "aggregation", "value": "additive", "vendor": "fincommons"})

    assert "only a measure has an aggregation" in problems(contract)


def test_a_custom_property_of_another_vendor_is_not_the_frameworks():
    element = {"customProperties": [
        {"property": "kind", "value": "colour", "vendor": "atlan"},
        {"property": "kind", "value": "rate", "vendor": "fincommons"},
    ]}

    assert c.custom_property(element, "kind") == "rate"
    assert c.custom_property({"customProperties": element["customProperties"][:1]},
                             "kind") is None


def test_a_product_names_contracts_that_are_shipped_in_that_version():
    product = copy.deepcopy(c.products()["fincommons.exchangerate"])
    product["outputPorts"][0]["version"] = "2.0.0"
    product["outputPorts"].append({"name": "gold_rates", "version": "1.0.0",
                                   "contractId": "fincommons.exchangerate.gold.rates"})

    with pytest.raises(c.ContractError) as raised:
        c.validate_product(product)
    assert "is shipped in version 1.0.0, the port names 2.0.0" in str(raised.value)
    assert "no package ships the contract 'fincommons.exchangerate.gold.rates'" \
        in str(raised.value)


def test_a_draft_contract_is_not_in_the_catalogue(monkeypatch):
    # A draft specifies a table no code writes yet, so its columns are no
    # description of anything a reader can find.
    shipped = c.contracts()
    draft = copy.deepcopy(shipped["fincommons.exchangerate.silver.exchangerate"])
    draft["id"] = "fincommons.exchangerate.silver.planned"
    draft["status"] = "draft"
    draft["schema"][0]["name"] = "planned"
    draft["schema"][0]["properties"][0]["name"] = "planned_column"
    monkeypatch.setattr(c, "contracts",
                        lambda packages=("fincommons",): {**shipped, draft["id"]: draft})

    columns = {column for _, column, *_ in c.catalogue_rows()}

    assert "planned_column" not in columns
    assert "ref_date" in columns


def test_an_unknown_contract_names_those_that_exist():
    with pytest.raises(KeyError, match=EXCHANGERATE):
        c.load_contract("fincommons.sec.silver.facts")


def test_the_contract_describes_the_columns_of_its_table():
    described = describe_column("imputed", dataset="exchangerate")[0]

    assert described.tables == ["exchangerate.silver"]
    assert described.kind == "flag"
    assert described.source == "derived"
    assert described.description.startswith("True where the day carries")


def test_the_schema_of_a_contract_in_spark_types():
    pytest.importorskip("pyspark")
    from pyspark.sql import types

    schema = c.spark_schema(c.load_contract(EXCHANGERATE))

    assert schema.fieldNames() == ["ref_date", "base", "quote", "value",
                                   "type", "source", "imputed"]
    assert schema["ref_date"].dataType == types.DateType()
    assert schema["value"].dataType == types.DoubleType()
    assert not schema["value"].nullable


def test_a_decimal_keeps_its_precision_and_an_unknown_type_is_an_error():
    pytest.importorskip("pyspark")
    from pyspark.sql import types

    contract = a_contract()
    column(contract, "value")["physicalType"] = "decimal(28, 10)"
    assert c.spark_schema(contract)["value"].dataType == types.DecimalType(28, 10)

    column(contract, "value")["physicalType"] = "money"
    with pytest.raises(c.ContractError, match="no Spark type"):
        c.spark_schema(contract)
