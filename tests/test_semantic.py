"""The semantic layer generated from the contracts.

The generation returns text, so most of it is tested by reading that text
back. Two parts are run as well, on the local Spark session, because a
statement that reads well can still be refused: the guarded measure, which
has to give the rate of one currency pair and nothing for several, and the
comments, which have to land on a Delta table as they were written. What
cannot be run here is the metric view itself, which only Databricks reads.
"""

from __future__ import annotations

import copy

import pytest
import yaml

from fincommons import contracts as c
from fincommons import semantic

EXCHANGERATE = "fincommons.exchangerate.silver.exchangerate"
SOURCE = "main.fincommons_exchangerate.exchangerate"


@pytest.fixture
def named(tmp_path):
    """A configuration file that gives every Silver layer a catalogue prefix."""
    config = tmp_path / "paths.yaml"
    modules = {port["contractId"].split(".")[1]
               for product in c.products().values() for port in product["outputPorts"]}
    config.write_text(yaml.safe_dump({
        module: {"silver": {"table": f"main.fincommons_{module}"}} for module in modules}))
    return str(config)


def definition(contract=None, source=SOURCE):
    return yaml.safe_load(semantic.metric_view(contract or c.load_contract(EXCHANGERATE), source))


def column(contract, name):
    return next(p for p in contract["schema"][0]["properties"] if p["name"] == name)


# The metric view
# --------------------------------------------------------------------------


def test_the_metric_view_reads_the_table_it_describes():
    view = definition()

    assert view["version"] == semantic.METRIC_VIEW_VERSION == "1.1"
    assert view["source"] == SOURCE
    assert view["comment"].startswith("One row per calendar day")


def test_a_dimension_becomes_a_field_under_the_name_of_its_column():
    fields = {f["name"]: f for f in definition()["fields"]}

    assert list(fields) == ["ref_date", "base", "quote", "type", "source", "imputed"]
    assert fields["base"]["expr"] == "base"
    assert fields["base"]["display_name"] == "Base currency"
    assert fields["imputed"]["comment"].startswith("True where the day carries")


def test_a_reader_is_given_the_names_of_the_sources_but_not_former_names_of_the_package():
    fields = {f["name"]: f for f in definition()["fields"]}

    assert fields["ref_date"]["synonyms"] == ["Date", "TIME_PERIOD"]
    assert "synonyms" not in fields["base"]


def test_a_rate_is_a_measure_only_within_one_currency_pair():
    (measure,) = definition()["measures"]

    assert measure["name"] == "exchange_rate"
    assert measure["display_name"] == "Exchange rate"
    assert measure["expr"] == (
        "CASE WHEN COUNT(DISTINCT base) <= 1 AND COUNT(DISTINCT quote) <= 1 "
        "THEN AVG(value) END")


def test_the_guard_gives_the_rate_of_one_pair_and_nothing_for_several(spark):
    """The expression of the measure, run as a query, which is what a metric
    view runs for each group a reader asks for."""
    rows = [("2025-01-02", "EUR", "USD", 1.0), ("2025-01-03", "EUR", "USD", 1.2),
            ("2025-01-02", "EUR", "GBP", 0.8), ("2025-01-03", "EUR", "GBP", 0.9)]
    spark.createDataFrame(rows, "ref_date string, base string, quote string, value double") \
        .createOrReplaceTempView("rates")
    (measure,) = definition()["measures"]

    per_pair = {(r["base"], r["quote"]): r["m"] for r in spark.sql(
        f"SELECT base, quote, {measure['expr']} AS m FROM rates GROUP BY base, quote").collect()}
    per_day = [r["m"] for r in spark.sql(
        f"SELECT ref_date, {measure['expr']} AS m FROM rates GROUP BY ref_date").collect()]

    assert per_pair == {("EUR", "USD"): pytest.approx(1.1), ("EUR", "GBP"): pytest.approx(0.85)}
    assert per_day == [None, None]


def test_an_amount_is_added_only_within_one_currency():
    contract = copy.deepcopy(c.load_contract("fincommons.firds.silver.main"))

    measures = {m["name"]: m for m in definition(contract, "t")["measures"]}

    assert measures["avg_total_issued_nominal_amount"]["expr"] == (
        "CASE WHEN COUNT(DISTINCT isin) <= 1 "
        "AND COUNT(DISTINCT total_issued_nominal_currency) <= 1 "
        "THEN AVG(total_issued_nominal_amount) END")


def test_a_stock_is_read_at_the_last_date_of_a_period():
    contract = copy.deepcopy(c.load_contract(EXCHANGERATE))
    value = column(contract, "value")
    value["customProperties"] = [
        p for p in value["customProperties"]
        if p["property"] not in ("aggregation", "aggregateWithin")] + [
        {"property": "aggregation", "value": "semiAdditive", "vendor": "fincommons"},
        {"property": "timeProperty", "value": "ref_date", "vendor": "fincommons"}]
    value["transformLogic"] = "SUM(value)"

    (measure,) = definition(contract)["measures"]

    assert measure["expr"] == "SUM(value)"
    assert measure["window"] == [{"order": "ref_date", "range": "current",
                                  "semiadditive": "last"}]


def test_a_measure_named_like_a_column_is_named_apart_from_it():
    contract = copy.deepcopy(c.load_contract(EXCHANGERATE))
    column(contract, "value")["businessName"] = "Base"

    (measure,) = definition(contract)["measures"]

    assert measure["name"] == "avg_base"


def test_the_statement_creates_the_view_from_the_definition():
    contract = c.load_contract(EXCHANGERATE)

    statement = semantic.create_metric_view(contract, SOURCE, SOURCE + "_metrics")

    head, body, tail = statement.split("$$")
    assert head == f"CREATE OR REPLACE VIEW {SOURCE}_metrics\nWITH METRICS LANGUAGE YAML AS "
    assert yaml.safe_load(body) == definition(contract)
    assert tail == ""


# The comments
# --------------------------------------------------------------------------


def test_every_column_has_its_description_as_its_comment(spark, tmp_path):
    """Run on a Delta table, so that a statement Spark refuses or a quote
    that ends a literal early fails here; the descriptions of the exchange
    rate table quote the names of the source columns."""
    contract = c.load_contract(EXCHANGERATE)
    spark.sql("DROP TABLE IF EXISTS semantic_rates")
    spark.sql(f"CREATE TABLE semantic_rates (ref_date DATE, base STRING, quote STRING, "
              f"value DOUBLE, type STRING, source STRING, imputed BOOLEAN) "
              f"USING delta LOCATION '{tmp_path / 'rates'}'")
    try:
        statements = semantic.comment_statements(contract, "semantic_rates")
        for statement in statements:
            spark.sql(statement)

        described = {r["col_name"]: r["comment"]
                     for r in spark.sql("DESCRIBE TABLE semantic_rates").collect()}
        table = spark.sql("DESCRIBE TABLE EXTENDED semantic_rates") \
            .where("col_name = 'Comment'").collect()[0]["data_type"]
    finally:
        spark.sql("DROP TABLE IF EXISTS semantic_rates")

    assert len(statements) == 1 + len(contract["schema"][0]["properties"])
    for prop in contract["schema"][0]["properties"]:
        assert described[prop["name"]] == " ".join(prop["description"].split())
    assert described["ref_date"].endswith("(from 'Date' in ECB data, 'TIME_PERIOD' in BIS data).")
    assert table.startswith("One row per calendar day")


# For a product, on a platform
# --------------------------------------------------------------------------


def test_a_product_is_served_from_the_tables_its_configuration_names(named):
    generated = semantic.databricks(named, "fincommons.exchangerate")

    *comments, view = generated["statements"]
    assert comments[0].startswith(f"COMMENT ON TABLE {SOURCE} IS ")
    assert view.startswith(f"CREATE OR REPLACE VIEW {SOURCE}_metrics\n")
    assert not any(s.rstrip().endswith(";") for s in generated["statements"])


@pytest.mark.parametrize("product", sorted(c.products()))
def test_every_shipped_product_can_be_served(named, product):
    generated = semantic.databricks(named, product)

    views = [s for s in generated["statements"] if s.startswith("CREATE OR REPLACE VIEW")]
    assert len(views) == len(c.products()[product]["outputPorts"])
    for statement in views:
        assert yaml.safe_load(statement.split("$$")[1])["fields"]


def test_a_draft_table_is_not_served(named, monkeypatch):
    # A draft contract specifies a table no code writes yet: no comment, no
    # metric view and no instructions describe it.
    shipped = c.contracts()
    draft = copy.deepcopy(shipped[EXCHANGERATE])
    draft["id"] = "fincommons.exchangerate.silver.planned"
    draft["status"] = "draft"
    draft["schema"][0]["name"] = "planned"
    product = copy.deepcopy(c.products()["fincommons.exchangerate"])
    product["outputPorts"].append({"name": "silver_planned", "contractId": draft["id"],
                                   "version": draft["version"]})
    monkeypatch.setattr(c, "contracts",
                        lambda packages=("fincommons",): {**shipped, draft["id"]: draft})
    monkeypatch.setattr(c, "products",
                        lambda packages=("fincommons",): {product["id"]: product})

    generated = semantic.databricks(named, "fincommons.exchangerate")

    assert not any("planned" in statement for statement in generated["statements"])
    assert [s for s in generated["statements"] if s.startswith("CREATE OR REPLACE VIEW")]
    assert "planned" not in generated["assistant"]


def test_a_table_addressed_by_path_cannot_be_served(tmp_path):
    config = tmp_path / "paths.yaml"
    config.write_text(yaml.safe_dump({"exchangerate": {"silver": str(tmp_path / "silver")}}))

    with pytest.raises(ValueError, match="addressed by path"):
        semantic.databricks(str(config), "fincommons.exchangerate")


def test_an_unknown_product_names_those_that_exist(named):
    with pytest.raises(KeyError, match="fincommons.exchangerate"):
        semantic.databricks(named, "fincommons.nosuchmodule")


def test_applying_runs_every_statement_and_returns_the_instructions(named):
    class Session:
        def __init__(self):
            self.ran = []

        def sql(self, statement):
            self.ran.append(statement)

    session = Session()

    instructions = semantic.apply_databricks(session, named, "fincommons.exchangerate")

    assert session.ran == semantic.databricks(named, "fincommons.exchangerate")["statements"]
    assert instructions.startswith("# exchangerate")


# The assistant
# --------------------------------------------------------------------------


def test_an_assistant_is_told_the_rules_the_product_and_its_contracts_state():
    text = semantic.assistant_instructions(c.products()["fincommons.exchangerate"])

    assert "the rate of US dollars in pounds is the EUR/GBP rate divided by" in text
    assert "A row is identified by ref_date, base, quote." in text
    assert "- Never sum value. Average it only within one value of base and quote." in text
    assert "- Do not invert a rate by swapping base and quote" in text
    assert "- What was the euro reference rate of the US dollar on 30 June 2025?" in text


def test_a_factor_that_compounds_is_to_be_multiplied_not_averaged():
    text = semantic.assistant_instructions(c.products()["fincommons.marketpricedata"])

    assert "- Never sum factor. Multiply it, and only within one value of instr_id." in text
    assert "- Never sum value. Average it only within one value of instr_id." in text


# The guards and the rules of aggregation
# --------------------------------------------------------------------------


def test_a_guard_reads_a_group_whose_currency_is_empty_throughout(spark):
    """A price quoted in percent has no currency; MIN = MAX would be empty
    there and take the measure with it."""
    contract = copy.deepcopy(c.load_contract("fincommons.firds.silver.main"))
    measures = {m["name"]: m for m in definition(contract, "t")["measures"]}
    expression = measures["avg_total_issued_nominal_amount"]["expr"]
    spark.createDataFrame(
        [("A", None, 1.0), ("A", None, 3.0), ("B", "EUR", 5.0), ("B", "USD", 7.0)],
        "isin string, total_issued_nominal_currency string, "
        "total_issued_nominal_amount double").createOrReplaceTempView("t")

    got = {r["isin"]: r["m"] for r in spark.sql(
        f"SELECT isin, {expression} AS m FROM t GROUP BY isin").collect()}

    assert got == {"A": pytest.approx(2.0), "B": None}


def with_aggregation(contract, name, logic, aggregation, **properties):
    prop = column(contract, name)
    prop["customProperties"] = [
        p for p in prop["customProperties"]
        if p["property"] not in ("aggregation", "aggregateWithin", "timeProperty")] + [
        {"property": "aggregation", "value": aggregation, "vendor": "fincommons"}] + [
        {"property": k, "value": v, "vendor": "fincommons"} for k, v in properties.items()]
    prop["transformLogic"] = logic
    return contract


def test_a_stock_may_be_summed_only_within_some_dimensions():
    contract = with_aggregation(copy.deepcopy(c.load_contract(EXCHANGERATE)), "value",
                                "SUM(value)", "semiAdditive", timeProperty="ref_date",
                                aggregateWithin=["base"])

    c.validate_contract(contract)  # raises where the contract is not valid
    (measure,) = definition(contract)["measures"]
    assert measure["expr"] == "CASE WHEN COUNT(DISTINCT base) <= 1 THEN SUM(value) END"
    assert measure["window"][0]["semiadditive"] == "last"
    rules = semantic._rules(contract)
    assert "Sum value only within one value of base." in rules
    assert "Do not sum value over ref_date: over a period, read it at the last ref_date of the period." in rules


def test_aggregate_within_names_columns_of_the_table():
    contract = with_aggregation(copy.deepcopy(c.load_contract(EXCHANGERATE)), "value",
                                "SUM(value)", "additive", aggregateWithin=["nowhere"])

    with pytest.raises(c.ContractError, match="aggregateWithin names columns of the table"):
        c.validate_contract(contract)


def test_a_measure_summed_within_groups_is_not_told_never_to_be_summed():
    contract = with_aggregation(copy.deepcopy(c.load_contract(EXCHANGERATE)), "value",
                                "SUM(value)", "nonAdditive", aggregateWithin=["base", "quote"])

    (rule,) = [r for r in semantic._rules(contract) if r.startswith(("Sum", "Never", "Do not"))]
    assert rule == ("Sum value only within one value of base and quote, as SUM(value); "
                    "across them it does not add up.")


def test_a_maximum_is_told_how_it_aggregates():
    contract = with_aggregation(copy.deepcopy(c.load_contract(EXCHANGERATE)), "value",
                                "MAX(value)", "nonAdditive", aggregateWithin=["base"])

    assert semantic._rules(contract)[0] == (
        "Do not sum value. Aggregate it only as MAX(value), and only within one value of base.")


# Tables written by path
# --------------------------------------------------------------------------


def test_a_table_written_by_path_is_registered_before_it_is_described(tmp_path):
    config = tmp_path / "paths.yaml"
    config.write_text(yaml.safe_dump({"exchangerate": {"silver": str(tmp_path / "silver")}}))
    where = semantic.Location(name="main.regime.rates", path=r"D:\lake\silver\fx\o'rates")

    generated = semantic.databricks(str(config), "fincommons.exchangerate",
                                    locate=lambda contract_id: where)

    register, comment, *_, view = generated["statements"]
    assert register == ("CREATE TABLE IF NOT EXISTS main.regime.rates USING DELTA "
                        r"LOCATION 'D:/lake/silver/fx/o\'rates'")
    assert comment.startswith("COMMENT ON TABLE main.regime.rates IS ")
    assert view.startswith("CREATE OR REPLACE VIEW main.regime.rates_metrics\n")


def test_a_partitioned_parquet_table_has_its_partitions_recovered():
    where = semantic.Location(name="`main`.`s`.`t`", path="/lake/t", format="parquet",
                              partitioned=True)

    assert semantic.register_statements(where) == [
        "CREATE TABLE IF NOT EXISTS `main`.`s`.`t` USING PARQUET LOCATION '/lake/t'",
        "MSCK REPAIR TABLE `main`.`s`.`t`"]
    assert semantic._view_name(where.name, "_metrics") == "`main`.`s`.`t_metrics`"


def test_a_table_is_registered_in_a_format_spark_reads():
    with pytest.raises(ValueError, match="delta or parquet"):
        semantic.Location(name="t", path="/lake/t", format="csv")


@pytest.mark.parametrize("fmt, partitioned", [("delta", False), ("parquet", True)])
def test_registration_gives_the_files_of_a_table_a_name(spark, tmp_path, fmt, partitioned):
    location = str(tmp_path / "registered")
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "x int, part string")
    writer = frame.write.format(fmt)
    (writer.partitionBy("part") if partitioned else writer).save(location)
    spark.sql("DROP TABLE IF EXISTS semantic_registered")
    try:
        statements = semantic.register_statements(semantic.Location(
            name="semantic_registered", path=location, format=fmt, partitioned=partitioned))
        for statement in statements + statements:  # a second run leaves it as it is
            spark.sql(statement)
        rows = spark.sql("SELECT x FROM semantic_registered ORDER BY x").collect()
    finally:
        spark.sql("DROP TABLE IF EXISTS semantic_registered")

    assert [r["x"] for r in rows] == [1, 2]
