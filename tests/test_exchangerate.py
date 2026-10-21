"""Tests of the exchange rate module.

The module builds one daily FX table out of up to three sources, so the tests
are about the rules that govern how those sources are combined: which source
wins where they overlap, how far the table is allowed to reach, and how the
gaps between quoted days are closed. Bronze fixtures are written inline, so
that every assertion refers to a rate that is known by construction.

The conversion helper :func:`fincommons.exchangerate.convert2ccy` is tested
on Spark frames against a Silver table written as Delta.
"""

from __future__ import annotations

import io
import os
import uuid
import zipfile
from datetime import date, datetime

import pandas
import pyarrow
import pyarrow.parquet
import pytest

import fincommons.exchangerate as ex


@pytest.fixture
def data_lake(tmp_path):
    """A configuration file pointing at an empty exchange rate data lake."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "exchangerate:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    ex.create_directory_tree(config_path)
    return config_path


# The Bronze layer
# --------------------------------------------------------------------------


def write_ecb(config_path, dates, **quotes):
    """Write a Bronze table in the wide shape the ECB publishes."""
    table = pyarrow.table({"Date": list(dates), **{q: list(v) for q, v in quotes.items()}})
    pyarrow.parquet.write_table(
        table,
        ex.path_bronze_ecb_exchangerate(config_path) + "/ecb_exchangerate.parquet",
    )


def write_internal(config_path, rows):
    """Write the optional proprietary source as the CSV the module expects."""
    lines = ["REFDATE,BASE,QUOTE,VALUE,TYPE,SOURCE"]
    lines += [f"{d},{base},{quote},{value},ref,internal" for d, base, quote, value in rows]
    with open(ex.path_bronze_internal_exchangerate(config_path) + "/exchangerate.csv", "w") as f:
        f.write("\n".join(lines) + "\n")


def silver(spark, config_path):
    """Read the Silver table back with the reference dates as text."""
    frame = ex.connect_to_exchangerate(spark, config_path).toPandas()
    frame["REFDATE"] = frame["REFDATE"].astype(str)
    return frame


def rate(frame, refdate, base, quote):
    """The single rate quoted for one pair on one day."""
    rows = frame[(frame.REFDATE == refdate) & (frame.BASE == base) & (frame.QUOTE == quote)]
    assert len(rows) == 1, f"expected one {base}/{quote} rate on {refdate}, got {len(rows)}"
    return rows.iloc[0]


def test_the_directory_tree_is_created(data_lake):
    for folder in [
        ex.path_bronze_bis_exchangerate(data_lake),
        ex.path_bronze_ecb_exchangerate(data_lake),
        ex.path_bronze_internal_exchangerate(data_lake),
        ex.path_silver_exchangerate(data_lake),
    ]:
        assert os.path.isdir(folder)


def test_creating_the_tree_twice_is_harmless(data_lake):
    ex.create_directory_tree(data_lake)


def test_the_tree_is_created_below_parents_that_do_not_exist(tmp_path):
    """The configuration may put the layers below a directory that has not been
    created yet, and setting the tree up should not require creating the path
    to it by hand first."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "exchangerate:\n"
        f"  bronze: {tmp_path / 'volume' / 'exchangerate' / 'bronze'}\n"
        f"  silver: {tmp_path / 'volume' / 'exchangerate' / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)

    ex.create_directory_tree(config_path)

    assert os.path.isdir(ex.path_bronze_internal_exchangerate(config_path))
    assert os.path.isdir(ex.path_silver_exchangerate(config_path))


# The download
# --------------------------------------------------------------------------


class FakeResponse:
    """The part of a streamed ``requests`` response the module uses."""

    def __init__(self, payload):
        self.payload = payload
        self.closed = False

    def iter_content(self, chunk_size):
        for start in range(0, len(self.payload), chunk_size):
            yield self.payload[start:start + chunk_size]

    def raise_for_status(self):
        """A successful response has nothing to raise."""

    def close(self):
        self.closed = True


def zipped_csv(name, text):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text)
    return buffer.getvalue()


def test_a_download_leaves_only_the_parquet_file_in_the_data_lake(monkeypatch, data_lake):
    """The archive is unpacked on local disk. Writing it into the Bronze layer
    only to delete it again would put megabytes through a Unity Catalog volume
    for nothing."""
    payload = zipped_csv("eurofxref-hist.csv", "Date,USD\n2020-01-01,1.10\n")
    monkeypatch.setattr(
        "fincommons.exchangerate.bronze.requests.get",
        lambda url, **kwargs: FakeResponse(payload),
    )

    ex.update_bronze_ecb_exchangerate(data_lake)

    folder = ex.path_bronze_ecb_exchangerate(data_lake)
    assert sorted(os.listdir(folder)) == ["ecb_exchangerate.parquet"]


def test_a_download_is_stored_as_parquet_unchanged(monkeypatch, data_lake):
    payload = zipped_csv("eurofxref-hist.csv", "Date,USD,GBP\n2020-01-01,1.10,0.90\n")
    monkeypatch.setattr(
        "fincommons.exchangerate.bronze.requests.get",
        lambda url, **kwargs: FakeResponse(payload),
    )

    ex.update_bronze_ecb_exchangerate(data_lake)

    written = pandas.read_parquet(
        ex.path_bronze_ecb_exchangerate(data_lake) + "/ecb_exchangerate.parquet"
    )
    assert list(written.columns) == ["Date", "USD", "GBP"]
    assert written["USD"].iloc[0] == pytest.approx(1.10)


def test_the_response_is_closed_even_when_the_archive_is_broken(monkeypatch, data_lake):
    monkeypatch.setattr(
        "fincommons.exchangerate.bronze.requests.get",
        lambda url, **kwargs: FakeResponse(b"not a zip file"),
    )

    with pytest.raises(zipfile.BadZipFile):
        ex.update_bronze_ecb_exchangerate(data_lake)


# The Silver layer
# --------------------------------------------------------------------------


def test_the_ecb_table_is_unpivoted_into_one_row_per_pair(spark, data_lake):
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10], GBP=[0.90])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert set(frame.QUOTE) == {"USD", "GBP"}
    assert set(frame.BASE) == {"EUR"}
    assert rate(frame, "2020-01-01", "EUR", "USD").VALUE == pytest.approx(1.10)
    assert rate(frame, "2020-01-01", "EUR", "GBP").VALUE == pytest.approx(0.90)


def test_quotes_that_are_missing_for_a_currency_are_dropped(spark, data_lake):
    # The ECB file carries a column per currency for the whole history, so a
    # currency that did not exist yet is present but empty.
    write_ecb(data_lake, ["2020-01-01", "2020-01-02"], USD=[1.10, 1.11], HRK=[None, 7.5])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert frame[(frame.QUOTE == "HRK") & (frame.REFDATE == "2020-01-01")].empty
    assert rate(frame, "2020-01-02", "EUR", "HRK").VALUE == pytest.approx(7.5)


def test_the_days_between_two_quotes_carry_the_earlier_rate(spark, data_lake):
    # The ECB quotes on business days only. A contract concluded on a Saturday
    # still has to be converted, so the table has to cover every calendar day.
    write_ecb(data_lake, ["2020-01-03", "2020-01-06"], USD=[1.10, 1.20])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert sorted(frame.REFDATE) == ["2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06"]
    assert rate(frame, "2020-01-04", "EUR", "USD").VALUE == pytest.approx(1.10)
    assert rate(frame, "2020-01-05", "EUR", "USD").VALUE == pytest.approx(1.10)
    assert rate(frame, "2020-01-06", "EUR", "USD").VALUE == pytest.approx(1.20)


def test_the_internal_source_takes_precedence_over_the_ecb(spark, data_lake):
    """The precedence must hold by construction and not by the order in which
    the rows of the union happen to arrive."""
    write_ecb(data_lake, ["2020-01-01", "2020-01-02"], USD=[1.10, 1.11])
    write_internal(data_lake, [("2020-01-02", "EUR", "USD", 9.99)])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    overlapping = rate(frame, "2020-01-02", "EUR", "USD")
    assert overlapping.VALUE == pytest.approx(9.99)
    assert overlapping.SOURCE == "internal"


def test_the_ecb_is_kept_where_the_internal_source_is_silent(spark, data_lake):
    write_ecb(data_lake, ["2020-01-01", "2020-01-02"], USD=[1.10, 1.11])
    write_internal(data_lake, [("2020-01-02", "EUR", "USD", 9.99)])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    uncontested = rate(frame, "2020-01-01", "EUR", "USD")
    assert uncontested.VALUE == pytest.approx(1.10)
    assert uncontested.SOURCE == "ECB"


def test_the_gap_between_two_sources_is_closed(spark, data_lake):
    """The days between two quotes are filled from the last quote that stands,
    whichever source it came from. Filling each source on its own would leave
    a hole wherever the next quote of a pair comes from a different source."""
    write_ecb(data_lake, ["2020-01-01", "2020-01-06"], USD=[1.10, 1.20])
    write_internal(
        data_lake,
        [("2020-01-03", "EUR", "USD", 9.99), ("2020-01-06", "EUR", "USD", 9.00)],
    )

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert sorted(frame.REFDATE) == [
        "2020-01-01",
        "2020-01-02",
        "2020-01-03",
        "2020-01-04",
        "2020-01-05",
        "2020-01-06",
    ]
    # 2 January stands on the ECB quote of 1 January, the last one before it,
    # even though the next quote of the pair is an internal one.
    assert rate(frame, "2020-01-02", "EUR", "USD").VALUE == pytest.approx(1.10)
    assert rate(frame, "2020-01-04", "EUR", "USD").VALUE == pytest.approx(9.99)


def test_the_table_stops_at_the_last_day_every_source_covers(spark, data_lake):
    # A source that lags behind would otherwise leave the last days of the
    # table resting on a subset of the sources.
    write_ecb(data_lake, ["2020-01-01", "2020-01-02", "2020-01-03"], USD=[1.10, 1.11, 1.12])
    write_internal(data_lake, [("2020-01-01", "EUR", "USD", 9.99)])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert max(frame.REFDATE) == "2020-01-01"


def test_the_silver_layer_is_parquet_unless_delta_is_asked_for(spark, data_lake):
    """Parquet is the default, because it is what every engine reads without a
    further dependency."""
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10])

    ex.update_silver_exchangerate(spark, data_lake)

    path = ex.path_silver_exchangerate_table(data_lake)
    assert not os.path.isdir(os.path.join(path, "_delta_log"))
    assert any(name.endswith(".parquet") for name in os.listdir(path))


def test_the_silver_layer_is_a_delta_table_where_that_is_asked_for(spark, data_lake):
    """A Delta table replaces itself in one transaction, so a reader never sees
    the half-written file that overwriting a parquet file in place would leave
    behind."""
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10])

    ex.update_silver_exchangerate(spark, data_lake, file_format="delta")

    assert os.path.isdir(ex.path_silver_exchangerate_table(data_lake) + "/_delta_log")


def test_the_table_is_read_back_in_the_format_it_was_written_in(spark, data_lake):
    """The reader takes the format from the table rather than from the caller.
    Reading a Delta table as plain parquet would not fail; it would return the
    superseded files of earlier versions along with the current ones."""
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10])
    ex.update_silver_exchangerate(spark, data_lake, file_format="delta")

    # A second run leaves the first version of the table behind on disk.
    write_ecb(data_lake, ["2020-01-01"], USD=[1.20])
    ex.update_silver_exchangerate(spark, data_lake, file_format="delta")

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert rate(frame, "2020-01-01", "EUR", "USD").VALUE == pytest.approx(1.20)


def test_an_unknown_file_format_is_reported_as_such(data_lake):
    # The format is checked before the session is put to work, which is why
    # there is no session here to put to work.
    with pytest.raises(ValueError, match="csv"):
        ex.update_silver_exchangerate(None, data_lake, file_format="csv")


def test_a_table_keeps_the_format_it_is_stored_in(spark, data_lake):
    """An update that does not ask for a format leaves a Delta table Delta,
    rather than throwing its history away with a parquet overwrite."""
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10])
    ex.update_silver_exchangerate(spark, data_lake, file_format="delta")

    ex.update_silver_exchangerate(spark, data_lake)

    assert os.path.isdir(ex.path_silver_exchangerate_table(data_lake) + "/_delta_log")
    with pytest.raises(ValueError, match="stored as 'delta'"):
        ex.update_silver_exchangerate(spark, data_lake, file_format="parquet")


# A Silver layer in the catalogue
# --------------------------------------------------------------------------


def catalogued(tmp_path, schema):
    """A configuration file whose Silver layer is a schema in the catalogue."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "exchangerate:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        "  silver:\n"
        f"    table: {schema}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    ex.create_directory_tree(config_path)
    return config_path


def test_a_silver_layer_in_the_catalogue_is_addressed_by_name(tmp_path):
    config_path = catalogued(tmp_path, "lake")

    assert ex.ref_silver_exchangerate_table(config_path).name == "lake.exchangerate"
    assert ex.path_silver_exchangerate_table(config_path) is None
    assert ex.silver_format(ex.ref_silver_exchangerate_table(config_path)) == "delta"


@pytest.mark.parametrize("update", ["update_silver_exchangerate", "update_exchangerate"])
def test_parquet_is_refused_for_a_silver_layer_in_the_catalogue(tmp_path, update):
    # Refused before the session is put to work, or anything downloaded.
    config_path = catalogued(tmp_path, "lake")

    with pytest.raises(ValueError, match="cannot be written as 'parquet'"):
        getattr(ex, update)(None, config_path, file_format="parquet")


@pytest.fixture
def schema(spark, tmp_path):
    """A schema in the session catalogue whose tables live below ``tmp_path``.

    It stands in for a Unity Catalog schema: the tables in it are addressed by
    name, and the catalogue decides where their files go.
    """
    name = f"exchangerate_{uuid.uuid4().hex[:12]}"
    spark.sql(f"CREATE SCHEMA {name} LOCATION '{(tmp_path / 'warehouse').as_posix()}'")
    yield name
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def test_the_silver_layer_is_written_into_the_catalogue_where_the_configuration_says_so(
    spark, tmp_path, schema
):
    """A change of the configuration file is all it takes: the update is
    called exactly as it is against a directory."""
    config_path = catalogued(tmp_path, schema)
    write_ecb(config_path, ["2020-01-01"], USD=[1.10])
    ex.update_silver_exchangerate(spark, config_path)

    write_ecb(config_path, ["2020-01-01"], USD=[1.20])
    ex.update_silver_exchangerate(spark, config_path)

    name = f"{schema}.exchangerate"
    assert spark.catalog.tableExists(name)
    assert spark.sql(f"DESCRIBE DETAIL {name}").first()["format"] == "delta"
    frame = silver(spark, config_path)
    assert len(frame) == 1
    assert rate(frame, "2020-01-01", "EUR", "USD").VALUE == pytest.approx(1.20)


def test_a_second_update_replaces_the_table_rather_than_adding_to_it(spark, data_lake):
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10])
    ex.update_silver_exchangerate(spark, data_lake)

    write_ecb(data_lake, ["2020-01-01"], USD=[1.20])
    ex.update_silver_exchangerate(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert rate(frame, "2020-01-01", "EUR", "USD").VALUE == pytest.approx(1.20)


def test_rebasing_expresses_the_table_in_another_currency(spark, data_lake):
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10], GBP=[0.90])

    ex.update_silver_exchangerate(spark, data_lake, rebase="USD")
    frame = silver(spark, data_lake)

    assert set(frame.BASE) == {"USD"}
    # A cross rate is the quotient of the two euro legs.
    assert rate(frame, "2020-01-01", "USD", "GBP").VALUE == pytest.approx(0.90 / 1.10)
    # The euro itself becomes a quote, at the reciprocal of the old rate.
    assert rate(frame, "2020-01-01", "USD", "EUR").VALUE == pytest.approx(1 / 1.10)


def test_rebasing_does_not_quote_the_new_base_against_itself(spark, data_lake):
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10], GBP=[0.90])

    ex.update_silver_exchangerate(spark, data_lake, rebase="USD")
    frame = silver(spark, data_lake)

    assert frame[frame.BASE == frame.QUOTE].empty


@pytest.mark.parametrize("rebase", ["EUR", "XXX"], ids=["euro", "unquoted"])
def test_a_rebase_without_a_euro_leg_is_refused_and_the_table_kept(spark, data_lake, rebase):
    """Rebasing to a currency the ECB does not quote, or to the euro itself,
    found no euro leg and replaced the table with an empty one."""
    write_ecb(data_lake, ["2020-01-01"], USD=[1.10], GBP=[0.90])
    ex.update_silver_exchangerate(spark, data_lake)
    before = silver(spark, data_lake)

    with pytest.raises(ValueError, match=repr(rebase) if rebase != "EUR" else "euro"):
        ex.update_silver_exchangerate(spark, data_lake, rebase=rebase)

    assert len(silver(spark, data_lake)) == len(before) > 0


# The conversion helper
# --------------------------------------------------------------------------


@pytest.fixture
def converted(spark, data_lake):
    """A Silver table with the euro as the base currency."""
    rows = [
        (date(2020, 1, 1), "EUR", "USD", 1.10, "ref", "ECB"),
        (date(2020, 1, 1), "EUR", "XAU", 0.00065, "ref", "ECB"),
        (date(2022, 3, 1), "EUR", "RUB", 120.0, "ref", "ECB"),
    ]
    spark.createDataFrame(
        rows, "REFDATE date, BASE string, QUOTE string, VALUE double, TYPE string, SOURCE string"
    ).write.format("delta").mode("overwrite").save(
        ex.path_silver_exchangerate_table(data_lake)
    )
    return data_lake


def amounts(config_path, data, **names):
    return ex.convert2ccy(data, config_path, target="EUR", **names)


def frame(spark, rows, date_type="string"):
    """A Spark frame of ``(n, date, currency, value)`` rows; ``n`` orders them."""
    return spark.createDataFrame(
        rows, f"n int, date {date_type}, currency string, value double"
    )


def column(result, name):
    """The values of one column, in the order of ``n``."""
    return [row[name] for row in result.orderBy("n").collect()]


# The Silver table is a Delta table written with the fixture's session, and
# convert2ccy reads it with the session of the frame it is given.


def test_an_amount_is_divided_by_the_rate_of_its_currency(spark, converted):
    data = frame(spark, [(1, "2020-01-01", "USD", 110.0)])

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert column(result, "value_EUR") == [pytest.approx(100.0)]
    assert column(result, "value_FXRATE") == [pytest.approx(1.10)]


def test_an_amount_already_in_the_target_currency_is_left_alone(spark, converted):
    data = frame(spark, [(1, "2020-01-01", "EUR", 100.0)])

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert column(result, "value_EUR") == [pytest.approx(100.0)]
    assert column(result, "value_FXRATE") == [1.0]


def test_a_currency_the_table_does_not_cover_yields_no_amount(spark, converted):
    data = frame(spark, [(1, "2020-01-01", None, 1.0), (2, "2020-01-01", "JPY", 1.0)])

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert column(result, "value_EUR") == [None, None]
    assert result.count() == 2


def test_a_rate_is_only_used_on_the_day_it_was_quoted(spark, converted):
    data = frame(spark, [(1, "2020-01-01", "RUB", 120.0), (2, "2022-03-01", "RUB", 120.0)])

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert column(result, "value_EUR") == [None, pytest.approx(1.0)]


def test_the_date_column_may_carry_any_name(spark, converted):
    data = spark.createDataFrame(
        [("2020-01-01", "USD", 110.0)], "REFDT string, ccy string, notional double"
    )

    result = amounts(converted, data, date="REFDT", amount="notional", currency="ccy")

    assert result.first()["notional_EUR"] == pytest.approx(100.0)


@pytest.mark.parametrize("date_type, day", [
    ("string", "2020-01-01"),
    ("date", date(2020, 1, 1)),
    ("timestamp", datetime(2020, 1, 1)),
])
def test_dates_may_be_given_as_text_dates_or_timestamps(spark, converted, date_type, day):
    data = frame(spark, [(1, day, "USD", 110.0)], date_type=date_type)

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert column(result, "value_EUR") == [pytest.approx(100.0)]
    # The date column is handed back as it came in.
    assert dict(result.dtypes)["date"] == date_type


@pytest.mark.parametrize("date_type, day", [
    ("string", "2020-01-01 10:00:00"),
    ("timestamp", datetime(2020, 1, 1, 10, 0)),
])
def test_an_amount_dated_with_a_time_of_day_takes_the_rate_of_its_day(spark, converted, date_type, day):
    """Matched on the timestamp itself, 10:00 met no rate quoted at midnight."""
    data = frame(spark, [(1, day, "USD", 110.0)], date_type=date_type)

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert column(result, "value_EUR") == [pytest.approx(100.0)]


def test_a_date_that_cannot_be_read_yields_no_amount_rather_than_an_error(spark, converted):
    """With ANSI mode on, the default from Spark 4.0, a plain cast would fail
    the whole conversion over one bad date."""
    data = frame(spark, [(1, "not a date", "USD", 110.0), (2, "2020-01-01", "USD", 110.0)])
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        result = amounts(converted, data, date="date", amount="value", currency="currency")
        values = column(result, "value_EUR")
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)

    assert values == [None, pytest.approx(100.0)]


def pandas_amounts():
    """Three amounts in an order and under an index a Spark join would not keep."""
    return pandas.DataFrame(
        {"date": ["2022-03-01", "2020-01-01", "2020-01-01"],
         "currency": ["RUB", "USD", "JPY"],
         "value": [120.0, 110.0, 1.0]},
        index=pandas.Index(["c", "a", "b"], name="trade"),
    )


def test_a_pandas_frame_comes_back_as_pandas_in_its_own_order(converted):
    data = pandas_amounts()

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert isinstance(result, pandas.DataFrame)
    assert list(result.index) == ["c", "a", "b"] and result.index.name == "trade"
    assert list(result.columns) == ["date", "currency", "value", "value_FXRATE", "value_EUR"]
    assert result["value_EUR"].iloc[0] == pytest.approx(1.0)
    assert result["value_EUR"].iloc[1] == pytest.approx(100.0)
    assert pandas.isna(result["value_EUR"].iloc[2])


def test_a_polars_frame_comes_back_as_polars_in_its_own_order(converted):
    polars = pytest.importorskip("polars")
    data = polars.from_pandas(pandas_amounts().reset_index(drop=True))

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert isinstance(result, polars.DataFrame)
    assert result.columns == ["date", "currency", "value", "value_FXRATE", "value_EUR"]
    converted_values = result["value_EUR"].to_list()
    assert converted_values[0] == pytest.approx(1.0)
    assert converted_values[1] == pytest.approx(100.0)
    assert converted_values[2] is None


def test_the_frame_that_was_passed_in_is_not_changed(converted):
    """Callers convert a column of a table they go on using, so the helper has
    to leave that table as it found it."""
    data = pandas_amounts()
    before = data.copy()

    amounts(converted, data, date="date", amount="value", currency="currency")

    pandas.testing.assert_frame_equal(data, before)


def test_a_missing_session_is_reported_as_such(monkeypatch, converted):
    """A pandas frame has no session of its own to read the table with."""
    monkeypatch.setattr(
        "fincommons.exchangerate.silver.SparkSession.getActiveSession",
        staticmethod(lambda: None),
    )

    with pytest.raises(RuntimeError, match="Pass a session"):
        amounts(converted, pandas_amounts(), date="date", amount="value", currency="currency")


def test_something_that_is_not_a_frame_is_refused(converted):
    with pytest.raises(TypeError, match="Spark, pandas or polars"):
        amounts(converted, [("2020-01-01", "USD", 1.0)],
                date="date", amount="value", currency="currency")


def test_the_columns_passed_in_come_back_unchanged_with_two_added(spark, converted):
    """Callers go on using the table they converted a column of, so nothing of
    it is changed or dropped, and no helper column is left behind."""
    data = frame(spark, [(1, "2020-01-01", "USD", 110.0), (2, "2020-01-01", "GBP", 5.0)])

    result = amounts(converted, data, date="date", amount="value", currency="currency")

    assert result.columns == data.columns + ["value_FXRATE", "value_EUR"]
    assert result.drop("value_FXRATE", "value_EUR").orderBy("n").collect() \
        == data.orderBy("n").collect()


# The download, when the server does not answer as it should
# --------------------------------------------------------------------------


class ErrorResponse(FakeResponse):
    """A response carrying an HTTP error status and an error page."""

    def __init__(self, status_code):
        super().__init__(b"<html>Service Unavailable</html>")
        self.status_code = status_code

    def raise_for_status(self):
        import requests
        raise requests.HTTPError(f"{self.status_code} Server Error", response=self)


def test_a_download_is_given_a_timeout(monkeypatch, data_lake):
    """Without one, a stalled connection holds the daily run indefinitely."""
    seen = {}

    def get(url, **kwargs):
        seen.update(kwargs)
        return FakeResponse(zipped_csv("eurofxref-hist.csv", "Date,USD\n2020-01-01,1.10\n"))

    monkeypatch.setattr("fincommons.exchangerate.bronze.requests.get", get)

    ex.update_bronze_ecb_exchangerate(data_lake)

    assert seen.get("timeout") is not None


def test_an_error_page_is_refused_as_an_http_error(monkeypatch, data_lake):
    """Written to disk, it failed as a file that is not a zip archive, which
    points at the archive rather than at the server."""
    import requests

    response = ErrorResponse(503)
    monkeypatch.setattr("fincommons.exchangerate.bronze.requests.get",
                        lambda url, **kwargs: response)

    with pytest.raises(requests.HTTPError):
        ex.update_bronze_ecb_exchangerate(data_lake)

    assert response.closed
    assert os.listdir(ex.path_bronze_ecb_exchangerate(data_lake)) == []


# The conversion helper, against a table rebased elsewhere
# --------------------------------------------------------------------------


def test_converting_into_a_currency_the_table_is_not_based_on_is_refused(spark, converted):
    """The fixture's table is based on the euro. Converting into the dollar
    found no rate at all, and every amount came back empty without a word."""
    data = frame(spark, [(1, "2020-01-01", "GBP", 100.0)])

    with pytest.raises(ValueError, match="rebase='USD'"):
        ex.convert2ccy(data, converted, date="date", amount="value",
                       currency="currency", target="USD")


def test_amounts_already_in_the_target_currency_need_no_rate(spark, converted):
    """Only an amount that has to be converted needs the table to hold rates
    for the target, so this is left alone rather than refused."""
    data = frame(spark, [(1, "2020-01-01", "USD", 100.0)])

    result = ex.convert2ccy(data, converted, date="date", amount="value",
                            currency="currency", target="USD")

    assert column(result, "value_USD") == [pytest.approx(100.0)]


# The internal source, checked before it is combined
# --------------------------------------------------------------------------


@pytest.mark.parametrize("row, problem", [
    (("2020-1-2", "EUR", "USD", 9.99), "REFDATE"),
    (("2020-01-02 00:00:00", "EUR", "USD", 9.99), "REFDATE"),
    (("2020-01-02", "USD", "GBP", 0.80), "BASE other than EUR"),
    (("2020-01-02", "EUR", "USD", 0.0), "not positive"),
], ids=["short date", "date with a time", "dollar base", "zero rate"])
def test_an_internal_row_the_combination_would_misread_is_refused(spark, data_lake, row, problem):
    write_ecb(data_lake, ["2020-01-01", "2020-01-02"], USD=[1.10, 1.11])
    write_internal(data_lake, [("2020-01-01", "EUR", "USD", 9.99), row])

    with pytest.raises(ValueError, match=problem):
        ex.update_silver_exchangerate(spark, data_lake)


def test_one_day_and_pair_quoted_twice_internally_is_refused(spark, data_lake):
    """The combination would keep either of the two, and nothing says which."""
    write_ecb(data_lake, ["2020-01-01", "2020-01-02"], USD=[1.10, 1.11])
    write_internal(data_lake, [("2020-01-02", "EUR", "USD", 9.99),
                               ("2020-01-02", "EUR", "USD", 8.88)])

    with pytest.raises(ValueError, match="more than once"):
        ex.update_silver_exchangerate(spark, data_lake)


# The BIS source
# --------------------------------------------------------------------------


def write_bis(config_path, rows):
    """Write a Bronze table in the flat shape the BIS publishes, with the
    column names it carries -- name and label joined by a colon."""
    frequency, collection, currency, period, value = zip(*rows)
    table = pyarrow.table({
        "FREQ:Frequency": list(frequency),
        "COLLECTION:Collection": list(collection),
        "CURRENCY:Currency": list(currency),
        "TIME_PERIOD:Time period or range": list(period),
        "OBS_VALUE:Observation Value": list(value),
    })
    pyarrow.parquet.write_table(
        table, ex.path_bronze_bis_exchangerate(config_path) + "/bis_exchangerate.parquet")


DAILY, AVERAGE = "D: Daily", "A: Average of observations through period"


def test_a_bis_rate_is_turned_into_a_euro_rate_through_the_dollar(spark, data_lake):
    """The BIS quotes national currency per US dollar; times the dollars per
    euro of the ECB that is national currency per euro."""
    write_ecb(data_lake, ["2020-01-02"], USD=[1.10])
    write_bis(data_lake, [(DAILY, AVERAGE, "ARS: Argentine peso", "2020-01-02", 60.0)])

    ex.update_silver_exchangerate(spark, data_lake, use_bis=True)
    frame = silver(spark, data_lake)

    quote = rate(frame, "2020-01-02", "EUR", "ARS")
    assert quote.VALUE == pytest.approx(66.0)
    assert quote.SOURCE == "BIS"


def test_the_ecb_takes_precedence_over_the_bis(spark, data_lake):
    write_ecb(data_lake, ["2020-01-02"], USD=[1.10], GBP=[0.90])
    write_bis(data_lake, [(DAILY, AVERAGE, "GBP: Pound sterling", "2020-01-02", 0.50)])

    ex.update_silver_exchangerate(spark, data_lake, use_bis=True)
    frame = silver(spark, data_lake)

    assert rate(frame, "2020-01-02", "EUR", "GBP").SOURCE == "ECB"


@pytest.mark.parametrize("frequency, collection", [
    ("M: Monthly", AVERAGE), (DAILY, "E: End of period"),
], ids=["monthly", "end of period"])
def test_only_daily_averages_are_taken_from_the_bis(spark, data_lake, frequency, collection):
    write_ecb(data_lake, ["2020-01-02"], USD=[1.10])
    write_bis(data_lake, [(DAILY, AVERAGE, "ARS: Argentine peso", "2020-01-02", 60.0),
                          (frequency, collection, "BRL: Brazilian real", "2020-01-02", 4.0)])

    ex.update_silver_exchangerate(spark, data_lake, use_bis=True)
    frame = silver(spark, data_lake)

    assert "BRL" not in set(frame.QUOTE)


# One row per day and pair
# --------------------------------------------------------------------------


@pytest.mark.parametrize("rebase", [None, "USD"], ids=["euro base", "rebased"])
def test_the_table_holds_one_rate_per_day_and_pair(spark, data_lake, rebase):
    """Every join downstream is on the day and the currency, so a second rate
    for either would duplicate every contract it touches. All three sources,
    a gap, and a day quoted by more than one of them."""
    write_ecb(data_lake, ["2020-01-02", "2020-01-03", "2020-01-06"],
              USD=[1.10, 1.11, 1.12], GBP=[0.90, 0.91, 0.92])
    write_internal(data_lake, [("2020-01-03", "EUR", "USD", 9.99),
                               ("2020-01-06", "EUR", "USD", 9.98)])
    write_bis(data_lake, [(DAILY, AVERAGE, "ARS: Argentine peso", day, 60.0)
                          for day in ("2020-01-02", "2020-01-03", "2020-01-06")]
                         + [(DAILY, AVERAGE, "GBP: Pound sterling", "2020-01-03", 0.5)])

    ex.update_silver_exchangerate(spark, data_lake, rebase=rebase, use_bis=True)
    frame = silver(spark, data_lake)

    assert not frame.duplicated(["REFDATE", "BASE", "QUOTE"]).any()


# Which days carry a rate from an earlier day
# --------------------------------------------------------------------------
#
# IMPUTED is true on a day whose rate was not quoted on it but carried from
# the last quote before it -- a weekend or a TARGET closing day wherever the
# ECB is the source, and any other day without a quote as well.


def imputed(frame, refdate, base, quote):
    return bool(rate(frame, refdate, base, quote).IMPUTED)


def test_a_day_carried_from_an_earlier_quote_is_marked_imputed(spark, data_lake):
    write_ecb(data_lake, ["2020-01-03", "2020-01-06"], USD=[1.10, 1.20])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    days = ["2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06"]
    assert [imputed(frame, day, "EUR", "USD") for day in days] == [False, True, True, False]


def test_a_weekday_without_a_quote_is_imputed_as_well(spark, data_lake):
    """1 January is a Wednesday in 2020 and a TARGET closing day, so the ECB
    published nothing for it."""
    write_ecb(data_lake, ["2019-12-31", "2020-01-02"], USD=[1.10, 1.20])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert imputed(frame, "2020-01-01", "EUR", "USD") is True
    assert imputed(frame, "2020-01-02", "EUR", "USD") is False


def test_a_quote_on_a_weekend_is_not_imputed(spark, data_lake):
    """What decides it is whether a source quoted the day, not what day of
    the week it is: the internal source quotes this Saturday."""
    write_ecb(data_lake, ["2020-01-03", "2020-01-06"], USD=[1.10, 1.20])
    write_internal(data_lake, [("2020-01-04", "EUR", "USD", 1.15),
                               ("2020-01-06", "EUR", "USD", 1.20)])

    ex.update_silver_exchangerate(spark, data_lake)
    frame = silver(spark, data_lake)

    assert imputed(frame, "2020-01-04", "EUR", "USD") is False
    assert imputed(frame, "2020-01-05", "EUR", "USD") is True


def test_a_cross_rate_is_imputed_where_either_of_its_legs_is(spark, data_lake):
    """Rebased to the dollar, the pound on Saturday is quoted in euros by the
    internal source while the dollar is carried from Friday. The cross rate
    rests partly on a carried rate, so it is imputed."""
    write_ecb(data_lake, ["2020-01-03", "2020-01-06"], USD=[1.10, 1.20], GBP=[0.90, 0.92])
    write_internal(data_lake, [("2020-01-04", "EUR", "GBP", 0.91),
                               ("2020-01-06", "EUR", "GBP", 0.92)])

    ex.update_silver_exchangerate(spark, data_lake, rebase="USD")
    frame = silver(spark, data_lake)

    assert imputed(frame, "2020-01-03", "USD", "GBP") is False
    assert imputed(frame, "2020-01-04", "USD", "GBP") is True
    assert imputed(frame, "2020-01-03", "USD", "EUR") is False
    assert imputed(frame, "2020-01-04", "USD", "EUR") is True


def test_the_imputed_column_is_boolean_and_never_empty(spark, data_lake):
    from pyspark.sql.types import BooleanType

    write_ecb(data_lake, ["2020-01-03", "2020-01-06"], USD=[1.10, 1.20])

    ex.update_silver_exchangerate(spark, data_lake)
    table = ex.connect_to_exchangerate(spark, data_lake)

    assert isinstance(table.schema["IMPUTED"].dataType, BooleanType)
    assert table.filter("IMPUTED IS NULL").count() == 0
