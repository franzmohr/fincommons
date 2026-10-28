"""Tests of the market price data module.

The module collects prices and rates from several publishers and reduces all of
them to one table of three columns, so the tests are about what survives that
reduction: which series is picked out of a flow that carries many, what an
instrument ends up being called, and what happens when the same day is
delivered twice. Bronze fixtures are written inline and no test reaches the
ECB, the New York Fed, the Bank of England or the LBMA, so every assertion
refers to a value that is known by construction.

The exchange rate source is the one that is not fed from the Bronze layer but
from the Silver table of :mod:`fincommons.exchangerate`. That module is not
run here; its table is stood in for by a frame of the shape it publishes, so
what is asserted is the transformation this module applies to it.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import date

import pyarrow
import pyarrow.parquet
import pytest
import requests

import fincommons.marketpricedata as mp
from fincommons.marketpricedata import bronze_ecb, bronze_yahoo, download, pipeline
from fincommons.schemas import load_pyspark_schema
from pyspark.sql.types import DateType


@pytest.fixture
def data_lake(tmp_path):
    """A configuration file pointing at an empty market price data lake."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)
    mp.create_directory_tree(config_path)
    return config_path


class FakeResponse:
    """The part of a streamed ``requests`` response the module uses."""

    def __init__(self, payload, fails=False, status_code=200):
        self.payload = payload
        self.fails = fails
        self.status_code = status_code
        self.closed = False

    @property
    def text(self):
        return self.payload.decode("utf-8")

    def json(self):
        return json.loads(self.payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} for the test")

    def iter_content(self, chunk_size):
        for start in range(0, len(self.payload), chunk_size):
            if self.fails:
                raise OSError("the connection dropped")
            yield self.payload[start:start + chunk_size]

    def close(self):
        self.closed = True


def answer_with(monkeypatch, module, response):
    """Let the download of ``module`` find ``response``, and record the call.

    The ECB, the New York Fed, the Bank of England and the LBMA all download
    through :mod:`fincommons.marketpricedata.download`, so that is where the
    request is answered whichever ``module`` is named; the name is kept so that
    a test says which source it is about.

    The stub takes the arguments the download sends rather than ``**kwargs``,
    so that an argument the module stops sending -- the timeout that bounds the
    wait on a publisher, above all -- is a failing test here and not something
    a stub quietly absorbs.
    """
    call = {}

    def get(url, params=None, headers=None, stream=False, timeout=None):
        call["url"] = url
        call["params"] = params
        call["headers"] = headers or {}
        call["stream"] = stream
        call["timeout"] = timeout
        return response

    monkeypatch.setattr(
        "fincommons.marketpricedata.download.requests.get", get)
    return call


def estr_csv(*rows):
    """An answer of the ECB API in the shape the ``EST`` flow is published in.

    The header is taken from the schema the module parses with rather than
    written out here, so that a column added to the schema does not silently
    shift the fixture's values into the wrong fields.
    """
    fields = [f.name for f in load_pyspark_schema("marketpricedata_ecb_estr").fields]
    lines = [",".join(fields)]
    for row in rows:
        lines.append(",".join(str(row.get(f, "")) for f in fields))
    return ("\n".join(lines) + "\n").encode("utf-8")


def estr_row(refdt, value, freq="B", item="EU000A2X2A25", data_type="WT"):
    return {
        "KEY": f"EST.{freq}.{item}.{data_type}",
        "FREQ": freq,
        "BENCHMARK_ITEM": item,
        "DATA_TYPE_EST": data_type,
        "TIME_PERIOD": refdt,
        "OBS_VALUE": value,
    }


def fm_csv(*rows):
    """An answer of the ECB API in the shape the ``FM`` flow is published in.

    As with the ``EST`` flow, the header is taken from the schema the module
    parses with rather than written out here.
    """
    fields = [f.name for f in load_pyspark_schema("marketpricedata_ecb_fm").fields]
    lines = [",".join(fields)]
    for row in rows:
        lines.append(",".join(str(row.get(f, "")) for f in fields))
    return ("\n".join(lines) + "\n").encode("utf-8")


def fm_row(refdt, value, freq="D", rate="DFR", data_type="LEV",
           area="U2", instrument="KR"):
    """One observation of the ``FM`` flow, defaulting to the deposit facility.

    ``PROVIDER_FM_ID`` is the flow's own code for the series and is what the
    Silver stages read; it is kept in step with ``KEY`` here so that a fixture
    cannot say one thing in the key and another in the dimension.
    """
    return {
        "KEY": f"FM.{freq}.{area}.EUR.4F.{instrument}.{rate}.{data_type}",
        "FREQ": freq,
        "REF_AREA": area,
        "CURRENCY": "EUR",
        "PROVIDER_FM": "4F",
        "INSTRUMENT_FM": instrument,
        "PROVIDER_FM_ID": rate,
        "DATA_TYPE_FM": data_type,
        "TIME_PERIOD": refdt,
        "OBS_VALUE": value,
    }


def nyfed_json(*rows):
    """An answer of the New York Fed markets API.

    The observations arrive inside one ``refRates`` array rather than one to a
    line, which is why the download reads the file whole. Each carries the
    distribution the rate was drawn from and the flag saying whether it has been
    revised, none of which the mirror published.
    """
    def around(rate, offset):
        """The percentiles sit either side of the rate, or are absent with it."""
        return None if rate is None else rate + offset

    payload = {"refRates": [
        {"effectiveDate": d, "type": "SOFR", "percentRate": rate,
         "percentPercentile1": around(rate, -0.04),
         "percentPercentile25": around(rate, -0.02),
         "percentPercentile75": around(rate, 0.05),
         "percentPercentile99": around(rate, 0.08),
         "volumeInBillions": volume, "revisionIndicator": revision}
        for d, rate, volume, revision in rows
    ]}
    return json.dumps(payload).encode("utf-8")


def answer_nyfed_with(monkeypatch, response):
    """Let the New York Fed download find ``response``, and record the call."""
    return answer_with(monkeypatch, "bronze_nyfed", response)


def lbma_json(*rows):
    """An answer of the LBMA price service.

    One array of objects rather than one object per line, which is why the
    download reads the file whole. ``v`` holds the same price in US dollars,
    pounds sterling and euro, in that order, and the euro entry is null before
    the currency existed.
    """
    payload = [{"is_cms_locked": 0, "d": d, "v": list(v)} for d, v in rows]
    return json.dumps(payload).encode("utf-8")


def answer_lbma_with(monkeypatch, response):
    """Let the LBMA download find ``response``, and record the call."""
    return answer_with(monkeypatch, "bronze_lbma", response)


def electricity_json(*rows, unit="EUR / MWh"):
    """An answer of the electricity service.

    ``unix_seconds`` and ``price`` are parallel arrays with one entry per
    delivery period, which is a quarter of an hour in most zones and an hour in
    a few, so a day arrives as many entries rather than one.
    """
    return json.dumps({
        "unix_seconds": [s for s, _ in rows],
        "price": [p for _, p in rows],
        "unit": unit,
        "license_info": "CC BY 4.0 from Bundesnetzagentur | SMARD.de",
        "deprecated": False,
    }).encode("utf-8")


class FakeElectricity:
    """The electricity service, refusing a given number of times first."""

    def __init__(self, payload, refusals=0, status_code=429):
        self.payload = payload
        self.refusals = refusals
        self.status_code = status_code
        self.calls = []

    def __call__(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params,
                           "headers": headers or {}, "timeout": timeout})
        if len(self.calls) <= self.refusals:
            return FakeResponse(b"too many requests", status_code=self.status_code)
        return FakeResponse(self.payload)


def answer_electricity_with(monkeypatch, service):
    """Let the electricity download find ``service`` instead of the source."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.requests.get", service)
    # The retry sleeps; a test should not.
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.time.sleep",
        lambda seconds: None)
    return service


def boe_csv(series, *rows):
    """An answer of the Bank's statistical database.

    The Bank writes its dates as ``02 Jan 1997`` rather than as an ISO date,
    which is the whole reason the download parses them with a format instead of
    casting them, so the fixture writes them the way the Bank does.
    """
    lines = [f"DATE,{series}"]
    lines += [f"{d},{v}" for d, v in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def answer_boe_with(monkeypatch, response):
    """Let the Bank of England download find ``response``, and record the call.

    The recorded call is what the user agent is asserted on: the request has to
    carry one.
    """
    return answer_with(monkeypatch, "bronze_boe", response)


def write_internal(path, rows):
    """Write a proprietary delivery in the form the module picks it up in.

    Nothing in the package writes this file: it is placed in the Bronze layer
    by whoever maintains the quotes, so the fixture writes the CSV out itself
    rather than through a function of the module.
    """
    os.makedirs(path, exist_ok=True)
    lines = ["REFDT,INSTRID,VALUE"]
    lines += [f"{refdt},{instrid},{value}" for refdt, instrid, value in rows]
    with open(os.path.join(path, "marketpricedata_internal.csv"), "w",
              encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def write_yahoo_bronze(path, lastdt, rows):
    """Write one download of the Yahoo source into the partition it lands in.

    That Bronze layer is partitioned by the day of the download and appended to
    rather than overwritten, so a fixture is written one download at a time and
    the partitions accumulate, as they do in a data lake that has been running.
    """
    folder = os.path.join(path, f"LASTDT={lastdt}")
    os.makedirs(folder, exist_ok=True)
    table = pyarrow.table(
        {
            "REFDT": pyarrow.array([date.fromisoformat(d) for d, _, _ in rows],
                                   type=pyarrow.date32()),
            "ISIN": [i for _, i, _ in rows],
            "VALUE": [float(v) for _, _, v in rows],
        }
    )
    # A unique name, as Spark gives each file it appends to a partition.
    pyarrow.parquet.write_table(
        table, os.path.join(folder, f"part-{uuid.uuid4().hex}.parquet"))


def write_yahoo_commodity_bronze(path, lastdt, rows):
    """Write one download of the Yahoo commodity source into its partition."""
    folder = os.path.join(path, f"LASTDT={lastdt}")
    os.makedirs(folder, exist_ok=True)
    table = pyarrow.table(
        {
            "REFDT": pyarrow.array([date.fromisoformat(d) for d, _, _ in rows],
                                   type=pyarrow.date32()),
            "INSTRID": [i for _, i, _ in rows],
            "VALUE": [float(v) for _, _, v in rows],
        }
    )
    pyarrow.parquet.write_table(table, os.path.join(folder, "part.parquet"))


def silver(spark, config_path):
    """Read the Silver table back with the reference dates as text."""
    frame = mp.connect_to_marketpricedata(spark, config_path).toPandas()
    frame["REFDT"] = frame["REFDT"].astype(str)
    return frame


def value_of(frame, instrid, refdt):
    """The single value quoted for one instrument on one day."""
    rows = frame[(frame.INSTRID == instrid) & (frame.REFDT == refdt)]
    assert len(rows) == 1, f"expected one {instrid} on {refdt}, got {len(rows)}"
    return rows.iloc[0].VALUE


# The data lake
# --------------------------------------------------------------------------


def test_the_directory_tree_is_created(data_lake):
    for folder in [
        mp.path_bronze_ecb_estr(data_lake),
        mp.path_bronze_ecb_fm(data_lake),
        mp.path_bronze_nyfed_sofr(data_lake),
        mp.path_bronze_boe_sonia(data_lake),
        mp.path_bronze_internal_marketpricedata(data_lake),
        mp.path_bronze_yahoo_marketpricedata(data_lake),
        mp.path_silver_base(data_lake),
    ]:
        assert os.path.isdir(folder)


def test_creating_the_tree_twice_is_harmless(data_lake):
    mp.create_directory_tree(data_lake)


def test_the_tree_is_created_below_parents_that_do_not_exist(tmp_path):
    """The configuration may put the layers below a directory that has not been
    created yet, and setting the tree up should not require creating the path
    to it by hand first."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'volume' / 'marketpricedata' / 'bronze'}\n"
        f"  silver: {tmp_path / 'volume' / 'marketpricedata' / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)

    mp.create_directory_tree(config_path)

    assert os.path.isdir(mp.path_bronze_nyfed_sofr(config_path))
    assert os.path.isdir(mp.path_silver_base(config_path))


def test_every_path_is_read_from_the_configuration_file(tmp_path):
    """The module addresses the data lake through the configuration file alone.
    Two configurations must therefore lead to two data lakes, with nothing left
    pointing at a root written into the code."""
    first = tmp_path / "first.yaml"
    first.write_text(
        f"marketpricedata:\n  bronze: {tmp_path / 'a' / 'b'}\n"
        f"  silver: {tmp_path / 'a' / 's'}\n",
        encoding="utf-8",
    )
    second = tmp_path / "second.yaml"
    second.write_text(
        f"marketpricedata:\n  bronze: {tmp_path / 'z' / 'b'}\n"
        f"  silver: {tmp_path / 'z' / 's'}\n",
        encoding="utf-8",
    )

    for helper in [
        mp.path_bronze_ecb_estr,
        mp.path_bronze_nyfed_sofr,
        mp.path_bronze_boe_sonia,
        mp.path_bronze_internal_marketpricedata,
        mp.path_bronze_yahoo_marketpricedata,
        mp.path_silver_marketpricedata,
    ]:
        assert helper(str(first)) != helper(str(second))
        assert str(tmp_path / "a") in helper(str(first))
        assert str(tmp_path / "z") in helper(str(second))


def test_the_silver_table_is_addressed_by_name_where_the_layer_names_a_schema(
    tmp_path,
):
    """A configuration file that gives the Silver layer a catalogue prefix is
    describing a table in a schema, and the pipeline has to address it by name:
    that is what puts it within reach of Unity Catalog's grants and lineage."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        "  silver:\n"
        "    table: fincommons.marketpricedata\n"
        f"    path: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )
    config_path = str(config_path)

    ref = mp.ref_silver_marketpricedata(config_path)

    assert ref.is_named
    assert ref.name == "fincommons.marketpricedata.marketpricedata"


def test_a_schema_in_a_catalogue_is_not_created_as_a_directory(tmp_path):
    """Setting up a local data lake creates the Silver layer where it is a
    directory. Where it is a schema in a catalogue it is provisioned outside
    the pipeline, and the path beside it belongs to the marks alone."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        "  silver:\n"
        "    table: fincommons.marketpricedata\n",
        encoding="utf-8",
    )
    config_path = str(config_path)

    mp.create_directory_tree(config_path)

    assert os.path.isdir(mp.path_bronze_nyfed_sofr(config_path))
    assert mp.path_silver_base(config_path) is None


def test_a_silver_layer_without_a_path_has_nowhere_to_keep_the_marks(tmp_path):
    """The marks that say how far each source has been merged are files, and a
    schema in a catalogue is not a directory to write them to. Going on without
    them would have every incremental source collect and merge its whole
    history again on every run, so the configuration is refused instead."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        "marketpricedata:\n"
        f"  bronze: {tmp_path / 'bronze'}\n"
        "  silver:\n"
        "    table: fincommons.marketpricedata\n",
        encoding="utf-8",
    )
    config_path = str(config_path)

    with pytest.raises(ValueError, match="path"):
        mp.path_silver_watermarks(config_path)


# The download
# --------------------------------------------------------------------------


def test_a_download_leaves_no_csv_file_in_the_data_lake(monkeypatch, spark, data_lake):
    """The answer is fetched into a temporary directory on local disk. Writing
    it into the Bronze layer only to delete it again would put a file through a
    Unity Catalog volume that is never meant to be kept."""
    answer_with(monkeypatch, "bronze_ecb",
                FakeResponse(estr_csv(estr_row("2024-01-02", 3.9))))

    mp.update_bronze_ecb_estr(spark, data_lake)

    left_behind = [
        os.path.join(root, name)
        for root, _, names in os.walk(mp.path_bronze_base(data_lake))
        for name in names
        if name.endswith(".csv")
    ]
    assert left_behind == []


def test_the_header_row_of_an_ecb_answer_is_not_read_as_data(
    monkeypatch, spark, data_lake
):
    """A reader that is given a schema does not skip the header by itself. Where
    it is parsed it arrives as a row whose typed fields are all null, because
    neither TIME_PERIOD nor OBS_VALUE reads as a date or a number from its own
    column name, and that row then sits in the Bronze layer as an observation
    the source never published."""
    answer_with(monkeypatch, "bronze_ecb",
                FakeResponse(estr_csv(estr_row("2024-01-02", 3.9))))

    mp.update_bronze_ecb_estr(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_ecb_estr(data_lake))
    assert bronze.count() == 1
    assert bronze.first()["KEY"] != "KEY"
    assert bronze.first()["OBS_VALUE"] == pytest.approx(3.9)


def test_the_response_is_closed_even_when_the_download_fails(monkeypatch, spark, data_lake):
    response = FakeResponse(estr_csv(estr_row("2024-01-02", 3.9)), fails=True)
    answer_with(monkeypatch, "bronze_ecb", response)

    with pytest.raises(OSError):
        mp.update_bronze_ecb_estr(spark, data_lake)

    assert response.closed


def test_the_rate_is_kept_with_what_it_was_drawn_from(
        monkeypatch, spark, data_lake):
    """The administrator publishes the distribution and the traded volume beside
    the rate, and a revision flag with it. The Silver table has three columns
    and can hold the rate alone, so the Bronze layer is where the rest survives
    -- which is most of the reason for reading the administrator rather than a
    mirror that carried the rate on its own."""
    answer_nyfed_with(monkeypatch, FakeResponse(nyfed_json(
        ("2024-01-02", 5.31, 1600.0, ""),
        ("2024-01-03", 5.34, 1712.0, "Y"),
    )))

    mp.update_bronze_nyfed_sofr(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_nyfed_sofr(data_lake)).orderBy("date")
    assert bronze.count() == 2
    assert sorted(bronze.columns) == ["date", "percentile1", "percentile25",
                                      "percentile75", "percentile99", "revision",
                                      "sofr", "volume"]

    first = bronze.first()
    assert dict(bronze.dtypes)["date"] == "date"
    assert dict(bronze.dtypes)["sofr"] == "double"
    assert str(first["date"]) == "2024-01-02"
    assert first["sofr"] == pytest.approx(5.31)
    assert first["volume"] == pytest.approx(1600.0)
    assert bronze.collect()[1]["revision"] == "Y"


def test_the_new_york_fed_download_asks_for_the_whole_history(
        monkeypatch, spark, data_lake):
    """The API takes a range rather than a count -- the ``last/n`` form refuses a
    count large enough to cover the history -- so the whole of it is asked for by
    date. The request carries a user agent, as the other two do."""
    call = answer_nyfed_with(
        monkeypatch, FakeResponse(nyfed_json(("2024-01-02", 5.31, 1600.0, ""))))

    mp.update_bronze_nyfed_sofr(spark, data_lake)

    assert "User-Agent" in call["headers"]
    assert call["params"]["startDate"] < "2018-04-02"
    assert call["params"]["endDate"] > "2026-01-01"


def test_a_day_without_a_rate_is_not_read_as_an_observation(
        monkeypatch, spark, data_lake):
    """A rate the administrator has no value for arrives as null rather than as
    the full stop FRED uses, but it must not reach the table either way."""
    answer_nyfed_with(monkeypatch, FakeResponse(nyfed_json(
        ("2024-01-01", None, None, ""),
        ("2024-01-02", 5.31, 1600.0, ""),
    )))

    mp.update_bronze_nyfed_sofr(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_nyfed_sofr(data_lake))
    assert bronze.count() == 1
    assert str(bronze.first()["date"]) == "2024-01-02"


def test_the_bank_of_england_download_sends_a_user_agent(
        monkeypatch, spark, data_lake):
    """The Bank serves its database through a content delivery network that
    refuses a request without a user agent.

    This is pinned because of how it fails rather than because it fails. The
    refusal comes back as an HTML page with a 403, and a CSV reader given that
    page returns a table of one column and no rows -- so a download that lost
    its user agent would write an empty Bronze layer and report success."""
    call = answer_boe_with(
        monkeypatch, FakeResponse(boe_csv("IUDSOIA", ("02 Jan 2024", "5.19"))))

    mp.update_bronze_boe_sonia(spark, data_lake)

    assert "User-Agent" in call["headers"]
    assert call["params"]["SeriesCodes"] == "IUDSOIA"


def test_a_refused_download_leaves_the_bronze_layer_alone(
        monkeypatch, spark, data_lake):
    """A refusal has to stop the download rather than be parsed as data, so that
    the Bronze layer keeps what the last run put there."""
    answer_boe_with(monkeypatch,
                    FakeResponse(boe_csv("IUDSOIA", ("02 Jan 2024", "5.19"))))
    mp.update_bronze_boe_sonia(spark, data_lake)

    answer_boe_with(monkeypatch,
                    FakeResponse(b"<HTML><HEAD><TITLE>Access Denied</TITLE>",
                                 status_code=403))
    with pytest.raises(requests.HTTPError):
        mp.update_bronze_boe_sonia(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_boe_sonia(data_lake))
    assert bronze.count() == 1


#: An error page as a content delivery network or a proxy serves it, with
#: status 200. One of its lines carries a comma, so that a CSV reader finds a
#: second field in it rather than only lines of one column.
ERROR_PAGE = (
    b"<!DOCTYPE html>\n<html>\n<head><title>Service unavailable</title></head>\n"
    b"<body><p>Sorry, the service is unavailable. Please try later.</p></body>\n"
    b"</html>\n"
)

#: The sources whose series are written over the Bronze layer whole, each with
#: an answer holding one observation, the download and the Bronze layer.
WHOLE_SERIES_SOURCES = {
    "ECB": (lambda: estr_csv(estr_row("2024-01-02", 3.9)),
            "update_bronze_ecb_estr", "path_bronze_ecb_estr"),
    "BOE": (lambda: boe_csv("IUDSOIA", ("02 Jan 2024", "5.19")),
            "update_bronze_boe_sonia", "path_bronze_boe_sonia"),
    "NYFED": (lambda: nyfed_json(("2024-01-02", 5.31, 1600.0, "")),
              "update_bronze_nyfed_sofr", "path_bronze_nyfed_sofr"),
    "LBMA": (lambda: lbma_json(("2024-01-02", (2050.0, 1610.0, 1860.0))),
             "update_bronze_lbma_gold", "path_bronze_lbma_gold"),
}


@pytest.mark.parametrize("source", ["ECB", "BOE", "NYFED", "LBMA"])
@pytest.mark.parametrize("payload", [b"", ERROR_PAGE],
                         ids=["empty answer", "error page with status 200"])
def test_an_answer_without_an_observation_leaves_the_bronze_layer_alone(
        monkeypatch, spark, data_lake, source, payload):
    """Each of these series is downloaded whole and written over the Bronze
    layer. An answer that parses to nothing -- an empty body, or an error page
    sent with status 200 -- would replace years of history with nothing while
    the run reported success, so it has to fail, name the source, and leave the
    series the last run wrote where it is."""
    answer, update, path = WHOLE_SERIES_SOURCES[source]
    update, path = getattr(mp, update), getattr(mp, path)

    answer_with(monkeypatch, source, FakeResponse(answer()))
    update(spark, data_lake)

    answer_with(monkeypatch, source, FakeResponse(payload))
    with pytest.raises(ValueError, match=f"answer of {source}"):
        update(spark, data_lake)

    bronze = spark.read.parquet(path(data_lake))
    assert bronze.count() == 1


def test_an_empty_answer_is_refused_before_it_is_read(monkeypatch):
    """The shared download refuses an answer without a single byte itself, so
    that no reader is ever given it, and it closes the response all the
    same."""
    response = FakeResponse(b"")
    answer_with(monkeypatch, "download", response)

    with pytest.raises(ValueError, match="answer of BOE"):
        with download.downloaded("https://example.org/series", "BOE"):
            pytest.fail("an empty answer reached the reader")

    assert response.closed


def test_the_shared_download_sends_a_user_agent_and_a_timeout(monkeypatch):
    """Two publishers refuse a request without a user agent, and a request
    without a timeout can hang a scheduled run on one publisher for good. Both
    are therefore the download's business rather than each source's, and the
    file it yields is gone once the block that reads it has finished."""
    call = answer_with(monkeypatch, "download", FakeResponse(b"DATE,X\n"))

    with download.downloaded("https://example.org/series", "TEST",
                             params={"a": "b"}) as file_path:
        with open(file_path, "rb") as fd:
            assert fd.read() == b"DATE,X\n"

    assert not os.path.exists(file_path)
    assert call["headers"]["User-Agent"] == "fincommons"
    assert call["timeout"] == mp.HTTP_TIMEOUT
    assert call["stream"] is True
    assert call["params"] == {"a": "b"}


def test_sonia_is_read_with_the_dates_the_bank_writes(
        monkeypatch, spark, data_lake):
    """The Bank dates an observation ``02 Jan 2024``. Cast rather than parsed,
    every row of the series would come out null and the download would write an
    empty data set without failing."""
    answer_boe_with(
        monkeypatch,
        FakeResponse(boe_csv("IUDSOIA",
                             ("02 Jan 2024", "5.19"),
                             ("03 Jan 2024", "5.2"))))

    mp.update_bronze_boe_sonia(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_boe_sonia(data_lake)).orderBy("date")
    assert bronze.count() == 2
    assert str(bronze.first()["date"]) == "2024-01-02"


def test_sofr_reaches_the_table_as_on_sofr(
        monkeypatch, spark, data_lake):
    """SOFR is collected from the New York Fed, which administers it. As with
    SONIA the identifier names the rate and not the publisher, so it does not
    depend on the source.

    The New York Fed and the FRED mirror agree date by date on every one of
    the 2,112 dates they share, so the administrator adds the first observation
    the mirror lacks and alters nothing already collected."""
    answer_nyfed_with(monkeypatch,
                      FakeResponse(nyfed_json(("2024-01-02", 5.31, 1600.0, ""))))
    mp.update_bronze_nyfed_sofr(spark, data_lake)

    mp.update_silver_nyfed(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "ON-SOFR", "2024-01-02") == pytest.approx(5.31)


def test_sonia_reaches_the_table_as_on_sona(
        monkeypatch, spark, data_lake):
    """SONIA is collected from the Bank of England, which publishes it. The
    identifier names the rate and not the publisher, so it does not depend on
    the source: a change of identifier would leave the rows already collected
    behind in the table under the other one, which the upsert never removes."""
    answer_boe_with(
        monkeypatch, FakeResponse(boe_csv("IUDSOIA", ("02 Jan 2024", "5.19"))))
    mp.update_bronze_boe_sonia(spark, data_lake)

    mp.update_silver_boe(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "ON-SONA", "2024-01-02") == pytest.approx(5.19)


def test_the_lbma_price_is_taken_in_dollars(monkeypatch, spark, data_lake):
    """The combined table has no column saying what currency a value is in, so
    one of the three the association publishes has to be chosen. It is the
    dollar, which is what these benchmarks are quoted and settled in."""
    answer_lbma_with(monkeypatch, FakeResponse(lbma_json(
        ("2024-01-02", (2062.4, 1620.2, 1869.7)),
    )))

    mp.update_bronze_lbma_gold(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_lbma_gold(data_lake))
    assert bronze.count() == 1
    assert bronze.first()["gold"] == pytest.approx(2062.4)
    assert str(bronze.first()["date"]) == "2024-01-02"


def test_a_price_the_euro_did_not_exist_for_is_still_read(
        monkeypatch, spark, data_lake):
    """The association publishes the benchmark back to 1968 and the euro entry
    is null until 1999. That is the currency not existing yet rather than a gap
    in the data, and taking the dollar price means those thirty years are read
    like any other."""
    answer_lbma_with(monkeypatch, FakeResponse(lbma_json(
        ("1968-04-01", (37.7, 15.68, None)),
        ("2024-01-02", (2062.4, 1620.2, 1869.7)),
    )))

    mp.update_bronze_lbma_gold(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_lbma_gold(data_lake)).orderBy("date")
    assert bronze.count() == 2
    assert bronze.first()["gold"] == pytest.approx(37.7)


def test_the_lbma_download_sends_a_user_agent(monkeypatch, spark, data_lake):
    """As at the Bank of England, a request without one is refused."""
    call = answer_lbma_with(monkeypatch, FakeResponse(lbma_json(
        ("2024-01-02", (2062.4, 1620.2, 1869.7)),
    )))

    mp.update_bronze_lbma_gold(spark, data_lake)

    assert "User-Agent" in call["headers"]


def test_the_metals_reach_the_table_under_their_own_names(
        monkeypatch, spark, data_lake):
    answer_lbma_with(monkeypatch, FakeResponse(lbma_json(
        ("2024-01-02", (2062.4, 1620.2, 1869.7)),
    )))
    mp.update_bronze_lbma_gold(spark, data_lake)
    answer_lbma_with(monkeypatch, FakeResponse(lbma_json(
        ("2024-01-02", (23.5, 18.5, 21.3)),
    )))
    mp.update_bronze_lbma_silver(spark, data_lake)

    mp.update_silver_lbma(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "CO-GOLD", "2024-01-02") == pytest.approx(2062.4)
    assert value_of(frame, "CO-SILVER", "2024-01-02") == pytest.approx(23.5)


def test_the_commodity_mapping_table_is_shipped_with_the_package():
    """A commodity future has no ISIN, so this table names the identifier
    directly. Both columns have to be unique for the same reasons the instrument
    table's are, and no identifier may collide with one already in use."""
    commodities = mp.load_list_of_yahoo_commodities()

    assert list(commodities.columns) == ["INSTRID", "yahoo"]
    assert len(commodities) > 0
    assert commodities["INSTRID"].is_unique
    assert commodities["yahoo"].is_unique
    assert commodities["INSTRID"].str.startswith("CO-").all()

    instruments = mp.load_list_of_yahoo_tickers()
    assert not set(commodities["yahoo"]) & set(instruments["yahoo"])


def test_a_commodity_keeps_the_identifier_it_is_named_by(spark, data_lake):
    """The instruments arrive under an ISIN that becomes the identifier; a
    commodity arrives under the identifier itself, so nothing is renamed."""
    folder = mp.path_bronze_yahoo_commodities(data_lake)
    write_yahoo_commodity_bronze(folder, "2024-02-01",
                                 [("2024-01-31", "CO-BRENT", 81.7)])

    mp.update_silver_yahoo_commodities(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "CO-BRENT", "2024-01-31") == pytest.approx(81.7)


def test_the_most_recent_delivery_of_a_commodity_day_is_kept(spark, data_lake):
    """The commodity Bronze layer is appended to one download at a time, as the
    instrument one is, so it needs the same reduction before the merge."""
    folder = mp.path_bronze_yahoo_commodities(data_lake)
    write_yahoo_commodity_bronze(folder, "2024-02-01",
                                 [("2024-01-31", "CO-BRENT", 81.7)])
    write_yahoo_commodity_bronze(folder, "2024-02-08",
                                 [("2024-01-31", "CO-BRENT", 82.4)])

    mp.update_silver_yahoo_commodities(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "CO-BRENT", "2024-01-31") == pytest.approx(82.4)


def test_an_exchange_rate_that_leaves_for_a_day_and_comes_back_is_left_out(
        spark, data_lake):
    """Yahoo's dong of 8 November 2019, a hundredth of the days around it, and
    a taka that is let go and stays where it fell: the first is a misprint,
    the second a price."""
    folder = mp.path_bronze_yahoo_fx(data_lake)
    write_yahoo_commodity_bronze(folder, "2019-11-20", [
        ("2019-11-07", "EUR/VND", 25310.0), ("2019-11-08", "EUR/VND", 261.04),
        ("2019-11-12", "EUR/VND", 25183.0), ("2019-11-13", "EUR/VND", 25161.0),
        ("2019-11-07", "EUR/BDT", 94.0), ("2019-11-08", "EUR/BDT", 150.0),
        ("2019-11-12", "EUR/BDT", 151.0),
    ])

    mp.update_silver_yahoo_fx(spark, data_lake)

    frame = silver(spark, data_lake)
    vnd = frame[frame.INSTRID == "EUR/VND"]
    assert sorted(vnd.REFDT) == ["2019-11-07", "2019-11-12", "2019-11-13"]
    assert value_of(frame, "EUR/BDT", "2019-11-08") == pytest.approx(150.0)


def test_a_misprint_merged_on_the_last_day_is_removed_when_its_next_day_comes(
        spark, data_lake):
    """On the last day of a download there is no day after it to judge by, so
    the print is merged; the next download asks for that day again, with the
    day after it, and the print is taken back out of the table."""
    folder = mp.path_bronze_yahoo_fx(data_lake)
    write_yahoo_commodity_bronze(folder, "2019-11-08", [
        ("2019-11-06", "EUR/VND", 25390.0), ("2019-11-07", "EUR/VND", 25310.0),
        ("2019-11-08", "EUR/VND", 261.04),
    ])
    mp.update_silver_yahoo_fx(spark, data_lake)
    assert value_of(silver(spark, data_lake), "EUR/VND", "2019-11-08") == \
        pytest.approx(261.04)

    write_yahoo_commodity_bronze(folder, "2019-11-13", [
        ("2019-11-07", "EUR/VND", 25310.0), ("2019-11-08", "EUR/VND", 261.04),
        ("2019-11-12", "EUR/VND", 25183.0),
    ])
    mp.update_silver_yahoo_fx(spark, data_lake)

    frame = silver(spark, data_lake)
    assert sorted(frame.REFDT) == ["2019-11-06", "2019-11-07", "2019-11-12"]


def test_the_commodities_carry_on_from_their_own_partitions(data_lake):
    """The commodities are downloaded into a Bronze layer of their own, so where
    their next run starts must not be read off the instruments' partitions."""
    os.makedirs(os.path.join(mp.path_bronze_yahoo_marketpricedata(data_lake),
                             "LASTDT=2026-01-01"), exist_ok=True)
    os.makedirs(os.path.join(mp.path_bronze_yahoo_commodities(data_lake),
                             "LASTDT=2024-02-01"), exist_ok=True)

    assert mp.start_date_from_partitions(
        mp.path_bronze_yahoo_commodities(data_lake), "2020-01-01") == "2024-02-01"
    assert mp.get_start_date(data_lake, "2020-01-01") == "2026-01-01"


# The Silver layer
# --------------------------------------------------------------------------


def test_the_estr_rate_is_picked_out_of_the_flow(monkeypatch, spark, data_lake):
    answer_with(monkeypatch, "bronze_ecb",
                FakeResponse(estr_csv(estr_row("2024-01-02", 3.9))))
    mp.update_bronze_ecb_estr(spark, data_lake)

    mp.update_silver_ecb(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "ON-ESTR", "2024-01-02") == pytest.approx(3.9)


def test_the_other_series_of_the_est_flow_are_left_out(monkeypatch, spark, data_lake):
    """The flow carries fifteen series, nine of them on the same benchmark item:
    the rate, but also the number of reporting banks, the traded volume and the
    percentiles. All three parts of the filter are needed to reach the rate, and
    a test that pins only one of them would not notice the other two going."""
    answer_with(
        monkeypatch, "bronze_ecb",
        FakeResponse(estr_csv(
            estr_row("2024-01-02", 3.9),
            estr_row("2024-01-02", 47, data_type="NB"),     # reporting banks
            estr_row("2024-01-02", 2.41, data_type="R25"),  # 25th percentile
            estr_row("2024-01-02", 1.1, item="EU000A2QQF08"),
            estr_row("2024-01-02", 2.2, freq="A"),
        )),
    )
    mp.update_bronze_ecb_estr(spark, data_lake)

    mp.update_silver_ecb(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "ON-ESTR", "2024-01-02") == pytest.approx(3.9)


def test_the_key_ecb_rates_are_picked_out_of_the_fm_flow(monkeypatch, spark, data_lake):
    """The three rates are the corridor the euro money market is steered within,
    and each reaches the table under a name of its own."""
    answer_with(monkeypatch, "bronze_ecb",
                FakeResponse(fm_csv(
                    fm_row("2024-01-02", 4.00, rate="DFR"),
                    fm_row("2024-01-02", 4.50, rate="MRR_RT"),
                    fm_row("2024-01-02", 4.75, rate="MLFR"),
                )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 3
    assert value_of(frame, "KR-DFR", "2024-01-02") == pytest.approx(4.00)
    assert value_of(frame, "KR-MRO", "2024-01-02") == pytest.approx(4.50)
    assert value_of(frame, "KR-MLF", "2024-01-02") == pytest.approx(4.75)


def test_the_other_series_of_the_fm_flow_are_left_out(monkeypatch, spark, data_lake):
    """The flow carries a hundred and more series, and the three filters each
    keep out something that would otherwise collide with the rate itself.

    The same rate is published at five frequencies at once, so dropping the
    frequency filter delivers one identifier several times for one reference
    date -- and a merge whose source holds a key twice fails the run. The change
    in percentage points is published beside the level under the same name and
    the same date, so dropping that filter does the same. The main refinancing
    rate has two further variants, each covering only the years its tender
    regime ran in, so dropping the rate filter puts three different series into
    the table under one name."""
    answer_with(
        monkeypatch, "bronze_ecb",
        FakeResponse(fm_csv(
            fm_row("2024-01-02", 4.00),
            fm_row("2024-01-02", 4.00, freq="B"),            # same rate, daily
            fm_row("2024-01", 3.90, freq="M"),               # a month, not a date
            fm_row("2024-01-02", 0.25, data_type="CHG"),     # the change, not the level
            fm_row("2024-01-02", 4.50, rate="MRR_FR"),       # one tender regime
            fm_row("2024-01-02", 4.50, rate="MRR_MBR"),      # the other
        )),
    )
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "KR-DFR", "2024-01-02") == pytest.approx(4.00)


def test_the_monthly_us_series_are_dated_to_the_end_of_their_month(
        monkeypatch, spark, data_lake):
    """A month is not a date, so one has to be chosen for it, and the end of the
    period is the choice: it is the date by which the observation is complete.
    Dating it to the first of the month would put a figure on days it draws on
    but does not yet cover, and a reader carrying the last quote forward would
    carry it across its own reference period."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-02", 4.29, freq="M", rate="US10YT_RR",
               data_type="YLDA", area="US", instrument="BB"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm_monthly_us(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    # 2026 is not a leap year, so the end of February is the 28th: the date is
    # computed from the month rather than assumed to be the 30th or the 31st.
    assert value_of(frame, "YLD-US10Y-M", "2026-02-28") == pytest.approx(4.29)


def test_a_monthly_series_says_so_in_its_identifier(monkeypatch, spark, data_lake):
    """These are the only series in the module that are not daily. The table has
    no column for the frequency, so a reader joining them to the daily sources
    on the reference date would find a value on one day a month and nothing on
    the others, with nothing in the row to explain it. The suffix is the only
    place that warning can live."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-08", 4.74, freq="M", rate="US10YT_RR",
               data_type="YLDA", area="US", instrument="BB"),
        fm_row("2026-08", 4.73, freq="M", rate="USD10YZ_R",
               data_type="YLDE", area="US", instrument="BZ"),
        fm_row("2026-08", 0.68, freq="M", rate="US10YT_US1YT",
               data_type="SPRE", area="US", instrument="SP"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm_monthly_us(spark, data_lake)

    frame = silver(spark, data_lake)
    assert sorted(frame.INSTRID) == ["SPR-US10Y1Y-M", "YLD-US10Y-M", "YLD-US10YZ-M"]
    assert all(i.endswith("-M") for i in frame.INSTRID)
    assert value_of(frame, "SPR-US10Y1Y-M", "2026-08-31") == pytest.approx(0.68)


def test_the_other_frequencies_of_the_us_series_are_left_out(
        monkeypatch, spark, data_lake):
    """The same series is published yearly and quarterly as well. Without the
    frequency filter one identifier arrives three times for one reference date,
    and a merge whose source holds a key twice fails the run."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-08", 4.74, freq="M", rate="US10YT_RR",
               data_type="YLDA", area="US", instrument="BB"),
        fm_row("2026", 4.50, freq="A", rate="US10YT_RR",
               data_type="YLDA", area="US", instrument="BB"),
        fm_row("2026-Q3", 4.60, freq="Q", rate="US10YT_RR",
               data_type="YLDA", area="US", instrument="BB"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm_monthly_us(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "YLD-US10Y-M", "2026-08-31") == pytest.approx(4.74)


def test_the_daily_rates_and_the_monthly_series_do_not_take_each_others_rows(
        monkeypatch, spark, data_lake):
    """Both stages read the same Bronze layer and each has to leave the other's
    series alone, or a rate would arrive as a yield or a yield as a rate."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-08-31", 2.50),
        fm_row("2026-08", 4.74, freq="M", rate="US10YT_RR",
               data_type="YLDA", area="US", instrument="BB"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm(spark, data_lake)
    mp.update_silver_ecb_fm_monthly_us(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 2
    assert value_of(frame, "KR-DFR", "2026-08-31") == pytest.approx(2.50)
    assert value_of(frame, "YLD-US10Y-M", "2026-08-31") == pytest.approx(4.74)


def test_the_euro_area_curve_and_money_market_reach_the_table(
        monkeypatch, spark, data_lake):
    """The euro area block is a whole curve and the money market beside it, and
    the prefix of the identifier is the only thing saying which is which: a bond
    yield, a term interbank rate, an overnight rate, an equity index level."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-08", 3.665, freq="M", rate="U2_10Y",
               data_type="YLD", instrument="BB"),
        fm_row("2026-08", 2.924, freq="M", rate="U2_2Y",
               data_type="YLD", instrument="BB"),
        fm_row("2026-08", 2.513, freq="M", rate="EURIBOR3MD_",
               data_type="HSTA", instrument="MM"),
        fm_row("2026-08", 682.132, freq="M", rate="DJEURST",
               data_type="HSTA", instrument="EI"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm_monthly_ea(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 4
    assert value_of(frame, "YLD-EA10Y-M", "2026-08-31") == pytest.approx(3.665)
    assert value_of(frame, "YLD-EA2Y-M", "2026-08-31") == pytest.approx(2.924)
    assert value_of(frame, "IBOR-EUR3M-M", "2026-08-31") == pytest.approx(2.513)
    assert value_of(frame, "EQ-ESTOXX-M", "2026-08-31") == pytest.approx(682.132)
    assert all(i.endswith("-M") for i in frame.INSTRID)


def test_a_monthly_copy_of_a_daily_series_is_left_out(
        monkeypatch, spark, data_lake):
    """The flow publishes €STR and the EURO STOXX 50 monthly, and the table
    already carries both daily -- €STR as ON-ESTR and the index from Yahoo under
    its ISIN. A monthly copy of a series the table holds daily is not additional
    data, it is the same data at a coarser grain under a second name, and a
    reader comparing the two would be comparing a series with itself.

    This is a rule about what the block is for rather than about what it can
    parse, so nothing but a test records it."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-08", 3.665, freq="M", rate="U2_10Y",
               data_type="YLD", instrument="BB"),
        fm_row("2026-08", 2.187, freq="M", rate="UONSTR",
               data_type="HSTA", instrument="MM"),
        fm_row("2026-08", 6483.56, freq="M", rate="DJES50I",
               data_type="HSTA", instrument="EI"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm_monthly_ea(spark, data_lake)

    frame = silver(spark, data_lake)
    assert list(frame.INSTRID) == ["YLD-EA10Y-M"]


def test_the_euro_area_block_is_filtered_by_area_as_well(
        monkeypatch, spark, data_lake):
    """The flow publishes other areas under codes of the same shape, so the area
    is part of the filter: it says the stage is about the euro area rather than
    about whichever codes happen to be listed in it."""
    answer_with(monkeypatch, "bronze_ecb", FakeResponse(fm_csv(
        fm_row("2026-08", 3.665, freq="M", rate="U2_10Y",
               data_type="YLD", instrument="BB"),
        fm_row("2026-08", 9.99, freq="M", rate="U2_10Y",
               data_type="YLD", instrument="BB", area="JP"),
    )))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm_monthly_ea(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "YLD-EA10Y-M", "2026-08-31") == pytest.approx(3.665)


def test_no_identifier_is_claimed_by_two_of_the_fm_blocks():
    """The three mappings are read by three stages against one Bronze layer. An
    identifier in two of them would be written twice with different numbers, and
    a source code in two of them would be read twice."""
    from fincommons.marketpricedata import silver_ecb

    blocks = [silver_ecb.ECB_KEY_RATES,
              silver_ecb.ECB_MONTHLY_US,
              silver_ecb.ECB_MONTHLY_EA]

    identifiers = [i for block in blocks for i in block.values()]
    assert len(identifiers) == len(set(identifiers))

    codes = [c for block in blocks for c in block]
    assert len(codes) == len(set(codes))

    # Every monthly identifier says so, and no daily one pretends to.
    monthly = list(silver_ecb.ECB_MONTHLY_US.values()) + \
        list(silver_ecb.ECB_MONTHLY_EA.values())
    assert all(i.endswith("-M") for i in monthly)
    assert not any(i.endswith("-M") for i in silver_ecb.ECB_KEY_RATES.values())


def test_a_reference_date_of_the_fm_flow_arrives_as_a_date(
        monkeypatch, spark, data_lake):
    """The FM schema types TIME_PERIOD as a string where the EST schema types it
    as a date, so this stage is the one that has to cast it. Left as text it
    would reach the merge as a column the Silver table does not hold, and the
    first source to write the table would settle its type as a string."""
    answer_with(monkeypatch, "bronze_ecb",
                FakeResponse(fm_csv(fm_row("2024-01-02", 4.00))))
    mp.update_bronze_ecb_fm(spark, data_lake)

    mp.update_silver_ecb_fm(spark, data_lake)

    table = mp.connect_to_marketpricedata(spark, data_lake)
    assert isinstance(table.schema["REFDT"].dataType, DateType)
    assert str(table.first()["REFDT"]) == "2024-01-02"


def test_an_exchange_rate_becomes_a_currency_pair(monkeypatch, spark, data_lake):
    """A rate is not an instrument and has no ISIN, so the pair it converts
    between is what identifies it in the table."""
    rates = spark.createDataFrame(
        [(date(2024, 1, 2), "EUR", "USD", 1.10, "ref", "ECB")],
        "REFDATE date, BASE string, QUOTE string, VALUE double, TYPE string, SOURCE string",
    )
    monkeypatch.setattr(
        "fincommons.marketpricedata.silver_exchangerate.ex.connect_to_exchangerate",
        lambda spark, config_path: rates,
    )

    mp.update_silver_exchangerate(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "EUR/USD", "2024-01-02") == pytest.approx(1.10)


def test_the_internal_source_is_picked_up(spark, data_lake):
    write_internal(mp.path_bronze_internal_marketpricedata(data_lake),
                 [("2024-01-02", "OWN-1", 12.5)])

    mp.update_silver_internal(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "OWN-1", "2024-01-02") == pytest.approx(12.5)


def test_the_proprietary_quotes_keep_the_types_of_the_table(spark, data_lake):
    """The delivery is written by whoever maintains the quotes and not by a
    publisher, so the module declares its shape instead of learning it from the
    file. Read without a schema, every column of a CSV file is text, and the
    reference date and the price would reach the merge as strings."""
    write_internal(mp.path_bronze_internal_marketpricedata(data_lake),
                   [("2024-01-02", "OWN-1", 12.5)])

    mp.update_silver_internal(spark, data_lake)

    table = mp.connect_to_marketpricedata(spark, data_lake)
    assert dict(table.dtypes) == {
        "REFDT": "date", "INSTRID": "string", "VALUE": "double",
    }


def test_the_header_row_of_a_proprietary_file_is_not_read_as_a_quote(spark, data_lake):
    """A reader that is given a schema does not skip the header by itself. Where
    it is parsed it arrives as a row whose reference date and price are null,
    because neither reads as a date or a number from its own column name, and
    that row then sits in the table as a quote nobody delivered."""
    write_internal(mp.path_bronze_internal_marketpricedata(data_lake),
                   [("2024-01-02", "OWN-1", 12.5)])

    mp.update_silver_internal(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "OWN-1", "2024-01-02") == pytest.approx(12.5)


def test_a_bronze_layer_without_a_proprietary_file_is_a_quiet_no_op(spark, data_lake):
    """The directory is created with the rest of the data lake and stays empty
    in an organisation that keeps no proprietary quotes. The stage is called on
    every run of the Silver layer all the same, so finding nothing there has to
    leave the table alone rather than fail the run."""
    mp.update_silver_internal(spark, data_lake)

    assert not os.path.isdir(mp.path_silver_marketpricedata(data_lake))


def test_a_source_that_has_delivered_nothing_leaves_the_table_alone(spark, data_lake):
    """Every Silver stage is called whether or not its source has ever been
    downloaded, so an absent Bronze layer has to be a quiet no-op rather than a
    failure that takes the run with it."""
    write_internal(mp.path_bronze_internal_marketpricedata(data_lake),
                 [("2024-01-02", "OWN-1", 12.5)])
    mp.update_silver_internal(spark, data_lake)

    mp.update_silver_ecb(spark, data_lake)
    mp.update_silver_nyfed(spark, data_lake)
    mp.update_silver_boe(spark, data_lake)
    mp.update_silver_lbma(spark, data_lake)

    frame = silver(spark, data_lake)
    assert list(frame.INSTRID) == ["OWN-1"]


def test_a_source_that_has_delivered_nothing_says_so_in_the_log(
        spark, data_lake, caplog):
    """The no-op is quiet in the sense that the run carries on, not in the sense
    that nobody hears of it: a source without a Bronze layer did less than a
    reader of the run would assume, which is a warning naming the source."""
    with caplog.at_level(logging.INFO, logger="fincommons"):
        mp.update_silver_boe(spark, data_lake)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING
                and r.name.startswith("fincommons")]
    assert [r.ctx["source"] for r in warnings] == ["BOE"]


# Electricity and the further Yahoo families
# --------------------------------------------------------------------------


def test_a_day_of_electricity_becomes_one_baseload_price(
        monkeypatch, spark, data_lake):
    """The service publishes a price per delivery period and the table holds one
    value per identifier and day, so a day has to become one number. It becomes
    the average -- the baseload price -- and the lowest and highest of the day
    are kept beside it, because electricity moves within a day in a way nothing
    else in this table does."""
    # Four quarter-hours of one day, one of them negative.
    midnight = 1789423200
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"AT": "EL-AT"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (midnight, 100.0), (midnight + 900, 200.0),
        (midnight + 1800, -20.0), (midnight + 2700, 40.0),
    )))

    mp.update_bronze_electricity(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    assert bronze.count() == 1
    row = bronze.first()
    assert row["zone"] == "EL-AT"
    assert row["price"] == pytest.approx(80.0)       # the average of the four
    assert row["price_min"] == pytest.approx(-20.0)
    assert row["price_max"] == pytest.approx(200.0)
    assert row["periods"] == 4


def test_a_refused_electricity_request_is_tried_again(
        monkeypatch, spark, data_lake):
    """The service rate limits, and the refusals fall wherever the limiter
    happens to be rather than on any particular zone: the same zone answers on
    one run and is refused on the next. A download that gave up at the first
    refusal would collect a different, arbitrary subset of Europe every time."""
    midnight = 1789423200
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"AT": "EL-AT"})
    service = answer_electricity_with(
        monkeypatch,
        FakeElectricity(electricity_json((midnight, 100.0)), refusals=3))

    mp.update_bronze_electricity(spark, data_lake)

    assert len(service.calls) == 4
    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    assert bronze.count() == 1


def test_an_electricity_refusal_that_never_relents_is_raised(
        monkeypatch, spark, data_lake):
    """Retrying for ever would hang the pipeline. After the attempts are used up
    the source fails, which the layer sets aside like any other failing source."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"AT": "EL-AT"})
    answer_electricity_with(
        monkeypatch, FakeElectricity(b"", refusals=99))

    with pytest.raises(RuntimeError) as failure:
        mp.update_bronze_electricity(spark, data_lake)

    assert "rate limits" in str(failure.value)


def test_a_delivery_day_is_a_market_day_and_not_a_utc_day(
        monkeypatch, spark, data_lake):
    """A delivery day runs from midnight to midnight in the market's own time,
    so in summer its first period is at 22:00 UTC on the day before.

    Grouped by UTC date instead, every delivery day is split in two and the
    baseload average mixes the last two hours of one day with the first
    twenty-two of the next -- a price for a day that never traded. Nothing about
    the result looks wrong: there is a number for every date, and it is merely
    the wrong number.

    The two periods here are the first of the 15th of September and the last of
    the 14th, an hour apart in UTC and on either side of the market midnight."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1789419600, 40.0),   # 2026-09-14 21:00 UTC = 23:00 CEST, still the 14th
        (1789423200, 90.0),   # 2026-09-14 22:00 UTC = 00:00 CEST, now the 15th
        (1789426800, 110.0),  # 2026-09-14 23:00 UTC = 01:00 CEST
    )))

    mp.update_bronze_electricity(spark, data_lake)

    bronze = spark.read.parquet(
        mp.path_bronze_electricity_dayahead(data_lake)).orderBy("date")
    days = {str(row["date"]): row for row in bronze.collect()}

    assert sorted(days) == ["2026-09-14", "2026-09-15"]
    assert days["2026-09-14"]["price"] == pytest.approx(40.0)
    assert days["2026-09-14"]["periods"] == 1
    # The two periods after market midnight belong to the 15th together.
    assert days["2026-09-15"]["price"] == pytest.approx(100.0)
    assert days["2026-09-15"]["periods"] == 2


def test_the_market_day_follows_summer_time(monkeypatch, spark, data_lake):
    """The offset is not fixed: it is two hours in summer and one in winter, so
    a constant shift would be right for half the year. These two periods are the
    first of a January day and the first of a July day, and each is midnight in
    the market at a different UTC hour."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1767222000, 50.0),   # 2025-12-31 23:00 UTC = 2026-01-01 00:00 CET
        (1782856800, 70.0),   # 2026-06-30 22:00 UTC = 2026-07-01 00:00 CEST
    )))

    mp.update_bronze_electricity(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    assert sorted(str(row["date"]) for row in bronze.collect()) == \
        ["2026-01-01", "2026-07-01"]


def test_a_zone_east_of_the_others_keeps_its_own_day(
        monkeypatch, spark, data_lake):
    """Most of these zones trade in the coupled market, whose day is CET. Greece
    and Romania keep their own, an hour further east, so the same instant falls
    on a different day for them than for Germany."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU", "GR": "EL-GR"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1789419600, 40.0),   # 21:00 UTC: still the 14th in CEST, the 15th in EEST
    )))

    mp.update_bronze_electricity(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    by_zone = {row["zone"]: str(row["date"]) for row in bronze.collect()}
    assert by_zone == {"EL-DELU": "2026-09-14", "EL-GR": "2026-09-15"}


def test_an_empty_lake_is_collected_from_the_first_date(spark, data_lake):
    """There is nothing to carry on from, so the whole history is asked for."""
    path = mp.path_bronze_electricity_dayahead(data_lake)

    assert mp.electricity_start_date(spark, path) == mp.ELECTRICITY_FIRST_DATE


def test_a_second_run_asks_only_for_what_is_new(monkeypatch, spark, data_lake):
    """A scheduled run collects the days since the last one it holds.

    This is what makes the source cheap enough to run every morning: a decade
    of quarter-hours for twenty zones is several million periods, and a day of
    them is a few thousand. The test reads the start date out of the request
    the download actually made, because asking for less is the whole point --
    a run that quietly asked for everything again would write exactly the same
    table and pass any test that only looked at the result.
    """
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1788256800, 30.0),   # 2026-09-01, midday in Berlin
        (1789034400, 40.0),   # 2026-09-10
    )))

    mp.update_bronze_electricity(spark, data_lake, min_date = "2026-01-01")

    # The second run is given no date at all, which is how the scheduler calls
    # it, and answers with a day the first run did not have.
    service = answer_electricity_with(monkeypatch, FakeElectricity(
        electricity_json((1789120800, 50.0))))   # 2026-09-11

    mp.update_bronze_electricity(spark, data_lake)

    # A week before the last day held, so a republished day is seen again.
    assert service.calls[0]["params"]["start"] == "2026-09-03"

    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    days = {str(row["date"]): row["price"] for row in bronze.collect()}

    # The day before the window is kept, untouched by a download that never
    # asked for it, and the new day is there beside it.
    assert days["2026-09-01"] == pytest.approx(30.0)
    assert days["2026-09-11"] == pytest.approx(50.0)
    # Two rows for two days. The runs overlap by a week by design, and a day
    # delivered twice would break the merge into the Silver table, which cannot
    # be given the same key twice in one source. The 10th is gone because it
    # fell inside the window and the second answer did not carry it: within the
    # window the service is what the table says, which is what lets a day be
    # corrected and, in the same breath, withdrawn.
    assert sorted(days) == ["2026-09-01", "2026-09-11"]
    assert bronze.count() == 2


def test_a_day_that_is_republished_is_collected_again(
        monkeypatch, spark, data_lake):
    """The window exists for this case. A day already in the table is asked for
    again for a week, so a price the service corrects after the fact reaches the
    table rather than being fixed for good at whatever it first said."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1789034400, 40.0),   # 2026-09-10
    )))
    mp.update_bronze_electricity(spark, data_lake, min_date = "2026-01-01")

    # The same day comes back at a different price, within the revision window.
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1789034400, 44.0),
        (1789120800, 50.0),   # 2026-09-11
    )))
    mp.update_bronze_electricity(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    days = {str(row["date"]): row["price"] for row in bronze.collect()}

    assert days["2026-09-10"] == pytest.approx(44.0)
    assert len(days) == 2


def test_a_zone_west_of_the_others_keeps_its_own_day(
        monkeypatch, spark, data_lake):
    """Portugal is the mirror image of Greece: it shares a market with Spain but
    not a clock, and its day runs an hour behind the coupled market's.

    This is the case a map does not settle, because Portugal is in the Iberian
    market and the temptation is to give it the Spanish day. The data settles
    it. Asked for a history from the 1st of January, the first Portuguese day
    comes back one period short under a Berlin day and the last comes back as a
    four-period fragment dated to the day after -- both of them exactly the hour
    between Lisbon and Berlin, and neither of them present for any zone that
    really is on CET.

    The instant here is half past eleven at night in Lisbon, which is already
    the next day in Berlin."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU", "PT": "EL-PT"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1789425000, 60.0),   # 22:30 UTC: the 15th in CEST, still the 14th in WEST
    )))

    mp.update_bronze_electricity(spark, data_lake)

    bronze = spark.read.parquet(mp.path_bronze_electricity_dayahead(data_lake))
    by_zone = {row["zone"]: str(row["date"]) for row in bronze.collect()}
    assert by_zone == {"EL-DELU": "2026-09-15", "EL-PT": "2026-09-14"}


def test_the_baseload_price_reaches_the_table_under_its_zone(
        monkeypatch, spark, data_lake):
    """A price exists for a bidding zone and not for a country, so the zone is
    the identifier."""
    midnight = 1789423200
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (midnight, 90.0), (midnight + 900, 110.0),
    )))
    mp.update_bronze_electricity(spark, data_lake)

    mp.update_silver_electricity(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "EL-DELU", "2026-09-15") == pytest.approx(100.0)


def test_the_electricity_licence_travels_with_the_code():
    """The prices are CC BY 4.0 and the licence obliges attribution, which no
    other source in this module does. Keeping the wording beside the download
    means the obligation cannot be lost by rewriting a README."""
    from fincommons.marketpricedata import bronze_electricity

    licence = bronze_electricity.ELECTRICITY_LICENCE
    assert "CC BY 4.0" in licence
    assert "Bundesnetzagentur" in licence


def test_the_further_yahoo_families_are_named_and_kept_apart():
    """Three tables name a Yahoo ticker directly rather than by ISIN. No ticker
    and no identifier may appear in more than one of them, or a price would be
    collected twice under two names or once under two.

    Each family is also recognisable from its identifier alone, which is what
    the report tells them apart by."""
    commodities = mp.load_list_of_yahoo_commodities()
    volatility = mp.load_list_of_yahoo_volatility()
    indices = mp.load_list_of_yahoo_indices()

    for table, prefix in ((commodities, "CO-"), (volatility, "VOL-"),
                          (indices, "IX-")):
        assert list(table.columns) == ["INSTRID", "yahoo"]
        assert len(table) > 0
        assert table["INSTRID"].is_unique
        assert table["yahoo"].is_unique
        assert table["INSTRID"].str.startswith(prefix).all()

    tickers = list(commodities["yahoo"]) + list(volatility["yahoo"]) + \
        list(indices["yahoo"]) + list(mp.load_list_of_yahoo_tickers()["yahoo"])
    assert len(tickers) == len(set(tickers))

    identifiers = list(commodities["INSTRID"]) + list(volatility["INSTRID"]) + \
        list(indices["INSTRID"])
    assert len(identifiers) == len(set(identifiers))


def test_each_yahoo_family_carries_on_from_its_own_partitions(data_lake):
    """Four Bronze layers are downloaded the same way and each has its own
    history, so none of them may read where another left off."""
    folders = {
        "instruments": mp.path_bronze_yahoo_marketpricedata(data_lake),
        "commodities": mp.path_bronze_yahoo_commodities(data_lake),
        "volatility": mp.path_bronze_yahoo_volatility(data_lake),
        "indices": mp.path_bronze_yahoo_indices(data_lake),
    }
    assert len(set(folders.values())) == 4

    for i, folder in enumerate(folders.values()):
        os.makedirs(os.path.join(folder, f"LASTDT=2024-0{i + 1}-01"), exist_ok=True)

    for i, folder in enumerate(folders.values()):
        assert mp.start_date_from_partitions(folder, "2020-01-01") == \
            f"2024-0{i + 1}-01"


def test_a_volatility_index_reaches_the_table_under_its_name(spark, data_lake):
    write_yahoo_commodity_bronze(mp.path_bronze_yahoo_volatility(data_lake),
                                 "2024-02-01",
                                 [("2024-01-31", "VOL-VIX", 13.8)])

    mp.update_silver_yahoo_volatility(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "VOL-VIX", "2024-01-31") == pytest.approx(13.8)


def test_a_stock_index_reaches_the_table_under_its_name(spark, data_lake):
    write_yahoo_commodity_bronze(mp.path_bronze_yahoo_indices(data_lake),
                                 "2024-02-01",
                                 [("2024-01-31", "IX-N225", 36286.71)])

    mp.update_silver_yahoo_indices(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "IX-N225", "2024-01-31") == pytest.approx(36286.71)


# The upsert
# --------------------------------------------------------------------------


def test_a_day_that_is_delivered_again_replaces_the_row(spark, data_lake):
    """The pipeline is meant to be re-runnable, and the sources are downloaded
    whole rather than incrementally, so a day arrives again on every run. It has
    to replace what it delivered before instead of accumulating a second row,
    which is the difference between an upsert and an append."""
    path = mp.path_bronze_internal_marketpricedata(data_lake)
    write_internal(path, [("2024-01-02", "OWN-1", 12.5)])
    mp.update_silver_internal(spark, data_lake)

    write_internal(path, [("2024-01-02", "OWN-1", 99.0)])
    mp.update_silver_internal(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "OWN-1", "2024-01-02") == pytest.approx(99.0)


def test_a_day_that_is_new_is_added_beside_the_others(spark, data_lake):
    path = mp.path_bronze_internal_marketpricedata(data_lake)
    write_internal(path, [("2024-01-02", "OWN-1", 12.5)])
    mp.update_silver_internal(spark, data_lake)

    write_internal(path, [("2024-01-03", "OWN-1", 13.0)])
    mp.update_silver_internal(spark, data_lake)

    frame = silver(spark, data_lake)
    assert sorted(frame.REFDT) == ["2024-01-02", "2024-01-03"]


def test_the_same_day_of_two_instruments_is_two_rows(spark, data_lake):
    """A row is identified by the instrument and the date together. Keyed on the
    date alone, the second source of a day would overwrite the first."""
    write_internal(
        mp.path_bronze_internal_marketpricedata(data_lake),
        [("2024-01-02", "OWN-1", 12.5), ("2024-01-02", "OWN-2", 7.25)],
    )

    mp.update_silver_internal(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 2
    assert value_of(frame, "OWN-1", "2024-01-02") == pytest.approx(12.5)
    assert value_of(frame, "OWN-2", "2024-01-02") == pytest.approx(7.25)


def test_only_the_three_columns_reach_the_table(spark, data_lake):
    """A source may carry more than the table holds, and what it carries beyond
    the three columns must not reach the Silver layer: the sources do not agree
    on those extra columns, and a merge only works where the two sides match.

    The upsert is called directly here rather than through a source, because
    every source of the module now hands it the three columns and none of them
    would notice the projection going."""
    bronze = spark.createDataFrame(
        [(date(2024, 1, 2), "OWN-1", 12.5, "rates", "a note the module knows nothing about")],
        "REFDT date, INSTRID string, VALUE double, DESK string, COMMENT string",
    )

    mp.standard_upsert(spark, data_lake, bronze)

    frame = silver(spark, data_lake)
    assert list(frame.columns) == ["REFDT", "INSTRID", "VALUE"]


# The pipeline
# --------------------------------------------------------------------------


def test_a_source_that_fails_does_not_stop_the_others(
    monkeypatch, spark, data_lake, caplog
):
    """A publisher that is unreachable on a given morning should not keep the
    rest of the table from being brought up to date. The failure is recorded and
    the run carries on, which is the ERROR level of fincommons.log: a unit of
    work failed by design, not the run."""
    write_internal(mp.path_bronze_internal_marketpricedata(data_lake),
                 [("2024-01-02", "OWN-1", 12.5)])

    def unreachable(spark, config_path):
        raise RuntimeError("the ECB did not answer")

    monkeypatch.setattr(pipeline, "update_silver_ecb", unreachable)

    with caplog.at_level(logging.ERROR, logger="fincommons"):
        mp.update_silver_layer(spark, data_lake)

    failures = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("the ECB did not answer" in (r.exc_text or "") for r in failures)

    # The source that follows the one that failed still ran.
    frame = silver(spark, data_lake)
    assert value_of(frame, "OWN-1", "2024-01-02") == pytest.approx(12.5)


# Yahoo Finance
# --------------------------------------------------------------------------


def test_the_start_date_is_the_minimum_where_nothing_has_been_downloaded(data_lake):
    assert mp.get_start_date(data_lake, "2024-01-01") == "2024-01-01"


def test_the_start_date_is_the_minimum_where_the_bronze_layer_is_not_there(tmp_path):
    """The start date is read off the Bronze layer, which the first run of a
    data lake that has not been set up does not have."""
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(
        f"marketpricedata:\n  bronze: {tmp_path / 'bronze'}\n"
        f"  silver: {tmp_path / 'silver'}\n",
        encoding="utf-8",
    )

    assert mp.get_start_date(str(config_path), "2024-01-01") == "2024-01-01"


def test_the_start_date_is_the_day_the_last_download_stopped_at(data_lake):
    """The source is queried for a range, so the partitions already in the
    Bronze layer are what says where the next run carries on from -- and the
    newest of them is the one that says it, whatever order they are listed in."""
    folder = mp.path_bronze_yahoo_marketpricedata(data_lake)
    os.makedirs(os.path.join(folder, "LASTDT=2024-01-05"), exist_ok=True)
    os.makedirs(os.path.join(folder, "LASTDT=2024-01-31"), exist_ok=True)

    assert mp.get_start_date(data_lake, "2024-01-01") == "2024-01-31"


def test_no_reference_date_falls_between_two_downloads(data_lake):
    """The ranges of two consecutive runs have to meet exactly.

    A run queries ``[start, end)`` and names its partition after ``end``, so the
    partition is the first day that run did not collect. Starting the next run a
    day later leaves that day uncollected for good, because nothing goes back
    for it: the gap is one reference date per run and it is silent, since every
    run reports success and the missing day simply never appears in the table.

    The mistake is an easy one to make, because ``LASTDT`` reads like the last
    date collected rather than the day of the download."""
    folder = mp.path_bronze_yahoo_marketpricedata(data_lake)

    # A run made on the first of February, which collected up to the 31st of
    # January and no further.
    os.makedirs(os.path.join(folder, "LASTDT=2024-02-01"), exist_ok=True)

    assert mp.get_start_date(data_lake, "2024-01-01") == "2024-02-01"


def test_the_absence_of_yfinance_is_reported_in_terms_of_the_download(monkeypatch):
    """The import is deferred so that the rest of the module runs without the
    package. What the caller then sees has to say what they were doing and what
    to install, rather than naming an import line they never wrote."""
    monkeypatch.setitem(sys.modules, "yfinance", None)

    with pytest.raises(ImportError) as failure:
        bronze_yahoo._yfinance()

    assert "yfinance" in str(failure.value)
    assert "marketprice" in str(failure.value)


def test_the_mapping_table_is_shipped_with_the_package():
    """The tickers are package data, so they travel with the wheel and are not
    read from the data lake or from the working directory."""
    tickers = mp.load_list_of_yahoo_tickers()

    # first_date is optional per row: the history a new ticker is asked for
    # when it is not the default (see bronze_yahoo.new_ticker_starts).
    assert list(tickers.columns) == ["ISIN", "yahoo", "first_date", "name"]
    assert len(tickers) > 0
    assert tickers["ISIN"].is_unique
    given = tickers["first_date"].dropna()
    assert given.str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()


def test_the_mapping_table_pairs_each_instrument_exactly_once():
    """Neither column may repeat, and the repetitions fail differently.

    A ticker is joined onto the downloaded prices, so a ticker named twice turns
    every price of it into two rows under two identifiers. An ISIN named twice
    hands the merge the same key twice, which fails the Silver run outright. Both
    are the kind of fault a paste makes, and neither shows up as anything but a
    wrong number afterwards.

    The shape of the identifier is held here as well, because a reader tells one
    source from another by it: a malformed ISIN is not recognised as a listed
    instrument and is quietly taken for a delivery from an internal source."""
    tickers = mp.load_list_of_yahoo_tickers()

    assert tickers["ISIN"].is_unique
    assert tickers["yahoo"].is_unique
    assert tickers["ISIN"].str.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]").all()


def rows_of(spark, data_lake, refdt, instrid, value):
    """One row, in the three columns the Silver table holds."""
    return spark.createDataFrame(
        [(date.fromisoformat(refdt), instrid, float(value))],
        schema = mp.internal_schema)


def count_merges(monkeypatch):
    """How many times the run actually touched the Silver table.

    The number of merges is the thing this is about and the table cannot be
    asked: one merge of eleven sources and eleven merges of one source each
    leave exactly the same rows behind, and differ only in how many times the
    files of a million-row table were rewritten to get there.
    """
    merges = []
    original = mp.merge_into_silver

    def spy(spark, config_path, bronze):
        merges.append(bronze.count())
        return original(spark, config_path, bronze)

    monkeypatch.setattr(
        "fincommons.marketpricedata.common.merge_into_silver", spy)
    return merges


def test_the_sources_of_a_run_are_merged_once(monkeypatch, spark, data_lake):
    """A merge costs what it costs whatever it carries -- it rewrites the files
    it touches in the whole table -- so eleven sources merging one after another
    cost eleven times that however little each had to say."""
    merges = count_merges(monkeypatch)

    with mp.collected_upserts(spark, data_lake):
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "ON-ESTR", 3.9))
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "KR-DFR", 4.0))
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-30", "ON-ESTR", 3.8))

    assert merges == [3]

    frame = silver(spark, data_lake)
    assert len(frame) == 3
    assert value_of(frame, "ON-ESTR", "2024-01-31") == pytest.approx(3.9)
    assert value_of(frame, "KR-DFR", "2024-01-31") == pytest.approx(4.0)


def test_the_source_collected_last_is_the_one_that_is_kept(
        monkeypatch, spark, data_lake):
    """The proprietary source is meant to replace a public quote for the same
    instrument and day. Collected together the two would reach one merge as the
    same key twice, which fails rather than chooses, so the order has to decide
    it explicitly: the source collected last, which is the proprietary one,
    wins."""
    with mp.collected_upserts(spark, data_lake):
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "ON-ESTR", 3.9))
        # As update_silver_layer runs the proprietary source last.
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "ON-ESTR", 3.95))

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "ON-ESTR", "2024-01-31") == pytest.approx(3.95)


def test_a_batch_that_collected_nothing_does_not_merge(
        monkeypatch, spark, data_lake):
    """A morning on which every source is already up to date should cost
    nothing at all, which is the point of not merging them separately."""
    merges = count_merges(monkeypatch)

    with mp.collected_upserts(spark, data_lake):
        pass

    assert merges == []


def test_a_mark_waits_for_the_merge_of_the_whole_batch(spark, data_lake):
    """A mark must not run ahead of the merge it describes. Inside a batch the
    merge has not happened when the source finishes, so the mark is held until
    the batch closes."""
    with mp.collected_upserts(spark, data_lake):
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "ON-ESTR", 3.9))
        mp.write_watermark(data_lake, "TEST", "2024-02-01")

        # Still nothing on disk: the rows have not been merged yet.
        assert mp.read_watermark(data_lake, "TEST") is None

    assert mp.read_watermark(data_lake, "TEST") == "2024-02-01"


def test_a_source_that_fails_leaves_the_others_to_be_merged(
        monkeypatch, spark, data_lake):
    """A publisher that is unreachable should not cost the morning's update of
    the other ten. A stage puts its rows aside as its last act, so one that
    fails has contributed nothing and the rest are merged without it."""
    with mp.collected_upserts(spark, data_lake):
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "ON-ESTR", 3.9))
        try:
            raise RuntimeError("the publisher was unreachable")
        except RuntimeError:
            pass
        mp.standard_upsert(spark, data_lake,
                           rows_of(spark, data_lake, "2024-01-31", "KR-DFR", 4.0))

    frame = silver(spark, data_lake)
    assert sorted(frame.INSTRID) == ["KR-DFR", "ON-ESTR"]


def test_two_batches_at_once_are_refused(spark, data_lake):
    """One run merges into one table. Two batches would each claim to decide
    which source wins, and the answer would depend on which closed first."""
    with mp.collected_upserts(spark, data_lake):
        with pytest.raises(RuntimeError):
            with mp.collected_upserts(spark, data_lake):
                pass


def test_a_source_merged_on_its_own_still_merges(spark, data_lake):
    """Outside a batch nothing changes: a single source brought up to date by
    hand merges itself, as it always did."""
    mp.standard_upsert(spark, data_lake,
                       rows_of(spark, data_lake, "2024-01-31", "ON-ESTR", 3.9))

    frame = silver(spark, data_lake)
    assert value_of(frame, "ON-ESTR", "2024-01-31") == pytest.approx(3.9)


def merged_rows(monkeypatch):
    """Record what each Silver stage actually hands to the merge.

    The end state cannot answer the question this is asked for. Merging a
    download that has already been merged writes the values that are in the
    table, so an incremental stage and a full one leave exactly the same table
    behind and differ only in how much they moved to get there -- which is the
    whole point of the change and is invisible from the result.
    """
    calls = []
    original = mp.standard_upsert

    def spy(spark, config_path, bronze):
        calls.append(bronze.count())
        return original(spark, config_path, bronze)

    for module in ("silver_yahoo", "silver_electricity"):
        monkeypatch.setattr(
            f"fincommons.marketpricedata.{module}.standard_upsert", spy)
    return calls


def test_a_download_already_merged_is_not_merged_again(
        monkeypatch, spark, data_lake):
    """The Bronze layer holds every price ever downloaded and a morning adds one
    day to it, so a stage that read all of it every run would do the work of the
    whole history to fold in a day."""
    path = mp.path_bronze_yahoo_marketpricedata(data_lake)
    write_yahoo_bronze(path, "2024-02-01",
                       [("2024-01-30", "IE00B4L5Y983", 98.5),
                        ("2024-01-31", "IE00B4L5Y983", 99.0)])

    merged = merged_rows(monkeypatch)
    mp.update_silver_yahoo(spark, data_lake)
    assert merged == [2]

    # A second run has nothing to fold in and should not merge at all.
    mp.update_silver_yahoo(spark, data_lake)
    assert merged == [2]

    # A new download is one more delivery, and only that one is read.
    write_yahoo_bronze(path, "2024-02-02",
                       [("2024-02-01", "IE00B4L5Y983", 99.5)])
    mp.update_silver_yahoo(spark, data_lake)
    assert merged == [2, 1]

    frame = silver(spark, data_lake)
    assert len(frame) == 3
    assert value_of(frame, "IE00B4L5Y983", "2024-02-01") == pytest.approx(99.5)


def test_a_partition_that_grew_after_it_was_merged_is_merged_again(
        monkeypatch, spark, data_lake):
    """Tickers added to the mapping table after the morning's run are
    downloaded into the partition that run already wrote, and the mark names
    that partition as merged. Its file count says it has grown."""
    path = mp.path_bronze_yahoo_marketpricedata(data_lake)
    write_yahoo_bronze(path, "2024-02-01",
                       [("2024-01-31", "IE00B4L5Y983", 99.0)])
    merged = merged_rows(monkeypatch)
    mp.update_silver_yahoo(spark, data_lake)

    # The same day, a ticker added to the table brings its history.
    write_yahoo_bronze(path, "2024-02-01",
                       [("2024-01-30", "NL0010273215", 700.0),
                        ("2024-01-31", "NL0010273215", 705.0)])
    mp.update_silver_yahoo(spark, data_lake)
    assert value_of(silver(spark, data_lake), "NL0010273215", "2024-01-30") \
        == pytest.approx(700.0)

    # And once merged, the partition is left alone again.
    runs = len(merged)
    mp.update_silver_yahoo(spark, data_lake)
    assert len(merged) == runs


def test_the_whole_bronze_layer_can_be_merged_again(
        monkeypatch, spark, data_lake):
    """The mark says where the last run stopped, and there are reasons to ignore
    it: the identifiers a source writes can change, and the rows written under
    the old ones are not removed by a merge. ``full`` is how the table is put
    back together from a Bronze layer that is still intact."""
    path = mp.path_bronze_yahoo_marketpricedata(data_lake)
    write_yahoo_bronze(path, "2024-02-01",
                       [("2024-01-31", "IE00B4L5Y983", 99.0)])
    write_yahoo_bronze(path, "2024-02-02",
                       [("2024-02-01", "IE00B4L5Y983", 99.5)])

    merged = merged_rows(monkeypatch)
    mp.update_silver_yahoo(spark, data_lake)
    assert merged == [2]

    mp.update_silver_yahoo(spark, data_lake, full = True)
    assert merged == [2, 2]


def test_the_mark_is_written_only_after_the_merge_has_succeeded(
        monkeypatch, spark, data_lake):
    """A mark written before the merge would claim a delivery that never
    arrived, and the next run would skip it for good. A failed merge must
    therefore leave the mark where it was."""
    path = mp.path_bronze_yahoo_marketpricedata(data_lake)
    write_yahoo_bronze(path, "2024-02-01",
                       [("2024-01-31", "IE00B4L5Y983", 99.0)])

    def refuse(spark, config_path, bronze):
        raise RuntimeError("the table was not writable")

    monkeypatch.setattr(
        "fincommons.marketpricedata.silver_yahoo.standard_upsert", refuse)

    with pytest.raises(RuntimeError):
        mp.update_silver_yahoo(spark, data_lake)

    assert mp.read_watermark(data_lake, "YAHOO") is None

    # And once it does succeed, the delivery is still there to be folded in.
    monkeypatch.undo()
    mp.update_silver_yahoo(spark, data_lake)
    assert mp.read_watermark(data_lake, "YAHOO") == "2024-02-01"
    assert len(silver(spark, data_lake)) == 1


def test_only_the_revised_electricity_days_are_merged(
        monkeypatch, spark, data_lake):
    """The electricity Bronze layer is rewritten whole every run, so it cannot
    say which of its rows are new. The download can: it leaves every day before
    its window exactly as it was, so only the window has to be merged."""
    monkeypatch.setattr(
        "fincommons.marketpricedata.bronze_electricity.ELECTRICITY_ZONES",
        {"DE-LU": "EL-DELU"})
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1788256800, 30.0),   # 2026-09-01
        (1789034400, 40.0),   # 2026-09-10
    )))
    mp.update_bronze_electricity(spark, data_lake, min_date = "2026-01-01")

    merged = merged_rows(monkeypatch)
    mp.update_silver_electricity(spark, data_lake)
    assert merged == [2]
    assert mp.read_watermark(data_lake, "ELECTRICITY") == "2026-09-10"

    # The next download refreshes the week and adds a day. Only the days from
    # the start of that window are merged, not the September 1st behind it.
    answer_electricity_with(monkeypatch, FakeElectricity(electricity_json(
        (1789034400, 44.0),
        (1789120800, 50.0),   # 2026-09-11
    )))
    mp.update_bronze_electricity(spark, data_lake)
    mp.update_silver_electricity(spark, data_lake)

    assert merged == [2, 2]
    frame = silver(spark, data_lake)
    assert len(frame) == 3
    assert value_of(frame, "EL-DELU", "2026-09-10") == pytest.approx(44.0)
    assert value_of(frame, "EL-DELU", "2026-09-01") == pytest.approx(30.0)


def test_a_yahoo_price_reaches_the_table_under_its_isin(spark, data_lake):
    """The ticker names the instrument at one venue and the ISIN names the
    instrument, so it is the ISIN that reaches a table shared with sources that
    have never heard of Yahoo."""
    write_yahoo_bronze(mp.path_bronze_yahoo_marketpricedata(data_lake),
                       "2024-02-01", [("2024-01-31", "IE00B4L5Y983", 98.5)])

    mp.update_silver_yahoo(spark, data_lake)

    frame = silver(spark, data_lake)
    assert list(frame.INSTRID) == ["IE00B4L5Y983"]
    assert value_of(frame, "IE00B4L5Y983", "2024-01-31") == pytest.approx(98.5)


def test_the_day_a_price_was_fetched_does_not_reach_the_table(spark, data_lake):
    """The Bronze layer is keyed by the day of the download and the Silver table
    by the reference date of the price. Carrying the partition across would make
    the same day of the same instrument two rows."""
    write_yahoo_bronze(mp.path_bronze_yahoo_marketpricedata(data_lake),
                       "2024-02-01", [("2024-01-31", "IE00B4L5Y983", 98.5)])

    mp.update_silver_yahoo(spark, data_lake)

    frame = silver(spark, data_lake)
    assert sorted(frame.columns) == ["INSTRID", "REFDT", "VALUE"]


def test_the_most_recent_download_of_a_day_is_the_one_that_is_kept(spark, data_lake):
    """A reference date that has been downloaded twice is held once per
    download. The merge matches on the instrument and the reference date and
    fails outright where its source holds that pair twice, so the stage has to
    choose between the deliveries rather than hand both of them over."""
    folder = mp.path_bronze_yahoo_marketpricedata(data_lake)
    write_yahoo_bronze(folder, "2024-02-01", [("2024-01-31", "IE00B4L5Y983", 98.5)])
    write_yahoo_bronze(folder, "2024-02-08", [("2024-01-31", "IE00B4L5Y983", 99.5)])

    mp.update_silver_yahoo(spark, data_lake)

    frame = silver(spark, data_lake)
    assert len(frame) == 1
    assert value_of(frame, "IE00B4L5Y983", "2024-01-31") == pytest.approx(99.5)


def test_a_yahoo_layer_that_has_never_been_downloaded_leaves_the_table_alone(
        spark, data_lake):
    """The stage is called on every run of the Silver layer, including in a data
    lake whose Yahoo source has never been downloaded, so an empty Bronze layer
    has to be a quiet no-op."""
    write_internal(mp.path_bronze_internal_marketpricedata(data_lake),
                 [("2024-01-02", "OWN-1", 12.5)])
    mp.update_silver_internal(spark, data_lake)

    mp.update_silver_yahoo(spark, data_lake)

    frame = silver(spark, data_lake)
    assert list(frame.INSTRID) == ["OWN-1"]


def test_the_silver_layer_runs_the_yahoo_source(spark, data_lake):
    """A stage that built its frame and never wrote it would leave the source
    out of the pipeline without any error. This is the test that says it is
    in."""
    write_yahoo_bronze(mp.path_bronze_yahoo_marketpricedata(data_lake),
                       "2024-02-01", [("2024-01-31", "IE00B4L5Y983", 98.5)])

    mp.update_silver_layer(spark, data_lake)

    frame = silver(spark, data_lake)
    assert value_of(frame, "IE00B4L5Y983", "2024-01-31") == pytest.approx(98.5)
