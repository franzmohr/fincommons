"""Market price data: European wholesale electricity prices.

The day-ahead price is what electricity changes hands at on the exchanges, set
one day ahead for each delivery period and for each bidding zone. A bidding zone
is the area within which power can be traded without regard to the grid, so it is
the unit a price exists for at all: there is no one European electricity price,
and there is no single national one either where a country is split into several
zones.

They are taken from the Fraunhofer Institute's energy-charts service, which
serves them without a key. The alternative for Europe is the ENTSO-E
Transparency Platform, which is the authoritative source but answers an
unauthenticated request with 401: it needs a registered token, and every other
source in this module needs no credentials at all.

**The data is published under CC BY 4.0 and the licence requires attribution.**
The service names Bundesnetzagentur | SMARD.de as the origin of the prices, and
:data:`ELECTRICITY_LICENCE` carries what it says, so that the obligation travels
with the code rather than living only in a README.

Source: https://api.energy-charts.info
"""

import json
import os
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from pyspark.sql.functions import (avg, col, count, lit, max as spark_max,
                                   min as spark_min, to_date)
from pyspark.sql.types import (DoubleType, StringType, StructField,
                               StructType)

from .common import *

#: Where the service answers a request for the day-ahead price of one zone.
ELECTRICITY_URL = "https://api.energy-charts.info/price"

#: What the licence obliges anyone republishing these figures to say.
ELECTRICITY_LICENCE = (
    "CC BY 4.0 (creativecommons.org/licenses/by/4.0) "
    "from Bundesnetzagentur | SMARD.de, via energy-charts.info"
)

#: The bidding zones collected, as the service names them against the identifier
#: they reach the Silver table under.
#:
#: Italy and the Nordic countries are split into several zones and only one of
#: each is taken, because a zone is a price and not a country: ``IT-North`` is
#: the northern Italian zone rather than Italy, and ``NO2`` and ``DK1`` are one
#: Norwegian and one Danish zone rather than either country as a whole. The
#: identifiers say the zone rather than the country for the same reason.
ELECTRICITY_ZONES = {
    "DE-LU": "EL-DELU",
    "AT": "EL-AT",
    "FR": "EL-FR",
    "NL": "EL-NL",
    "BE": "EL-BE",
    "CH": "EL-CH",
    "ES": "EL-ES",
    "PT": "EL-PT",
    "IT-North": "EL-ITNORTH",
    "PL": "EL-PL",
    "CZ": "EL-CZ",
    "SK": "EL-SK",
    "HU": "EL-HU",
    "SI": "EL-SI",
    "RO": "EL-RO",
    "GR": "EL-GR",
    "DK1": "EL-DK1",
    "DK2": "EL-DK2",
    "SE3": "EL-SE3",
    "NO2": "EL-NO2",
}

#: The first date asked for. The service holds each zone from when it was formed
#: and answers a request that starts earlier with whatever it has.
ELECTRICITY_FIRST_DATE = "2015-01-01"

#: How many times a refused request is tried again, and how long the wait grows
#: by between attempts. The service rate limits: a run that asks for twenty
#: zones in succession is answered ``429 Too Many Requests`` for most of them,
#: and the refusals fall wherever the limiter happens to be rather than on any
#: particular zone -- the same zone answers on one run and is refused on the
#: next. Pacing alone does not avoid them.
ELECTRICITY_RETRIES = 5
ELECTRICITY_BACKOFF = 4.0

#: The time zone the delivery day of a zone is measured in.
#:
#: **A delivery day is not a UTC day.** It runs from midnight to midnight in the
#: market's own time, so in summer the first period of a day is at 22:00 UTC on
#: the day before. Grouping the periods by their UTC date therefore splits every
#: delivery day in two and averages the last two hours of one day together with
#: the first twenty-two of the next, which is a baseload price for a day that
#: does not exist.
#:
#: Most of these zones trade in the coupled European day-ahead market, whose
#: market time unit is CET and CEST. The three that do not keep their own local
#: day: an hour further east for Greece and Romania, an hour further west for
#: Portugal.
#:
#: Portugal is the one to check against the data rather than against a map,
#: because it shares a market with Spain and not a clock. The service delivers
#: its periods on the Portuguese clock, which shows in two places: asked for a
#: history beginning on the 1st of January, the first Portuguese day comes back
#: one period short under a Berlin day, and the last comes back as a fragment of
#: four quarter-hours dated to the following day. Both are exactly the one hour
#: between the two zones, and neither appears for any zone that really is on CET.
ELECTRICITY_MARKET_TIMEZONE = "Europe/Berlin"
ELECTRICITY_ZONE_TIMEZONE = {
    "GR": "Europe/Athens",
    "RO": "Europe/Bucharest",
    "PT": "Europe/Lisbon",
}

#: How many days of already collected history a run asks for again.
#:
#: A scheduled run collects only what is new, and "new" cannot mean "since the
#: last date held" alone: a delivery day can be republished after the fact, and
#: a run that began the morning after the last date would never see the
#: correction. It therefore begins a week earlier and lets those days be written
#: again, which costs one request per zone and is the difference between a
#: window that can be revised and one that is closed the moment it is read.
ELECTRICITY_REVISION_DAYS = 7

#: The shape the reduced answer is read back in. The download turns the two
#: parallel arrays of the service's answer into one record per delivery period,
#: already carrying the date of the market day it belongs to.
ELECTRICITY_SCHEMA = StructType([
    StructField("date", StringType(), nullable=False),
    StructField("price", DoubleType(), nullable=True),
])


def market_days(answer, zone):
    """Pair each delivery period of an answer with the market day it falls in.

    The date is computed here rather than in Spark because it is the one part of
    this source that is easy to get wrong and hard to see wrong: it depends on
    the market's time zone and on whether summer time was in force that day, and
    a Spark expression would additionally depend on the session's time zone,
    which is not this module's to set. ``zoneinfo`` knows the offsets and the
    dates they changed on.

    Yields ``(date, price)`` pairs, skipping any period the service has no price
    for.
    """
    market = ZoneInfo(ELECTRICITY_ZONE_TIMEZONE.get(zone,
                                                    ELECTRICITY_MARKET_TIMEZONE))
    seconds = answer.get("unix_seconds") or []
    prices = answer.get("price") or []

    for second, price in zip(seconds, prices):
        if second is None or price is None:
            continue
        moment = datetime.fromtimestamp(second, timezone.utc).astimezone(market)
        yield moment.date().isoformat(), float(price)


def electricity_start_date(spark, path, min_date=None):
    """The first delivery date the next download has to ask for.

    A date given by the caller wins, so a full history can always be asked for
    again. Otherwise the answer is a week before the last date already held --
    see :data:`ELECTRICITY_REVISION_DAYS` -- and, where nothing is held yet,
    :data:`ELECTRICITY_FIRST_DATE`.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    path: str
        Path of the Bronze layer of this source.
    min_date: str
        Earliest delivery date to collect, in the format YYYY-MM-DD, or None to
        carry on from what the Bronze layer holds.
    """
    if min_date is not None:
        return min_date

    if not os.path.isdir(path):
        return ELECTRICITY_FIRST_DATE

    held = spark.read.parquet(path).agg(spark_max("date").alias("last")).collect()
    last = held[0]["last"] if held else None
    if last is None:
        return ELECTRICITY_FIRST_DATE

    return str(last - timedelta(days=ELECTRICITY_REVISION_DAYS))


def fetch_electricity_zone(zone, start_date, end_date):
    """Ask the service for one zone's day-ahead prices, retrying a refusal.

    The refusal this retries is ``429``. The service rate limits, and a run that
    asks for twenty zones one after another is refused for most of them, so a
    download that gave up at the first refusal would collect a different and
    arbitrary subset of Europe on every run. Anything else is raised.

    Parameters
    ----------
    zone: str
        The bidding zone in the service's own terms, e.g. ``"DE-LU"``.
    start_date: str
        Earliest delivery date asked for, in the format YYYY-MM-DD.
    end_date: str
        Latest delivery date asked for, in the format YYYY-MM-DD.
    """
    parameters = {"bzn": zone, "start": start_date, "end": end_date}
    headers = {"User-Agent": "fincommons"}

    for attempt in range(ELECTRICITY_RETRIES):
        response = requests.get(ELECTRICITY_URL, params=parameters,
                                headers=headers, timeout=HTTP_TIMEOUT)
        if response.status_code == 200:
            return response.json()
        if response.status_code != 429:
            response.raise_for_status()
        time.sleep(ELECTRICITY_BACKOFF * (attempt + 1))

    raise RuntimeError(
        f"The electricity service refused {zone} {ELECTRICITY_RETRIES} times "
        f"with {response.status_code}. It rate limits, so that is a reason to "
        f"run again rather than a reason to think the zone is wrong."
    )


def update_bronze_electricity(spark, config_path, min_date=None):
    """Download the day-ahead electricity prices into the Bronze layer.

    **The prices are reduced to one row per zone and day on the way in.** The
    service publishes a price per delivery period -- every quarter of an hour in
    most zones and every hour in a few -- and the Silver table holds one value
    per identifier and date, so a day has to become one number somewhere. It
    becomes one here rather than in the Silver stage, so that what is thrown
    away is thrown away once and in the open.

    What is kept is the average over the day, which is the baseload price, and
    with it the lowest and the highest price of the day and how many periods
    they were computed from. The average alone would hide the thing that makes
    electricity unlike the other prices in this module: within one day it can
    run from negative, when there is more wind than demand, to several hundred
    euro. The count is kept because a day the service has only part of averages
    to a different thing from a whole one, and nothing else would show it.

    **A run collects only what is new.** Called without a ``min_date`` it asks
    each zone for the days since the last one the Bronze layer holds, less the
    week of :data:`ELECTRICITY_REVISION_DAYS`, which is what makes this source
    cheap enough to run from a scheduler: a decade of quarter-hours for twenty
    zones is several million periods and a day of them is a few thousand.

    The days before that window are kept exactly as they were collected, and
    the days from it are written again, so a republished day reaches the table
    and a day outside the window is never touched. Passing a ``min_date``
    widens the window and is how a history is collected again after the zones
    or the market days change, as they did when Portugal was found to be on
    its own clock.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    min_date: str
        Earliest delivery date to collect, in the format YYYY-MM-DD. Defaults to
        :data:`ELECTRICITY_FIRST_DATE`.

    Examples
    --------
    >>> update_bronze_electricity(spark, "path/to/configuration/file")
    """
    path = path_bronze_electricity_dayahead(config_path)
    start_date = electricity_start_date(spark, path, min_date)
    end_date = str(date.today())

    with tempfile.TemporaryDirectory() as workspace:
        daily = None

        for zone, instrid in ELECTRICITY_ZONES.items():
            answer = fetch_electricity_zone(zone, start_date, end_date)

            # The answer holds two arrays of one entry per delivery period. It
            # goes through a file on the driver's local disk rather than a data
            # frame built in the driver's memory -- a decade of quarter-hours is
            # hundreds of thousands of entries per zone -- and each period is
            # written with the market day it belongs to, one record to a line.
            file_path = os.path.join(workspace, f"{instrid}.json")
            with open(file_path, "w", encoding="utf-8") as fd:
                for day, price in market_days(answer, zone):
                    fd.write(json.dumps({"date": day, "price": price}) + "\n")

            frame = spark.read.format("json") \
                .schema(ELECTRICITY_SCHEMA) \
                .load(file_path) \
                .withColumn("date", to_date(col("date"))) \
                .groupBy("date") \
                .agg(avg("price").alias("price"),
                     spark_min("price").alias("price_min"),
                     spark_max("price").alias("price_max"),
                     count("price").alias("periods")) \
                .withColumn("zone", lit(instrid)) \
                .select("date", "zone", "price", "price_min", "price_max",
                        "periods")

            daily = frame if daily is None else daily.unionByName(frame)

        fresh = daily.dropna(subset=["date", "price"])

        # The days before the window the download asked for are kept as they
        # are. They go through a directory of their own on the way rather than
        # straight into the union, because Spark reads when it writes and not
        # before: a frame read from the Bronze layer and written back to it
        # would be reading the files that the write is in the middle of
        # replacing. The layer is one row per zone and day, so copying it twice
        # costs nothing worth measuring.
        if os.path.isdir(path):
            staging = os.path.join(workspace, "already_collected")
            spark.read.parquet(path) \
                .filter(col("date") < to_date(lit(start_date))) \
                .write.mode("overwrite").parquet(staging)
            fresh = spark.read.parquet(staging).unionByName(fresh)

        fresh.orderBy("zone", "date").write.mode("overwrite").parquet(path)
