# Market price data

Module `marketpricedata` builds one table of daily (and some monthly) prices and rates -- overnight and policy rates, exchange rates, precious metals, commodity futures, listed shares and indices, electricity prices -- collected from the public sources that publish them and reduced to the same three columns: a reference date, an instrument identifier and a value. You would use it to revalue a position, to compare amounts across reports, to convert a price into euro, or to discount a cash flow on a risk-free zero curve.

A supervisory data set records an amount, a notional or a price in the terms that were agreed. Comparing those figures across reports, revaluing a position, or discounting a cash flow all mean holding the market data they are to be measured against, and that data has to come from somewhere that is dated, traceable and the same for everyone who uses it. This module collects it from the public sources so that a price is looked up in one place and in one shape regardless of who published it.

**At a glance**

* **Sources:** the ECB (European Central Bank), the New York Fed, the Bank of England, the LBMA (London Bullion Market Association), Yahoo Finance, the Dhaka Stock Exchange, VNDirect (Hanoi), energy-charts.info (day-ahead electricity), the Silver table of [`exchangerate`](../exchangerate/README.md), and optionally a proprietary file of your own.
* **Update frequency:** daily; one call, `update_marketpricedata`, brings everything up to date and is safe to repeat.
* **Tables written:** the combined Silver table `marketpricedata` (columns `ref_date`, `instr_id`, `value`; one row per instrument and reference date) and `dse_corporate_actions` (one row per corporate action of a Dhaka share). The Silver layer holds the cleaned tables that analyses read; the Bronze layer holds each publisher's downloads, unchanged.
* **Needs:** network access (no credentials), Spark with **Delta** (the `local` extra off-cluster), and the `yfinance` package for the Yahoo Finance sources (the `marketprice` extra).

## Quick start

1. Install the package with the extras for a local run:

   ```bash
   pip install 'fincommons[local,marketprice]'
   ```

2. Give the configuration file (`paths.yaml`) a `marketpricedata` section with the keys `bronze` and `silver`:

   ```yaml
   marketpricedata:
       bronze: /data/marketpricedata/bronze
       silver: /data/marketpricedata/silver
   ```

3. Start a Spark session with the Delta extensions, build the data set and read it:

   ```python
   from delta import configure_spark_with_delta_pip
   from pyspark.sql import SparkSession

   from fincommons import marketpricedata as mp
   from fincommons.log import configure

   builder = SparkSession.builder \
                       .master("local[4]") \
                       .appName("fincommons") \
                       .config("spark.driver.memory", "15g") \
                       .config('spark.sql.parquet.int96RebaseModeInWrite', 'CORRECTED') \
                       .config('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED') \
                       .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
                       .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
                       .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

   spark = configure_spark_with_delta_pip(builder).getOrCreate()

   config_path = "paths.yaml"
   configure("INFO")                      # the run reports through the log, not print

   mp.create_directory_tree(config_path)   # optional: update_marketpricedata does it too
   outcome = mp.update_marketpricedata(spark, config_path)
   print(outcome["failed"], outcome["failed_sources"])

   prices = mp.connect_to_marketpricedata(spark, config_path)
   prices.filter(prices.instr_id == "ON-ESTR").orderBy("ref_date", ascending=False).show(5)
   ```

What to expect from the first run:

* It collects the whole history: from the 1st of January 2000 by default (`min_date`), from the start of 2020 for each Yahoo ticker (`YAHOO_NEW_TICKER_FIRST_DATE`), and two years back for the Dhaka Stock Exchange, whose first run alone sends some two thousand requests and takes about a quarter of an hour. Every later run collects only what is new.
* The currency pairs are read from the Silver table of [`exchangerate`](../exchangerate/README.md), so bring that module up to date first if you want them.
* A source that cannot be reached is set aside and the others carry on; `outcome["failed_sources"]` names it. See [Running it on a schedule](#running-it-on-a-schedule).
* On Databricks Delta is built in, so a cluster's existing `spark` session can be used as it is and the builder above is unnecessary.

## What you get

### The combined table `marketpricedata`

Every source ends in one Delta table, stored at

```
{path to Silver Layer of marketpricedata as specified in config_path}/marketpricedata
```

or, where the Silver layer is a catalogue schema, as `<schema>.marketpricedata` (see [Configuration](#configuration)). `connect_to_marketpricedata` opens it either way.

| Column | Type | Content |
| --- | --- | --- |
| `ref_date` | date | The reference date the quote belongs to. |
| `instr_id` | string | What the value is a price of, see the identifier table below. |
| `value` | double | The price or rate itself, in the unit the source publishes it in. |

* **Key:** one row per `instr_id` and `ref_date`. A source that delivers a date again replaces the row it delivered before rather than adding a second one.
* **None of the three columns is ever empty.** A row without an instrument, a date or a value is dropped before it is merged (see [Complete rows](#complete-rows-only)).
* **Nothing is filled in for days a source does not quote on.** A rate quoted on business days is absent at the weekend, and a caller who needs a value for every calendar day has to carry the last quote forward themselves. This differs from `exchangerate`, whose Silver table does hold one row per calendar day, and it is worth knowing before the two are joined.
* **There is no column for the source, the unit, the currency or the frequency.** The identifier is the only thing that tells the sources apart; `describe_instrument` reads all of that back for one identifier (see [Common tasks](#look-up-what-an-identifier-means)).

### What an identifier looks like

The instrument identifier is not a single code system but a convention per source:

| Source | `instr_id` | Example |
| --- | --- | --- |
| ECB, €STR | a name for the overnight rate | `ON-ESTR` |
| ECB, key rates | a name for the policy rate | `KR-DFR`, `KR-MRO`, `KR-MLF` |
| ECB, US yield curve | a name for the yield or spread, ending in `-M` | `YLD-US10Y-M`, `SPR-US10Y1Y-M` |
| ECB, euro area | a name for the yield, rate or index, ending in `-M` | `YLD-EA10Y-M`, `IBOR-EUR3M-M`, `EQ-ESTOXX-FINANC-M` |
| New York Fed, SOFR | a name for the overnight rate | `ON-SOFR` |
| Bank of England, SONIA | a name for the overnight rate | `ON-SONA` |
| `exchangerate` | the currency pair as `BASE/QUOTE` | `EUR/USD` |
| LBMA | a name for the metal | `CO-GOLD`, `CO-SILVER` |
| Yahoo Finance, commodities | a name for the commodity | `CO-BRENT`, `CO-WTI`, `CO-NATGAS`, `CO-COPPER` |
| Yahoo Finance, instruments | the ISIN of the instrument | `IE00B4L5Y983` |
| Yahoo Finance, exchange rates the ECB does not fix | the currency pair as `BASE/QUOTE` | `EUR/BDT`, `EUR/VND` |
| Yahoo Finance, volatility indices | a name for the index | `VOL-VIX`, `VOL-MOVE` |
| Yahoo Finance, stock indices | a name for the index | `IX-N225`, `IX-HSI` |
| Dhaka Stock Exchange | the trading code, and a name for the index | `DSE-GP`, `IX-DSEX` |
| VNDirect | the ISIN of a Hanoi share, and a name for the index | `VN000000SHS3`, `IX-HNX` |
| energy-charts.info | the bidding zone of a day-ahead electricity price | `EL-DELU`, `EL-AT` |
| internal | whatever the delivered file states | |

What the prefixes mean:

* `ON-` is an overnight rate the market traded at, `KR-` a policy rate a central bank decided on. The two kinds are not read the same way, which is why the prefix says which kind of rate it is.
* `-M` at the end marks a **monthly** series, dated to the last day of its month (see [The monthly ECB series](#the-monthly-ecb-series)).
* `YLD-`, `SPR-`, `IBOR-` and `EQ-` are the ECB's yields, spreads, interbank rates and equity indices; `CO-` commodities; `VOL-` and `IX-` volatility and stock indices; `EL-` electricity; `DSE-` Dhaka shares.

There is no column saying which source a row came from, so an identifier must not be claimed by two sources at once. The six rates are named rather than given an ISIN (International Securities Identification Number) for that reason: they are rates, not instruments, and an ISIN would collide with the Yahoo source.

### Joining to other modules

* An ISIN in `instr_id` is the ISIN [`firds`](../firds/README.md) (the ESMA Financial Instruments Reference Data System) records the instrument under; the European shares in the Yahoo mapping table were taken from FIRDS in the first place.
* A currency pair `EUR/XXX` is named and quoted as units per euro, as the ECB names and quotes its reference rates in `exchangerate`, so a price in `XXX` is divided by it to give euro.

### The DSE corporate actions `dse_corporate_actions`

One row per corporate action of a share listed on the Dhaka Stock Exchange (DSE), opened by `connect_to_dse_corporate_actions(spark, config_path)`:

| Column | Content |
| --- | --- |
| `instr_id` | the share, as the price table names it |
| `ex_date` | the first day it traded ex |
| `kind` | `bonus`, `cash` or `rights` |
| `factor` | what a close before the ex-date is multiplied by to be comparable with one on or after it |
| `record_date`, `detail`, `source`, `ex_date_from` | the record date, the terms as read, the notice, and whether the ex-date came from a notice or, where there was none, from the first day the share traded after the record date |

Why the table exists and how the factors are computed is under [Dhaka Stock Exchange](#public-data-from-the-dhaka-stock-exchange).

## Common tasks

The recipes assume `spark` exists, `config_path = "paths.yaml"`, and

```python
from pyspark.sql import functions as F
from fincommons import marketpricedata as mp

prices = mp.connect_to_marketpricedata(spark, config_path)
```

### Get one series

When you want the history of one instrument or rate:

```python
estr = prices.filter(F.col("instr_id") == "ON-ESTR").orderBy("ref_date")
```

### Look up what an identifier means

When you meet an identifier and want to know its name, publisher, unit, currency and frequency -- none of which is in the table -- use `describe_instrument`:

```python
print(mp.describe_instrument("EUR/VND"))
```

```
EUR/VND -- Vietnamese dong per euro
  kind         exchange rate
  source       Yahoo Finance, ticker EURVND=X
  unit         VND per EUR
  currency     --
  frequency    daily, Yahoo's trading days
  collected by update_bronze_yahoo_fx / update_silver_yahoo_fx
  note         A market quote at Yahoo's close, not a fixing. The ECB fixes no reference rate for VND, which is why it comes from Yahoo.
  note         Named and quoted like the ECB rates, so a price is divided by it the same way.
```

* The result is an `InstrumentDescription`, whose fields (`identifier`, `name`, `kind`, `source`, `source_id`, `unit`, `currency`, `frequency`, `collected_by`, `notes`) can also be read one by one. `currency` is None for a rate, a ratio or an index level.
* It knows every kind of identifier the public sources write -- ISINs of listed instruments (with the name and the ticker they are downloaded by), `EUR/XXX` rates from the ECB or Yahoo, the `ON-` and `KR-` rates, the ECB's `YLD-`, `SPR-`, `IBOR-` and `EQ-` series, the `EL-` electricity zones, the `CO-` commodities, the `VOL-` and `IX-` indices and the `DSE-` shares.
* It reads them from the same tables the downloads are driven by, so it cannot describe a series differently from how it is collected. The ISIN mapping table carries a `name` column for this: the full name FIRDS records for the European shares, VNDirect's English company name for the Vietnamese ones, and the index or fund name for the hand-chosen benchmarks.
* It needs neither Spark nor the data lake, with one exception: the name of a Hanoi share is only in VNDirect's listings, so it is read from the lake:

  ```python
  print(mp.describe_instrument("VN000000SHS3", config_path=config_path))
  ```

* An identifier no public source delivers raises `KeyError`; it may still be a proprietary quote, which the module takes as it comes.

### The latest quote at or before a date

When you need a value for a day the source did not quote on (a weekend, a holiday, a day inside a monthly series), carry the last quote forward:

```python
latest = prices \
    .filter((F.col("instr_id") == "ON-SOFR") & (F.col("ref_date") <= "2025-06-30")) \
    .orderBy(F.col("ref_date").desc()) \
    .limit(1)
```

### Convert a price into euro

When you have a price in another currency, divide it by the `EUR/XXX` rate of the same day -- the rates are quoted as units per euro, whether they come from the ECB through `exchangerate` or from Yahoo:

```python
share = prices.filter(F.col("instr_id") == "VN000000SHS3")          # a Hanoi share, in dong
eur_vnd = prices.filter(F.col("instr_id") == "EUR/VND") \
    .select("ref_date", F.col("value").alias("eur_vnd"))

in_euro = share.join(eur_vnd, "ref_date").withColumn("value_eur", F.col("value") / F.col("eur_vnd"))
```

The inner join keeps only the days both series quote on; carry the rate forward first where that matters.

### Build the risk-free zero curves and discount payments

When you want a discount factor for a currency and a maturity, `curves` builds a zero curve per currency for one reference date from the overnight rates and swap quotes in the table, and evaluates it on the maturities of any Spark frame:

```python
from fincommons.marketpricedata import curves

segments = curves.build_curves(spark, prices, "2025-06-30", ["EUR", "USD"])

payments = spark.createDataFrame([("EUR", 0.5), ("EUR", 5.0), ("USD", 2.0), ("JPY", 1.0)],
                                 ["currency", "years"])
discounted = curves.add_discount_factor(payments, segments, "currency", "years", "discount_factor")
```

* `years` is the maturity as a fraction of a year on actual/360. The `JPY` payment keeps its row and gets no discount factor, because there is no curve for it -- so a caller can report it as not covered rather than lose it.
* `curves.parallel_shift(segments, shifts)` makes one copy of the curves per assumed shift of the continuously compounded zero rate. `shifts` has the columns `currency`, `shock` (the size of the movement, for instance in standard deviations) and `shift` (the absolute change in the rate). Evaluate the shifted curves in the same join with `keys=["shock"]`, on a frame that carries a `shock` column.
* `curves.forward_rate(df, start_df_col, end_df_col, accrual_col, out_col)` gives the simple forward rate of an accrual period from its two discount factors.
* **No tenor pillar is delivered into the table yet, so a curve built today is flat at the overnight rate.** How the curve is built is under [Zero curves](#zero-curves-and-discount-factors).

### Collect or re-collect one publisher

When a single source needs to be collected, inspected or collected again on its own, call its Bronze and Silver stages:

```python
mp.update_bronze_lbma(spark, config_path)
mp.update_silver_lbma(spark, config_path)
```

Every publisher has such a pair; they are listed under [Reference](#public-functions). A source collected for a date range takes `min_date`, for instance `mp.update_bronze_yahoo(spark, config_path, min_date = "2024-01-01")`; an incremental Silver stage takes `full=True` to merge its whole Bronze layer again.

### Add your own quotes

When you hold prices no public source delivers, put them in a CSV file of three columns (`ref_date`, `instr_id`, `value`, with a header row) at

```
{path to Bronze Layer of marketpricedata as specified in config_path}/internal/marketpricedata_internal.csv
```

and merge it:

```python
mp.update_silver_internal(spark, config_path)
```

`update_marketpricedata` picks it up on every run, last, so a proprietary quote wins over a public one for the same instrument and day. The rules for the file are under [Proprietary data](#proprietary-data).

### Extend the Yahoo Finance coverage

When an instrument is missing, add a row to the mapping table shipped with the package, `src/fincommons/data/isin_yahoo.csv` (columns `isin`, `yahoo`, optionally `first_date` and `name`), rather than changing any code. A ticker added to a lake that already holds a history is asked for its own history on the next run.

## Running it on a schedule

**One call, `update_marketpricedata`, brings the whole table up to date and says what it could not reach.**

```python
mp.update_marketpricedata(spark, config_path)
```

It runs three stages in order:

1. `update_bronze_layer` -- collects the ECB, the New York Fed, electricity, the LBMA, the Bank of England, the DSE and VNDirect into the Bronze layer;
2. `update_bronze_yahoo_layer` -- collects the five Yahoo Finance sources: instruments, commodities, volatility indices, stock indices and exchange rates;
3. `update_silver_layer` -- folds the whole Bronze layer into the Silver table, in one merge.

It also creates the directory tree if it is missing.

**It is idempotent and needs no argument saying what to collect.** What a run has to collect is read from what the data lake already holds: the same call is correct on the first morning, when it collects the whole history, and on every morning after, when it collects a day.

* `min_date` decides where the history of a **new** data lake begins and defaults to the 1st of January 2000. It applies only to a source whose Bronze layer is still empty, so passing it to a lake that already holds data changes nothing; collecting a history again is done by calling that one source with a `min_date` of its own.
* `full = True` ignores the merge marks and merges every source from the beginning. A scheduled run never needs it (see [The merge is incremental](#the-merge-is-incremental-on-the-same-terms)).

**It reports through the log and its return value, never through standard output.** It records what it does in the log of `fincommons.log` -- each source as it is collected and merged, at `INFO`, a source that failed with its traceback at `ERROR`, and a closing `WARNING` that counts the sources set aside -- so call `fincommons.log.configure()` first to see it. It returns the outcome of the run, whose `failed` entry is the number of sources set aside in all three stages together and `failed_sources` their names as `layer/SOURCE`.

### The script a scheduler runs

No module of the package opens a Spark session, so a job scheduler runs a short script around the call rather than the module itself. The session belongs to whoever starts the run -- a cluster has one already, a workstation has to say how much of itself to give it -- and a pipeline that opened its own would be deciding that for every caller:

```python
# update-marketpricedata.py -- the script a job scheduler runs
# Spark launches its Python workers with whatever `python3` resolves to on
# the PATH, which need not be the interpreter running this script. This has
# to be settled before the session is built.
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from fincommons import marketpricedata as mp
from fincommons.log import configure

# The log goes to standard error. The script writes nothing to standard
# output: what it did is the log, and whether it did all of it is the exit
# code.
configure("INFO")

spark = ...  # the session of "Quick start" above

try:
    outcome = mp.update_marketpricedata(spark, "/path/to/paths.yaml")
finally:
    spark.stop()

# A source set aside is not a failure of the run, which is why the library
# returns rather than raises for it -- but it is a job that did less than it
# was scheduled to do, and a scheduler only sees the exit code.
sys.exit(1 if outcome["failed"] else 0)
```

The exit code is 0 only where every source was updated and 1 where any was set aside; a run that did not complete raises, which ends the script with 1 as well. A scheduler watching the exit code therefore sees a partial run as a failed job, and the log on standard error says which sources it was.

### The Spark session

* **The session has to be built with the Delta extensions.** Unlike `exchangerate` and most of the other modules, this one does not offer a choice of storage format: the Silver table is maintained by upsert rather than by overwrite, which is something only Delta does. Off-cluster that needs the `delta-spark` package, which comes with the `local` extra; on Databricks Delta is built in and the extra configuration is unnecessary.
* **The cores are stated as `local[4]`** rather than through `spark.cores.max`, which only a standalone or Mesos master reads. A session built without a master comes up as `local[*]` and runs a task on every core the machine has, dividing the driver heap -- which in local mode is the memory of the whole run, since the executors live in that JVM -- among all of them. The FIRDS [README](../firds/README.md) says what that costs on a machine with many cores.
* The three `parquet...RebaseMode...` settings and `spark.driver.memory` are as in the other modules of the package.
* **Sizing the cores, the heap and the scratch directory** for your machine, and checking that the session got them, is in [Setting up a Spark session](../../../docs/spark-session.md).

### When a run fails

* **A source set aside:** look up its name in `outcome["failed_sources"]` and its traceback at `ERROR` in the log. Nothing has to be repaired by hand: the next run collects what this one missed, since the merge marks are only written after a merge has succeeded.
* **A run that raised:** run it again. A run that fails before its merge folds the same deliveries in again next time, which costs a little and is the safe way round.
* **What actually arrived** is the latest `ref_date` of each `instr_id` in the Silver table.

### Running one layer at a time

The two layer functions run the sources of a layer in turn, and `update_marketpricedata` is the three calls below:

```python
# Download everything the module collects, except Yahoo Finance
mp.update_bronze_layer(spark, config_path)

# The five Yahoo Finance sources
mp.update_bronze_yahoo_layer(spark, config_path)

# Reduce it all into the Silver table
mp.update_silver_layer(spark, config_path)
```

A publisher that is unreachable on a given morning should not stop the others from being brought up to date, so a source that fails is set aside with its traceback and the run carries on. That is the `ERROR` level of `fincommons.log`: a unit of work failed by design, not the run. Each function returns the outcome of its stage, whose `failed` entry counts the sources that were set aside and whose `failed_sources` entry names them, so a caller can act on a partial run without reading the log back. Configure logging with `fincommons.log.configure()` to see the tracebacks.

**The Yahoo Bronze stages are not part of `update_bronze_layer`.** They are the only downloads that have to be told how far back to go and the only ones that need a package outside the dependencies of the project, so they are run by `update_bronze_yahoo_layer`, which `update_marketpricedata` calls between the two. A missing `yfinance` is therefore not reported by a failing import of `update_bronze_layer` and does not stop the rest of it. Their Silver stages need neither and are part of `update_silver_layer`.

## How it works

### What "only new data" means, by source

**Some sources are collected incrementally and some in full on every run; which kind each one is matters when reading how long a run takes and what a run can repair.**

**Collected incrementally: Yahoo Finance, the electricity prices, the DSE and VNDirect.** Their histories are large -- a decade of quarter-hours for twenty bidding zones is several million delivery periods -- so fetching them again every morning would *be* the cost of the job. They ask only for the days they are missing:

* Yahoo carries on from the partitions its Bronze layer is written in, less a fortnight (`YAHOO_REVISIT_DAYS`).
* Electricity carries on from the last date it holds, less a week (`ELECTRICITY_REVISION_DAYS`).
* VNDirect carries on from the last partition less a week; the DSE is described under its own section below.

Yahoo's fortnight is there for a different reason from electricity's week:

* **Yahoo does not fail a download it cannot answer in full.** When it rate limits, it leaves the tickers it refused out and returns the rest. A run that carried on exactly where the last one stopped would never go back for them, so one bad morning would leave a permanent gap in those series. Asking for the last fortnight again closes the gap on the next run that succeeds. A ticker that comes back with nothing over a fortnight is named in a warning, and a download with nothing for any ticker is raised, not written.
* **An electricity day can be republished.** "New" cannot mean "after the last date held" alone: a run that began the morning after the last date would never see the correction. Within the week the service is what the table says, so a day it revises is revised here and a day it withdraws is withdrawn here. Outside it, a day already collected is never touched.

**Collected in full, every run: the ECB, the New York Fed, the Bank of England and the LBMA.** That is deliberate rather than unfinished:

* Each publishes its whole series as one file and offers no way to ask for part of it.
* That file is also how a **revision to an older value** arrives: SOFR carries a revision marker, and a download that only ever asked for days after the last one held would never see a figure corrected behind it.
* Because the Silver stage merges rather than appends, collecting the file again replaces the rows it has seen before, so the whole download is what keeps the table *current* rather than merely longer.

They are not all small, and it is worth knowing which is which before reading a run's timings: the ECB `FM` flow is about 94,000 rows and the €STR file 26,000, the two LBMA benchmarks about 15,000 each, SONIA 7,500 and SOFR 2,100.

### The merge is incremental on the same terms

**The Silver stages read only the downloads that have not been merged yet.** Collecting less is only half of it: a stage that read its whole Bronze layer every morning would do the work of the entire history to fold in a day, which for the Yahoo stages is roughly a million rows.

* A Yahoo Bronze layer is partitioned by `LASTDT`, the day the download ran, and is appended to rather than rewritten, so a partition never changes once it is written: merging one a second time writes values that are already in the table.
* The electricity stage is the same idea from the other end -- its Bronze layer *is* rewritten whole, so it cannot say which rows are new, but the download can, because it leaves every day before its window exactly as it was.

How far each source has been merged is recorded in one small file per source under `_watermarks` beside the Silver table:

* It is kept there rather than in the table because a row in the table carries the reference date of a price and not the day it was fetched, so the table cannot be asked where the last run stopped.
* **The mark is written after the merge has succeeded**, never before: a mark written first would claim a delivery that never arrived, and the next run would skip it for good. A run that fails therefore folds the same deliveries in again, which costs a little and is the safe way round.
* A source without a mark, or with one that cannot be read, is merged from the beginning, which is what puts the table into the state the marks then claim.

Passing `full = True` ignores the marks and merges every source from the beginning. A scheduled run never needs it. It is how the table is rebuilt from a Bronze layer that is still intact, which is worth doing after the identifiers a source writes have changed, since the rows written under the old ones are not removed by a merge and have to be dealt with separately.

### One merge for the run, not one per source

**A merge costs what it costs whether it carries a million rows or a hundred**, so `update_silver_layer` collects every source and merges them **once**, through `collected_upserts`.

A merge rewrites the files it touches in the whole table, and against a Silver table of about a million rows that fixed cost dominates everything else. Measured here with each source merged on its own: the New York Fed stage merged 2,100 rows in 15 seconds, and the electricity stage, merging only its revision window, merged 160 rows in 15 seconds as well. Eleven sources merging one after another therefore cost eleven times that, however little any of them had to say.

Two things follow, and the module does both:

* **Skipping a merge saves time; making a merge smaller does not** -- which is why the incremental Yahoo stages, which merge nothing at all when there is nothing new, cost half a second each instead of fifteen.
* The remaining cost is a count of merges rather than a volume of rows, so the sources are merged in one batch.

The effect on a run with nothing new to fold in, against the same table:

| | separately, whole | separately, incremental | one merge |
|---|---|---|---|
| Silver phase | ~197s | 151s | **25s** |
| merges | 11 | 10 | **1** |

The single merge carried 402,000 rows in 20 seconds -- more rows than any of the eleven, in less time than two of them.

**The single merge still decides which source wins.** A reference date and an instrument delivered by two sources can only be one row, and a merge handed that pair twice fails outright rather than choosing, so the collected rows are reduced to the last delivery of each pair. *Last* means latest collected, which is the same outcome as merging the sources one after another, each overwriting what the one before it had written. That is why `update_silver_layer` runs the proprietary source last.

The marks are held back in the same way and written only once the merge has succeeded, for the reason they are written after a merge at all. A source that fails inside the batch has contributed nothing, because a stage puts its rows aside as its last act, so the run merges what the others delivered and carries on.

### Complete rows only

**None of the three columns is ever empty**, because `complete_rows` drops every incomplete row before any merge:

* A row without an instrument or a reference date would match nothing in the table and be inserted again on every run, because a null equals nothing.
* A row without a value is a publisher saying it has no observation, and it would otherwise replace one or be read as the newest quote.
* Inside a batch the rows are dropped before the sources are reduced to one delivery per day, so a source with nothing for a day cannot win that day over one that has a figure.

A row is identified by `instr_id` and `ref_date` together. A source that delivers a date again replaces the row it delivered before rather than adding a second one, which is what `standard_upsert` does and what makes the pipeline safe to re-run on a day that has already been loaded. `standard_upsert` also projects a delivery onto the three columns, because the sources do not agree on what they carry beyond them and a merge only works where the two sides match.

### What a successful run does not promise

**A source that fails is counted, not only logged.** A publisher that is unreachable is set aside with its traceback and the run carries on, which is the point of the design: one source being down should not cost a morning's update of the other ten.

* Each stage records the sources it set aside in its outcome -- `failed`, the number, and `failed_sources`, their names as `layer/SOURCE` -- and ends with a warning that counts them, and `update_marketpricedata` adds the three stages up and returns the total.
* The functions return rather than raise for it, because a caller in a notebook should still get what the other sources delivered.
* The script a scheduler runs turns the count into its exit code, as the one [above](#the-script-a-scheduler-runs) does.
* The latest `ref_date` of each `instr_id` in the Silver table remains the answer to what actually arrived.

### From source to Bronze layer

**Each publisher gets a directory of its own in the Bronze layer**, so that a price is always traceable to the source it was taken from.

The downloads pass through a temporary directory on the local disk of the driver and not through the data lake: the CSV files are never meant to be kept, and on Databricks the Bronze layer is a Unity Catalog volume that should not be used as scratch space. Only the parquet data set reaches the data lake.

**The ECB, the New York Fed, the Bank of England and the LBMA share one download, and refuse an answer that is not the series before it is written.** Their series are written over the Bronze layer whole, so the check matters:

* The download is `download.downloaded`, which sends the user agent `fincommons` (`USER_AGENT`) and the timeout `HTTP_TIMEOUT` and closes the response whatever happens.
* An answer with an error status or without a single byte is refused by the download itself.
* One that parses to no observation -- an error page served with status 200 is the usual case -- is refused by `download.refuse_empty_answer`, which reads the answer exactly as it is about to be written.
* Either way the download raises, naming the source and the address, and the Bronze layer keeps the series the last run wrote.

Without the check an error page would replace years of history with nothing and the run would report success, because the Silver stage would then find nothing to take and the table would simply stop moving.

### Public data from the ECB

**Two flows of the ECB's API, `EST` and `FM`, are downloaded unchanged, each into a directory of its own.** The API answers with CSV, one request per data flow.

```python
# Download ECB data and store it in the bronze layer folder
mp.update_bronze_ecb(spark, config_path)

# or one flow at a time
mp.update_bronze_ecb_estr(spark, config_path)
mp.update_bronze_ecb_fm(spark, config_path)
```

* Both are parsed with a schema shipped in the package, `marketpricedata_ecb_estr` and `marketpricedata_ecb_fm`, rather than with an inferred one, so that a change in what the API returns shows up as a column that does not fit instead of as a table that has quietly changed shape (see [Adding or renewing an ECB schema](#adding-or-renewing-an-ecb-schema)).
* The header row the API sends is skipped: a reader that is given a schema does not skip it by itself, and where it is parsed it arrives as a row whose typed fields are all null.
* The two flows are not typed alike. In the `EST` schema `TIME_PERIOD` is a date and `OBS_VALUE` a double, so the €STR series reaches the Silver layer in the types the table wants without a cast. In the `FM` schema `TIME_PERIOD` is a string, which `update_silver_ecb_fm` casts.

The Silver stage, `update_silver_ecb`, runs four stages: `update_silver_ecb_estr`, `update_silver_ecb_fm`, `update_silver_ecb_fm_monthly_us` and `update_silver_ecb_fm_monthly_ea`. It is mostly a matter of picking one series out of a flow that carries many.

**€STR: three filters, all needed.** The `EST` flow holds fifteen series, nine on the €STR benchmark alone -- the rate itself, but also the number of banks reporting, the volume, the 25th and 75th percentiles and so on. The one that is wanted is `FREQ = 'B'`, `BENCHMARK_ITEM = 'EU000A2X2A25'`, `DATA_TYPE_EST = 'WT'`: the volume-weighted trimmed mean rate on a business-day frequency, which is the number normally meant by "€STR". All three parts of the filter are needed, because the benchmark item alone still leaves nine series and the frequency is shared by all of them.

**The key ECB rates: daily only.** The `FM` flow is the same problem several times over: it carries 115 series at five frequencies at once, and the frequency is what decides most of what can be taken from it. The Silver table is keyed by a date, and a year, a quarter and a month are not dates, so only `FREQ = 'D'` can be read at all -- and that is also the right answer rather than merely the available one, because a monthly average of a rate is not the same kind of number as the daily quotes it would sit beside. What the daily frequency holds is the key ECB interest rates, on every calendar day back to 1999. Three of them are taken (`ECB_KEY_RATES`):

| Series | Identifier | Rate | Role |
| --- | --- | --- | --- |
| `DFR` | `KR-DFR` | deposit facility | what a bank earns on money left with the Eurosystem overnight -- the floor |
| `MRR_RT` | `KR-MRO` | main refinancing operations | the rate on the weekly tenders |
| `MLFR` | `KR-MLF` | marginal lending facility | what a bank pays to borrow overnight against collateral -- the ceiling |

* `DATA_TYPE_FM = 'LEV'` is the third part of the filter, because the flow publishes the change in percentage points beside the level under the same name and the same date, and the table holds what a rate is rather than what it moved by.
* `MRR_RT` is the main refinancing rate and not `MRR_FR` or `MRR_MBR`, which the flow publishes beside it. Those two are the rate under one tender regime each and each covers only the years its regime ran in -- the minimum bid rate stops in October 2008, when the ECB moved to fixed rate full allotment. `MRR_RT` carries both, which is why it is the only one of the three that spans the whole history.

#### The monthly ECB series

**The monthly US and euro area series are the one part of the module that is not daily; they are dated to the last day of the month and their identifiers end in `-M`.**

The US yield curve series are taken by `update_silver_ecb_fm_monthly_us` (`ECB_MONTHLY_US`):

| Identifier | Series | What it is |
| --- | --- | --- |
| `YLD-US10Y-M` | `US10YT_RR` | US 10-year government benchmark bond yield, averaged over the month |
| `YLD-US10YZ-M` | `USD10YZ_R` | US 10-year zero coupon yield, at the end of the month |
| `SPR-US10Y1Y-M` | `US10YT_US1YT` | the spread between the 10-year and the 1-year |

* **A month is dated to its last day.** A table keyed by a date has to say which day a month is. The last day is the date by which the observation is complete, so nothing is dated earlier than the last day it draws on, and a reader carrying the last quote forward never carries a figure across days it already covers -- which dating it to the first of the month would do.
* **The identifier says the series is monthly**, ending in `-M`. The Silver table has no column for the frequency, so a reader who joins these to the daily sources on the reference date finds a value on one day a month and nothing on the others, with nothing in the row to explain it. The suffix is the one place that warning can live, and it is what makes these series safe to have in the table at all.
* `YLD-US10Y-M` is an average of daily observations through the month while `YLD-US10YZ-M` is a month-end figure, so neither is a month-end quote of the other.
* `SPR-US10Y1Y-M` is a **term spread**, not a term premium: the spread is two observed yields subtracted, whereas the premium is the ten-year yield less the average short rate expected over ten years, which is not observable and takes a term structure model. They coincide only if expected future short rates equal today's one-year rate.

The euro area block is taken the same way, by `update_silver_ecb_fm_monthly_ea` (`ECB_MONTHLY_EA`), and is the widest of the three:

| Prefix | What it is | Unit | Series |
| --- | --- | --- | --- |
| `YLD-EA…-M` | government benchmark curve at 2, 3, 5, 7 and 10 years, plus the real 10-year | per cent per annum | 6 |
| `IBOR-EUR…-M` | EURIBOR at 1, 3, 6 and 12 months, plus the real 3-month | per cent per annum | 5 |
| `ON-EONIA-M` | the euro overnight benchmark until €STR replaced it | per cent per annum | 1 |
| `EQ-ESTOXX…-M` | the EURO STOXX index and ten of its sectors | index points | 11 |

* The curve reaches back to 1970.
* The prefix is the only thing that says which kind of number an identifier holds, which matters most between the first three and the last: a rate in per cent and an index level in points are not comparable, and nothing in the row says so.
* **Two of the twenty-five euro area series the flow publishes are deliberately left out.** `UONSTR` is €STR, which the table already carries daily as `ON-ESTR`, and `DJES50I` is the EURO STOXX 50, which arrives daily from Yahoo Finance under its ISIN. A monthly copy of a series held daily is not more data; it is the same data at a coarser grain under a second name, and a reader comparing the two would be comparing a series with itself. `test_a_monthly_copy_of_a_daily_series_is_left_out` is what records that, since nothing in the code would otherwise say why those two are missing.
* `EONIA` is kept although it stopped in December 2021. Nothing else in the table carries it, and a supervisory data set still meets contracts that reference it; its end shows up in its last reference date like that of any other series that has stopped.
* What is left out is everything the flow carries yearly and quarterly, which duplicates the monthly series at a coarser grain.

The three FM stages leave each other's series alone and claim no identifier twice.

### Public data from the New York Fed

**SOFR is read from the Federal Reserve Bank of New York, which administers and publishes it**, rather than from FRED, which mirrors it, for the same reason SONIA is read from the Bank of England.

```python
# Download New York Fed data and store it in the bronze layer folder
mp.update_bronze_nyfed(spark, config_path)

# or one rate at a time
mp.update_bronze_nyfed_sofr(spark, config_path)
```

* The markets API answers JSON, with the observations inside one `refRates` array rather than one to a line, so the file is read whole as the LBMA answer is.
* It takes a date range rather than a count -- the `last/n` form refuses a count large enough to cover the history -- so the whole of it is asked for by date.
* The request carries a user agent, as at the Bank of England and the LBMA.

**The Bronze layer keeps more than the rate**, and that is most of the reason for reading the administrator. The mirror carries the rate alone; the administrator publishes the distribution it was drawn from and a flag saying whether the figure has been revised:

| Column | Content |
| --- | --- |
| `sofr` | the rate |
| `percentile1`, `percentile25`, `percentile75`, `percentile99` | the distribution of the transactions behind it |
| `volume` | the traded volume, in billions |
| `revision` | set where the observation has been republished |

* The percentiles and the volume say whether a rate was drawn from a thin or a dispersed market, which is the question a supervisor asks of a benchmark.
* `revision` matters more than it looks: the Silver table is maintained by upsert, so a revised figure silently replaces the one collected before it, and without the flag nothing anywhere would record that a figure had ever moved.
* None of it fits the three columns of the Silver table, which takes the rate -- so the Bronze layer is where it survives.

Compared date by date, the two agree on every one of the 2,112 dates they share, and the administrator has one the mirror lacks: the 2nd of April 2018, the first observation.

### Public data from the Bank of England

**SONIA is the Bank of England's own benchmark and is taken from the Bank, not from FRED**, which mirrors the series under the Bank's code. A mirror is a convenience rather than a source: it runs a day behind at times, it can stop being maintained without saying so, and a supervisory data set should be able to name the body that published each of its figures.

```python
# Download Bank of England data and store it in the bronze layer folder
mp.update_bronze_boe(spark, config_path)

# or one series at a time
mp.update_bronze_boe_sonia(spark, config_path)
```

Two things about this source are properties of it rather than choices, and both fail quietly if they are undone:

* **The request has to carry a user agent.** The Bank serves its statistical database through a content delivery network that answers a request without one with `403 Access Denied` and an HTML page. A CSV reader handed that page returns a table of one column and no rows, so a download that lost its user agent would write an empty Bronze layer and report success. The download therefore checks the status before it writes anything, and refuses an answer that parses to no observation, which is what the same page served with status 200 would be.
* **The dates have to be parsed rather than cast.** The Bank writes an observation as `02 Jan 2026` and not as `2026-01-02`, so a cast yields null for every row of the series. The month is an English abbreviation, which is the locale Spark parses in by default.

The series is `IUDSOIA`, the Bank's code for the daily SONIA rate, and the identifier in the Silver table stays `ON-SONA`. An identifier names the rate and not the publisher, so a change of source is not a change of identifier -- and renaming it would leave the rows collected under the old name behind, because the upsert replaces a row but never removes one.

### Public data from the LBMA

**Gold and silver are benchmarks that a body sets and publishes**: the London Bullion Market Association sets the LBMA Gold Price and the LBMA Silver Price, and they are taken from the association for the same reason SONIA is taken from the Bank of England.

```python
# Download the precious metal benchmarks into the bronze layer folder
mp.update_bronze_lbma(spark, config_path)

# or one metal at a time
mp.update_bronze_lbma_gold(spark, config_path)
mp.update_bronze_lbma_silver(spark, config_path)
```

* Gold is set twice a day and the afternoon benchmark is the one taken: it is what is normally meant by "the LBMA Gold Price" and what a holding is valued at, because it is set while both London and New York are trading. Silver is set once a day, so there is nothing to choose between.
* The association serves JSON rather than CSV, as one array rather than one object per line, so the file is read whole. It refuses a request without a user agent, as the Bank of England does.
* **The price is taken in US dollars.** Each entry carries the same price in three currencies, in the order US dollar, pound sterling, euro. The combined table has no column saying what currency a value is in, so one has to be chosen and written down: the dollar, which is what these benchmarks are quoted and settled in, and which is also the only one of the three that spans the whole history. The euro entry is null before 1999, which is the currency not existing yet rather than a gap in the data -- changing `LBMA_CURRENCY` to take it would silently drop thirty years.

### Public data from Yahoo Finance

**Yahoo Finance covers instruments rather than rates, and it is the one source that is queried for a date range instead of for the whole history.** Its Bronze layer is therefore partitioned by the date of the download and appended to, not overwritten: the partitions already present are what tells the next run where to carry on from.

```python
# Download closing prices and store them in the bronze layer folder
mp.update_bronze_yahoo(spark, config_path, min_date = "2024-01-01")
```

`min_date` only applies to the first run. After that the start date is read off the partitions in the Bronze layer, and a run that has nothing left to fetch logs that the Bronze layer is current and stops.

The download itself needs the `yfinance` package, which comes with the `marketprice` extra:

```bash
pip install 'fincommons[marketprice]'
```

It is an extra and not a dependency because it is the only thing in the package that needs it, and the module imports and runs without it: the import is deferred to the one function that downloads from Yahoo, so the ECB, the New York Fed and the internal sources are unaffected. The Silver stage needs none of it and runs with the rest of the layer.

**The prices are the closes that were traded.** Yahoo Finance adjusts its history by default, for dividends and for splits, and neither adjustment gives a price anyone dealt at:

* With the dividend adjustment an Allianz share that closed at 156.00 euro on the 4th of January 2016 is shown at 91.93.
* With the split adjustment NVIDIA, which traded at about 1,200 dollars the week before its ten-for-one split in June 2024, is shown at about 120.
* Both adjustments also move whenever a new dividend or split comes in, so a history collected one download at a time would be adjusted to a different day in each partition.

The download therefore asks for the closes without the dividend adjustment (`auto_adjust=False`) and with the splits (`actions=True`), and `traded_closes` multiplies each close by the ratios of the splits that took effect after it. The query runs to the day after the run, so that a split taking effect on the day of the run is in the answer; that day's own close is not kept.

**A partition is named after the day the download was *run*, not after the last reference date it brought back.** Yahoo Finance is queried for the half-open range `[start_date, end_date)` and `end_date` is the day of the run, so `LASTDT=2024-02-01` holds prices up to the 31st of January and the next run starts on the first of February -- on the day the partition names rather than the day after it. `get_start_date` is the one place that has to know this, and `test_no_reference_date_falls_between_two_downloads` is what holds it.

#### The instruments

The instruments to be downloaded are named by a mapping table shipped with the package, `data/isin_yahoo.csv`, which pairs an ISIN with the ticker Yahoo knows it under in the columns `isin` and `yahoo`. It covers 4,808 instruments:

* **65 chosen by hand**: the Austrian listings, the significant euro area banks, a handful of the large non-financials the European indices move with, and the benchmark indices themselves.
* **4,338 shares listed on a European regulated market**, taken from FIRDS: every share (CFI `ES`) with a record in force whose issuer requested admission to a regulated market, 6,176 in September 2026.
  * The ticker comes from OpenFIGI's listings of the ISIN, spelt the way Yahoo spells that exchange, and a row is kept only where Yahoo returned prices for it over the last three months.
  * A share is named by its listing on the home market of its ISIN's country wherever Yahoo carries that market, and elsewhere by its primary listing abroad -- a Luxembourg company on Xetra, a Bermudan one in Oslo -- but never by a secondary quotation such as the Frankfurt floor, whose prices for a foreign share are thin and often stale.
  * The 51 hand-chosen shares the method also reached came out with the same ticker.
  * The 1,787 it did not reach are mostly Swedish small caps (378), whose Yahoo spelling could not be found or which Yahoo does not carry, and listings in Sofia, Nicosia, Zagreb and Valletta, exchanges Yahoo does not serve at all.
* **405 shares listed in Ho Chi Minh City** (HOSE), by the ISIN and trading code the exchange publishes, all of them served by Yahoo as `<code>.VN` in September 2026. They are quoted in dong; `EUR/VND` is among the exchange rates below. The Hanoi exchange and the history of the VN-Index are not served by Yahoo and come from VNDirect instead; see below.

Extending the coverage is a matter of adding rows to that file rather than of changing any code.

**A ticker added to a lake that already holds a history is asked for its own.** A run carries on from the partitions of its Bronze layer, which every ticker shares, so a row added after the first run would otherwise only ever be asked for the last fortnight. A ticker the layer holds no price for is therefore asked for from `YAHOO_NEW_TICKER_FIRST_DATE`, the start of 2020 -- not from 2000, because the European listings above number in the thousands -- unless its row gives a `first_date` of its own, as the Vietnamese rows do (2010, the first year Yahoo has).

**The tickers are asked for `YAHOO_BATCH_SIZE` at a time with a pause between batches**, because Yahoo does not refuse a client that asks too fast but answers it with no prices, which a run would read as tickers that did not trade.

#### Commodities

The commodities are downloaded the same way, from a second mapping table, `data/commodity_yahoo.csv`. A commodity future has no ISIN, so that table names the identifier directly in the columns `INSTRID` and `yahoo`, and the two sources write into Bronze layers of their own so that each carries on from its own partitions.

```python
# Download commodity prices and store them in the bronze layer folder
mp.update_bronze_yahoo_commodities(spark, config_path, min_date = "2024-01-01")
```

**What is collected is the front month future of each, which is not a spot price.** It is the price of the nearest contract still trading, so the series steps whenever one contract expires and the next becomes the front month, and that step is a change of contract rather than a movement in the market -- it is plainest in natural gas. It is the closest thing to a daily commodity price that can be read without a licence, which is why it is what the module collects and why the difference is written down here rather than left to be discovered.

The volatility indices (`VOL-`) and the stock indices named by ticker (`IX-`) are collected the same way, from `data/volatility_yahoo.csv` and `data/index_yahoo.csv`, by `update_bronze_yahoo_volatility` and `update_bronze_yahoo_indices`.

#### Exchange rates the ECB does not fix

The exchange rates the ECB does not fix come from Yahoo Finance as well, from a third mapping table, `data/fx_yahoo.csv`:

```python
mp.update_bronze_yahoo_fx(spark, config_path)
```

* It holds two rows today: the Bangladeshi taka, which the Dhaka prices below are quoted in, and the Vietnamese dong, which the Ho Chi Minh City listings above are; without them neither could be priced in euro at all.
* The rates are named `EUR/BDT` and `EUR/VND` and quoted as units per euro, which is how the ECB names and quotes its own, so a reader converts with either the same way; its Bronze layer, `yahoo_fx`, keeps it traceable to who published it.
* It is a market quote at Yahoo's close rather than a fixing, and both currencies are managed, so the series move in the steps their central banks allow rather than day by day.

**A rate that leaves for one day and comes back is left out.** Yahoo delivered 261.04 dong per euro on the 8th of November 2019, between 25,310 and 25,183, and every Vietnamese price converted that day came out a hundred times too large.

* The Silver stage of the exchange rates therefore drops a rate that stands more than `YAHOO_FX_ISOLATED_FACTOR` (1.5) away from both the day before and the day after while those two agree, and takes it out of the table where an earlier merge wrote it.
* A print on the last day of a download has no day after it yet, and is judged when the next download asks for that fortnight again.
* A step a currency stays at is kept, because the day after agrees with it.
* Each dropped rate is named in a warning; `update_silver_yahoo_fx(spark, config_path, full=True)` judges the whole history again.

### Public data from the Dhaka Stock Exchange

**The DSE is collected from the JSON service behind the data archive of its website**: the day-end prices of every listed security, the DSEX and DS30 indices, and the notices its listed companies file.

```python
mp.update_bronze_dse(spark, config_path)
```

* A share reaches the Silver table as `DSE-` and its trading code, `DSE-GP` for Grameenphone, and the indices as `IX-DSEX` and `IX-DS30`. The exchange publishes no ISIN beside its codes, and an ISIN written here could not be checked against anything.
* The value is the close of a day the share traded; a day it did not trade, which the archive reports with the last close carried, is left out.
* **The archive reaches back two years and no further.** The lake holds a longer history only because it keeps what it collected, so it reaches back to two years before the first run and no further, and a lake not updated for two years loses the days in between for good.
* **An answer is capped at a few hundred rows and does not page.** A trading day has some six hundred and sixty securities with the bonds and the funds, so the prices are asked for one security at a time, a year per request, and the news one day at a time. An answer that reports more rows than it carries is raised rather than written. A first run sends some two thousand requests and takes about a quarter of an hour; a daily run sends one per security.

**The closes are not adjusted for corporate actions, and on this exchange that matters.** A bonus issue is the ordinary way a Bangladeshi company pays out, and most banks issue one every year, so the closes fall by a sixth on a day nobody lost anything: City Bank closed at 22.80 taka on the 21st of May 2025 and at 19.00 on the next day it traded, which was a 12.5% stock and a 12.5% cash dividend going ex.

The exchange publishes no ex-dates, but its news carry the two notices that fix one: the dividend declaration, with the percentages and the record date, and the *resumption after record date*, with the day trading resumed once the register was drawn, which is the first day the share traded ex. `dse_events` reads them, and the Silver stage writes one row per event into a table of its own beside the prices, [`dse_corporate_actions`](#the-dse-corporate-actions-dse_corporate_actions):

```python
events = mp.connect_to_dse_corporate_actions(spark, config_path)
```

How the factor is computed:

* a bonus issue of b per cent has the factor 1/(1 + b/100);
* a cash dividend of d taka against the close P before it, 1 - d/P, taking the face value as the ten taka the Commission set every share to in 2011;
* a rights issue of n shares for m at a price S, the theoretical ex-rights price (mP + nS)/((m + n)P), and none where S is above P.

Multiplying by the bonus and rights factors gives the price history of a holding, and by all three its total return.

Measured over the two years the lake first collected, 540 events of 481 securities: on the ex-dates of the 86 bonus and rights issues the median close moved by -6.4% as traded and by -0.1% once adjusted, and 27 of them moved by more than 10% as traded against 5 adjusted. The five are small, thinly traded shares, and the reader does not claim more than that: a notice worded in a way it does not know is an event it misses, and the close then shows the fall it would have explained.

The table is rebuilt whole every run: an event can only be placed once the notices on both sides of its record date have arrived, so one that was pending in one run is complete in a later one without any single download bringing it.

### Public data from VNDirect: the Hanoi Stock Exchange

**The shares listed on the Hanoi Stock Exchange (HNX) and the Vietnamese indices come from the public market-data service of VNDirect, a Vietnamese broker.** It is a broker's service rather than the exchange's own, and it is used because the exchange publishes nothing comparable that can be read without a browser, while Yahoo carries neither.

```python
mp.update_bronze_vndirect(spark, config_path)
```

* A Hanoi share reaches the Silver table under the ISIN VNDirect's listing gives beside its trading code -- `VN000000SHS3` for Saigon-Hanoi Securities -- so it is named the way the Ho Chi Minh City shares from Yahoo are.
* The four indices are `IX-HNX`, `IX-HNX30`, `IX-VNINDEX` and `IX-VN30`.
* The value is the close of a day the share traded, in dong: **VNDirect quotes prices in thousands of dong** (Vinamilk's 59,800 is 59.8 there), so the close is multiplied by a thousand, which puts a Hanoi close and a Ho Chi Minh City close on one scale. A day with no volume is left out.
* The Bronze layer keeps the close that traded and the one VNDirect adjusts for corporate actions, `AD_CLOSE`; only the traded close reaches the table, as for every other source, because the adjusted history is recomputed whenever a new action goes ex and differs from one download to the next.
* The listings are collected whole every run, since they pair each code with its ISIN, and a code that no listing has ever named is left out of the table with a warning.
* The history reaches back to 2013 for most shares and to August 2017 for the indices.
* An answer is paged and says how many rows it holds; a page is asked for five thousand rows at a time, which is the whole history of a share, and an answer that ends short is raised rather than written.
* A run carries on from the last partition less a week, and a share or an index the layer has never held is asked for its whole history, also on a day the rest have nothing to fetch.

### Day-ahead electricity prices

The `EL-` series are the day-ahead electricity prices of twenty bidding zones (`ELECTRICITY_ZONES`), collected from energy-charts.info by `update_bronze_electricity` and merged by `update_silver_electricity`. They are collected incrementally, with a revision window of a week, as described under [What "only new data" means](#what-only-new-data-means-by-source). The data is published under CC BY 4.0, with attribution to Bundesnetzagentur | SMARD.de required (`ELECTRICITY_LICENCE`).

### Proprietary data

**The module picks up proprietary quotes from one file, but does not download them.** Due to the heterogeneity of the data source, the module does not provide any functionality to download data from proprietary sources. It picks such data up from

```
{path to Bronze Layer of marketpricedata as specified in config_path}/internal/marketpricedata_internal.csv
```

if that file exists, and expects a comma-separated file with a header row and the following three columns, in this order. They are the three columns of the Silver table, under the same names.

| Column | Type | Content |
| --- | --- | --- |
| `ref_date` | date | The reference date of the quote, as `YYYY-MM-DD`. |
| `instr_id` | string | What the value is a price of. See the [identifier convention](#what-an-identifier-looks-like), and pick identifiers that no public source already uses. |
| `value` | double | The price or rate. |

* The columns are read by position, not by the names in the header, so a file still headed `REFDT,INSTRID,VALUE`, as the file was before the column names of the package were put into snake case, is read the same way.
* This is the form the proprietary sources of `exchangerate` and `entitymasterdata` take as well: one file, under a name the module knows, placed in the Bronze layer by whoever maintains the quotes. Nothing in the module writes into that directory.
* The file is read with the schema declared in `common.py` (`internal_schema`) rather than with an inferred one, which is why the columns have to be the three above and in that order: a reader that is given a schema maps it onto the columns of the file by position and does not go by the header. The types are therefore the ones the Silver table holds, and a reference date that does not parse arrives as a missing value rather than as text.
* **A file with such a row is refused whole**: the stage raises, naming the first rows that did not read, the run sets the source aside, and the quotes collected before stay in the table as they were. A quote that silently went missing from the one source that is the last word on its quotes would be worse than a run that says the file needs fixing.

Nothing sets the proprietary quotes apart from the public ones once they are in the table, and a delivery is merged like any other source: where the file carries an instrument and a reference date that a public source has already delivered, the proprietary quote replaces it, as long as the file is picked up after that source. `update_silver_layer` runs it last for that reason.

### From Bronze to Silver layer

**Each source has a Silver update function of its own, all with the same signature, and each one does nothing (and says so) if its input is not there**, so a source that has never been downloaded does not stop the others.

```python
mp.update_silver_ecb(spark, config_path)           # €STR, the key ECB rates, the US and euro area monthly series
mp.update_silver_nyfed(spark, config_path)         # SOFR
mp.update_silver_boe(spark, config_path)           # SONIA
mp.update_silver_exchangerate(spark, config_path)  # currency pairs
mp.update_silver_lbma(spark, config_path)          # gold and silver
mp.update_silver_yahoo_commodities(spark, config_path)  # other commodities
mp.update_silver_yahoo(spark, config_path)         # listed instruments
mp.update_silver_yahoo_fx(spark, config_path)      # exchange rates the ECB does not fix
mp.update_silver_yahoo_volatility(spark, config_path)  # VOL- indices
mp.update_silver_yahoo_indices(spark, config_path) # IX- stock indices
mp.update_silver_dse(spark, config_path)           # Dhaka Stock Exchange
mp.update_silver_dse_corporate_actions(spark, config_path)  # its corporate actions
mp.update_silver_vndirect(spark, config_path)      # Hanoi shares and Vietnamese indices
mp.update_silver_electricity(spark, config_path)   # day-ahead electricity prices
mp.update_silver_internal(spark, config_path)      # proprietary data
```

"Says so" means:

* a `WARNING` from `read_bronze` where the Bronze layer of the source holds no data set at all -- the run did less than a reader would assume;
* an `INFO` where an incremental source has simply received nothing since the last merge, or where the lake keeps no proprietary file.

Only Spark's refusal to analyse a path that is missing or empty counts as "not there": any other failure to read is raised, so that the run sets the source aside with its traceback rather than reporting it as empty.

**The exchange rates are not downloaded again here.** They are read from the Silver table of `exchangerate`, which is the one place they are maintained, so that module has to be up to date first. Each rate enters as the price of a currency pair named `BASE/QUOTE`.

### Zero curves and discount factors

**`curves` turns the rates in the table into a risk-free zero curve per currency for one reference date.** It is imported by name (`from fincommons.marketpricedata import curves`) rather than through the star import of the package, because names such as `forward_rate` mean something different elsewhere.

* **Pillars.** The short end is the overnight rate of each currency (`ON-ESTR` for EUR, `ON-SOFR` for USD, `ON-SONA` for GBP), and the longer maturities are overnight indexed swap quotes under `OIS-<index>-<tenor>`, for instance `OIS-ESTR-1Y`. Every pillar takes the latest quote at or before the reference date, and the log says when that is older than the date.
* **Quote conventions.** A quote shorter than a year is read as a simple money market rate on actual/360, a longer one as annually compounded.
* **Interpolation.** Between the pillars the curve is log-linear in the discount factors, so the forward rate is constant on each segment, and beyond the last pillar the last forward rate carries on.
* **No tenor pillar is delivered into the table yet, so a curve built today is flat at the overnight rate.** The `KR-` policy rates are not read: a rate a central bank decided on is not a quote for a maturity. The swap quotes are picked up under the identifiers above as soon as a source delivers them, without a change to the module.
* A payment in a currency without a curve keeps its row and gets no discount factor, so a caller can report it as not covered rather than lose it.
* `parallel_shift` makes one copy of the curves per assumed shift of the continuously compounded zero rate, which `add_discount_factor` then evaluates in the same join with `keys=["shock"]`, and `forward_rate` gives the simple forward rate of an accrual period from its two discount factors.
* The curves carry no credit or funding spread.

`build_curves` runs the pieces in this order, which can also be called one by one:

```python
pillars = curves.read_pillars(spark, prices, "2025-06-30")      # quotes -> {ccy: {years: rate}}
segments = curves.segment_frame(spark, curves.build_segments(pillars))
```

## Reference

### Configuration

The paths of the data lake are read from the configuration file, which needs a `marketpricedata` section:

| Key | Holds | May be a catalogue name? |
| --- | --- | --- |
| `bronze` | the Bronze layer: one directory per publisher, and `internal/` for proprietary quotes | no |
| `silver` | the Silver layer: the tables `marketpricedata` and `dse_corporate_actions`, and `_watermarks` | yes, as `table` plus `path` |

The Silver layer may name a schema in a catalogue instead of a directory, which is what puts the combined table within reach of Unity Catalog's grants, row filters and lineage. It then needs a `path` beside the `table`, because the marks that record how far each source has been merged are files and a schema is not a directory to write them to:

```yaml
marketpricedata:
    bronze: /Volumes/fincommons/marketpricedata/lake/bronze
    silver:
        table: fincommons.marketpricedata
        path: /Volumes/fincommons/marketpricedata/lake/silver
```

The combined table is then `fincommons.marketpricedata.marketpricedata`, and `create_directory_tree` leaves the layer alone, since a schema is provisioned outside the pipeline.

`create_directory_tree(config_path)` creates the folders of the module. Directories that already exist are left alone and missing parents are created along the way, so the call can be repeated. It is meant for a local data lake; on Databricks the volume and the directories in it are provisioned outside the pipeline.

### Public functions

All are called as `mp.<function>` after `from fincommons import marketpricedata as mp`, except the curve functions, which are `curves.<function>`.

**Run everything**

| Function | What it does |
| --- | --- |
| `update_marketpricedata(spark, config_path, min_date="2000-01-01", full=False)` | Bring the table up to date, from the sources to the Silver table; returns the outcome. |
| `update_bronze_layer(spark, config_path)` | Collect every source but Yahoo Finance into the Bronze layer. |
| `update_bronze_yahoo_layer(spark, config_path, min_date="2000-01-01")` | Collect the five Yahoo Finance sources into the Bronze layer. |
| `update_silver_layer(spark, config_path, full=False)` | Merge every source into the Silver table, in one merge. |
| `create_directory_tree(config_path)` | Create the folders of the data lake. |
| `migrate_folder_names(config_path)` | Rename the hyphenated folders and files of an older lake to snake_case. |

**Read**

| Function | What it does |
| --- | --- |
| `connect_to_marketpricedata(spark, config_path)` | Open the combined Silver table, by path or by catalogue name. |
| `connect_to_dse_corporate_actions(spark, config_path)` | Open the DSE corporate actions. |
| `describe_instrument(identifier, config_path=None)` | Describe one identifier; returns an `InstrumentDescription`. |

**Bronze stages, per publisher** (all `(spark, config_path)`; those marked * also take `min_date`)

| Function | Collects |
| --- | --- |
| `update_bronze_ecb`, `update_bronze_ecb_estr`, `update_bronze_ecb_fm` | the ECB `EST` and `FM` flows |
| `update_bronze_nyfed`, `update_bronze_nyfed_sofr` | SOFR |
| `update_bronze_boe`, `update_bronze_boe_sonia` | SONIA |
| `update_bronze_lbma`, `update_bronze_lbma_gold`, `update_bronze_lbma_silver` | the LBMA Gold and Silver Prices |
| `update_bronze_yahoo`* | listed instruments |
| `update_bronze_yahoo_commodities`* | commodity front month futures |
| `update_bronze_yahoo_fx`* | exchange rates the ECB does not fix |
| `update_bronze_yahoo_volatility`* | `VOL-` volatility indices |
| `update_bronze_yahoo_indices`* | `IX-` stock indices named by ticker |
| `update_bronze_dse`* | DSE day-end prices, market summary and news |
| `update_bronze_vndirect`* | Hanoi listings, prices and the Vietnamese indices |
| `update_bronze_electricity`* | day-ahead electricity prices (`min_date` defaults to None, i.e. carry on) |

**Silver stages, per publisher** (all `(spark, config_path)`; those marked † also take `full=False`)

| Function | Merges |
| --- | --- |
| `update_silver_ecb` | runs `update_silver_ecb_estr`, `update_silver_ecb_fm`, `update_silver_ecb_fm_monthly_us`, `update_silver_ecb_fm_monthly_ea` |
| `update_silver_nyfed`, `update_silver_nyfed_sofr` | SOFR |
| `update_silver_boe`, `update_silver_boe_sonia` | SONIA |
| `update_silver_lbma`, `update_silver_lbma_gold`, `update_silver_lbma_silver` | gold and silver |
| `update_silver_exchangerate` | the currency pairs of `exchangerate` |
| `update_silver_yahoo`†, `update_silver_yahoo_commodities`†, `update_silver_yahoo_fx`†, `update_silver_yahoo_volatility`†, `update_silver_yahoo_indices`† | the Yahoo Finance sources |
| `update_silver_dse`†, `update_silver_dse_corporate_actions` | DSE prices and indices; the corporate actions table, rebuilt whole |
| `update_silver_vndirect`† | Hanoi shares and Vietnamese indices |
| `update_silver_electricity`† | day-ahead electricity prices |
| `update_silver_internal` | the proprietary file |

**Curves** (`from fincommons.marketpricedata import curves`)

| Function | What it does |
| --- | --- |
| `build_curves(spark, prices, refdt, currencies=None)` | The whole construction, from the price table to a frame of curve segments. |
| `add_discount_factor(df, segments, currency_col, years_col, out_col, keys=None)` | Discount factors for the maturities of a frame (left outer join). |
| `parallel_shift(segments, shifts)` | One copy of every curve per assumed parallel shift. |
| `forward_rate(df, start_df_col, end_df_col, accrual_col, out_col)` | The simple forward rate of an accrual period. |
| `read_pillars`, `build_segments`, `segment_frame` | The steps `build_curves` runs. |
| `pillar_identifier(currency, tenor)`, `tenor_in_years(tenor)`, `discount_of(rate, years)` | Helpers: the `OIS-` identifier of a pillar, a tenor in years, the discount factor of a quote. |

**Helpers** used by the stages: `standard_upsert`, `complete_rows`, `collected_upserts`, `read_bronze`, `traded_closes`, `get_start_date`, `isolated_prints`, the `load_list_of_yahoo_*` readers of the mapping tables, and the path helpers below.

### Constants worth knowing

| Constant | Value | Meaning |
| --- | --- | --- |
| `YAHOO_FIRST_DATE` | `2000-01-01` | default `min_date` of a new lake |
| `YAHOO_NEW_TICKER_FIRST_DATE` | `2020-01-01` | where a ticker new to the lake starts, unless its row says otherwise |
| `YAHOO_REVISIT_DAYS` | 14 | how far back a Yahoo run asks again |
| `YAHOO_BATCH_SIZE` | 200 | tickers per Yahoo request batch |
| `YAHOO_FX_ISOLATED_FACTOR` | 1.5 | how far a one-day exchange rate print may stand from its neighbours |
| `ELECTRICITY_REVISION_DAYS` | 7 | the electricity revision window |
| `LBMA_CURRENCY` | 0 | which of the LBMA's three currencies is taken (0 = US dollar) |
| `HTTP_TIMEOUT` | 120 | timeout of the shared download, in seconds |
| `USER_AGENT` | `fincommons` | user agent of the shared download |
| `INTERNAL_FILE` | `marketpricedata_internal.csv` | name of the proprietary file |

### The layout of the data lake

The helpers in `common.py` are the only place these paths are spelled out, and the rest of the module reaches the data lake through them rather than through a path written into the code.

| Path helper | Below `bronze:` or `silver:` | Holds |
| --- | --- | --- |
| `path_bronze_ecb_estr` | `ecb/estr` | the ECB `EST` flow, as published |
| `path_bronze_ecb_fm` | `ecb/fm` | the ECB `FM` flow, as published |
| `path_bronze_nyfed_sofr` | `nyfed/sofr` | SOFR, with its percentiles, volume and revision flag |
| `path_bronze_boe_sonia` | `boe/sonia` | SONIA, as a date and a value |
| `path_bronze_lbma_gold` | `lbma/gold` | the LBMA Gold Price, as a date and a value |
| `path_bronze_lbma_silver` | `lbma/silver` | the LBMA Silver Price, as a date and a value |
| `path_bronze_yahoo_marketpricedata` | `yahoo` | closing prices, partitioned by `LASTDT` |
| `path_bronze_yahoo_commodities` | `yahoo_commodities` | commodity futures, partitioned by `LASTDT` |
| `path_bronze_yahoo_fx` | `yahoo_fx` | the exchange rates the ECB does not fix, partitioned by `LASTDT` |
| `path_bronze_yahoo_volatility` | `yahoo_volatility` | the `VOL-` volatility indices, partitioned by `LASTDT` |
| `path_bronze_yahoo_indices` | `yahoo_indices` | the `IX-` stock indices named by ticker, partitioned by `LASTDT` |
| `path_bronze_electricity_dayahead` | `electricity/dayahead` | the `EL-` day-ahead electricity prices per bidding zone, rewritten whole by every run |
| `path_bronze_dse_dayend` | `dse/dayend` | DSE day-end prices of every security, partitioned by `LASTDT` |
| `path_bronze_dse_summary` | `dse/summary` | the DSEX and DS30 and the market's turnover, partitioned by `LASTDT` |
| `path_bronze_dse_news` | `dse/news` | the notices of DSE-listed companies, partitioned by `LASTDT` |
| `path_bronze_vndirect_prices` | `vndirect/prices` | Hanoi share prices from VNDirect, in thousands of dong, partitioned by `LASTDT` |
| `path_bronze_vndirect_indices` | `vndirect/indices` | the HNX-Index, HNX30, VN-Index and VN30, partitioned by `LASTDT` |
| `path_bronze_vndirect_listings` | `vndirect/listings` | the Hanoi listings, code and ISIN, partitioned by `LASTDT` |
| `path_bronze_internal_marketpricedata` | `internal` | `marketpricedata_internal.csv`, the proprietary quotes, if any |
| `ref_silver_marketpricedata` | `marketpricedata` | the combined table, by path or by catalogue name |
| `ref_silver_dse_corporate_actions` | `dse_corporate_actions` | the DSE corporate actions, one row per event |
| `path_silver_watermarks` | `_watermarks` | how far each incremental source has been merged |

### Files shipped with the package

In `src/fincommons/data/`:

| File | Holds |
| --- | --- |
| `isin_yahoo.csv` | ISIN to Yahoo ticker (`isin`, `yahoo`, and `first_date`, `name`), 4,808 instruments |
| `commodity_yahoo.csv` | commodity identifier to Yahoo ticker (`INSTRID`, `yahoo`) |
| `fx_yahoo.csv` | the exchange rates the ECB does not fix, to Yahoo ticker (`INSTRID`, `yahoo`, `first_date`) |
| `volatility_yahoo.csv`, `index_yahoo.csv` | `VOL-` and `IX-` identifiers to Yahoo ticker (`INSTRID`, `yahoo`) |
| `marketpricedata_ecb_estr.json`, `marketpricedata_ecb_fm.json` | the schemas the ECB flows are parsed with |

### Adding or renewing an ECB schema

A schema file is the plain JSON form of a Spark `StructType`, as `StructType.fromJson` reads it. Where the API starts returning a column that does not fit, derive it again from a downloaded answer rather than editing the file by hand:

```python
import json

df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
with open("src/fincommons/data/marketpricedata_ecb_estr.json", "w") as f:
    json.dump(df.schema.jsonValue(), f, indent=4)
```

Check the inferred types by hand afterwards. Inference reads the types off whichever observations happen to be in the file, so a column that is empty in the sample comes out as a string, and a code that happens to be numeric throughout comes out as a number.

### Further reading and licences

* The handbook chapter: [`docs/handbook/01-marketpricedata.tex`](../../../docs/handbook/01-marketpricedata.tex).
* The electricity prices are published under CC BY 4.0 from Bundesnetzagentur | SMARD.de, via energy-charts.info; `describe_instrument` repeats the attribution for every `EL-` identifier.

### Tests

The module is covered by the tests in `tests/test_marketpricedata.py`, which run against the local Spark session built by `tests/conftest.py`, and by `tests/test_marketpricedata_pipeline.py`, `test_marketpricedata_curves.py`, `test_marketpricedata_describe.py`, `test_marketpricedata_dse.py`, `test_marketpricedata_vndirect.py` and `test_marketpricedata_yahoo_new_tickers.py`. Nothing in them reaches the ECB, the New York Fed, the Bank of England, the LBMA or Yahoo: the answers of the APIs are written inline and handed to the download in place of a response, so what is asserted is what the module does with an answer rather than what those sources happen to be publishing today.

| Group | Covers |
| --- | --- |
| the data lake | creating the directory tree, including below a directory that does not exist yet, repeating the call, and every path coming from the configuration file |
| the download | that no CSV file is left in the data lake, that the header row is skipped, that the response is closed when the download fails, that the shared download sends a user agent and a timeout and refuses an empty answer, that an empty answer or an error page with status 200 leaves the Bronze layer of the ECB, the New York Fed, the Bank of England and the LBMA alone and fails naming the source, the typing of a New York Fed answer including a day it has no rate for, that its Bronze layer keeps the percentiles, the volume and the revision flag, that the whole history is asked for by date, and, for the Bank of England, that the request carries a user agent, that a refusal leaves the Bronze layer alone and that the Bank's own date format is parsed |
| the Silver layer | picking €STR out of the flow and leaving the other series, dating a monthly series to the end of its month, its identifier saying that it is monthly, the three FM stages leaving each other's series alone and claiming no identifier twice, the euro area block being filtered by area and leaving out the monthly copies of daily series, picking the three key ECB rates out of the FM flow and leaving its other frequencies, its changes and its regime-specific variants, casting a reference date that flow types as a string, the name SOFR arrives under, a rate becoming a currency pair, and a source that has delivered nothing, which leaves the table alone and says so in a warning |
| the proprietary source | a delivery being picked up, the types it arrives in, the header row not becoming a quote, and a data lake without such a file |
| the upsert | a day delivered again replacing its row, a new day being added, two instruments on one day staying two rows, and columns beyond the three being dropped |
| the pipeline | a source that fails being set aside while the others carry on; and, in `tests/test_marketpricedata_pipeline.py` and without Spark, a stage counting and reporting the source it set aside, the run adding up its stages, the outcome reporting a failure a scheduler can turn into an exit code where a source was set aside and none where every source was updated, and no module of the library calling `print` |
| commodities | that the LBMA price is taken in dollars, that a price predating the euro is still read, that the LBMA request carries a user agent, the two metals reaching the table under their names, the commodity mapping table and its identifiers, a commodity keeping the identifier it is named by, the most recent delivery of a commodity day winning, and the commodities carrying on from their own partitions |
| Yahoo Finance | where a run starts from and that it meets the previous run without a gap, the absence of `yfinance` being reported in terms of the download, the mapping table travelling with the package, a price arriving under its ISIN, the day of the download being dropped, the most recent delivery of a day winning, and the source being part of the Silver layer |

Some of these are worth naming, because they are pinned to rules that are easy to undo by accident rather than to behaviour that would obviously break:

* `test_the_header_row_of_an_ecb_answer_is_not_read_as_data` holds the `header` option on a reader that is also given a schema. Without it the header is parsed as an observation with its typed fields nulled. The €STR filter happens to discard that row, so the two Silver tests pass either way and only an assertion on the Bronze layer catches it -- which is why this test sits where it does.
* `test_the_other_series_of_the_est_flow_are_left_out` holds all three parts of the €STR filter at once. The flow carries the number of reporting banks and the traded volume beside the rate, and dropping any one part of the filter lets several of them through under the name of the rate.
* `test_a_day_that_is_delivered_again_replaces_the_row` and `test_the_same_day_of_two_instruments_is_two_rows` hold the two halves of the merge condition. The sources are downloaded whole on every run, so the first is what makes the pipeline re-runnable; the second is what keeps the second source of a day from overwriting the first.
* `test_every_path_is_read_from_the_configuration_file` holds the data lake being addressed through the configuration alone, with no root left written into the code.
* `test_only_the_three_columns_reach_the_table` holds the projection in `standard_upsert`. The sources do not agree on what they carry beyond the three columns, and a merge only works where the two sides match. It calls the upsert directly rather than through a source, because every source of the module hands it the three columns and none of them would notice the projection going.
* `test_the_header_row_of_a_proprietary_file_is_not_read_as_a_quote` holds the `header` option on the reader of the proprietary file, which is given a schema and does not skip the header by itself. Unlike the €STR flow, nothing filters that row out again: it would reach the table as a quote with no date and no value.
* `test_the_most_recent_download_of_a_day_is_the_one_that_is_kept` holds the reduction of the Yahoo Bronze layer to one row per instrument and reference date. That layer is partitioned by the day of the download and appended to, so a reference date delivered by two downloads is held twice, and a merge whose source carries the same key twice fails outright rather than choosing between them -- which would take the whole Silver run with it and not only that source.

Run the suite from the root of the repository:

```bash
pytest tests/test_marketpricedata.py
```

## Troubleshooting

* **A source is listed in `failed_sources`** → it was unreachable or refused its answer; the traceback is at `ERROR` in the log → nothing to repair, the next run collects what was missed. Configure logging with `fincommons.log.configure()` to see it.
* **A series stopped moving although the runs report success** → a source delivered nothing since the last merge, which the stage logs at `INFO` (or at `WARNING` where its Bronze layer holds no data set at all) → check the latest `ref_date` of the `instr_id` and the log of that stage.
* **The Yahoo sources are missing and `update_bronze_layer` did not complain** → the Yahoo downloads are run by `update_bronze_yahoo_layer`, not by `update_bronze_layer`, and need `yfinance` → install `fincommons[marketprice]` and use `update_marketpricedata` or call `update_bronze_yahoo_layer`.
* **The merge fails because the session does not know Delta** → the Silver table is maintained by upsert, which only Delta does → build the session with the Delta extensions as in [Quick start](#quick-start).
* **No currency pairs `EUR/XXX` from the ECB in the table** → they are read from the Silver table of `exchangerate` → bring that module up to date first.
* **A curve is flat** → no `OIS-` tenor pillar is in the table yet, so the curve is the overnight rate alone → expected until a source delivers swap quotes.
* **A payment has no discount factor** → there is no curve for its currency (only EUR, USD and GBP have one) or no quote at or before the reference date → report it as not covered.
* **A daily join with an `-M` series finds a value on one day a month** → the `-M` series are monthly, dated to the last day of the month → carry the last quote forward.
* **The proprietary source is set aside** → a row of `marketpricedata_internal.csv` did not parse (the stage names the first ones) and the file is refused whole → fix the file; the quotes collected before stay in the table.
* **Old rows remain after a source changed its identifiers** → the upsert replaces a row but never removes one → delete the rows under the old identifiers separately, then rebuild with `full = True`.
* **A Yahoo ticker added to `isin_yahoo.csv` has only a short history** → it is asked for from `YAHOO_NEW_TICKER_FIRST_DATE` (2020) unless its row gives a `first_date` → add a `first_date` to the row.
* **A Dhaka share falls by a sixth overnight** → a bonus issue went ex; the closes are traded, not adjusted → multiply by the factors in `dse_corporate_actions`.
* **A gap of more than two years in the DSE history** → the archive reaches back two years and no further → keep the lake updated at least that often; the days in between cannot be recovered.
