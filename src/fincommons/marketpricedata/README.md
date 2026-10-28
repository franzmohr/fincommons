# Market price data

## Introduction

Module `marketpricedata` implements the creation of a data set, which contains price data for various financial instruments.

A supervisory data set records an amount, a notional or a price in the terms that were agreed. Comparing those figures across reports, revaluing a position, or discounting a cash flow all mean holding the market data they are to be measured against, and that data has to come from somewhere that is dated, traceable and the same for everyone who uses it. This module collects it from the public sources and reduces every one of them to the same three columns, so that a price is looked up in one place and in one shape regardless of who published it.

## Setting up

The paths of the data lake are read from the configuration file, which needs a `marketpricedata` section with the keys `bronze` and `silver`:

```yaml
marketpricedata:
    bronze: /data/marketpricedata/bronze
    silver: /data/marketpricedata/silver
```

The Silver layer may name a schema in a catalogue instead of a directory, which is what puts the combined table within reach of Unity Catalog's grants, row filters and lineage. It then needs a `path` beside the `table`, because the marks that record how far each source has been merged are files and a schema is not a directory to write them to:

```yaml
marketpricedata:
    bronze: /Volumes/fincommons/marketpricedata/lake/bronze
    silver:
        table: fincommons.marketpricedata
        path: /Volumes/fincommons/marketpricedata/lake/silver
```

The combined table is then `fincommons.marketpricedata.marketpricedata`, and `create_directory_tree` leaves the layer alone, since a schema is provisioned outside the pipeline.



```python
# Set path to configuration file, where paths are specified
config_path = "paths.yaml"

# Load the module
from fincommons import marketpricedata as mp

# Create folders for market price data, unless they already exist
mp.create_directory_tree(config_path = config_path)
```

Directories that already exist are left alone and missing parents are created along the way, so the call can be repeated. It is meant for a local data lake; on Databricks the volume and the directories in it are provisioned outside the pipeline.

### Start the Spark session

Unlike `exchangerate` and most of the other modules, this one does not offer a choice of storage format. The Silver table is maintained by upsert rather than by overwrite, which is something only Delta does, so **the session has to be built with the Delta extensions**. Off-cluster that needs the `delta-spark` package, which comes with the `local` extra; on Databricks Delta is built in and the extra configuration is unnecessary.


```python
# Start Spark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder \
                    .appName("fincommons") \
                    .config("spark.cores.max", "4") \
                    .config("spark.driver.memory", "15g") \
                    .config('spark.sql.parquet.int96RebaseModeInWrite', 'CORRECTED') \
                    .config('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED') \
                    .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
                    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
                    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

## Keeping the table up to date

Everything below this section is the module taken apart: one source at a time, so that a single publisher can be collected, inspected or re-collected on its own. A scheduled run wants none of that. It wants one call that brings the table up to date and says what it could not reach, and that call is `update_marketpricedata`:


```python
mp.update_marketpricedata(spark, config_path)
```

The call writes nothing to standard output. It records what it does in the log of `fincommons.log` — each source as it is collected and merged, at `INFO`, a source that failed with its traceback at `ERROR`, and a closing `WARNING` that counts the sources set aside — so call `fincommons.log.configure()` first to see it. It returns the outcome of the run, whose `failed` entry is the number of sources set aside and `failed_sources` their names.


It creates the directory tree if it is missing, collects every source into the Bronze layer, and folds the whole Bronze layer into the Silver table. It takes no argument saying what to collect, because what a run has to collect is read from what the data lake already holds: the same call is correct on the first morning, when it collects the whole history, and on every morning after, when it collects a day. From a job scheduler it is easier to run the module than to write a script around it:

```
python -m fincommons.marketpricedata --config /path/to/paths.yaml --log-level INFO
```

`--min-date` decides where the history of a **new** data lake begins and defaults to the 1st of January 2000. It applies only to a source whose Bronze layer is still empty, so passing it to a lake that already holds data changes nothing; collecting a history again is done by calling that one source with a `min_date` of its own.

### What "only new data" means, by source

The sources are not incremental in the same way, and which kind each one is matters when reading how long a run takes and what a run can repair.

**Collected incrementally.** The Yahoo Finance sources and the electricity prices ask only for the days they are missing. Their histories are large — a decade of quarter-hours for twenty bidding zones is several million delivery periods — so fetching them again every morning would *be* the cost of the job. Yahoo carries on from the partitions its Bronze layer is written in, less a fortnight; electricity carries on from the last date it holds, less a week.

Yahoo's fortnight is `YAHOO_REVISIT_DAYS`, and it is there for a different reason from electricity's week. Yahoo does not fail a download it cannot answer in full: when it rate limits, it leaves the tickers it refused out and returns the rest. A run that carried on exactly where the last one stopped would never go back for them, so one bad morning would leave a permanent gap in those series. Asking for the last fortnight again closes the gap on the next run that succeeds. A ticker that comes back with nothing over a fortnight is named in a warning, and a download with nothing for any ticker is raised, not written.

That week is `ELECTRICITY_REVISION_DAYS`, and it is there because "new" cannot mean "after the last date held" alone: a delivery day can be republished, and a run that began the morning after the last date would never see the correction. Within that window the service is what the table says, so a day it revises is revised here and a day it withdraws is withdrawn here. Outside it, a day already collected is never touched.

**Collected in full, every run.** The ECB, the New York Fed, the Bank of England and the LBMA are downloaded whole each time, and that is deliberate rather than unfinished. Each publishes its whole series as one file and offers no way to ask for part of it, and that file is also how a **revision to an older value** arrives: SOFR carries a revision marker, and a download that only ever asked for days after the last one held would never see a figure corrected behind it. Because the Silver stage merges rather than appends, collecting the file again replaces the rows it has seen before, so the whole download is what keeps the table *current* rather than merely longer.

They are not all small, and it is worth knowing which is which before reading a run's timings: the ECB `FM` flow is about 94,000 rows and the €STR file 26,000, the two LBMA benchmarks about 15,000 each, SONIA 7,500 and SOFR 2,100.

### The merge is incremental on the same terms

Collecting less is only half of it. The Silver stage merges what was collected, and a stage that read its whole Bronze layer every morning would do the work of the entire history to fold in a day, which for the Yahoo stages is roughly a million rows.

They therefore read only the downloads that have not been merged yet. A Yahoo Bronze layer is partitioned by `LASTDT`, the day the download ran, and is appended to rather than rewritten, so a partition never changes once it is written: merging one a second time writes values that are already in the table. The electricity stage is the same idea from the other end — its Bronze layer *is* rewritten whole, so it cannot say which rows are new, but the download can, because it leaves every day before its window exactly as it was.

How far each source has been merged is recorded in one small file per source under `_watermarks` beside the Silver table. It is kept there rather than in the table because a row in the table carries the reference date of a price and not the day it was fetched, so the table cannot be asked where the last run stopped. **The mark is written after the merge has succeeded**, never before: a mark written first would claim a delivery that never arrived, and the next run would skip it for good. A run that fails therefore folds the same deliveries in again, which costs a little and is the safe way round.

Passing `full = True` — or `--full` on the command line — ignores the marks and merges every source from the beginning. A scheduled run never needs it. It is how the table is rebuilt from a Bronze layer that is still intact, which is worth doing after the identifiers a source writes have changed, since the rows written under the old ones are not removed by a merge and have to be dealt with separately.

A source without a mark, or with one that cannot be read, is merged from the beginning, which is what puts the table into the state the marks then claim.

### One merge for the run, not one per source

**A merge costs what it costs whether it carries a million rows or a hundred.** It rewrites the files it touches in the whole table, and against a Silver table of about a million rows that fixed cost dominates everything else. Measured here with each source merged on its own: the New York Fed stage merged 2,100 rows in 15 seconds, and the electricity stage, merging only its revision window, merged 160 rows in 15 seconds as well. Eleven sources merging one after another therefore cost eleven times that, however little any of them had to say.

Two things follow, and the module does both. **Skipping a merge saves time; making a merge smaller does not** — which is why the incremental Yahoo stages, which merge nothing at all when there is nothing new, cost half a second each instead of fifteen. And the remaining cost is a count of merges rather than a volume of rows, so `update_silver_layer` collects every source and merges them **once**, through `collected_upserts`.

The effect on a run with nothing new to fold in, against the same table:

| | separately, whole | separately, incremental | one merge |
|---|---|---|---|
| Silver phase | ~197s | 151s | **25s** |
| merges | 11 | 10 | **1** |

The single merge carried 402,000 rows in 20 seconds — more rows than any of the eleven, in less time than two of them.

**The single merge still decides which source wins.** A reference date and an instrument delivered by two sources can only be one row, and a merge handed that pair twice fails outright rather than choosing, so the collected rows are reduced to the last delivery of each pair. *Last* means latest collected, which is the same outcome as merging the sources one after another, each overwriting what the one before it had written. That is why `update_silver_layer` runs the proprietary source last.

The marks are held back in the same way and written only once the merge has succeeded, for the reason they are written after a merge at all. A source that fails inside the batch has contributed nothing, because a stage puts its rows aside as its last act, so the run merges what the others delivered and carries on.

### What a successful run does not promise

A publisher that is unreachable is set aside with its traceback and the run carries on, which is the point of the design: one source being down should not cost a morning's update of the other ten. **It is counted rather than only logged.** Each stage records the sources it set aside in its outcome — `failed`, the number, and `failed_sources`, their names as `layer/SOURCE` — and ends with a warning that counts them, and `update_marketpricedata` adds the three stages up and returns the total. The functions return rather than raise for it, because a caller in a notebook should still get what the other sources delivered; the command line turns the count into its exit code, which is 0 only where every source was updated and 1 where any was set aside or the run did not complete. A scheduler watching the exit code therefore sees a partial run as a failed job, and the log on standard error says which sources it was. The coverage table of the report — or the `last` column of the Silver table — remains the answer to what actually arrived.

## The shape of the Silver layer

Every source ends in one table, stored as Delta at

```
{path to Silver Layer of marketpricedata as specified in config_path}/marketpricedata
```

with three columns:

| Column | Type | Content |
| --- | --- | --- |
| `REFDT` | date | The reference date the quote belongs to. |
| `INSTRID` | string | What the value is a price of, see below. |
| `VALUE` | double | The price or rate itself, in the unit the source publishes it in. |

A row is identified by `INSTRID` and `REFDT` together. A source that delivers a date again replaces the row it delivered before rather than adding a second one, which is what `standard_upsert` does and what makes the pipeline safe to re-run on a day that has already been loaded.

**None of the three columns is ever empty.** A row without an instrument or a reference date would match nothing in the table and be inserted again on every run, because a null equals nothing. A row without a value is a publisher saying it has no observation, and it would otherwise replace one or be read as the newest quote. `complete_rows` drops all such rows before any merge, and inside a batch it drops them before the sources are reduced to one delivery per day, so a source with nothing for a day cannot win that day over one that has a figure.

Because every source writes the same three columns, the instrument identifier is the only thing that tells them apart. It is not a single code system but a convention per source:

| Source | `INSTRID` | Example |
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
| internal | whatever the delivered file states | |

There is no column saying which source a row came from, so an identifier must not be claimed by two sources at once. The six rates are named rather than given an ISIN for that reason: they are rates, not instruments, and an ISIN would collide with the Yahoo source. The prefix says which kind of rate, because the two kinds are not read the same way: `ON-` is an overnight rate the market traded at, `KR-` a policy rate a central bank decided on.

Nothing in the table is filled in for days a source does not quote on. A rate quoted on business days is absent at the weekend, and a caller who needs a value for every calendar day has to carry the last quote forward themselves. This differs from `exchangerate`, whose Silver table does hold one row per calendar day, and it is worth knowing before the two are joined.

## From source to bronze layer

Each publisher gets a directory of its own in the Bronze layer, so that a price is always traceable to the source it was taken from. The downloads pass through a temporary directory on the local disk of the driver and not through the data lake: the CSV files are never meant to be kept, and on Databricks the Bronze layer is a Unity Catalog volume that should not be used as scratch space. Only the parquet data set reaches the data lake.

The ECB, the New York Fed, the Bank of England and the LBMA share one download, `download.downloaded`, which sends the user agent `fincommons` and the timeout `HTTP_TIMEOUT` and closes the response whatever happens. **Their series are written over the Bronze layer whole, so an answer that is not the series is refused before it is written.** An answer with an error status or without a single byte is refused by the download itself, and one that parses to no observation — an error page served with status 200 is the usual case — by `download.refuse_empty_answer`, which reads the answer exactly as it is about to be written. Either way the download raises, naming the source and the address, and the Bronze layer keeps the series the last run wrote. Without the check an error page would replace years of history with nothing and the run would report success, because the Silver stage would then find nothing to take and the table would simply stop moving.

### Public data from the ECB

The ECB publishes through an API that answers with CSV, one request per data flow. Two flows are downloaded, `EST` and `FM`, each unchanged into a directory of its own.


```python
# Download ECB data and store it in the bronze layer folder
mp.update_bronze_ecb(spark, config_path)

# or one flow at a time
mp.update_bronze_ecb_estr(spark, config_path)
mp.update_bronze_ecb_fm(spark, config_path)
```

Both are parsed with a schema shipped in the package, `marketpricedata_ecb_estr` and `marketpricedata_ecb_fm`, rather than with an inferred one, so that a change in what the API returns shows up as a column that does not fit instead of as a table that has quietly changed shape. The header row the API sends is skipped: a reader that is given a schema does not skip it by itself, and where it is parsed it arrives as a row whose typed fields are all null.

The two flows are not typed alike. In the `EST` schema `TIME_PERIOD` is a date and `OBS_VALUE` a double, so the €STR series reaches the Silver layer in the types the table wants without a cast. In the `FM` schema `TIME_PERIOD` is a string, which `update_silver_ecb_fm` casts.

### Public data from the New York Fed

SOFR is administered by the Federal Reserve Bank of New York, which publishes it, and it is read from the Bank rather than from FRED, which mirrors it, for the same reason SONIA is read from the Bank of England.



```python
# Download New York Fed data and store it in the bronze layer folder
mp.update_bronze_nyfed(spark, config_path)

# or one rate at a time
mp.update_bronze_nyfed_sofr(spark, config_path)
```

The markets API answers JSON, with the observations inside one `refRates` array rather than one to a line, so the file is read whole as the LBMA answer is. It takes a date range rather than a count — the `last/n` form refuses a count large enough to cover the history — so the whole of it is asked for by date. The request carries a user agent, as at the Bank of England and the LBMA.

**The Bronze layer keeps more than the rate**, and that is most of the reason for reading the administrator. The mirror carries the rate alone; the administrator publishes the distribution it was drawn from and a flag saying whether the figure has been revised:

| Column | Content |
| --- | --- |
| `sofr` | the rate |
| `percentile1`, `percentile25`, `percentile75`, `percentile99` | the distribution of the transactions behind it |
| `volume` | the traded volume, in billions |
| `revision` | set where the observation has been republished |

The percentiles and the volume say whether a rate was drawn from a thin or a dispersed market, which is the question a supervisor asks of a benchmark. `revision` matters more than it looks: the Silver table is maintained by upsert, so a revised figure silently replaces the one collected before it, and without the flag nothing anywhere would record that a figure had ever moved. None of it fits the three columns of the Silver table, which takes the rate — so the Bronze layer is where it survives.

Compared date by date, the two agree on every one of the 2,112 dates they share, and the administrator has one the mirror lacks: the 2nd of April 2018, the first observation.

### Public data from the Bank of England

SONIA is the Bank of England's own benchmark and is taken from the Bank, not from FRED, which mirrors the series under the Bank's code. A mirror is a convenience rather than a source: it runs a day behind at times, it can stop being maintained without saying so, and a supervisory data set should be able to name the body that published each of its figures.



```python
# Download Bank of England data and store it in the bronze layer folder
mp.update_bronze_boe(spark, config_path)

# or one series at a time
mp.update_bronze_boe_sonia(spark, config_path)
```

Two things about this source are properties of it rather than choices, and both fail quietly if they are undone.

**The request has to carry a user agent.** The Bank serves its statistical database through a content delivery network that answers a request without one with `403 Access Denied` and an HTML page. A CSV reader handed that page returns a table of one column and no rows, so a download that lost its user agent would write an empty Bronze layer and report success. The download therefore checks the status before it writes anything, and refuses an answer that parses to no observation, which is what the same page served with status 200 would be.

**The dates have to be parsed rather than cast.** The Bank writes an observation as `02 Jan 2026` and not as `2026-01-02`, so a cast yields null for every row of the series. The month is an English abbreviation, which is the locale Spark parses in by default.

The series is `IUDSOIA`, the Bank's code for the daily SONIA rate, and the identifier in the Silver table stays `ON-SONA`. An identifier names the rate and not the publisher, so a change of source is not a change of identifier — and renaming it would leave the rows collected under the old name behind, because the upsert replaces a row but never removes one.

### Public data from the LBMA

Gold and silver are benchmarks that a body sets and publishes. The London Bullion Market Association sets the LBMA Gold Price and the LBMA Silver Price, and they are taken from the association for the same reason SONIA is taken from the Bank of England.



```python
# Download the precious metal benchmarks into the bronze layer folder
mp.update_bronze_lbma(spark, config_path)

# or one metal at a time
mp.update_bronze_lbma_gold(spark, config_path)
mp.update_bronze_lbma_silver(spark, config_path)
```

Gold is set twice a day and the afternoon benchmark is the one taken: it is what is normally meant by "the LBMA Gold Price" and what a holding is valued at, because it is set while both London and New York are trading. Silver is set once a day, so there is nothing to choose between.

The association serves JSON rather than CSV, as one array rather than one object per line, so the file is read whole. It refuses a request without a user agent, as the Bank of England does.

Each entry carries the same price in three currencies, in the order US dollar, pound sterling, euro. The combined table has no column saying what currency a value is in, so one has to be chosen and written down: the dollar, which is what these benchmarks are quoted and settled in, and which is also the only one of the three that spans the whole history. The euro entry is null before 1999, which is the currency not existing yet rather than a gap in the data — changing `LBMA_CURRENCY` to take it would silently drop thirty years.

### Public data from Yahoo Finance

This source covers instruments rather than rates, and it is the one source that is queried for a date range instead of for the whole history. Its Bronze layer is therefore partitioned by the date of the download and appended to, not overwritten: the partitions already present are what tells the next run where to carry on from.



```python
# Download closing prices and store them in the bronze layer folder
mp.update_bronze_yahoo(spark, config_path, min_date = "2024-01-01")
```

`min_date` only applies to the first run. After that the start date is read off the partitions in the Bronze layer, and a run that has nothing left to fetch logs that the Bronze layer is current and stops.

**The prices are the closes that were traded.** Yahoo Finance adjusts its history by default, for dividends and for splits, and neither adjustment gives a price anyone dealt at. With the dividend adjustment an Allianz share that closed at 156.00 euro on the 4th of January 2016 is shown at 91.93, and with the split adjustment NVIDIA, which traded at about 1,200 dollars the week before its ten-for-one split in June 2024, is shown at about 120. Both adjustments also move whenever a new dividend or split comes in, so a history collected one download at a time would be adjusted to a different day in each partition. The download therefore asks for the closes without the dividend adjustment (`auto_adjust=False`) and with the splits (`actions=True`), and `traded_closes` multiplies each close by the ratios of the splits that took effect after it. The query runs to the day after the run, so that a split taking effect on the day of the run is in the answer; that day's own close is not kept.

A partition is named after the day the download was *run*, not after the last reference date it brought back. Yahoo Finance is queried for the half-open range `[start_date, end_date)` and `end_date` is the day of the run, so `LASTDT=2024-02-01` holds prices up to the 31st of January and the next run starts on the first of February — on the day the partition names rather than the day after it. `get_start_date` is the one place that has to know this, and `test_no_reference_date_falls_between_two_downloads` is what holds it.

The instruments to be downloaded are named by a mapping table shipped with the package, `data/isin_yahoo.csv`, which pairs an ISIN with the ticker Yahoo knows it under in the columns `ISIN` and `yahoo`. It covers 4,808 instruments:

* **65 chosen by hand**: the Austrian listings, the significant euro area banks, a handful of the large non-financials the European indices move with, and the benchmark indices themselves.
* **4,338 shares listed on a European regulated market**, taken from FIRDS: every share (CFI `ES`) with a record in force whose issuer requested admission to a regulated market, 6,176 in September 2026. The ticker comes from OpenFIGI's listings of the ISIN, spelt the way Yahoo spells that exchange, and a row is kept only where Yahoo returned prices for it over the last three months. A share is named by its listing on the home market of its ISIN's country wherever Yahoo carries that market, and elsewhere by its primary listing abroad -- a Luxembourg company on Xetra, a Bermudan one in Oslo -- but never by a secondary quotation such as the Frankfurt floor, whose prices for a foreign share are thin and often stale. The 51 hand-chosen shares the method also reached came out with the same ticker. The 1,787 it did not reach are mostly Swedish small caps (378), whose Yahoo spelling could not be found or which Yahoo does not carry, and listings in Sofia, Nicosia, Zagreb and Valletta, exchanges Yahoo does not serve at all.
* **405 shares listed in Ho Chi Minh City** (HOSE), by the ISIN and trading code the exchange publishes, all of them served by Yahoo as `<code>.VN` in September 2026. They are quoted in dong; `EUR/VND` is among the exchange rates below. The Hanoi exchange and the history of the VN-Index are not served by Yahoo and come from VNDirect instead; see below.

Extending the coverage is a matter of adding rows to that file rather than of changing any code.

**A ticker added to a lake that already holds a history is asked for its own.** A run carries on from the partitions of its Bronze layer, which every ticker shares, so a row added after the first run would otherwise only ever be asked for the last fortnight. A ticker the layer holds no price for is therefore asked for from `YAHOO_NEW_TICKER_FIRST_DATE`, the start of 2020 -- not from 2000, because the European listings above number in the thousands -- unless its row gives a `first_date` of its own, as the Vietnamese rows do (2010, the first year Yahoo has). The tickers are asked for `YAHOO_BATCH_SIZE` at a time with a pause between batches, because Yahoo does not refuse a client that asks too fast but answers it with no prices, which a run would read as tickers that did not trade.

The commodities are downloaded the same way, from a second mapping table, `data/commodity_yahoo.csv`. A commodity future has no ISIN, so that table names the identifier directly in the columns `INSTRID` and `yahoo`, and the two sources write into Bronze layers of their own so that each carries on from its own partitions.


```python
# Download commodity prices and store them in the bronze layer folder
mp.update_bronze_yahoo_commodities(spark, config_path, min_date = "2024-01-01")
```

What is collected is the **front month future** of each, which is not a spot price. It is the price of the nearest contract still trading, so the series steps whenever one contract expires and the next becomes the front month, and that step is a change of contract rather than a movement in the market — it is plainest in natural gas. It is the closest thing to a daily commodity price that can be read without a licence, which is why it is what the module collects and why the difference is written down here rather than left to be discovered.

The download itself needs the `yfinance` package, which comes with the `marketprice` extra:

```
pip install 'fincommons[marketprice]'
```

The exchange rates the ECB does not fix come from Yahoo Finance as well, from a third mapping table, `data/fx_yahoo.csv`. It holds two rows today: the Bangladeshi taka, which the Dhaka prices below are quoted in, and the Vietnamese dong, which the Ho Chi Minh City listings above are; without them neither could be priced in euro at all. The rates are named `EUR/BDT` and `EUR/VND` and quoted as units per euro, which is how the ECB names and quotes its own, so a reader converts with either the same way; its Bronze layer, `yahoo_fx`, keeps it traceable to who published it. It is a market quote at Yahoo's close rather than a fixing, and both currencies are managed, so the series move in the steps their central banks allow rather than day by day.

**A rate that leaves for one day and comes back is left out.** Yahoo delivered 261.04 dong per euro on the 8th of November 2019, between 25,310 and 25,183, and every Vietnamese price converted that day came out a hundred times too large. The Silver stage of the exchange rates therefore drops a rate that stands more than `YAHOO_FX_ISOLATED_FACTOR` (1.5) away from both the day before and the day after while those two agree, and takes it out of the table where an earlier merge wrote it: a print on the last day of a download has no day after it yet, and is judged when the next download asks for that fortnight again. A step a currency stays at is kept, because the day after agrees with it. Each dropped rate is named in a warning; `update_silver_yahoo_fx(spark, config_path, full=True)` judges the whole history again.

```python
mp.update_bronze_yahoo_fx(spark, config_path)
```

It is an extra and not a dependency because it is the only thing in the package that needs it, and the module imports and runs without it: the import is deferred to the one function that downloads from Yahoo, so the ECB, the New York Fed and the internal sources are unaffected. The Silver stage needs none of it and runs with the rest of the layer.

### Public data from the Dhaka Stock Exchange

The DSE is collected from the JSON service behind the data archive of its website: the day-end prices of every listed security, the DSEX and DS30 indices, and the notices its listed companies file.

```python
mp.update_bronze_dse(spark, config_path)
```

A share reaches the Silver table as `DSE-` and its trading code, `DSE-GP` for Grameenphone, and the indices as `IX-DSEX` and `IX-DS30`. The exchange publishes no ISIN beside its codes, and an ISIN written here could not be checked against anything. The value is the close of a day the share traded; a day it did not trade, which the archive reports with the last close carried, is left out.

**The archive reaches back two years and no further.** The lake holds a longer history only because it keeps what it collected, so it reaches back to two years before the first run and no further, and a lake not updated for two years loses the days in between for good.

**An answer is capped at a few hundred rows and does not page.** A trading day has some six hundred and sixty securities with the bonds and the funds, so the prices are asked for one security at a time, a year per request, and the news one day at a time. An answer that reports more rows than it carries is raised rather than written. A first run sends some two thousand requests and takes about a quarter of an hour; a daily run sends one per security.

**The closes are not adjusted for corporate actions, and on this exchange that matters.** A bonus issue is the ordinary way a Bangladeshi company pays out, and most banks issue one every year, so the closes fall by a sixth on a day nobody lost anything: City Bank closed at 22.80 taka on the 21st of May 2025 and at 19.00 on the next day it traded, which was a 12.5% stock and a 12.5% cash dividend going ex. The exchange publishes no ex-dates, but its news carry the two notices that fix one: the dividend declaration, with the percentages and the record date, and the *resumption after record date*, with the day trading resumed once the register was drawn, which is the first day the share traded ex. `dse_events` reads them, and the Silver stage writes one row per event into a table of its own beside the prices:

```python
events = mp.connect_to_dse_corporate_actions(spark, config_path)
```

| Column | Content |
| --- | --- |
| `INSTRID` | the share, as the price table names it |
| `EXDATE` | the first day it traded ex |
| `KIND` | `bonus`, `cash` or `rights` |
| `FACTOR` | what a close before the ex-date is multiplied by to be comparable with one on or after it |
| `RECORD`, `DETAIL`, `SOURCE`, `EXDATE_FROM` | the record date, the terms as read, the notice, and whether the ex-date came from a notice or, where there was none, from the first day the share traded after the record date |

A bonus issue of b per cent has the factor 1/(1 + b/100); a cash dividend of d taka against the close P before it, 1 − d/P, taking the face value as the ten taka the Commission set every share to in 2011; a rights issue of n shares for m at a price S, the theoretical ex-rights price (mP + nS)/((m + n)P), and none where S is above P. Multiplying by the bonus and rights factors gives the price history of a holding, and by all three its total return. Measured over the two years the lake first collected, 540 events of 481 securities: on the ex-dates of the 86 bonus and rights issues the median close moved by -6.4% as traded and by -0.1% once adjusted, and 27 of them moved by more than 10% as traded against 5 adjusted. The five are small, thinly traded shares, and the reader does not claim more than that: a notice worded in a way it does not know is an event it misses, and the close then shows the fall it would have explained.

The table is rebuilt whole every run: an event can only be placed once the notices on both sides of its record date have arrived, so one that was pending in one run is complete in a later one without any single download bringing it.

### Public data from VNDirect: the Hanoi Stock Exchange

The shares listed on the Hanoi Stock Exchange (HNX) and the Vietnamese indices come from the public market-data service of VNDirect, a Vietnamese broker. It is a broker's service rather than the exchange's own, and it is used because the exchange publishes nothing comparable that can be read without a browser, while Yahoo carries neither.

```python
mp.update_bronze_vndirect(spark, config_path)
```

A Hanoi share reaches the Silver table under the ISIN VNDirect's listing gives beside its trading code -- `VN000000SHS3` for Saigon-Hanoi Securities -- so it is named the way the Ho Chi Minh City shares from Yahoo are. The four indices are `IX-HNX`, `IX-HNX30`, `IX-VNINDEX` and `IX-VN30`. The value is the close of a day the share traded, in dong: **VNDirect quotes prices in thousands of dong** (Vinamilk's 59,800 is 59.8 there), so the close is multiplied by a thousand, which puts a Hanoi close and a Ho Chi Minh City close on one scale. A day with no volume is left out.

The Bronze layer keeps the close that traded and the one VNDirect adjusts for corporate actions, `AD_CLOSE`; only the traded close reaches the table, as for every other source, because the adjusted history is recomputed whenever a new action goes ex and differs from one download to the next. The listings are collected whole every run, since they pair each code with its ISIN, and a code that no listing has ever named is left out of the table with a warning.

The history reaches back to 2013 for most shares and to August 2017 for the indices. An answer is paged and says how many rows it holds; a page is asked for five thousand rows at a time, which is the whole history of a share, and an answer that ends short is raised rather than written. A run carries on from the last partition less a week, and a share or an index the layer has never held is asked for its whole history, also on a day the rest have nothing to fetch.

### Proprietary data

Due to the heterogeneity of the data source, the module does not provide any functionality to download data from proprietary sources. It picks such data up from

```
{path to Bronze Layer of marketpricedata as specified in config_path}/internal/marketpricedata_internal.csv
```

if that file exists, and expects a comma-separated file with a header row and the three columns of the Silver table, in this order:

| Column | Type | Content |
| --- | --- | --- |
| `REFDT` | date | The reference date of the quote, as `YYYY-MM-DD`. |
| `INSTRID` | string | What the value is a price of. See the convention above, and pick identifiers that no public source already uses. |
| `VALUE` | double | The price or rate. |

This is the form the proprietary sources of `exchangerate` and `entitymasterdata` take as well: one file, under a name the module knows, placed in the Bronze layer by whoever maintains the quotes. Nothing in the module writes into that directory.

The file is read with the schema declared in `common.py` rather than with an inferred one, which is why the columns have to be the three above and in that order: a reader that is given a schema maps it onto the columns of the file by position and does not go by the header. The types are therefore the ones the Silver table holds, and a reference date that does not parse arrives as a missing value rather than as text. **A file with such a row is refused whole**: the stage raises, naming the first rows that did not read, the run sets the source aside, and the quotes collected before stay in the table as they were. A quote that silently went missing from the one source that is the last word on its quotes would be worse than a run that says the file needs fixing.

Nothing sets the proprietary quotes apart from the public ones once they are in the table, and a delivery is merged like any other source: where the file carries an instrument and a reference date that a public source has already delivered, the proprietary quote replaces it, as long as the file is picked up after that source. `update_silver_layer` runs it last for that reason.

## From bronze to silver layer

Each source has an update function of its own, all with the same signature. Every one of them checks whether its input is there, says so and does nothing if it is not, so a source that has never been downloaded does not stop the others. "Says so" means a `WARNING` from `read_bronze` where the Bronze layer of the source holds no data set at all — the run did less than a reader would assume — and an `INFO` where an incremental source has simply received nothing since the last merge, or where the lake keeps no proprietary file. Only Spark's refusal to analyse a path that is missing or empty counts as "not there": any other failure to read is raised, so that the run sets the source aside with its traceback rather than reporting it as empty.


```python
mp.update_silver_ecb(spark, config_path)           # €STR, the key ECB rates, the US and euro area monthly series
mp.update_silver_nyfed(spark, config_path)         # SOFR
mp.update_silver_boe(spark, config_path)           # SONIA
mp.update_silver_exchangerate(spark, config_path)  # currency pairs
mp.update_silver_lbma(spark, config_path)          # gold and silver
mp.update_silver_yahoo_commodities(spark, config_path)  # other commodities
mp.update_silver_yahoo(spark, config_path)         # listed instruments
mp.update_silver_internal(spark, config_path)      # proprietary data
```

The exchange rates are not downloaded again here. They are read from the Silver table of `exchangerate`, which is the one place they are maintained, so that module has to be up to date first. Each rate enters as the price of a currency pair named `BASE/QUOTE`.

The ECB stage is mostly a matter of picking one series out of a flow that carries many. The `EST` flow holds fifteen of them, nine on the €STR benchmark alone — the rate itself, but also the number of banks reporting, the volume, the 25th and 75th percentiles and so on. The one that is wanted is `FREQ = 'B'`, `BENCHMARK_ITEM = 'EU000A2X2A25'`, `DATA_TYPE_EST = 'WT'`: the volume-weighted trimmed mean rate on a business-day frequency, which is the number normally meant by "€STR". All three parts of the filter are needed, because the benchmark item alone still leaves nine series and the frequency is shared by all of them.

The `FM` flow is the same problem several times over: it carries 115 series at five frequencies at once, and the frequency is what decides most of what can be taken from it. The Silver table is keyed by a date, and a year, a quarter and a month are not dates, so only `FREQ = 'D'` can be read at all — and that is also the right answer rather than merely the available one, because a monthly average of a rate is not the same kind of number as the daily quotes it would sit beside. What the daily frequency holds is the key ECB interest rates, on every calendar day back to 1999.

Three of them are taken, under the names `KR-DFR`, `KR-MRO` and `KR-MLF`:

| Series | Rate | Role |
| --- | --- | --- |
| `DFR` | deposit facility | what a bank earns on money left with the Eurosystem overnight — the floor |
| `MRR_RT` | main refinancing operations | the rate on the weekly tenders |
| `MLFR` | marginal lending facility | what a bank pays to borrow overnight against collateral — the ceiling |

`DATA_TYPE_FM = 'LEV'` is the third part of the filter, because the flow publishes the change in percentage points beside the level under the same name and the same date, and the table holds what a rate is rather than what it moved by.

`MRR_RT` is the main refinancing rate and not `MRR_FR` or `MRR_MBR`, which the flow publishes beside it. Those two are the rate under one tender regime each and each covers only the years its regime ran in — the minimum bid rate stops in October 2008, when the ECB moved to fixed rate full allotment. `MRR_RT` carries both, which is why it is the only one of the three that spans the whole history.

The monthly US yield curve series are taken as well, by a stage of their own, and they are the one part of the module that is not daily:

| Identifier | Series | What it is |
| --- | --- | --- |
| `YLD-US10Y-M` | `US10YT_RR` | US 10-year government benchmark bond yield, averaged over the month |
| `YLD-US10YZ-M` | `USD10YZ_R` | US 10-year zero coupon yield, at the end of the month |
| `SPR-US10Y1Y-M` | `US10YT_US1YT` | the spread between the 10-year and the 1-year |

A table keyed by a date has to say which day a month is. **A month is dated to its last day.** That is the date by which the observation is complete, so nothing is dated earlier than the last day it draws on, and a reader carrying the last quote forward never carries a figure across days it already covers — which dating it to the first of the month would do.

**The identifier says the series is monthly**, ending in `-M`. The Silver table has no column for the frequency, so a reader who joins these to the daily sources on the reference date finds a value on one day a month and nothing on the others, with nothing in the row to explain it. The suffix is the one place that warning can live, and it is what makes these series safe to have in the table at all.

Two warnings about what they are. `YLD-US10Y-M` is an average of daily observations through the month while `YLD-US10YZ-M` is a month-end figure, so neither is a month-end quote of the other. And `SPR-US10Y1Y-M` is a **term spread**, not a term premium: the spread is two observed yields subtracted, whereas the premium is the ten-year yield less the average short rate expected over ten years, which is not observable and takes a term structure model. They coincide only if expected future short rates equal today's one-year rate.

The euro area block is taken the same way, by `update_silver_ecb_fm_monthly_ea`, and is the widest of the three:

| Prefix | What it is | Unit | Series |
| --- | --- | --- | --- |
| `YLD-EA…-M` | government benchmark curve at 2, 3, 5, 7 and 10 years, plus the real 10-year | per cent per annum | 6 |
| `IBOR-EUR…-M` | EURIBOR at 1, 3, 6 and 12 months, plus the real 3-month | per cent per annum | 5 |
| `ON-EONIA-M` | the euro overnight benchmark until €STR replaced it | per cent per annum | 1 |
| `EQ-ESTOXX…-M` | the EURO STOXX index and ten of its sectors | index points | 11 |

The curve reaches back to 1970. The prefix is the only thing that says which kind of number an identifier holds, which matters most between the first three and the last: a rate in per cent and an index level in points are not comparable, and nothing in the row says so.

**Two of the twenty-five euro area series the flow publishes are deliberately left out.** `UONSTR` is €STR, which the table already carries daily as `ON-ESTR`, and `DJES50I` is the EURO STOXX 50, which arrives daily from Yahoo Finance under its ISIN. A monthly copy of a series held daily is not more data; it is the same data at a coarser grain under a second name, and a reader comparing the two would be comparing a series with itself. `test_a_monthly_copy_of_a_daily_series_is_left_out` is what records that, since nothing in the code would otherwise say why those two are missing.

`EONIA` is kept although it stopped in December 2021. Nothing else in the table carries it, and a supervisory data set still meets contracts that reference it; its end shows up in the coverage table like any other series that has stopped.

What is left out is everything the flow carries yearly and quarterly, which duplicates the monthly series at a coarser grain.

## Running a whole layer

The two functions below run the sources of a layer in turn. A publisher that is unreachable on a given morning should not stop the others from being brought up to date, so a source that fails is set aside with its traceback and the run carries on. That is the `ERROR` level of `fincommons.log`: a unit of work failed by design, not the run.


```python
# Download everything the module collects
mp.update_bronze_layer(spark, config_path)

# Reduce it all into the Silver table
mp.update_silver_layer(spark, config_path)
```

Both return the outcome of their stage, whose `failed` entry counts the sources that were set aside and whose `failed_sources` entry names them, so a caller can act on a partial run without reading the log back. Configure logging with `fincommons.log.configure()` to see the tracebacks.

The Bronze stage of the Yahoo source is called by neither of the two, see below.

## Reading the table


```python
prices = mp.connect_to_marketpricedata(spark, config_path)
```

This returns the Silver table as a Spark data frame, addressed by path or by catalogue name according to what the configuration file says about the Silver layer.

### What an identifier stands for

The table holds a date, an identifier and a value and nothing else, so what a series *is* -- its name, publisher, unit, currency and frequency -- is in the conventions of this module rather than in the data. `describe_instrument` reads them back for one identifier:

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

It knows every kind of identifier the public sources write -- ISINs of listed instruments (with the name and the ticker they are downloaded by), `EUR/XXX` rates from the ECB or Yahoo, the `ON-` and `KR-` rates, the ECB's `YLD-`, `SPR-`, `IBOR-` and `EQ-` series, the `EL-` electricity zones, the `CO-` commodities, the `VOL-` and `IX-` indices and the `DSE-` shares -- and it reads them from the same tables the downloads are driven by, so it cannot describe a series differently from how it is collected. The result is an `InstrumentDescription`, whose fields (`unit`, `currency`, `source_id`, ...) can also be read one by one. It needs neither Spark nor the data lake, with one exception: the name of a Hanoi share is only in VNDirect's listings, so `describe_instrument("VN000000SHS3", config_path=config_path)` reads it from the lake. An identifier no public source delivers raises `KeyError`; it may still be a proprietary quote, which the module takes as it comes.

The ISIN mapping table carries a `name` column for this: the full name FIRDS records for the European shares, VNDirect's English company name for the Vietnamese ones, and the index or fund name for the hand-chosen benchmarks.

### Zero curves and discount factors

`curves` turns the rates in the table into a risk-free zero curve per currency for one reference date, and evaluates it on the maturities of any Spark frame:

```python
from fincommons.marketpricedata import curves

segments = curves.build_curves(spark, prices, "2025-06-30", ["EUR", "USD"])
discounted = curves.add_discount_factor(payments, segments, "CCY", "YEARS", "DF")
```

The short end is the overnight rate of each currency (`ON-ESTR`, `ON-SOFR`, `ON-SONA`), and the longer maturities are overnight indexed swap quotes under `OIS-<index>-<tenor>`, for instance `OIS-ESTR-1Y`. Every pillar takes the latest quote at or before the reference date, and the log says when that is older than the date. A quote shorter than a year is read as a simple money market rate on actual/360, a longer one as annually compounded. Between the pillars the curve is log-linear in the discount factors, so the forward rate is constant on each segment, and beyond the last pillar the last forward rate carries on.

**No tenor pillar is delivered into the table yet, so a curve built today is flat at the overnight rate.** The `KR-` policy rates are not read: a rate a central bank decided on is not a quote for a maturity. The swap quotes are picked up under the identifiers above as soon as a source delivers them, without a change to the module.

A payment in a currency without a curve keeps its row and gets no discount factor, so a caller can report it as not covered rather than lose it. `parallel_shift` makes one copy of the curves per assumed shift of the continuously compounded zero rate, which `add_discount_factor` then evaluates in the same join with `keys=["SHOCK"]`, and `forward_rate` gives the simple forward rate of an accrual period from its two discount factors. The curves carry no credit or funding spread.

## The layout of the data lake

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

## What is not finished yet

One source is downloaded outside the Bronze pipeline. It is not reported by a failing import and it does not stop the rest of the pipeline.

* **The Yahoo Bronze stage is not part of `update_bronze_layer`.** It is the only download that has to be told how far back to go and the only one that needs a package outside the dependencies of the project, so `update_bronze_yahoo` is called on its own. Its Silver stage needs neither and is part of `update_silver_layer`.

### Adding or renewing an ECB schema

A schema file is the plain JSON form of a Spark `StructType`, as `StructType.fromJson` reads it. Where the API starts returning a column that does not fit, derive it again from a downloaded answer rather than editing the file by hand:



```python
import json

df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
with open("src/fincommons/data/marketpricedata_ecb_estr.json", "w") as f:
    json.dump(df.schema.jsonValue(), f, indent=4)
```

Check the inferred types by hand afterwards. Inference reads the types off whichever observations happen to be in the file, so a column that is empty in the sample comes out as a string, and a code that happens to be numeric throughout comes out as a number.

## Tests

The module is covered by the tests in `tests/test_marketpricedata.py`, which run against the local Spark session built by `tests/conftest.py`. Nothing in them reaches the ECB, the New York Fed, the Bank of England, the LBMA or Yahoo: the answers of the two APIs are written inline and handed to the download in place of a response, so what is asserted is what the module does with an answer rather than what those sources happen to be publishing today.

| Group | Covers |
| --- | --- |
| the data lake | creating the directory tree, including below a directory that does not exist yet, repeating the call, and every path coming from the configuration file |
| the download | that no CSV file is left in the data lake, that the header row is skipped, that the response is closed when the download fails, that the shared download sends a user agent and a timeout and refuses an empty answer, that an empty answer or an error page with status 200 leaves the Bronze layer of the ECB, the New York Fed, the Bank of England and the LBMA alone and fails naming the source, the typing of a New York Fed answer including a day it has no rate for, that its Bronze layer keeps the percentiles, the volume and the revision flag, that the whole history is asked for by date, and, for the Bank of England, that the request carries a user agent, that a refusal leaves the Bronze layer alone and that the Bank's own date format is parsed |
| the Silver layer | picking €STR out of the flow and leaving the other series, dating a monthly series to the end of its month, its identifier saying that it is monthly, the three FM stages leaving each other's series alone and claiming no identifier twice, the euro area block being filtered by area and leaving out the monthly copies of daily series, picking the three key ECB rates out of the FM flow and leaving its other frequencies, its changes and its regime-specific variants, casting a reference date that flow types as a string, the name SOFR arrives under, a rate becoming a currency pair, and a source that has delivered nothing, which leaves the table alone and says so in a warning |
| the proprietary source | a delivery being picked up, the types it arrives in, the header row not becoming a quote, and a data lake without such a file |
| the upsert | a day delivered again replacing its row, a new day being added, two instruments on one day staying two rows, and columns beyond the three being dropped |
| the pipeline | a source that fails being set aside while the others carry on; and, in `tests/test_marketpricedata_pipeline.py` and without Spark, a stage counting and reporting the source it set aside, the run adding up its stages, the command line exiting with 1 where a source was set aside and with 0 where none was, and no module of the library calling `print` |
| commodities | that the LBMA price is taken in dollars, that a price predating the euro is still read, that the LBMA request carries a user agent, the two metals reaching the table under their names, the commodity mapping table and its identifiers, a commodity keeping the identifier it is named by, the most recent delivery of a commodity day winning, and the commodities carrying on from their own partitions |
| Yahoo Finance | where a run starts from and that it meets the previous run without a gap, the absence of `yfinance` being reported in terms of the download, the mapping table travelling with the package, a price arriving under its ISIN, the day of the download being dropped, the most recent delivery of a day winning, and the source being part of the Silver layer |

Some of these are worth naming, because they are pinned to rules that are easy to undo by accident rather than to behaviour that would obviously break:

* `test_the_header_row_of_an_ecb_answer_is_not_read_as_data` holds the `header` option on a reader that is also given a schema. Without it the header is parsed as an observation with its typed fields nulled. The €STR filter happens to discard that row, so the two Silver tests pass either way and only an assertion on the Bronze layer catches it — which is why this test sits where it does.
* `test_the_other_series_of_the_est_flow_are_left_out` holds all three parts of the €STR filter at once. The flow carries the number of reporting banks and the traded volume beside the rate, and dropping any one part of the filter lets several of them through under the name of the rate.
* `test_a_day_that_is_delivered_again_replaces_the_row` and `test_the_same_day_of_two_instruments_is_two_rows` hold the two halves of the merge condition. The sources are downloaded whole on every run, so the first is what makes the pipeline re-runnable; the second is what keeps the second source of a day from overwriting the first.
* `test_every_path_is_read_from_the_configuration_file` holds the data lake being addressed through the configuration alone, with no root left written into the code.
* `test_only_the_three_columns_reach_the_table` holds the projection in `standard_upsert`. The sources do not agree on what they carry beyond the three columns, and a merge only works where the two sides match. It calls the upsert directly rather than through a source, because every source of the module hands it the three columns and none of them would notice the projection going.
* `test_the_header_row_of_a_proprietary_file_is_not_read_as_a_quote` holds the `header` option on the reader of the proprietary file, which is given a schema and does not skip the header by itself. Unlike the €STR flow, nothing filters that row out again: it would reach the table as a quote with no date and no value.
* `test_the_most_recent_download_of_a_day_is_the_one_that_is_kept` holds the reduction of the Yahoo Bronze layer to one row per instrument and reference date. That layer is partitioned by the day of the download and appended to, so a reference date delivered by two downloads is held twice, and a merge whose source carries the same key twice fails outright rather than choosing between them — which would take the whole Silver run with it and not only that source.

Spark cannot start natively on a Windows workstation, so the suite is run in the container described in [`docker/README.md`](../../../docker/README.md):

```
docker compose -f docker/compose.yaml run --rm tests pytest tests/test_marketpricedata.py -q
```

Where Spark does run natively, the same thing from the root of the repository:

```
pytest tests/test_marketpricedata.py
```
