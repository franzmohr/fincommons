# Exchange rates

This module downloads the euro reference rates of the European Central Bank (ECB) and, optionally, the exchange rate statistics of the Bank for International Settlements (BIS), combines them with your own proprietary rates into one table with a row per calendar day and currency pair, and converts a column of amounts into a single currency with it -- for instance to make amounts reported in different currencies under EMIR (the European Market Infrastructure Regulation) comparable in euro.

Exchange rate data is a basic building block of a serious financial analytics framework. From a European supervisory data engineering perspective, it is also required to convert data that was reported in a non-domestic currency unit to a single currency in order to make monetary units comparable.

At a glance:

* **Sources:** the ECB reference rates (primary), the BIS bulk exchange rate statistics (optional, `use_bis = True`), and a proprietary CSV file of your own (optional, picked up when it exists).
* **Update frequency:** the ECB publishes on every TARGET business day; the BIS publishes several days late. Each update rebuilds the table in full from the Bronze layer and replaces it.
* **Table written:** one Silver table, `exchangerate`, one row per calendar day, base currency and quoted currency. The base is the euro unless you rebase it to another currency.
* **Needs:** network access to the ECB (and the BIS, where used), no credentials, and a Spark session for the Silver layer and for `convert2ccy`.
* **Layers:** the Bronze layer holds the files as the sources published them; the Silver layer holds the cleaned, combined table that analyses read.

## Quick start

1. Add the module's section to `paths.yaml`:

```yaml
exchangerate:
    bronze: /path/to/lake/exchangerate/bronze
    silver: /path/to/lake/exchangerate/silver
```

2. Start Spark (the full session, with the reasons for each setting, is under [Running it on a schedule](#running-it-on-a-schedule)), create the directories, run the update and read the result:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder.master("local[4]").appName("fincommons") \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set path to configuration file, where paths are specified
config_path = "paths.yaml"

# Load the module
from fincommons import exchangerate as ex

# Create folders for exchange rate data, unless they already exist
ex.create_directory_tree(config_path = config_path)

# Download the ECB rates into the Bronze layer and build the Silver table
ex.update_exchangerate(spark, config_path)

fxrate = ex.connect_to_exchangerate(spark, config_path)
fxrate.filter("quote = 'USD'").orderBy("ref_date", ascending = False).show(5)
```

3. Convert amounts into euro:

```python
rows = [("2020-01-01", "USD", 1.0), ("2022-03-01", "RUB", 1.0)]
data = spark.createDataFrame(rows, "date string, currency string, value double")
ex.convert2ccy(data, config_path, date = 'date', amount = 'value', currency = 'currency', target = 'EUR').show()
```

## What you get

### The `exchangerate` table

One row per calendar day (`ref_date`), base currency (`base`) and quoted currency (`quote`); these three columns identify a row. The base is the euro, or the currency named in `rebase`.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `ref_date` | date | The day the rate applies to. Every calendar day is present, weekends included, see [Days without a quote](#days-without-a-quote). | `2024-06-03` |
| `base` | string | ISO 4217 code of the base currency. | `EUR` |
| `quote` | string | ISO 4217 code of the quoted currency. | `USD` |
| `value` | double | Units of `quote` per one unit of `base`. Never summed; averaged only within one currency pair. | `1.0882` |
| `type` | string | The kind of rate: `ref` for an ECB reference rate, `avg` for a BIS daily average, or whatever the proprietary file states. | `ref` |
| `source` | string | Where the winning quote came from: `ECB`, `BIS`, or the label in the proprietary file. | `ECB` |
| `imputed` | boolean | `True` on a day whose rate was carried from the last quote before it, `False` on a day a source quoted. | `False` |

The authoritative description of the columns is the data contract [`data/contract_exchangerate_silver_exchangerate.yaml`](../data/contract_exchangerate_silver_exchangerate.yaml), see [Data contract and quality rules](#data-contract-and-quality-rules).

### How it joins to other data

The table joins to any frame that holds a date, a currency code and an amount -- for instance the notional amounts of EMIR or SFTR (Securities Financing Transactions Regulation) reports, or prices in other fincommons modules. `convert2ccy` does that join for you, on the calendar day of the date column and on the currency code.

## Common tasks

### Convert an amount column of a Spark frame into euro

When your frame holds a date, a currency and an amount column, `convert2ccy` adds the rate and the converted amount:

```python
# Create the rows: date, currency and amount
rows = [("2020-01-01", "USD", 1.0), ("2022-03-01", "RUB", 1.0), ("2022-03-02", "RUB", 1.0),
        ("2020-01-01", None, 1.0), ("2020-01-01", "XAU", 1.0), ("2020-01-01", "EUR", 1.0)]

# Make a Spark data frame
data = spark.createDataFrame(rows, "date string, currency string, value double")

ex.convert2ccy(data, config_path, date = 'date', amount = 'value', currency = 'currency', target = 'EUR').show()
```

The result is the frame with two columns added, named after the column that was converted. With `amount = 'value'` and `target = 'EUR'` these are:

* `value_FXRATE`: the rate that was applied, and
* `value_EUR`: the converted amount, `value / value_FXRATE`.

In the example above, the `EUR` row is converted at a rate of 1, while the row without a currency, and the `XAU` row if the table does not cover gold on that date, get neither a rate nor a converted amount. What else gives no rate is listed under [Troubleshooting](#troubleshooting).

### Convert an amount column of a pandas or polars frame

Pass the frame as it is; it comes back as the same kind of frame, in its own row order, and a pandas frame with its own index. The conversion runs on the active Spark session; where there is none, pass it with `spark = ...`:

```python
import pandas as pd

trades = pd.DataFrame({"trade_date": ["2024-06-03", "2024-06-08"],
                       "ccy": ["USD", "GBP"],
                       "notional": [1_000_000.0, 250_000.0]})
converted = ex.convert2ccy(trades, config_path, date = "trade_date", amount = "notional",
                           currency = "ccy", target = "EUR", spark = spark)
converted[["notional", "notional_FXRATE", "notional_EUR"]]
```

A polars frame is converted the same way. A Delta table is converted by reading it as a Spark frame first, with `spark.read.table(name)` or `spark.read.format("delta").load(path)`.

### Look up the rate of one currency pair on one day

```python
fxrate = ex.connect_to_exchangerate(spark, config_path)
fxrate.filter("quote = 'USD' AND ref_date = DATE'2024-06-08'").show()   # a Saturday: imputed = true
```

### Rebase the table to a currency other than the euro

For an organisation whose domestic currency is not the euro, rebuild the table with `rebase` and convert into that currency. `convert2ccy` reads the rates quoted against the currency it converts into, so any target other than the euro needs the table rebased to it:

```python
ex.update_exchangerate(spark, config_path, rebase = "SEK")
ex.convert2ccy(data, config_path, date = 'date', amount = 'value', currency = 'currency', target = 'SEK').show()
```

The rebased table holds rates against the new base only, with the euro added as a quoted currency, so converting into `EUR` afterwards is refused. The currency must be one the ECB quotes, and `rebase = "EUR"` is refused; see [Rebasing](#rebasing).

### Add a proprietary rate source

When you have rates the public sources lack, write them to `exchangerate.csv` in the `internal` folder of the Bronze layer (`ex.path_bronze_internal_exchangerate(config_path)`), with a header row and these columns in this order:

```
ref_date,base,quote,value,type,source
2024-06-03,EUR,XAU,0.000465,ref,treasury
```

Check the file before the update, with the same check the update runs:

```python
import os
internal_file = os.path.join(ex.path_bronze_internal_exchangerate(config_path), "exchangerate.csv")
internal = spark.read.options(header = 'True', delimiter = ',').schema(ex.internal_schema).csv(internal_file)
ex.check_internal_rates(internal, internal_file)   # raises ValueError naming each problem

ex.update_exchangerate(spark, config_path)
```

Its quotes take precedence over the ECB and the BIS. The rules the file must follow are under [The proprietary file](#the-proprietary-file). Update the file before you run the update.

### Include the BIS rates

When you need currencies the ECB does not quote, add the BIS. `update_exchangerate` then downloads the BIS file as well; the table ends on the BIS's last day:

```python
ex.update_exchangerate(spark, config_path, use_bis = True)
```

### Check the table against its contract

```python
from fincommons.contracts import check_quality, load_contract, quality_failures
results = check_quality(ex.connect_to_exchangerate(spark, "paths.yaml"),
                        load_contract("fincommons.exchangerate.silver.exchangerate"))
quality_failures(results)              # the errors; severities=("error", "warning") for both
```

## Running it on a schedule

### The Spark session

The Silver layer is written as parquet unless the user asks for Delta using `file_format = 'delta'` or addresses it by catalogue name. Where Delta is used, the session has to be built with the Delta extensions off-cluster, which needs the `delta-spark` package that comes with the `local` extra; on Databricks Delta is built in and the extra configuration is unnecessary. A session built this way reads a parquet table just as well, so it is the safe thing to start from either way.

```python
# Start Spark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

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
```

* `master("local[4]")`: the cores are stated here rather than through `spark.cores.max`, which only a standalone or Mesos master reads. A session built without a master comes up as `local[*]` and runs a task on every core the machine has, dividing the driver heap -- which in local mode is the memory of the whole run, since the executors live in that JVM -- among all of them. The FIRDS [README](../firds/README.md) says what that costs on a machine with many cores.
* `spark.driver.memory`: the heap of that one JVM.
* the three `CORRECTED` rebase modes: read and write old dates and timestamps in parquet without the legacy calendar conversion.
* the two Delta settings and `configure_spark_with_delta_pip`: the Delta extensions, needed off-cluster only.

How to size the cores, the heap and the scratch directory for your machine, and how to check that a session got what it was asked for, is in [Setting up a Spark session](../../../docs/spark-session.md).

### The update

The all-in-one function `update_exchangerate` updates the entire data set, in this order:

1. download the BIS file into the Bronze layer (only with `use_bis = True`), with `update_bronze_bis_exchangerate`;
2. download the ECB file into the Bronze layer, with `update_bronze_ecb_exchangerate`;
3. rebuild the Silver table, with `update_silver_exchangerate`. If proprietary data is provided in the `internal` folder, it is used to augment the ECB data set.

**Note that you should update your internal exchange rate data set prior to running this function.**

```python
ex.update_exchangerate(spark, config_path, use_bis = False)
```

It passes `rebase`, `use_bis` and `file_format` on to `update_silver_exchangerate`, so the storage format is chosen here as well:

```python
ex.update_exchangerate(spark, config_path, file_format = "delta")
```

The steps can also be run one at a time:

```python
# Download BIS data and store it in bronze layer folder
ex.update_bronze_bis_exchangerate(config_path)

# Download ECB data and store it in bronze layer folder
ex.update_bronze_ecb_exchangerate(config_path)

# Application for users in the euro area
ex.update_silver_exchangerate(spark, config_path, use_bis = False, file_format = 'parquet')
```

`update_silver_exchangerate` takes these arguments:

| Argument | Meaning |
| --- | --- |
| `spark` | The open Spark session. |
| `config_path` | The path to the configuration file, where the paths of the data lake are specified. |
| `rebase` | An ISO-3 currency code naming the currency the table should be based on instead of the euro, for organisations where the euro is not the domestic currency. Default `None` (euro). It must be a currency the ECB quotes. `convert2ccy` refuses to convert into a currency the table is not based on. |
| `use_bis` | Whether the exchange rate data of the BIS in the Bronze layer is combined in as well. Default `False`, since the BIS data is not updated daily. A run that uses it also ends the table on the latest date for which BIS data is available, see [How far the table reaches](#how-far-the-table-reaches). |
| `file_format` | The format the table is stored in, `'parquet'` or `'delta'`. Default `None`: the configuration file decides -- a Silver layer addressed by catalogue name is Delta, and one addressed by path keeps the format its table is stored in, or is written as parquet where there is no table yet. Parquet is the default for a new table at a path because it is the format every engine reads without a further dependency. |

### What a run does and leaves behind

* During the update the function logs the latest date in each data set for which observations are available, and, where the BIS is used, the date the table is cut back to.
* The table is stored in the folder given in `paths.yaml` under `exchangerate/silver`, or under the catalogue name given there. Each update replaces the table rather than adding to it, so a run can simply be repeated.
* A Delta update replaces the table in a single transaction, so a reader sees either the previous version in full or the new one, never a half-written file, and the previous versions remain readable. A parquet update deletes the table before it writes the new one, which leaves a window in which a reader finds it missing or half-written. This might be interesting to some users.
* A Bronze download is written beside the old file and put in its place once complete, so a download that dies leaves the last good file. Waiting on the server is limited by `DOWNLOAD_TIMEOUT` (30 seconds to connect, 120 seconds between two chunks).
* The checks that refuse a run -- a broken proprietary file, a rebase to a currency the ECB does not quote, BIS data without a daily average, a format the table is not stored in -- are made before anything is written, so the existing table is left as it was. Fix the cause (see [Troubleshooting](#troubleshooting)) and run again.

### Addressing the Silver layer by catalogue name

The Silver layer may instead be addressed by catalogue name, which puts the table within reach of Unity Catalog's grants and lineage. The configuration file alone says so; no argument of the update has to change:

```yaml
exchangerate:
    bronze: /Volumes/fincommons/exchangerate/lake/bronze
    silver:
        table: fincommons.exchangerate
```

The table is then `fincommons.exchangerate.exchangerate`, and it is always a Delta table, because that is what a Unity Catalog managed table is. Asking for `file_format = 'parquet'` there is refused before anything is read.

## How it works

### Bronze layer

**The Bronze layer holds what the sources published, as parquet, and nothing else.**

* The ECB file `eurofxref-hist.zip` is stored as `ecb/ecb_exchangerate.parquet`.
* The BIS bulk file `WS_XRU_csv_flat.zip` is stored as `bis/bis_exchangerate.parquet`, keeping only the columns the Silver layer reads (`BIS_COLUMNS`).
* The archives are fetched and unpacked in a temporary directory on the driver, not in the data lake; only the parquet file reaches the lake.

### The proprietary file

**Proprietary rates are read from `{bronze of exchangerate in paths.yaml}/internal/exchangerate.csv`, if that file exists.**

Due to the heterogeneity of the data source, the module does not provide any functionality to download data from proprietary sources. It expects a comma-separated file with a header row and the following columns, in this order. They are the columns of the Silver table, under the same names.

| Column | Type | Content |
| --- | --- | --- |
| `ref_date` | string | The reference date of the quote, as `YYYY-MM-DD`. |
| `base` | string | The ISO-3 code of the base currency. It must be `EUR`, the base the sources are combined on. |
| `quote` | string | The ISO-3 code of the quoted currency. |
| `value` | double | Units of `quote` per one unit of `base`. |
| `type` | string | The kind of rate, for instance `ref` for a reference rate. |
| `source` | string | A label stating where the quote came from. It is carried through to the Silver layer. |

The file is checked (by `check_internal_rates`) before it is combined, and refused as a whole where a row would be misread:

* a `ref_date` not written as `YYYY-MM-DD`;
* a `base` other than `EUR`;
* a `value` that is missing or not positive;
* one day and pair quoted twice.

The date check matters most, because the combination compares dates as text: a date written `2020-10-5` is not the same day as the ECB's `2020-10-05` to it, and one such row could cut the whole table short. A file with a header but no rows is accepted with a warning, and only the public sources are combined.

The columns are read by position, not by the names in the header, so a file still headed `REFDT,BASE,QUOTE,VALUE,TYPE,SOURCE`, as the file was before the column names of the package were put into snake case, is read the same way.

### How the sources are combined

**Where more than one source quotes the same currency pair on the same day, the proprietary data takes precedence over the ECB, and the ECB over the BIS.**

Only the winning quote reaches the Silver layer, and the `source` column of the result states which source it came from. The BIS quotes against the US dollar; its rates are turned into rates against the euro with the ECB's EUR/USD rate of the same day.

### How far the table reaches

**Without the BIS, every source counts up to its own last day; with the BIS, the whole table ends on the BIS's last day.**

Without the BIS, where one source has fallen behind, the others describe the days it does not reach, as they do on any day it is silent, rather than the newest rates of the up-to-date sources being thrown away.

With `use_bis = True` the table ends on the latest date for which BIS data is available. The reasoning:

* The BIS publishes its bulk statistics several days late, and the days beyond it would be described by the ECB and the proprietary source alone.
* A pair only the BIS quotes would then stop days before the rest, and the day an amount is converted on would decide which currencies the table covers.
* The cut keeps the whole table on the same footing, at the price of the few newest days of the other sources, which the next run picks up once the BIS has caught up.

The cut is applied after the days without a quote have been filled, so a day before the cut still carries the last rate quoted on or before it. Where the BIS data in the Bronze layer holds no daily average at all there is no day to cut back to: the update is refused and the existing table left as it is, rather than replaced by an empty one.

### Days without a quote

**Every calendar day carries the last rate quoted on or before it, for at most `MAX_FILL_DAYS`, seven days, after that quote.**

The sources quote on business days, but an amount agreed on a Saturday still has to be converted, so the Silver layer holds one row per calendar day. The fill runs along the pair, whichever source the last quote came from.

* A week covers weekends and the TARGET closing days (the days the Eurosystem's payment system, and with it the ECB's reference rates, pause, such as around Easter and Christmas), and a source that is a few days late.
* A pair that goes unquoted for longer has no rate on the days beyond, rather than one that may be years old. The ECB, for instance, quoted no Icelandic krona from December 2008 to February 2018.
* The fill stops at the last quote of a pair: the last quoted day is present once and is not carried forward beyond itself. A currency that stops being quoted therefore ends with its last observation, and an amount dated after that gets no rate.
* Days before the first quote of a pair are not filled either.

The column `imputed` says which rows are filled: it is `True` on a day whose rate was carried from the last quote before it, and `False` on a day a source quoted. Wherever the ECB is the source, those are the weekends and the TARGET closing days, on which it does not publish; any other day without a quote is marked as well, whatever the reason for it. A rebased cross rate is imputed where either of the two euro rates it is computed from is.

### Rebasing

**With `rebase`, the whole combined table is recalculated against the named currency.**

For countries where the domestic currency is not the euro, the argument `rebase` can be specified with the desired currency code. The function then re-calculates the whole combined data set, proprietary and BIS quotes included, against the currency named, and adds the euro itself as a quoted currency.

```python
ex.update_silver_exchangerate(spark, config_path, rebase = "SEK")
```

A rebase to `EUR`, or to a currency the ECB reference rates in the Bronze layer do not quote, is refused before anything is written: the latter would find no euro leg to divide by and leave an empty table.

### Storage format

**`connect_to_exchangerate` and `convert2ccy` read the table in whichever form it is addressed and stored in, so the choice does not have to be repeated when the table is queried.**

A table that exists already keeps its format: an update that asks for the other one is refused before anything is written, because Delta will not adopt parquet files it has no transaction log for, and parquet written over a Delta table would throw its history away. Delete the directory before the first update in the new format to change it. The storage itself is shared with the other modules and described in [`fincommons.storage`](../storage.py).

### How `convert2ccy` converts

**`convert2ccy` takes a Spark, pandas or polars data frame and returns the same kind of frame, with two columns added and the columns passed in unchanged.**

* The conversion itself is done with Spark. A Spark frame is converted with its own session; a pandas or polars frame is handed to the active session, which is the one a Databricks notebook already provides and the one created above off-cluster. Where there is none, pass it with `spark = ...`.
* A pandas or polars frame comes back in its own row order, and a pandas frame with its own index.
* The date column may hold dates, timestamps or text such as `2020-01-01`. An amount is converted at the rate of its calendar day, so a time of day does not matter; a date that cannot be read gets no rate.
* Rows whose currency already is the target currency are converted at a rate of 1.
* Rows whose currency is missing, and rows whose currency the exchange rate data set does not cover on that date, get neither a rate nor a converted amount.
* The columns that are passed in come back unchanged.

## Reference

### Configuration

The module's section of `paths.yaml`:

| Key | Holds | Catalogue name allowed? |
| --- | --- | --- |
| `exchangerate.bronze` | The Bronze layer directory, with the subfolders `ecb`, `bis` and `internal` (created by `create_directory_tree`). On Databricks a Unity Catalog volume path. | No, a path. |
| `exchangerate.silver` | The Silver layer directory, holding the table `exchangerate`; or, as `table: <catalogue>.<schema>`, a schema in Unity Catalog. | Yes, see [Addressing the Silver layer by catalogue name](#addressing-the-silver-layer-by-catalogue-name). |

### Functions

Everything is reached as `from fincommons import exchangerate as ex`.

| Function | What it does |
| --- | --- |
| **Update** | |
| `update_exchangerate(spark, config_path, rebase=None, use_bis=False, file_format=None)` | Downloads the ECB (and, with `use_bis`, the BIS) data and rebuilds the Silver table. |
| `update_bronze_ecb_exchangerate(config_path)` | Downloads the ECB reference rates into the Bronze layer. |
| `update_bronze_bis_exchangerate(config_path)` | Downloads the BIS bulk exchange rate statistics into the Bronze layer. |
| `update_silver_exchangerate(spark, config_path, rebase=None, use_bis=False, file_format=None)` | Combines the Bronze data into the Silver table. |
| **Read and convert** | |
| `connect_to_exchangerate(spark, config_path)` | Opens the Silver table as a Spark frame. |
| `convert2ccy(data, config_path, date, amount, currency, target, spark=None)` | Converts a column of amounts into `target`, adding `<amount>_FXRATE` and `<amount>_<target>`. |
| **Helpers** | |
| `create_directory_tree(config_path)` | Creates the directories of the data lake; repeatable, creates missing parents, meant for a local data lake. |
| `check_internal_rates(internal, source, base="EUR")` | Refuses a proprietary file whose rows the combination would misread. |
| `internal_schema` | The Spark schema the proprietary file is read with. |
| `path_bronze_ecb_exchangerate`, `path_bronze_bis_exchangerate`, `path_bronze_internal_exchangerate(config_path)` | The Bronze folders. |
| `ref_silver_exchangerate_table(config_path)`, `path_silver_exchangerate_table(config_path)` | Where the Silver table lives; the path is `None` where it is addressed by catalogue name. |
| `migrate_folder_names(config_path)` | Renames hyphenated folders and files of an older lake (`ecb-exchangerate.parquet`) to the snake_case names the module reads. |
| `MAX_FILL_DAYS`, `DOWNLOAD_TIMEOUT` | The longest fill (7 days) and the download timeouts (30, 120 seconds). |

### Data contract and quality rules

What the columns of the table hold, and what a reader may rely on, is stated by its data contract, [`data/contract_exchangerate_silver_exchangerate.yaml`](../data/contract_exchangerate_silver_exchangerate.yaml), written in the Open Data Contract Standard. It is the one place the columns are described: `fincommons.describe_column("imputed")` reads its answer from there. The contract also states that the reference date, the base and the quote identify a row, and that a rate is never summed and is averaged only within one currency pair. The tests hold the table the module writes to the contract, with and without a rebase. [`data/product_exchangerate.yaml`](../data/product_exchangerate.yaml) describes the module as a data product whose one output port is this table. See [`fincommons.contracts`](../contracts.py).

```python
from fincommons.contracts import load_contract, spark_schema
contract = load_contract("fincommons.exchangerate.silver.exchangerate")
spark_schema(contract)
```

The contract also states the rules the table is held to: one rate per day and pair, no currency quoted in itself, ISO 4217 codes, every rate stated and greater than zero, and, as a warning, no rate carried more than a week after its quote. `check_quality` runs them on the table and says, for each, what it counted and whether it passed; see [Check the table against its contract](#check-the-table-against-its-contract).

### Semantic layer on Databricks

On Databricks the contract also serves dashboards and assistants, through the semantic layer that [`fincommons.semantic`](../semantic.py) generates from it:

* a metric view beside the table, whose measure `exchange_rate` gives the rate of one currency pair and is empty for a group holding several;
* the descriptions of the contract as comments on the table and its columns;
* instructions for an assistant such as Genie, from the `context` of `data/product_exchangerate.yaml` -- how to compute a cross rate, what not to do, and example questions.

The Silver layer has to be addressed by catalogue name for this.

```python
from fincommons import semantic
generated = semantic.databricks("paths.yaml", "fincommons.exchangerate")  # text, anywhere
instructions = semantic.apply_databricks(spark, "paths.yaml", "fincommons.exchangerate")  # on Databricks
```

### Tests

The module is covered by the tests in [`tests/test_exchangerate.py`](../../../tests/test_exchangerate.py), which run against the local Spark session built by `tests/conftest.py`. Nothing in them reaches the ECB or the BIS: the Bronze fixtures are written inline and the download is given an archive of its own, so what is asserted is what the module does with a file rather than what those two happen to be publishing.

| Group | Covers |
| --- | --- |
| the data lake | creating the directory tree, including below a directory that does not exist yet, and repeating the call |
| the download | that an archive is fetched and unpacked without the data lake ever seeing it, that the CSV reaches parquet unchanged, that the BIS file keeps only the columns the Silver layer reads, that a write that dies leaves the last good file, and that the connection is closed when the archive is broken |
| the Silver layer | unpivoting the ECB table, dropping quotes a currency does not have, the precedence of the sources, filling the days between two quotes for a week at most, each source counting up to its own last day and the table ending on the BIS's last day where the BIS is used, a stable choice between two rows of one source, rebasing, the storage format and that the table replaces itself |
| `convert2ccy` | the conversion itself, the target currency passing through at a rate of 1, an unavailable currency yielding nothing, dates given as text, dates or timestamps and with a time of day, an unreadable date, a date column of any name, pandas and polars frames coming back as their own kind in their own order, a missing session, a frame converted into the target before being refused, and the columns passed in coming back unchanged |

Some of these are worth naming, because they are pinned to rules that are easy to undo by accident rather than to behaviour that would obviously break:

* `test_the_internal_source_takes_precedence_over_the_ecb` holds the order of the sources. The precedence rests on an explicit rank rather than on the order in which the rows of the union arrive, which is not specified.
* `test_the_gap_between_two_sources_is_closed` holds the fill running along the time series of the currency pair rather than of the individual source. Filling each source on its own leaves a hole wherever the next quote of a pair comes from a different source, and an amount dated in that hole converts to nothing.
* `test_the_days_between_two_quotes_carry_the_earlier_rate` and `test_a_rate_is_carried_for_a_week_at_most` hold the two halves of the rule described under [Days without a quote](#days-without-a-quote).
* `test_the_columns_passed_in_come_back_unchanged_with_two_added` holds `convert2ccy` leaving the columns of its argument alone, which callers rely on when they go on using the table they converted a column of.

Run them, from the root of the repository, with

```bash
pytest tests/test_exchangerate.py
```

## Troubleshooting

* **A converted amount is empty** → the currency is missing; the table does not cover it on that day (not quoted, quoted only after that day, more than `MAX_FILL_DAYS` since its last quote, or dated after its last quote); or the date cannot be read → check the row against `connect_to_exchangerate`; add the BIS or a proprietary rate for currencies the ECB lacks.
* **`ValueError: The exchange rate table holds no rate quoted against ...`** → the table is not based on the `target` of `convert2ccy` → rebuild it with `rebase = <target>`, or convert into the currency it is based on.
* **`ValueError: The frame already has the column(s) ...`** → the frame has been converted into the same target before → drop `<amount>_FXRATE` and `<amount>_<target>` first.
* **`RuntimeError` from `convert2ccy`** → a pandas or polars frame and no active Spark session → pass `spark = ...`.
* **`TypeError` from `convert2ccy`** → the frame is not a Spark, pandas or polars data frame.
* **`ValueError: The internal exchange rates in ... cannot be combined`** → the proprietary file breaks one of the rules under [The proprietary file](#the-proprietary-file) → fix the rows the message names.
* **`ValueError: Cannot rebase to ...`** → the ECB does not quote that currency, or it is `EUR` → choose an ECB currency, or leave `rebase` out for the euro.
* **`ValueError: Cannot combine the BIS rates`** → the BIS file in the Bronze layer holds no daily average (an empty or changed download) → download it again with `update_bronze_bis_exchangerate`.
* **An update refuses the storage format** → the table exists in the other format, or parquet was asked of a catalogue table → leave `file_format` out, or delete the directory before the first update in the new format.
* **The Bronze files are not found after an upgrade** → an older lake spells them with hyphens (`ecb-exchangerate.parquet`) → run `ex.migrate_folder_names(config_path)` once.
* **A download fails with a timeout or an HTTP error** → the server stalled beyond `DOWNLOAD_TIMEOUT` or returned an error page, which is refused rather than written → the last good Bronze file is kept; run again later.
