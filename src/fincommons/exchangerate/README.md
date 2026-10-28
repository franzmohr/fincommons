# Update the exchange rate repository

Under EMIR and SFTR, some fields are reported in a non-domestic currency units and, thus, need to be converted to a single currency to make monetary units comparable. There are multiple sources for exchange rate data with different coverage. For the generation of the ASR on European derivatives markets, ESMA uses reference rates provided by the ECB as a main data source, which is augmented using data from further sources.

## Introduction

This document gives an overview of the functionality of module `exchangerate`.


```python
# Set path to configuration file, where paths are specified
config_path = "paths.yaml"

# Load the module
from fincommons import exchangerate as ex

# Create folders for exchange rate data, unless they already exist
ex.create_directory_tree(config_path = config_path)
```

Start the Spark session


The Silver layer is written as parquet unless `file_format = 'delta'` asks for
Delta or the configuration file addresses it by catalogue name (see below).
Where it is Delta, the session has to be built with the Delta extensions
off-cluster, which needs the `delta-spark` package that comes with the `local`
extra; on Databricks Delta is built in and the extra configuration is
unnecessary. A session built this way reads a parquet table just as well, so it
is the safe thing to start from either way.

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


## From source to bronze layer

### Public data from BIS


```python
# Download BIS data and store it in bronze layer folder
ex.update_bronze_bis_exchangerate(config_path)
```

### Public data from ECB


```python
# Download ECB data and store it in bronze layer folder
ex.update_bronze_ecb_exchangerate(config_path)
```

### Proprietary data

Due to the heterogeneity of the data source, the module does not provide any functionality to download data from proprietary sources. It picks such data up from

```
{path to Bronze Layer of exchangerate as specified in config_path}/internal/exchangerate.csv
```

if that file exists, and expects a comma-separated file with a header row and the following columns:

| Column | Type | Content |
| --- | --- | --- |
| `REFDATE` | string | The reference date of the quote, as `YYYY-MM-DD`. |
| `BASE` | string | The ISO-3 code of the base currency. This should be `EUR`, because the rest of the pipeline is built around euro quotes. |
| `QUOTE` | string | The ISO-3 code of the quoted currency. |
| `VALUE` | double | Units of `QUOTE` per one unit of `BASE`. |
| `TYPE` | string | The kind of rate, for instance `ref` for a reference rate. |
| `SOURCE` | string | A label stating where the quote came from. It is carried through to the Silver layer. |

The file is checked before it is combined, and refused as a whole where a row would be misread: a `REFDATE` not written as `YYYY-MM-DD`, a `BASE` other than `EUR`, a `VALUE` that is missing or not positive, or one day and pair quoted twice. The first matters most, because the combination compares dates as text: a date written `2020-10-5` is not the same day as the ECB's `2020-10-05` to it, and one such row could cut the whole table short.

## From bronze to silver layer

Function `update_silver_exchangerate` updates the exchange rate data set. It takes the following arguments:
* `spark`: The open Spark session.
* `config_path`: The path to the configuration file, where the paths of the data lake are specified.
* `use_bis`: A boolean indicating if exchange rate data from the website of the Bank for International Settlements should be used. The default specification of this argument is `False` since the data is not updated daily.
* `rebase`: An ISO-3 currency code indicating into which FX rate the exchange rate data set should be converted, for organisations where the euro is not the domestic currency. The SFTR, EMIR and MiFIR stages and `convert2ccy` read the rates quoted against the currency they convert into, so any target other than the euro needs the table rebased to it. SFTR, EMIR and `convert2ccy` refuse to run against a table that is not; MiFIR warns and leaves the volumes it cannot convert empty. A `rebase` to a currency the ECB reference rates do not quote, or to `EUR` itself, is refused before anything is written, and the existing table is left as it was.

During the update process, the function logs the latest date in each data set for which observations are available. Every source counts up to its own last date: where a source has fallen behind, the others describe the days it does not reach, as they do on any day it is silent, and the log shows which source is behind.

* `file_format`: The format the table is stored in, `'parquet'` or `'delta'`. Where it is left out, the configuration file decides: a Silver layer addressed by path is written as parquet, because it is the format every engine reads without a further dependency, and a table that exists already keeps the format it is stored in.

The resulting data set will be stored in `{path to Silver Layer of exchangerate as specified in config_path}/exchangerate`. Each update replaces the table rather than adding to it.

The Silver layer may instead be addressed by catalogue name, which puts the table within reach of Unity Catalog's grants and lineage. The configuration file alone says so; no argument of the update has to change:

```yaml
exchangerate:
    bronze: /Volumes/fincommons/exchangerate/lake/bronze
    silver:
        table: fincommons.exchangerate
```

The table is then `fincommons.exchangerate.exchangerate`, and it is always a Delta table, because that is what a Unity Catalog managed table is. Asking for `file_format = 'parquet'` there is refused before anything is read.

Delta is worth asking for where the table may be queried while it is rebuilt: a Delta update replaces the table in a single transaction, so a reader sees either the previous version in full or the new one, never a half-written file, and the previous versions remain readable. A parquet update deletes the table before it writes the new one, which leaves a window in which a reader finds it missing or half-written.

`connect_to_exchangerate` and `convert2ccy` read the table in whichever form it is addressed and stored in, so the choice does not have to be repeated when the table is queried. A table that exists already keeps its format: an update that asks for the other one is refused before anything is written, because Delta will not adopt parquet files it has no transaction log for, and parquet written over a Delta table would throw its history away. Delete the directory before the first update in the new format to change it. The storage itself is shared with the other modules and described in `fincommons.storage`.

### How the sources are combined

Where more than one source quotes the same currency pair on the same day, the proprietary data takes precedence over the ECB, and the ECB over the BIS. Only the winning quote reaches the Silver layer, and the `SOURCE` column of the result states which source it came from.

### Days without a quote

The sources quote on business days, but an amount agreed on a Saturday still has to be converted, so the Silver layer holds one row per calendar day. Every day carries the last rate quoted on or before it, whichever source that quote came from, for at most `MAX_FILL_DAYS`, seven days, after that quote. A week covers weekends and the TARGET closing days; a pair that goes unquoted for longer has no rate on the days beyond, rather than one that may be years old. The ECB, for instance, quoted no Icelandic krona from December 2008 to February 2018.

The fill stops at the last quote of a pair: the last quoted day is present once and is not carried forward beyond itself. A currency that stops being quoted therefore ends with its last observation, and an amount dated after that gets no rate. Days before the first quote of a pair are not filled either.

The column `IMPUTED` says which rows are filled: it is `True` on a day whose rate was carried from the last quote before it, and `False` on a day a source quoted. Wherever the ECB is the source, those are the weekends and the TARGET closing days, on which it does not publish; any other day without a quote is marked as well, whatever the reason for it. A rebased cross rate is imputed where either of the two euro rates it is computed from is.

### Application for euro area countries

For countries with the euro as their domestic currency, the command `update_silver_exchangerate` can be executed without any further specification:


```python
ex.update_silver_exchangerate(spark, config_path, use_bis = False)
```
                                                                                

### Application for non-EA countries

For countries where the domestic currency is not the euro, the argument `rebase` can be specified with the desired currency code. The function then re-calculates the whole combined data set, proprietary and BIS quotes included, against the currency named, and adds the euro itself as a quoted currency.


```python
ex.update_silver_exchangerate(spark, config_path, rebase = "SEK")
```
                                                                                  

## All-in-one function

The following all-in-one function can be used to update the entire exchange rate data set. If proprietary data is provided in the folder described under *Proprietary data* above, it will try to use this data to augment the ECB data set.

**Note that you should update your internal exchange rate data set prior to running the following function.**


```python
ex.update_exchangerate(spark, config_path, use_bis = False)
```

It passes `rebase`, `use_bis` and `file_format` on to `update_silver_exchangerate`, so the storage format is chosen here as well:

```python
ex.update_exchangerate(spark, config_path, file_format = "delta")
```
                                                                                  

## The `convert2ccy` function

This convenience function for data scientists allows to convert data of any currency into a specified target currency - as long as the data is available in the exchange rate data set.

It takes a Spark, pandas or polars data frame and returns the same kind of frame. The conversion itself is done with Spark: a Spark frame is converted with its own session, and a pandas or polars frame is handed to the active session, which is the one a Databricks notebook already provides and the one created above off-cluster; where there is none, pass it with `spark = ...`. A pandas or polars frame comes back in its own row order, and a pandas frame with its own index. A Delta table is converted by reading it as a Spark frame first, with `spark.read.table(name)` or `spark.read.format("delta").load(path)`.

**Example**

First, create an artificial data set


```python
# Create the rows: date, currency and amount
rows = [("2020-01-01", "USD", 1.0), ("2022-03-01", "RUB", 1.0), ("2022-03-02", "RUB", 1.0),
        ("2020-01-01", None, 1.0), ("2020-01-01", "XAU", 1.0), ("2020-01-01", "EUR", 1.0)]

# Make a Spark data frame
data = spark.createDataFrame(rows, "date string, currency string, value double")

# Show result
data.show()
```

Second, use `convert2ccy` to convert the data in a column into the target currency, here into euros.


```python
ex.convert2ccy(data, config_path, date = 'date', amount = 'value', currency = 'currency', target = 'EUR').show()
```

The function returns the data frame with two columns added, named after the column that was converted. With `amount = 'value'` and `target = 'EUR'` as above, these are

* `value_FXRATE`: the rate that was applied, and
* `value_EUR`: the converted amount.

The date column may hold dates, timestamps or text such as `2020-01-01`. An amount is converted at the rate of its calendar day, so a time of day does not matter; a date that cannot be read gets no rate. Rows whose currency already is the target currency are converted at a rate of 1. Rows whose currency is missing, and rows whose currency the exchange rate data set does not cover on that date, get neither a rate nor a converted amount. The columns that are passed in come back unchanged.

## Tests

The module is covered by the tests in `tests/test_exchangerate.py`, which run
against the local Spark session built by `tests/conftest.py`. Nothing in them
reaches the ECB or the BIS: the Bronze fixtures are written inline and the
download is given an archive of its own, so what is asserted is what the module
does with a file rather than what those two happen to be publishing.

| Group | Covers |
| --- | --- |
| the data lake | creating the directory tree, including below a directory that does not exist yet, and repeating the call |
| the download | that an archive is fetched and unpacked without the data lake ever seeing it, that the CSV reaches parquet unchanged, that the BIS file keeps only the columns the Silver layer reads, that a write that dies leaves the last good file, and that the connection is closed when the archive is broken |
| the Silver layer | unpivoting the ECB table, dropping quotes a currency does not have, the precedence of the sources, filling the days between two quotes for a week at most, each source counting up to its own last day, a stable choice between two rows of one source, rebasing, the storage format and that the table replaces itself |
| `convert2ccy` | the conversion itself, the target currency passing through at a rate of 1, an unavailable currency yielding nothing, dates given as text, dates or timestamps and with a time of day, an unreadable date, a date column of any name, pandas and polars frames coming back as their own kind in their own order, a missing session, a frame converted into the target before being refused, and the columns passed in coming back unchanged |

Some of these are worth naming, because they are pinned to rules that are easy
to undo by accident rather than to behaviour that would obviously break:

* `test_the_internal_source_takes_precedence_over_the_ecb` holds the order of
  the sources. The precedence rests on an explicit rank rather than on the
  order in which the rows of the union arrive, which is not specified.
* `test_the_gap_between_two_sources_is_closed` holds the fill running along the
  time series of the currency pair rather than of the individual source. Filling
  each source on its own leaves a hole wherever the next quote of a pair comes
  from a different source, and an amount dated in that hole converts to nothing.
* `test_the_days_between_two_quotes_carry_the_earlier_rate` and
  `test_a_rate_is_carried_for_a_week_at_most` hold the two halves of the rule
  described under *Days without a quote*.
* `test_the_columns_passed_in_come_back_unchanged_with_two_added` holds
  `convert2ccy` leaving the columns of its argument alone, which callers rely
  on when they go on using the table they converted a column of.

Run them, from the root of the repository, with

```
pytest tests/test_exchangerate.py
```
