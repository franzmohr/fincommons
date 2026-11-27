# The Kenneth R. French Data Library

Module `famafrenchdata` brings the [Kenneth R. French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html) into the data lake as one table: the factors of the Fama-French three- and five-factor models, momentum and the reversal factors, the returns of the portfolios they are built from -- sorted on size, book-to-market, profitability, investment, prior returns, industries and more -- and the NYSE breakpoints of the sorts, for the US since 1926 and for the international regions since 1990. Use it when you need a factor or a portfolio return series in an analysis -- the monthly market, size and value factors, say -- without writing a parser for one more zipped CSV file.

At a glance:

* **Source:** the library's web page, read on every run; some three hundred zipped CSV files, about 150 MB zipped. No data is shipped with fincommons, and the files are copyright Fama and French (see [Licence and citation](#licence-and-citation)).
* **Update frequency:** Kenneth French rebuilds every file once a month as CRSP is updated; a run on any other day usually downloads nothing.
* **Tables written:** the Bronze layer (the raw text of each file, line by line) and one Silver table (cleaned, read by analyses), `famafrenchdata`, one row per file, table, series and period.
* **Key:** `dataset`, `table`, `frequency`, `series`, `ref_date`.
* **Needs:** network access to the library, no credentials, and a Spark session for the update (the Silver layer parses the files on the Spark workers).

## Quick start

1. Give the configuration file (`paths.yaml`) a `famafrenchdata` section with the keys `bronze` and `silver`:

```yaml
famafrenchdata:
    bronze: /data/famafrenchdata/bronze
    silver: /data/famafrenchdata/silver
```

2. Start Spark, collect two files and read one of them back in the wide shape it was published in:

```python
import os
import sys

# The Silver layer parses the files on the Spark workers, which are launched
# with `python3` from the PATH unless told otherwise.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (SparkSession.builder.master("local[4]")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

from fincommons import famafrenchdata as ffd

config_path = "paths.yaml"

# Sets up the directories, collects the two files into the Bronze layer and
# reads them into the Silver table. Leave `datasets` out to collect the whole
# library, about 150 MB zipped.
outcome = ffd.update_famafrenchdata(
    spark, config_path,
    datasets=["F-F_Research_Data_Factors", "6_Portfolios_2x3"])
assert outcome["failed"] == 0

df = ffd.connect_to_famafrenchdata(spark, config_path)

# Mkt-RF, SMB, HML and RF by month, in percent, indexed by the last day of
# the month.
factors = ffd.series(df, "F-F_Research_Data_Factors", "FACTORS", "M")
print(factors.tail())
```

No module of the package opens a Spark session: the session is the caller's. A fuller session, with memory and a scratch directory, is in [Running it on a schedule](#running-it-on-a-schedule).

Built from the whole 202609 edition on four cores, the Silver table took about two and a half minutes. The Bronze layer then takes about 320 MB as parquet and the Silver table about 230 MB.

## What you get

### The Silver table `famafrenchdata`

One row per file, table, series and period, partitioned by `dataset`. Filter on `dataset` first: the table is partitioned by it.

| Column | Meaning |
| --- | --- |
| `ref_date` | the date of the period: the day for daily and weekly rows (a weekly row is keyed by the last day of its week), the last day of the month for monthly rows, 31 December for annual rows |
| `dataset` | the file, e.g. `F-F_Research_Data_Factors`, `6_Portfolios_2x3`: its name as the page links it, without `_CSV.zip` |
| `table` | the kind of table within the file, a code of the [vocabulary of tables](#the-vocabulary-of-tables) |
| `frequency` | `D`, `W`, `M` or `A` |
| `series` | the column as the file's header names it, trimmed: `Mkt-RF`, `SMALL LoBM`, `NoDur` |
| `value` | the value as published: returns and factors in percent, numbers of firms, average size in millions of dollars, ratios as the table states them |
| `title` | the title of the table as published, whitespace collapsed, for traceability; empty for the untitled first table of a factor file and for breakpoints |

What a reader may rely on:

* **No column is ever null.** The library writes a missing value as `-99.99` or `-999`; such a value is left out of the table rather than written as a null, so a series has a row for the periods it has a value for.
* **One value per key** -- the file, the table, the frequency, the series and the reference date.
* **Dates are period ends:** a monthly row is dated by the last day of its month, an annual row by 31 December.
* **An annual table of the characteristics of portfolios** -- book-to-market, earnings-to-price, the breakpoints -- **is keyed by the year the portfolios were formed.** The year in progress therefore already has its row, dated 31 December, months after the last return of the file.
* `value` holds returns, counts of firms, sizes and ratios in one column, so the data contract declares it no measure: what it holds is said by the table it stands in.
* `connect_to_famafrenchdata` returns the columns in the order of the declared schema (above), whether the table is Parquet, which puts the partition column `dataset` last when it is read, or Delta.

On the whole library as collected on 27 November 2026, 297 files and 86,029,613 values, every rule held. The US files are built from CRSP and Compustat, the international files from Bloomberg; each file says which vintage in its first line ("This file was created using the 202609 CRSP database."), which the Bronze layer keeps.

The table holds no LEI, ISIN or other identifier of an entity or instrument, so it does not join to other modules by key; it is joined to other series by date.

### The vocabulary of tables

The titles of the tables are prose and vary from file to file; 57 distinct titles occur in the 202609 edition. Each is mapped onto a code in `table`:

* The frequency is not part of the code: the monthly and the annual returns of a file are both `RET_VW`, told apart by `frequency`.
* Nor is the treatment of dividends, which the library publishes as files of their own (`..._Wout_Div`), told apart by `dataset`.

| `table` | What it holds | Published as, e.g. |
| --- | --- | --- |
| `FACTORS` | the factor returns of a factor file, in percent | the untitled first table; `Annual Factors: January-December` |
| `BREAKPOINTS` | the breakpoints of a sort: counts of firms and percentiles | the `*_Breakpoints` files |
| `RET_VW` | value-weighted returns of the portfolios, in percent | `Average Value Weighted Returns -- Monthly`, `Value Weight Returns -- Annual from January to December` |
| `RET_EW` | equal-weighted returns of the portfolios, in percent | `Average Equal Weighted Returns -- Daily` |
| `NFIRMS` | the number of firms in each portfolio | `Number of Firms in Portfolios` |
| `AVG_SIZE` | the average market capitalisation, in millions of dollars | `Average Firm Size`, `Average Market Cap` |
| `BEME_SUM`, `EP_SUM`, `CFP_SUM` | the sum of book equity, earnings or cash flow over the sum of market equity | `Sum of BE / Sum of ME` |
| `BEME_VW` | value-weighted average of BE/ME, ME at the end of December of the prior year | `Value-Weighted Average of BE/ME`, `... Value Weight Average of BE/ME ... / ME(Dec t-1)] ...` |
| `BEME_JUN_VW` | value-weighted average of BE/ME, ME at the end of June of the year of formation | `... Value Weight Average of BE_FYt-1/ME_June t ...` |
| `OP_VW`, `INV_VW` | value-weighted average of operating profitability, of investment | `Value Weight Average of OP`, `... of investment (rate of growth of assets) ...` |
| `EP_VW`, `CFP_VW`, `DP_VW` | value-weighted average of earnings, cash flow, dividends to price | `Value Weight Average of E/P when portfolio is formed` |
| `AC_VW`, `NI_VW` | value-weighted average of accruals, of net share issues | `Value-Weighted Average of ACCR` |
| `BETA_VW`, `VAR_VW` | value-weighted average of prior beta, of past return variance | `Value-Weighted Average of Prior Beta` |
| `PRIOR_VW`, `PRIOR_EW` | value- and equal-weighted average of the prior return the portfolios are formed on | `Value-Weighted Average of Prior Returns` |

The series of a `BREAKPOINTS` table are named by the module, since the files do not name them: `N` or the counts the header names (`<= 0`, `>0` for the breakpoints of a ratio), followed by the twenty NYSE percentiles `P05` to `P100`.

Two cases fall outside the vocabulary, and neither loses a table:

* **A title no rule matches** is filed under a code made from the title -- `Median Dividend Yield -- Monthly` becomes `MEDIAN_DIVIDEND_YIELD_MONTHLY` -- and a `WARNING` names it on the driver. A table the library adds is thus in the table under a name that says what it is until a rule is written for it.
* **Two tables of one file that land on the same code and frequency** are numbered, the later one `RET_VW_2`, with a `WARNING`. It happens once in the 202609 edition: `6_Portfolios_2x3_weekly` titles both of its first two tables `Average Value Weighted Returns -- Weekly`, where the daily file of the same portfolios has value- and then equal-weighted returns. Which of the two the second one is, the file does not say, so it is not guessed here.

The vocabulary and its rules are `TABLE_CODES` and `TABLE_RULES` in `fincommons.famafrenchdata.parse`, and a test checks every one of the 57 published titles against them.

### The Bronze layer

The text of every file exactly as published, one row per line: `LINENO`, `LINE`, `LASTMODIFIED` (the server's `Last-Modified`, UTC) and `COLLECTDT`, partitioned by `DATASET`, the name of the file. Nothing is dropped -- the preamble says which vintage of CRSP the file was built from, how its portfolios are formed and which value stands for a missing one, and the notes between tables say what the tables are. Its layout is described under [How it works](#the-bronze-layer-on-disk).

## Common tasks

### Get the monthly three-factor table, wide as published

`series(df, dataset, table, frequency, columns=None)` brings one table of one file back to the driver as a pandas frame: indexed by `ref_date` as a `DatetimeIndex`, in order of time, a column per series, `NaN` where a series has no value for a period. A Spark frame is filtered before anything is collected, so only the rows asked for leave Spark.

```python
df = ffd.connect_to_famafrenchdata(spark, "paths.yaml")
factors = ffd.series(df, "F-F_Research_Data_Factors", "FACTORS", "M",
                     columns=["Mkt-RF", "SMB", "HML", "RF"])
```

Without `columns` the series come in alphabetical order, as the table does not record the order of the columns of the file. The annual factors of the same file are `frequency="A"`. `series` also accepts rows of the table already in pandas.

### List which files and tables exist

The files the lake holds, and the tables and frequencies within each:

```python
(df.select("dataset", "table", "frequency")
   .distinct()
   .orderBy("dataset", "table", "frequency")
   .show(200, truncate=False))

# The series of one table.
(df.filter("dataset = '6_Portfolios_2x3' AND table = 'RET_VW' AND frequency = 'M'")
   .select("series").distinct().show())
```

The files the library itself offers right now, by name, with their addresses (this reads the library's page):

```python
offered = ffd.library_index()
print(len(offered), list(offered)[:10])
```

### Collect more files, or the whole library

```python
# A few more files...
ffd.update_famafrenchdata(spark, "paths.yaml",
                          datasets=["F-F_Research_Data_5_Factors_2x3", "F-F_Momentum_Factor"])

# ...or every file the page links.
ffd.update_famafrenchdata(spark, "paths.yaml")
```

A name the page does not link is counted as failed, since it is a file that was asked for and not collected.

### Rebuild HML and SMB from the six portfolios

This rebuilds two of the published factors from the portfolios they are made of, using the two files of the [Quick start](#quick-start):

```python
# The monthly value-weighted returns of the six portfolios formed on size
# and book-to-market, and the published factors, each as the file shows them:
# one column per series, indexed by the last day of the month.
portfolios = ffd.series(df, "6_Portfolios_2x3", "RET_VW", "M")
factors = ffd.series(df, "F-F_Research_Data_Factors", "FACTORS", "M")

# HML: value minus growth, the average of the two high book-to-market
# portfolios minus the average of the two low ones.
hml = (portfolios["SMALL HiBM"] + portfolios["BIG HiBM"]) / 2 \
    - (portfolios["SMALL LoBM"] + portfolios["BIG LoBM"]) / 2

# SMB: small minus big, the average of the three small portfolios minus the
# average of the three big ones.
smb = (portfolios["SMALL LoBM"] + portfolios["ME1 BM2"] + portfolios["SMALL HiBM"]) / 3 \
    - (portfolios["BIG LoBM"] + portfolios["ME2 BM2"] + portfolios["BIG HiBM"]) / 3

print((factors["HML"] - hml).abs().max(), factors["HML"].corr(hml))
print((factors["SMB"] - smb).abs().max(), factors["SMB"].corr(smb))
```

**Result:** on the 202609 edition of the library, over all 1,203 months from July 1926 to September 2026, both rebuilt series match the published factors to within 0.005 percentage points, and their correlation is 1.0. That is the rounding of the published factors, which have two decimals where the portfolio returns have four: the rebuilt factor is the published one before it was rounded.

What this shows, and what it does not:

* **It shows** that the published files are consistent with one another -- the factor file is the six-portfolio file combined as the definitions say -- and that the module reads both correctly: the right table, the right series under the right name, every month on the right date. A reader that took the equal-weighted table for the value-weighted one, shifted a month or dropped a portfolio would miss by far more than the rounding.
* **It does not** reconstruct the factors from their sources. The portfolios themselves are built by French from CRSP and Compustat -- the NYSE breakpoints, book equity, the June rebalancing -- and fincommons has neither database. Whether the portfolios are right is a question the library answers, not this check.

The same comparison runs in the test suite against synthetic files (`tests/test_famafrenchdata_silver.py`), and against the library itself where `FINCOMMONS_NETWORK=1` is set.

### Produce the global factor factsheet

`templates/famafrench-global-factsheet.qmd` is a factsheet on what the factors have done across the world, written in R and rendered with Quarto to a PDF of four pages. It reads the Silver table and nothing else, so collect the whole library first (it needs the five-factor and momentum files of every region, monthly and daily). Then:

```bash
cd src/fincommons/famafrenchdata/templates
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render famafrench-global-factsheet.qmd
```

| Environment variable | What it sets |
| --- | --- |
| `FINCOMMONS_CONFIG` | the configuration file; a `paths.yaml` beside the template where unset |
| `FINCOMMONS_MONTH` | the last month reported on, as `YYYY-MM`, e.g. `2025-06`; the last month the library holds for every region where unset |
| `FINCOMMONS_REGIONS` | the regions, by the names the files give them, separated by commas, e.g. `US,Europe`; all eight where unset |

What is on it is described under [How it works](#the-global-factor-factsheet).

### Read one downloaded file without Spark

The reader is plain Python, so a file downloaded by hand can be read on its own into the long records of the table (without the `dataset` column):

```python
from fincommons.famafrenchdata import parse_lines, read_archive, split_lines

text = read_archive("F-F_Research_Data_Factors_CSV.zip", "F-F_Research_Data_Factors")
frame = parse_lines("F-F_Research_Data_Factors", split_lines(text))
```

### Rebuild the whole table after an upgrade of fincommons

`full=True` rebuilds every partition of the table from the Bronze layer, whether its file changed or not. A scheduled run never needs it; it is how the table follows a new version of the reader.

```python
ffd.update_famafrenchdata(spark, "paths.yaml", full=True)
```

## Running it on a schedule

The run needs no argument saying what to collect, so a scheduler makes the same call every day:

```python
ffd.update_famafrenchdata(spark, config_path)
```

On the first day it collects the whole library; on most days after it downloads nothing; in the days after French's monthly update it downloads everything.

No module of the package opens a Spark session, so a job scheduler runs a script of a few lines rather than the module itself. The session belongs to whoever starts the run -- a cluster has one already, a workstation has to say how much of itself to give it -- and a pipeline that opened its own would be deciding that for every caller:

```python
# update-famafrenchdata.py -- the script a job scheduler runs
# Spark launches its Python workers with whatever `python3` resolves to on
# the PATH, which need not be the interpreter running this script. This has
# to be settled before the session is built.
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from fincommons import famafrenchdata as ffd
from fincommons.log import configure

# The log goes to standard error. The script writes nothing to standard
# output: what it did is the log, and whether it did all of it is the exit
# code.
configure("INFO")

builder = (SparkSession.builder
           .master("local[4]")
           .appName("fincommons-famafrenchdata")
           .config("spark.driver.memory", "4g")
           .config("spark.local.dir", "/data/spark-tmp")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    outcome = ffd.update_famafrenchdata(spark, "paths.yaml")
finally:
    spark.stop()

# A file set aside is not a failure of the run, which is why the library
# returns rather than raises for it -- but it is a job that did less than it
# was scheduled to do, and a scheduler only sees the exit code.
sys.exit(1 if outcome["failed"] else 0)
```

`spark.local.dir` is named because Spark's own default is `/tmp`, which on a good many Linux installations is a tmpfs sized at half the machine's memory rather than a directory on a disk: a shuffle that outgrows it ends the run in `No space left on device` while the disk the lake sits on is all but empty. [Setting up a Spark session](../../../docs/spark-session.md) says more about that, and about choosing the cores and the heap together; the handbook's Infrastructure chapter gives the reasons.

What a run does, in order:

1. Sets up the directory tree (`create_directory_tree`; directories that exist are left alone).
2. Collects into the Bronze layer every file of the library that has changed (`update_bronze_famafrenchdata`).
3. Rebuilds the partitions of the Silver table whose file the Bronze layer now holds a newer collection of (`update_silver_famafrenchdata`).

`update_famafrenchdata(spark, config_path, full=False, file_format=None, datasets=None, url=LIBRARY_URL)`:

* `datasets` restricts the run to named files, e.g. `["F-F_Research_Data_Factors"]`.
* `full=True` rebuilds every partition of the table from the Bronze layer.
* `file_format` is `'parquet'` or `'delta'`; see [Configuration](#configuration).
* `url` is the page of the library.

It returns the outcome of the run: `failed` is the number of files set aside in the two layers together, and `failed_sources` names them as `bronze/<name>` or `silver/<name>`.

**When a file fails, the others carry on.** A download cut off, an answer that is not a zip archive, a file whose layout the reader does not know: the file is logged with its traceback at `ERROR`, counted in `failed` and named in `failed_sources`, and a closing `WARNING` counts them. The file keeps what the lake held of it before, in both layers. The run is idempotent, so the next run simply tries the file again: its collection record and its Silver mark are written only after its data, so a file that failed is still seen as pending.

## How it works

### Collecting the files

**The run reads the library's page for the files it offers and downloads only those that changed.**

1. The list of files is not kept in the package, because the library adds and renames files without notice; the page is read on every run (`library_index`).
2. For each file the server is asked, with a `HEAD` request, when it was last modified and what its entity tag is (`probe`). That costs a few hundred bytes where the file costs up to sixteen megabytes.
3. A file whose tag is the one kept from the last collection is not downloaded. Where there is no tag, the time of the last modification decides.
4. Where the server does not answer, the file is downloaded and its text compared with the one kept, by its SHA-256, so an unchanged file is never written again.

Each file is collected and read on its own, so one that fails does not stop the others.

### The Bronze layer on disk

```
bronze/
    files/
        DATASET=F-F_Research_Data_Factors/part-00000.parquet
        DATASET=6_Portfolios_2x3/part-00000.parquet
        ...
    collections/
        F-F_Research_Data_Factors.json
        ...
```

* `files/` holds the text of each file, one row per line, as described under [What you get](#the-bronze-layer). A new collection of a file replaces its partition.
* `collections/<DATASET>.json` records the address (`url`), the `Last-Modified` and `ETag` the server sent, the SHA-256 of the text, the number of lines and when it was collected.
* The archives are downloaded into a temporary directory on the driver, not into the lake, and only the parquet file reaches the Bronze layer.

### Rebuilding the Silver table

**The table is rebuilt file by file: each file whose Bronze collection the table was not built from is read again and replaces its partition, and the other partitions are left as they are.**

* A mark per file, `silver/_watermarks/<DATASET>.json`, records the SHA-256 of the text the partition was built from. It is written after the partition is in place, so a run that dies leaves the mark behind and the next run builds the file again.
* The reading runs on the executors, one file per task, so the hundred million values of the library never pass through the driver. Files are parsed and written in batches of at most `BATCH_SIZE` (50) files and `BATCH_LINES` (400,000) lines.
* As Delta, a rebuild replaces its partitions in one transaction; as parquet, a reader of a partition being rewritten may find it missing for a moment.

Built from the whole 202609 edition on four cores, the table took about two and a half minutes and holds 86,029,613 rows.

### How a file is read

**A file is read for its shape rather than from a description of each file.**

* A line that begins with a comma is the header of a table.
* The lines of text directly above it are its title.
* The rows below it are keyed by a year (`2025`), a month (`202509`) or a day (`20250930`).
* The preamble, the notes and the copyright line are passed over.

The breakpoint files are the exception that proves the rule: they have no header, or one that names only the counts, and their columns are named here (see [the vocabulary of tables](#the-vocabulary-of-tables)).

Run over all 297 files of the 202609 edition, the reader gives 86,029,613 values, and its run turned up the irregularities it now handles:

* a title written over two lines (`Annual Factors:` / `January-December` in the momentum file);
* a carriage return alone at the start of a header row (`6_Portfolios_2x3`) -- so a line ends at a line feed and nowhere else;
* a blank line between a header and its first row (`Portfolios_Formed_on_VAR`);
* a misspelt title (`Aerage Value Weighted Returns`).

A row that holds more values than its header has columns, or a value that is not a number, fails the file rather than putting values under the wrong series.

### The global factor factsheet

**A four-page PDF of the five factors of Fama and French (2015) and momentum in every region of the library.** The regions are the United States, North America, Europe, Japan, Asia Pacific ex Japan, the emerging markets, the developed markets and the developed markets ex US. It reports up to the last month the library holds for all of them, or an earlier one with `FINCOMMONS_MONTH`.

What is on it:

1. Three heatmaps, regions by factors, set them side by side: the twelve-month return coloured by its rank among every twelve-month return before it, the drawdown coloured by its depth against the deepest, and the volatility of the last quarter against its median.
2. A line per region then says what passes the thresholds set at the top of the template -- a twelve-month return in the outer five per cent of its history, a volatility half again its median, a drawdown half as deep as the deepest.
3. The charts follow: the market of every region over the last three years, the five styles likewise, and the spread of book-to-market between the cheapest and the dearest decile.

Every region is held to the same history, since July 1990, when the regional files begin: the files of the United States reach back to 1926, and a rank over a century would not mean what a rank over thirty-six years means in Japan.

Three limits are stated on the page:

* **The library is a month or two behind**, being rebuilt once a month after the month has closed, so the factsheet says what the factors did; what the market is doing today is for the prices of `fincommons.marketpricedata`.
* **The emerging markets have no daily file**, so their volatility is left empty rather than measured another way.
* **The value spread is American**: the library publishes the book-to-market of its portfolios for the United States only, so the factsheet shows it as a reference and says so.

The PDF is a tagged PDF/UA-1 document: its heatmaps are tables whose numbers are text, and every chart has alt text computed from its data. The look comes from the package's corporate design script, `src/fincommons/templates/r-corporate-design-functions-ggplot2.R` (a copy beside the template wins over it), and `tests/test_report_accessibility.py` holds the template to the standard. It needs Quarto and the R packages `ggplot2`, `dplyr`, `tidyr`, `yaml` and `arrow`, and reads the files of the table with Arrow from `<silver>/famafrenchdata` -- from the `path` where the Silver layer is given as a mapping.

### Why one table, kept apart from the market prices

Much of empirical asset pricing rests on these files, and every institution that uses them downloads them, opens a zipped CSV file meant for a person to read, and writes a parser for the file it needs. The library publishes some three hundred such files, and Kenneth French rebuilds all of them each month as CRSP is updated. This module reads every one of them into one table, in one shape, and keeps it up to date.

Because each month is a new edition of the whole history rather than a few new rows, a file's partition is replaced rather than merged into, which is why the library is not part of `fincommons.marketpricedata`.

## Reference

### Configuration

The `famafrenchdata` section of `paths.yaml`:

| Key | What it holds | May be a catalogue name? |
| --- | --- | --- |
| `bronze` | the directory of the Bronze layer (`files/`, `collections/`) | no |
| `silver` | the directory of the Silver table and its marks (`_watermarks/`) | yes, as a mapping of `table` and `path` |

The Silver layer may name a schema in a catalogue instead of a directory, which puts the table within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the marks that record which collection of each file the table was built from are files, and a schema is not a directory to write them to:

```yaml
famafrenchdata:
    bronze: /Volumes/fincommons/famafrenchdata/lake/bronze
    silver:
        table: fincommons.famafrenchdata
        path: /Volumes/fincommons/famafrenchdata/lake/silver
```

The table is then `fincommons.famafrenchdata.famafrenchdata`, always Delta. A table at a path keeps the format it is stored in; a new one takes the `file_format` asked for, or the package's default (parquet) where none is.

### Public functions

All are importable from `fincommons.famafrenchdata`.

Update:

| Function | What it does |
| --- | --- |
| `update_famafrenchdata(spark, config_path, full=False, file_format=None, datasets=None, url=LIBRARY_URL)` | the whole pipeline in one call; returns the outcome |
| `update_bronze_famafrenchdata(config_path, datasets=None, url=LIBRARY_URL)` | collects the changed files into the Bronze layer; outcome counts `collected`, `unchanged`, `failed` |
| `update_silver_famafrenchdata(spark, config_path, full=False, file_format=None, datasets=None)` | rebuilds the partitions of the changed files; outcome counts `written`, `rows`, `failed` |
| `create_directory_tree(config_path)` | creates the directories of a local data lake |

Read:

| Function | What it does |
| --- | --- |
| `connect_to_famafrenchdata(spark, config_path)` | opens the Silver table |
| `series(data, dataset, table, frequency, columns=None)` | one table of one file as a wide pandas frame; raises `LookupError` for a name it does not find |

Collecting (Bronze):

| Function | What it does |
| --- | --- |
| `library_index(url=LIBRARY_URL)` | reads the library's page and returns the files it links, name to address |
| `parse_index(html, base_url=LIBRARY_URL)` | the same, from the page's HTML |
| `probe(url)` | the `Last-Modified` and `ETag` of a file, by a `HEAD` request |
| `collect_dataset(config_path, dataset, url, now=None)` | brings one file up to date in the Bronze layer; `"collected"` or `"unchanged"` |
| `read_archive(path, dataset)` | the text of the one CSV file of a zip archive |
| `read_collection(config_path, dataset)`, `read_collections(config_path)` | the collection records of the Bronze layer |

Reading a file (plain Python, no Spark):

| Function | What it does |
| --- | --- |
| `parse_lines(dataset, lines)` | the values of one file as a pandas frame of `COLUMNS` |
| `read_tables(dataset, lines)` | splits the lines of a file into its tables |
| `decode(raw)`, `split_lines(text)` | the text of a file from its bytes, and its lines |
| `table_code(title, breakpoints=False)`, `normalise_title(lines)` | the code a title is filed under, and the title as one line |
| `frequency_of(period, weekly=False)`, `is_weekly(dataset)`, `is_breakpoints(dataset)`, `breakpoint_series(dataset, published, width)` | frequency and breakpoint helpers |

Silver helpers: `pending_datasets(config_path, full=False, datasets=None)` (the files the table has to be rebuilt for) and `batches(pending, size=None, lines=None)`.

Paths and marks: `path_bronze_base`, `path_bronze_files`, `path_bronze_collections`, `path_bronze_dataset`, `path_silver_base`, `path_silver_famafrenchdata`, `path_silver_watermarks`, `ref_silver_famafrenchdata`, `read_watermark`, `write_watermark`, `write_json`.

Constants: `LIBRARY_URL`, `SOURCE`, `bronze_schema`, `silver_schema`, `COLUMNS`, `MISSING_VALUES` (`-99.99`, `-999.0`), `PERCENTILES` (`P05` to `P100`), `TABLE_CODES`, `TABLE_RULES`, `BATCH_SIZE`, `BATCH_LINES`, `CHUNK_ROWS` (parsed values per frame sent back from the executors, 250,000).

### Data contract and handbook

What the columns of the table hold, and what a reader may rely on, is stated by its data contract in the Open Data Contract Standard, [`data/contract_famafrenchdata_silver_famafrenchdata.yaml`](../data/contract_famafrenchdata_silver_famafrenchdata.yaml), which [`data/product_famafrenchdata.yaml`](../data/product_famafrenchdata.yaml) names as the output port of the module. It is the one place the columns are described -- `fincommons.describe_column("series", dataset="famafrenchdata")` reads its answer from there -- and the declared schema is held to it by the tests. The contract states the key of the table and the rules listed under [What you get](#the-silver-table-famafrenchdata).

The handbook has a chapter on the module, `docs/handbook/01-famafrenchdata.tex`.

### Licence and citation

The files are downloaded from the library when the pipeline runs. **No data of the library is shipped with fincommons**: the package holds the code that collects it and a few synthetic files in its tests. The files are copyright Eugene F. Fama and Kenneth R. French. The library's page states no licence; a use in research cites the library and the papers the factors come from, and any other use is a question for its authors, not something fincommons can grant.

> Kenneth R. French, *Data Library*, https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html.
>
> Fama, E. F. and K. R. French (1993), Common risk factors in the returns on stocks and bonds, *Journal of Financial Economics* 33(1), 3--56.
>
> Fama, E. F. and K. R. French (2015), A five-factor asset pricing model, *Journal of Financial Economics* 116(1), 1--22.

## Troubleshooting

* **`series` raises `LookupError`** → the table holds no row for the file, table and frequency, which is a misspelt name far more often than an empty table, or a series of `columns` is not in the table → check the names against the `dataset`, `table` and `frequency` columns ([List which files and tables exist](#list-which-files-and-tables-exist)).
* **`outcome["failed"]` is above nought** → a file was set aside; its traceback is in the log at `ERROR` → read `outcome["failed_sources"]`; the lake keeps the previous version of the file, and the next run tries it again.
* **A file named in `datasets` fails with "The library offers no file"** → the page does not link that name; the library renames files without notice → look the name up with `ffd.library_index()`.
* **`No space left on device` while the disk is nearly empty** → Spark's scratch directory defaults to `/tmp`, often a small tmpfs → set `spark.local.dir` to a directory on a disk.
* **The workers use a different Python, or fail to import fincommons** → Spark launches them with `python3` from the PATH → set `PYSPARK_PYTHON` before the session is built, as in the scripts above.
* **A `table` code you do not know, such as `MEDIAN_DIVIDEND_YIELD_MONTHLY` or `RET_VW_2`** → a title no rule matches, or two tables on one code and frequency → see [the vocabulary of tables](#the-vocabulary-of-tables); a `WARNING` names it.
* **An annual characteristics table has a row for the year in progress** → it is keyed by the year the portfolios were formed, not by the last return → expected.
* **A parquet reader briefly finds a partition missing** → the partition is being rewritten → use Delta, which replaces partitions in one transaction.
* **A catalogue-named Silver layer is refused** with "addressed by catalogue name alone" → the marks need a directory → give a `path` beside the `table`.
* **The factsheet stops with "The Silver layer holds no table"** → build the table first with `update_famafrenchdata`. **"The table holds no factor files of ..."** → collect the whole library, or name only regions it holds in `FINCOMMONS_REGIONS`. **"... cannot be reported"** → `FINCOMMONS_MONTH` is later than the last month the library holds for every region.
