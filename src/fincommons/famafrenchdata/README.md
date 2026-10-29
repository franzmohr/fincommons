# The Kenneth R. French Data Library

## Introduction

Module `famafrenchdata` brings the [Kenneth R. French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html) into the data lake: the factors of the Fama-French three- and five-factor models, momentum and the reversal factors, the returns of the portfolios they are built from -- sorted on size, book-to-market, profitability, investment, prior returns, industries and more -- and the NYSE breakpoints of the sorts, for the US since 1926 and for the international regions since 1990.

Much of empirical asset pricing rests on these files, and every institution that uses them downloads them, opens a zipped CSV file meant for a person to read, and writes a parser for the file it needs. The library publishes some three hundred such files, and Kenneth French rebuilds all of them each month as CRSP is updated. This module reads every one of them into one table, in one shape, and keeps it up to date.

### Source, copyright and citation

The files are downloaded from the library when the pipeline runs. **No data of the library is shipped with fincommons**: the package holds the code that collects it and a few synthetic files in its tests. The files are copyright Eugene F. Fama and Kenneth R. French. The library's page states no licence; a use in research cites the library and the papers the factors come from, and any other use is a question for its authors, not something fincommons can grant.

> Kenneth R. French, *Data Library*, https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html.
>
> Fama, E. F. and K. R. French (1993), Common risk factors in the returns on stocks and bonds, *Journal of Financial Economics* 33(1), 3--56.
>
> Fama, E. F. and K. R. French (2015), A five-factor asset pricing model, *Journal of Financial Economics* 116(1), 1--22.

The US files are built from CRSP and Compustat, the international files from Bloomberg; each file says which vintage in its first line ("This file was created using the 202609 CRSP database.").

## Setting up

The paths of the data lake are read from the configuration file, which needs a `famafrenchdata` section with the keys `bronze` and `silver`:

```yaml
famafrenchdata:
    bronze: /data/famafrenchdata/bronze
    silver: /data/famafrenchdata/silver
```

The Silver layer may name a schema in a catalogue instead of a directory, which puts the table within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the marks that record which collection of each file the table was built from are files, and a schema is not a directory to write them to:

```yaml
famafrenchdata:
    bronze: /Volumes/fincommons/famafrenchdata/lake/bronze
    silver:
        table: fincommons.famafrenchdata
        path: /Volumes/fincommons/famafrenchdata/lake/silver
```

The table is then `fincommons.famafrenchdata.famafrenchdata`, always Delta.

## Reproducing a Fama-French factor

This is the module in five minutes: set it up, bring two files of the library into the lake, read them, and rebuild two of the published factors from the portfolios they are made of.

```python
# Start Spark, with the Delta extensions where delta-spark is installed
from fincommons.famafrenchdata.__main__ import build_session
spark = build_session("local[*]")

from fincommons import famafrenchdata as ffd

config_path = "paths.yaml"

# Collect two files into the Bronze layer and read them into the Silver table.
# Leave datasets out to collect the whole library, about 150 MB zipped.
outcome = ffd.update_famafrenchdata(
    spark, config_path,
    datasets=["F-F_Research_Data_Factors", "6_Portfolios_2x3"])
assert outcome["failed"] == 0

df = ffd.connect_to_famafrenchdata(spark, config_path)

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

On the 202609 edition of the library, over all 1,203 months from July 1926 to September 2026, both rebuilt series match the published factors to within 0.005 percentage points, and their correlation is 1.0. That is the rounding of the published factors, which have two decimals where the portfolio returns have four: the rebuilt factor is the published one before it was rounded.

What this shows, and what it does not:

* It shows that the published files are consistent with one another -- the factor file is the six-portfolio file combined as the definitions say -- and that the module reads both correctly: the right table, the right series under the right name, every month on the right date. A reader that took the equal-weighted table for the value-weighted one, shifted a month or dropped a portfolio would miss by far more than the rounding.
* It does not reconstruct the factors from their sources. The portfolios themselves are built by French from CRSP and Compustat -- the NYSE breakpoints, book equity, the June rebalancing -- and fincommons has neither database. Whether the portfolios are right is a question the library answers, not this check.

The same comparison runs in the test suite against synthetic files (`tests/test_famafrenchdata_silver.py`), and against the library itself where `FINCOMMONS_NETWORK=1` is set.

## Keeping the table up to date

```python
ffd.update_famafrenchdata(spark, config_path)
```

or, as the command a job scheduler runs,

```
python -m fincommons.famafrenchdata --config paths.yaml --log-level INFO
```

which exits with 1 where a file was set aside. `--dataset NAME` restricts the run to a file and may be given more than once; `--full` rebuilds every partition of the table from the Bronze layer, which is how the table follows a new version of the reader.

The run needs no argument saying what to collect. It reads the library's page for the files it offers -- the list is not kept in the package, because the library adds and renames files without notice -- and asks the server, with a `HEAD` request, when each file was last modified and what its entity tag is. A file whose tag is the one kept from the last collection is not downloaded. Where the server does not answer, the file is downloaded and its text compared with the one kept by its SHA-256, so an unchanged file is never written again. On most days a run downloads nothing; in the days after French's monthly update it downloads everything.

Each file is collected and read on its own. One that fails -- a download cut off, an answer that is not a zip archive, a file whose layout the reader does not know -- is logged with its traceback at `ERROR`, counted in the outcome as `failed` and named in `failed_sources`, and the others carry on. The file keeps what the lake held of it before, in both layers.

## The Bronze layer

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

The Bronze layer keeps the text of every file exactly as published, one row per line: `LINENO`, `LINE`, `LASTMODIFIED` (the server's `Last-Modified`, UTC) and `COLLECTDT`, partitioned by `DATASET`, the name of the file as the page links it without `_CSV.zip`. Nothing is dropped -- the preamble says which vintage of CRSP the file was built from, how its portfolios are formed and which value stands for a missing one, and the notes between tables say what the tables are. A new collection of a file replaces its partition.

The archives are downloaded into a temporary directory on the driver, not into the lake, and only the parquet file reaches the Bronze layer. `collections/<DATASET>.json` records the address, the `Last-Modified` and `ETag` the server sent, the SHA-256 of the text, the number of lines and when it was collected.

## The Silver table

One table, `famafrenchdata`, with a row per file, table, series and period, partitioned by `DATASET`:

| Column | Meaning |
| --- | --- |
| `REFDT` | the date of the period: the day for daily and weekly rows (a weekly row is keyed by the last day of its week), the last day of the month for monthly rows, 31 December for annual rows |
| `DATASET` | the file, e.g. `F-F_Research_Data_Factors`, `6_Portfolios_2x3` |
| `TABLE` | the kind of table within the file, a code of the vocabulary below |
| `FREQUENCY` | `D`, `W`, `M` or `A` |
| `SERIES` | the column as the file's header names it, trimmed: `Mkt-RF`, `SMALL LoBM`, `NoDur` |
| `VALUE` | the value as published: returns and factors in percent, numbers of firms, average size in millions of dollars, ratios as the table states them |
| `TITLE` | the title of the table as published, whitespace collapsed, for traceability; empty for the untitled first table of a factor file and for breakpoints |

No column is ever null. The library writes a missing value as `-99.99` or `-999`, and such a value is left out of the table rather than written as a null, so a series has a row for the periods it has a value for. Filter on `DATASET` first: the table is partitioned by it.

The table is rebuilt file by file: each file whose Bronze collection the table was not built from is read again and replaces its partition, and the other partitions are left as they are. A mark per file, `silver/_watermarks/<DATASET>.json`, records the SHA-256 of the text the partition was built from; it is written after the partition is in place, so a run that dies leaves the mark behind and the next run builds the file again. The table is written as parquet or Delta as for the other modules (`file_format`); as Delta a rebuild replaces its partitions in one transaction, as parquet a reader of a partition being rewritten may find it missing for a moment.

The reading runs on the executors, one file per task, so the hundred million values of the library never pass through the driver. Built from the whole 202609 edition on four cores, the table took about two and a half minutes and holds 86,029,613 rows; the Bronze layer takes about 320 MB as parquet and the Silver table about 230 MB. `series(df, dataset, table, frequency, columns=None)` brings one table of one file back to the driver in the wide shape it was published in.

### How a file is read

A file is read for its shape rather than from a description of each file: a line that begins with a comma is the header of a table, the lines of text directly above it are its title, and the rows below it are keyed by a year (`2025`), a month (`202509`) or a day (`20250930`). The preamble, the notes and the copyright line are passed over. The breakpoint files are the exception that proves the rule: they have no header, or one that names only the counts, and their columns are named here -- `N` or the counts the header names (`<= 0`, `>0` for the breakpoints of a ratio), followed by the twenty NYSE percentiles `P05` to `P100`.

Run over all 297 files of the 202609 edition, the reader gives 86,029,613 values, and its run turned up the irregularities it now handles: a title written over two lines (`Annual Factors:` / `January-December` in the momentum file), a carriage return alone at the start of a header row (`6_Portfolios_2x3`) -- so a line ends at a line feed and nowhere else --, a blank line between a header and its first row (`Portfolios_Formed_on_VAR`), and a misspelt title (`Aerage Value Weighted Returns`). A row that holds more values than its header has columns, or a value that is not a number, fails the file rather than putting values under the wrong series.

### The vocabulary of tables

The titles are prose and vary from file to file; 57 distinct titles occur in the 202609 edition. Each is mapped onto a code, and the frequency is not part of the code -- the monthly and the annual returns of a file are both `RET_VW`, told apart by `FREQUENCY`. Nor is the treatment of dividends, which the library publishes as files of their own (`..._Wout_Div`), told apart by `DATASET`.

| `TABLE` | What it holds | Published as, e.g. |
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

The vocabulary and its rules are `TABLE_CODES` and `TABLE_RULES` in `fincommons.famafrenchdata.parse`, and a test checks every one of the 57 published titles against them.

Two cases fall outside it, and neither loses a table:

* A title no rule matches is filed under a code made from the title -- `Median Dividend Yield -- Monthly` becomes `MEDIAN_DIVIDEND_YIELD_MONTHLY` -- and a `WARNING` names it on the driver. A table the library adds is thus in the table under a name that says what it is until a rule is written for it.
* Two tables of one file that land on the same code and frequency are numbered, the later one `RET_VW_2`, with a `WARNING`. It happens once in the 202609 edition: `6_Portfolios_2x3_weekly` titles both of its first two tables `Average Value Weighted Returns -- Weekly`, where the daily file of the same portfolios has value- and then equal-weighted returns. Which of the two the second one is, the file does not say, so it is not guessed here.
