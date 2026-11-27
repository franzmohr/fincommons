# FIRDS reference data

Module `firds` builds a day-by-day history of the reference data of every
financial instrument admitted to trading on an EU trading venue -- name, CFI
code, issuer, maturity, currency and so on -- from the files ESMA publishes in
FIRDS, its Financial Instruments Reference Data System. Use it to describe an
ISIN (International Securities Identification Number) as it stood on the date
of a transaction or a holding, or to list what a venue trades.

At a glance:

* **Source:** the reference data trading venues report to ESMA under Article
  27 of MiFIR, downloaded from ESMA's public register as described in
  [FIRDS - Instructions on access and download of full and delta reference data files](https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files).
  No credentials are needed; the download step needs network access.
* **Update frequency:** daily; ESMA publishes a delta file (DLTINS) each day,
  a full file (FULINS) the history starts from, and a cancellations file
  (FULCAN).
* **Tables written:** `main`, the full history (Delta), and `compact`, one row
  per ISIN in its latest state (Parquet by default).
* **Key:** ISIN, trading venue and `valid_from` for `main`; ISIN for
  `compact`.
* **Needs:** a Spark session with Delta Lake. No module of the package opens a
  session; it is the caller's.

## Quick start

1. Add a `firds` section to `paths.yaml`, the configuration file naming the
   paths of the data lake:

   ```yaml
   firds:
       landing: /data/firds/landing
       bronze: /data/firds/bronze
       silver: /data/firds/silver
       metadata: /data/firds/metadata
   ```

2. Start Spark, create the directories and run the four steps of the
   pipeline, then read the result:

   ```python
   import os
   import sys

   # Spark's Python workers must use this interpreter.
   os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
   os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

   from pyspark.sql import SparkSession
   from delta import configure_spark_with_delta_pip

   builder = SparkSession.builder \
                       .master("local[4]") \
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
   spark = configure_spark_with_delta_pip(builder).getOrCreate()

   from fincommons import firds

   config_path = "paths.yaml"
   firds.create_directory_tree(config_path)

   # 1. Download the full file and the delta files of a short window.
   firds.update_landing_import(config_path, start_date = "2026-11-01", end_date = "2026-11-10")
   # 2. Read them into the Bronze layer.
   firds.update_from_landing_to_bronze(spark, config_path)
   # 3. Build the historical database `main`.
   firds.update_from_bronze_to_silver_main(spark, config_path)
   # 4. Derive the one-row-per-ISIN table `compact`.
   firds.update_from_silver_main_to_silver_compact(spark, config_path)

   compact = firds.connect_to_firds(spark, config_path, "compact")
   compact.show(5)
   ```

The layers are the stages of the data lake: the landing zone holds the files
as downloaded, the Bronze layer the same data as Parquet, and the Silver layer
the cleaned tables analyses read. The four steps are the daily run, in that
order, and each is safe to call again: what a run has to do is read from the
data lake rather than passed to it. For a session with memory, scratch
directory and logging settled, use the script under
[Running it on a schedule](#running-it-on-a-schedule).

## What you get

### `main` -- the historical database

**One row per version of the reference data of an instrument on a trading
venue:** ISIN, trading venue (a MIC, ISO 10383 Market Identifier Code) and
first day of validity. It is built as section 4.2 of ESMA's instructions
prescribes, from one full file and the daily delta files after it. The table
is always Delta and carries deletion vectors.

| Column | Meaning |
|---|---|
| `isin` | ISIN of the instrument (key) |
| `trading_venue` | MIC of the trading venue (key) |
| `full_name`, `short_name` | full name; short name (the ISO 18774 FISN) |
| `cfi` | CFI classification code |
| `commodity_derivative_indicator` | whether the instrument is a commodity derivative |
| `issuer` | LEI (Legal Entity Identifier, ISO 17442) of the issuer |
| `issuer_request`, `admission_approval_date_by_issuer`, `request_for_admission_date`, `first_trade_date`, `termination_date` | admission to and trading on the venue |
| `notional_currency` | notional currency |
| `total_issued_nominal_amount`, `total_issued_nominal_currency`, `maturity_date`, `nominal_value_per_unit_amount`, `nominal_value_per_unit_currency`, `debt_seniority` | debt instruments |
| `interest_rate_fixed`, `interest_rate_floating_id_type`, `interest_rate_floating_id`, `interest_rate_floating_term_value`, `interest_rate_floating_term_unit`, `interest_rate_floating_spread` | interest rate of a debt instrument |
| `expiry_date`, `price_multiplier`, `option_type`, `option_exercise_style`, `delivery_type` | derivatives |
| `tech_publication_period_from_date`, `tech_publication_period_to_date`, `tech_relevant_competent_authority`, `tech_relevant_trading_venue` | technical attributes of the record |
| `valid_from` | first day the version is valid on (key) |
| `valid_to` | last day the version is valid on, both days inclusive; empty while the version is in force |
| `latest_record` | whether this is the most recent version received for the instrument |
| `action_type` | `NewRcrd`, `ModfdRcrd`, `TermntdRcrd` or `CancRcrd`, the delta-file element the record came in; `FULINS` for a version from the full file |

An instrument traded on several venues has one row per venue, and the venues
need not agree. A cancelled record stays as the version in force of its
instrument and venue. The history begins with the full file it was started
from.

### `compact` -- one row per ISIN

**One row per ISIN, from the latest version of every venue's record of it in
`main`, cancelled records left out.** In `main` an instrument traded on
several venues multiplies rows when the table is joined with another;
`compact` avoids that. It is rebuilt from `main` in full, so the write replaces
it, and it holds the latest state only.

| Column | How the venues' values are combined |
|---|---|
| `isin` | key |
| `cfi`, `issuer`, `trading_venue`, `short_name` | text: the different values joined with `+` |
| `maturity_date`, `expiry_date`, `price_multiplier` | typed: the value the venues agree on, empty where they disagree |
| `full_name`, `notional_currency`, `option_type`, `option_exercise_style`, `delivery_type` | the value the venues agree on, empty where they disagree |

An ISIN whose every venue was cancelled is not in `compact` at all.

`compact` also carries the reference data a MiFIR transaction report states
for an instrument that is not on a trading venue, RTS 22 fields 42 to 56:
`full_name`, `cfi`, `notional_currency`, `price_multiplier`, `option_type`,
`option_exercise_style`, `maturity_date`, `expiry_date` and `delivery_type`.

* These are properties of the instrument and not of the venue it trades on, so
  the five added for this purpose hold the value the venues agree on and are
  empty where they disagree, as the typed fields do: a name joined with a `+`
  is no name, and `CALL+PUTO` is not an option type.
* `cfi` is older than this and is still concatenated, so a reader filling a
  report field from it should expect a value of more than one code.
* The fields of that range the table cannot supply are the second notional
  currency, the underlying instrument code, the underlying index and its term,
  and the strike and its currency (45, 47, 48, 49, 51 and 52): the analytics
  schema of section 4.1 does not read them out of the register.

### Joining to other modules

* `isin` is the key other modules name an instrument by.
* `issuer` is an LEI and joins to `entitymasterdata` and `gleif`.
* `trading_venue` is a MIC.

### What a reader may rely on

What the columns of each table hold is stated by its data contract in the
Open Data Contract Standard:
[`contract_firds_silver_main.yaml`](../data/contract_firds_silver_main.yaml)
and
[`contract_firds_silver_compact.yaml`](../data/contract_firds_silver_compact.yaml),
which [`product_firds.yaml`](../data/product_firds.yaml) names as the module's
output ports. They are the one place the columns are described. A contract
states the key of its table and which columns are measures and how they may be
combined: **an amount or a rate is repeated for every venue and every version
of an instrument, so it is never summed over the rows of `main`.**

## Common tasks

### Open a table

`connect_to_firds` opens either table, in whichever format it is stored in.

```python
firds_main = firds.connect_to_firds(spark, config_path, "main")
firds_compact = firds.connect_to_firds(spark, config_path, "compact")
```

`main` carries deletion vectors, so a reader outside Spark needs support for
them; `compact` is the table meant for such readers.

### Look up one ISIN

When you want an instrument's current reference data, read `compact`; when you
want it per venue and every version of it, read `main`.

```python
from pyspark.sql import functions as F

firds_compact.filter(F.col("isin") == "DE0007164600").show(truncate = False)

firds_main.filter(F.col("isin") == "DE0007164600") \
    .orderBy("trading_venue", "valid_from") \
    .select("trading_venue", "full_name", "cfi", "valid_from", "valid_to",
            "latest_record", "action_type") \
    .show(truncate = False)
```

### The reference data as it stood on a day

When you want what held on the date of a transaction or a holding, read
`main` -- `compact` holds the latest state only. `firds.history.records_as_of`
selects the version in force on a day (`valid_from <= day` and `valid_to` empty
or `>= day`); `firds.history.latest_records` selects the most recent version
of every instrument.

```python
as_of = firds.history.records_as_of(firds_main, "2026-06-30")
latest = firds.history.latest_records(firds_main)

# A cancelled record stays as the version in force; leave it out where it
# should not count as the instrument being on that venue.
as_of = as_of.filter(F.col("action_type") != "CancRcrd")
```

The result still has one row per venue. Group by `isin` yourself, or use
`compact` if the latest state is enough.

### Which instruments a venue trades

When you want the instruments on one venue, filter `main` on `trading_venue`.
In `compact` the column holds every venue joined with `+`, so test for one
value with a split rather than equality.

```python
on_xetra = firds.history.latest_records(firds_main) \
    .filter((F.col("trading_venue") == "XETR")
            & (F.col("action_type") != "CancRcrd")) \
    .select("isin", "full_name", "cfi", "first_trade_date", "termination_date")

# The same from compact
on_xetra_compact = firds_compact.filter(
    F.array_contains(F.split("trading_venue", r"\+"), "XETR")
)
```

Add a condition on `termination_date` where instruments whose trading on the
venue has ended should be left out.

### Look up what a column means

```python
import fincommons

fincommons.describe_column("cfi", dataset="firds")
```

### Get the schema a table promises

```python
from fincommons.contracts import load_contract, spark_schema
spark_schema(load_contract("fincommons.firds.silver.main"))
```

## Running it on a schedule

### The production script

No module of the package opens a session; this one is the caller's. The
script below is the daily job:

```python
# Path to the configuration file naming the paths of the data lake
config_path = "paths.yaml"

# The stages report what they do through the standard library's logging
from fincommons.log import configure

configure("INFO")

# Spark launches its Python workers with whatever `python3` resolves to on
# the PATH, which need not be the interpreter running this script. This has
# to be settled before the session is built.
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Start Spark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
                    .master("local[8]") \
                    .appName("fincommons") \
                    .config("spark.driver.memory", "32g") \
                    .config("spark.local.dir", "/data/spark-tmp") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# What the session actually got. A session that already existed took none of
# the settings above and said nothing about it, which is the one failure here
# that is silent. The core settings are read off SparkConf, because
# spark.conf is the registry of the SQL settings and raises
# SQL_CONF_NOT_FOUND for one that is not among them.
conf = spark.sparkContext.getConf()
if not conf.get("spark.local.dir", None):
    raise RuntimeError(
        "spark.local.dir did not take; stop the session and build it again"
    )

# Load module
from fincommons import firds

# Make sure that all the necessary directories exist
firds.create_directory_tree(config_path)

# The four steps, in this order
firds.update_landing_import(config_path, start_date = "2026-01-01")
firds.update_from_landing_to_bronze(spark, config_path)
firds.update_from_bronze_to_silver_main(spark, config_path)
firds.update_from_silver_main_to_silver_compact(spark, config_path)

# Routine upkeep, after the weekly compaction of Friday's delta file: a week
# of versions stays readable.
import datetime

if datetime.date.today().weekday() == 5:
    firds.vacuum_silver_main(spark, config_path)
```

### What to settle about the session

Four things about the session are easy to get wrong without being told so:

1. **The cores and the heap are one decision** (`local[8]` and
   `spark.driver.memory`).
2. **The scratch directory** defaults to a `/tmp` that is often a tmpfs; set
   `spark.local.dir`.
3. **The Python workers** are launched with whatever `python3` is on the PATH.
4. **A session that already exists takes none of these settings** and says so
   nowhere -- which is why the scratch directory is read back in the script.

[Setting up a Spark session](../../../docs/spark-session.md) explains each of
them, how to size them for your machine, and what the failures look like; the
[Infrastructure chapter](../../../docs/handbook/00-infrastructure.tex) of the
handbook gives the reasons and the measurements.

The interpreter is settled in the script because this is where the session is
built, not because this is where it would be missed: FIRDS launches no Python
worker at all, since the parsing is the JVM's XML data source, so a FIRDS run
with the wrong one completes and says nothing. It is the Silver layers of
`exchangerate`, `famafrenchdata`, `marketpricedata` and `financialreport` that
raise the `ModuleNotFoundError`, in a session that may well have been built
while reading this page.

### Order, reruns and failures

* **Order:** download, Bronze, `main`, `compact`; then, the day after the
  weekly compaction, the vacuum.
* **Idempotency:** each step is safe to call again. The landing zone downloads
  only what it does not already hold, a file imported into Bronze is moved to
  `finished`, and `main` records how far it has got in a watermark table, so a
  second run over the same files does not apply them twice.
* **When a run stops:** see [Troubleshooting](#troubleshooting). A run that
  died within a batch resumes that same batch when it is run again.

### Backfills

A backfill -- months or years of delta files in one run -- wants different
settings from the daily run; see [Compaction cadence](#compaction-cadence),
[Batching dates](#batching-dates) and [Cleaning up: `vacuum`](#cleaning-up-vacuum).
In short:

```python
# Only the period wanted, not every file published since start_date.
firds.update_landing_import(config_path, start_date = "2025-12-27", end_date = "2026-11-17")
# Four files at a time, which is what uses the cores of a session on a
# backfill of several hundred delta files.
firds.update_from_landing_to_bronze(spark, config_path, workers = 4)
# A backfill applied twenty dates at a time, compacted every forty dates and
# once more at the end.
firds.update_from_bronze_to_silver_main(spark, config_path, compact = 40, batch = 20)
firds.compact_silver_main(spark, config_path)
# After a backfill and its final compaction: every replaced file is only hours
# old, so the default frees nothing. Keep the current version alone.
firds.vacuum_silver_main(spark, config_path, retention_hours = 0)
firds.update_from_silver_main_to_silver_compact(spark, config_path)
```

## How it works

Why each step works the way it does -- how the register is paged, why the
Bronze layer is plain Parquet, how a delta file is applied, why the historical
database is clustered rather than partitioned -- is the
[FIRDS chapter](../../../docs/handbook/01-firds.tex) of the handbook. What each
function takes and returns is its own docstring.

### Download into the landing zone

**`update_landing_import` asks ESMA's register what it has published and
downloads what the landing zone does not already hold.** That is the full file
(FULINS) the history starts from, every delta file (DLTINS) published after
it, and the most recent cancellations file (FULCAN). The files land in the
`import` folder, and the Bronze import moves each one to `finished` or to
`error`.

```python
firds.update_landing_import(config_path, start_date = "2026-01-01")
```

`start_date` decides where a **new** history begins:

* It is the date the full file is sought from, or the next day ESMA published
  one.
* Giving a date earlier than the full file already held moves the start of the
  history back, where ESMA published a full file between the two, and the
  Silver layer then builds the history again from the earlier one.
* `start_date = "2018-12-29"` starts from the last full file of 2018, which is
  as far back as the register reaches.

`end_date` is the last publication date to download. The daily job leaves it
out and takes everything published up to yesterday. Naming it closes the
window, which is what rebuilding the history for a fixed period needs; a run
that closes the window records its landing watermark at the end of that
window, so a later run without an end date continues from there.

### The Bronze layer

**`update_from_landing_to_bronze` reads the files of the `import` folder and
writes them as Parquet, one directory per message definition and one
partition per publication day.** A file that was read is moved to `finished`,
which is the record of what the Bronze layer has been built from; one that was
not, or that was imported before, is moved to `error` with a warning.

```python
firds.update_from_landing_to_bronze(spark, config_path)

# Four files at a time, which is what uses the cores of a session on a
# backfill of several hundred delta files.
firds.update_from_landing_to_bronze(spark, config_path, workers = 4)
```

**`workers` is how many files are read at a time, one by default.**

* A higher figure is worth it because a file is read by a single task whatever
  the session is given -- the XML reader reports a file as one it cannot split
  -- so the cores of a session are used by having several files in flight
  rather than by giving one file more.
* The bound is memory rather than cores, since each file being read is held in
  the driver heap. The parts of a full file therefore want fewer workers than
  the daily delta files do.
* `spark.local.dir` is worth settling before raising it: files read at once
  spill at once.

The handbook measures it.

### The historical database `main`

**`update_from_bronze_to_silver_main` builds the historical database of
section 4.2 of the Guide.**

1. On the first run it initialises the history from the full file.
2. On every run after it applies each delta file newer than the watermark, in
   chronological order, closing the versions the incoming records supersede
   and appending the rest.
3. How far it has got is recorded in a watermark table of its own, so a second
   run over the same files does not apply them twice.

```python
firds.update_from_bronze_to_silver_main(spark, config_path)
```

**The layout of the table -- clustering and deletion vectors -- is declared
when the database is created.** The statements are idempotent, so an existing
database can be brought up to it:

```python
firds.declare_silver_main_layout(spark, config_path)
```

### Compaction cadence

**`compact` says how often the historical database is compacted while the
dates are applied; the default, `'weekly'`, suits the daily run, and a
backfill should name its own.**

* `'weekly'` compacts on the delta file of each Friday, which is what the
  daily job has always done.
* The compaction clusters the files written since the last one, and it also
  rewrites the files in which the rows the merges marked deleted have
  accumulated -- most of the table -- so its cost follows the size of the
  history rather than the number of dates since the last one.
* A daily run pays that once a week. A backfill applies the dates of months or
  years in one run, and the weekly cadence then rewrites a growing history
  once per week of the period being built, some forty times over a year, for
  a clustering the end of the run would have given it once.

A backfill therefore names a cadence of its own: a whole number of applied
dates, or `'never'`. What it must not do is end uncompacted, because the rows
the merges mark deleted stay in the files they were written to until something
rewrites them, and every later merge reads those files to find the versions in
force:

```python
# A backfill of several hundred delta files, compacted every forty dates and
# once more at the end.
firds.update_from_bronze_to_silver_main(spark, config_path, compact = 40)
firds.compact_silver_main(spark, config_path)
```

Which figure is right is a property of the machine and of the period, so it is
asked for rather than guessed. `compact = 'never'` with the one compaction at
the end is the fastest run and the one that leaves the merges reading the most
deleted rows; the daily cadence is the other end of that trade.

### Batching dates

**`batch` says how many dates are applied together; a backfill reads the
history twice per batch instead of twice per date.**

* Each date applied on its own is a plan, a merge and an append, and the plan
  and the merge each read the whole history to find the versions in force that
  the date's records meet, however few those are. A backfill pays those two
  reads once per date.
* With `batch`, the records of several dates are planned as one delta file
  would be -- the records an instrument receives are chained in order, across
  the files as within one -- and applied in one merge and one append, so the
  history is read twice per batch instead.
* The history that results is the one the dates build applied in turn.

```python
# A backfill applied twenty dates at a time, compacted every forty dates and
# once more at the end.
firds.update_from_bronze_to_silver_main(spark, config_path, compact = 40, batch = 20)
firds.compact_silver_main(spark, config_path)
```

The rules of a batch:

* The default is one date, which is what a daily run has to apply anyway.
* A batch ends early before a date whose file held no records and before a
  date the run stops at, so the dates before it are applied.
* It is committed as one merge and one append tagged with its first and last
  date, and a run that died within one resumes that same batch whatever
  `batch` it is then given.
* A batch is refused where a delta file begins a version before one an earlier
  file of the batch began, which applied one after another is refused too.
* A batch holds its records in memory for the plan, so how large it can be is
  again a property of the machine.

### Row group size

**What a reading task holds is the other bound: every write to the history --
the merge, the append, the compaction -- fills its row groups to
`silver.ROW_GROUP_SIZE`, 32 megabytes, a quarter of Parquet's default.**

* A task reads a row group one column chunk at a time and each chunk in one
  piece.
* At the default the compaction wrote groups of 2.7 million rows whose names
  alone came to 136 megabytes read, and a backfill on eight cores with a
  driver of 14 gigabytes ran out of memory on the first batch after one.
* Groups already written keep their size until a compaction rewrites their
  file. A history built before this release still holds them, and a run over
  it that runs out of memory is lighter on fewer cores (`local[4]`) until the
  next compaction has rewritten it.

### When a run stops

**A run stops rather than building a history around a gap.** It stops:

* at a day whose delta file is not fully imported,
* at a record that does not say which instrument it is about, and
* at a day ESMA published after the history had passed it.

Each refusal names what it found. `firds.clear_late_day`,
`firds.record_history_from` and `firds.update_metadata_maxdate` are what
resolve them once someone has decided what the gap means; their docstrings say
what each does, and the handbook says when each case arises.

### Cleaning up: `vacuum`

**`firds.vacuum_silver_main` deletes the files no version within the retention
reads; nothing the history holds is removed, what goes is the ability to read
the table as it stood before.**

Every merge and every compaction leaves behind the files it replaced, which
only the earlier versions of `main` still read, so the table on disk grows
faster than the history in it: the lake built from 27 December 2025 to 17
November 2026 took 39 gigabytes for 261 million versions.

```python
# Routine upkeep, after the weekly compaction: a week of versions stays readable.
firds.vacuum_silver_main(spark, config_path)

# After a backfill and its final compaction: every replaced file is only hours
# old, so the default frees nothing. Keep the current version alone.
firds.vacuum_silver_main(spark, config_path, retention_hours = 0)
```

* The default retention is a week (`retention_hours = 168`), as Delta's own.
* A retention below a week must not run while an update is writing to the
  table.
* The vacuum is refused while a batch has its merge committed and not its
  append, since the batch is resumed from the version before that merge;
  running the update again completes it.

### The compact table

**`update_from_silver_main_to_silver_compact` rebuilds `compact` from the
latest versions in `main`**, combining the venues' values as described under
[What you get](#compact----one-row-per-isin).

```python
firds.update_from_silver_main_to_silver_compact(spark, config_path)
```

**`file_format` is `'parquet'` by default, because that is what every engine
reads without a further dependency.** Delta is worth asking for where the
table may be queried while it is rebuilt, since the rebuild then replaces it in
one transaction:

```python
firds.update_from_silver_main_to_silver_compact(spark, config_path, file_format = "delta")
```

A table that already stands keeps the format it is stored in; delete the
directory to change it. A compact table addressed by catalogue name is Delta.

## Reference

### Configuration

The `firds` section of `paths.yaml`:

| Key | Holds | May be a catalogue name? |
|---|---|---|
| `landing` | the landing zone the files are downloaded into (`import`, `finished`, `error`) | no |
| `bronze` | the Bronze layer, one directory per message definition | no |
| `silver` | the Silver layer holding `main` and `compact` | yes |
| `metadata` | the Delta table of watermarks that records how far `main` has been built | yes |

The Silver layer and the metadata layer may name a schema in a catalogue
instead of a directory, as for the other modules; see `fincommons.addressing`.

### Public functions

All are reached as `firds.<name>`, except the `history` ones, which are
`firds.history.<name>`.

| Group | Function | What it does |
|---|---|---|
| Set-up | `create_directory_tree(config_path)` | create the directories of the data lake; existing ones are left alone |
| Update | `update_landing_import(config_path, start_date, end_date=None)` | download what ESMA published into the landing zone |
| Update | `update_from_landing_to_bronze(spark, config_path, workers=1)` | import the landing files into the Bronze layer |
| Update | `update_from_bronze_to_silver_main(spark, config_path, compact='weekly', batch=1)` | build or extend the historical database `main` |
| Update | `update_from_silver_main_to_silver_compact(spark, config_path, file_format=None)` | rebuild `compact` from `main` |
| Upkeep | `declare_silver_main_layout(spark, config_path)` | declare the clustering and deletion vectors of `main` (idempotent) |
| Upkeep | `compact_silver_main(spark, config_path)` | cluster and compact the files of `main` |
| Upkeep | `vacuum_silver_main(spark, config_path, retention_hours=168)` | delete the files no version within the retention reads |
| Upkeep | `migrate_silver_main_to_decimal(spark, config_path)` | rewrite a `main` that carries its amounts as doubles |
| Read | `connect_to_firds(spark, config_path, table)` | open `'main'` or `'compact'` |
| Read | `history.records_as_of(history, day)` | the version of every instrument in force on `day` |
| Read | `history.latest_records(history)` | the most recent version of every instrument |
| Read | `get_analytical_fields()` | the reference data fields of `main`, without key and technical attributes |
| Watermarks | `read_metadata(spark, config_path)` | the whole watermark table, for inspection |
| Watermarks | `get_metadata_maxdate(spark, config_path, table)` | the reference date a table has been built up to |
| Watermarks | `update_metadata_maxdate(spark, config_path, table, maxdate)` | set that date, e.g. by hand for `'MAIN'` |
| Watermarks | `read_landing_state(config_path)` | what the landing zone knows, as it last recorded it |
| Watermarks | `clear_late_day(config_path, day)` | take a day off the list of days published late, accepting that the history has passed it |
| Watermarks | `record_history_from(config_path, day)` | record the full file the history has been built from |

The module also exports the lower-level steps these are built from -- the
`path_*` and `ref_*` helpers of the layout, the `read_xml_*` readers of each
message definition, `initialise_firds_database`, `upsert_from_bronze`,
`translate_xml_to_analytics_schema` and the algorithm of section 4.2 in
`firds.history` -- each documented in its docstring.

### Data contracts and documentation

* Data contracts: [`contract_firds_silver_main.yaml`](../data/contract_firds_silver_main.yaml),
  [`contract_firds_silver_compact.yaml`](../data/contract_firds_silver_compact.yaml);
  data product: [`product_firds.yaml`](../data/product_firds.yaml).
  `fincommons.describe_column("cfi", dataset="firds")` reads its answer from
  the contracts.
* Handbook: the [FIRDS chapter](../../../docs/handbook/01-firds.tex) and the
  [Infrastructure chapter](../../../docs/handbook/00-infrastructure.tex).
* ESMA:
  [FIRDS - Instructions on access and download of full and delta reference data files](https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files).

### Tests

```bash
pytest tests/ -k firds
```

## Troubleshooting

* **A run raises at a day whose delta file is not fully imported** → some
  parts of that day's file are not in `finished` → run `update_landing_import`
  and `update_from_landing_to_bronze` again, then the Silver update.
* **A run raises at a day ESMA published after the history had passed it** →
  the day was published late → decide what the gap means (the handbook says
  when this case arises); to accept it, `firds.clear_late_day(config_path,
  day)` takes the day off the list, its records stay out of the history, and
  the update carries on.
* **A run raises at a record that does not say which instrument it is about**
  → the refusal names the file → decide what the gap means; the docstrings of
  `clear_late_day`, `record_history_from` and `update_metadata_maxdate` and the
  handbook say which applies.
* **A backfill runs out of memory** → row groups written before this release
  are large → run on fewer cores (`local[4]`) until the next compaction has
  rewritten them; keep `batch` and `workers` within the driver heap.
* **The run ignored the session settings** → a session already existed and
  took none of them → stop it and build it again; the production script checks
  `spark.local.dir` for this.
* **`ModuleNotFoundError` in another module's Silver layer** → the Python
  workers use the wrong interpreter → set `PYSPARK_PYTHON` before building the
  session. FIRDS itself completes silently in that case.
* **The vacuum is refused** → a batch has its merge committed and not its
  append → run `update_from_bronze_to_silver_main` again, then vacuum.
* **`file_format` is refused for `compact`** → a table that already stands
  keeps its format → delete the directory to change it.
* **A table joined with `main` has too many rows** → `main` has one row per
  venue and version → use `compact`, or `records_as_of` and group by `isin`.
