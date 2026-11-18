# FIRDS data

## Introduction

Module `firds` builds a historical database of the reference data that trading
venues report to ESMA under Article 27 of MiFIR, as described in ESMA's
[FIRDS - Instructions on access and download of full and delta reference data files](https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files).
It does so in four steps, which are what this file describes:

* `update_landing_import` downloads the files ESMA publishes into the landing zone;
* `update_from_landing_to_bronze` reads them into the Bronze layer, one publication day per partition;
* `update_from_bronze_to_silver_main` applies them to the historical database `main`;
* `update_from_silver_main_to_silver_compact` derives the one-row-per-ISIN table `compact` from it.

The four are the daily run, in that order, and each is safe to call again: what
a run has to do is read from the data lake rather than passed to it.

Why each step works the way it does -- how the register is paged, why the
Bronze layer is plain Parquet, how a delta file is applied, why the historical
database is clustered rather than partitioned -- is the FIRDS chapter of the
handbook, in `docs/handbook`. What each function takes and returns is its own
docstring.

## Setting up

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
```

No module of the package opens a session; this one is the caller's. Four
things about it are easy to get wrong without being told so: the cores and the
heap are one decision, the scratch directory defaults to a `/tmp` that is
often a tmpfs, the Python workers are launched with whatever `python3` is on
the PATH, and a session that already exists takes none of these settings and
says so nowhere -- which is why the scratch directory is read back above. The
interpreter is settled here because this is where the session is built, not
because this is where it would be missed: FIRDS launches no Python worker at
all, since the parsing is the JVM's XML data source, so a FIRDS run with the
wrong one completes and says nothing. It is the Silver layers of
`exchangerate`, `famafrenchdata`, `marketpricedata` and `financialreport` that
raise the `ModuleNotFoundError`, in a session that may well have been built
while reading this page. The Infrastructure chapter of the handbook gives the
reasons for all four.

## Download data into the landing zone

`update_landing_import` asks ESMA's register what it has published and
downloads what the landing zone does not already hold: the full file (FULINS)
the history starts from, every delta file (DLTINS) published after it, and the
most recent cancellations file (FULCAN). The files land in the `import` folder,
and the Bronze import moves each one to `finished` or to `error`.

```python
firds.update_landing_import(config_path, start_date = "2026-01-01")
```

`start_date` decides where a **new** history begins; it is the date the full
file is sought from, or the next day ESMA published one. Giving a date earlier
than the full file already held moves the start of the history back, where ESMA
published a full file between the two, and the Silver layer then builds the
history again from the earlier one. `start_date = "2018-12-29"` starts from the
last full file of 2018, which is as far back as the register reaches.

## Update the Bronze layer

`update_from_landing_to_bronze` reads the files of the `import` folder and
writes them as Parquet, one directory per message definition and one partition
per publication day. A file that was read is moved to `finished`, which is the
record of what the Bronze layer has been built from; one that was not, or that
was imported before, is moved to `error` with a warning.

`workers` is how many files are read at a time, one by default. A higher figure is worth it because a file is read by a single task whatever the session is given -- the XML reader reports a file as one it cannot split -- so the cores of a session are used by having several files in flight rather than by giving one file more. The bound is memory rather than cores, since each file being read is held in the driver heap, so the parts of a full file want fewer workers than the daily delta files do, and `spark.local.dir` is worth settling before raising it: files read at once spill at once. The handbook measures it.

```python
firds.update_from_landing_to_bronze(spark, config_path)

# Four files at a time, which is what uses the cores of a session on a
# backfill of several hundred delta files.
firds.update_from_landing_to_bronze(spark, config_path, workers = 4)
```

## Update the Silver layer: `main`

`update_from_bronze_to_silver_main` builds the historical database of section
4.2 of the Guide. On the first run it initialises the history from the full
file; on every run after it applies each delta file newer than the watermark,
in chronological order, closing the versions the incoming records supersede and
appending the rest. How far it has got is recorded in a watermark table of its
own, so a second run over the same files does not apply them twice.

```python
firds.update_from_bronze_to_silver_main(spark, config_path)
```

The layout of the table -- clustering and deletion vectors -- is declared when
the database is created. The statements are idempotent, so an existing database
can be brought up to it:

```python
firds.declare_silver_main_layout(spark, config_path)
```

`compact` says how often the historical database is compacted while the dates
are applied. The default, `'weekly'`, compacts on the delta file of each
Friday, which is what the daily job has always done. The compaction clusters
the files written since the last one, and it also rewrites the files in which
the rows the merges marked deleted have accumulated -- most of the table -- so
its cost follows the size of the history rather than the number of dates since
the last one. A daily run pays that once a week. A backfill applies the dates
of months or years in one run, and the weekly cadence then rewrites a growing
history once per week of the period being built, some forty times over a year,
for a clustering the end of the run would have given it once.

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

A run stops rather than building a history around a gap: at a day whose delta
file is not fully imported, at a record that does not say which instrument it
is about, and at a day ESMA published after the history had passed it. Each
refusal names what it found. `firds.clear_late_day`,
`firds.record_history_from` and `firds.update_metadata_maxdate` are what
resolve them once someone has decided what the gap means; their docstrings say
what each does, and the handbook says when each case arises.

## Update the Silver layer: `compact`

An instrument traded on several venues has one row per venue in `main`, which
multiplies rows when the table is joined with another. `compact` carries one
row per ISIN instead: text fields are concatenated with `+` over the venues,
the typed fields hold the value the venues agree on and are empty where they
disagree, and a cancelled record is left out. It is rebuilt from `main` in
full, so the write replaces it.

Beside those it carries the reference data a MiFIR transaction report states
for an instrument that is not on a trading venue, RTS 22 fields 42 to 56:
`full_name`, `cfi`, `notional_currency`, `price_multiplier`, `option_type`,
`option_exercise_style`, `maturity_date`, `expiry_date` and `delivery_type`.
These are properties of the instrument and not of the venue it trades on, so
the five added for this purpose hold the value the venues agree on and are
empty where they disagree, as the typed fields do: a name joined with a `+`
is no name, and `CALL+PUTO` is not an option type. `cfi` is older than this
and is still concatenated, so a reader filling a report field from it should
expect a value of more than one code.

The fields of that range the table cannot supply are the second notional
currency, the underlying instrument code, the underlying index and its term,
and the strike and its currency (45, 47, 48, 49, 51 and 52): the analytics
schema of section 4.1 does not read them out of the register.

```python
firds.update_from_silver_main_to_silver_compact(spark, config_path)
```

`file_format` is `'parquet'` by default, because that is what every engine
reads without a further dependency. Delta is worth asking for where the table
may be queried while it is rebuilt, since the rebuild then replaces it in one
transaction:

```python
firds.update_from_silver_main_to_silver_compact(spark, config_path, file_format = "delta")
```

A table that already stands keeps the format it is stored in; delete the
directory to change it.

## Accessing the data

`connect_to_firds` opens either table, in whichever format it is stored in:

```python
firds_main = firds.connect_to_firds(spark, config_path, "main")
firds_compact = firds.connect_to_firds(spark, config_path, "compact")
```

`main` carries deletion vectors, so a reader outside Spark needs support for
them; `compact` is the table meant for such readers.

## Tests

```
pytest tests/ -k firds
```
