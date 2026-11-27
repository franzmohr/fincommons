# Setting up a Spark session

The fincommons pipelines take a `SparkSession` as their first argument, and
no module of the package ever builds one: the session is yours to build. This
page shows how to do that on one machine (a workstation or a server, with
Spark in local mode), which settings matter, and how to tell when one of them
did not take.

**At a glance**

- Trying things out: copy the [minimal session](#a-minimal-session).
- Scheduled runs and backfills: copy the [production session](#a-production-session)
  and set its four values for your machine.
- On Databricks you build nothing: use the `spark` the notebook provides.
- Only reading a parquet table? You may need no Spark at all -- see
  [Do you need Spark?](#do-you-need-spark).

Why the package never builds a session itself: a library that builds its own
session decides the cores, the memory, the extensions and the scratch space of
whoever imports it, and does so invisibly. The reasoning, and the
measurements behind the advice below, are in the
[Infrastructure chapter](handbook/00-infrastructure.tex) of the handbook.

## Do you need Spark?

| You want to | Spark needed? |
| --- | --- |
| run a pipeline (`update_...`, `create_...`, the FIRDS steps) | yes |
| read a table with `connect_to_...` | yes -- it returns a Spark DataFrame |
| read a Silver table rebuilt in full (parquet by default) with pandas, polars, DuckDB, Arrow or R | no |
| look up a column or a series (`fincommons.describe_column`, `marketpricedata.describe_instrument`) | no |

Point a non-Spark reader at a table rebuilt in full rather than at one updated
in place, since a table carrying deletion vectors is read correctly only by a
reader that supports them.

## Before you start

1. **Python 3.11 or later**, with fincommons installed with the `local` extra,
   which brings PySpark and `delta-spark`:

   ```bash
   pip install -e ".[local]"
   ```

2. **Java 17 or 21.** Java 17 is what continuous integration runs on; Spark 4
   also runs on Java 21. Check with `java -version`. A Java that Spark does
   not support need not fail with a clear message -- the run may just stall.

3. **Network access to Maven the first time.** `configure_spark_with_delta_pip`
   asks Spark to fetch the Delta jars when a session first starts; later
   starts use the local copy. A machine that cannot reach Maven Central (or a
   mirror of it) cannot start a Delta session.

4. **Not Windows itself.** Spark cannot start natively on Windows; work in WSL
   there.

## A minimal session

Enough for trying a module out or running a small pipeline:

```python
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.master("local[4]")
    .appName("fincommons")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

What each part is for:

- The two `PYSPARK_*` lines make Spark's Python workers use the interpreter
  running this script -- see [setting 1](#1-the-python-interpreter).
- `local[4]` and `4g` are four cores and a 4 GB heap -- see
  [setting 2](#2-cores-and-memory-one-decision).
- The two `spark.sql...` settings and `configure_spark_with_delta_pip` add
  Delta Lake. Every table carried forward by merges (the histories of `firds`
  and `gleif`, the market price table) or addressed by catalogue name needs
  them; they cost nothing where unused, so
  always add them.

To see the pipelines' log as they run, also call
`fincommons.log.configure("INFO")` -- see Logging in the
[top-level README](../README.md#logging).

## A production session

For a scheduled job, a backfill, or anything that moves more than a few
hundred megabytes. It sets the four values that decide whether a session on
one machine is fit for the work, in the order they have to be set, and then
checks that they took:

```python
import os
import sys

# 1. The interpreter, before the session: the launcher reads the
#    environment, not the configuration of the session.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# 2. The cores and the memory are one decision; the heap is read here
#    only because this is the first session of the process.
builder = (SparkSession.builder
    .master("local[10]")
    .appName("fincommons")
    .config("spark.driver.memory", "30g")
    # 3. The scratch directory, read when the context starts.
    .config("spark.local.dir", "/data/spark-tmp")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 4. What the session actually got. A session that already existed took
#    none of the above and said nothing; SparkConf is asked rather than
#    spark.conf, which raises for a core setting with no value.
conf = spark.sparkContext.getConf()
if not conf.get("spark.local.dir", None):
    raise RuntimeError(
        "spark.local.dir did not take; stop the session and build it again"
    )
```

A module README may add settings of its own to this session (the
exchangerate one, for instance, shows three parquet rebase modes); those are
explained there. The four settings below apply to every module. Each says
what to choose, then why.

### 1. The Python interpreter

**Set `PYSPARK_PYTHON` to `sys.executable` before the session is built.**

Spark launches its Python workers with whatever `python3` resolves to on the
`PATH`, which off-cluster is rarely the interpreter the pipeline runs with: a
virtual environment is exactly a different one. The worker still starts,
because Spark puts its own `pyspark.zip` on the worker's path, and then fails
on the first import that is not Spark's with a `ModuleNotFoundError` naming a
package that is plainly installed. Where the two interpreters carry different
versions of numpy or pandas, it fails instead with errors such as
`No module named 'numpy._core.numeric'`.

- `PYSPARK_PYTHON` is what the launcher reads, and it has to be in the
  environment before the session is built. `spark.pyspark.python` on the
  builder does not do it in local mode.
- `setdefault` leaves a value already in the environment alone, so a run can
  still be aimed at another interpreter on purpose.
- Not every module notices a wrong interpreter. FIRDS launches no Python
  worker, since its parsing is the JVM's XML data source, so a FIRDS run with
  the wrong one completes and says nothing. It is the Silver layers of
  `exchangerate`, `famafrenchdata`, `marketpricedata` and `financialreport`
  that raise -- possibly in a later session of the same notebook.

### 2. Cores and memory: one decision

**Choose the heap first, then at most as many cores as leave each task about
a gigabyte.**

- **Cores** are stated in the master, as `local[10]`, and not through
  `spark.cores.max`, which only a standalone or Mesos master reads. A session
  built without a master comes up as `local[*]` and runs a task on every core
  the machine has.
- **Memory** is `spark.driver.memory`. In local mode the executors live
  inside the driver's JVM, so this is the memory of the whole run;
  `spark.executor.memory` is ignored. The pool is divided among the tasks
  running at once.

How the two interact, measured:

| Heap | Cores | Heap per task | Outcome |
| --- | --- | --- | --- |
| 30 GB | 96 (`local[*]` on a large server) | ~95 MB | not enough for one day's plan of a reference data pipeline |
| 30 GB | 10 | nearly 1 GB | fine |
| 3 GB | 4 | ~750 MB | fine for the FIRDS daily run on a history of fourteen million rows |
| 8 GB | 2 (test suite) | 4 GB | the whole fincommons test suite; 4 GB ran out at some 1,400 tests |

Leave the operating system and Python its share: the heap is only part of
what the process uses, and on a machine of 8 GB the kernel kills a process
that grows to some 7.5 GB resident.

The failure looks like this: first blocks that could not be cached and were
persisted to disk instead, then a `SparkOutOfMemoryError` raised when a task
cannot acquire what a merge needs.

**The heap is fixed when the JVM starts.** PySpark launches the JVM through
`spark-submit` when the first session of the process is built, and reads
`spark.driver.memory` from the builder for that launch only. A session built
while a JVM is already running keeps the heap the first launch asked for,
whatever its own configuration says. Where a process may already have started
one -- a notebook, a test suite, a script that imports another -- pass the
heap to the launcher instead, before anything builds a session:

```python
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 30g pyspark-shell")
```

The value must end in `pyspark-shell`. This is how `fincommons.testing`
sizes the session of the test suites.

### 3. The scratch directory

**Set `spark.local.dir` to a directory on a disk with room to spare.**

Spark writes every shuffle block, spill file and broadcast block under
`spark.local.dir`, which defaults to `java.io.tmpdir` and thus to `/tmp`. On
many Linux installations `/tmp` is a *tmpfs* sized at half the machine's
memory rather than a directory on a disk. A run that spills there spends the
memory it spilled in order to save, and fails once the shuffle outgrows that
half. Whether the default would do:

```bash
findmnt -no FSTYPE /tmp      # "tmpfs" means: set spark.local.dir
```

How much room a run needs:

- An import from a landing zone into a Bronze layer shuffles at most one
  delivery at a time.
- The FIRDS history, once the demanding case, now needs little: on four cores
  with a 3 GB heap against a history of fourteen million rows, the scratch
  directory did not pass 160 MB over a fortnight of deliveries. What is left
  is its weekly compaction.
- A backfill needs more than the daily job, roughly in proportion to the
  dates it applies in one session, because the files behind a completed job
  are released only when its structures are collected, which in a long loop
  lags the loop.
- Files read at once spill and shuffle at once: a scratch directory that coped
  with one file at a time may not cope with six (see
  [`workers`](#what-a-pipeline-may-then-be-given)).

Unlike the heap, this setting is read when the Spark context starts, so
stopping the context and building another applies it in the same process.

### 4. Reading the settings back

**After building the session, check that it got what you asked for, and stop
if it did not.**

A session that already exists takes none of the settings above.
`getOrCreate` hands back the session that is there, passes over every
configuration given to the builder, and raises nothing. Measured: a builder
given `spark.local.dir` against a live session returned a working session that
reported the setting as unset and wrote not one block under the path it had
been asked for.

Of the four settings, this is the one whose failure says nothing at all. A
wrong interpreter ends in a `ModuleNotFoundError` and a small heap in an
`OutOfMemoryError`; an ignored scratch directory merely fills `/tmp` for the
length of a backfill and then reports a full disk.

Read the settings through `spark.sparkContext.getConf()`, not `spark.conf`.
The latter is the registry of the SQL settings: asking it for a core setting
that was never given a value raises `SQL_CONF_NOT_FOUND` rather than
returning a default, which would turn the check into a crash in precisely the
case it exists to catch.

## What a pipeline may then be given

Two arguments of the pipelines depend on the session rather than on the data:

- **`workers` of the FIRDS Bronze import.** The XML reader cannot split a
  file, so a file is read by a single task however many cores the session
  has: a file of 164 MB comes back as one partition. To use the cores, have
  several files in flight. Measured on four cores over 394 MB of delta files,
  four workers took 22.4 seconds against 45.9 for one, and six were no faster
  than four. The bound is the memory, since each file being read is held in
  the driver's heap.

  ```python
  firds.update_from_landing_to_bronze(spark, config_path, workers = 4)
  ```

- **The session given to the entity master data import.** Here the session
  decides whether the stage runs at all. The GLEIF golden copy of Level 1 is
  487 MB zipped and 4.74 GB as csv. Given no session, the stage reads it into
  one pandas frame, and on a machine of 8 GB the kernel kills the import at
  7.5 GB resident; given one, it reads the csv in splits and the same import
  peaks at 1.26 GB. Pass the session.

## In other settings

- **Jupyter and other notebooks.** The session lives as long as the kernel.
  To change the heap, restart the kernel; to change the scratch directory,
  `spark.stop()` and build again. Build the session in the first cell, so no
  other cell starts a JVM first.
- **R through reticulate.** The same applies: the JVM is started by the first
  session the R process builds through Python.
- **Databricks.** Do not build a session; the notebook's `spark` already has
  Delta, and the cluster's configuration decides cores, memory and scratch
  space. See the Databricks section of the [top-level README](../README.md#on-databricks)
  for the paths `paths.yaml` has to use there.
- **Test suites.** Import the shared fixture rather than building a session:
  `from fincommons.testing import spark` in `conftest.py`. It sets the
  interpreter and an 8 GB heap as above. See [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError` for a package that is installed, or `No module named 'numpy._core.numeric'` | Spark's Python workers run a different interpreter | set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` before building the session ([1](#1-the-python-interpreter)) |
| warnings that blocks could not be cached and were persisted to disk, then `SparkOutOfMemoryError` | too little heap per task | fewer cores in `local[n]`, or a larger heap ([2](#2-cores-and-memory-one-decision)) |
| the heap you set has no effect | a JVM was already running when the session was built | restart the process, or use `PYSPARK_SUBMIT_ARGS` ([2](#2-cores-and-memory-one-decision)) |
| `No space left on device` while the lake's disk is nearly empty, after `DiskBlockObjectWriter: Error deleting ...` warnings and `Utils: Suppressing exception in finally` | scratch space on a tmpfs `/tmp` | set `spark.local.dir` ([3](#3-the-scratch-directory)) |
| settings silently ignored; `/tmp` fills up | `getOrCreate` returned a session that already existed | stop it (`spark.stop()`, or restart the kernel) and build again; keep the check of [4](#4-reading-the-settings-back) |
| `SQL_CONF_NOT_FOUND` when checking a setting | the check asked `spark.conf` for a core setting | ask `spark.sparkContext.getConf()` |
| the session does not start, or a run stalls without an error | a Java Spark does not support | use Java 17 or 21 ([Before you start](#before-you-start)) |
| the first session fails while resolving `delta-spark` | no access to Maven | allow access for the first start, or use a mirror ([Before you start](#before-you-start)) |
| a Delta table is reported as absent, or `DeltaCatalog` is not found | the session lacks the Delta extensions | build it with the two `spark.sql...` settings and `configure_spark_with_delta_pip` |
