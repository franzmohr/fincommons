# File sizing of partitioned Parquet tables

## The problem

A pipeline that writes a table partitioned by reference date writes each date
on its own, and the frame that reaches
`df.write.partitionBy("DATADT").mode("append").parquet(...)` has typically
been through several joins and window functions. Each of those spreads the
rows over `spark.sql.shuffle.partitions` tasks (200 by default), and a
partitioned write produces one file per task and partition. Written as it
stands, a single day arrives as up to 200 files, each holding a fraction of a
megabyte. A query has to open every file of the partitions it reads, and that
cost is paid per file rather than per row, so queries over such a table slow
down with the number of its files rather than the volume of its data.

The cause is the shape of the write, not the format. Every module of
fincommons that writes partitioned Parquet has that shape: FIRDS Bronze and
marketpricedata.

## How the files are sized

### `fincommons.layout.rebalanced`

[`src/fincommons/layout.py`](../src/fincommons/layout.py) provides
**`rebalanced(df, *columns)`**, which returns `df` with Spark's `rebalance`
hint on the partition columns. The hint adds one shuffle by those columns
before the write. Adaptive query execution then merges the small partitions of
that shuffle and splits the large ones until each is about
`spark.sql.adaptive.advisoryPartitionSizeInBytes` (64 MB by default). The
result:

- a small reference date is written as one file,
- a large reference date is written as a few files of a reasonable size,
- a frame spanning many dates is written as about one file per date instead
  of one per date and task.

On synthetic data, a rebalanced write produces one file per partition where
the same data written without the hint produces eight.

The two alternatives are not used:

- `coalesce(n)` avoids the shuffle, but it also runs every upstream step in
  only n tasks, window functions included.
- A fixed `repartition(n)` gets the file size wrong for any day that is
  unusually large or small.

The hint needs Spark 3.2 or later; fincommons requires 3.5. It relies on
adaptive query execution, which is on by default. If adaptive execution is
switched off, the hint still produces one file per partition value, but it
does not split a large one.

### `fincommons.layout.compact_partitions`

**`compact_partitions(spark, path, target_file_size=128 MB)`** rewrites the
partitions of an existing Parquet table that consist of small files, whatever
left them behind.

- Every folder that holds Parquet files directly is rewritten as
  `ceil(size / target_file_size)` files, but only if it holds more than that.
  This covers a partition at any depth of nesting, or the table itself if it
  isn't partitioned. Folders that are already compact are left alone, so the
  function can be rerun at any time.
- Rows, values and partitioning are unchanged. Only the number of files
  changes.
- A partition is first written to `.compact_new_<name>` beside it. Its rows
  are counted against the original, and only then are the folders swapped
  (old folder → `.compact_old_<name>`, new folder → `<name>`, old one
  deleted). Spark and Arrow both skip folders whose names begin with a dot,
  so a reader never mistakes a work folder for a partition. An underscore
  wouldn't work: Spark only skips such a folder if its name has no `=`, and a
  partition's name always does.
- Files are read with `mergeSchema`. Otherwise Spark would read the folder
  with the schema of a single file, and a column that only some of the files
  carry would be lost in the rewrite.
- A partition whose files disagree on a column's type cannot be merged. It is
  left untouched and logged at `ERROR` with its traceback, the other
  partitions are compacted anyway, and the result and a closing `WARNING`
  report the failures. The FIRDS Bronze layer can contain such partitions
  where files of two schema versions share a folder.
- Empty partition folders are left alone. The FIRDS Bronze layer uses an
  empty folder to record a file that held no records.
- A run that died in the middle of a swap is finished at the start of the next
  run:
  - if a verified staging folder exists, it is put in place;
  - a leftover retired folder is removed;
  - an unverified staging folder is discarded.
- Delta tables are refused with a `ValueError` pointing to `OPTIMIZE`.
  Rewriting their files behind the transaction log would corrupt them.

It returns a dict with these keys:

- `compacted`: the partitions that were rewritten,
- `files_before` and `files_after`: their Parquet file counts before and after,
- `failed`: the partitions that could not be rewritten.

The swap is two renames, not a transaction. Run the function when the
pipeline that writes the table isn't running and nothing important is reading
the table. It uses plain `os` file operations, so it works on a local data
lake. On Databricks it relies on the Unity Catalog volume supporting
directory renames.

```python
from fincommons.firds import common
from fincommons.layout import compact_partitions

result = compact_partitions(spark, common.path_bronze_auth_017_001_02(config_path))
print(result["files_before"], "->", result["files_after"], "files;", len(result["failed"]), "failed")
```

### The hint applied at every partitioned Parquet write

Every partitioned Parquet write has the form
`rebalanced(X, cols).write.partitionBy(cols)`:

| Module | Files |
| --- | --- |
| firds | `bronze.py` |
| marketpricedata | `bronze_dse.py`, `bronze_vndirect.py`, `bronze_yahoo.py` |

**Not covered by the hint:**

- `fincommons.addressing.write_delta`. Delta tables size their files with
  `optimizeWrite` and are compacted with `OPTIMIZE`. FIRDS `main` is such a
  table.
- `Window.partitionBy(...)` calls, which are not writes.

## Tests

[`tests/test_layout.py`](../tests/test_layout.py) tests the layout functions
on synthetic data; the comparison of a write with and without the hint above
is one of them.

## Documentation

The handbook's section *Physical layout of a table*
(`fincommons.tex`, `sec:intro-data-layout`) explains why the framework's
plain Parquet tables need their own remedy and why the rebalance hint is
chosen over `coalesce` and `repartition`.
