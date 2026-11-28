# Shared infrastructure

New to fincommons? Start with the [top-level README](../../README.md), which
says what the package is for, how to install it and how to run a first
pipeline.

This folder holds the package's code. The data sets each have a module of
their own, with a README that starts with a quick start:

| Module | Use it to | README |
| --- | --- | --- |
| `exchangerate` | convert amounts between currencies with the reference rates of the ECB and the BIS | [exchangerate](exchangerate/README.md) |
| `marketpricedata` | get prices and rates from public publishers in one table, and the zero curves they imply | [marketpricedata](marketpricedata/README.md) |
| `firds` | look up the reference data of any instrument traded on an EU venue (ESMA's FIRDS) | [firds](firds/README.md) |
| `sts` | analyse ESMA's register of simple, transparent and standardised securitisations | [sts](sts/README.md) |
| `gleif` | keep a day-by-day history of who every LEI belongs to, who owns whom, and who operates which market | [gleif](gleif/README.md) |
| `entitymasterdata` | resolve a counterparty code into who the entity is and what it does | [entitymasterdata](entitymasterdata/README.md) |
| `famafrenchdata` | work with the Kenneth R. French Data Library, every file in one table | [famafrenchdata](famafrenchdata/README.md) |
| `financialreport` | read XBRL-tagged periodic reports: `esef`, `sec`, `edinet`, and `combined` | [financialreport](financialreport/README.md), [esef](financialreport/esef/README.md), [sec](financialreport/sec/README.md), [edinet](financialreport/edinet/README.md), [combined](financialreport/combined/README.md) |

This page documents the rest: the single-file modules beside those folders,
which every data module is built on. Most of them you never call yourself --
the pipelines do -- but they are where the questions the module READMEs send
you here are answered: what a catalogue name in `paths.yaml` is, when a table
is parquet and when Delta, how a table is checked against its data contract,
where an API key comes from, and how to switch the log on.

## The modules at a glance

| Module | What it is for | Do you call it directly? |
| --- | --- | --- |
| [`addressing`](#addressing----where-a-table-lives) | reads `paths.yaml` and turns an entry into a path or a catalogue name; reads and writes Delta tables at either | rarely -- each module has `path_...`/`ref_...` helpers; yes if you write a pipeline of your own |
| [`storage`](#storage----parquet-or-delta) | reads and writes a Silver table rebuilt in full, as parquet or Delta, by path or by name | rarely -- the `file_format` argument of the updates is the user-facing part |
| [`contracts`](#contracts----what-a-table-promises) | the data contracts (ODCS) and products (ODPS) the package ships; runs their quality rules | **yes** -- `load_contract`, `spark_schema`, `check_quality` |
| [`dataquality`](#dataquality----issue-tables-and-the-catalogue-of-checks) | the machinery of the issue tables of `esef`, `sec` and `edinet`, and the catalogue of what each check means | **yes** to read issues (`describe_check`, `issue_counts`, `with_issues`); the checks themselves run inside the modules |
| [`credentials`](#credentials----where-an-api-key-comes-from) | where an API key comes from (EDINET) | no -- you only put the key where it looks |
| [`log`](#log----switching-the-log-on) | one shape for the log of every pipeline | **yes** -- `configure("INFO")` in every script or notebook |
| [`columns`](#columns----what-a-column-holds) | the catalogue of what every column holds | **yes** -- `fincommons.describe_column(...)` |
| [`spreadsheet`](#spreadsheet----tables-as-opendocument-spreadsheets) | writes tables as OpenDocument spreadsheets (`.ods`) | yes, if you want to hand a table to a reader |
| [`semantic`](#semantic----the-semantic-layer-on-databricks) | generates the semantic layer on Databricks (comments, metric views, assistant instructions) from the contracts | yes, once per deployment on Databricks |
| [`layout`](#layout----the-file-size-of-parquet-tables) | sizes the files of partitioned parquet tables; compacts small files | no for `rebalanced`; `compact_partitions` as maintenance |
| [`schemas`](#schemas----declared-spark-schemas) | loads the Spark schemas shipped as JSON in `data/` | no -- unless you read ISO 20022 XML yourself |
| [`testing`](#testing----the-shared-spark-session-of-the-test-suites) | the Spark session fixture of the test suites | only in a test suite |

None of these modules opens a Spark session or configures logging on import.
Where a function needs Spark it takes the session as its first argument; how
to build one is in [Setting up a Spark session](../../docs/spark-session.md).
The examples below that need no Spark were run as shown; those that take
`spark` assume a session and a `paths.yaml` in the working directory.

The design behind these modules is in the handbook
([`docs/handbook/fincommons.tex`](../../docs/handbook/fincommons.tex)):
the sections *Data architecture*, *Data contracts and the semantic layer*,
*Physical layout of a table*, *Naming conventions* and *Logging*.

---

## `addressing` -- where a table lives

**Reads the configuration file, `paths.yaml`, and turns one entry of it into
the address of a table: a directory, or a name in a catalogue.**

Use it when you want to know where a module keeps a table, when you want to
move a Silver layer into Unity Catalog, or when you write a pipeline of your
own that should follow the same configuration.

```yaml
# paths.yaml
exchangerate:
    bronze: /data/exchangerate/bronze
    silver: /data/exchangerate/silver
firds:
    bronze: /data/firds/bronze
    silver:
        table: fincommons.firds
```

```python
from fincommons.addressing import resolve, sql_table

ref = resolve("paths.yaml", "exchangerate", "silver", "exchangerate")
ref.path, ref.is_named        # ('/data/exchangerate/silver/exchangerate', False)
sql_table(ref)                # 'delta.`/data/exchangerate/silver/exchangerate`'

ref = resolve("paths.yaml", "firds", "silver", "main")
ref.name, ref.is_named        # ('fincommons.firds.main', True)

resolve("paths.yaml", "firds", "bronze").path   # '/data/firds/bronze', the layer itself
```

### The configuration file

- **One top-level key per data set** (`exchangerate`, `firds`, ...), and under
  it one key per layer (`landing`, `bronze`, `silver`, `gold`). Every function
  of the package takes the path of this file as `config_path`.
- **A layer given as a string is a directory.** A file that names only
  directories describes a lake addressed entirely by path.
- **A layer given as a mapping** may carry `path`, `table` or both. Carrying
  `table` is what moves it into the catalogue; where both are given, the name
  wins and the path is kept for the files the module also writes there.
- **There is no setting for the environment.** A configuration file describes
  one data lake, and the pipeline believes what it says: a file that names a
  catalogue is meant for a place where that catalogue exists.

### Catalogue names

**A catalogue name puts a table under the governance of Unity Catalog.**
Grants, row filters, lineage and Catalog Explorer all key off the name, and a
table addressed only by path is invisible to them.

- **Which layers may be named.** Only layers that hold Delta tables -- Silver
  and Gold. The file layers, up to and including Bronze, are always
  directories: there is no table there to name. A local data lake with no
  catalogue at all stays on paths.
- **The prefix is written out in full.** Unity Catalog takes
  `catalog.schema` (`main.fincommons_exchangerate`); the session catalogue of
  a plain Spark installation takes the schema alone (`fincommons_firds`). The
  table is appended to it: `resolve(..., "firds", "silver", "main")` gives
  `.../silver/main` against a directory and `fincommons.firds.main` against
  a prefix.
- **A hyphen becomes an underscore in a name** (`identifier("balance-sheet")`
  is `balance_sheet`), because Spark rejects an unquoted hyphen. The
  directory keeps its spelling.
- **A named table is always Delta**, see [`storage`](#storage----parquet-or-delta).
- **A few modules keep files in their Silver layer** -- the marks of
  `famafrenchdata`, the sector counts of `entitymasterdata` -- and then need a
  `path` beside the `table`. Their READMEs say so in their table of
  configuration keys ("May be a catalogue name?").

On Databricks, give the directories as Unity Catalog volumes
(`/Volumes/...`); the [top-level README](../../README.md#on-databricks)
explains why neither `/dbfs/...` nor a URI works.

### Renaming old folders

Every folder the pipelines name is spelled in snake_case, because the names
travel into a SQL catalogue. `rename_folders_to_snake_case(*roots)` renames a
lake laid out under the older, hyphenated names; `rename_files(pairs)` renames
files after it. Neither touches a partition directory (`DATADT=2024-06-03`), a
Delta log, or a folder named by a date, and neither merges two folders: where
both spellings exist, a `FileExistsError` asks you to decide which to keep.
The modules wrap them for their own layers (for example
`exchangerate.common.migrate_folder_names(config_path)`).

### Functions

| Function | What it does |
| --- | --- |
| `load_paths(config_path="paths.yaml")` | the parsed file, cached by modification time and size; treat the dict as read-only |
| `resolve(config_path, dataset, layer, leaf=None)` | the `TableRef` of one table, or of the layer where `leaf` is left out; `KeyError` where the data set or layer is missing |
| `TableRef(path=None, name=None)` | an address: `.path`, `.name`, `.is_named`; `str(ref)` is the name, or else the path |
| `identifier(name)` | a table name as a legal unquoted SQL identifier (hyphen to underscore) |
| `sql_table(ref)` | how the table is written in SQL: the name, or ``delta.`<path>` `` |
| `read_delta(spark, ref)` | open the Delta table at a reference |
| `read_delta_version(spark, ref, version)` | open one version of it (`versionAsOf`) |
| `delta_table_exists(spark, ref)` | whether a Delta table stands there -- asked, not inferred from a failed read |
| `delta_table(spark, ref)` | the `delta.tables.DeltaTable`, for merges and updates |
| `write_delta(df, ref, mode="overwrite", partition_by=(), options=None)` | write a frame as Delta; partition columns the frame lacks are ignored; returns `ref` |
| `rename_folders_to_snake_case(*roots)` | rename hyphenated folders below the roots; returns `(old, new)` pairs |
| `rename_files(pairs)` | rename files from old to new path, skipping missing ones; returns the pairs renamed |

---

## `storage` -- parquet or Delta

**Reads and writes the Silver tables that are rebuilt in full on every run,
and decides whether such a table is parquet or Delta.**

Several modules keep such tables -- the exchange rates, the entity master
data, the tables of `sts`, `esef`, `sec`, `edinet` and `financialreport`. The
updates of those modules take a `file_format` argument, and this module is
what decides what it means. You call it directly only to read a Silver table
generically or to write one of your own.

```python
from fincommons.storage import silver_format, read_silver, write_silver
from fincommons.addressing import resolve

ref = resolve("paths.yaml", "exchangerate", "silver", "exchangerate")
silver_format(ref)            # 'parquet' at a new path, 'delta' for a catalogue name,
                              # otherwise the format the table is stored in -- no Spark needed
df = read_silver(spark, ref)  # reads whichever format is on disk
```

### Which format you get

| The Silver layer is | `file_format=None` gives | Asking for the other format |
| --- | --- | --- |
| a path, no table there yet | parquet (`DEFAULT_SILVER_FORMAT`) | `'delta'` is taken |
| a path, table exists | the format on disk | refused with `ValueError`, before anything is written |
| a catalogue name | Delta | `'parquet'` is refused with `ValueError` |

- **A table keeps its format.** Delta will not adopt parquet files it has no
  log for, and a parquet overwrite of a Delta table silently throws its
  history away. To change the format, remove the table first.
- **The format is read off the disk** (a `_delta_log` folder means Delta),
  not asked of the caller: reading a Delta table as plain parquet does not
  fail, it returns the superseded files of earlier versions along with the
  current ones.
- **A URI is refused** (`abfss://`, `s3://`, `dbfs:/`). Python's file APIs
  cannot look into one, so a Delta table there would look absent and a
  parquet overwrite would be let through. Use a local directory or a
  `/Volumes/...` path.
- **An empty directory is not a table.** It is what `create_directory_tree`
  leaves behind.

### What each format guarantees

| | Delta (path or name) | parquet (path) |
| --- | --- | --- |
| rebuild in full (`write_silver`) | replaced in one transaction: a reader sees the old table or the new one; earlier versions stay readable | the table is deleted, then written: a reader may find it missing or half-written; nothing of the old table remains |
| replace some partitions (`replace_silver_partitions`) | one transaction with `replaceWhere` | dynamic partition overwrite; a partition being rewritten may be missing for a moment; the others are untouched |
| read by | Spark with Delta, or a reader that supports Delta | every engine -- pandas, polars, DuckDB, Arrow, R -- without a further dependency |

Parquet is the default at a path because it is the format every engine reads;
choose Delta where a concurrent reader must never see a fraction of a run, or
where earlier versions should stay readable. Off Databricks a Delta table
needs the session to be built with the Delta extensions (the `local` extra
brings `delta-spark`); see [Setting up a Spark session](../../docs/spark-session.md).
Point a non-Spark reader at a table rebuilt in full rather than one updated in
place, since a table carrying deletion vectors is read correctly only by a
reader that supports them.

Details a write takes care of:

- the overwrite is always a full one, whatever the session's
  `spark.sql.sources.partitionOverwriteMode` says;
- a Delta write replaces the schema with the frame's (`overwriteSchema`) and
  sets `delta.autoOptimize.optimizeWrite` on the table (`OPTIMIZE_WRITE`), so
  that it is not stored as many small files; a table in the catalogue stays
  the same table, so its grants survive the write;
- in `replace_silver_partitions`, a partition named in `values` that the frame
  has no row for is emptied as Delta and left as it was as parquet -- leave a
  piece you have nothing to write for out of `values`.

### Functions

Every function takes a `TableRef` from `resolve` or a plain path string.

| Function | What it does |
| --- | --- |
| `silver_format(ref, file_format=None)` | the format a write would use, or `ValueError` saying why it may not be written; needs no Spark |
| `read_silver(spark, ref)` | read the table in whichever format it is; `FileNotFoundError` (path) or `LookupError` (name) where none exists yet |
| `write_silver(df, ref, file_format=None, partition_by=())` | replace the table with the frame |
| `replace_silver_partitions(df, ref, column, values, file_format=None)` | replace the partitions of `column` named in `values`, leave the rest; creates the table where missing |
| `silver_table_exists(spark, ref)` | whether a table stands there, in either format |
| `file_format_on_disk(path, default="parquet")` | `'delta'`, `'parquet'`, or `default` where nothing is there |
| `check_file_format(file_format)` | refuse anything but `'parquet'` or `'delta'` |
| `check_local_path(path)` | refuse a URI |

Constants: `SILVER_FORMATS` (`("parquet", "delta")`), `DEFAULT_SILVER_FORMAT`
(`"parquet"`), `OPTIMIZE_WRITE`.

---

## `contracts` -- what a table promises

**Loads the data contracts the package ships, gives you the Spark schema of a
table, and runs the quality rules a contract states against the table.**

A data contract states, in a form a machine can read, what a Silver table
holds: the name, type and meaning of every column, which columns identify a
row, how a measure may be aggregated, and the rules the data is held to. The
contracts are written in the Open Data Contract Standard (ODCS, pinned at
`v3.2.0`); each module is described as a data product in the Open Data
Product Standard (ODPS, `v1.1.0`), whose output ports name the contracts of
its tables.

Use it when you want to know what a table promises, to build a frame with the
exact schema of a table, or to check a table you read -- or wrote -- against
its rules.

```python
from fincommons.contracts import load_contract, spark_schema

contract = load_contract("fincommons.exchangerate.silver.exchangerate")
spark_schema(contract).simpleString()
# 'struct<ref_date:date,base:string,quote:string,value:double,type:string,source:string,imputed:boolean>'
```

```python
from fincommons import exchangerate as ex
from fincommons.contracts import check_quality, load_contract, quality_failures

results = check_quality(ex.connect_to_exchangerate(spark, "paths.yaml"),
                        load_contract("fincommons.exchangerate.silver.exchangerate"))
for r in results:
    print(r.rule, r.severity, r.value, r.passed)
quality_failures(results)                          # [] where every error-level rule held
quality_failures(results, ("error", "warning"))    # warnings as well
```

### Where the contracts are

- In [`data/`](data/), one file per table, `contract_<module>_<layer>_<table>.yaml`,
  and one per module, `product_<module>.yaml`.
- A contract's identifier is `<package>.<module>.<layer>.<table>`, e.g.
  `fincommons.sts.silver.notification` -- the logger name of the module
  followed by the table, so a reader can find the code from the contract.
  `contracts()` lists every one shipped.
- Products: one per module -- `fincommons.exchangerate`, `marketpricedata`,
  `firds`, `gleif`, `entitymasterdata`, `sts`, `famafrenchdata`, `esef`,
  `sec`, `edinet` and `financialreport` (`products()`); every Silver table,
  and the FIRDS watermarks, has a contract (thirty in all).
- A package that builds on fincommons ships its own and passes
  `packages=("fincommons", "<its package>")` to every function here.

### What a contract states

- **It is the single source of truth for its columns.** The column catalogue
  ([`columns`](#columns----what-a-column-holds)) takes the descriptions of
  those tables from the contract, and the test suite holds contract and code
  against each other in both directions: a column a module writes must be in
  its contract, and a column in the contract must be written. A helper column
  dropped before writing starts with an underscore and is in no contract.
- **It is a template, not a deployment.** A shipped contract states no
  server, service level, team, support channel, price or roles
  (`DEPLOYMENT_SECTIONS`); where a table lives is the business of `paths.yaml`.
- **The framework's own properties** are ODCS custom properties with
  `vendor: fincommons` (`CUSTOM_PROPERTIES`):
  - `kind` -- identifier, code, text, date, datetime, amount, currency, rate,
    flag, count, quantity, number, weight or other; and `source` -- the field
    a column is read from, or `derived`. Both are required on every column.
  - `aggregation`, on a measure (`AGGREGATIONS`): `additive` may be summed
    over every dimension; `semiAdditive` over every dimension but time, and
    over a period it is read at the last date, named in `timeProperty` (the
    total assets of a balance sheet); `nonAdditive` is never summed and only
    averaged within the groups `aggregateWithin` names (an exchange rate,
    within one currency pair). An additive or semi-additive measure may name
    `aggregateWithin` too, where it may be summed only within one value of
    those columns (a percentage of one issuer's capital).
  - `currencyProperty` -- the column holding an amount's currency; an amount
    that is a measure must name it.
- **A draft is a specification.** A contract whose `status` is `proposed` or
  `draft` (`DRAFT_STATUSES`) describes a table no code writes yet; it is
  validated, but left out of the column catalogue and the semantic layer.

### Quality rules

A contract can state rules on the table and on each column. `check_quality`
runs two of the four ODCS rule types with Spark and returns one
`QualityResult` per rule -- the table's rules first, then each column's, in
the order of the contract:

| Rule type | How it is run |
| --- | --- |
| `library` | a predefined metric compared with a threshold. On a column: `nullValues`, `missingValues`, `invalidValues` (with `validValues` or a `pattern`), `duplicateValues`. On the table: `rowCount`, and `duplicateValues` over the columns in `arguments.properties` |
| `sql` | a query whose first value is compared the same way; `${table}` and `${column}` stand for the table and the column |
| `custom`, `text` | not run: listed with `value` and `passed` set to `None` |

- **Comparisons:** exactly one of `mustBe`, `mustNotBe`, `mustBeGreaterThan`,
  `mustBeGreaterOrEqualTo`, `mustBeLessThan`, `mustBeLessOrEqualTo`,
  `mustBeBetween`, `mustNotBeBetween`. With `unit: percent` the count is
  turned into a percentage of the rows.
- **Every rule run** has a name unique in its contract, a `dimension`
  (completeness, uniqueness, conformity, accuracy, ...) and a `severity` of
  `error`, `warning` or `info` (`QUALITY_SEVERITIES`, most severe first, in
  the same order as `dataquality.SEVERITIES`).
- **`QualityResult`** carries `contract`, `rule`, `column` (`None` for a rule
  of the table), `dimension`, `severity`, `description`, `value` (what was
  measured) and `passed`.

A rule from `data/contract_exchangerate_silver_exchangerate.yaml`:

```yaml
quality:
  - name: one_rate_per_day_and_pair
    description: No day and currency pair has more than one rate.
    type: library
    metric: duplicateValues
    arguments:
      properties: [ref_date, base, quote]
    mustBe: 0
    dimension: uniqueness
    severity: error
```

Contract rules and the issue tables of [`dataquality`](#dataquality----issue-tables-and-the-catalogue-of-checks)
are complementary: a rule answers with one number about a table as a whole
(no key is repeated, no rate is negative), an issue table with one row per
record a check finds at fault.

### Functions

| Function | What it does |
| --- | --- |
| `load_contract(identifier, packages=("fincommons",))` | one contract, as a dict; `KeyError` naming those that exist |
| `contracts(packages=...)`, `products(packages=...)` | every contract or product shipped, keyed by identifier |
| `spark_schema(contract)` | the table's schema as a `StructType`, in contract order; nullable unless `required` |
| `check_quality(frame, contract)` | run the contract's rules on a frame; list of `QualityResult` |
| `quality_failures(results, severities=("error",))` | the results that failed with one of `severities` |
| `custom_property(element, name, default=None)` | a `vendor: fincommons` custom property of a column or table |
| `catalogue_rows(packages=...)` | the columns of all active contracts as rows of the column catalogue |
| `validate_contract(contract, file_name=None)` | check a contract against the ODCS JSON schema and the framework's rules; `ContractError` listing every problem |
| `validate_product(product, packages=...)` | check a product against ODPS and the contracts shipped: an output port names its contract's exact version, an input port the same major version and no older a minor and patch |

`validate_contract` and `validate_product` need `jsonschema`, which the `dev`
extra installs; reading a contract needs only the YAML reader. Constants:
`ODCS_VERSION`, `ODPS_VERSION`, `VENDOR`, `CUSTOM_PROPERTIES`, `AGGREGATIONS`,
`DEPLOYMENT_SECTIONS`, `DRAFT_STATUSES`, `QUALITY_SEVERITIES`; the exception
`ContractError` (a `ValueError`).

---

## `dataquality` -- issue tables and the catalogue of checks

**The shared machinery behind the issue tables of `esef`, `sec` and
`edinet`, and the catalogue that says what each check code means.**

A pipeline brings data in as the publisher gave it out; whether it is any good
is a separate question. A module that checks its data writes one Silver table
of issues beside the tables it checked, one row per record a check found at
fault, carrying the code of the check (`issue`) and the day it was made
(`checked`). Nothing is dropped and nothing is corrected: a filter would leave
a table that looks clean and a population that is not the published one.

Use it when you read an issue table: to turn a code into a sentence, to count
the issues, or to label records with them.

```python
from fincommons.dataquality import check_catalogue, describe_check

print(describe_check("RESTATED_MATERIALLY"))
# RESTATED_MATERIALLY (warning) -- A later report states a different figure for the same period
#   data set  sec
#   means     An annual report filed later states a figure ...
#   remedy    Read both filings. ...

describe_check("IMBAL_BALANCE_SHEET", dataset="esef")   # a code several data sets define
check_catalogue(dataset="sec", severity="error")         # a pandas frame
```

```python
from fincommons.financialreport import sec
from fincommons.dataquality import issue_counts, with_issues

issues = sec.connect_to_sec(spark, "paths.yaml", "issues")
issue_counts(issues, dataset="sec").show()               # one row per check, severest first
facts = sec.connect_to_sec(spark, "paths.yaml", "facts")
with_issues(facts, issues, ["cik", "filing", "fact"])    # one column of joined codes per record
```

### The catalogue of checks

- **It is data, not code:** [`data/dataquality_checks.csv`](data/dataquality_checks.csv),
  one row per check, with the columns `dataset`, `code`, `severity`, `title`,
  `meaning` and `remedy` (`CHECK_CATALOGUE_COLUMNS`). It currently holds 19
  checks of `esef`, 16 of `sec` and 26 of `edinet`.
- **Severities** (`SEVERITIES`): `error`, a record that cannot be right (a
  balance sheet that does not balance); `warning`, one that may be right and
  is worth a look (a restatement); `info`, one only worth counting. The
  severity is this package's opinion, not anybody's rule.
- **A check judges a filing, not an issuer.**
- **The checks themselves live with their data set**, in
  `financialreport.esef.quality`, `financialreport.sec.quality` and
  `financialreport.edinet.quality`, because what a check looks at depends on
  the data set. A deployment with checks of its own ships a catalogue of the
  same shape in its own package's `data/` and passes `packages=`.

### How a module runs its checks

`update_silver_esef_quality` and its counterparts follow one pattern, which a
check of your own can follow too:

1. `run_checks(frame, checks, keys, catalogue_dataset=...)` runs each
   `(code, function)` pair. A function returns the rows it objects to; only
   `keys` and the code are kept, so two checks objecting to one record give two
   rows. A code the catalogue does not define fails the run. `frame` is
   handed to each function as it is, so it may be a mapping of several
   frames where a check needs more than one table, as the financialreport
   quality modules do.
2. `merged_issues(found, existing, column, rechecked)` keeps the issues of the
   partitions the run did not look at, and drops the old rows of those it did,
   so a fixed problem stops being reported.
3. The result is written with `fincommons.storage.write_silver`, partitioned by
   `partitioning(issues, column)` -- not partitioned where it is empty, since
   an empty partitioned parquet write leaves no file to read a schema from.
4. `outstanding_partitions(source, issues, column)` says which partitions no
   run has looked at yet.

```python
from pyspark.sql import functions as F
from fincommons.dataquality import run_checks
from fincommons.financialreport import esef

facts = esef.connect_to_esef(spark, "paths.yaml")
negative = lambda f: f.where((F.col("concept") == "ifrs-full:Revenue") & (F.col("value") < 0))
found = run_checks(facts, [("NEGATIVE_REVENUE", negative)], keys=["filing", "fact"])
```

(Without `catalogue_dataset` the code need not be in the catalogue; with it,
it must.)

### Functions

| Function | What it does |
| --- | --- |
| `check_catalogue(dataset=None, severity=None, packages=...)` | the catalogue as a pandas frame, ordered by data set, severity, code |
| `describe_check(code, dataset=None, packages=...)` | one check, printable (`CheckDescription`); `ValueError` where several data sets define the code and `dataset` is not given |
| `checks_of(dataset, packages=...)` | the codes of one data set, severest first |
| `issue_counts(issues, by=None, packages=..., dataset=None)` | one row per code (and `by`) with severity, title and `records` |
| `with_issues(records, issues, keys, separator="+")` | the records with one `issue` column of their codes, null where nothing objected |
| `suppressed(issues, exceptions, keys=None)` | the issues left once known exceptions (a Spark or pandas frame carrying `issue`) are taken out |
| `run_checks(frame, checks, keys, catalogue_dataset=None, packages=...)` | gather the rows every check finds into one frame of issues |
| `outstanding_partitions(source, issues, column)` | partition values in the source that the issue table does not mention |
| `merged_issues(found, existing, column, rechecked)` | this run's findings plus the old rows of the partitions it did not check |
| `partitioning(issues, column)` | `(column,)`, or `()` for an empty frame |

Constants: `SEVERITIES`, `CHECK_CATALOGUE_FILE`, `CHECK_CATALOGUE_COLUMNS`,
`ISSUE_COLUMN` (`"issue"`), `CHECKED_COLUMN` (`"checked"`).

---

## `credentials` -- where an API key comes from

**The one place that says where an API key is found, for the sources that
refuse a request without one.**

Almost everything fincommons reads is public. The EDINET API of Japan's
Financial Services Agency is not: it needs a registered key, and
[`financialreport.edinet`](financialreport/edinet/README.md) reads it through
this module. Other keyed sources -- Korea's OpenDART API, for instance, which
no module reads yet -- would get their key the same way, here or in a package
built on fincommons.

You do not call it; you put the key where it looks. Three places, first found
wins:

1. **the `key` argument** of the update, for a notebook or a test;
2. **the environment variable `FINCOMMONS_<NAME>_KEY`**, e.g.
   `FINCOMMONS_EDINET_KEY` -- what a scheduler sets, and what a secret scope
   becomes on Databricks;
3. **a file named `<name>`** (e.g. `edinet`) in the directory the environment
   variable `FINCOMMONS_VAULT` names -- the shape a Kubernetes or Docker
   secret already has, and one that keeps the key out of every process's
   environment.

```bash
export FINCOMMONS_VAULT=/run/secrets        # one file per key, mode 600
printf '%s' "$EDINET_KEY" > /run/secrets/edinet
```

```python
from fincommons.credentials import api_key, redacted

api_key("edinet", key="ZZZ")          # 'ZZZ'
api_key("edinet")                      # from the environment or the vault; ValueError naming
                                       # all three places where none holds one
redacted("https://example.org/a?date=2026-01-01&Subscription-Key=ZZZ")
# 'https://example.org/a?date=2026-01-01&Subscription-Key=***'
```

- **Never put a key in `paths.yaml`.** That file is committed with a
  deployment, handed to every module and read by every Quarto template, so a
  key in it would travel with the repository. `api_key` deliberately never
  looks there.
- **A key is never logged.** EDINET takes its key as a query parameter, so
  the address of a request carries the secret; `redacted` replaces the value
  of the parameters in `SECRET_PARAMETERS` (`Subscription-Key`, `crtfc_key`,
  `api_key`, `apikey`, `key`, `token`, ...) before an address is logged or
  recorded.
- The key is stripped of surrounding whitespace, so a file ending in a
  newline works.

| Function | What it does |
| --- | --- |
| `api_key(name, key=None, env=None, vault=None)` | the key, from the argument, the environment (or the variable `env` names) or the vault (or the directory `vault` names) |
| `vault_path(name, vault=None)` | the file a key would be read from, or `None` where no vault is set; `name` is limited to lower-case letters, digits, `-` and `_` |
| `redacted(url)` | the address with any key replaced by `***` |

Constants: `KEY_ENVIRONMENT` (`"FINCOMMONS_{name}_KEY"`), `VAULT_ENVIRONMENT`
(`"FINCOMMONS_VAULT"`).

---

## `log` -- switching the log on

**Gives the log of every pipeline one shape, and lets a script or notebook
switch it on with one call.**

The package logs through the standard library's `logging`, under the logger
tree `fincommons.<module>.<layer>`, and never configures logging on import.
Until you call `configure`, a pipeline is silent.

```python
from fincommons.log import configure

configure("INFO")        # to standard error; "DEBUG" for the detail to reproduce a run
```

```
2026-09-08 03:14:22 INFO     fincommons.firds.silver  Applied delta file  module=firds layer=silver table=MAIN datadt=2026-09-04
```

- **The handler goes on the `fincommons` logger, not on the root logger**, and
  propagation is switched off. That is why a local run and a run on
  Databricks give the same log: the Databricks driver already has a root
  handler, and configuring that one would write every record twice.
- **The call is idempotent.** A second call adjusts the level and leaves the
  handler alone; `force=True` replaces it (useful in a notebook re-run against
  another stream). It also sets `py4j` to `WARNING`, which is otherwise the
  loudest thing in a `DEBUG` log.
- **A package built on fincommons** configures its own logger tree with
  `configure(..., logger_name="<its package>")`.

### What the levels mean

| Level | Meaning |
| --- | --- |
| `DEBUG` | detail for reproducing a run; off by default |
| `INFO` | the record of what a run did: stages, durations, reference dates, rows written |
| `WARNING` | work skipped or degraded while the result stayed valid |
| `ERROR` | a unit of work failed and was set aside while the run carried on by design; always with its traceback |
| `CRITICAL` | not used: a problem that invalidates the run raises instead |

Logging is never how a pipeline decides to carry on. `print` keeps its place:
result tables a script writes for someone watching the console go to standard
output, the log to standard error.

### Logging from code of your own

Whatever a record may later be filtered or grouped by goes into the context,
not into the sentence, so the message stays a constant string. The
vocabulary the pipelines use: `module`, `layer`, `source`, `table`, `datadt`,
`refdt`, `file`, `rows`, `elapsed`.

```python
import argparse
import logging

from fincommons.log import add_log_level_argument, configure, context, pipeline_run

parser = argparse.ArgumentParser()
add_log_level_argument(parser)                 # --log-level DEBUG|INFO|WARNING|ERROR
args = parser.parse_args()
configure(args.log_level)

logger = logging.getLogger("fincommons.mymodule.silver")
logger.info("Applied delta file", extra=context(datadt="2026-09-04", rows=18432))

with pipeline_run(logger, "update_silver", module="mymodule") as outcome:
    outcome["rows"] = 18432
# ... INFO ... update_silver started  stage=update_silver module=mymodule
# ... INFO ... update_silver finished  stage=update_silver module=mymodule rows=18432 elapsed=0.0s
```

`pipeline_run` logs a failure with its traceback and re-raises it; it never
decides that a run may carry on. A context value of `None` is dropped, and a
value with a space is quoted.

| Name | What it is |
| --- | --- |
| `configure(level="INFO", stream=None, force=False, logger_name="fincommons")` | attach a handler to the package's logger; returns the logger |
| `context(**values)` | the `extra=` argument of a log call |
| `pipeline_run(logger, stage, **values)` | context manager: a `started` and a `finished` (or `failed`) record, with the outcome and `elapsed` |
| `add_log_level_argument(parser)` | add `--log-level` to an `argparse` parser |
| `ContextFormatter` | the formatter that appends the context as `key=value` pairs, before any traceback |

Constants: `LOGGER_NAME` (`"fincommons"`), `DEFAULT_FORMAT`,
`DEFAULT_DATE_FORMAT`.

---

## `columns` -- what a column holds

**Answers "what is this column?" for every column of every catalogued data
set, without Spark or a data lake.**

```python
import fincommons

print(fincommons.describe_column("trading_venue"))
# trading_venue in firds -- tables firds.silver
#   kind         code
#   source       TradgVnRltdAttrbts.Id
#   description  Trading venue: the MIC of the venue the instrument is admitted to or traded on (RTS 23 field 6).

fincommons.describe_column("action_type", dataset="firds")   # one data set only
fincommons.column_catalogue("exchangerate")                  # every column of a data set, as pandas
```

- **A name may mean different things in different data sets**, so every data
  set that has the column is described unless `dataset` narrows it. Case does
  not matter; a name that is not found raises a `KeyError` suggesting similar
  ones.
- **The data contracts are the catalogue.** Every Silver table, and the
  FIRDS watermarks, has a contract
  ([`contracts`](#contracts----what-a-table-promises)), and
  `column_catalogue` reads one row per data set and column out of them: the
  tables that write it, the source field or `derived`, a kind and a
  description. The contract is the single source of truth for a column; the
  `data/column_catalogue.csv` the package shipped before its tables had
  contracts is gone.
- **The catalogue cannot drift from the code.** `defined_columns` reads the
  column names out of the modules' source (`withColumn`, `alias`,
  `StructField`, ...), and a test fails where a column has no description.
- **Two kinds of name are not held to a contract.** A Bronze table keeps the
  columns its publisher delivers; the few the package adds there (`DATADT`
  of FIRDS, `REFDT` of the entity registers, the columns of the Bronze copy of
  the French Data Library) are in upper case and described where the module
  writes them. And a column a public function returns beside those of a table
  -- `measure_priority`, for instance -- is described by that function's
  docstring; `OUTPUT_COLUMNS` names them.
- **`marketpricedata` is not catalogued:** its two Silver tables have
  contracts, but what matters in its table is the series identifier -- see
  `marketpricedata.describe_instrument`.
- A package that builds on fincommons ships contracts of its own, or still a
  `data/column_catalogue.csv` for the modules it has not given contracts yet,
  and passes `catalogues={"fincommons": CATALOGUED_MODULES, "<its package>": (...)}`.

| Name | What it is |
| --- | --- |
| `describe_column(name, dataset=None, catalogues=None)` | the descriptions of one column, one `ColumnDescription` per data set; printable |
| `column_catalogue(dataset=None, catalogues=None)` | the catalogue as a pandas frame: `module`, `column`, `tables` (`;`-separated), `source`, `kind`, `description` |
| `defined_columns(catalogues=None, schema_declaring=..., snake_case=...)` | the columns the modules' code defines, read from the source -- what the tests hold the catalogue to |
| `ColumnDescription` | `column`, `dataset`, `tables`, `source`, `kind`, `description` |

Constants: `CATALOGUED_MODULES`, `CATALOGUES`, `CONTRACTED_MODULES`,
`OUTPUT_COLUMNS`, `SNAKE_CASE_MODULES`, `SCHEMA_DECLARING_MODULES`,
`MODULE_FOLDERS` (where a data set's code lives
when its folder is not its name, e.g. `esef` in `financialreport/esef`),
`NOT_COLUMNS`. `describe_column` and `column_catalogue` are also importable
from `fincommons` itself.

---

## `spreadsheet` -- tables as OpenDocument spreadsheets

**Writes tables as an OpenDocument spreadsheet (`.ods`) -- the ISO/IEC 26300
standard that LibreOffice, Excel, Google Sheets and others open -- from the
standard library alone.**

Use it when a table leaves the lake for a person rather than a program.
[`financialreport.esef`'s `export_esef_reports`](financialreport/esef/README.md#one-issuers-reports-as-a-spreadsheet)
is built on it.

```python
from datetime import date
from fincommons.spreadsheet import Sheet, write_ods

write_ods("revenue.ods", [
    Sheet("revenue", ["issuer", "period_end", "revenue"], [
        ("voestalpine AG", date(2026, 3, 31), 16_000_000_000.0),
        ("Issuer AG", date(2025, 12, 31), None),
    ]),
    Sheet("notes", None, [("Figures in euro, as reported.",)]),   # no header row
], title="Revenue")
```

A pandas frame goes in as `Sheet("name", list(df.columns), df.itertuples(index=False))`;
a Spark frame through `toPandas()` first, or its `collect()`.

- **Types are kept.** A number is written as a number, a date as a date
  (shown `2026-03-31`), a `datetime` as a date and time, a `bool` as a
  boolean, anything else as text -- so a column of figures sums and a column
  of dates sorts.
- **Empty means empty.** `None`, `""`, `NaN`, infinity, pandas' `NaT` and `NA`
  are all written as an empty cell.
- **The header** is bold, declared a header row (repeated on every printed
  page) and frozen on screen. A sheet with `header=None` has none of that.
- **Widths** follow the content, between 1.8 and 14 cm.
- **No formulas.** A text starting with `=` stays text; the formula
  injection a CSV export has to guard against does not arise.
- **Sheet names** are cleaned of `[]*?:/\'"` and made unique, without regard
  to case (`sheet_name`).
- **The file is written whole or not at all** (to `<path>.partial`, then put
  in place); `.ods` is appended where missing and directories are created.
  Pass `created=` for byte-identical output.

| Name | What it is |
| --- | --- |
| `write_ods(path, sheets, *, created=None, title=None, generator=None)` | write the sheets; returns the path |
| `Sheet(name, header=None, rows=())` | one sheet; `rows` is any iterable of sequences, read once (a generator works) |
| `sheet_name(name, taken=())` | a name a spreadsheet accepts and not already in `taken` |

Constants: `MEDIA_TYPE`, `SUFFIX` (`".ods"`).

---

## `semantic` -- the semantic layer on Databricks

**Turns the data contracts into what Databricks needs to serve the tables to
dashboards and assistants: comments on the tables and columns, a metric view
per table, and instructions for an assistant such as Genie.**

A contract says what a table holds; the semantic layer says how its values may
be combined -- which columns to group by, which to aggregate, and how. Both
are derived from the same contract, so the meaning of a column and the rule
for aggregating it are stated once.

Use it once per deployment on Databricks, after a module's Silver layer has
been written, and again when the contracts change. **The Silver layer must be
addressed by catalogue name** (see [`addressing`](#catalogue-names)); a layer
addressed by path is refused, since a metric view describes a table in a
catalogue. A package whose modules write by path in a layout of their own
passes `locate=`, a function that returns a `Location(name, path,
format="delta", partitioned=False)` for a contract: the files are registered
as an external table under that name (`CREATE TABLE IF NOT EXISTS ... USING
DELTA|PARQUET LOCATION ...`, and `MSCK REPAIR TABLE` for a partitioned
Parquet table) before they are described.

```yaml
# paths.yaml on Databricks
exchangerate:
    bronze: /Volumes/main/fincommons_exchangerate/lake/bronze
    silver:
        table: main.fincommons_exchangerate
```

```python
from fincommons import semantic

generated = semantic.databricks("paths.yaml", "fincommons.exchangerate")   # text only, runs anywhere
for statement in generated["statements"]:
    print(statement)                       # COMMENT ON TABLE ..., ALTER TABLE ... ALTER COLUMN ...,
                                           # CREATE OR REPLACE VIEW ..._metrics WITH METRICS ...
print(generated["assistant"])              # Markdown for the assistant

instructions = semantic.apply_databricks(spark, "paths.yaml", "fincommons.exchangerate")  # on Databricks
```

- **Generating writes nothing.** `databricks` returns text and needs no
  connection, so it can be reviewed and tested on a workstation;
  `apply_databricks` runs the statements one at a time in a Spark session on
  the platform, so a statement the platform refuses is named by the error.
  The assistant instructions are returned, not applied: paste them into the
  assistant's own interface.
- **What is generated, per output port of the product** (drafts excepted):
  the comments from the contract's descriptions, and a metric view
  `<table>_metrics` beside the table (change the suffix with `suffix=`),
  written in version 1.1 of the metric view specification.
- **How the aggregation rules become measures:**
  - a dimension becomes a field, with its business name as display name and
    its synonyms (at most ten);
  - a measure is named after its business name in snake_case
    (`exchange_rate`, not `value`), its expression the contract's
    `transformLogic`;
  - `semiAdditive`: a window on `timeProperty` that reads the measure at the
    last date of a period;
  - any measure with `aggregateWithin` (required for `nonAdditive`) or a
    `currencyProperty`: the expression only where each guarding column holds
    at most one distinct value in the group (`COUNT(DISTINCT c) <= 1`), and
    empty otherwise -- a rate averaged over several currency pairs, or
    amounts in several currencies added up, come back empty rather than
    wrong, while a group whose currency is empty throughout is still read.
- **The assistant instructions** come from the product's ODPS `context`
  (instructions, constraints, sample questions) and from the contracts: what
  each table holds, its key, and the aggregation rules as "Never ..."
  constraints.

| Function | What it does |
| --- | --- |
| `databricks(config_path, product_id, packages=("fincommons",), suffix="_metrics", locate=None)` | `{"statements": [...], "assistant": "..."}` for one product |
| `apply_databricks(spark, config_path, product_id, packages=..., suffix="_metrics", locate=None)` | run those statements; returns the assistant instructions |
| `Location(name, path, format="delta", partitioned=False)` | where a table written by path lives, for `locate=` |
| `register_statements(location)` | the statements that register a table written by path under its name |
| `metric_view(contract, source)` | the YAML definition of a metric view over the table `source` |
| `create_metric_view(contract, source, view)` | the `CREATE OR REPLACE VIEW ... WITH METRICS` statement |
| `comment_statements(contract, table_name)` | the `COMMENT ON TABLE` and `ALTER COLUMN ... COMMENT` statements |
| `assistant_instructions(product, packages=...)` | the Markdown instructions for an assistant |

Constant: `METRIC_VIEW_VERSION` (`"1.1"`). Product identifiers are those of
`fincommons.contracts.products()`, e.g. `fincommons.exchangerate`.

---

## `layout` -- the file size of parquet tables

**Keeps partitioned parquet tables from turning into thousands of tiny
files.**

A frame that has been through joins is spread over
`spark.sql.shuffle.partitions` tasks (200 by default), and a partitioned write
produces one file per task and partition -- a single day can arrive as two
hundred files of a fraction of a megabyte. A query pays per file, not per row.

- **`rebalanced(df, *columns)`** is applied by the pipelines before every
  partitioned parquet write (FIRDS Bronze, the Yahoo, DSE and VNDirect
  sources of `marketpricedata`, and the tables of `financialreport`). It adds
  Spark's `rebalance` hint, and adaptive query execution then sizes the files
  to about `spark.sql.adaptive.advisoryPartitionSizeInBytes`. You only use it
  in a pipeline of your own.
- **`compact_partitions(spark, path, target_file_size=TARGET_FILE_SIZE)`** is
  maintenance you run yourself: it rewrites every partition of a parquet table
  that holds more than `ceil(size / 128 MB)` files, and leaves the others
  alone, so it can be rerun at any time.

```python
from fincommons.firds import common
from fincommons.layout import compact_partitions

result = compact_partitions(spark, common.path_bronze_auth_017_001_02("paths.yaml"))
print(result["files_before"], "->", result["files_after"], "files;",
      len(result["failed"]), "failed")
```

- A partition is rewritten into `.compact_new_<name>` beside it, its rows are
  counted against the original, and only then swapped in. The swap is two
  renames, not a transaction: run it while the pipeline that writes the table
  does not, and nothing important reads it. A run that died mid-swap is
  completed by the next.
- Files are read with `mergeSchema`; a partition whose files disagree on a
  column's type is left as it was, logged at `ERROR` and listed in `failed`.
- **Delta tables are refused** -- compact them with `OPTIMIZE`, which keeps
  the transaction log in step. `storage.write_silver` sets `optimizeWrite` on
  every Delta table it writes.

| Name | What it is |
| --- | --- |
| `rebalanced(df, *columns)` | `df` with the `rebalance` hint on the partition columns |
| `compact_partitions(spark, path, target_file_size=TARGET_FILE_SIZE)` | compact a parquet table; returns `compacted`, `files_before`, `files_after`, `failed` |
| `TARGET_FILE_SIZE` | 128 MB |

The measurements and the reasoning:
[`docs/partitioned_parquet_file_sizing.md`](../../docs/partitioned_parquet_file_sizing.md).

---

## `schemas` -- declared Spark schemas

**Loads the Spark schemas that ship as JSON files in [`data/`](data/), so
that semi-structured sources are read under a declared schema rather than an
inferred one.**

FIRDS reads its ISO 20022 XML files with them (`firds_schema_017_001_02`,
`firds_schema_036_001_03`, `firds_schema_102_001_01`), and `marketpricedata`
its ECB answers (`marketpricedata_ecb_estr`, `marketpricedata_ecb_fm`). You
need it only to read such a file yourself. The files are generated from an
XSD or an ECB answer by the scripts in [`tools/`](../../tools/README.md).

```python
from fincommons.schemas import load_pyspark_schema

df = spark.read.format("xml") \
    .options(rowTag="FinInstrm") \
    .schema(load_pyspark_schema("firds_schema_036_001_03")) \
    .load("path/to/file.xml")
```

| Function | What it does |
| --- | --- |
| `load_pyspark_schema(schema, package=None)` | the `StructType` stored as `data/<schema>.json` |
| `load_replacement_table(table, package=None)` | the JSON stored as `data/<table>.json`, as Python objects -- a lookup table for a data set; fincommons ships none itself, packages built on it do |

`package` names another package whose `data/` holds the file; by default this
one. Only `load_pyspark_schema` needs PySpark, which it imports when called;
`load_replacement_table` works without it.

---

## `testing` -- the shared Spark session of the test suites

**Provides the `spark` pytest fixture, so that the suites of fincommons and of
packages built on it run against one session with one configuration.**

```python
# conftest.py
from fincommons.testing import spark  # noqa: F401
```

- The session is `local[2]` with two shuffle partitions, no UI, and the Delta
  extensions where `delta-spark` is installed (the `local` and `dev` extras);
  without it, only the tests that touch Delta fail.
- Importing the module sets `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to
  the interpreter running the tests, and `PYSPARK_SUBMIT_ARGS` to an 8 GB
  driver heap (`DRIVER_MEMORY`) -- each only where not already set. A suite of
  some 1,400 tests ran out of 4 GB.
- It is meant for test suites only, because of those environment variables.

How to run the suite is in the [top-level README](../../README.md#tests) and
[`CONTRIBUTING.md`](../../CONTRIBUTING.md).

---

## Stability

The names that packages built on fincommons import are listed in
[`tests/test_public_api.py`](../../tests/test_public_api.py); removing or
renaming one is a breaking change and calls for a new major version. Each
module here has its own tests in [`tests/`](../../tests/) (`test_addressing_folders.py`,
`test_storage.py`, `test_contracts.py`, `test_dataquality.py`,
`test_credentials.py`, `test_log.py`, `test_columns.py`,
`test_spreadsheet.py`, `test_semantic.py`, `test_layout.py`).
