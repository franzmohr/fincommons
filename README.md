# fincommons

[![CI](https://github.com/franzmohr/fincommons/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/franzmohr/fincommons/actions/workflows/ci.yml)
[![Handbook](https://github.com/franzmohr/fincommons/actions/workflows/handbook.yml/badge.svg?branch=main)](https://github.com/franzmohr/fincommons/actions/workflows/handbook.yml)
[![codecov](https://codecov.io/gh/franzmohr/fincommons/branch/main/graph/badge.svg)](https://codecov.io/gh/franzmohr/fincommons)
[![Python](https://img.shields.io/badge/python-%E2%89%A53.11-blue)](pyproject.toml)
[![License: EUPL-1.2](https://img.shields.io/badge/license-EUPL--1.2-blue)](LICENSES/EUPL-1.2.txt)
[![REUSE compliant](https://img.shields.io/badge/REUSE-compliant-green)](https://reuse.software/)

Shared infrastructure for basic financial data work.

fincommons downloads public financial data -- exchange rates, market prices,
instrument and entity reference data, securitisation registers, factor
returns, XBRL financial reports -- and turns each into clean, documented
tables you can query with Spark, pandas, polars or any engine that reads
parquet. It runs unchanged on a workstation, on a shared server and on
Databricks.

The library is intended to incite public debate and identify best practice
approaches for handling basic data pipelines.

**At a glance**

- One Python package, one module per data set; each module stands on its own.
- Every path it reads or writes comes from one configuration file, `paths.yaml`.
- Data flows through the layers of a medallion architecture -- see
  [Concepts](#concepts-in-two-minutes).
- Built on Spark and Delta Lake; reading the results needs neither.
- Licensed under the EUPL 1.2; any use, commercial included, is allowed.

## What is in it

| Module | Use it to | README |
| --- | --- | --- |
| `exchangerate` | convert amounts between currencies with the reference rates of the ECB and the BIS, combined into one table per calendar day and currency pair | [exchangerate](src/fincommons/exchangerate/README.md) |
| `marketpricedata` | get prices and rates from public publishers (ECB, Federal Reserve Bank of New York, Bank of England, Yahoo Finance and others) in one table, and the risk-free zero curves its overnight and swap rates imply | [marketpricedata](src/fincommons/marketpricedata/README.md) |
| `firds` | look up the reference data of any instrument traded on an EU venue (ESMA's FIRDS): download, import, and the historical database ESMA's instructions describe | [firds](src/fincommons/firds/README.md) |
| `sts` | analyse ESMA's public register of the securitisations notified as simple, transparent and standardised: the notifications, their tranches, their parties, the claims they make against the criteria, and what has happened to them | [sts](src/fincommons/sts/README.md) |
| `gleif` | keep a day-by-day history of who every LEI belongs to and who owns whom, and which entity operates which market | [gleif](src/fincommons/gleif/README.md) |
| `entitymasterdata` | resolve a counterparty code into who the entity is and what it does: GLEIF, and the registers of the ECB, EIOPA and ESMA | [entitymasterdata](src/fincommons/entitymasterdata/README.md) |
| `famafrenchdata` | work with the Kenneth R. French Data Library: the Fama-French factors, the portfolios they are built from and the breakpoints of the sorts, every file in one table | [famafrenchdata](src/fincommons/famafrenchdata/README.md) |
| `financialreport` | read the XBRL-tagged periodic reports of listed issuers, one sub-module per regime -- `esef` for the European Single Electronic Format, `sec` for the inline XBRL mandate of the Securities and Exchange Commission, `edinet` for Japan -- and `combined`, which puts them side by side. Each stands on its own; the namespace imports none of them | [overview](src/fincommons/financialreport/README.md): [esef](src/fincommons/financialreport/esef/README.md), [sec](src/fincommons/financialreport/sec/README.md), [edinet](src/fincommons/financialreport/edinet/README.md), [combined](src/fincommons/financialreport/combined/README.md) |
| `addressing`, `storage`, `log`, `layout`, `schemas`, `columns`, `contracts`, `spreadsheet` | the infrastructure the modules share: locations from `paths.yaml`, how a Silver table rebuilt in full is read and written as parquet or Delta, logging, the file layout of tables, declared schemas, the catalogue of what a column holds, data contracts, and tables written out as OpenDocument spreadsheets | [shared infrastructure](src/fincommons/README.md) |
| `tools/` | derive the Spark schema files `fincommons.schemas` reads from an ISO 20022 XSD or an ECB data API answer, and rebuild Silver tables from the command line (`rebuild_silver.py`) | [tools](tools/README.md) |

fincommons is open source and complete on its own. Pipelines on supervisory
reporting data (EMIR, SFTR, SECR, MiFIR, SSR) can build on it and be
distributed separately, for instance only among central banks and supervisory
authorities; fincommons knows nothing of them.

## Quick start

Five minutes on a workstation, from a clone to a table of exchange rates.

**1. Install** (Python 3.11 or later, and Java 17 or 21 for Spark):

```bash
git clone https://github.com/franzmohr/fincommons.git
cd fincommons
pip install -e ".[local]"
```

**2. Say where the data goes.** Create `paths.yaml` with one section per
module you use:

```yaml
exchangerate:
  bronze: /data/exchangerate/bronze
  silver: /data/exchangerate/silver
```

**3. Run a pipeline and read the result:**

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from fincommons.log import configure
from fincommons import exchangerate as ex

configure("INFO")                                   # show what the pipeline does

builder = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

ex.create_directory_tree("paths.yaml")
ex.update_exchangerate(spark, "paths.yaml")         # download the ECB rates, write Silver

rates = ex.connect_to_exchangerate(spark, "paths.yaml")
rates.filter("quote = 'USD'").orderBy("ref_date", ascending=False).show(5)
```

The first time a session starts, `delta-spark` fetches its jars from Maven,
so that run needs network access. Every other module follows the same
pattern -- a `paths.yaml` section, an `update_<module>` function and a
`connect_to_<module>` function -- and its README starts with a quick start
of its own.

The session above is enough to try things out. For a scheduled job or a
backfill, size the cores, the memory and the scratch space for your machine
as [Setting up a Spark session](docs/spark-session.md) describes; it also
says what to do when a session misbehaves.

## Concepts in two minutes

**Layers.** Each module writes its data in the layers of a medallion
architecture:

| Layer | Holds | You read it? |
| --- | --- | --- |
| landing | the files as the publisher served them | rarely |
| Bronze | the same data in a uniform format, one delivery per partition | to debug |
| Silver | the cleaned, typed, documented tables | **yes -- this is what analyses read** |
| Gold | tables combined across data sets | yes, where a module builds one |

**`paths.yaml`.** Each module has a section naming the location of each of
its layers. A layer can also be given as a schema in a catalogue instead of a
directory (see `fincommons.addressing`). Every function takes the path of
this file as its `config_path` argument.

**Parquet or Delta.** A Silver table rebuilt in full by every run is parquet
by default, which every engine reads on its own. Written as Delta, or
addressed by catalogue name, a rebuild replaces the table in one transaction,
so an analysis never reads a fraction of a run. Tables carried forward by
merges -- the histories of `firds` and `gleif`, and the market price table --
are always Delta.

**Naming.** Every folder and file the pipelines name is spelled in
snake_case, because those names travel into a SQL catalogue, where a hyphen
has to be quoted. A partition directory such as `DATADT=2024-06-03`, and a
folder named by a date, keep their hyphens. Columns are snake_case too --
see [Looking up what a column or a series is](#looking-up-what-a-column-or-a-series-is).

**Nothing is shipped.** The data is downloaded when a pipeline runs; it is
not part of the package, and data from third-party publishers remains subject
to their own terms.

## Installation

The package is built with [Hatchling](https://hatch.pypa.io/latest/) and ships
as a pure-Python wheel (`py3-none-any`). It is not published on PyPI, so it is
installed from the repository or from a wheel built with `python -m build`.

| Extra | Adds | Needed for |
| --- | --- | --- |
| `local` | PySpark and `delta-spark` | running the pipelines outside a Spark-provided environment |
| `marketprice` | `yfinance` | downloading from Yahoo Finance in `marketpricedata` (nothing else needs it) |
| `dev` | pytest, PySpark, Delta, `jsonschema` | running the test suite |

Pick the setting you are in:

### On a workstation (local development)

```bash
pip install -e ".[local,dev]"
```

[`CONTRIBUTING.md`](CONTRIBUTING.md) has the details: virtual environments,
`uv`, which Java to install, and WSL on Windows. Then build a session as
[Setting up a Spark session](docs/spark-session.md) describes.

### On a shared server

A server that serves notebooks or R sessions to several people -- an analysis
server of an organisation, or a machine a team shares -- is neither a
workstation nor a Databricks cluster. Two decisions come before the install.

**1. Does the server need Spark at all?** The pipeline functions take a
`SparkSession` and need the `local` extra, a JVM (Java 17 or 21) and, the
first time a session starts, access to Maven, from which `delta-spark`
resolves its jars. *Reading* the lake needs none of that: a Silver table
rebuilt in full is parquet by default, which every engine reads on its own,
and that is what the format is for. Point a non-Spark reader at such a table
rather than at one updated in place, since a table carrying deletion vectors
is read correctly only by a reader that supports them.

**2. Where does the environment live?** Where the sessions run in a container
built afresh on every start, an install into the interpreter of the image is
discarded with it, and only the paths mounted into the container survive. A
virtual environment under one of those -- the home directory, usually -- is
installed once and is still there on the next start:

```bash
python3 -m venv ~/.venvs/fincommons
~/.venvs/fincommons/bin/pip install "fincommons @ git+https://github.com/franzmohr/fincommons.git@<commit>"
```

- **Name a commit, not a branch.** The environment is then the same after a
  reinstall as before it, which is what a result computed on that server has
  to be able to rest on.
- **No access to GitHub?** Build a wheel with `python -m build` elsewhere and
  copy it over.
- **Running the pipelines there?** Add the `local` extra:
  `"fincommons[local] @ git+..."`.
- **Check `python3 -V` first.** The package needs 3.11 or later, and an image
  built for R need not carry one.

To use the environment from notebooks, register it as a kernel of its own:

```bash
~/.venvs/fincommons/bin/pip install ipykernel
~/.venvs/fincommons/bin/python -m ipykernel install --user --name fincommons
```

To use it from R, name the interpreter in `~/.Renviron`, so that reticulate
resolves it at the start of a session rather than whichever interpreter it
finds first:

```
RETICULATE_PYTHON=/home/<user>/.venvs/fincommons/bin/python
```

and import the modules with `reticulate::import("fincommons.firds")`.

As on a workstation, `paths.yaml` gives the server's own paths. A path given
as a URI is refused, for the reason set out under Databricks below.

### On Databricks

The Databricks Runtime already provides Spark, NumPy and PyArrow, so PySpark
is **not** a runtime dependency of this package -- installing it from PyPI on
a cluster would shadow the runtime's own Spark installation. Build the wheel
with `python -m build` (or `hatch build`) and install it alone:

```
%pip install /Volumes/<catalog>/<schema>/<volume>/fincommons-<version>-py3-none-any.whl
```

The wheel in `dist/` works on any Databricks Runtime with Python 3.11 or
later.

**`paths.yaml` must point at Unity Catalog volumes (`/Volumes/...`).** Local
paths and DBFS do not work in their place. Why:

- The modules hand the same path string to two resolvers: to Python's file
  APIs, which read the driver's local filesystem, and to Spark, which resolves
  a bare path against DBFS.
- A volume is the one location that means the same thing to both, because it
  is FUSE-mounted for Python and addressable under the same path by Spark.
- A `/dbfs/...` path reaches the right file from Python but sends Spark to
  `dbfs:/dbfs/...`, and a `dbfs:/...` path does the reverse, so neither
  spelling satisfies both halves of the pipeline.
- A path given as a URI (`abfss://...`, `s3://...`, `dbfs:/...`) is refused
  when a Silver table is read or written: the format of a table is read off
  the file system, which cannot see into a URI, so a Delta table there would
  look absent and a parquet overwrite would throw its history away.

## Logging

Every pipeline records what it did through the standard library's `logging`,
under the logger tree `fincommons.<module>.<layer>`. Nothing in the package
configures logging on import, so importing it never overrides what a notebook
or a scheduled job has already set up. To switch the log on:

```python
from fincommons.log import configure

configure("INFO")
```

The handler is attached to the `fincommons` logger rather than to the root
logger and propagation is switched off, so a local run and a run on Databricks
produce the same log: the Databricks driver already carries a root handler,
and configuring that one would write every record twice into the driver log.

The level says what a reader has to do about a record:

| Level | Meaning |
| --- | --- |
| `DEBUG` | Detail for reproducing a run. Off by default. |
| `INFO` | The record of what the run did: the stages, their durations, the reference dates applied, the rows written. |
| `WARNING` | The run stayed valid but did less than a reader might assume: nothing to update, a file whose schema is not recognised, a source that holds the combined table back. |
| `ERROR` | A unit of work failed and was set aside while the run carried on by design, e.g. a delivery moved to the error folder. Always recorded with its traceback. |
| `CRITICAL` | Not used. A problem that invalidates the run raises instead. |

Whatever a record may have to be filtered by later is recorded beside the
message rather than inside it, so that the sentence stays constant across
runs:

```
2026-09-08 03:14:22 INFO     fincommons.firds.silver  Applied delta file  module=firds layer=silver table=MAIN datadt=2026-09-04
```

**Scheduled runs.** No module of the package opens a Spark session or parses
a command line, so a scheduled run is a script the operator owns:

- it sets the level with `fincommons.log.configure`;
- `add_log_level_argument` adds `--log-level` to that script's own parser,
  where it takes one;
- `print` keeps its place next to the log: result tables that a script writes
  for someone watching the console are the output of the program and stay on
  standard output, while the log goes to standard error.

Each module's README shows the script for its pipeline.

## Looking up what a column or a series is

The tables name their columns in snake_case -- `trading_venue`,
`notional_currency` -- and keep the field a column was read from in a
catalogue rather than in its name; the market price table names its series by
identifiers such as `EUR/VND` or `YLD-EA10Y-M`. Two functions say what they
are, and neither needs Spark or a data lake:

```python
import fincommons
from fincommons import marketpricedata as mp

print(fincommons.describe_column("trading_venue"))     # every data set with that column
fincommons.describe_column("action_type", dataset="firds")  # one data set
fincommons.column_catalogue("exchangerate")            # all columns of a data set

print(mp.describe_instrument("EUR/VND"))               # one series of the price table
```

What `describe_column` reads: the data contracts of the Silver tables,
shipped in `data/` as `contract_<module>_<layer>_<table>.yaml` in the Open
Data Contract Standard -- one row per data set and column, with the tables
that write it, the report field it is read from or "derived", a kind and a
description.

- **Which data sets:** every module whose columns are catalogued -- `firds`,
  `entitymasterdata`, `exchangerate`, `famafrenchdata`, `esef`, `sec`,
  `edinet`, `financialreport`, `gleif` and `sts` -- has a contract for each
  of its Silver tables, and the contracts are the only place a column is
  described.
- **What is not covered:** the Bronze layers keep the columns their
  publishers deliver and have no contracts, and the few columns a public
  function returns beside those of a table are described by its docstring.

What you can rely on:

- A test reads the column names out of the modules' code and fails if a
  column has no description.
- Where a description names a field of a regulatory reporting standard (an
  RTS field, an ESMA template field) that is a pointer to the standard rather
  than a quotation of it.

A package that builds on fincommons ships contracts of its own, or still a
`data/column_catalogue.csv` for tables it has not given one, and reads both
through the `catalogues` argument.

## Modules

A short tour of each module. Its own README has a quick start, the columns of
every table, recipes for common tasks and how it works.

### `fincommons.exchangerate`

Daily exchange rates for converting reported amounts into a single currency.
Under EMIR and SFTR an amount is reported in the currency it was agreed in, so
comparing amounts across reports means converting them first.

The module downloads the reference rates published by the ECB and,
optionally, the bulk exchange rate statistics of the BIS, combines them with a
proprietary source where one is provided, and writes one table with a row per
calendar day and currency pair. Each update replaces that table; it is
parquet by default, and written as Delta, or addressed by catalogue name, it
is replaced in a single transaction, so a refresh cannot be observed
half-written.

| Column    | Meaning                                           |
|-----------|---------------------------------------------------|
| `ref_date` | calendar day the rate applies to                 |
| `base`     | base currency, `EUR` unless the table was rebased |
| `quote`    | quoted currency                                  |
| `value`    | units of `quote` per one unit of `base`          |
| `type`     | kind of rate, e.g. `ref` for a reference rate    |
| `source`   | source the quote came from                       |
| `imputed`  | whether the rate was carried from an earlier quote |

```python
from fincommons import exchangerate as ex

ex.update_exchangerate(spark, "paths.yaml")     # download, combine, write Silver

ex.convert2ccy(data, "paths.yaml", date="REFDT", amount="NMNLVAL",
               currency="CCY", target="EUR")    # convert a column of a Spark, pandas or polars frame
```

- **Precedence:** where more than one source quotes the same pair on the same
  day, the proprietary data takes precedence over the ECB and the ECB over the
  BIS.
- **Gaps:** the sources quote on business days only, so every calendar day
  carries the last rate quoted on or before it, for a week at most. The fill
  stops at a pair's last quote, so a currency that stops being quoted ends
  with its last observation. Each source counts up to its own last day.
- **Outside the euro area:** pass `rebase` with your own currency code, which
  recalculates the whole table against it.

Details, including the layout a proprietary source has to have:
[`src/fincommons/exchangerate/README.md`](src/fincommons/exchangerate/README.md).

### `fincommons.marketpricedata`

Prices and rates from public publishers (ECB, Federal Reserve Bank of New
York, Bank of England, Yahoo Finance and others) in one table, and the
risk-free zero curves its overnight and swap rates imply. See
[`src/fincommons/marketpricedata/README.md`](src/fincommons/marketpricedata/README.md).

### `fincommons.firds`

ESMA's FIRDS reference data: download, import and the historical database
ESMA's instructions describe. See
[`src/fincommons/firds/README.md`](src/fincommons/firds/README.md).

### `fincommons.gleif`

A day-by-day history of GLEIF's reference data: who every LEI belongs to, who
consolidates or manages whom, and which entity operates which market (the
MIC-to-LEI mapping). `entitymasterdata` builds on it where it is configured.
See [`src/fincommons/gleif/README.md`](src/fincommons/gleif/README.md).

### `fincommons.entitymasterdata`

Entity master data for the counterparties of a report. A report identifies
its counterparty by a code alone, so anything asked of the data beyond a
single contract -- which sector is exposed, which group a set of entities
belongs to, which country a position sits in -- needs that code resolved into
a description of the entity.

The module builds one table that does so, following [Lenoci and Letizia
(2021)](https://link.springer.com/chapter/10.1007/978-3-030-66891-4_6): GLEIF
says who the entity is and how it is related to other entities, and the
registers of the ECB, EIOPA and ESMA say what it does.

| Column | Meaning |
|--------|---------|
| `id` | running number over the table, reassigned by every rebuild |
| `lei` | the LEI of the entity, where it has one |
| `internal_id_1` .. `internal_id_4` | the identifiers of the proprietary list |
| `mic` | the market identifier codes of the markets it operates, from GLEIF's MIC-to-LEI mapping |
| `name`, `country` | legal name and country of the legal address, from GLEIF |
| `sector`, `subsector` | what the entity does, from the sectoral registers |
| `ultimate_parent_lei` | the entity that ultimately consolidates it |
| `amc` | for a fund, the company that manages it |
| `if_policy`, `if_type`, `if_ucits_compl` | for a fund, its characteristics |

```yaml
entitymasterdata:
  landing: /data/entitymasterdata/landing
  bronze: /data/entitymasterdata/bronze
  silver: /data/entitymasterdata/silver
```

```python
from fincommons import entitymasterdata as em

em.create_directory_tree("paths.yaml")
em.update_entitymasterdata(spark, "paths.yaml")     # landing to Silver

em.connect_to_entitymasterdata(spark, "paths.yaml") # what an analysis reads
```

- **Downloads:** every register is downloaded by the update: the GLEIF golden
  copies through GLEIF's API, the ECB's lists and EIOPA's registers by reading
  the current link or form off their pages, and the lists of CCPs from ESMA's
  pdf files.
- **A failed download** is logged as an error and the update carries on with
  what the Bronze layer already holds. A register can still be placed in the
  landing zone by hand, under a name the Bronze import reads the reference
  date off.
- **Overlaps:** where an entity stands in more than one register -- a central
  counterparty is licensed as a credit institution as well -- the narrower
  description wins, and where a proprietary list describes an LEI it displaces
  the public description of it in full.
- **Rebuilds:** both Silver tables are rebuilt in full by every run, as
  parquet by default; written as Delta, or addressed by catalogue name, a
  rebuild replaces a table in one transaction, so an analysis never reads a
  fraction of a run.

Details, including where each register has to be placed and the layout a
proprietary list has to have:
[`src/fincommons/entitymasterdata/README.md`](src/fincommons/entitymasterdata/README.md).

### `fincommons.sts`

ESMA's public register of the securitisations notified to it as simple,
transparent and standardised, read through the machine-to-machine services of
its registers portal, which need no key and no registration.

```yaml
sts:
    bronze: /data/sts/bronze
    silver: /data/sts/silver
```

| Table | Holds |
|-------|-------|
| `notification` | one row per notification the register serves, with the identifier scheme that says whether it joins to anything |
| `assertion` | the claims a public notification makes against the STS criteria, long rather than 130 columns wide |
| `tranche` | its tranches, by ISIN, which join to `firds` |
| `party` | its originators, original lenders, sponsors and special purpose entities, under one column for the role; the LEIs join to `entitymasterdata` |
| `event` | what is known to have happened to a securitisation, folded from the register, two frozen spreadsheets and the difference between two snapshots |
| `synthetic` | the synthetic STS securitisations, which are notified by e-mail and appear in no register |

```python
from fincommons import sts

sts.create_directory_tree("paths.yaml")
sts.update_sts(spark, "paths.yaml")        # a snapshot, then the tables
sts.collected_snapshots("paths.yaml")      # the days a snapshot is held for
df = sts.connect_to_sts(spark, "paths.yaml", "notification")
```

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-register-report.qmd
```

**The register is current state, so the snapshots are the history.** A
cancelled notification leaves the register, and the two spreadsheets that
recorded removals were frozen in 2022, so a lake's own dated snapshots are the
only record of what the register once held. A collection therefore writes a
complete copy beside the ones before it and never rewrites one, and what left
the register between two runs is a diff of two snapshots. Do not compact them
away.

**Two populations.** Of the 1,147 notifications held on 20 November 2026, 502
were public and carried the whole STS template, their tranches and their
parties; 645 were private and carried sixteen to eighteen fields and no child
documents at all.

- A public notification's securitisation identifier is the one the disclosure
  templates use, so it joins to reported securitisation data as it stands; a
  private one carries a synthetic identifier that joins to nothing.
- The tranches carry ISINs, which join to `firds`, and the originator, the
  original lender and the sponsor carry legal entity identifiers, which join
  to `entitymasterdata`; the special purpose entity carries only a country.

**Reports.** `templates/sts-register-report.qmd` is a report on one snapshot:
what the register holds, what it does not say, the census of its fields, the
checks it fails, and what has arrived and left since the snapshot before.
`templates/sts-country-factsheet.qmd` reports one country, or the Union with
`EU` -- what it securitises, who originates it, where the vehicles are -- and
the only setting it needs is that code.

Details: [`src/fincommons/sts/README.md`](src/fincommons/sts/README.md).

### `fincommons.famafrenchdata`

The [Kenneth R. French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html):
the factors of the Fama-French models, the returns of the portfolios they are
built from and the NYSE breakpoints of the sorts, for the US since 1926 and
for international regions since 1990. The library publishes some three
hundred zipped CSV files written for a person to read, and rebuilds all of
them every month; the module reads every one of them into one long table.

| Column | Meaning |
|--------|---------|
| `ref_date` | the date of the period: the day, the last day of the month, or 31 December |
| `dataset` | the file, e.g. `F-F_Research_Data_Factors` |
| `table` | the kind of table in the file, e.g. `FACTORS`, `RET_VW`, `NFIRMS`, `BREAKPOINTS` |
| `frequency` | `D`, `W`, `M` or `A` |
| `series` | the column as the file names it, e.g. `HML`, `SMALL LoBM` |
| `value` | the value as published, returns in percent |
| `title` | the title of the table as published |

```python
from fincommons import famafrenchdata as ffd

ffd.update_famafrenchdata(spark, "paths.yaml")     # the files that changed, to Silver
df = ffd.connect_to_famafrenchdata(spark, "paths.yaml")
ffd.series(df, "F-F_Research_Data_Factors", "FACTORS", "M")   # as published, wide
```

A file is downloaded only where the server says it has changed, and the table
is rebuilt file by file, each file replacing its own partition. The data is
copyright Eugene F. Fama and Kenneth R. French, and research that uses it
cites the library. The details, the vocabulary of tables and a vignette that
rebuilds HML and SMB from the six portfolios:
[`src/fincommons/famafrenchdata/README.md`](src/fincommons/famafrenchdata/README.md).

### `fincommons.financialreport.esef`

The annual financial reports that issuers on European regulated markets file
in the European Single Electronic Format (ESEF), as collected by
[filings.xbrl.org](https://filings.xbrl.org), together with the reports of
the United Kingdom and of Ukraine filed the same way. The repository
converts each report's Inline XBRL to xBRL-JSON; the module keeps the indexes
of the repository and the reports of the countries asked for, and reads them
into two tables:

| Table | One row per |
|--------|---------|
| `filings` | filing of the repository: `filing`, `lei`, `name`, `country`, `period_end`, when it was processed, how its validation went, where its files are |
| `facts` | fact of a report collected: `filing`, `lei`, `concept` (e.g. `ifrs-full:Revenue`), `prd_start`, `prd_end`, `unit`, `dimensions`, `value` or `text` |

```python
from fincommons.financialreport import esef

esef.update_esef(spark, "paths.yaml", ["AT"], contact="me@example.org")
facts = esef.connect_to_esef(spark, "paths.yaml")
filings = esef.connect_to_esef_filings(spark, "paths.yaml")
```

- A report is downloaded again only where the repository has processed its
  filing anew, and the table of facts is rebuilt country by country.
- Text blocks -- the notes, with their markup -- are left out of the table and
  kept in the Bronze layer.
- The facts are keyed by LEI, as the entity master data is.

Details: [`src/fincommons/financialreport/esef/README.md`](src/fincommons/financialreport/esef/README.md).
The other regimes and the combining layer have READMEs of their own beside
it; [`src/fincommons/financialreport/README.md`](src/fincommons/financialreport/README.md)
says which to start with and how they fit together.

## Tests

```bash
pytest -m "not spark"   # the tests that need no Spark session, in seconds
pytest                  # the whole suite
pytest tests/ -k firds  # a single module
```

- **Markers.** Every test that uses the Spark session is marked `spark` as it
  is collected, so the first command leaves those out without any test having
  to be marked by hand.
- **Setup.** The suite needs the `dev` extra. [`CONTRIBUTING.md`](CONTRIBUTING.md)
  says how to set up, which Java version Spark needs, and what the continuous
  integration checks.
- **One Spark session.** The suite runs against one local Spark session,
  built by `fincommons.testing` with the Delta extensions that the FIRDS
  watermark and the exchange rate tests require; `tests/conftest.py` imports
  it. Packages that build on fincommons use the same fixture, so that their
  suites run against the same configuration. That module also points Spark's
  Python workers at the interpreter running the tests, so no environment
  variables have to be set by hand.
- **No network.** Where a module downloads something -- the FIRDS files from
  ESMA's register, the reference rates from the ECB and the BIS, the GLEIF
  golden copies, the French Data Library -- the test supplies the response,
  so that what is asserted is what the module does with an answer rather than
  what the publisher happens to be serving that day. The one exception is
  opt-in: with `FINCOMMONS_NETWORK=1` set, a test of `famafrenchdata` rebuilds
  two factors from the French Data Library itself, and is skipped otherwise.
- **Public API.** `tests/test_public_api.py` lists the names that packages
  building on fincommons use, and fails if one of them disappears; removing
  or renaming one of them is a breaking change.

## Handbook

The handbook in [`docs/handbook`](docs/handbook) explains the architecture the
package is built on and documents every data set: how its pipeline works,
which decisions were taken and why, and what a user may rely on. It is a
LaTeX document; build it with a TeX distribution that has `biber`:

```bash
cd docs/handbook
latexmk -pdf fincommons.tex
```

A change to the code that alters what a chapter states comes with a change to
that chapter.

## Contributing

Contributions are welcome from supervisory authorities, central banks and
financial firms alike, and so are corrections to the documentation, which for
a README can be made in the browser on GitHub. [`CONTRIBUTING.md`](CONTRIBUTING.md)
says how.

Asking for something the package does not do yet is a contribution as well,
and it puts you under no obligation to write it. Open an issue with the
template that fits:

| Template | When |
| --- | --- |
| *Feature* | you need something the package does not do. Say what you need, what it is for and what you do without it today; what fincommons holds is decided by what the institutions using it need, so the use behind a request is the argument for it |
| *New data set* | a public data set the package does not collect yet |
| *Change at a data source* | a publisher changed a file or an address |

[`CONTRIBUTING.md`](CONTRIBUTING.md) says what a request should carry and what
happens to it.

## Security

Report a vulnerability privately, never in a public issue; [`SECURITY.md`](SECURITY.md)
says how, and how it is handled and disclosed.

## Licence

fincommons is licensed under the [European Union Public Licence 1.2](LICENSES/EUPL-1.2.txt).

The licence permits any use, commercial use included, so a firm may run
fincommons in its own internal processes, and change it for them, without
asking. Only distributing the software, changed or not, or making it available
to others over a network obliges the distributor to pass the source on, under
the same licence or one it lists as compatible.

fincommons is provided as is, without any warranty, as articles 7 and 8 of the
licence set out. No guarantee is given that the software, the data it collects
or the figures it computes are correct, complete or up to date. Data from
third-party publishers remains subject to their own terms. Whoever uses
fincommons is responsible for checking that its results are fit for their
purpose. Nothing in it is investment, legal or regulatory advice.
