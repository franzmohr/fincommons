# fincommons

Shared infrastructure for basic financial data work.

fincommons builds data lakes from public financial data, in the layers of a
medallion architecture (landing, Bronze, Silver, Gold), on Spark and Delta
Lake. It runs unchanged on a workstation and on Databricks, and every path it
reads or writes comes from one configuration file, `paths.yaml`.

The library is intended to incite public debate and identify best practice
approaches for handling basic data pipelines.

| Module | Holds |
| --- | --- |
| `exchangerate` | the reference rates of the ECB and the BIS, combined into one table per calendar day and currency pair, and conversion of amounts |
| `marketpricedata` | prices and rates from public publishers (ECB, Federal Reserve Bank of New York, Bank of England, Yahoo Finance and others) in one table, and the risk-free zero curves its overnight and swap rates imply |
| `firds` | ESMA's FIRDS reference data: download, import and the historical database ESMA's instructions describe |
| `entitymasterdata` | who a legal entity is: GLEIF, and the registers of the ECB, EIOPA and ESMA |
| `frenchdatalibrary` | the Kenneth R. French Data Library: the Fama-French factors, the portfolios they are built from and the breakpoints of the sorts, every file in one table |
| `addressing`, `storage`, `log`, `layout`, `schemas`, `columns` | the infrastructure the modules share: locations from `paths.yaml`, how a Silver table rebuilt in full is read and written as parquet or Delta, logging, the file layout of tables, declared schemas, and the catalogue of what a column holds |

fincommons is open source and complete on its own. Pipelines on supervisory
reporting data (EMIR, SFTR, SECR, MiFIR, SSR) can build on it and be
distributed separately, for instance only among central banks and supervisory
authorities; fincommons knows nothing of them.

## Installation

The package is built with [Hatchling](https://hatch.pypa.io/latest/) and ships as a
pure-Python wheel (`py3-none-any`).

### Local development

Install with the `local` extra, which adds PySpark:

```
pip install -e ".[local,dev]"
```

### Databricks

The Databricks Runtime already provides Spark, NumPy and PyArrow, so PySpark is
**not** a runtime dependency of this package -- installing it from PyPI on a
cluster would shadow the runtime's own Spark installation. Install the wheel
alone:

```
%pip install /Volumes/<catalog>/<schema>/<volume>/fincommons-0.1.0-py3-none-any.whl
```

Build the wheel first with `python -m build` (or `hatch build`); the result in
`dist/` works on any Databricks Runtime with Python 3.10 or later.

Note that `paths.yaml` must point at Unity Catalog volumes (`/Volumes/...`)
rather than at local paths, and that DBFS does not work in their place. The
modules hand the same path string to two resolvers: to Python's file APIs,
which read the driver's local filesystem, and to Spark, which resolves a bare
path against DBFS. A volume is the one location that means the same thing to
both, because it is FUSE-mounted for Python and addressable under the same path
by Spark. A `/dbfs/...` path reaches the right file from Python but sends Spark
to `dbfs:/dbfs/...`, and a `dbfs:/...` path does the reverse, so neither
spelling satisfies both halves of the pipeline. A path given as a URI
(`abfss://...`, `s3://...`, `dbfs:/...`) is refused when a Silver table is
read or written: the format of a table is read off the file system, which
cannot see into a URI, so a Delta table there would look absent and a parquet
overwrite would throw its history away.

Every folder and file the pipelines name is spelled in snake_case, because
those names travel into a SQL catalogue, where a hyphen has to be quoted. A
partition directory such as `DATADT=2024-06-03`, and a folder named by a date,
keep their hyphens.

## Logging

Every pipeline records what it did through the standard library's `logging`,
under the logger tree `fincommons.<module>.<layer>`. Nothing in the package
configures logging on import, so importing it never overrides what a notebook or
a scheduled job has already set up. To switch the log on:

```python
from fincommons.log import configure

configure("INFO")
```

The handler is attached to the `fincommons` logger rather than to the root
logger and propagation is switched off, so a local run and a run on Databricks
produce the same log: the Databricks driver already carries a root handler, and
configuring that one would write every record twice into the driver log.

The level says what a reader has to do about a record:

| Level | Meaning |
| --- | --- |
| `DEBUG` | Detail for reproducing a run. Off by default. |
| `INFO` | The record of what the run did: the stages, their durations, the reference dates applied, the rows written. |
| `WARNING` | The run stayed valid but did less than a reader might assume: nothing to update, a file whose schema is not recognised, a source that holds the combined table back. |
| `ERROR` | A unit of work failed and was set aside while the run carried on by design, e.g. a delivery moved to the error folder. Always recorded with its traceback. |
| `CRITICAL` | Not used. A problem that invalidates the run raises instead. |

Whatever a record may have to be filtered by later is recorded beside the
message rather than inside it, so that the sentence stays constant across runs:

```
2026-09-08 03:14:22 INFO     fincommons.firds.silver  Applied delta file  module=firds layer=silver table=MAIN datadt=2026-09-04
```

`print` keeps a job next to this. Result tables that a script writes for
someone watching the console are the output of the program and stay on
standard output; the log goes to standard error. `python -m
fincommons.marketpricedata` and `python -m fincommons.frenchdatalibrary` take
`--log-level`.

## Looking up what a column or a series is

The tables name their columns in the compressed style of ISO 20022 --
`TRADGVN`, `NTNLCCY`, `BUYRDCSNMAKRFRSTNM` -- and the market price table names
its series by identifiers such as `EUR/VND` or `YLD-EA10Y-M`. Two functions say
what they are, and neither needs Spark or a data lake:

```python
import fincommons
from fincommons import marketpricedata as mp

print(fincommons.describe_column("TRADGVN"))           # every data set with that column
fincommons.describe_column("ACTNTP", dataset="firds")  # one data set
fincommons.column_catalogue("exchangerate")            # all columns of a data set

print(mp.describe_instrument("EUR/VND"))               # one series of the price table
```

`describe_column` reads `data/column_catalogue.csv`: one row per data set and
column (75 of them across five modules), with the tables that write it, the
report field it is read from or "derived", a kind and a description. A test
reads the column names out of the modules' code and fails if the two drift
apart, so a column added to a module needs a catalogue row. The descriptions
are taken from the code, the XML paths and the module READMEs; where they
name a field of a regulatory reporting standard (an RTS field, an ESMA
template field) that is a pointer to the standard rather than a quotation of
it, and a description that could not be fully established ends in
"(inferred)". A package that builds on fincommons can ship a catalogue of its
own and read both through the `catalogues` argument.

## Modules

### `fincommons.entitymasterdata`

Entity master data for the counterparties of a report. A report identifies its
counterparty by a code alone, so anything asked of the data beyond a single
contract -- which sector is exposed, which group a set of entities belongs to,
which country a position sits in -- needs that code resolved into a description
of the entity. The module builds one table that does so, following [Lenoci and
Letizia (2021)](https://link.springer.com/chapter/10.1007/978-3-030-66891-4_6):
GLEIF says who the entity is and how it is related to other entities, and the
registers of the ECB, EIOPA and ESMA say what it does.

| Column | Meaning |
|--------|---------|
| `ID` | running number over the table, reassigned by every rebuild |
| `LEI` | the LEI of the entity, where it has one |
| `INTERNALID_1` .. `INTERNALID_4` | the identifiers of the proprietary list |
| `NAME`, `CTRY` | legal name and country of the legal address, from GLEIF |
| `SECTOR`, `SUBSECTOR` | what the entity does, from the sectoral registers |
| `ULTPAR_LEI` | the entity that ultimately consolidates it |
| `AMC` | for a fund, the company that manages it |
| `IF_POLICY`, `IF_TYPE`, `IF_UCITS_COMPL` | for a fund, its characteristics |

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

Only the GLEIF golden copies and the list of CCPs are downloaded. The ECB and
EIOPA registers are published behind a form rather than at a stable address, so
they are placed in the landing zone by hand, under names the Bronze import reads
the reference date off. `update_entitymasterdata` therefore starts from the
point at which the landing zone has been filled.

Where an entity stands in more than one register -- a central counterparty is
licensed as a credit institution as well -- the narrower description wins, and
where a proprietary list describes an LEI it displaces the public description of
it in full. Both Silver tables are Delta tables that a rebuild replaces in one
transaction, so an analysis never reads a fraction of a run.

The details, including where each register has to be placed and the layout a
proprietary list has to have, are documented in
[`src/fincommons/entitymasterdata/README.md`](src/fincommons/entitymasterdata/README.md).

### `fincommons.exchangerate`

Daily exchange rates for converting reported amounts into a single currency.
Under EMIR and SFTR an amount is reported in the currency it was agreed in, so
comparing amounts across reports means converting them first. The module
downloads the reference rates published by the ECB and, optionally, the bulk
exchange rate statistics of the BIS, combines them with a proprietary source
where one is provided, and writes one Delta table with a row per calendar day
and currency pair. Each update replaces that table in a single transaction, so
a refresh cannot be observed half-written.

| Column    | Meaning                                           |
|-----------|---------------------------------------------------|
| `REFDATE` | calendar day the rate applies to                  |
| `BASE`    | base currency, `EUR` unless the table was rebased |
| `QUOTE`   | quoted currency                                   |
| `VALUE`   | units of `QUOTE` per one unit of `BASE`           |
| `TYPE`    | kind of rate, e.g. `ref` for a reference rate     |
| `SOURCE`  | source the quote came from                        |

```python
from fincommons import exchangerate as ex

ex.update_exchangerate(spark, "paths.yaml")     # download, combine, write Silver

ex.convert2ccy(data, "paths.yaml", date="REFDT", amount="NMNLVAL",
               currency="CCY", target="EUR")    # convert a column of a Spark, pandas or polars frame
```

Where more than one source quotes the same pair on the same day, the
proprietary data takes precedence over the ECB and the ECB over the BIS. The
sources quote on business days only, so every calendar day carries the last
rate quoted on or before it, for a week at most, and the fill stops at a pair's
last quote, so a currency that stops being quoted ends with its last
observation. Each source counts up to its own last day. Organisations outside the euro area pass `rebase` with
their own currency code, which recalculates the whole table against it.

The details, including the layout a proprietary source has to have, are
documented in
[`src/fincommons/exchangerate/README.md`](src/fincommons/exchangerate/README.md).

### `fincommons.frenchdatalibrary`

The [Kenneth R. French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html):
the factors of the Fama-French models, the returns of the portfolios they are
built from and the NYSE breakpoints of the sorts, for the US since 1926 and
for international regions since 1990. The library publishes some three
hundred zipped CSV files written for a person to read, and rebuilds all of
them every month; the module reads every one of them into one long table.

| Column | Meaning |
|--------|---------|
| `REFDT` | the date of the period: the day, the last day of the month, or 31 December |
| `DATASET` | the file, e.g. `F-F_Research_Data_Factors` |
| `TABLE` | the kind of table in the file, e.g. `FACTORS`, `RET_VW`, `NFIRMS`, `BREAKPOINTS` |
| `FREQUENCY` | `D`, `W`, `M` or `A` |
| `SERIES` | the column as the file names it, e.g. `HML`, `SMALL LoBM` |
| `VALUE` | the value as published, returns in percent |
| `TITLE` | the title of the table as published |

```python
from fincommons import frenchdatalibrary as fdl

fdl.update_frenchdatalibrary(spark, "paths.yaml")     # the files that changed, to Silver
df = fdl.connect_to_frenchdatalibrary(spark, "paths.yaml")
fdl.series(df, "F-F_Research_Data_Factors", "FACTORS", "M")   # as published, wide
```

A file is downloaded only where the server says it has changed, and the table
is rebuilt file by file, each file replacing its own partition. The data is
downloaded when the pipeline runs and is not shipped with the package; it is
copyright Eugene F. Fama and Kenneth R. French, and research that uses it
cites the library. The details, the vocabulary of tables and a vignette that
rebuilds HML and SMB from the six portfolios are documented in
[`src/fincommons/frenchdatalibrary/README.md`](src/fincommons/frenchdatalibrary/README.md).

### `fincommons.firds` and `fincommons.marketpricedata`

See [`src/fincommons/firds/README.md`](src/fincommons/firds/README.md) and
[`src/fincommons/marketpricedata/README.md`](src/fincommons/marketpricedata/README.md).

## Tests

```
pytest
```

The suite needs the `dev` extra. It runs against one local Spark session,
built by `fincommons.testing` with the Delta extensions that the FIRDS
watermark and the exchange rate tests require; `tests/conftest.py` imports
it. Packages that build on fincommons use the same fixture, so that their
suites run against the same configuration. That module also points Spark's
Python workers at the interpreter running the tests, so no environment
variables have to be set by hand.

Nothing in the suite reaches the network. Where a module downloads something
-- the FIRDS files from ESMA's register, the reference rates from the ECB and
the BIS, the GLEIF golden copies, the French Data Library -- the test supplies
the response, so that
what is asserted is what the module does with an answer rather than what the
publisher happens to be serving that day.

`tests/test_public_api.py` lists the names that packages building on
fincommons use, and fails if one of them disappears; removing or renaming one
of them is a breaking change.

A single module can be run on its own, for instance with

```
pytest tests/ -k firds
```

## Handbook

The handbook in [`docs/handbook`](docs/handbook) explains the architecture the
package is built on and documents every data set: how its pipeline works,
which decisions were taken and why, and what a user may rely on. It is a
LaTeX document; build it with

```
cd docs/handbook
latexmk -pdf fincommons.tex
```

which needs a TeX distribution with `biber`. A change to the code that alters
what a chapter states comes with a change to that chapter.

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
