# Entity master data

Module `entitymasterdata` builds one table that says who the counterparties of a report are: for every legal entity identifier (LEI), the entity's name, country, sector, ultimate parent, fund manager and the markets it operates. A report such as EMIR, SECR or SFTR identifies a counterparty by a code alone; join this table on the LEI to ask which sector is exposed, which group a set of entities belongs to, or which country a position sits in. The approach is based on [Lenoci and Letizia (2021)](https://link.springer.com/chapter/10.1007/978-3-030-66891-4_6): GLEIF says who an entity is and how it is related to other entities, and the registers of the ECB, EIOPA and ESMA say what it does.

**At a glance**

- **Sources:** GLEIF Level 1 and Level 2 (or the history of [`fincommons.gleif`](../gleif/README.md), where it has been built), GLEIF's MIC-to-LEI mapping via [`fincommons.gleif`](../gleif/README.md#the-mic-to-lei-mapping), the ECB's lists of MFIs and investment funds, EIOPA's registers of insurance undertakings and IORPs, ESMA's lists of CCPs, and optionally a proprietary list of your own.
- **Update:** `update_entitymasterdata` downloads every register on each run; run it daily or whenever the registers change.
- **Tables written:** `main` (the entity master data set) and `public` (the public sources alone) in the Silver layer (the cleaned tables analyses read), one row per LEI.
- **Needs:** network access to GLEIF, the ECB, EIOPA and ESMA (no credentials), and a Spark session. Parquet by default; Delta on request or where the Silver layer is in a catalogue.

## Quick start

1. Add an `entitymasterdata` section to `paths.yaml`:

```yaml
entitymasterdata:
  landing: /data/entitymasterdata/landing
  bronze: /data/entitymasterdata/bronze
  silver: /data/entitymasterdata/silver
```

2. Create the folders and start Spark:

```python
config_path = "paths.yaml"   # the configuration file, where the paths are specified

from fincommons import entitymasterdata as em

# Create folders for entity master data, unless they already exist
em.create_directory_tree(config_path = config_path)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder \
                    .master("local[20]") \
                    .appName("fincommons") \
                    .config("spark.driver.memory", "15g") \
                    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
                    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

3. Run the whole pipeline and read the result:

```python
em.update_entitymasterdata(spark, config_path)

em.connect_to_entitymasterdata(spark, config_path) \
    .select("lei", "name", "country", "sector", "ultimate_parent_lei", "mic").show(5)
```

The first run downloads the GLEIF Level 1 golden copy (about half a gigabyte zipped, 4.7 GB as csv), Level 2, every ECB list of PSRIs since 2014, and the current lists and registers of the ECB, EIOPA and ESMA.

**About the session.**

- The Silver layer is written as parquet unless `file_format = 'delta'` asks for Delta or the configuration file addresses it by catalogue name (see [Storage format and catalogue](#storage-format-and-catalogue)). Where it is Delta, the session has to be built with the Delta extensions off-cluster, which needs the `delta-spark` package that comes with the `local` extra; on Databricks Delta is built in and the extra configuration is unnecessary. A session built this way reads a parquet table just as well, so it is the safe thing to start from either way.
- The cores are stated as `local[20]` rather than through `spark.cores.max`, which only a standalone or Mesos master reads: a session built without a master comes up as `local[*]` and runs a task on every core the machine has, dividing the driver heap -- which in local mode is the memory of the whole run, since the executors live in that JVM -- among all of them. The FIRDS [README](../firds/README.md) says what that costs on a machine with many cores.
- Sizing the cores, the heap and the scratch directory for your machine, and checking that the session got them, is in [Setting up a Spark session](../../../docs/spark-session.md). It also says why the import has to be given a session at all: without one, the GLEIF golden copy is read into a single pandas frame, which a machine of 8 GB does not survive.

**Optional, but recommended: build the GLEIF history first.** Where `paths.yaml` also has a `gleif` section and [`fincommons.gleif`](../gleif/README.md#quick-start) has built its history, this module reads names, countries and parents from that history and the MICs from its MIC-to-LEI table. Without the MIC-to-LEI table the column `mic` stays empty, with a warning.

```python
from fincommons import gleif

gleif.update_gleif(spark, config_path, start_date="2025-01-01")
gleif.update_gleif_mic_lei(spark, config_path)
em.update_entitymasterdata(spark, config_path)
```

## What you get

Two tables in the Silver layer:

| Table | Where | What it is | Read with |
| --- | --- | --- | --- |
| `main` | `{silver}/main` | The entity master data set: the public table with a proprietary list in front of it, plus two catch-all entities. | `connect_to_entitymasterdata(spark, config_path)` |
| `public` | `{silver}/public` | The public sources alone, combined; the table an organisation without a list of its own would already query. | `connect_to_entitymasterdata(spark, config_path, "public")` |

### The entity master data set (`main`)

One row per entity: one per LEI, plus every entity of the proprietary list that has no LEI.

| Column | Content |
| --- | --- |
| `id` | A running number over the table. It is assigned by the update and is not stable across rebuilds. |
| `lei` | The lei of the entity, where it has one. |
| `internal_id_1` to `internal_id_4` | The identifiers of the proprietary list, empty where there is none. |
| `mic` | The market identifier codes (MIC, ISO 10383) of the markets the entity operates, a sorted list, from GLEIF's MIC-to-LEI mapping; empty where there is none. |
| `name` | The legal name of the entity. |
| `country` | The ISO-2 code of the country of its legal address. |
| `sector`, `subsector` | What the entity does, see [Sectors](#sectors-and-which-register-wins). |
| `ultimate_parent_lei` | The lei of the entity that ultimately consolidates it. |
| `amc` | For a fund, the lei of the company that manages it. |
| `if_policy`, `if_type`, `if_ucits_compl` | For a fund, its investment policy, whether it is an ETF or a PEF, and whether it is UCITS compliant. They are filled for every entity the ECB fund register covers, including one another register describes -- a money market fund, say, whose `sector` is `MMF`. |

Two catch-all entities that no register carries are always in it:

| `lei` | `name` | Constant | Use |
| --- | --- | --- | --- |
| `XOTHERX0000000000064` | `Other` | `LEI_OTHER` | collects what an aggregation does not show on its own -- the EMIR and SFTR data quality monitoring files the reporting counterparties below its top fifteen under it |
| `XUNKNOWNX00000000025` | `Unknown` | `LEI_UNKNOWN` | an entity that could not be identified |

### The public table (`public`)

The columns of `main` except for `id` and the `internal_id_*` columns, which the entity master data set itself adds, plus `ref_date`: the reference date of the GLEIF data it was built from, which is what dates the table -- the day of the golden copy of the Bronze layer, or the day of the latest publication the history of [`fincommons.gleif`](../gleif/README.md) has applied where GLEIF is read from there. It is a date, the same on every row, and the columns stand in the order of `PUBLIC_COLUMNS` whichever way GLEIF was read.

### Sectors

| Register | `sector` |
| --- | --- |
| ESMA list of CCPs | `CCP`, with the type of CCP as `subsector` (`EUCCP`, `T1CCP` or `T2CCP`) |
| EIOPA insurance undertakings | `INUN` |
| EIOPA IORPs | `ORPI` |
| ECB MFIs | `CB`, `CDTI`, `OFI` or `MMF`, after the category of the register, and `MFI` where the category is not one of the four |
| ECB investment funds | `IF`, with `UCIT`, `AIFD` or `INVF` as `subsector` after what the register says about UCITS compliance |

An entity on more than one register takes the sector of the one higher in this table; see [Sectors and which register wins](#sectors-and-which-register-wins). An entity no register covers has an empty `sector`.

### How it joins

- `lei`, `ultimate_parent_lei` and `amc` join to the LEI of a counterparty in a report, and to `lei`, `start_lei` and `end_lei` of [`fincommons.gleif`](../gleif/README.md#what-you-get).
- Each code in `mic` joins to `mic` of GLEIF's MIC-to-LEI table and to `trading_venue` of [FIRDS](../firds/README.md).

### The data contracts

What the columns of the two tables hold, and what a reader may rely on, is stated by their data contracts in the Open Data Contract Standard, `data/contract_entitymasterdata_silver_public.yaml` and `data/contract_entitymasterdata_silver_main.yaml`, which `data/product_entitymasterdata.yaml` names as the output ports of the module, the public table first since the main one is built from it. They are the one place the columns are described -- `fincommons.describe_column("sector", dataset="entitymasterdata")` reads its answer from there -- and the tests hold `PUBLIC_COLUMNS` and `COLUMNS` to them.

- **The public table** is keyed by `lei`, and its contract holds it to one row per LEI, one reference date on every row, a `sector` and a `subsector` from the vocabulary under [Sectors](#sectors), and a subsector only for a CCP or a fund.
- **The main table** is keyed by `id`, the running number of the build, and its contract holds it to ids running from one without gaps, one row per LEI where there is one, and the two catch-all entities. Its sectors are not checked, because a proprietary list may describe an entity in a vocabulary of its own. `lei` is no key of the main table, since an entity of the proprietary list may have none.
- **Measured:** built on 27 November 2026 from the registers then in the Bronze layer and the GLEIF history, the public table held 3,508,564 entities and the main one 3,508,566, and every rule held.
- **The fund columns** keep what the ECB register says where the module does not abbreviate it: `if_ucits_compl` holds `Non-UCITS compliant, other than AIF` for 317 funds, and `if_policy` the register's own words (`Bonds`, `Equities`, `Mixed`, `Real estate`, ...).

## Common tasks

**Read the entity master data set, or the public part of it.**

```python
em.connect_to_entitymasterdata(spark, config_path)            # the entity master data set
em.connect_to_entitymasterdata(spark, config_path, "public")  # the public sources alone
```

**Give the counterparties of a report a sector.** Join on the LEI; `report` stands for any data frame with a column `counterparty_lei`.

```python
entities = em.connect_to_entitymasterdata(spark, config_path) \
    .select("lei", "name", "country", "sector", "ultimate_parent_lei")
report.join(entities, report.counterparty_lei == entities.lei, "left")
```

**Find the operator of a market.**

```python
from pyspark.sql import functions as F

em.connect_to_entitymasterdata(spark, config_path) \
    .where(F.array_contains("mic", "XWBO")).select("lei", "name", "mic").show()
```

**The registers as they stood on a day.** `entities_as_of` combines the latest snapshot of every register on or before the day by the rules of the public table, and also returns which snapshot each register was read from (`None` where it had none by then).

```python
import datetime

entities, used = em.entities_as_of(spark, config_path, datetime.date(2026, 6, 30))
```

**Count the entities of every sector over time, and render the sector report.**

```python
em.write_sector_counts(spark, config_path)
```

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render entitymasterdata-sector-report.qmd
```

**Put your own list in front of the public data.** Save it as `{bronze}/internal/entitymasterdata_internal.csv` (columns in [The proprietary list](#the-proprietary-list)) and rebuild `main`:

```python
em.update_silver_entitymasterdata(spark, config_path)
```

**Store the tables as Delta.**

```python
em.update_entitymasterdata(spark, config_path, file_format = "delta")
```

**Download an earlier GLEIF golden copy by hand.** Take the link of the **csv** file in the *LEI-CDF* (Level 1) or *RR-CDF* (Level 2) column of [the golden copy download page](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/):

```python
em.download_gleif_level_1(config_path, "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/1255025/20260724-0000-gleif-goldencopy-lei2-golden-copy.csv.zip")
em.download_gleif_level_2(config_path, "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/1255070/20260724-0000-gleif-goldencopy-rr-golden-copy.csv.zip")
```

## Running it on a schedule

`update_entitymasterdata` runs the whole pipeline:

1. the downloads that can be automated ([Sources](#1-sources-and-the-landing-zone));
2. the import of everything the landing zone holds into the Bronze layer ([The Bronze layer](#2-the-bronze-layer));
3. the public table, then the entity master data set ([The public table](#3-the-public-table), [The final table](#4-the-final-table)).

```python
em.update_entitymasterdata(
    spark,
    config_path,
    url_gleif_level_1 = "https://leidata-preview.gleif.org/storage/...lei2-golden-copy.csv.zip",
    url_gleif_level_2 = "https://leidata-preview.gleif.org/storage/...rr-golden-copy.csv.zip",
)
```

- **Every register is downloaded on each run:** the GLEIF golden copies from the links given, or else the latest publication the Bronze layer lacks, and the list of CCPs and the registers of the ECB and EIOPA.
- **The GLEIF links are optional.** Where they are not given, the latest golden copy is downloaded unless the Bronze layer holds one of its day. A link that is given is downloaded, and its failure stops the update, since it was asked for.
- **Other download failures do not stop it.** A register whose download fails is logged as an error and the update carries on with the register the Bronze layer already holds -- so a cluster without internet access, a host that is down or a page that has changed does not stop the update.
- **`file_format`** is passed on to both steps of the Silver layer, so the storage format is chosen here as well. Both tables are checked before anything is downloaded, so that a format either of them refuses does not surface only after the downloads.
- **Each update replaces its tables** rather than adding to them, so a run can simply be repeated.
- **With the GLEIF history**, run [`gleif.update_gleif` and `gleif.update_gleif_mic_lei`](../gleif/README.md#running-it-on-a-schedule) first, so that this module reads their newest state.

## How it works

### 1 Sources and the landing zone

**Every source can be fetched over the network, and `update_entitymasterdata` does so on every run; a register can still be placed in the landing zone by hand.** A register whose download fails is logged as an error and the update carries on with the register the Bronze layer already holds. A register placed by hand goes under the names given below: the Bronze import reads the reference date off the name of the file, so the name is what dates the register.

The registers stored by hand as workbooks have to be saved as `.xlsx`. The legacy `.xls` format is not read -- doing so needs a package `fincommons` does not depend on -- and neither is anything else. A file the import leaves alone is named in a warning rather than passed over in silence, because in a folder filled by hand the likeliest reason for a file of the wrong kind is the register having been saved in the wrong format.

| Register | Download | Landing folder | File name |
| --- | --- | --- | --- |
| GLEIF Level 1 | `download_latest_gleif`, `download_gleif_level_1` | `gleif/level_1` | as GLEIF published it, beginning with the reference date |
| GLEIF Level 2 | `download_latest_gleif`, `download_gleif_level_2` | `gleif/level_2` | as GLEIF published it |
| ECB investment funds | `download_ecb_investment_fund` | `ecb/investment_fund` | `IF_<year>_<month>.xlsx`, `IF_<year>_Q<quarter>.xlsx` |
| ECB MFIs | `download_ecb_mfi` | `ecb/mfi` | `<YYYYMMDD>.csv` (or `YYYYMMDD.xlsx` by hand) |
| ECB FVCs | `download_ecb_fvc` | `ecb/fvc` | `FVC_<year>_Q<quarter>.xlsx` |
| ECB PSRIs | `download_ecb_psri` | `ecb/psri` | `PSRIs_<year>.xlsx`, `PSRIs_<year>_H<half>.xlsx` |
| EIOPA insurance undertakings | `download_eiopa_insurance_undertaking` | `eiopa/insurance_undertaking` | `<YYYYMMDD>.csv` |
| EIOPA IORPs | `download_eiopa_iorp` | `eiopa/iorp` | `<YYYYMMDD>.csv` |
| ESMA CCPs | `download_esma_ccp` | `esma/ccp` | `YYYYMMDD.csv` |

The folders are below the `landing` path of the `entitymasterdata` section.

#### 1.1 GLEIF data

##### 1.1.1 Level 1 data

GLEIF Level 1 data links an LEI to the name and the country of the entity it identifies. GLEIF publishes the golden copy three times a day, and the link of a file changes with every publication. Its [Golden Copy API](https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest) names the files of the latest one, and `download_latest_gleif` downloads Level 1 and Level 2 from there -- each only where the Bronze layer holds no golden copy of that day, since Level 1 is about half a gigabyte zipped and a second publication of the same day differs by hours. `update_entitymasterdata` does this on every run.

```python
em.download_latest_gleif(config_path)
```

An earlier golden copy, or one the API cannot be reached for, is downloaded from its link instead. The links are on [the golden copy download page](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/): take the link of the **csv** file in the *LEI-CDF* column and pass it as `url`.

```python
em.download_gleif_level_1(config_path, "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/1255025/20260724-0000-gleif-goldencopy-lei2-golden-copy.csv.zip")
```

The file is stored under the name GLEIF published it with, which begins with the reference date. A link that is not the link of a file is refused before anything is fetched.

**A download either leaves the file it was asked for or leaves nothing at all.** The landing zone is not a scratch directory -- the Bronze import reads the reference date off the name of a file, so a file under the name of a register is a claim that the register is there.

- An error answer from the server is therefore raised rather than stored, and so is an answer that does not open as an archive, which is what a stale link, a proxy or a login wall returns with a success status.
- The bytes are written under a name ending in `.part` and given the name the Bronze import looks for only once they are all there, so an interrupted download leaves nothing behind either.
- Requests carry a timeout, so a stalled connection does not hold the run indefinitely.

This matters most for GLEIF, because the address of the golden copy changes with every release: a link taken from the download page some time ago is the failure to expect, not an exotic one. Its error page, stored under the name of the register, would break the Bronze import, and -- since a file that cannot be read stays in the landing zone by design -- every later run as well, including the import of the other six registers, until it is removed by hand.

##### 1.1.2 Level 2 data

The second GLEIF data set covers the ownership relationships between legal entities, from which the module takes the ultimate parent of an entity and, for a fund, the company that manages it. `download_latest_gleif` downloads it with Level 1; by hand, take the link of the **csv** file in the *RR-CDF* column of the same page.

```python
em.download_gleif_level_2(config_path, "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/1255070/20260724-0000-gleif-goldencopy-rr-golden-copy.csv.zip")
```

##### 1.1.3 GLEIF from its history

**Where the configuration file has a `gleif` section and the history of [`fincommons.gleif`](../gleif/README.md) has been built, the names, countries and parents of the entities are read from that history rather than from the golden copies this module's own Bronze layer holds.** They are read as they stand for the public table, and as they stood on a day for `entities_as_of` and the sector counts. For a day before the history was begun, the state it was begun with is read.

Without it, this module reads its own golden copies, as it always has. `gleif_history_held` says which applies. The golden copies are still downloaded and imported by `update_entitymasterdata` either way.

##### 1.1.4 MICs from the gleif module

The market identifier codes (MIC) an entity operates come from the MIC-to-LEI history of [`fincommons.gleif`](../gleif/README.md#the-mic-to-lei-mapping), which `gleif.update_gleif_mic_lei` keeps; this module reads it and downloads nothing for it. Where the configuration file has no `gleif` section or the history has not been built, `mic` is left empty and a warning says so; the update is not stopped, since the MICs add an identifier to entities the table describes already.

#### 1.2 ECB data

The ECB's page of the [lists of financial institutions](https://www.ecb.europa.eu/stats/financial_corporations/list_of_financial_institutions/html/index.en.html) links each list under a name that changes with the list, so the downloads read the current link off the page rather than keeping an address. The lists of investment funds, FVCs and PSRIs come as zip archives of workbooks, one per period, and are replaced as new lists are published. The download takes the latest archive and puts into the landing zone each workbook whose period the Bronze layer does not hold yet, under the name the ECB gives it. A run after the first therefore unpacks only the lists published since.

##### 1.2.1 Investment funds

```python
em.download_ecb_investment_fund(config_path)
```

The latest archive of a single year ("IFs Overview 2026") is taken; the page also links archives of several years, which hold no newer list. The list is published per month or per quarter and the name of the workbook states which, as `IF_<year>_<month>.xlsx` or `IF_<year>_Q<quarter>.xlsx`. The reference date of the Bronze file is the last day of that period. By hand: unzip the archive and save the workbooks in `{landing zone of entitymasterdata}/ecb/investment_fund`.

##### 1.2.2 Monetary financial institutions

```python
em.download_ecb_mfi(config_path)
```

The ECB publishes a [daily list of MFIs](https://www.ecb.europa.eu/stats/financial_corporations/list_of_financial_institutions/html/daily_list-MID.en.html), the "Full Dataset" of the day as `mfi_csv_<YYMMDD>.csv`. The download stores it in `{landing zone of entitymasterdata}/ecb/mfi` as `<YYYYMMDD>.csv`, unaltered, and does not fetch a day the Bronze layer holds. The file is text separated by tabs and encoded in UTF-16, which is why it used to be opened in a spreadsheet and saved as a workbook; the import reads it as it is, and still reads a workbook `YYYYMMDD.xlsx` stored by hand.

Every column is imported as text, as the register writes it. A postcode is not a number: read as one, `00187` loses its leading zero and `1012 AB` becomes a missing value.

##### 1.2.3 Financial vehicle corporations

```python
em.download_ecb_fvc(config_path)
```

The quarterly lists of financial vehicle corporations engaged in securitisation, `FVC_<year>_Q<quarter>.xlsx`, from the latest archive of a single year, stored in `{landing zone of entitymasterdata}/ecb/fvc`. Each is imported under the last day of its quarter.

##### 1.2.4 Payment statistics relevant institutions

```python
em.download_ecb_psri(config_path)
```

The lists of payment statistics relevant institutions -- payment service providers and payment system operators -- yearly until 2021, `PSRIs_<year>.xlsx`, and half-yearly since, `PSRIs_<year>_H<half>.xlsx`, all in one archive and stored in `{landing zone of entitymasterdata}/ecb/psri`. The first run puts every list since 2014 into the landing zone. Each is imported under the last day of its period, and with the columns as that list names them: the lists until 2021 spell `Country of residence` and have a column `PSP Geo Scope`, the later ones spell `Country of Residence`.

The FVCs and the PSRIs are imported into the Bronze layer but not yet part of the entity master data set: where they rank among the other registers in the field `sector` is still to be decided.

#### 1.3 EIOPA data

**EIOPA's registers are exported as CSV by sending the page's form back, as the "CSV" link would.** EIOPA's registers are SharePoint pages whose export is not a link to a file but a postback of the page's form. The download reads the page for its form -- the view state and the other hidden fields -- and sends it back as the "**CSV**" link would, then stores the answer in the landing zone as `<YYYYMMDD>.csv` under the day of the download. A second download on the same day replaces the first. EIOPA's gateway now and then answers an export with `403 Forbidden` that it serves a moment later, so an export turned away is asked for again from a fresh page, three times at most, thirty seconds apart.

The CSV export is taken rather than the Excel one because the workbook stores an LEI made of digits alone as a number, and a twenty-digit number does not survive that: in the register of insurance undertakings of 28 October 2026, 276 LEIs came out of the workbook as `31570010000000098304`, `31570010000000000000` and the like -- several undertakings under one LEI -- and 337 in the register of IORPs. The CSV export writes every value as the register holds it. It is imported as text, every column; its dates, written day first with a time and now and then as ISO 8601 in the same column, are read into `YYYY-MM-DD`.

By hand: click "**CSV**" on the page and store the file under the current date, `YYYYMMDD.csv`. A workbook `YYYYMMDD.xlsx` from the "Excel" link is still read, with the LEIs it has lost.

##### 1.3.1 Insurance undertakings

```python
em.download_eiopa_insurance_undertaking(config_path)
```

From the [register of insurance undertakings](https://register.eiopa.europa.eu/registers/register-of-insurance-undertakings), into `{landing zone of entitymasterdata}/eiopa/insurance_undertaking`.

##### 1.3.2 Institutions for Occupational Retirement Provision

```python
em.download_eiopa_iorp(config_path)
```

From the [register of institutions for occupational retirement provision](https://register.eiopa.europa.eu/registers/register-of-institutions-for-occupational-retirement-provision), into `{landing zone of entitymasterdata}/eiopa/iorp`.

#### 1.4 ESMA data: CCPs

```python
em.download_esma_ccp(config_path)
```

**ESMA's two pdf lists of central counterparties (CCPs) are read for the CCPs that are current, with their Tier, and stored as a dated csv file.** ESMA publishes the CCPs authorised in the Union and the third-country CCPs recognised to offer their services in it, with the Tier each of them is in, as two pdf files at fixed addresses, `ESMA_EU_CCP_URL` and `ESMA_TC_CCP_URL`:

* [CCPs authorised under EMIR](https://www.esma.europa.eu/sites/default/files/library/ccps_authorised_under_emir.pdf), and
* [third-country CCPs recognised under EMIR](https://www.esma.europa.eu/sites/default/files/library/third-country_ccps_recognised_under_emir.pdf).

`download_esma_ccp` reads Table 1 of both and stores the CCPs that are current as `YYYYMMDD.csv` under the day of the download, as EIOPA's registers are stored, with the columns `LEI`, `SECTOR` (always `CCP`) and `SUBSECTOR` (`EUCCP`, `T1CCP` or `T2CCP`). It runs inside `update_entitymasterdata`.

The tables are read for where their text stands, not for their words alone:

* **A withdrawn CCP is struck through.** ESMA leaves a CCP whose authorisation or recognition was withdrawn in the table and crosses its row out -- the British CCPs since Brexit, the Indian ones since 2023. The strike-through is not a property of the text but a thin bar drawn through it, so the reader compares the bars of a page with the position of each LEI. A CCP withdrawn and recognised again stands in the table twice, crossed out and not, and counts as current.
* **An LEI is joined where the table wraps it.** The LEI column is narrow, and an LEI is often split over two lines. The pieces are joined where they stand one below the other, and only where the result has the check digits of an LEI (ISO 17442), so that a wrong join yields no LEI rather than a false one.
* **The Tier is read off the row** of a recognised third-country CCP. LCH Ltd and ICE Clear Europe, no longer authorised in the Union, are current in the second list as CCPs of Tier 2.

**A list that looks wrong is refused, and the one held is kept.** In October 2026 the lists named 15 current EU CCPs, 39 of Tier 1 and 2 of Tier 2. A list read as naming fewer than `ESMA_CCP_MINIMUM` -- five and ten -- or a current third-country CCP without a Tier is refused, since that is a layout the reader no longer understands rather than a market without CCPs. Nothing is stored then, the failure is logged as an error, and the update carries on with the list the Bronze layer already holds.

**The Bronze layer is the only record of how the lists changed.** Like every other register the file leaves the landing zone once the Bronze import has read it, and the Bronze layer keeps one list per day of download, the latest of which is current. ESMA publishes only the current lists and dates neither, so the lists kept are the only record of how they changed: a list replaced at ESMA cannot be fetched again. A list stored under the undated name `ccp_list` by an earlier version is renamed to the day it was last written on the next import, in the landing zone and in the Bronze layer; where a dated list of that day is already there, the import stops and names both rather than dropping either.

The list used to be taken from a reviewed copy kept by hand on GitHub. Reading ESMA's lists added six CCPs that copy lacked: CCP.RO Bucharest, authorised in July 2026, and CDS Clearing, Nasdaq Dubai, Dubai Clear, Dubai Commodities Clearing and the Clearing Corporation of India, recognised -- or recognised again -- since it was last updated. A list kept by hand can still be stored as `YYYYMMDD.csv` in `{landing zone of entitymasterdata}/esma/ccp`.

### 2 The Bronze layer

**`update_from_landing_to_bronze` brings every register the landing zone holds into parquet, one entry per register and reference date, and removes a file from the landing zone only once it has been written.**

```python
em.update_from_landing_to_bronze(config_path, spark)
```

A file that cannot be read stops the run and stays where it is, rather than leaving a gap in the Bronze layer that nothing points at.

**Pass the Spark session: it is what lets the GLEIF golden copies be imported on a small machine.** The Level 1 copy is about half a gigabyte zipped and 4.7 GB as csv over some two hundred columns; read into one pandas frame it needs more memory than a machine of 8 GB has, where the kernel kills the import. Given a session, the copy is read in splits and written without ever being held, and the import stays near 1.3 GB. The other registers are small enough that the session does not matter, and the stage runs without one as it always has -- the two GLEIF functions then read with pandas, which is still the right thing on a machine with the memory for it.

The session is asked for rather than taken from whichever one happens to be active. No module of this package opens a session, and a stage whose reader depended on one another stage had left behind would read the same file in two ways for reasons its caller cannot see.

A GLEIF entry in the Bronze layer is therefore a parquet *directory* where a session was given and a parquet *file* where it was not. Both are read the same way, by `spark.read.parquet`, and the Silver layer picks up whichever is there.

### 3 The public table

```python
em.update_silver_entitymasterdata_public(spark, config_path)
```

**`update_silver_entitymasterdata_public` combines the most recent version of every register in the Bronze layer into one row per lei,** stored in the Silver layer under `{silver layer of entitymasterdata}/public`.

* `file_format`: The format the table is stored in, `'parquet'` or `'delta'`. Where it is left out, the configuration file decides: a Silver layer addressed by path is written as parquet, and a table that exists already keeps the format it is stored in. See [Storage format and catalogue](#storage-format-and-catalogue) for what the choice amounts to.

What each source supplies:

- **GLEIF Level 1** supplies the row itself, with the name and the country.
- **The sectoral registers** supply `sector` and, where they have one, `subsector` (table in [Sectors](#sectors)).
- **GLEIF Level 2** supplies `ultimate_parent_lei` and `amc`.
- **GLEIF's MIC-to-LEI mapping** supplies `mic`.

#### How an LEI is written

**Every register is joined on the lei as text, so an lei is first written the way GLEIF writes it:** blanks removed -- the non-breaking space a spreadsheet leaves among them -- and letters in upper case. An lei holds neither, so this changes no lei that was well-formed, and it is what the registers need: the ESMA list of CCPs of September 2026 carried six LEIs with a trailing space and the EIOPA IORP register three in lower case, which, joined as they stand, leave two central counterparties and three pension funds without a sector. An lei that still does not look like one -- eighteen letters or digits and two check digits -- is kept, but named in a warning with the register it came from, because it will not match GLEIF.

#### Sectors and which register wins

**The narrower description wins, in the order of the [sector table](#sectors).** An entity can stand in more than one register -- a central counterparty is licensed as a credit institution as well.

A register can also carry the same lei twice: a central counterparty authorised under more than one heading stands in the list of CCPs once per heading, and an institution licensed in several member states stands in the list of MFIs once per state. The order of the table settles nothing there, so the choice runs on through the sector and the subsector, preferring a row that states one to a row that does not and otherwise taking the alphabetical one. That decides nothing anyone asked about, but it decides it the same way on every run, which is what the table has to do to be comparable with itself.

The insurance register stands above the register of institutions for occupational retirement provision because the entities in both are insurers running occupational pension business, and the insurance register states the authorisation the entity holds while the IORP register states an activity carried on under it.

**`if_policy`, `if_type` and `if_ucits_compl` are not part of this contest.** They are facts about a fund that no other register carries, so they are taken from the fund register whichever register ends up describing the entity. A money market fund is the case this matters for, because it stands on the list of MFIs by definition and that register wins the sector; taken from the winning row, the fund characteristics would be lost along with the row the contest discards.

**`subsector` is part of the contest, and stays the statement of the register that won it.** It is the refinement that register offers, so filling it in from the fund register once another register has taken the sector would pair a sector from one taxonomy with a refinement from another. A fund that loses the sector to another register loses little by that, because whether it is a UCITS -- what the fund register would have contributed as `subsector` -- is what `if_ucits_compl` carries, and its investment policy is in `if_policy`.

**The `MFI` sector is the fallback for a category the module does not recognise.** A category the ECB relabels, or a list converted by hand in a way that alters the spelling, would otherwise leave the sector empty -- and an empty sector is what an entity no register covers carries, which is the one thing an institution that is on the register is not. The categories that fell through are named in a warning, so the run says where the description is coarser than the register would allow.

#### MICs

**`mic` holds the market identifier codes (ISO 10383) GLEIF links to the lei now, sorted, as a list,** since an operator often runs several markets or segments -- the busiest LEI held seven MICs in November 2026. A MIC whose lei GLEIF Level 1 does not know names no entity of the table. `entities_as_of` takes the MICs that held on its day.

#### Parents and fund managers

**Only a relationship GLEIF still marks `ACTIVE` is read.** GLEIF Level 2 supplies `ultimate_parent_lei` from the `IS_ULTIMATELY_CONSOLIDATED_BY` relationship and `amc` from `IS_FUND-MANAGED_BY`. The file is a record of what has been reported rather than of what holds today: a relationship that has ended stays in it, marked `INACTIVE`, so an entity whose parent changed stands in two records of the same type and only one of them says who consolidates it now. An entity whose only record is an inactive one is left without a parent, which is what it has.

### 4 The final table

```python
em.update_silver_entitymasterdata(spark, config_path)
```

**`update_silver_entitymasterdata` puts a proprietary list in front of the public data** and writes the entity master data set to `{silver layer of entitymasterdata}/main`.

* `file_format`: The format the table is stored in, `'parquet'` or `'delta'`, decided as for the public table where it is left out. It says nothing about the format of the public table this one is built from, which is read in whichever format it was written in, so the two tables need not agree.

**Where an lei is described by both, the proprietary description wins in full:** the public row is dropped rather than filled in from, because a list an organisation maintains itself states what that organisation holds the entity to be. Entities the proprietary list carries without an lei are all kept, since there is nothing to match them on.

**`mic` is the exception to the proprietary description winning:** it is GLEIF's identifier of the markets an lei operates rather than a description of the entity, so it is taken by lei whichever source describes the entity, and the proprietary list has no column for it. A public table built before the `mic` column existed is still read, as one without MICs.

**The catch-all entities.** The table also carries two catch-all entities that no register does (see [the table above](#the-entity-master-data-set-main)). Both LEIs have the form ISO 17442 gives an lei, check digits included, so they pass wherever an lei is checked. No lei of the GLEIF golden copy of 8 September 2026 begins with `XOTH` or `XUNK`. A proprietary list that describes either of them takes precedence, as it does for any other entity.

#### The proprietary list

**Due to the heterogeneity of the data source, the module does not provide any functionality to download data from proprietary sources.** It picks such data up from

```
{path to Bronze Layer of entitymasterdata as specified in config_path}/internal/entitymasterdata_internal.csv
```

if that file exists, and expects a comma-separated file with a header row and the following columns. Every column is read as text, so an identifier that happens to be all digits keeps its leading zeros.

| Column | Content |
| --- | --- |
| `LEI` | The LEI of the entity, where it has one. An entity without one is kept as it stands. |
| `INTERNALID_1` to `INTERNALID_4` | The identifiers the organisation knows the entity by. Any that are not used may be left out of the file. |
| `NAME` | The name of the entity. |
| `CTRY` | The ISO-2 code of its country. |
| `SECTOR`, `SUBSECTOR` | The sector, in the vocabulary of the [sector table](#sectors). |
| `ULTPAR_LEI`, `AMC` | The LEI of the ultimate parent and, for a fund, of the asset management company. |
| `IF_POLICY`, `IF_TYPE`, `IF_UCITS_COMPL` | The fund characteristics, for a fund. |

The columns are taken from the header, so they may come in any order and the identifier columns that are not used may be left out. The LEIs are written the way GLEIF writes them, as the public registers' are (see [How an LEI is written](#how-an-lei-is-written)). The update refuses a list

* without an `LEI` column, which would displace nothing;
* with a column the table above does not name -- `Sector` for `SECTOR`, say, or a `MIC` column -- which would otherwise be dropped when the table is written, with everything the list says under it;
* that describes an LEI more than once, however it is spelled, since the choice between the two rows would then be left to the order Spark reads them in.

#### Storage format and catalogue

**Parquet is the default; ask for Delta where a table may be queried while it is rebuilt.** Each update replaces its table rather than adding to it. Parquet is the default, because it is the format every engine reads without a further dependency. A Delta update replaces the table in a single transaction, so a reader sees either the previous version in full or the new one, never a half-written file, and the previous versions remain readable. A parquet update deletes the table before it writes the new one, which leaves a window in which a reader finds it missing or half-written.

`connect_to_entitymasterdata` reads a table in whichever form it is addressed and stored in, so the choice does not have to be repeated when the table is queried.

**A table that exists already keeps its format.** An update that asks for the other one is refused before anything is written, because Delta will not adopt parquet files it has no transaction log for, and parquet written over a Delta table would throw its history away. Delete the directory before the first update in the new format to change it. The storage itself is shared with the other modules and described in `fincommons.storage`.

**The Silver layer may instead be addressed by catalogue name,** which puts the two tables within reach of Unity Catalog's grants and lineage. The configuration file alone says so; no argument of the updates has to change:

```yaml
entitymasterdata:
    landing: /Volumes/fincommons/entitymasterdata/lake/landing
    bronze: /Volumes/fincommons/entitymasterdata/lake/bronze
    silver:
        table: fincommons.entitymasterdata
```

The tables are then `fincommons.entitymasterdata.public` and `fincommons.entitymasterdata.main`, and they are always Delta tables, because that is what a Unity Catalog managed table is. Asking for `file_format = 'parquet'` there is refused before anything is read. `ref_silver_public` and `ref_silver_main` say where the tables live in either case; `path_silver_public` and `path_silver_main` are `None` for a table in the catalogue.

### 5 The sector report

**`write_sector_counts` counts the entities of every sector on every date a register was published,** and the template [`templates/entitymasterdata-sector-report.qmd`](templates/entitymasterdata-sector-report.qmd) turns the counts into a report on banks, insurers and investment funds over time, the funds broken down by UCITS compliance and by investment policy.

```python
em.write_sector_counts(spark, config_path)
```

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render entitymasterdata-sector-report.qmd
```

- Each date combines the latest snapshot of every register on or before it by the rules the public table is built by (`entities_as_of`), so the report is the history of the registers in miniature.
- A register with no snapshot by a date is shown as not yet collected rather than as zero.
- GLEIF is the exception among the snapshots: where no golden copy of the date or before is held the earliest one is used, since it is the frame the registers are joined onto rather than a statement about a sector.
- The counts are written to `sector_counts.parquet` and the snapshots behind them to `sector_snapshots.parquet`, in the Silver folder of the module, which is where the template reads them. `sector_counts_over_time` returns the same two frames without writing them.

## Reference

### Configuration

| Key in the `entitymasterdata` section | Holds | Catalogue name allowed? |
| --- | --- | --- |
| `landing` | the downloaded registers, in `gleif/level_1`, `gleif/level_2`, `ecb/investment_fund`, `ecb/mfi`, `ecb/fvc`, `ecb/psri`, `eiopa/insurance_undertaking`, `eiopa/iorp`, `esma/ccp` | no, a directory |
| `bronze` | the imported registers as parquet, in the same folders, plus `internal/entitymasterdata_internal.csv`, the proprietary list | no, a directory |
| `silver` | the tables `public` and `main`, and the sector counts | yes, as `table: <catalogue>.<schema>` (see [Storage format and catalogue](#storage-format-and-catalogue)) |

A `gleif` section, read by [`fincommons.gleif`](../gleif/README.md#configuration), switches on reading GLEIF from its history and the MICs.

### Functions

All are available as `em.<name>` after `from fincommons import entitymasterdata as em`.

**Run and read**

| Function | What it does |
| --- | --- |
| `update_entitymasterdata(spark, config_path, url_gleif_level_1=None, url_gleif_level_2=None, file_format=None)` | The whole pipeline: downloads, Bronze import, both Silver tables. |
| `connect_to_entitymasterdata(spark, config_path, table="main")` | Open `"main"` or `"public"`. |
| `entities_as_of(spark, config_path, day)` | The public entity master data as the registers stood on `day`; returns the frame and the snapshot each register was read from. |
| `sector_counts_over_time(spark, config_path, dates=None)` | The entities per sector, subsector and investment policy on every date, as two pandas frames. |
| `write_sector_counts(spark, config_path)` | Write those counts for the sector report. |
| `create_directory_tree(config_path)` | Create the folders of the landing zone, the Bronze layer and a Silver layer addressed by path; can be repeated. |

**Single stages**

| Function | What it does |
| --- | --- |
| `download_latest_gleif(config_path)` | GLEIF Level 1 and Level 2 of the latest publication, unless the Bronze layer holds one of its day. |
| `download_gleif_level_1(config_path, url)`, `download_gleif_level_2(config_path, url)` | A GLEIF golden copy from its link. |
| `download_ecb_investment_fund`, `download_ecb_mfi`, `download_ecb_fvc`, `download_ecb_psri` | The ECB's lists (each takes `config_path`). |
| `download_eiopa_insurance_undertaking`, `download_eiopa_iorp` | EIOPA's registers (each takes `config_path`). |
| `download_esma_ccp(config_path)` | ESMA's lists of CCPs. |
| `update_from_landing_to_bronze(config_path, spark=None)` | Import everything the landing zone holds into the Bronze layer. |
| `update_silver_entitymasterdata_public(spark, config_path, file_format=None)` | Build the public table. |
| `update_silver_entitymasterdata(spark, config_path, file_format=None)` | Build the entity master data set. |

**Helpers**

| Function | What it does |
| --- | --- |
| `gleif_history_held(spark, config_path)` | Whether GLEIF is read from the history of `fincommons.gleif`. |
| `gleif_from_history(spark, config_path, day=None)` | GLEIF's entities and relationships from that history, in this module's names. |
| `gleif_mic_lei(spark, config_path)` | The MICs GLEIF links to each LEI now, or `None` where the history has not been built. |
| `combine_registers(...)` | The rules the public table is built by, applied to one snapshot of each register. |
| `lei_is_valid(lei)` | Whether an LEI has the check digits of ISO 17442. |
| `ref_silver_public`, `ref_silver_main`, `path_silver_public`, `path_silver_main`, `path_landing_*`, `path_bronze_*` | Where the files and tables live. |
| `migrate_folder_names(config_path)` | Rename the hyphenated folders and files of an older lake to the snake_case names the module reads. |

**Constants:** `LEI_OTHER`, `LEI_UNKNOWN`, `ESMA_EU_CCP_URL`, `ESMA_TC_CCP_URL`, `ESMA_CCP_MINIMUM`, `GLEIF_LATEST_URL`, `COLUMNS` (the columns of `main`, in order).

### Further reading

- [Lenoci and Letizia (2021)](https://link.springer.com/chapter/10.1007/978-3-030-66891-4_6), the approach the table follows.
- The handbook chapter, [`docs/handbook/01-entitymasterdata.tex`](../../../docs/handbook/01-entitymasterdata.tex).
- [`fincommons.gleif`](../gleif/README.md), the GLEIF history and MIC-to-LEI mapping this module reads where they have been built.

### Tests

The module is covered by the tests in `tests/test_entitymasterdata.py`, which run against the local Spark session built by `tests/conftest.py`. Nothing in them reaches GLEIF, the ECB, EIOPA or ESMA: the landing zone fixtures are written inline in the shapes the publishers use, and every download is given a response of its own.

| Group | Covers |
| --- | --- |
| the data lake | creating the directory tree, including below a directory that does not exist yet, repeating the call, refusing an unknown table name, reporting a table that has never been built, and the package not re-exporting the modules its submodules import |
| the downloads | that a GLEIF file keeps the name it was published under, that a link carrying a query string or a fragment still yields that name, that a link which is not a file is refused, that every request carries a timeout, that the connection is closed when the download fails, and that an error answer, an answer that is not an archive and an interrupted download all leave the landing zone as it was; that the ECB's lists are found on its pages, only the lists the Bronze layer lacks are unpacked and an answer that is not the list of MFIs is refused; and that EIOPA's form is read off the page and sent for the CSV export, asked for again when it is turned away, and refused where the answer is not the register; and that ESMA's lists of CCPs are read for Table 1 alone, a wrapped LEI is joined, a struck-through row counts as withdrawn and a CCP recognised again as current, a list read as naming too few CCPs is refused and an answer that is not a pdf does not replace the list held |
| the Bronze layer | the reference date read off the name, the file leaving the landing zone only once it has been written -- the list of CCPs included, one per day of download, with an undated list of an earlier version dated by the day it was written -- a file that cannot be read staying where it is, files of another kind being left alone and named in a warning, the monthly and the quarterly list of investment funds, a missing LEI staying missing, MFI postcodes kept as text, and the import carrying on where the list of CCPs was never downloaded; the MFI list and EIOPA's registers read from their CSV as text, both spellings of EIOPA's dates, a workbook with mixed cells, and the lists of FVCs and PSRIs dated at the end of their period |
| the public table | the name and country from GLEIF, the sector each register contributes, which register wins where two describe the same entity, the fund characteristics surviving a register that outranks the fund register, a register that carries an LEI twice answering the same way whichever order the file holds the rows in, telling the two Level 2 relationships apart, reading only the relationships that are still active, the fallback sector for an unrecognised MFI category, using the most recent register, and reporting a register the Bronze layer never received |
| how an LEI is written | a trailing space, lower case and a non-breaking space in a register, a parent written differently in the Level 2 data, and an LEI still malformed being named with its register |
| the entity master data set | the public data passing through, the catch-all entities and the form of their LEIs, the numbering, the promised columns, what a proprietary list displaces -- written differently included --, entities without an LEI, leading zeros, a list describing an LEI twice, carrying a column the table does not have or no LEI column being refused, and the storage format of both tables |
| GLEIF and MICs from `fincommons.gleif` | names, countries and parents read from the GLEIF history as they stand and as they stood; every MIC an entity operates, as a sorted list; the MICs of now and of a day; the table built without the MIC-to-LEI history; an entity the proprietary list describes keeping its MICs; a public table built before the `mic` column; a `MIC` column in the proprietary list being refused |

Some of these are worth naming, because they are pinned to rules that are easy to undo by accident rather than to behaviour that would obviously break:

* `test_the_narrower_register_wins_where_two_describe_the_same_entity` holds the order of the registers. The precedence rests on an explicit rank rather than on the order in which the rows of the union arrive, which is not specified.
* `test_internal_entities_without_an_lei_are_all_kept` holds those rows being set aside before the deduplication. A window partitioned by `lei` gathers every row without one into a single group and keeps one of them.
* `test_a_file_that_cannot_be_read_stays_in_the_landing_zone` holds the import raising rather than reporting and carrying on, which is what keeps a register from silently going missing from the Bronze layer.
* `test_an_error_answer_is_not_stored_as_the_register` holds the download refusing what the server said rather than storing it. The two rules meet here: a file that cannot be read stays in the landing zone, so a bad answer stored under the name of a register blocks every later run of the Bronze import.
* `test_an_internal_identifier_keeps_its_leading_zeros` holds the proprietary list being read as text throughout.
* `test_a_relationship_that_has_ended_is_not_the_current_one` holds the status of a Level 2 relationship being read. The parents in it are named so that the lapsed one sorts first, because ordering the rows of an LEI is what makes the choice repeatable and not what makes it right.
* `test_an_institution_on_the_mfi_register_is_never_read_as_uncovered` holds the fallback sector. Without it an unrecognised category is indistinguishable from an entity no register covers.
* `test_the_subsector_stays_the_statement_of_the_register_that_won` holds the line between the two kinds of column the fund register contributes. The fund characteristics cross the precedence of the registers and `subsector` does not, which is a decision rather than an accident of the code.
* `test_a_register_carrying_an_lei_twice_answers_the_same_way_either_way` runs the same two rows in both of the orders the file can hold them in, and holds that the answer does not depend on which. Asserting a single pick would pass against an incomplete ordering too, because Spark reads a small test file in one partition; it is the two orders having to agree that makes the test bite.

Run them, from the root of the repository, with

```bash
pytest tests/test_entitymasterdata.py
```

## Troubleshooting

- **The Bronze import stops on a file it cannot read, on every run.** A file that cannot be read stays in the landing zone by design -- often a GLEIF error page stored from a stale link by hand. Remove it from the landing zone; then download from a fresh link.
- **A register saved by hand is ignored, with a warning.** Only `.xlsx` workbooks (and `.csv` where the register comes as one) are read, not `.xls`. Save it again as `.xlsx`.
- **The import of the GLEIF golden copy is killed on a machine with 8 GB.** It was read with pandas. Pass the session: `update_from_landing_to_bronze(config_path, spark)`.
- **An update is refused because of the format.** A table keeps the format it is stored in, and parquet is refused for a table in a catalogue. Delete the directory before the first update in the new format.
- **The proprietary list is refused.** It has no `LEI` column, a column the [list's table](#the-proprietary-list) does not name, or an LEI described twice.
- **A warning names a malformed LEI.** A register carries an LEI that does not have eighteen letters or digits and two check digits even after normalisation; it will not match GLEIF.
- **A warning names MFI categories.** The ECB used a category the module does not recognise; those entities have sector `MFI`.
- **The list of CCPs is refused.** ESMA's layout changed so that too few CCPs were read, or a third-country CCP has no Tier; the list held is kept. Store a reviewed list by hand as `YYYYMMDD.csv` in `esma/ccp` if needed.
- **`mic` is empty, with a warning.** `paths.yaml` has no `gleif` section or the MIC-to-LEI history of [`fincommons.gleif`](../gleif/README.md#the-mic-to-lei-mapping) has not been built; run `gleif.update_gleif_mic_lei`.
- **`write_sector_counts` raises `ValueError`.** The counts are written as files, which a Silver layer addressed by catalogue name has no folder for.
- **A hyphenated lake from an earlier version is not found.** Run `migrate_folder_names(config_path)` once.
