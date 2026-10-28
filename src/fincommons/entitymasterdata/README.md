# Entity master data

Version: 24 July 2026

This script describes the creation of a entity master data set that can be used in the context of, for example, the analysis of EMIR, SECR or SFTR data. The approach is based on [Lenoci and Letizia (2021)](https://link.springer.com/chapter/10.1007/978-3-030-66891-4_6).



```python
# Set path to configuration file, where paths are specified
config_path = "/path/to/paths.yaml"

# Load the module
from fincommons import entitymasterdata as em

# Create folders for entity master data, unless they already exist
em.create_directory_tree(config_path = config_path)
```

Start the Spark session.

The Silver layer is written as parquet unless `file_format = 'delta'` asks for Delta or the configuration file addresses it by catalogue name (see section 3.2). Where it is Delta, the session has to be built with the Delta extensions off-cluster, which needs the `delta-spark` package that comes with the `local` extra; on Databricks Delta is built in and the extra configuration is unnecessary. A session built this way reads a parquet table just as well, so it is the safe thing to start from either way.


```python
# Start Spark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder \
                    .appName("fincommons") \
                    .config("spark.cores.max", "20") \
                    .config("spark.driver.memory", "15g") \
                    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
                    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```


## 1 Raw data collection in the landing zone

Every source can be fetched over the network, and `update_entitymasterdata` does so on every run. A register whose download fails is logged as an error and the update carries on with the register the Bronze layer already holds. A register can still be placed in the landing zone by hand, under the names given below: the Bronze import reads the reference date off the name of the file, so the name is what dates the register.

The registers stored by hand as workbooks have to be saved as `.xlsx`. The legacy `.xls` format is not read -- doing so needs a package `fincommons` does not depend on -- and neither is anything else. A file the import leaves alone is named in a warning rather than passed over in silence, because in a folder filled by hand the likeliest reason for a file of the wrong kind is the register having been saved in the wrong format.

### 1.1 GLEIF data

#### 1.1.1 Level 1 data

GLEIF Level 1 data links an LEI to the name and the country of the entity it identifies. GLEIF publishes the golden copy three times a day, and the link of a file changes with every publication. Its [Golden Copy API](https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest) names the files of the latest one, and `download_latest_gleif` downloads Level 1 and Level 2 from there -- each only where the Bronze layer holds no golden copy of that day, since Level 1 is about half a gigabyte zipped and a second publication of the same day differs by hours. `update_entitymasterdata` does this on every run.


```python
em.download_latest_gleif(config_path)
```

An earlier golden copy, or one the API cannot be reached for, is downloaded from its link instead. The links are on [the golden copy download page](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/): take the link of the **csv** file in the *LEI-CDF* column and pass it as `url`.


```python
em.download_gleif_level_1(config_path, "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/1255025/20260724-0000-gleif-goldencopy-lei2-golden-copy.csv.zip")
```

The file is stored under the name GLEIF published it with, which begins with the reference date. A link that is not the link of a file is refused before anything is fetched.

A download either leaves the file it was asked for or leaves nothing at all. The landing zone is not a scratch directory -- the Bronze import reads the reference date off the name of a file, so a file under the name of a register is a claim that the register is there. An error answer from the server is therefore raised rather than stored, and so is an answer that does not open as an archive, which is what a stale link, a proxy or a login wall returns with a success status. The bytes are written under a name ending in `.part` and given the name the Bronze import looks for only once they are all there, so an interrupted download leaves nothing behind either. Requests carry a timeout, so a stalled connection does not hold the run indefinitely.

This matters most for GLEIF, because the address of the golden copy changes with every release: a link taken from the download page some time ago is the failure to expect, not an exotic one. Its error page, stored under the name of the register, would break the Bronze import, and -- since a file that cannot be read stays in the landing zone by design -- every later run as well, including the import of the other six registers, until it is removed by hand.

#### 1.1.2 Level 2 data

The second GLEIF data set covers the ownership relationships between legal entities, from which the module takes the ultimate parent of an entity and, for a fund, the company that manages it. `download_latest_gleif` downloads it with Level 1; by hand, take the link of the **csv** file in the *RR-CDF* column of the same page.


```python
em.download_gleif_level_2(config_path, "https://leidata-preview.gleif.org/storage/golden-copy-files/2026/07/24/1255070/20260724-0000-gleif-goldencopy-rr-golden-copy.csv.zip")
```

### 1.2 ECB data

The ECB's page of the [lists of financial institutions](https://www.ecb.europa.eu/stats/financial_corporations/list_of_financial_institutions/html/index.en.html) links each list under a name that changes with the list, so the downloads read the current link off the page rather than keeping an address. The lists of investment funds, FVCs and PSRIs come as zip archives of workbooks, one per period, and are replaced as new lists are published. The download takes the latest archive and puts into the landing zone each workbook whose period the Bronze layer does not hold yet, under the name the ECB gives it. A run after the first therefore unpacks only the lists published since.

#### 1.2.1 Investment funds

```python
em.download_ecb_investment_fund(config_path)
```

The latest archive of a single year ("IFs Overview 2026") is taken; the page also links archives of several years, which hold no newer list. The list is published per month or per quarter and the name of the workbook states which, as `IF_<year>_<month>.xlsx` or `IF_<year>_Q<quarter>.xlsx`. The reference date of the Bronze file is the last day of that period. By hand: unzip the archive and save the workbooks in `{landing zone of entitymasterdata}/ecb/investment_fund`.

#### 1.2.2 Monetary financial institutions

```python
em.download_ecb_mfi(config_path)
```

The ECB publishes a [daily list of MFIs](https://www.ecb.europa.eu/stats/financial_corporations/list_of_financial_institutions/html/daily_list-MID.en.html), the "Full Dataset" of the day as `mfi_csv_<YYMMDD>.csv`. The download stores it in `{landing zone of entitymasterdata}/ecb/mfi` as `<YYYYMMDD>.csv`, unaltered, and does not fetch a day the Bronze layer holds. The file is text separated by tabs and encoded in UTF-16, which is why it used to be opened in a spreadsheet and saved as a workbook; the import reads it as it is, and still reads a workbook `YYYYMMDD.xlsx` stored by hand.

Every column is imported as text, as the register writes it. A postcode is not a number: read as one, `00187` loses its leading zero and `1012 AB` becomes a missing value.

#### 1.2.3 Financial vehicle corporations

```python
em.download_ecb_fvc(config_path)
```

The quarterly lists of financial vehicle corporations engaged in securitisation, `FVC_<year>_Q<quarter>.xlsx`, from the latest archive of a single year, stored in `{landing zone of entitymasterdata}/ecb/fvc`. Each is imported under the last day of its quarter.

#### 1.2.4 Payment statistics relevant institutions

```python
em.download_ecb_psri(config_path)
```

The lists of payment statistics relevant institutions -- payment service providers and payment system operators -- yearly until 2021, `PSRIs_<year>.xlsx`, and half-yearly since, `PSRIs_<year>_H<half>.xlsx`, all in one archive and stored in `{landing zone of entitymasterdata}/ecb/psri`. The first run puts every list since 2014 into the landing zone. Each is imported under the last day of its period, and with the columns as that list names them: the lists until 2021 spell `Country of residence` and have a column `PSP Geo Scope`, the later ones spell `Country of Residence`.

The FVCs and the PSRIs are imported into the Bronze layer but not yet part of the entity master data set: where they rank among the other registers in the field `SECTOR` is still to be decided.

### 1.3 EIOPA data

EIOPA's registers are SharePoint pages whose export is not a link to a file but a postback of the page's form. The download reads the page for its form -- the view state and the other hidden fields -- and sends it back as the "**CSV**" link would, then stores the answer in the landing zone as `<YYYYMMDD>.csv` under the day of the download. A second download on the same day replaces the first. EIOPA's gateway now and then answers an export with `403 Forbidden` that it serves a moment later, so an export turned away is asked for again from a fresh page, three times at most, thirty seconds apart.

The CSV export is taken rather than the Excel one because the workbook stores an LEI made of digits alone as a number, and a twenty-digit number does not survive that: in the register of insurance undertakings of 28 October 2026, 276 LEIs came out of the workbook as `31570010000000098304`, `31570010000000000000` and the like -- several undertakings under one LEI -- and 337 in the register of IORPs. The CSV export writes every value as the register holds it. It is imported as text, every column; its dates, written day first with a time and now and then as ISO 8601 in the same column, are read into `YYYY-MM-DD`.

By hand: click "**CSV**" on the page and store the file under the current date, `YYYYMMDD.csv`. A workbook `YYYYMMDD.xlsx` from the "Excel" link is still read, with the LEIs it has lost.

#### 1.3.1 Insurance undertakings

```python
em.download_eiopa_insurance_undertaking(config_path)
```

From the [register of insurance undertakings](https://register.eiopa.europa.eu/registers/register-of-insurance-undertakings), into `{landing zone of entitymasterdata}/eiopa/insurance_undertaking`.

#### 1.3.2 Institutions for Occupational Retirement Provision

```python
em.download_eiopa_iorp(config_path)
```

From the [register of institutions for occupational retirement provision](https://register.eiopa.europa.eu/registers/register-of-institutions-for-occupational-retirement-provision), into `{landing zone of entitymasterdata}/eiopa/iorp`.

### 1.4 ESMA data

#### 1.4.1 CCPs

**No manual steps required. This section can be skipped.**

There are different types of CCP in the EU:

* a list of EU CCPs is available at [https://www.esma.europa.eu/sites/default/files/library/ccps_authorised_under_emir.pdf](https://www.esma.europa.eu/sites/default/files/library/ccps_authorised_under_emir.pdf), and
* a list of third-country CCPs at [https://www.esma.europa.eu/central-counterparties/third-country-ccps](https://www.esma.europa.eu/central-counterparties/third-country-ccps).

ESMA publishes both only as pdf files, which are difficult to read with Python. Until that changes, the list is downloaded from the public GitHub repository [https://github.com/franzmohr/ccp-list](https://github.com/franzmohr/ccp-list), where the same information is kept in a machine-readable form. It is a csv file with the columns `LEI`, `SECTOR` (always `CCP`) and `SUBSECTOR` (`EUCCP`, `T1CCP` or `T2CCP`).

Function `download_esma_ccp` fetches it, from a fixed commit of that repository, `ESMA_CCP_URL`, rather than from its current branch: what the pipeline imports is the version that was reviewed, and a newer list reaches the data lake only when the address is moved to a newer commit. It runs inside `update_entitymasterdata` as well, so it does not normally have to be called on its own. Where the download fails there -- on a cluster without internet access, say -- the failure is logged as an error and the update carries on with the list the Bronze layer already holds. Like every other register the file leaves the landing zone once the Bronze import has read it: the list is the one register whose name carries no reference date, so a copy left behind would be indistinguishable from one just downloaded, and a run whose download had failed would import it and report it as the register.


```python
em.download_esma_ccp(config_path)
```

Once ESMA publishes the information in a machine-readable form, a function can be written that reads it from the ESMA website directly.

## 2 Updating the Bronze layer

The following function brings the content of every register the landing zone currently holds into a queryable format, one parquet file per register and reference date. A file is removed from the landing zone only once it has been written; a file that cannot be read stops the run and stays where it is, rather than leaving a gap in the Bronze layer that nothing points at.


```python
em.update_from_landing_to_bronze(config_path)
```

## 3 Updating the Silver layer

### 3.1 Public entity master data set

Function `update_silver_entitymasterdata_public` combines the most recent version of every register in the Bronze layer into one row per LEI. The result is stored in the Silver layer under `{silver layer of entitymasterdata}/public`.


```python
em.update_silver_entitymasterdata_public(spark, config_path)
```

                                                                                    

* `file_format`: The format the table is stored in, `'parquet'` or `'delta'`. Where it is left out, the configuration file decides: a Silver layer addressed by path is written as parquet, and a table that exists already keeps the format it is stored in. See section 3.2 for what the choice amounts to.

GLEIF Level 1 supplies the row itself, with the name and the country. The sectoral registers supply `SECTOR` and, where they have one, `SUBSECTOR`:

| Register | `SECTOR` |
| --- | --- |
| ESMA list of CCPs | `CCP`, with the type of CCP as `SUBSECTOR` |
| EIOPA insurance undertakings | `INUN` |
| EIOPA IORPs | `ORPI` |
| ECB MFIs | `CB`, `CDTI`, `OFI` or `MMF`, after the category of the register, and `MFI` where the category is not one of the four |
| ECB investment funds | `UCIT`, `AIFD` or `INVF`, after what the register says about UCITS compliance, with the investment policy as `SUBSECTOR` |

Every register is joined on the LEI as text, so an LEI is first written the way GLEIF writes it: blanks removed -- the non-breaking space a spreadsheet leaves among them -- and letters in upper case. An LEI holds neither, so this changes no LEI that was well-formed, and it is what the registers need: the ESMA list of CCPs of September 2026 carried six LEIs with a trailing space and the EIOPA IORP register three in lower case, which, joined as they stand, leave two central counterparties and three pension funds without a sector. An LEI that still does not look like one -- eighteen letters or digits and two check digits -- is kept, but named in a warning with the register it came from, because it will not match GLEIF.

The table carries the columns of section 5 except for `ID` and the `INTERNALID_*` columns, which the entity master data set itself adds, plus `REFDATE`: the reference date of the GLEIF Level 1 file it was built from, which is what dates the table.

An entity can stand in more than one of them -- a central counterparty is licensed as a credit institution as well -- and the narrower description wins, in the order of the table. A register can also carry the same LEI twice: a central counterparty authorised under more than one heading stands in the list of CCPs once per heading, and an institution licensed in several member states stands in the list of MFIs once per state. The order of the table settles nothing there, so the choice runs on through the sector and the subsector, preferring a row that states one to a row that does not and otherwise taking the alphabetical one. That decides nothing anyone asked about, but it decides it the same way on every run, which is what the table has to do to be comparable with itself. The insurance register stands above the register of institutions for occupational retirement provision because the entities in both are insurers running occupational pension business, and the insurance register states the authorisation the entity holds while the IORP register states an activity carried on under it.

`IF_POLICY`, `IF_TYPE` and `IF_UCITS_COMPL` are not part of this contest. They are facts about a fund that no other register carries, so they are taken from the fund register whichever register ends up describing the entity. A money market fund is the case this matters for, because it stands on the list of MFIs by definition and that register wins the sector; taken from the winning row, the fund characteristics would be lost along with the row the contest discards.

`SUBSECTOR` is part of the contest, and stays the statement of the register that won it. It is the refinement that register offers, so filling it in from the fund register once another register has taken the sector would pair a sector from one taxonomy with a refinement from another. A fund loses nothing by that, because the investment policy the fund register would have contributed as `SUBSECTOR` is what `IF_POLICY` carries.

The `MFI` sector is the fallback for a category the module does not recognise. A category the ECB relabels, or a list converted by hand in a way that alters the spelling, would otherwise leave the sector empty -- and an empty sector is what an entity no register covers carries, which is the one thing an institution that is on the register is not. The categories that fell through are named in a warning, so the run says where the description is coarser than the register would allow.

GLEIF Level 2 supplies `ULTPAR_LEI` from the `IS_ULTIMATELY_CONSOLIDATED_BY` relationship and `AMC` from `IS_FUND-MANAGED_BY`. Only a relationship the golden copy still marks `ACTIVE` is read. The file is a record of what has been reported rather than of what holds today: a relationship that has ended stays in it, marked `INACTIVE`, so an entity whose parent changed stands in two records of the same type and only one of them says who consolidates it now. An entity whose only record is an inactive one is left without a parent, which is what it has.

### 3.2 Final entity master data set

Function `update_silver_entitymasterdata` puts a proprietary list in front of the public data and writes the entity master data set to `{silver layer of entitymasterdata}/main`.


```python
em.update_silver_entitymasterdata(spark, config_path)
```

    26/09/21 17:49:16 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    26/09/21 17:49:16 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    26/09/21 17:49:16 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    26/09/21 17:49:19 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    26/09/21 17:49:19 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    26/09/21 17:49:22 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    26/09/21 17:49:22 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
                                                                                    

* `file_format`: The format the table is stored in, `'parquet'` or `'delta'`, decided as for the public table where it is left out. It says nothing about the format of the public table this one is built from, which is read in whichever format it was written in, so the two tables need not agree.

Where an LEI is described by both, the proprietary description wins in full: the public row is dropped rather than filled in from, because a list an organisation maintains itself states what that organisation holds the entity to be. Entities the proprietary list carries without an LEI are all kept, since there is nothing to match them on.

The table also carries two catch-all entities that no register does: `Other` under the LEI `XOTHERX0000000000064` (`LEI_OTHER`), which collects what an aggregation does not show on its own -- the EMIR and SFTR data quality monitoring files the reporting counterparties below its top fifteen under it -- and `Unknown` under `XUNKNOWNX00000000025` (`LEI_UNKNOWN`), for an entity that could not be identified. Both LEIs have the form ISO 17442 gives an LEI, check digits included, so they pass wherever an LEI is checked. No LEI of the GLEIF golden copy of 8 September 2026 begins with `XOTH` or `XUNK`. A proprietary list that describes either of them takes precedence, as it does for any other entity.

Each update replaces its table rather than adding to it. Parquet is the default, because it is the format every engine reads without a further dependency. Delta is worth asking for where a table may be queried while it is rebuilt: a Delta update replaces the table in a single transaction, so a reader sees either the previous version in full or the new one, never a half-written file, and the previous versions remain readable. A parquet update deletes the table before it writes the new one, which leaves a window in which a reader finds it missing or half-written.

`connect_to_entitymasterdata` reads a table in whichever form it is addressed and stored in, so the choice does not have to be repeated when the table is queried. A table that exists already keeps its format: an update that asks for the other one is refused before anything is written, because Delta will not adopt parquet files it has no transaction log for, and parquet written over a Delta table would throw its history away. Delete the directory before the first update in the new format to change it. The storage itself is shared with the other modules and described in `fincommons.storage`.

The Silver layer may instead be addressed by catalogue name, which puts the two tables within reach of Unity Catalog's grants and lineage. The configuration file alone says so; no argument of the updates has to change:

```yaml
entitymasterdata:
    landing: /Volumes/fincommons/entitymasterdata/lake/landing
    bronze: /Volumes/fincommons/entitymasterdata/lake/bronze
    silver:
        table: fincommons.entitymasterdata
```

The tables are then `fincommons.entitymasterdata.public` and `fincommons.entitymasterdata.main`, and they are always Delta tables, because that is what a Unity Catalog managed table is. Asking for `file_format = 'parquet'` there is refused before anything is read. `ref_silver_public` and `ref_silver_main` say where the tables live in either case; `path_silver_public` and `path_silver_main` are `None` for a table in the catalogue.

### 3.3 Proprietary data

Due to the heterogeneity of the data source, the module does not provide any functionality to download data from proprietary sources. It picks such data up from

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
| `SECTOR`, `SUBSECTOR` | The sector, in the vocabulary of the table in section 3.1. |
| `ULTPAR_LEI`, `AMC` | The LEI of the ultimate parent and, for a fund, of the asset management company. |
| `IF_POLICY`, `IF_TYPE`, `IF_UCITS_COMPL` | The fund characteristics, for a fund. |

The columns are taken from the header, so they may come in any order and the identifier columns that are not used may be left out. The LEIs are written the way GLEIF writes them, as the public registers' are (section 3.1). The update refuses a list

* without an `LEI` column, which would displace nothing;
* with a column the table above does not name -- `Sector` for `SECTOR`, say -- which would otherwise be dropped when the table is written, with everything the list says under it;
* that describes an LEI more than once, however it is spelled, since the choice between the two rows would then be left to the order Spark reads them in.

## 4 All-in-one function

The following function runs the whole pipeline: the downloads that can be automated, the import of everything the landing zone holds, and the two steps of the Silver layer. The GLEIF links are optional; where they are not given, the latest golden copy is downloaded unless the Bronze layer holds one of its day. A link that is given is downloaded, and its failure stops the update, since it was asked for.

**Every register is downloaded on each run, as described in section 1: the GLEIF golden copies from the links given, or else the latest publication the Bronze layer lacks.**

```python
em.update_entitymasterdata(
    spark,
    config_path,
    url_gleif_level_1 = "https://leidata-preview.gleif.org/storage/...lei2-golden-copy.csv.zip",
    url_gleif_level_2 = "https://leidata-preview.gleif.org/storage/...rr-golden-copy.csv.zip",
)
```

It passes `file_format` on to both steps of the Silver layer, so the storage format is chosen here as well:

```python
em.update_entitymasterdata(spark, config_path, file_format = "delta")
```

## 5 Reading the result

```python
em.connect_to_entitymasterdata(spark, config_path)            # the entity master data set
em.connect_to_entitymasterdata(spark, config_path, "public")  # the public sources alone
```

The entity master data set carries one row per entity:

| Column | Content |
| --- | --- |
| `ID` | A running number over the table. It is assigned by the update and is not stable across rebuilds. |
| `LEI` | The LEI of the entity, where it has one. |
| `INTERNALID_1` to `INTERNALID_4` | The identifiers of the proprietary list, empty where there is none. |
| `NAME` | The legal name of the entity. |
| `CTRY` | The ISO-2 code of the country of its legal address. |
| `SECTOR`, `SUBSECTOR` | What the entity does, see section 3.1. |
| `ULTPAR_LEI` | The LEI of the entity that ultimately consolidates it. |
| `AMC` | For a fund, the LEI of the company that manages it. |
| `IF_POLICY`, `IF_TYPE`, `IF_UCITS_COMPL` | For a fund, its investment policy, whether it is an ETF or a PEF, and whether it is UCITS compliant. They are filled for every entity the ECB fund register covers, including one another register describes -- a money market fund, say, whose `SECTOR` is `MMF`. |

## Tests

The module is covered by the tests in `tests/test_entitymasterdata.py`, which run against the local Spark session built by `tests/conftest.py`. Nothing in them reaches GLEIF or GitHub: the landing zone fixtures are written inline in the shapes the publishers use, and the two downloads are given a response of their own.

| Group | Covers |
| --- | --- |
| the data lake | creating the directory tree, including below a directory that does not exist yet, repeating the call, refusing an unknown table name, reporting a table that has never been built, and the package not re-exporting the modules its submodules import |
| the downloads | that a GLEIF file keeps the name it was published under, that a link carrying a query string or a fragment still yields that name, that a link which is not a file is refused, that every request carries a timeout, that the connection is closed when the download fails, and that an error answer, an answer that is not an archive and an interrupted download all leave the landing zone as it was; that the ECB's lists are found on its pages, only the lists the Bronze layer lacks are unpacked and an answer that is not the list of MFIs is refused; and that EIOPA's form is read off the page and sent for the CSV export, asked for again when it is turned away, and refused where the answer is not the register |
| the Bronze layer | the reference date read off the name, the file leaving the landing zone only once it has been written -- the list of CCPs included -- a file that cannot be read staying where it is, files of another kind being left alone and named in a warning, the monthly and the quarterly list of investment funds, a missing LEI staying missing, MFI postcodes kept as text, and the import carrying on where the list of CCPs was never downloaded; the MFI list and EIOPA's registers read from their CSV as text, both spellings of EIOPA's dates, a workbook with mixed cells, and the lists of FVCs and PSRIs dated at the end of their period |
| the public table | the name and country from GLEIF, the sector each register contributes, which register wins where two describe the same entity, the fund characteristics surviving a register that outranks the fund register, a register that carries an LEI twice answering the same way whichever order the file holds the rows in, telling the two Level 2 relationships apart, reading only the relationships that are still active, the fallback sector for an unrecognised MFI category, using the most recent register, and reporting a register the Bronze layer never received |
| how an LEI is written | a trailing space, lower case and a non-breaking space in a register, a parent written differently in the Level 2 data, and an LEI still malformed being named with its register |
| the entity master data set | the public data passing through, the catch-all entities and the form of their LEIs, the numbering, the promised columns, what a proprietary list displaces -- written differently included --, entities without an LEI, leading zeros, a list describing an LEI twice, carrying a column the table does not have or no LEI column being refused, and the storage format of both tables |

Some of these are worth naming, because they are pinned to rules that are easy to undo by accident rather than to behaviour that would obviously break:

* `test_the_narrower_register_wins_where_two_describe_the_same_entity` holds the order of the registers. The precedence rests on an explicit rank rather than on the order in which the rows of the union arrive, which is not specified.
* `test_internal_entities_without_an_lei_are_all_kept` holds those rows being set aside before the deduplication. A window partitioned by `LEI` gathers every row without one into a single group and keeps one of them.
* `test_a_file_that_cannot_be_read_stays_in_the_landing_zone` holds the import raising rather than reporting and carrying on, which is what keeps a register from silently going missing from the Bronze layer.
* `test_an_error_answer_is_not_stored_as_the_register` holds the download refusing what the server said rather than storing it. The two rules meet here: a file that cannot be read stays in the landing zone, so a bad answer stored under the name of a register blocks every later run of the Bronze import.
* `test_an_internal_identifier_keeps_its_leading_zeros` holds the proprietary list being read as text throughout.
* `test_a_relationship_that_has_ended_is_not_the_current_one` holds the status of a Level 2 relationship being read. The parents in it are named so that the lapsed one sorts first, because ordering the rows of an LEI is what makes the choice repeatable and not what makes it right.
* `test_an_institution_on_the_mfi_register_is_never_read_as_uncovered` holds the fallback sector. Without it an unrecognised category is indistinguishable from an entity no register covers.
* `test_the_subsector_stays_the_statement_of_the_register_that_won` holds the line between the two kinds of column the fund register contributes. The fund characteristics cross the precedence of the registers and `SUBSECTOR` does not, which is a decision rather than an accident of the code.
* `test_a_register_carrying_an_lei_twice_answers_the_same_way_either_way` runs the same two rows in both of the orders the file can hold them in, and holds that the answer does not depend on which. Asserting a single pick would pass against an incomplete ordering too, because Spark reads a small test file in one partition; it is the two orders having to agree that makes the test bite.

Run them, from the root of the repository, with

```
pytest tests/test_entitymasterdata.py
```



```python

```
