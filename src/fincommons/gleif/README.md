# GLEIF: legal entity identifiers and their history

Module `gleif` keeps a history of the reference data of the [Global Legal Entity Identifier Foundation](https://www.gleif.org) (GLEIF) in the Silver layer (the cleaned Delta tables analyses read): who every legal entity identifier (LEI) belongs to, who owns or manages whom, and which entity operates which market, on any day since the history was begun. Use it to resolve the LEI of a counterparty into a name and a country as they stood on the day of a report, to find the ultimate parent of an entity, or to find the operator of a trading venue.

It is the GLEIF counterpart of the FIRDS history of [`fincommons.firds`](../firds/README.md), and it is read the same way: every state a record held is one version, with the first and the last day it held.

**At a glance**

- **Source:** GLEIF's golden copy (three publications a day, full copies back to February 2018, daily delta files for about two months) and GLEIF's MIC-to-LEI mapping (about weekly, back to August 2022).
- **Tables written:** `entities` (Level 1, key `lei`), `relationships` (Level 2, key `start_lei`, `end_lei`, `relationship_type`) and `mic_lei` (key `mic`), each with one row per version.
- **Update:** `update_gleif` daily for the two levels, `update_gleif_mic_lei` for the mapping; both carry on from where the last run stopped.
- **Needs:** network access to GLEIF (no credentials), and Spark with Delta Lake -- all three tables are Delta tables.
- **Used by:** [`fincommons.entitymasterdata`](../entitymasterdata/README.md), which takes names, countries, parents and MICs from here where this history has been built; see [Use in the entity master data](#use-in-the-entity-master-data).

## Quick start

1. Add a `gleif` section to `paths.yaml`:

```yaml
gleif:
  landing: /data/gleif/landing
  bronze: /data/gleif/bronze
  silver: /data/gleif/silver
```

2. Start a Spark session with Delta Lake, build the history and read it:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder \
    .master("local[4]") \
    .appName("fincommons") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

from fincommons import gleif

gleif.update_gleif(spark, "paths.yaml", start_date="2025-01-01")   # Level 1 and Level 2
gleif.update_gleif_mic_lei(spark, "paths.yaml")                     # the MIC-to-LEI mapping

gleif.entities_as_of(spark, "paths.yaml", "2026-06-30") \
    .select("lei", "legal_name", "legal_address_country").show(5)
```

The folders of the landing zone and the Bronze layer are created by the updates themselves; `create_directory_tree` creates them on its own. The session above is the minimum; for a scheduled run, size the cores, the memory and the scratch directory as [Setting up a Spark session](../../../docs/spark-session.md) describes. The session of the [entity master data README](../entitymasterdata/README.md#quick-start) works as well.

**How long the first run takes.** A full copy is some 3.5 million LEIs, 490 MB zipped, and takes some five minutes to apply; a daily delta takes about one. The first run reads one full copy per month (`months`) from `start_date` until the delta files begin, about two months back, and then one delta per day -- so the further back `start_date` lies, the longer it takes. The MIC-to-LEI mapping is some eighty small files.

## What you get

Three Delta tables in the Silver layer, each a history: one row per version of a record.

| Table | GLEIF's file | What it says | Key | Read with |
| --- | --- | --- | --- | --- |
| `entities` | Level 1, `lei2` | who an entity is: its legal name, addresses, legal form, jurisdiction, and the status of the entity and of its registration | `lei` | `connect_to_gleif(spark, config, "level_1")` |
| `relationships` | Level 2, `rr` | who an entity is consolidated by or managed by, and since when | `start_lei`, `end_lei`, `relationship_type` | `connect_to_gleif(spark, config, "level_2")` |
| `mic_lei` | the MIC-to-LEI mapping | which entity operates which market | `mic` | `connect_to_mic_lei(spark, config)` |

A contract states the key of its table together with `valid_from`.

### Version columns (every table)

| Column | Meaning |
| --- | --- |
| `valid_from` | first day the version held |
| `valid_to` | last day the version held; empty while it holds |
| `latest_record` | whether this is the last state known of the record -- kept by an ended record too |
| `record_hash` | digest of the columns that open a version (`entities` and `relationships` only); see [What a version is](#what-a-version-is) |
| `publication` | the GLEIF publication the version was read from: `YYYYMMDD-HHMM` for the two levels, `YYYYMMDD` (the file's day) for `mic_lei` |

Every column GLEIF writes is kept as text, dates and timestamps included, as the ISO 8601 text of the file.

### `entities` (Level 1)

One row per version of an LEI. It keeps 39 of the file's some 340 columns:

| Group | Columns |
| --- | --- |
| name | `legal_name` and its language `legal_name_language`; the first other name `other_name` and its `other_name_type`; the first transliterated name `transliterated_name` |
| legal address | `legal_address_line` (first line), `legal_address_city`, `legal_address_region`, `legal_address_country`, `legal_address_postal_code` |
| headquarters address | `hq_address_line` (first line), `hq_address_city`, `hq_address_region`, `hq_address_country`, `hq_address_postal_code` |
| registration authority | `registration_authority_id` and the entity's number in it, `registration_authority_entity_id` |
| jurisdiction and form | `legal_jurisdiction`; `entity_category`, `entity_sub_category`; the legal form as an ISO 20275 code `legal_form_code` or in words `legal_form_other` |
| associated entity of a fund or branch | `associated_lei`, `associated_entity_type` |
| entity | `entity_status`, `entity_creation_date`, `entity_expiration_date`, `entity_expiration_reason`, the first successor `successor_lei` |
| registration | `initial_registration_date`, `last_update_date`, `registration_status`, `next_renewal_date`, the managing local operating unit `managing_lou`, `validation_sources`, `validation_authority_id`, `validation_authority_entity_id` |
| data quality | `conformity_flag` |

Every further other name and address line is left out, so that a change of a translated name alone opens no version.

### `relationships` (Level 2)

One row per version of a reported relationship: the two LEIs (`start_lei` is consolidated or managed by `end_lei`) and their ID types (`start_node_id_type`, `end_node_id_type`), the type (`relationship_type`, e.g. `IS_ULTIMATELY_CONSOLIDATED_BY`, `IS_FUND-MANAGED_BY`) and the status (`relationship_status`) of the relationship, its own period (`relationship_start`, `relationship_end`) and the accounting period it rests on (`accounting_period_start`, `accounting_period_end`) -- each found among the up to five numbered periods by its type, since they come in no fixed order -- the first quantifier, usually the share held (`measurement_method`, `quantifier_amount`, `quantifier_units`), and the registration of the relationship record (`initial_registration_date`, `last_update_date`, `registration_status`, `next_renewal_date`, `managing_lou`, `validation_sources`, `validation_documents`).

`deleted_at` is set on a relationship a delta file reports as deleted; it is not part of the record's hash.

### `mic_lei`

One row per version of a market identifier code (MIC, ISO 10383 -- the code of a trading venue or a segment of one): `mic`, `lei`, `valid_from`, `publication` (the file the version was first read from, `YYYYMMDD`), `valid_to` and `latest_record`. An operator of several markets or segments has one row per MIC. MIC and LEI are written in upper case without blanks.

### How the tables join

- `lei`, `start_lei`, `end_lei` and `mic_lei.lei` join to `lei` of the [entity master data](../entitymasterdata/README.md#the-entity-master-data-set-main) and to each other.
- `mic_lei.mic` joins to `trading_venue` of [FIRDS](../firds/README.md), which is the MIC of the venue an instrument is traded on.

## Common tasks

**What held on a day.** One row per LEI, relationship or MIC, as GLEIF described it on that day; a record that had ended by then has none.

```python
gleif.entities_as_of(spark, "paths.yaml", "2026-06-30")
gleif.relationships_as_of(spark, "paths.yaml", "2026-06-30")
gleif.mic_lei_as_of(spark, "paths.yaml", "2026-06-30")
```

**The latest state.** The latest version of every LEI or relationship, including those that have ended; for the mapping, what the latest file says.

```python
gleif.latest_entities(spark, "paths.yaml")
gleif.latest_relationships(spark, "paths.yaml")
gleif.current_mic_lei(spark, "paths.yaml")
```

**Every version of a record.** The whole history, e.g. to see how an entity's name changed.

```python
from pyspark.sql import functions as F

gleif.connect_to_gleif(spark, "paths.yaml", "level_1") \
    .where(F.col("lei") == "529900T8BM49AURSDO55") \
    .orderBy("valid_from") \
    .select("valid_from", "valid_to", "legal_name").show()
```

**The ultimate parent of every entity on a day.**

```python
from pyspark.sql import functions as F

gleif.relationships_as_of(spark, "paths.yaml", "2026-06-30") \
    .where(F.col("relationship_type") == "IS_ULTIMATELY_CONSOLIDATED_BY") \
    .select(F.col("start_lei").alias("lei"), F.col("end_lei").alias("ultimate_parent_lei"))
```

**Who operates a market.** The mapping joined onto the entities of the same day.

```python
day = "2026-06-30"
gleif.mic_lei_as_of(spark, "paths.yaml", day) \
    .join(gleif.entities_as_of(spark, "paths.yaml", day).select("lei", "legal_name"), on="lei", how="left") \
    .select("mic", "lei", "legal_name").show()
```

**What a column means, and the schema of a table.** The data contracts are the one place the columns are described:

```python
import fincommons
from fincommons.contracts import load_contract, spark_schema

fincommons.describe_column("relationship_start")
spark_schema(load_contract("fincommons.gleif.silver.relationships"))
```

**Bring in a MIC-to-LEI file by hand.** A file placed into `{landing}/mic_lei` under the name GLEIF gave it (`LEI-MIC-YYYYMMDD.zip`):

```python
import os

path = os.path.join(gleif.path_landing_mic_lei("paths.yaml"), "LEI-MIC-20261116.zip")
gleif.import_mic_lei_file("paths.yaml", path)
gleif.write_mic_lei_history(spark, "paths.yaml")
```

## Running it on a schedule

Run both updates once a day, the GLEIF history before [`entitymasterdata`](../entitymasterdata/README.md#running-it-on-a-schedule) so that the entity master data set reads the newest state:

```python
from fincommons import gleif

gleif.update_gleif(spark, "paths.yaml", start_date="2025-01-01")
gleif.update_gleif_mic_lei(spark, "paths.yaml")
```

- **The same call serves the first run and every daily one.** `start_date` decides where a history begins and nothing after that: every later run carries the history on from the publication it stands at.
- **Idempotent.** A file already downloaded or imported is not fetched or read again, and a publication already applied is not applied again. `update_gleif` returns the labels of the publications it applied, `update_gleif_mic_lei` the days of the files it brought in.
- **Too early in the day.** A run that finds today's publication of its hour not yet out stops and waits for the next run.
- **A run that died** is resumed by the next one, which applies the next publication after the last one each level holds; see [How the history is stored](#how-the-history-is-stored).
- **When to run.** The history is carried at the hour of day it was begun at (00:00, 08:00 or 16:00 UTC), so a run after that day's publication of the hour is out applies it; see [Which publications are read](#which-publications-are-read).

## How it works

### Which publications are read

**The history begins with a full copy, then moves on by monthly full copies while no deltas exist, and by daily deltas with a weekly full copy once they do.**

GLEIF publishes the golden copy three times a day, at 00:00, 08:00 and 16:00 UTC, and keeps every publication downloadable as a full copy -- some 3.5 million LEIs, 490 MB zipped -- back to February 2018. Beside each recent one it publishes delta files, the records changed in the day before it, some 26,000 a day; those are kept for about two months only. The history is therefore carried forward in two ways:

1. It is begun from the full copy of the first publication on or after `start_date`. A history can be begun on any day since February 2018.
2. While no delta files exist, it is carried forward one full copy per step, a month by default (`months`).
3. Once they do, it is carried forward by the delta of each day, at the hour of day the history was begun at, so that each delta covers exactly the 24 hours since the one before. A day whose publication is missing, or offers no delta, is bridged by that day's full copy.
4. Once a week, on Sundays by default (`full_weekday`), the full copy is read instead of the delta.

Why the weekly full copy: the deltas list the records whose update time changed in the day, and that is not every change. A relationship GLEIF drops can leave them without a deletion -- 570 of half a million had, after a month of deltas -- and a full copy closes what it no longer lists. The weekly copy bounds how long such a record stays open to a week, at the cost of some five minutes against one for a delta.

How well it works: a history begun on 1 September 2026 and carried this way to the publication of 27 November agreed with that publication's full copy on every one of its 3.5 million LEIs, and held open 76 of half a million relationships that the full copy no longer listed, all of them dropped since the Sunday before.

Each publication's two files go through the landing zone and the Bronze layer before they are applied, and a file already downloaded or imported is not fetched or read again.

### What a version is

**A record has a new version when anything the history keeps about it changes, except two columns.** `record_hash` is a digest of those columns, and two are not among them:

- The day of the last update (`last_update_date`): GLEIF's local operating units touch a record without changing anything about the entity, and that opens no version.
- The conformity flag (`conformity_flag`), GLEIF's own verdict on a record's data quality, which GLEIF recomputes without the record being updated, so that the daily deltas do not carry its changes; a history fed by them was out on 73,335 of 3.5 million flags after a month.

Both are kept as they were read with the version.

**A version begins on the day of the record's last update.** GLEIF records when a record was last changed, and that day does not depend on how often the history was fed: a record that changed on the 3rd of a month begins its version on the 3rd whether the change was read the next morning from a delta file or at the end of the month from a full copy. What a monthly step loses is the intermediate states of a record that changed more than once within the month, not the days of the changes it keeps. Two exceptions:

- A change GLEIF made without the record being updated -- its update time is no later than the one the version in force was read at -- begins on the day of the publication it was first read in.
- A change on the day the version in force began replaces that version, rather than opening a second one on the same day.

**A version ends on the day before the one that follows it.**

- A relationship a delta file reports as deleted is closed the day before its deletion.
- A record a full copy no longer lists -- which GLEIF does not do with LEIs, but does with relationships -- is closed the day before that publication, which for a deletion no delta reported is the day before the weekly full copy rather than the day it happened.
- An ended record keeps `latest_record`, since it is the last state known, and has no version on a day after it ended.

**A history before its start holds each record as it was when the history was begun,** from its last update before that. What a record was before that update is not known, and an entity created later has no version before its creation.

### How the history is stored

**Each level is one Delta table, and a publication is applied to it in one atomic, tagged merge.**

- The merge's commit names the publication in its `userMetadata`, e.g. `gleif-silver-level_1:20261126-0000:delta`.
- The commit is atomic, so a level either holds a publication or does not, and the last tagged commit says how far the history has been carried. A run that died is resumed by applying the next publication after it.
- The two levels are carried forward together, one publication at a time, so that on any day the entities and the relationships describe the same publication. A run that finds them at different publications stops and says which is behind (see [Troubleshooting](#troubleshooting)).

The rules a history is built by are in `history`, as transformations of data frames independent of the storage, as the FIRDS rules are; the tests set the merge against them.

### The MIC-to-LEI mapping

**The mapping has a pipeline of its own, rebuilt in full from the Bronze layer on every run, since nothing about it follows the publications of the golden copy.**

A market identifier code (MIC, ISO 10383) names a trading venue or a segment of one, and GLEIF links every MIC to the LEI of the entity operating the market. It publishes the mapping about weekly as a zipped csv file of two columns, `LEI` and `MIC`, named after its day -- `LEI-MIC-20261116.zip`, some thousand rows -- and its [mapping API](https://mapping.gleif.org/api/v2/mic-lei) lists every file it still holds, back to August 2022.

What `update_gleif_mic_lei` does:

1. Every file GLEIF holds that the Bronze layer lacks is downloaded into `{landing}/mic_lei` under the name GLEIF gave it.
2. It is imported into `{bronze}/mic_lei` as `YYYYMMDD.parquet`.
3. The history is rebuilt from every file the Bronze layer holds and written over the Delta table `mic_lei` in one commit -- all the files together are some eighty thousand rows.

The first run therefore begins the history with the oldest file GLEIF still holds, and a later run keeps what earlier ones brought in whether or not GLEIF still lists it. A file placed into the landing zone by hand under GLEIF's name is brought in by `import_mic_lei_file` and `write_mic_lei_history` (see [Common tasks](#common-tasks)).

**A version of a MIC** holds from the day of the first file that links it to its LEI to the day before the first file that no longer does, because it names another LEI or does not name the MIC at all.

- The files carry no dates of their own, so a change is dated by the file it was first read in, up to a week after GLEIF made it.
- A MIC named again after a gap opens a new version, since nothing is known of the days between.
- MIC and LEI are written in upper case without blanks.
- A MIC one file links to two LEIs -- which no file up to November 2026 did -- is given the first of them in order and named in a warning.

### Use in the entity master data

**Where the configuration file has a `gleif` section and the history has been built, [`fincommons.entitymasterdata`](../entitymasterdata/README.md) takes its names, countries and parents from here, rather than from golden copies of its own.** It reads them as they stand for its public table, and as they stood on a day for its `entities_as_of` and the sector counts.

It takes the MICs an entity operates from the `mic_lei` table where that has been built, and downloads nothing for them itself. Without the `mic_lei` table its column `mic` is left empty, with a warning.

## Reference

### Configuration

| Key in the `gleif` section | Holds | Catalogue name allowed? |
| --- | --- | --- |
| `landing` | the downloaded files: `level_1/`, `level_2/`, `mic_lei/` | no, a directory |
| `bronze` | the imported files, as parquet: `level_1/`, `level_2/`, `mic_lei/` | no, a directory |
| `silver` | the Delta tables `entities`, `relationships` and `mic_lei` | yes |

The Silver layer can be given a catalogue prefix instead of a path, as for the other modules; see [`fincommons.addressing`](../addressing.py). The three tables are Delta tables either way, because the history is carried forward by merges.

### Functions

All are available as `gleif.<name>` after `from fincommons import gleif`.

**Update**

| Function | What it does |
| --- | --- |
| `update_gleif(spark, config_path, start_date, months=1, until=None, session=None, full_weekday=6)` | Carry the history of both levels forward to GLEIF's latest publication. `until` stops at the publications of a day; `full_weekday=None` reads no weekly full copy; `session` is a `requests.Session`. Returns the labels applied. |
| `update_gleif_mic_lei(spark, config_path, session=None)` | Download and import every MIC-to-LEI file the Bronze layer lacks and rebuild the history. Returns the days brought in. |
| `create_directory_tree(config_path)` | Create the folders of the landing zone and the Bronze layer. |

**Read**

| Function | What it does |
| --- | --- |
| `connect_to_gleif(spark, config_path, level="level_1")` | Every version of one level, `"level_1"` or `"level_2"`. |
| `entities_as_of(spark, config_path, day)` | The entities as GLEIF described them on `day`, one row per LEI. |
| `relationships_as_of(spark, config_path, day)` | The relationships GLEIF reported as standing on `day`. |
| `latest_entities(spark, config_path)` | The latest version of every LEI, including those that have ended. |
| `latest_relationships(spark, config_path)` | The latest version of every relationship, including those that have ended. |
| `connect_to_mic_lei(spark, config_path)` | Every version of every MIC. |
| `mic_lei_as_of(spark, config_path, day)` | Which LEI operated which MIC on `day`, one row per MIC. |
| `current_mic_lei(spark, config_path)` | The MICs the latest file links to an LEI. |
| `mic_lei_held(spark, config_path)` | Whether the MIC-to-LEI history is there to read. |

**Steps and helpers** (for repairs and tests)

| Function | What it does |
| --- | --- |
| `list_publications(until, session=None)` | GLEIF's publications back to the first on or after `until`, oldest first, as `Publication` objects. |
| `plan_publications(publications, start_date, applied=None, months=1, full_weekday=6)` | The publications to read, in order, and whether each as a full copy. |
| `download_publication(config_path, publication, kind, session=None)` | Download both levels' files of one publication into the landing zone. |
| `landing_name(publication, level, kind)`, `parse_landing_name(name)` | The name a landing file is stored under, and the label, level and kind read back off it. |
| `import_file(spark, config_path, path)` | Import one landing file into the Bronze layer; returns the Bronze path. |
| `apply_publication(spark, config_path, level, label, kind, path)` | Apply one Bronze table to the history of a level, in one tagged commit. |
| `applied_publication(spark, config_path, level)` | The label of the last publication applied to a level, or `None`. |
| `list_mic_lei_files(session=None)`, `download_mic_lei_file(config_path, file, session=None)` | The MIC-to-LEI files GLEIF holds, and the download of one. |
| `import_mic_lei_file(config_path, path)`, `write_mic_lei_history(spark, config_path)` | Import one landing file into the Bronze layer; rebuild and write the history. |
| `path_landing`, `path_bronze`, `ref_silver` (by level); `path_landing_mic_lei`, `path_bronze_mic_lei`, `ref_silver_mic_lei` | Where the files and tables live. |
| `gleif.history` | The rules a history is built by, independent of the storage (`records_as_of`, `latest_records`, `plan_changes`, ...). |

### Data contracts

What the columns of each table hold is stated by its data contract in the Open Data Contract Standard: `data/contract_gleif_silver_entities.yaml`, `data/contract_gleif_silver_relationships.yaml` and `data/contract_gleif_silver_mic_lei.yaml`, which [`data/product_gleif.yaml`](../data/product_gleif.yaml) names as the module's output ports. They are the one place the columns are described, and the column catalogue reads them from there (`fincommons.describe_column`). A contract states the key of its table -- the LEI, the two LEIs and the type of the relationship, or the MIC, and `valid_from` -- and which columns a version always has.

The handbook has a chapter on the module, [`docs/handbook/01-gleif.tex`](../../../docs/handbook/01-gleif.tex).

## Troubleshooting

- **`update_gleif` stops: the levels stand at different publications.** A run died between applying a publication to Level 1 and to Level 2. The error names the level behind. Apply the missing publication to it from its landing file, then update as usual:

  ```python
  import os

  name = "20261126-0000-gleif-goldencopy-rr-last-day.csv.zip"   # the file of the level behind
  path = os.path.join(gleif.path_landing("paths.yaml", "level_2"), name)
  label, level, kind = gleif.parse_landing_name(name)
  gleif.apply_publication(spark, "paths.yaml", level, label, kind,
                          gleif.import_file(spark, "paths.yaml", path))
  ```

- **"the history of ... has to begin with a full copy".** A level's history can only be begun from a full copy, never a delta.
- **A relationship GLEIF dropped is still open.** The deltas did not report the deletion; the next weekly full copy closes it, the day before that publication. Setting `full_weekday=None` turns the weekly copy off and leaves such records open.
- **`conformity_flag` differs from GLEIF's current file.** The flag is not part of the hash and its changes are not carried by the deltas, so it stays as read with the version.
- **The entity master data set has an empty `mic`.** The `mic_lei` history has not been built, or `paths.yaml` has no `gleif` section; run `update_gleif_mic_lei`.
- **`write_mic_lei_history` raises `FileNotFoundError`.** The Bronze layer holds no MIC-to-LEI file yet; run `update_gleif_mic_lei` or import a file first.
