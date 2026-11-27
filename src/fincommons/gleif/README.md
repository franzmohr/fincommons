# GLEIF

Module `gleif` keeps a history of the reference data of the [Global Legal Entity Identifier Foundation](https://www.gleif.org) in the Silver layer: who every legal entity identifier (LEI) belongs to, and who owns or manages whom, on any day since the history was begun. It is the GLEIF counterpart of the FIRDS history of [`fincommons.firds`](../firds/README.md), and it is read the same way: every state a record held is one version, with the first and the last day it held.

| Level | GLEIF's file | What it says | Silver table | Key |
| --- | --- | --- | --- | --- |
| 1 | `lei2` | who an entity is: its legal name, addresses, legal form, jurisdiction, and the status of the entity and of its registration | `entities` | `lei` |
| 2 | `rr` | who an entity is consolidated by or managed by, and since when | `relationships` | `start_lei`, `end_lei`, `relationship_type` |

[`fincommons.entitymasterdata`](../entitymasterdata/README.md) builds on it: where the configuration file has a `gleif` section and the history has been built, the entity master data set takes its names, countries and parents from here, as they stand or as they stood on a day, rather than from golden copies of its own.

## 1 Setting up

```yaml
gleif:
  landing: /data/gleif/landing
  bronze: /data/gleif/bronze
  silver: /data/gleif/silver
```

The Silver layer can be given a catalogue prefix instead of a path, as for the other modules; the two tables are Delta tables either way, because the history is carried forward by merges.

## 2 Running it

```python
from fincommons import gleif

gleif.update_gleif(spark, "paths.yaml", start_date="2025-01-01")
```

**`start_date` is the user's to set.** It is the first day the history covers, and it decides where a history begins and nothing after that: every later run carries the history on from the publication it stands at, so the same call serves the first run and every daily one. A history can be begun on any day since February 2018.

### Which publications are read

GLEIF publishes the golden copy three times a day, at 00:00, 08:00 and 16:00 UTC, and keeps every publication downloadable as a full copy -- some 3.5 million LEIs, 490 MB zipped -- back to February 2018. Beside each recent one it publishes delta files, the records changed in the day before it, some 26,000 a day; those are kept for about two months only. The history is therefore carried forward in two ways:

1. It is begun from the full copy of the first publication on or after `start_date`.
2. While no delta files exist, it is carried forward one full copy per step, a month by default (`months`).
3. Once they do, it is carried forward by the delta of each day, at the hour of day the history was begun at, so that each delta covers exactly the 24 hours since the one before. A day whose publication is missing, or offers no delta, is bridged by that day's full copy.
4. Once a week, on Sundays by default (`full_weekday`), the full copy is read instead of the delta. The deltas list the records whose update time changed in the day, and that is not every change: a relationship GLEIF drops can leave them without a deletion -- 570 of half a million had, after a month of deltas -- and a full copy closes what it no longer lists. The weekly copy bounds how long such a record stays open to a week, at the cost of some five minutes against one for a delta. A history begun on 1 September 2026 and carried this way to the publication of 27 November agreed with that publication's full copy on every one of its 3.5 million LEIs, and held open 76 of half a million relationships that the full copy no longer listed, all of them dropped since the Sunday before.

A run that finds today's publication of its hour not yet out stops and waits for the next run. Each publication's two files go through the landing zone and the Bronze layer before they are applied, and a file already downloaded or imported is not fetched or read again.

## 3 What a version is

A record has a new version when anything the history keeps about it changes. `record_hash` is a digest of those columns, and two are not among them. The day of the last update (`last_update_date`): GLEIF's local operating units touch a record without changing anything about the entity, and that opens no version. And the conformity flag (`conformity_flag`), GLEIF's own verdict on a record's data quality, which GLEIF recomputes without the record being updated, so that the daily deltas do not carry its changes; a history fed by them was out on 73,335 of 3.5 million flags after a month. Both are kept as they were read with the version.

**A version begins on the day of the record's last update.** GLEIF records when a record was last changed, and that day does not depend on how often the history was fed: a record that changed on the 3rd of a month begins its version on the 3rd whether the change was read the next morning from a delta file or at the end of the month from a full copy. What a monthly step loses is the intermediate states of a record that changed more than once within the month, not the days of the changes it keeps. Two exceptions:

- A change GLEIF made without the record being updated -- its update time is no later than the one the version in force was read at -- begins on the day of the publication it was first read in.
- A change on the day the version in force began replaces that version, rather than opening a second one on the same day.

**A version ends** on the day before the one that follows it. A relationship a delta file reports as deleted is closed the day before its deletion; a record a full copy no longer lists -- which GLEIF does not do with LEIs, but does with relationships -- is closed the day before that publication, which for a deletion no delta reported is the day before the weekly full copy rather than the day it happened. An ended record keeps `latest_record`, since it is the last state known, and has no version on a day after it ended.

**A history before its start** holds each record as it was when the history was begun, from its last update before that. What a record was before that update is not known, and an entity created later has no version before its creation.

## 4 Reading it

```python
gleif.connect_to_gleif(spark, "paths.yaml", "level_1")     # every version
gleif.latest_entities(spark, "paths.yaml")                 # the latest version of every LEI
gleif.entities_as_of(spark, "paths.yaml", "2026-06-30")    # what held on a day
gleif.relationships_as_of(spark, "paths.yaml", "2026-06-30")
```

The version columns are `valid_from`, `valid_to` (empty while the version holds), `latest_record`, `record_hash`, and `publication`, the GLEIF publication the version was read from (`YYYYMMDD-HHMM`). Every column GLEIF writes is kept as text, dates and timestamps included, as the ISO 8601 text of the file.

**Level 1** keeps 39 of the file's some 340 columns: the legal name and its language; the first other name and transliterated name; the first line, city, region, country and postal code of the legal and the headquarters address; the registration authority and the entity's number in it; the legal jurisdiction, category and sub-category; the legal form as an ISO 20275 code or in words; the associated entity of a fund or branch; the entity's status, creation and expiration; the first successor; and the registration -- its status, initial and last update, next renewal, managing local operating unit, validation sources and authority -- and the conformity flag. Every further other name and address line is left out, so that a change of a translated name alone opens no version.

**Level 2** keeps the two LEIs, the type and the status of the relationship, its own period and the accounting period it rests on -- each found among the up to five numbered periods by its type, since they come in no fixed order -- the first quantifier (usually the share held), and the registration of the relationship record. `deleted_at` is set on a relationship a delta file reports as deleted; it is not part of the record's hash.

What the columns of each table hold is stated by its data contract in the Open Data Contract Standard, `data/contract_gleif_silver_entities.yaml` and `data/contract_gleif_silver_relationships.yaml`, which `data/product_gleif.yaml` names as the module's output ports. They are the one place the columns are described, and the column catalogue reads them from there: `fincommons.describe_column("relationship_start")`. A contract states the key of its table -- the LEI, or the two LEIs and the type of the relationship, and `valid_from` -- and which columns a version always has.

```python
from fincommons.contracts import load_contract, spark_schema
spark_schema(load_contract("fincommons.gleif.silver.relationships"))
```

## 5 How the history is stored

Each level is one Delta table. A publication is applied to a level in **one merge**, and the merge's commit names the publication in its `userMetadata`, e.g. `gleif-silver-level_1:20261126-0000:delta`. The commit is atomic, so a level either holds a publication or does not, and the last tagged commit says how far the history has been carried; a run that died is resumed by applying the next publication after it. The two levels are carried forward together, one publication at a time, so that on any day the entities and the relationships describe the same publication. A run that finds them at different publications stops and says which is behind.

The rules a history is built by are in `history`, as transformations of data frames independent of the storage, as the FIRDS rules are; the tests set the merge against them.
