# STS securitisations (ESMA register)

Module `sts` collects the public register of securitisations notified as
simple, transparent and standardised (STS) that the European Securities and
Markets Authority (ESMA) keeps, and turns it into six tables. Use it to see
which securitisations are notified as STS, who originates them, which tranches
(by ISIN) they issued, and what has happened to them -- including what left
the register, which ESMA itself no longer records.

At a glance:

* **Source:** ESMA's registers portal, read through its machine-to-machine
  services (no key, no registration), plus four spreadsheets ESMA publishes
  beside the register.
* **Update:** one complete, dated snapshot per run, never rewritten. Run it as
  often as you want a record -- the snapshots are the only history there is.
* **Tables:** `notification`, `assertion`, `tranche`, `party`, `event`,
  `synthetic` in the Silver layer (cleaned tables, read by analyses); the
  untouched answers of the portal in the Bronze layer (raw files as
  downloaded).
* **Key:** the register's own document `id`, not either of the identifiers
  ESMA publishes.
* **Needs:** network access to esma.europa.eu; Spark only for the Silver layer
  (collecting into Bronze is plain Python); R and Quarto for the two reports.

## Quick start

1. Add an `sts` section to `paths.yaml`:

```yaml
sts:
    bronze: /data/sts/bronze
    silver: /data/sts/silver
```

2. Start Spark, take a snapshot, build the tables and read one:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from fincommons import sts

builder = SparkSession.builder \
    .master("local[4]") \
    .appName("fincommons") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

sts.create_directory_tree("paths.yaml")
sts.update_sts(spark, "paths.yaml")                 # collect, then build
sts.collected_snapshots("paths.yaml")               # ['2026-11-20']
sts.connect_to_sts(spark, "paths.yaml", "notification").show(5)
```

This session is enough for the register; for a scheduled job, see
[Setting up a Spark session](../../../docs/spark-session.md).

The register is small: the snapshot of 20 November 2026 held 1,147
notifications and 4,613 child documents, which is a dozen or so pages of 500
documents from the portal plus the four spreadsheets, and some forty thousand
rows across the six tables.

## What you get

### The six tables

| Table | Rows on 20 November 2026 | One row per | Key |
| --- | --- | --- | --- |
| `notification` | 1,147 | notification the register serves | `id` |
| `assertion` | 33,568 | claim a public notification makes against the STS criteria | `id`, `template`, `requirement` |
| `tranche` | 2,275 | tranche of a notification, by ISIN | none |
| `party` | 2,338 | originator, original lender, sponsor or special purpose entity | none |
| `event` | 2,395 | thing known to have happened to a securitisation | none |
| `synthetic` | 228 | synthetic STS securitisation, which is in no register | securitisation and notification number |

The first four are built from one snapshot and carry the day of it in
`snapshot`, so a lake that rebuilds from an older snapshot produces tables that
say so rather than tables that look current. `event` and `synthetic` carry
`source_as_of` instead.

**`id` is the key, not either identifier.** 1,147 notifications carried 1,115
distinct securitisation identifiers, so every join on
`securitisation_identifier` is one to many. Why three tables have no key is
under [The data contracts](#the-data-contracts).

### `notification`

| Column | Meaning |
| --- | --- |
| `id` | the register's own document identifier; the key of this and every other table of the module |
| `securitisation_identifier` | `stsre_sec_id`, trimmed; not unique |
| `notification_identifier` | `stsre_notif_id`, trimmed; not unique |
| `identifier_scheme` | `lei`, `private`, `synthetic`, `missing` or `other`; only `lei` joins to anything outside the register (see `identifier_scheme`) |
| `securitisation_name` | the name; multivalued names are joined with `; ` |
| `securitisation_type`, `type_code` | public or private, as text and as code |
| `securitisation_class`, `class_code` | non-ABCP, ABCP or ABCP programme, as text and as code |
| `compliance_status` | the register's compliance status |
| `sanction_status` | `Not Sanctioned` on all 1,147 notifications of 20 November 2026 |
| `underlying_exposure` | what the securitisation is backed by, in the wording of the technical standards |
| `first_notified`, `notified` | the first notification date (`stsre_init_notif_dt`) and the latest (`stsre_notif_dt`) |
| `repository` | the securitisation repositories it reports to (multivalued, joined) |
| `reporting_entity_name`, `reporting_entity_lei` | the entity that made the notification (`de_name`, `de_lei_priv`) |
| `reporting_entity_legal_name` | the same entity as its LEI records it (`Lloyds Bank GmbH` against `LLOYDS BANK GMBH`) |
| `verifier_name`, `verifier_country`, `verifier_authority`, `verifier_statement` | the authorised third party of Article 28 that verified compliance, and what it said |
| `prospectus_country`, `prospectus_identifier`, `prospectus_issue_date` | the prospectus |
| `notified_again`, `notified_again_reason` | whether and why the securitisation was notified more than once |
| `fields` | how many fields the document carried: a median of 147 on a public notification, 18 on a private one |
| `snapshot` | the day of the snapshot |

The reporting entity is the sole originator on 394 of the 502 notifications
that carry it, one of several on 25, and on 74 not an originator at all but the
special purpose entity or a group entity reporting on another's behalf. The
originators are in `party`. The verifier's name is not normalised -- one
appears under thirteen spellings -- so a count by verifier is a count of
spellings until they are mapped. 485 of the 502 public notifications carry a
verifier statement.

### `assertion`

| Column | Meaning |
| --- | --- |
| `id`, `securitisation_identifier` | the notification |
| `template` | `non_abcp` or `abcp`, the two sets of criteria |
| `requirement` | the register's field name without the template prefix and the `_flag` / `_comment` ending, ESMA's spelling kept (`requirementsOnTrasparency`) |
| `flag` | the code as published: `C`, `N`, `Y`, `E`, `A` or `U` seen, `C` and `N` by far the most often; not interpreted, since the register documents no domain |
| `comment` | the explanatory comment beside the flag |
| `snapshot` | the day of the snapshot |

A few requirements are not assertions at all -- where the data on historical
defaults is published, the name of the entity retaining the risk, whether the
originator is a credit institution -- and for those the register puts text in
`flag`: 1,816 of the 33,771 rows of the snapshot of 27 November 2026.

### `tranche`

| Column | Meaning |
| --- | --- |
| `id`, `securitisation_identifier` | the notification it belongs to |
| `isin` | the ISIN (International Securities Identification Number) of the tranche |
| `instrument_name` | its name |
| `snapshot` | the day of the snapshot |

Only public notifications have tranches. Ten tranche documents carry no ISIN
and 267 no instrument name.

### `party`

| Column | Meaning |
| --- | --- |
| `id`, `securitisation_identifier` | the notification it belongs to |
| `role` | `originator`, `lender` (original lender), `sponsor` or `sspe` (securitisation special purpose entity) |
| `lei` | its LEI (Legal Entity Identifier); null for `sspe` |
| `name` | its name; null for `sspe` |
| `country` | its two-letter country code |
| `country_name` | the country's name |
| `snapshot` | the day of the snapshot |

**The special purpose entity has no identifier and no name in the register**,
only a country. That is a gap in the register, not in the reading of it.

### `event`

| Column | Meaning |
| --- | --- |
| `securitisation_identifier`, `identifier_scheme` | the securitisation (events are kept by securitisation, not by notification) |
| `event` | `notified`, `updated`, `non_compliant`, `cancelled` or `left_register` |
| `event_date` | when it happened; for `left_register` the day of the later snapshot, when the departure was first seen |
| `reason` | ESMA's reason for a cancellation, or the status for `non_compliant` |
| `source` | `register`, `snapshot`, or ESMA's reference of the spreadsheet (`ESMA33-128-760`, `ESMA33-128-761`) |
| `source_as_of` | the day that source speaks for |

| `event` | Meaning | From |
| --- | --- | --- |
| `notified` | the securitisation was notified | the register's first notification date, or the frozen list of 2019 to 2022 |
| `updated` | its notification moved | the register, where the notification date differs from the first |
| `non_compliant` | marked not compliant on the day of the snapshot | the register |
| `cancelled` | recorded as cancelled, with ESMA's reason | the cancellation list |
| `left_register` | in one snapshot and not in the next | a comparison of two snapshots -- the only record of a removal for anything after May 2022 |

Contradictions are kept rather than resolved: two securitisations cancelled in
March 2021 are still in the register and still marked compliant, and the ten
the register marks non-compliant appear in no cancellation list.

### `synthetic`

| Column | Meaning |
| --- | --- |
| `securitisation_identifier` | a scheme of its own, `SYSEC_Private_2026_10_13_01`, overlapping with nothing |
| `notification_number` | ESMA's notification number |
| `notified` | notification date to ESMA |
| `securitisation_type` | private or public (every row is private) |
| `protection` | funded or unfunded credit protection |
| `instrument` | credit derivative or financial guarantee |
| `underlying_exposure` | the underlying assets |
| `securitisation_name`, `originator_country` | empty on every row |
| `sanction_status` | whether administrative sanctions are present |
| `source_as_of` | the day the list was collected |

A synthetic STS securitisation is notified to ESMA by e-mail and appears in no
register. ESMA's spreadsheet is the only public record of it, so it is a table
of its own rather than rows of `notification`; no row has an ISIN.

### What joins to what

| From | On | To |
| --- | --- | --- |
| `tranche.isin` | ISIN | [`fincommons.firds`](../firds/README.md) |
| `party.lei` | LEI | [`fincommons.entitymasterdata`](../entitymasterdata/README.md) |
| `notification.securitisation_identifier` | the identifier of the disclosure templates, where `identifier_scheme` is `lei` | securitisation data reported through a repository |

On the lake this was written against, 1,364 of the 2,250 distinct tranche
ISINs are in the FIRDS (Financial Instruments Reference Data System)
reference data. The securitisation identifier, on a public notification, is
the reporting entity's LEI, a letter for the class and the period
(`2138001WO21Z3B8Y8K20N202602`); a private one carries a synthetic
`Private_2020_12_18_01`, which joins to nothing. So a join to reported
securitisation data reaches about five hundred notifications rather than all
of them.

## Common tasks

### List the public notifications of a country

A notification belongs to a country through its parties, and only public
notifications have any. Here, by the originator's country:

```python
from pyspark.sql import functions as F

notifications = sts.connect_to_sts(spark, "paths.yaml", "notification")
parties = sts.connect_to_sts(spark, "paths.yaml", "party")

austrian = parties \
    .where((F.col("role") == "originator") & (F.col("country") == "AT")) \
    .select("id").distinct()

notifications.join(austrian, "id") \
    .select("id", "securitisation_identifier", "securitisation_name",
            "underlying_exposure", "first_notified") \
    .orderBy("first_notified") \
    .show(truncate=False)
```

Use `role == "sspe"` instead to see where the vehicles are established, which
is a different country for much of the market.

### Join the tranches to FIRDS by ISIN

```python
from fincommons import firds

tranches = sts.connect_to_sts(spark, "paths.yaml", "tranche")
instruments = firds.connect_to_firds(spark, "paths.yaml", "compact")

tranches.join(instruments, "isin", "left") \
    .select("isin", "instrument_name", "full_name", "cfi", "issuer") \
    .show(truncate=False)
```

### Join the parties to the entity master data by LEI

```python
from pyspark.sql import functions as F
from fincommons import entitymasterdata as em

parties = sts.connect_to_sts(spark, "paths.yaml", "party")
entities = em.connect_to_entitymasterdata(spark, "paths.yaml") \
    .select("lei", F.col("name").alias("legal_name"), "sector", "subsector")

parties.where(F.col("lei").isNotNull()) \
    .join(entities, "lei", "left") \
    .groupBy("role", "sector").count() \
    .show()
```

### See what left the register between two snapshots

The build records departures against the snapshot before, so the latest
comparison is already in `event`:

```python
events = sts.connect_to_sts(spark, "paths.yaml", "event")
events.where("event = 'left_register'").show(truncate=False)
```

Any two snapshots can be compared without Spark, with the same functions the
build uses:

```python
held = sts.collected_snapshots("paths.yaml")
before, _ = sts.notification_rows("paths.yaml", held[0])
after, _ = sts.notification_rows("paths.yaml", held[-1])
gone = sts.departures(before, after, held[-1])   # tuples in the order of sts.event_schema
spark.createDataFrame(gone, schema=sts.event_schema).show(truncate=False)
```

### Look at a snapshot as the portal sent it

```python
sts.collected_snapshots("paths.yaml")                    # the days held, oldest first
sts.snapshot_pages("paths.yaml", "2026-11-20", "parent") # or "child"
sts.read_json(sts.path_register_record("paths.yaml", "2026-11-20"))
```

### Check a table against its data contract

```python
from fincommons import contracts

contract = contracts.load_contract("fincommons.sts.silver.notification")
results = contracts.check_quality(
    sts.connect_to_sts(spark, "paths.yaml", "notification"), contract)
contracts.quality_failures(results)                      # [] where every rule held
```

### Render the register report or a country factsheet

Both templates are R, rendered with Quarto (Typst) to a tagged PDF/UA-1
document. Run them from the templates folder; the PDF lands beside the
template.

```bash
cd src/fincommons/sts/templates

# the report on the most recent snapshot, or on the one named
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-register-report.qmd
FINCOMMONS_SNAPSHOT=2026-11-20 \
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-register-report.qmd

# a factsheet on one country, or on the Union as a whole
FINCOMMONS_COUNTRY=AT \
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-country-factsheet.qmd
FINCOMMONS_COUNTRY=EU \
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-country-factsheet.qmd
```

The report reads the Bronze layer and needs only a snapshot. The factsheet
reads the Silver layer as parquet (the default format) and refuses a Delta
table; see [Troubleshooting](#troubleshooting). What each contains is under
[The reports](#the-reports).

## Running it on a schedule

The register publishes no history, so **collect even on days nothing is
read**: a snapshot not taken is a record lost.

```python
from fincommons import sts

# spark: the session of the Quick start
sts.update_sts(
    spark, "paths.yaml",
    collect=True,       # take today's snapshot first; False rebuilds from what is held
    sheets=True,        # collect the four spreadsheets as well
    compare=True,       # record what left the register since the snapshot before
    file_format=None,   # "parquet" (default at a path) or "delta"; a catalogue name is always Delta
)
```

The two steps can run apart:

```python
sts.update_bronze_sts("paths.yaml")            # collect only -- plain Python, no Spark
sts.update_silver_sts(spark, "paths.yaml")     # build from the most recent snapshot held
sts.update_silver_sts(spark, "paths.yaml", day="2026-11-20")   # or from a given one
```

A snapshot can also be taken on its own with `collect_register`, and one
spreadsheet with `collect_sheet`:

```python
sts.collect_register("paths.yaml", page_size=500)
sts.collect_sheet("paths.yaml", "synthetic")
sts.collect_sheets("paths.yaml")               # all four, independently
```

What a rerun does:

* **Same day:** a snapshot already held for the day is left alone (outcome
  `held`); pass `force=True` to `update_bronze_sts` or `collect_register` to
  collect it again. Days are in UTC.
* **The register cannot be collected:** `update_bronze_sts` raises
  `RuntimeError`. Run it again later; nothing held is rewritten.
* **A spreadsheet cannot be collected:** reported in the outcome and logged,
  without stopping the others -- the four are independent and three of them
  frozen.
* **The Silver layer** is rebuilt in full from one snapshot every time, so a
  rerun simply replaces it.

## How it works

### Where the data comes from

**Five sources, none of which needs a key.**

| Source | Route | Holds |
| --- | --- | --- |
| the register | `https://registers.esma.europa.eu/solr/esma_registers_stsre/select` | current state only |
| ESMA33-128-760 | file on esma.europa.eu | the notifications of 2019 to 14 January 2022, frozen |
| ESMA33-128-761 | file | the cancellations to 23 May 2022, frozen, with ESMA's reasons |
| ESMA82-402-39 | file | synthetic STS, current, with columns of its own |
| `esma_register_secr.xlsx` | file | the two registered securitisation repositories, with their LEIs |

The portal's services are documented at
<https://registers.esma.europa.eu/publication/helpApp>. The registers are held
as Solr indexes and queried by URL, with `q`, `rows`, `start`, `fl`, `sort`
and `wt` parameters. No key, no registration and no declaration of interest is
needed, which is what separates this register from the securitisation data a
repository holds.

The cancellation list states ESMA33-128-761 on its own first sheet, although
ESMA files it under a name beginning `esma33-128-765`; the module uses the
reference the workbook states.

A fifth spreadsheet, the list of securitisations that lost STS status in the
Union through Brexit, is still linked from ESMA's securitisation page, but the
link answers 404 with a page of HTML. It is named in
`common.BREXIT_SHEET_URL` and is not collected. `collect_sheet` checks the
content type and the first bytes of what arrives, so a stale link of this kind
is recorded as `unavailable` rather than stored as a spreadsheet.

### Why a snapshot rather than an update

**The register is current state, so the snapshots are the history.**

The register answers for what is STS today and for nothing else. A cancelled
notification leaves it: of the 109 cancellations ESMA published up to May
2022, 107 are no longer in the register. The two spreadsheets that recorded
removals were frozen in 2022, and nothing published has recorded one since.

So the Bronze layer keeps a complete copy of the register per run, filed under
the day it was taken, and a collection never rewrites a snapshot it already
holds unless it is told to. What left the register between two runs is a diff
of two snapshots, which is the only way that question can be answered at all.

**A lake that compacts these snapshots away destroys the only record it has.**

Paging is ordered (`sort=id asc`), and a collection checks that it ended with
as many documents as the portal first said there were, so a snapshot taken
while the index moved says so rather than looking complete.

### The layout of the Bronze layer

```
bronze/
    register/2026-11-20/parent-0000.json      one answer of the portal, as it came back
                       parent-0001.json
                       child-0000.json
    sheets/2026-11-20/traditional.xlsx
                      cancelled.xlsx
                      synthetic.xlsx
                      repositories.xlsx
    collections/register-2026-11-20.json      what was collected, and what the portal said
                sheet-synthetic-2026-11-20.json
```

The record of a collection of the register carries:

* the documents and pages collected per kind,
* the totals the portal stated,
* how many fields were seen,
* the fields that are missing or unexpected.

It is what the next run reads to know a day is already held, and what a reader
consults to find out whether a snapshot is complete. On every collection the
module reports which of the fields expected of every notification are missing
and which have appeared.

### Two populations, and what they mean

**Every private notification is core-only and childless; every public one
carries the detail.** Of the 1,147 notifications the register held on 20
November 2026:

| | Public | Private |
| --- | --- | --- |
| Notifications | 502 | 645 |
| Fields per document | 119 to 155, and 19 on the four without the template | 16 to 18 |
| Child documents | 498 have them | none at all |
| Securitisation identifier | `2138001WO21Z3B8Y8K20N202602` | `Private_2020_12_18_01` |
| The STS template assertions | all of them | none |

A private notification's identifier is synthetic and joins to nothing, so a
join to reported securitisation data reaches about five hundred notifications
rather than all of them.

Class and type are not what a small sample suggests: 682 non-ABCP, 464 ABCP
(asset-backed commercial paper) and one ABCP programme; 502 public and 645
private. Only three of the ABCP notifications are public, which is why the
`abcp_*` template fields appear on three documents.

### The child documents

**The tranches and the parties are children, joined to a notification by
`_root_`, which is the parent's `id`.**

| `entity_type` | Rows | Carries |
| --- | --- | --- |
| `issins` | 2,275 | `stsre_isin_instrument`, `stsre_isin_name` |
| `originator` | 1,282 | `originator_lei`, `originator_name`, `originator_country` |
| `sspe` | 474 | `sspe_country_code`, `sspe_country_name` -- **no LEI, no name** |
| `lender` | 463 | `lender_lei`, `lender_name`, `lender_country` |
| `sponsor` | 119 | `sponsor_lei`, `sponsor_name`, `sponsor_country` |

The `lei`, `name` and `country` fields are JSON arrays while the
`*_country_name` beside each is a plain string, so a reader has to absorb the
inconsistency rather than trust it. The Silver layer takes the first value of
each array.

### What identifies a row

**Neither identifier in the register is unique; the document `id` is.**

The 1,147 notifications carried 1,115 distinct securitisation identifiers --
20 appear on more than one notification, a securitisation being notified again
when it is restructured or re-notified -- and 1,131 distinct notification
identifiers. Any join on the securitisation identifier is one-to-many.

* `stsre_notif_id` is the securitisation identifier plus a two-digit sequence
  on the public half, and identical to it on the private half.
* `sts_notification_information_notification_indentifier` -- ESMA's spelling,
  with the typo -- equals `stsre_notif_id` wherever it appears, so it is a
  duplicate.

### What to expect of the data

**The data has known flaws, which are why the tables carry quality
checks.** Found in one pass over the register and the spreadsheets on 20
November 2026:

* Three notifications carry a single space in both identifier fields, and
  eight more have a leading space. The Silver layer trims them; the three
  become empty, with `identifier_scheme` `missing`.
* 20 securitisation identifiers appear on more than one notification, while
  only 27 notifications carry a reason for having been notified twice.
* Two date formats occur in one document: `stsre_notif_dt` is
  `2026-09-10T00:00:00Z` while `prospectus_issue_date` is
  `2026-09-10 00:00:00.0`. The Silver layer reads the day from the front of
  both.
* Ten ISIN children carry no ISIN, and 267 no instrument name.
* Four public notifications have no child documents at all.
* `stsre_sanc_status` is `Not Sanctioned` on all 1,147, so the column carries
  no information until the day it does.
* The spreadsheets and the register use different vocabularies for the
  underlying exposures -- the sheet's `SME loans` against the register's
  `credit facilities, including loans and leases, provided to any type of
  enterprise` -- which needs a mapping table rather than a rule.
* The traditional spreadsheet's `ISIN code` column uses three separators and
  three placeholders for absence, and holds two malformed ISINs.

### The Silver layer

**Six tables, built from one snapshot on the driver and rebuilt in full.** The
register is small, so there is nothing to distribute and nothing to resume
from: a snapshot is a complete statement of the register on one day rather
than an increment on the one before.

* **The assertions are held long rather than wide.** The STS template is some
  130 fields on a public notification, each a flag with a comment beside it.
  As columns that is a table 130 wide and half empty, and the template is
  being revised -- the review of the securitisation framework proposes cutting
  the fields of the public templates by at least a third. As rows of
  (notification, requirement, flag, comment) it is a vocabulary that grows.
  `requirement` keeps the register's own field name, so nothing is renamed --
  ESMA's `requirementsOnTrasparency` is carried with its typographical error,
  because a name invented here would not be findable in what ESMA publishes.
* **The parties are one table with a column for the role**, the four kinds
  differing only in the prefix the register gives their fields. The special
  purpose entity carries nulls in `lei` and `name` because the register gives
  it neither.
* **The events are folded from four sources, and contradictions are kept
  rather than resolved.** The register gives `notified`, `updated` and
  `non_compliant`; the frozen list of 2019 to 2022 gives `notified`; the
  cancellation list gives `cancelled` with ESMA's reason; and a comparison of
  two snapshots gives `left_register`, which is the only record of a removal
  there is for anything after May 2022. Each row names its source and the day
  that source speaks for. The status of a securitisation is a fold over this
  table, because no source states it.
* **A field the register adds is warned about, not dropped silently.** It
  stays in the snapshot and in no table until it is mapped.

### The data contracts

**Each table has a data contract, and on the collection of 27 November 2026
every rule held.**

What the columns of each table hold, and what a reader may rely on, is stated
by its data contract in the Open Data Contract Standard, one per table --
[`data/contract_sts_silver_notification.yaml`](../data/contract_sts_silver_notification.yaml)
and so on -- which [`data/product_sts.yaml`](../data/product_sts.yaml) names as
the output ports of the module. They are the one place the columns are
described -- `fincommons.describe_column("identifier_scheme", dataset="sts")`
reads its answer from there -- and the declared schemas of the module are held
to them by the tests.

A contract states the key of its table where it has one (the document; the
document, template and requirement; a synthetic securitisation and its
notification number) and the rules the table is held to:

* one row per key,
* one snapshot per build,
* the identifier schemes, types, classes, templates, roles and events among
  those the module knows,
* the LEIs, ISINs and country codes in their forms.

`fincommons.contracts.check_quality` runs them. On the register as collected on
27 November 2026 -- 1,151 notifications, 33,771 assertions, 2,292 tranches,
2,346 parties, 2,399 events, 229 synthetic securitisations -- every one held.

Three tables have no key, and their contracts say why:

* `tranche`: a notification can carry a tranche document with neither ISIN
  nor name more than once.
* `party`: a party is a child document with nothing that identifies it within
  its notification.
* `event`: the events are kept by securitisation rather than by notification,
  so two notifications that share a securitisation identifier give the same
  event twice -- 52 of the 2,399 events of that collection repeat another.

### The reports

Both templates use the corporate design in
`r-corporate-design-functions-ggplot2.R`, one file for the whole package in
[`src/fincommons/templates`](../templates), two levels up from the
templates, which every template of every module sources so that a change to
the design is made once. A copy of it beside a template wins over the shared
one, which is what lets a template taken out of the package carry a design of
its own.

Both end with a disclaimer. A securitisation counted in them is one that was
*notified* as simple, transparent and standardised, which is a notification
made by its own parties and not a judgement by ESMA or by anyone else, and the
documents say so where they would otherwise be read as a quality mark.
Whoever renders them for a purpose of their own is responsible for what they
publish.

#### A report on one snapshot

**`templates/sts-register-report.qmd` sets out what one snapshot of the
register holds, what it does not say, and what to expect of the data.** It
reads the snapshots of the Bronze layer directly, not the Silver layer, so it
needs no Spark build.

The snapshot is the most recent one the lake holds, or the one named in
`FINCOMMONS_SNAPSHOT`, which wins where it is set so that a run reporting one
snapshot after another needs no edit of the template.

| Section | Holds |
| --- | --- |
| The register at a glance | the notifications, the two populations, and the class and type of each |
| What the securitisations are backed by | the underlying exposures |
| When they were notified | first notification by year, and how many notifications have since moved |
| The identifiers | the schemes they follow, and why neither of them identifies a row |
| The tranches and the parties | the child documents per kind, what each carries, and the originators named in most notifications |
| What the notifications say of themselves | the census of the fields, and the third parties that verified compliance |
| What changed since the snapshot before | the arrivals and the departures against the snapshot before, which is the only record there is of a departure |
| What to expect of the data | the checks, counted over the snapshot |
| The spreadsheets beside the register | what was collected beside the register, and how much of the frozen list the register has since dropped |
| Sources | and the disclaimer |

The report compares the snapshot with the one before it. A lake holding one
snapshot gets a section that says so and why it matters, rather than an empty
one: the register publishes no history, so the first comparison is only
possible once a second snapshot exists.

#### A factsheet on one country

**`templates/sts-country-factsheet.qmd` reports the securitisations of one
country, or of the Union as a whole.** The only setting you have to make is
the two-letter country code, or `EU`, in the first chunk or in
`FINCOMMONS_COUNTRY`, which wins where it is set. A code the register does not
name is refused with the list of the ones it does, `EU` among them, rather than
rendered empty.

It gives what is securitised in the country against the rest of the register,
when the securitisations were notified, who originates them and how
concentrated that is, where their vehicles are established, who verified them,
and what has happened to them since.

Three things it is careful about, because the data invites three mistakes:

* **It covers the public half only, and says so first.** A private
  notification carries no party at all, so there is nothing in it to attach to
  a country. A count here is a count of a visible part of a market.
* **A country with few securitisations gets counts, not percentages.** Below
  ten notifications the factsheet says how many a share rests on; Luxembourg
  has two originations, and one of them is half of everything.
* **A country that hosts more vehicles than it originates is told so.** 85
  securitisations originated elsewhere use a special purpose entity in
  Luxembourg against the two originated there, which is the fact a reader who
  typed `LU` is probably after.

`EU` adds two sections the single-country factsheet has no use for: the
member states side by side, and the securitisations that cross a border
between the country they were originated in and the country of their vehicle.
On the register of 20 November 2026 the Union holds 492 of the 502 public
notifications, across 12 of the 27 member states, and 122 of 471 pairings
cross a border -- 68 of them German originations in Luxembourg vehicles.

The member states are stated in the template rather than taken from the
register, because which countries belong in a figure about the Union is a
question about the Union: the register also names Norway, whose one
securitisation the factsheet leaves out and says it leaves out.

## Reference

### Configuration

| Key of `sts` | Holds | Catalogue name allowed? |
| --- | --- | --- |
| `bronze` | the snapshots, the spreadsheets and the collection records | no -- a directory |
| `silver` | the six tables, one folder each | yes (the tables are then Delta) |

### Template settings

| Template | Setting | Default |
| --- | --- | --- |
| both | `FINCOMMONS_CONFIG` (environment) | `paths.yaml` beside the template |
| report | `FINCOMMONS_SNAPSHOT` (environment) or `report_snapshot` | the most recent snapshot |
| report | `compare_snapshot` | the snapshot before |
| report | `top_n` | 15 |
| factsheet | `FINCOMMONS_COUNTRY` (environment) or `report_country` | `AT` |
| factsheet | `country_role` -- which party's country | `originator` |
| factsheet | `top_n` | 12 |
| factsheet | `small_n` -- below this many, counts instead of shares | 10 |
| factsheet | `union_members` -- add the three EEA states for the European Economic Area | the 27 member states |

The R templates need the packages `ggplot2`, `dplyr`, `tidyr` and `yaml`, plus
`jsonlite` for the report and `arrow` for the factsheet.

### Public functions

Everything below is importable as `sts.<name>`.

| Function | What it does |
| --- | --- |
| **Update** | |
| `update_sts(spark, config_path, collect=True, sheets=True, file_format=None, compare=True)` | take a snapshot and rebuild the tables from it |
| `update_bronze_sts(config_path, register=True, sheets=True, day=None, page_size=500, force=False)` | collect the register and the spreadsheets into Bronze |
| `collect_register(config_path, day=None, page_size=500, force=False)` | collect one snapshot of the register; returns what happened |
| `collect_sheet(config_path, name, day=None, force=False)` | collect one spreadsheet (`traditional`, `cancelled`, `synthetic`, `repositories`); returns `collected`, `held` or `unavailable` |
| `collect_sheets(config_path, names=None, day=None, force=False)` | collect several, each independently; returns the outcome per sheet |
| `update_silver_sts(spark, config_path, day=None, file_format=None, compare=True)` | rebuild the Silver layer from one snapshot |
| **Read** | |
| `connect_to_sts(spark, config_path, table="notification")` | open a Silver table |
| `collected_snapshots(config_path)` | the days a snapshot is held for, oldest first |
| `latest_snapshot(config_path)`, `previous_snapshot(config_path, day)` | the most recent snapshot, and the one before a given one |
| `snapshot_pages(config_path, day, kind="parent")` | the page files of one snapshot |
| `notification_rows(config_path, day)`, `child_rows(config_path, day, identifiers=None)` | a snapshot read into rows, in plain Python |
| `departures(before, after, day)` | the `left_register` events between two lists of notification rows |
| `unmapped_fields(config_path, day)` | fields a snapshot carries that no table reads |
| **Helpers** | |
| `create_directory_tree(config_path)` | create the local folders |
| `identifier_scheme(value)` | `lei`, `private`, `synthetic`, `missing` or `other` |
| `path_bronze_*`, `path_register_record`, `path_sheet_record`, `path_silver`, `ref_silver` | where things live |
| `read_json(path)` | a record of the Bronze layer, or `None` |
| `fetch`, `fetch_page`, `build_snapshot`, `build_events`, `build_synthetic`, `register_events`, `sheet_events`, `synthetic_rows`, `sheet_rows` | the steps the updates are made of |

The schemas are `notification_schema`, `assertion_schema`, `tranche_schema`,
`party_schema`, `event_schema` and `synthetic_schema`; the sources are in
`REGISTER_URL`, `SHEETS`, `SHEET_REFERENCES` and `BREXIT_SHEET_URL`.

### Further reading

* The handbook chapter [STS securitisations](../../../docs/handbook/01-sts.tex).
* ESMA publishes a Python package of its own for its registers,
  [`esma_data_py`](https://github.com/European-Securities-Markets-Authority/esma_data_py).
  It is worth reading; this module does not depend on it.

### Licence of the data

The data is downloaded when the pipeline runs and is never shipped with the
package. It is a publication of the European Securities and Markets Authority.

## Troubleshooting

* **`RuntimeError: The Bronze layer of the sts module holds no snapshot`** --
  nothing has been collected. Run `sts.update_bronze_sts("paths.yaml")` first.
* **The factsheet stops with "is a Delta table, which this template does not
  read"** -- it reads parquet with `arrow`, which would count a Delta table's
  replaced files as rows. Build the Silver layer as parquet
  (`file_format="parquet"`, the default at a path) or read the table through
  Spark.
* **The factsheet stops with "has no table"** -- build the Silver layer first:
  `sts.update_silver_sts(spark, "paths.yaml")`.
* **`collect_sheet` returns `unavailable`** -- the address no longer serves a
  spreadsheet (ESMA answers a moved file with HTML). Nothing is stored; the
  collection record has the status and content type.
* **A warning that the register carries fields this layer does not read** --
  ESMA added fields. They are in the snapshot and in no table until mapped.
* **A warning that the list of synthetic securitisations is not in the
  snapshot** -- the `synthetic` table was not built for that day; collect the
  sheet with `sts.collect_sheet("paths.yaml", "synthetic")`.
* **`RuntimeError` "holds no notification"** -- the snapshot was collected
  against a register that answered nothing. Collect again with `force=True`.
