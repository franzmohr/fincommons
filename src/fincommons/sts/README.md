# Processing STS register data

## Introduction

This README gives an overview of the functionality of module `sts`.

Current functionalities:
* Collecting the public STS register through the machine-to-machine services of
  ESMA's registers portal, which need no key: the notifications and, as child
  documents, their tranches, originators, original lenders, sponsors and
  special purpose entities
* One complete, dated snapshot per run, stored as the portal answered it, never
  rewritten
* Collecting the four spreadsheets ESMA publishes beside the register, each
  filed under the day it was collected
* Reporting, on every collection, which of the fields expected of every
  notification are missing and which have appeared

The Silver layer is not built yet. Its tables are being designed from the
snapshots rather than from a description of the register; what the design has
to account for is below.

## Where the data comes from

| Source | Route | Holds |
| --- | --- | --- |
| the register | `https://registers.esma.europa.eu/solr/esma_registers_stsre/select` | current state only |
| ESMA33-128-760 | file on esma.europa.eu | the notifications of 2019 to 14 January 2022, frozen |
| ESMA33-128-761 | file | the cancellations to 23 May 2022, frozen, with ESMA's reasons |
| ESMA82-402-39 | file | synthetic STS, current, with columns of its own |
| `esma_register_secr.xlsx` | file | the two registered securitisation repositories, with their LEIs |

The portal's services are documented at
<https://registers.esma.europa.eu/publication/helpApp>: the registers are held
as Solr indexes and queried by URL, with `q`, `rows`, `start`, `fl`, `sort` and
`wt` parameters. No key, no registration and no declaration of interest is
needed, which is what separates this register from the securitisation data a
repository holds.

A fifth spreadsheet, the list of securitisations that lost STS status in the
Union through Brexit, is still linked from ESMA's securitisation page but the
link answers 404 with a page of HTML. It is named in
`common.BREXIT_SHEET_URL` and is not collected.

## Why a snapshot rather than an update

The register answers for what is STS today and for nothing else. A cancelled
notification leaves it: of the 109 cancellations ESMA published up to May 2022,
107 are no longer in the register. The two spreadsheets that recorded removals
were frozen in 2022, and nothing published has recorded one since.

So the Bronze layer keeps a complete copy of the register per run, filed under
the day it was taken, and a collection never rewrites a snapshot it already
holds unless it is told to. What left the register between two runs is a diff
of two snapshots, which is the only way that question can be answered at all.

**A lake that compacts these snapshots away destroys the only record it has.**

```python
from fincommons import sts

sts.create_directory_tree("paths.yaml")
sts.update_bronze_sts("paths.yaml")
sts.collected_snapshots("paths.yaml")          # ['2026-11-20']
sts.snapshot_pages("paths.yaml", "2026-11-20", "parent")
```

A snapshot is taken on its own with `collect_register`, and one spreadsheet
with `collect_sheet`:

```python
sts.collect_register("paths.yaml", page_size=500)
sts.collect_sheet("paths.yaml", "synthetic")
sts.collect_sheets("paths.yaml")               # all four, independently
```

## The layout of the Bronze layer

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

The record of a collection of the register carries the documents and pages
collected per kind, the totals the portal stated, how many fields were seen,
and the fields that are missing or unexpected. It is what the next run reads to
know a day is already held, and what a reader consults to find out whether a
snapshot is complete.

## Two populations, and what they mean for the Silver layer

Of the 1,147 notifications the register held on 20 November 2026:

| | Public | Private |
| --- | --- | --- |
| Notifications | 502 | 645 |
| Fields per document | 119 to 155 | 16 to 19 |
| Child documents | 498 have them | none at all |
| Securitisation identifier | `2138001WO21Z3B8Y8K20N202602` | `Private_2020_12_18_01` |
| The STS template assertions | all of them | none |

The split is exactly clean: every private notification is core-only and
childless, every public one carries the detail. A private notification's
identifier is synthetic and joins to nothing, so a join to reported
securitisation data reaches about five hundred notifications rather than all of
them.

Class and type are not what a small sample suggests: 682 non-ABCP, 464 ABCP and
one ABCP programme; 502 public and 645 private. Only three of the ABCP
notifications are public, which is why the `abcp_*` template fields appear on
three documents.

## The child documents

The tranches and the parties are children, joined to a notification by
`_root_`, which is the parent's `id`:

| `entity_type` | Rows | Carries |
| --- | --- | --- |
| `issins` | 2,275 | `stsre_isin_instrument`, `stsre_isin_name` |
| `originator` | 1,282 | `originator_lei`, `originator_name`, `originator_country` |
| `sspe` | 474 | `sspe_country_code`, `sspe_country_name` — **no LEI, no name** |
| `lender` | 463 | `lender_lei`, `lender_name`, `lender_country` |
| `sponsor` | 119 | `sponsor_lei`, `sponsor_name`, `sponsor_country` |

The ISINs join to `fincommons.firds` and the LEIs to
`fincommons.entitymasterdata`. The special purpose entity has no identifier in
the register, which is a gap in the register rather than in the reading of it.

The `lei`, `name` and `country` fields are JSON arrays while the
`*_country_name` beside each is a plain string, so a reader has to absorb the
inconsistency rather than trust it.

## What identifies a row

Neither identifier in the register is unique. The 1,147 notifications carried
1,115 distinct securitisation identifiers — 20 appear on more than one
notification, a securitisation being notified again when it is restructured or
re-notified — and 1,131 distinct notification identifiers. The register's own
document `id` is what identifies a row, and any join on the securitisation
identifier is one-to-many.

`stsre_notif_id` is the securitisation identifier plus a two-digit sequence on
the public half, and identical to it on the private half.
`sts_notification_information_notification_indentifier` — ESMA's spelling, with
the typo — equals `stsre_notif_id` wherever it appears, so it is a duplicate.

## What to expect of the data

Found in one pass over the register and the spreadsheets on 20 November 2026,
and the reason the Silver layer will carry data quality checks:

* Three notifications carry a single space in both identifier fields, and eight
  more have a leading space
* 20 securitisation identifiers appear on more than one notification, while
  only 27 notifications carry a reason for having been notified twice
* Two date formats occur in one document: `stsre_notif_dt` is
  `2026-09-10T00:00:00Z` while `prospectus_issue_date` is
  `2026-09-10 00:00:00.0`
* Ten ISIN children carry no ISIN, and 267 no instrument name
* Four public notifications have no child documents at all
* `stsre_sanc_status` is `Not Sanctioned` on all 1,147, so the column carries
  no information until the day it does
* The spreadsheets and the register use different vocabularies for the
  underlying exposures — the sheet's `SME loans` against the register's `credit
  facilities, including loans and leases, provided to any type of enterprise` —
  which needs a mapping table rather than a rule
* The traditional spreadsheet's `ISIN code` column uses three separators and
  three placeholders for absence, and holds two malformed ISINs

## A report on one snapshot

`templates/sts-register-report.qmd` is a template for a report on one snapshot
of the register, written in R and rendered with Quarto to a PDF. It reads the
snapshots of the Bronze layer directly — there being no Silver layer yet — and
sets out what the register holds, what it does not say, and what to expect of
the data.

```
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-register-report.qmd
```

The snapshot is the most recent one the lake holds, or the one named in
`FINCOMMONS_SNAPSHOT`, which wins where it is set so that a run reporting one
snapshot after another needs no edit of the template:

```
FINCOMMONS_SNAPSHOT=2026-11-20 \
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sts-register-report.qmd
```

| Section | Holds |
| --- | --- |
| at a glance | the notifications, the two populations, and the class and type of each |
| underlying exposures | what the notified securitisations are backed by |
| when they were notified | first notification by year, and how many notifications have since moved |
| the identifiers | the schemes they follow, and why neither of them identifies a row |
| tranches and parties | the child documents per kind, what each carries, and the originators named in most notifications |
| what the notifications say of themselves | the census of the fields, and the third parties that verified compliance |
| what changed | the arrivals and the departures against the snapshot before, which is the only record there is of a departure |
| what to expect of the data | the checks, counted over the snapshot |
| the spreadsheets | what was collected beside the register, and how much of the frozen list the register has since dropped |

The report compares the snapshot with the one before it. A lake holding one
snapshot gets a section that says so and why it matters, rather than an empty
one: the register publishes no history, so the first comparison is only
possible once a second snapshot exists.

The corporate design is in `r-corporate-design-functions-ggplot2.R`, one file
for the whole package in `src/fincommons/templates`, two levels up from this
template, which every template of every module sources so that a change to the
design is made once; a copy of it beside a template wins over the shared one,
which is what lets a template taken out of the package carry a design of its
own.

The last section is a disclaimer. A securitisation counted in the report is
one that was *notified* as simple, transparent and standardised, which is a
notification made by its own parties and not a judgement by ESMA or by anyone
else, and the report says so where it would otherwise be read as a quality
mark. Whoever renders it for a purpose of their own is responsible for what
they publish.

## Notes

The data is downloaded when the pipeline runs and is never shipped with the
package. It is a publication of the European Securities and Markets Authority.

ESMA publishes a Python package of its own for its registers,
[`esma_data_py`](https://github.com/European-Securities-Markets-Authority/esma_data_py).
It is worth reading; this module does not depend on it.
