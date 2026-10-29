# ESEF filings

## Introduction

Module `esef` brings the annual financial reports of listed issuers into the data lake, fact by fact. Issuers whose securities are admitted to trading on a regulated market in the European Union file their annual financial reports in the [European Single Electronic Format](https://www.esma.europa.eu/issuer-disclosure/electronic-reporting) (ESEF): an XHTML document whose figures -- and, from 2022 on, the notes -- are tagged in Inline XBRL against the ESEF taxonomy, which is the IFRS taxonomy with ESMA's additions.

The reports are filed with the officially appointed mechanism of each member state, each with its own site. [filings.xbrl.org](https://filings.xbrl.org), run by XBRL International, collects them in one place, together with the reports of the United Kingdom (UKSEF) and of Ukraine filed the same way, validates them, and publishes for each filing the report package as filed, the report in a viewer and the tagged facts as xBRL-JSON. The module keeps the indexes of the repository and the reports of the countries asked for, and reads them into two tables: one row per filing, and one row per fact.

### Source and terms

The indexes and the reports are downloaded from filings.xbrl.org when the pipeline runs. **No filing is shipped with fincommons**: the package holds the code that collects them and a few made-up documents in its tests. The reports are the issuers' own publications, which the Transparency Directive requires them to make public. The repository states no licence for its copies; it provides its API free of charge and reserves the right to alter it, rate-limit it or withdraw it, and asks a client that uses it routinely to name a contact in its user agent, which the `contact` argument does.

The repository is not complete. A filing with technical errors may have no xBRL-JSON, which leaves it in the table of filings without facts, and a filing whose LEI or date the repository cannot determine is missing altogether.

## Setting up

The paths of the data lake are read from the configuration file, which needs an `esef` section with the keys `bronze` and `silver`:

```yaml
esef:
    bronze: /data/esef/bronze
    silver: /data/esef/silver
```

The Silver layer may name a schema in a catalogue instead of a directory, which puts the tables within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the marks that record which reports each country was built from are files:

```yaml
esef:
    bronze: /Volumes/fincommons/esef/lake/bronze
    silver:
        table: fincommons.esef
        path: /Volumes/fincommons/esef/lake/silver
```

The tables are then `fincommons.esef.filings` and `fincommons.esef.facts`, always Delta.

## The module in five minutes

```python
from fincommons import esef

# Start Spark, with the Delta extensions where delta-spark is installed, as
# for the other modules; then collect the indexes and the reports of Austria
# and read them into the tables.
outcome = esef.update_esef(spark, "paths.yaml", ["AT"], contact="me@example.org")

filings = esef.connect_to_esef_filings(spark, "paths.yaml")
facts = esef.connect_to_esef(spark, "paths.yaml")

# The revenue each Austrian issuer reported for its last financial year, in
# the currency of its report: the total, without dimensions, for a period
# that ends on the period end of the filing.
from pyspark.sql import functions as F

revenue = (facts
    .filter((F.col("COUNTRY") == "AT")
            & (F.col("CONCEPT") == "ifrs-full:Revenue")
            & F.col("DIMENSIONS").isNull()
            & (F.col("PRDEND") == F.col("PERIODEND")))
    .join(filings.select("FILING", "NAME"), "FILING")
    .select("LEI", "NAME", "PERIODEND", "UNIT", "VALUE"))
```

An issuer may file the same report more than once -- in two languages, or corrected -- and each filing is a row of `filings` with facts of its own. The facts are keyed by `LEI`, which joins them with the entity master data of `fincommons.entitymasterdata`, and an amount is converted into another currency with `fincommons.exchangerate`.

## Keeping the tables up to date

`update_esef` is the call a job scheduler makes, and `python -m fincommons.esef` the command:

```
python -m fincommons.esef --config paths.yaml --country AT --country DE --contact me@example.org
```

It takes no argument saying what changed: that is read from the repository and from what the lake already holds, so the same command is right on the first day, when it collects every report of the countries, and on every day after it. `--year` restricts the reports collected to filings whose period ends in a year, `--packages` collects the report packages as well, and `--full` rebuilds every country of the table of facts, which is how the table follows a new version of the reader. A report that fails, in either layer, is set aside with its traceback and the run carries on; the exit code is 1 where one was set aside.

A country is the unit a run collects because the reports are large: between a few hundred kilobytes and some tens of megabytes each, most of it the notes as text blocks. A year of the repository is a few thousand filings.

## The Bronze layer

| Folder | Holds |
|--------|-------|
| `indexes/filings-<year>.json` | the index of the filings whose period ends in the year, as published |
| `reports/COUNTRY=<country>/<filing>.json` | the report of a filing in xBRL-JSON, as published |
| `packages/COUNTRY=<country>/<filing>.zip` | the report package as filed, where `packages` asks for it (`.xbri` for the newer packages) |
| `collections/indexes/<year>.json`, `collections/reports/<filing>.json` | when each was collected, and what the server said about it |

The repository publishes one index file per year of the period end and asks a client that keeps a copy of the index to read these rather than every page of its API, and to send the `Last-Modified` of the last answer as `If-Modified-Since`. An index that has not changed comes back as `304 Not Modified` without a byte. Every index is collected, since all of them together are a few tens of megabytes.

A report is downloaded when the Bronze layer does not hold it, or holds it from before the repository last processed the filing, as `processed` in the index says. A package is downloaded when the layer does not hold one with the SHA-256 the index gives, and is refused where the download does not have it. Every file is written under a temporary name and put in place once complete, and an answer that is not an index or a report -- an error page sent with status 200 -- is refused before it replaces what the layer holds.

## The Silver tables

### `filings`

One row per filing of the indexes, every country and year, whether its report was collected or not. It is rebuilt in full on every run.

| Column | Meaning |
|--------|---------|
| `FILING` | the identifier of the filing at the repository, e.g. `529900...-2025-12-31-ESEF-AT-0` |
| `LEI`, `NAME` | the issuer |
| `COUNTRY` | the country, as the repository codes it |
| `SYSTEM` | the filing system: `ESEF` (which the repository uses for the United Kingdom too) or `UAIFRS` |
| `PERIODEND` | the last day of the period the filing reports on |
| `LANG` | the language of the report, where the name of its file says |
| `PUBLISHED`, `ADDED`, `PROCESSED` | when the source published the filing, when the repository added it and when it last processed it |
| `ERRORS`, `WARNINGS`, `INCONSISTENCIES` | what the repository's validation found |
| `PACKAGEURL`, `REPORTURL`, `VIEWERURL`, `JSONURL` | where the files are, relative to `https://filings.xbrl.org`; no `JSONURL` means no facts |
| `PACKAGESHA256` | the SHA-256 of the report package |

### `facts`

One row per fact of a report collected, partitioned by `COUNTRY`.

| Column | Meaning |
|--------|---------|
| `COUNTRY`, `FILING`, `LEI`, `PERIODEND` | the filing the fact is reported in |
| `FACT` | the identifier of the fact within its report |
| `CONCEPT` | the concept, e.g. `ifrs-full:Revenue`, or an extension concept of the issuer |
| `PRDSTART`, `PRDEND` | the first and the last day of the period, both included; an instant, such as a balance, has no `PRDSTART` |
| `UNIT` | the unit of a number, e.g. `iso4217:EUR` |
| `DIMENSIONS` | the axes and members, as `axis=member;...`; empty for the total |
| `VALUE` | a number, as reported and in full units |
| `TEXT` | any other value |
| `DECIMALS` | the accuracy the issuer states for a number |
| `LANG` | the language of a text |

xBRL-JSON writes the end of a period as the start of the following day, so that a financial year to 31 March 2026 ends at `2026-04-01T00:00:00`; `PRDEND` is the day it stands for, 31 March. A fact reported as nil is left out.

**Text blocks are left out.** Most of a report, by size, is its notes, tagged whole as text blocks: the note on intangible assets of one report is half a megabyte of HTML, tagged again for goodwill and for intangible assets and goodwill together. The concept does not say that it is a text block -- that is a type of the taxonomy, which the report does not carry -- but the value does, and a text with markup in it is the note as the report presents it rather than a value to compare. The Bronze layer keeps the whole report.

The table is rebuilt country by country, and only for the countries whose reports changed. A country is one partition, and one whose Bronze layer holds a report its partition was not built from is read again and replaces its partition, leaving the others as they were. A report that cannot be read is logged and counted; its country is written without it and left unmarked, so that the next run tries it again.

## An analyst factsheet

`factsheet/esef-analyst-factsheet.qmd` is a template for a factsheet on one issuer, written in R and rendered with Quarto, with Raiffeisen Bank International AG as its example. It reads the two Silver tables as parquet and shows the profile of the issuer from the texts of its latest report, its key figures and ratios over the last six financial years, the composition of its balance sheet and of its operating income, its earnings and dividend per share, checks on the tags and the filings it was built from. It shows numbers and their development; the only prose on the page is the text of the filing itself, and what the reader of the template needs to know about the figures is in the comments of its chunks.

```
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-analyst-factsheet.qmd
```

The issuer is set by its LEI in the first chunk, together with the number of years and the language; the country of the issuer has to have been collected by the pipeline. The figures of a year are the totals its own filing reports for it, from the filing in the language of the factsheet where an issuer files in two. Only concepts of the IFRS taxonomy are used, because an issuer's extension concepts change their names from one year to the next -- RBI tags its net provisioning under three names in six years, one of them German -- and the figures a bank presents only under extension concepts, such as its operating income, are derived from those of the taxonomy. The derivation follows RBI's presentation and is the part to adapt for another issuer. The corporate design is in `r-corporate-design-functions-ggplot2.R` beside the template.

A Delta Silver layer is refused rather than read: the table of facts replaces a country's partition whenever its reports change, and a Delta table keeps the files it replaced until it is vacuumed, which a reader of the files alone would count again.
