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
        table: fincommons.financialreport.esef
        path: /Volumes/fincommons/esef/lake/silver
```

The tables are then `fincommons.financialreport.esef.filings` and `fincommons.financialreport.esef.facts`, always Delta.

## The module in five minutes

```python
from fincommons.financialreport import esef

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
    .filter((F.col("country") == "AT")
            & (F.col("concept") == "ifrs-full:Revenue")
            & F.col("dimensions").isNull()
            & (F.col("prd_end") == F.col("period_end")))
    .join(filings.select("filing", "name"), "filing")
    .select("lei", "name", "period_end", "unit", "value"))
```

An issuer may file the same report more than once -- in two languages, or corrected -- and each filing is a row of `filings` with facts of its own. The facts are keyed by `lei`, which joins them with the entity master data of `fincommons.entitymasterdata`, and an amount is converted into another currency with `fincommons.exchangerate`.

## Keeping the tables up to date

`update_esef` is the call a job scheduler makes:

```python
outcome = esef.update_esef(spark, "paths.yaml", ["AT", "DE"],
                           contact="me@example.org")
```

It takes no argument saying what changed: that is read from the repository and from what the lake already holds, so the same call is right on the first day, when it collects every report of the countries, and on every day after it. `years` restricts the reports collected to filings whose period ends in those years, `packages=True` collects the report packages as well, and `full=True` rebuilds every country of the table of facts, which is how the table follows a new version of the reader. A report that fails, in either layer, is set aside with its traceback and the run carries on, and the outcome counts it in `failed`.

No module of the package opens a Spark session, so a job scheduler runs a script around that call rather than the module itself. The session belongs to whoever starts the run -- a cluster has one already, a workstation has to say how much of itself to give it -- and a pipeline that opened its own would be deciding that for every caller:

```python
# update-esef.py -- the script a job scheduler runs
# Spark launches its Python workers with whatever `python3` resolves to on
# the PATH, which need not be the interpreter running this script. This has
# to be settled before the session is built.
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from fincommons.financialreport import esef
from fincommons.log import configure

# The log goes to standard error. The script writes nothing to standard
# output: what it did is the log, and whether it did all of it is the exit
# code.
configure("INFO")

builder = (SparkSession.builder
           .master("local[4]")
           .appName("fincommons-esef")
           .config("spark.driver.memory", "4g")
           .config("spark.local.dir", "/data/spark-tmp")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    outcome = esef.update_esef(spark, "paths.yaml", ["AT", "DE"],
                               contact="me@example.org")
finally:
    spark.stop()

# A report set aside is not a failure of the run, which is why the library
# returns rather than raises for it -- but it is a job that did less than it
# was scheduled to do, and a scheduler only sees the exit code.
sys.exit(1 if outcome["failed"] else 0)
```

A report is read whole before its facts are handed back and some are tens of megabytes of JSON, so the cores and the heap are chosen together: in local mode the executors live in the driver, and the unified pool is divided among the tasks running at once. `spark.local.dir` is named because Spark's own default is `/tmp`, which on a good many Linux installations is a tmpfs sized at half the machine's memory rather than a directory on a disk: a shuffle that outgrows it ends the run in `No space left on device` while the disk the lake sits on is all but empty. The handbook says more about both, in the Infrastructure chapter.

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
| `filing` | the identifier of the filing at the repository, e.g. `529900...-2025-12-31-ESEF-AT-0` |
| `lei`, `name` | the issuer |
| `country` | the country, as the repository codes it |
| `system` | the filing system: `ESEF` (which the repository uses for the United Kingdom too) or `UAIFRS` |
| `period_end` | the last day of the period the filing reports on |
| `lang` | the language of the report, where the name of its file says |
| `published`, `added`, `processed` | when the source published the filing, when the repository added it and when it last processed it |
| `errors`, `warnings`, `inconsistencies` | what the repository's validation found |
| `package_url`, `report_url`, `viewer_url`, `json_url` | where the files are, relative to `https://filings.xbrl.org`; no `json_url` means no facts |
| `package_sha256` | the SHA-256 of the report package |

### `facts`

One row per fact of a report collected, partitioned by `country`.

| Column | Meaning |
|--------|---------|
| `country`, `filing`, `lei`, `period_end` | the filing the fact is reported in |
| `fact` | the identifier of the fact within its report |
| `concept` | the concept, e.g. `ifrs-full:Revenue`, or an extension concept of the issuer |
| `prd_start`, `prd_end` | the first and the last day of the period, both included; an instant, such as a balance, has no `prd_start` |
| `unit` | the unit of a number, e.g. `iso4217:EUR` |
| `dimensions` | the axes and members, as `axis=member;...`; empty for the total |
| `value` | a number, as reported and in full units |
| `text` | any other value |
| `decimals` | the accuracy the issuer states for a number |
| `lang` | the language of a text |

xBRL-JSON writes the end of a period as the start of the following day, so that a financial year to 31 March 2026 ends at `2026-04-01T00:00:00`; `prd_end` is the day it stands for, 31 March. A fact reported as nil is left out.

**Text blocks are left out.** Most of a report, by size, is its notes, tagged whole as text blocks: the note on intangible assets of one report is half a megabyte of HTML, tagged again for goodwill and for intangible assets and goodwill together. The concept does not say that it is a text block -- that is a type of the taxonomy, which the report does not carry -- but the value does, and a text with markup in it is the note as the report presents it rather than a value to compare. The Bronze layer keeps the whole report.

The table is rebuilt country by country, and only for the countries whose reports changed. A country is one partition, and one whose Bronze layer holds a report its partition was not built from is read again and replaces its partition, leaving the others as they were. A report that cannot be read is logged and counted; its country is written without it and left unmarked, so that the next run tries it again.

## Data quality

`quality` holds the checks the figures are held to, and `update_silver_esef_quality` writes what they found into a third Silver table, `issues`, beside the two the pipeline builds. The machinery is shared with the SEC module in [`fincommons.dataquality`](../../dataquality.py); what lives here are the nineteen checks themselves, read from the concepts of the IFRS taxonomy.

```python
from fincommons.financialreport import esef
from fincommons.dataquality import describe_check, issue_counts

esef.update_silver_esef_quality(spark, "paths.yaml", countries=["AT"])

issues = esef.connect_to_esef_issues(spark, "paths.yaml")
issue_counts(issues, by=["country"], dataset="esef").show()
print(describe_check("HIGH_EXTENSION_SHARE"))
```

A run told to check part of the population replaces those countries and carries the rest forward, which is how the pipeline collects: one country at a time.

**Nothing is dropped and nothing is corrected.** A check writes a row and leaves the figure where it is. A filing with nothing wrong with it has no row at all.

The valuable checks are the arithmetic ones, and they are the relations a statement states on both sides: total assets against the current and the non-current assets and again against the equity and the liabilities, revenue against the cost of sales and the gross profit, the result before tax against the tax and the result, the three cash flows against the movement in cash. They are run over the undimensioned facts only -- a figure broken down by segment is a part of a total and not a total.

Two checks have no counterpart in the SEC module, because the repository publishes something the Commission does not: it validates every filing and publishes the counts, which `VALIDATION_ERRORS` carries into the table rather than leaving on its site, and it lists filings whose xBRL-JSON it could not produce, which `NO_FACTS` counts. Both read the table of filings, which covers the whole index rather than the reports a lake has collected, so their rows name filings whose facts are not in the lake -- which is right, since both questions are answered by the index and a filing nobody downloaded is not thereby sound. On one index of 24,180 filings, 983 carry validation errors and 192 have no facts to publish. Against that, the restatement check of the SEC module has no counterpart here: the repository publishes each filing on its own.

`HIGH_EXTENSION_SHARE` is the one check that says nothing is wrong. A report that tags more than half its figures with concepts of the issuer's own making is not faulty, it is hard to compare, and the code says so where the coverage report says it at the level of the market.

What each code means, how severe it is and what to do about it are rows of `data/dataquality_checks.csv` rather than branches in the code. Nothing in it is authoritative; `describe_check` prints a row, and a deployment that keeps checks of its own ships a catalogue of the same shape and names it.

The checks were run over six real voestalpine reports, for the financial years to March 2021 through 2026, and they found nothing in the figures: the balance sheets balance, the income statements reconcile and the cash flows sum. Getting there corrected one check. The identity of the result before tax, the tax and the result does not hold without the discontinued operations, which fall between those lines; voestalpine tags them, and its reports for 2021/22 and 2022/23 were reported as faulty by exactly the discontinued result -- 257.9 and 93.4 million -- until the check read it.

Two more corrections came from the Austrian and Dutch reports of the lake. The three cash flows sum to the movement in cash before the effect of exchange rates on the cash held, which IAS 7 presents on a line of its own, and the check had read the movement after it: 59 of the 72 reports it found faulty were out by nothing but their exchange differences. It now reads the movement before the effect, and accepts the movement as tagged as well, because some reports put the movement before the effect under the concept for the one after it and their figures agree. The asset split takes a gap as explained where it is the assets held for sale, which some issuers present below the current assets rather than among them -- TenneT's German business in 2023, 27.6 billion of 41.7 -- under either of the two concepts the taxonomy has for that line; that was 9 of 12. The three left are Österreichische Post's, which presents the assets of bank99 as a third block beside the current and the non-current ones: the figures agree, and the check is right that the two subtotals are not the whole. And the identity of the result before tax, the tax and the result takes a gap as explained where it is the share of the profit of associates, which ASML presents below the tax line: its reports for 2023 to 2025 were out by exactly its share of Carl Zeiss SMT, as was the one other report the check had found.

A report filed twice for one period in one language is two things. Where the copies state different facts it is a correction, and `DUPL_FILING_PERIOD` warns that a reader taking either is taking one of two. Where every copy states the same facts it is one report the repository received twice -- on the Austrian reports most such pairs were added within minutes of each other -- and `DUPL_FILING_COPY` notes it without faulting it. The facts are compared by a fingerprint over their concepts, periods, dimensions, units, values and texts, leaving out the identifiers a report gives its facts, which differ between two uploads of one file. Of the 77 filings the warning had named in the Austrian and Dutch reports, 42 were copies.

## A factsheet on one issuer

`templates/esef-analyst-factsheet.qmd` is a template for a factsheet on one issuer, written in R and rendered with Quarto to a PDF of some twenty pages, with voestalpine AG as its example. It reads the two Silver tables as parquet and sets, from the facts of the issuer's own reports and from nothing else:

| Pages | Hold |
|-------|------|
| cover, contents | the issuer, the years covered, the figures of the latest year, and how the figures are put together |
| at a glance | the latest financial year against the one before it, in sixteen lines |
| the issuer | the identity of the issuer from the texts of its latest report, and the years in one table |
| profit or loss | the income statement by function, the same statement as a share of revenue, the growth on the year before, what revenue was spent on, and the margins |
| financial position | the assets, the equity and liabilities, the capital structure and the net debt, the leverage, the liquidity and what the debt costs, the working capital and the cash conversion cycle |
| returns | the returns on equity, on total assets, on capital employed and on invested capital, and the decomposition of the return on equity into margin, turnover and leverage |
| cash flows | the cash flow statement, the quality of the cash flow against the result, and the capital expenditure |
| equity | the comprehensive income and the equity by component, which is the one place the factsheet reads the dimensions of the taxonomy |
| per share | the earnings, the dividend and the book value per share, the number of shares behind them, and what was distributed |
| screening | Piotroski's F-score, test by test, and Altman's Z''-score, term by term |
| the tags | how much of each report could be read, the lines read from the issuer's own concepts, the lines looked for and not tagged, and checks that the statements add up |
| the filings | every filing of the years shown, what a figure rests on, and the source |

It shows numbers and their development; the prose on the page is the text of the filing itself, a note on how the figures are put together and the definitions of the indicators, and what the reader of the template needs to know about a figure is in the comments of its chunk.

```
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-analyst-factsheet.qmd
```

The issuer is set by its LEI, either in the first chunk together with the number of years and the language, or in `FINCOMMONS_ISSUER`, which wins where it is set so that a run rendering one issuer after another needs no edit of the template:

```
FINCOMMONS_ISSUER=529900ARBU9SBJFNX419 \
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-analyst-factsheet.qmd
```

The country of the issuer has to have been collected by the pipeline. The figures of a year are the totals its own filing reports for it, from the filing in the language of the factsheet where an issuer files in two; the comparative figures of the previous year that every filing carries as well are left aside, so that a figure in the column of a year is the one that year was reported with. One year more than the factsheet shows is read behind it, because a return puts the result of a year against the average of the capital at its start and its end, and because the screening indicators compare a year with the one before it.

Concepts of the IFRS taxonomy come first, because an issuer's extension concepts change their names from one year to the next, and a key may list several concepts in order of preference for a line whose tag changed: voestalpine tags its property, plant and equipment under two names in six years. The lines the taxonomy has no concept for -- voestalpine's capital expenditure and the movement in its working capital, both of which it presents in its cash flow statement -- are kept in a list of the issuer's own concepts beside it. A figure read from one of those carries a dagger wherever it is shown, and the page on the coverage of the tags names every one of them with the number of years it was found in, so that a reader can tell a figure of the taxonomy from a figure of the issuer's own making.

The figures are those of an industrial issuer that presents its income statement by function -- revenue, cost of sales, gross profit, EBIT -- and the figures it does not tag, such as its net debt, its capital employed or the number of its shares, are derived from those it does. The derivation follows voestalpine's presentation and is the part to adapt for another issuer; a bank needs other figures altogether. **No EBITDA is derived**: depreciation and amortisation are not a line of the primary statements under a concept of the taxonomy, and an issuer that tags them nowhere else leaves every indicator that would rest on EBITDA out of reach, so the leverage and the cover ratios stand against the operating result and the operating cash flow instead. A financial year that does not end in December is labelled by the two years it spans, as 2025/26.

The corporate design is in `r-corporate-design-functions-ggplot2.R`, one file for the whole package in `src/fincommons/templates`, three levels up from a template, which every template of every module sources so that a change to the design is made once; a copy of it beside a template wins over the shared one, which is what lets a template taken out of the package carry a design of its own. The charts use its categorical palette, the ten colours of Tableau, so that eight lines of a balance sheet in one chart can still be told apart.

The last page is a disclaimer, and the cover says in its first line and in its small print what the page states in full: the document is produced by a template and the issuer is an example, no warranty is given for the correctness or the completeness of a figure, nobody who made it is associated with the issuer or with any other body named on the pages, nothing in it is advice, and liability is disclaimed as far as the law allows, in the terms of Articles 7 and 8 of the EUPL the project is licensed under. Whoever renders the factsheet for a purpose of their own is responsible for what they publish: the disclaimer is written for a template published as an example, and the paragraph on the example, the line on the cover and the reference to the licence are to be replaced by terms that fit the use.

A Delta Silver layer is refused rather than read: the table of facts replaces a country's partition whenever its reports change, and a Delta table keeps the files it replaced until it is vacuumed, which a reader of the files alone would count again.

## A factsheet on a sector

`templates/esef-sector-factsheet.qmd` is the counterpart of the factsheet on one issuer: several issuers at once, each year's figures beside the years around them and beside the market they were filed in. It reads the two Silver tables of this module as parquet and, where the lake holds them, the combined tables of `fincommons.financialreport.combined` and the exchange rates of `fincommons.exchangerate`, and renders with Quarto to a PDF.

```
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-sector-factsheet.qmd
```

Two examples ship with it. The template's own first chunk holds the Austrian construction sector, nine issuers, which is small enough to read in the template and is what the page renders with no further setting. `templates/esef-sector-panel-europe.csv` holds the same sector read across Europe -- 145 issuers in seventeen countries -- and is used instead by naming it:

```
FINCOMMONS_PANEL=.../esef-sector-panel-europe.csv \
FINCOMMONS_FINANCIAL=.../financial-leis.txt \
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-sector-factsheet.qmd
```

### The panel is a stated list

**This is the one thing about the template to understand before using it.** An ESEF report carries no sector code: a filing states the issuer's legal entity identifier and nothing about what the issuer does, and the sector classification this package does hold -- `sector` in `fincommons.entitymasterdata` -- is filled from the registers of the ECB, EIOPA and ESMA and covers financial entities only. So whoever writes the panel decides what the sector is. A different panel gives different totals and different medians, which is why the panel is printed in full at the front of the rendered document and why the disclaimer on the last page says in as many words that the page is about the issuers named and about no population wider than them.

A panel of nine fits in the template. A panel of a hundred and forty-five does not, so the panel may instead be read from a csv named in `FINCOMMONS_PANEL`, with `SHORT` and `GROUP` required and an `LEI`, a `CIK` or an `EDINET_CODE` on every row, and `NACE`, `COUNTRY`, `ACTIVITY_EN`, `ACTIVITY_DE` and `NAME` used where the file has them. The headers are upper case, as they are in the two files shipped beside the template: a panel is a list kept by hand and keeps the names it has always had, while the Silver columns it is joined to are snake_case since 0.5.0, which is why every join between the two names both sides. `NAME` is for whoever maintains the file; the page names an issuer as its own report names it.

Each row carries a group, and the groups are the reason the template exists in this shape rather than as one list of issuers. A contractor turns its balance sheet several times a year on advance payments, a developer holds a project for years and reports the result when it sells, a materials maker runs plant and carries inventory, a merchant holds stock and no projects at all. A median over all of them hides the thing a reader came for, so every table and chart is shown by group as well as for the panel as a whole, and the colour of a line is its group. The European panel uses five: contractors, developers, building materials, building technology, and merchants and distribution.

What the repository holds limits any such panel, and the limit is worth stating because it is invisible in the output. **filings.xbrl.org holds no German and no Irish filings at all.** Hochtief, Bilfinger and Heidelberg Materials are not in it, and the Irish issuers that are -- Kingspan, Cairn Homes, Glenveagh -- are there under `GB`, filed in London. A European panel built from this repository is therefore European minus Germany, and says so.

### Why the European panel stops at Ukraine

The repository holds the reports of Ukraine as well, filed the same way under the system it codes `UAIFRS`, and they are a third of everything in it: 9,871 filings of 3,164 issuers against 17,797 of 4,136 for the rest of Europe together. Among them are real construction companies -- Івано-Франківськцемент, Миколаївцемент, Київметробуд, Трест Житлобуд-1 -- so leaving them out of a European panel is a decision and not an oversight. Three things decide it, and the first two are properties of the data rather than judgements:

* **They are not keyed by an LEI.** A Ukrainian filing is identified as `EDRPOU-01432552-2023-12-31-UAIFRS-UA-0`: the code of the Ukrainian register, not a legal entity identifier. Nothing in this package that joins on an LEI reaches them -- not the sectoral registers that find the banks, not `fincommons.entitymasterdata`. The test that reads a filer's own statements still does, which is the one place this costs nothing.
* **The hryvnia is not quoted.** The exchange rate table of `fincommons.exchangerate` carries 41 currencies against the euro and the hryvnia is not among them, the ECB publishing no reference rate for it. Every Ukrainian amount would therefore stay unconverted by the rule above -- correctly, and uselessly: the issuers would appear in every table with every amount missing.
* **They would carry the population.** A benchmark is a median over the filings in scope, and three thousand Ukrainian issuers reporting under a different regime, in a currency that cannot be converted, through a war, would be most of that median while saying little about what a European contractor's balance sheet looks like.

A lake that wants them collects `UA` like any other country and the tables hold them; what this template's example panel and benchmark leave out is a choice made in one line of configuration, and this is the reasoning behind it.

### Amounts in more than one currency

A European panel reports in a dozen currencies, and a sum over them is arithmetic on different units. The template converts, and it converts by the convention of the supervisory pipelines that build on this package, which is worth stating because the column names carry it: **an amount keeps the figure and the currency the issuer filed, and the conversion only ever adds two columns beside it.** `<key>_FXRATE` is the rate that was applied and is 1 where the amount was filed in the target currency already; `<key>_DOMESTIC` is the filed amount divided by that rate. The name says `DOMESTIC` rather than `EUR` so that nothing downstream has to be renamed when the target changes.

An amount whose currency the rate table does not quote stays unconverted. `_DOMESTIC` is missing, the sums pass over it, the count beside the sum is smaller by one, and the page names the issuer-years it happened to. It is never the filed figure carried over as though it were already in the target currency, and never a nought. A lake whose rate table holds nothing quoted against the target is refused outright rather than converted into a page of blanks -- the same check the EMIR and SFTR pipelines make, and for the same reason: a null layer reads afterwards as a quiet market rather than as a table that could not be built.

Which rate is applied follows IAS 21 rather than one rate per filing: a balance at the closing rate of the reporting date, an income or expense at the average of the daily rates over the financial year. **A ratio is not converted at all.** A margin, an equity ratio and a return are each a quotient of two amounts of one filing in one currency, so the currency cancels and they are computed from the figures as filed -- which is also why an issuer whose currency the table does not reach still stands in the benchmark while its amounts are missing from the sums.

A panel in one currency needs none of this, reads no rate table, and needs no `exchangerate` section in the configuration file.

### A panel that reaches past one regime

A panel row is keyed by the identifier its regime uses: an `LEI` for a filing under ESEF, a `CIK` for one with the Commission, an `EDINET_CODE` for one with the Financial Services Agency of Japan. No body publishes a mapping between them -- which is why the combined table carries all three columns and fills one of them in every row -- so the template does the same, and a row needs one of them. An EDINET code is the letter E and five digits (`E00058` is Kajima); the securities code (`18120`) is not it, and the template refuses a panel that gives one. Japanese annual securities reports (document types `120` and `130`) count as annual reports in the benchmark, and the cross-regime table adds each regime's amounts up in its own currency, so a Japanese row is in yen. `templates/esef-sector-panel-crossregime.csv` is the example: the 145 European issuers of the panel above together with 86 American ones, 231 in all.

What crosses the frameworks is **the eight measures of the catalogue and nothing else**. Everything the template reads from the ESEF concepts -- the operating result, the cash flow, the inventories, the net debt -- reaches the European members alone, because no pair of concepts for an operating result is in the catalogue: `describe_measure("ebit")` returns nothing. Pairing `ifrs-full:ProfitLossFromOperatingActivities` with `us-gaap:OperatingIncomeLoss` would be this template inventing a mapping the `combined` module declines to make, so it does not. The cross-regime tables are built from the measures and the per-issuer tables keep saying what each regime's filer tagged.

Three things the Commission's filings force, none of which an ESEF-only lake shows:

* **A filing with the Commission may be quarterly.** There are three `10-Q` for every `10-K`, and a quarter's profit over a year's equity is not a return, so the benchmark keeps the annual forms and a filing that carries no form at all, which is what an ESEF filing does.
* **A filing with the Commission carries no country.** The combined table has the column and `sec` leaves it empty, so the country filter keeps a row whose country is missing. Without that, naming countries -- or letting them default to the panel's own -- would quietly empty the American half of a pooled benchmark and leave a page that looked complete.
* **An issuer is not keyed by its LEI.** Grouping the benchmark on one would collapse every American registrant into a single issuer, since they all have none. The key is the identifier of the regime, with the other side empty.

The result and the equity a pooled question wants are the total where the filer states it and the parent's share where it does not. The regimes lean opposite ways -- most ESEF filers state the total, most American registrants only the parent's share -- so neither alone reaches both, and the page carries a table of **what each pooled figure is made of**: how many filings of each source contributed a total and how many a parent's share. A pooled median is not wrong, and it is not one population either.

### The benchmark, and what counts as a bank

The benchmark is read from the combined table through the materialised `measure` column rather than by resolving a measure per filing. That is the division of labour the combined README argues for, and it has a cost the page states: only the first concept of each framework carries a measure, so a filing that stated revenue as IFRS 15 revenue alone is in the tables of the panel and not in the quartiles, and every count beside a benchmark figure is a count of filings that stated **and named** the figure rather than a population. Which filings are in scope is three lines -- source, country, framework -- so a lake that has run `sec` or `edinet` widens the same page to those regimes, with the warning that module makes about measures that are not skewed alike between them. Left out, the countries are the ones the panel itself filed in, which is the right default rather than a convenient one -- the market a panel is held against is the market it was filed in -- and which also removes a trap: the panel can be replaced by a file without touching that list, and a country written out there would quietly measure a European panel against one national market.

Banks and insurers are shown apart rather than dropped, because an equity ratio of ten per cent in the same column as one of forty says something worth seeing about what the ratio means. Two tests identify them, and the second exists because the first has a hole in it:

* the **sectoral registers** -- the ECB's register of monetary financial institutions, EIOPA's of insurance undertakings and of institutions for occupational retirement provision, and ESMA's list of central counterparties, which is where `sector` of `fincommons.entitymasterdata` gets its answer. The LEIs are read from a file named in `FINCOMMONS_FINANCIAL`, one per line, which `entitymasterdata.download_ecb_mfi` and its siblings are enough to produce; GLEIF is not needed for this one question;
* the **filings themselves**, for a filer whose primary statements carry a line only a financial undertaking has: interest revenue computed with the effective interest method, or one of the IFRS 17 insurance lines. The concepts are named in the first chunk.

The hole is twofold and is what the second test closes. A register lists the authorised entity, and what files an annual financial report is usually the group above it -- BAWAG P.S.K. is on the register and BAWAG Group AG, which files, is not. And the registers are European, so a British bank has not been on them since 2020, which for a lake holding the United Kingdom would leave the largest banks in the population unflagged. Over the Austrian filings the registers alone reach 22 of the 27 the two tests together find; the five they miss are holding companies, and their own statements name them.

### How a figure is picked

| What it sets out | From |
|---|---|
| the panel, with the group, the country and the years each issuer filed for | `filings` |
| the totals of the sector, year by year, each with the number of issuers that stated the figure | `facts` |
| growth like for like, over the issuers present in both years rather than over those totals | `facts` |
| the groups apart: revenue, the operating result, the margin and the equity ratio of each | `facts` |
| each issuer in the latest year, and its revenue since the first year shown | `facts` |
| the quartiles of the panel against the quartiles of the market, for four indicators | `financialreport.fact` |
| the equity ratio of every issuer against the interquartile range of the market, year by year | both |
| which lines each issuer tagged, in how many years | `facts` |

One filing per issuer and financial year is chosen, by the rule of the single-issuer factsheet: the report in the language of the page, then the one with fewer validation errors, then the one the repository processed last. That last tie-break is not cosmetic. PORR filed its report for 2024 twice, both in German and both validating without error, and the two differ by a factor of a thousand -- one states revenue of 6,190,521 with the unit `iso4217:EUR` and the other 6,190,521,000 -- so the filing the repository processed last is the one that is right.

A financial year is placed in the year its period end falls in, a year ending before July counting into the year before, so that Zumtobel's year to 30 April 2026 is the sector's 2025 and is still labelled 2025/26 wherever it stands alone. **No EBITDA is derived**, for the reason it is not derived in the factsheet on one issuer. A Delta Silver layer is refused in either module, for the reason given above.

## A data quality report on the findings

`templates/esef-quality-report.qmd` is the third template of the module and the counterpart of the table of issues: what the checks found, read over the whole record rather than filing by filing. It reads the three Silver tables as parquet and renders with Quarto to a PDF of some sixteen pages. **No number in it is an amount out of a report**; every figure is a count of filings, findings or checks.

| Section | Holds |
|---------|-------|
| the record checked | what was checked per country and per financial year, which countries the checks have run over at all, and the table of the checks with the population each is asked of |
| what the checks found | the findings by severity, every check ranked by the share of filings it named, the bands of findings per filing, and the same by country and by financial year |
| the record that holds up | the share of filings free of errors and the share with no finding at all, the five identities read together, what a silent pass is, the filings with the most found in them, and what the repository's own validation found |
| what this report cannot say | the six things a count of findings does not settle |
| the checks in full | every entry of the catalogue -- what it finds, what a finding means, what to do about it -- with the count it found in this record |
| the run | what was read, how to run the checks again, the sources and a disclaimer |

```
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-quality-report.qmd
```

The countries and the financial years are set in the first chunk and default to every country the lake holds and the last five years in it. The catalogue of checks is read from `data/dataquality_checks.csv`, the same file the module reads, so a check added to the module appears in section 5 without the template being touched; a code in the table of issues that the catalogue does not describe is refused rather than printed bare.

Two things decide what the report says, and it states both rather than leaving them to be inferred.

**The population.** A share is only readable against the right denominator. A check that reads the figures of a report could only fire on a filing the *table of facts* holds facts for, which is what the pipeline collected and not what the repository could have given it; `VALIDATION_ERRORS` and `NO_FACTS` read the index and could fire on every filing listed. Since the one population sits inside the other, the filings with facts are the only population every check was asked of, and every rate in the report -- the share free of errors, the shares by country and by year, the bands of findings per filing -- is taken over those. Over the whole index instead, a record of which a part has been collected would read as sound because nothing had looked at the rest of it. The per-check table keeps each check's own denominator and names it.

**Findings against filings.** A check names the records that failed it: the filing, where the fault is a fault of the report as a whole, and the fact, where one figure is at fault. A filing therefore has as many rows as it has bad facts, and the two counts are different things. The report names them apart throughout -- findings are rows, filings are the distinct filings those rows name -- and no table adds one to the other. The loudest one per cent of the filings carrying a third of the findings is the ordinary case, which is why the share of filings free of errors and not the count of findings is the report's headline.

A country with no row in the table of issues is either a country every check passed or one the checks have never run over, and nothing in the tables tells the two apart. The report names those countries rather than counting them as sound. A Delta Silver layer is refused for the same reason as in the other two templates.

## A coverage report on the record

`templates/esef-coverage-report.qmd` is the counterpart of the factsheet: one issuer in depth against the whole record at a glance. It reads the same two Silver tables as parquet and renders with Quarto to a PDF of some sixteen pages, which counts what the repository lists and what the issuers tagged, and nothing else. **No number in it is an amount out of a report**: every figure is a count of filings, facts, concepts or issuers.

| Section | Holds |
|---------|-------|
| the record | what the indexes list per country -- filings, issuers, second filings of a year, filings without facts -- the reports read per financial year, the days from the period end to publication against the four months Article 4 of the Transparency Directive allows, what the repository's own validation found, and the record against the shares FIRDS holds |
| what is tagged | the facts of a median report, numeric, dimensional and text, the taxonomies the facts are tagged against, how much of a report sits under concepts of the issuer's own making, the concepts of the taxonomy that the most issuers tag, and the local names the most issuers invent a concept for |
| what can be compared | the share of issuers tagging each line of the three primary statements, the lines ranked, how many of the lines an issuer tags, and the share of issuers each indicator of the factsheet could be computed for |
| what this report cannot say | the four things the facts do not carry, and what each puts out of reach |
| the record read | the run, the lines looked for with the concepts of each, the sources and a disclaimer |

```
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render esef-coverage-report.qmd
```

The countries and the financial years are set in the first chunk and default to every country the lake holds and the last five years in it; `report_countries <- "AT"` and `report_years <- 2025L` narrow it to one country and one year, which renders in seconds. A country named there that the table of filings does not list is refused rather than shown as an empty column.

An issuer that filed its report twice -- in two languages, or again with corrections -- is counted once per financial year, since a cross-section counts issuers: the report kept is the one with facts, then the one with fewer validation errors, then the one the repository processed last. The language is not part of that choice, as it is in the factsheet, because the two filings of a year state the same figures and this report reads no texts. A line counts as tagged in the year it was reported for, so the facts counted are those without dimensions whose period ends on the period end of the filing; the comparative figures of the year before, which every report carries as well, are left aside.

The lines looked for are the lines the factsheet reads, which is what makes the two templates one pair: **the coverage of a line is the answer to how many issuers the factsheet could be built for**, and the share for an indicator is the size of the panel a cross-sectional analysis of that indicator could rest on. A fact under `ifrs-full`, `ifrs-smes`, `esef_cor`, `esef_all`, `ifrs` or `esma` counts as the taxonomy and anything else as a concept the issuer created for its own report; the list is `taxonomy_prefixes` in the first chunk, and a lake holding reports tagged against a taxonomy it does not name would count them as extensions.

The comparison with FIRDS needs the `firds` module's Silver layer in the same configuration file and is left out with a note where there is none. It is counted over the whole record the lake holds rather than over the countries and years of the report, so that a report narrowed to one country does not read as though the rest of the market had never filed. Two things qualify it, and the page says both: the compact table of FIRDS carries no flag for admission to a regulated market at the issuer's request, so the count is of every issuer of a share FIRDS still holds a record for, which is the wider population; and FIRDS is a register of the Union, while the repository also collects the United Kingdom and Ukraine.

Four absences of the Silver tables decide what the report is, and it states them on a page of its own rather than in a footnote. A fact in xBRL-JSON carries no **presentation role**, so the statements the report groups its lines into are its own grouping and not the issuers' presentation. **Anchoring and the calculation relationships** live in the extension taxonomy of the report package, which the module collects only when asked and never reads, so the report counts extensions without saying what they were anchored to, and makes no check that a statement adds up. A **label** is in that taxonomy too, so two issuers count as having invented the same thing only where they spelt the local name the same way. And the **text blocks** of the notes are not in the table at all, so nothing here measures the tagging of the notes.

The template reads the facts through four queries that arrow answers over the parquet files, each returning a result of the size of the market rather than of the size of the lake; the table of facts is never collected. A Delta Silver layer is refused for the same reason as in the factsheet, and the more sharply: this report counts facts, and a Delta table keeps the files it has replaced until it is vacuumed.

## Two factsheets on one country

Two short templates take a country and nothing else, and do the rest from the lake: the years shown are those the country has reports with facts for, and every issuer of it is in. Both read the Silver tables as parquet and render with Quarto to a PDF of four or five pages; the country is set in `FINCOMMONS_COUNTRY`, so one copy of a template renders every country the pipeline has collected:

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml FINCOMMONS_COUNTRY=NL quarto render esef-country-quality-factsheet.qmd -o quality-NL.pdf
FINCOMMONS_CONFIG=/path/to/paths.yaml FINCOMMONS_COUNTRY=NL quarto render esef-country-economic-factsheet.qmd -o economy-NL.pdf
```

Both stop rather than render where the country has no facts, and both refuse a session that is not in a UTF-8 locale, which would print the names of issuers as `<U+00D6>` escapes.

### Data quality

`templates/esef-country-quality-factsheet.qmd` answers one question: whose figures to check before using them. Where the quality report reads the record as a whole, this ranks issuers. Each check of the module scores once per issuer and financial year, however many facts or reports it names -- a report filed again for the same year repeats the findings of the first, and counting them per report would rank an issuer by how often it re-filed. An error scores 3, a warning 1 and a note nothing; the weights are `points` in the first chunk. `MISS_REVENUE` and `IMBAL_ASSETS` are not counted for a bank or an insurer: its revenue is interest or premium income and tagged as such, and its assets are presented by liquidity or, in a group with a bank, with the bank's assets as a block beside the current and the non-current ones. A bank or an insurer is an issuer that presents one of their lines: interest revenue under the effective interest method, an IFRS 17 line, or a bank's loans to other banks or deposits of its customers. The balance sheet lines are there for a bank that tags its interest income under another concept, as Oesterreichische Kontrollbank does; without them it was ranked for three missing revenue lines.

Where an arithmetic check fired, the detail page says by how much: the two sides, the gap, and the gap as a share of the figure the check measures it against. A gap of a tenth of a percent and one of half the balance sheet are different things to look for. On the Austrian reports it shows what kind of mistake each is: Österreichische Post's three cash flows miss the movement in cash by a fifth of a percent of its assets, a line left out, while Raiffeisenlandesbank Niederösterreich-Wien's three cash flows of 972 million stand against a movement in cash of 31 thousand, which is another line tagged with the concept of the movement.

The checks look at one report at a time, and some of the telling mistakes only show when an issuer's reports are set side by side. So the template adds six screens over six headline figures:

| Screen | What it finds |
| --- | --- |
| restated | a report's figure for the year before differs by more than 1 % from the figure that year's own report gave |
| scale | the same, by a factor of 50 or more: thousands tagged as units, or the reverse |
| sign | the same, with the sign flipped |
| jump | within one report, a figure 50 times its own prior year, or a fiftieth of it |
| currency | a report in a currency the issuer uses in no other year |
| identity | a report that names another entity than the one of the LEI it is filed under, where no other report under the LEI matches either |

The identity screen exists because of two Austrian filings found on the lake. One is a filing agent's test report, "Mustermandant AG" of "12345 Musterstadt", filed under the agent's own LEI with a revenue of 17.6 billion. The other is EVN's annual report filed under the LEI of the data vendor that submitted it. A renamed company passes, since its newer reports state the name the LEI carries. These reports, and reports in a stray currency, are listed on the first page whatever their rank, under *Reports to set aside*. The pages then give the ranking, a map of which check fired for which issuer, the country against the rest of the lake, the trend by year, and every finding of the ten issuers ranked highest.

### Economic facts and trends

`templates/esef-country-economic-factsheet.qmd` describes what a country's listed companies reported: revenue, operating profit and result, margins, returns, equity ratios, cash flow, dividends and staff costs. It shows the largest companies, the movers, the largest losses and the spread across companies. Banks and insurers are shown on their own page and kept out of every corporate figure.

What makes the figures readable is how they are taken:

- **Growth is like for like.** A company's figure for the year is set against the figure its own report gives for the year before, and the change is summed over the companies stating both. A year still being filed has a lower sum for that reason alone, so its like-for-like changes and medians are the ones to read, and the cover says when the latest year is short.
- **One report per company and year,** the one with the most facts.
- **Only group figures with no breakdown,** for the twelve months or the balance-sheet date of the report.
- **A year is shown where it holds at least half the companies of the best-covered year.** A year the pipeline was not asked to collect is not drawn as a trend.
- **Sums are in euros and convert nothing.** A company reporting in another currency is in every median and ratio and in no sum.
- **A bank** is a company whose interest revenue under the effective interest method is at least half its revenue, or which tags no revenue. **An insurer** presents a line of IFRS 17. A postal operator with a bank stays among the companies.
- **A group reported twice,** by a listed holding and by its listed subsidiary, is counted once. Heineken Holding and Heineken, and ING Groep and ING Bank, are the Dutch examples: total assets and revenue within 2 % and equity within 10 %.
- **A report the identity screen of the quality factsheet catches is left out.** Its figures are either not real or already counted under the right LEI.

The page covers listed companies and issuers of listed bonds, the population ESEF reaches. It says so, since the sums are not the economy as a whole.
