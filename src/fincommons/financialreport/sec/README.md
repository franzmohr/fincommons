# SEC filings

Module `sec` brings the tagged figures of the registrants of the U.S. Securities and Exchange Commission into the data lake, fact by fact: the consolidated figures of every annual and quarterly report a company has tagged, one row per figure. Use it to read a US-listed company's revenue, balance sheet or cash flows over the years, to see which figures a later report restated, or to put US filers beside the European ones of [`esef`](../esef/README.md).

At a glance:

- **Source:** the company files the Commission publishes on `data.sec.gov` -- one JSON file per registrant, holding every fact it has ever tagged.
- **Update:** whenever you run it; a company file that has not changed since the last run is not downloaded again.
- **Tables written:** `filings`, `facts` and, if you run the checks, `issues`, all in the Silver layer (the cleaned tables analyses read).
- **Key:** the central index key (CIK) of the registrant, padded to ten digits, and the accession number of the filing.
- **Needs:** network access, Spark, and a **contact address**: the Commission refuses requests that do not name one. No API key.

## Quick start

> **You must pass `contact=`.** The Commission requires a client to identify itself in the user agent of its requests and answers one that does not with `403 Forbidden`. Every function that downloads takes a `contact` argument -- an e-mail address, for instance `contact="me@example.org"` -- and refuses to send a request without one.

1. Add a `sec` section to `paths.yaml`:

```yaml
sec:
    bronze: /data/sec/bronze
    silver: /data/sec/silver
```

2. Start Spark, collect one registrant and read the tables. A first run for one registrant is one download of a few megabytes; the full Spark session with Delta is under [Running it on a schedule](#running-it-on-a-schedule).

```python
from pyspark.sql import SparkSession
from fincommons.financialreport import sec

spark = SparkSession.builder.master("local[4]").getOrCreate()   # see docs/spark-session.md for a scheduled run

sec.create_directory_tree("paths.yaml")

# Collect the company file of NVIDIA and read it into the tables. The key is
# the central index key, which the tables spell padded to ten digits.
sec.update_sec(spark, "paths.yaml", ["0001045810"], contact="me@example.org")

filings = sec.connect_to_sec(spark, "paths.yaml", "filings")
facts = sec.connect_to_sec(spark, "paths.yaml", "facts")
filings.orderBy("filed").show(5)
```

3. Ask your first question -- what the registrant reported for its last financial years: the facts of its own annual reports, for a period that ends on the period end of the filing.

```python
revenue = (
    facts.where("concept = 'us-gaap:Revenues'")
         .join(filings.where("form = '10-K'").select("filing", "period_end"),
               "filing")
         .where("prd_end = period_end")
         .select("period_end", "value"))
revenue.orderBy("period_end").show()
```

You do not know the CIK? `sec.registrants(contact)` reads the Commission's own list of tickers and central index keys; see [Common tasks](#common-tasks).

## What you get

A company whose securities are registered with the Commission files its periodic reports -- the annual `10-K`, the quarterly `10-Q`, the `20-F` and `40-F` of a foreign private issuer -- with the financial statements tagged in Inline XBRL, which [Release 33-10514](https://www.sec.gov/rules/final/2018/33-10514.pdf) has required of every filer since the phase-in ended in 2021. By default the tables keep those four forms and their amendments (`PERIODIC_FORMS`).

What the columns of each table hold, and what a reader may rely on, is stated by its data contract (see [Reference](#reference)). `connect_to_sec` returns each table in the order of its declared schema, whether it is stored as Parquet, which puts the partition column `filed_year` last when it is read, or as Delta.

### `filings` -- one row per filing

One row per filing the company file names, reconstructed from the facts. Key: `filing`.

| column | meaning |
| --- | --- |
| `filing` | the accession number, which identifies a filing with the Commission, in the form `0000000000-00-000000` |
| `cik` | central index key of the registrant, ten digits |
| `name` | name of the registrant, as the Commission holds it |
| `form` | `10-K`, `10-Q`, `20-F`, `40-F`; an amendment carries `/A` |
| `filed` | the day the filing was made |
| `period_end` | the latest period the filing reports on under a reporting taxonomy, which for a periodic report is the period it is about and not one of its comparatives. The cover page is deliberately not counted: it states a date after the period closed |
| `fy`, `fp` | the fiscal year and period of the cover page (`FY`, `Q1` to `Q4`), which is how a registrant whose financial year ends in January names it |
| `taxonomies` | the taxonomies the filing tagged against, separated by semicolons (`dei;us-gaap`) |
| `framework` | `US-GAAP` or `IFRS`, taken from the taxonomies the filing tagged against, so that a foreign private issuer reporting under IFRS is recognised as such rather than taken for a US GAAP filer because of where it filed |
| `facts` | how many facts the filing stated, of the forms the run kept |

### `facts` -- one row per fact

One row per fact of a filing, partitioned by `filed_year`. Key: `filing` and `fact`.

| column | meaning |
| --- | --- |
| `cik` | central index key of the registrant |
| `filing` | accession number of the filing the fact came from |
| `filed_year` | the year the filing was made, the partition column -- monotonic, as a fiscal year is not, since one filing states several of them |
| `fact` | an identifier made for the fact (`accession:concept:start:end:unit`), so that a file read again does not grow the table |
| `concept` | taxonomy and name, for instance `us-gaap:Revenues` |
| `prd_start` | first day of the period; empty for a fact of an instant |
| `prd_end` | last day of the period, or the day an instant stands for |
| `unit` | `USD`, `shares`, `pure`, `USD/shares` ... |
| `value` | the value, as reported |
| `form`, `filed` | form and day of the filing, repeated for convenience |
| `frame` | the Commission's own label for a period it considers comparable across companies, where it gave the fact one; carried as published and nothing here depends on it |

The columns a fact shares with `esef` -- `filing`, `fact`, `concept`, `prd_start`, `prd_end`, `unit`, `value` -- are spelled as that module spells them, so that a module combining the two is combining two tables of the same shape rather than translating one into the other. There is no `dimensions`, no `text`, no `decimals` and no `lang`, each absent because the company files do not carry it: the Commission publishes the value and not the precision the filing claimed for it, and files in English. As for ESEF, `value` is no measure: it holds every figure in one column, and the measures of a financial report are those of `fincommons.financialreport.combined`.

### `issues` -- one row per issue found

Written by `update_silver_sec_quality`; see [Data quality](#data-quality). Columns `cik`, `filed_year` (partition), `filing`, `fact` and `concept` (empty where the issue is about the filing as a whole), `issue` (the code of the check) and `checked` (the day of the run).

### What a company file leaves out

This is worth stating plainly, because it decides what the data can be asked. A company file holds the **undimensioned numeric facts** of a filing and not the whole of it. Of a quarterly report of 2026 that states 941 facts, 480 of them without a dimension, the company file carries 321.

| in the file | not in the file |
| --- | --- |
| the consolidated figures of the primary statements | every breakdown by segment, product, class or geography |
| the numeric facts of the notes that carry no dimension | the narrative facts and the text blocks of the notes |
| the concepts of `us-gaap`, `ifrs-full`, `dei` and `srt` | every concept the registrant created for its own report |

The tables say so by having no column for a dimension or for text, rather than by having columns that are always empty. What is absent needs the filings themselves and a parser, and is a module of its own if it is ever wanted.

### Joining to other modules

- **To ESEF and EDINET:** module `financialreport` combines the output of this module with that of `esef` and `edinet` into one table of facts and one of filings, keyed by source; see its [README](../combined/README.md). It reads all three and none of them reads it, so the tables of this module stand on their own and can be analysed without it.
- **To an LEI (legal entity identifier):** not possible. Neither the Commission nor GLEIF publishes a mapping from a central index key to an LEI, so this module cannot be joined to `esef` or `entitymasterdata` by entity; see [No mapping to the LEI](#no-mapping-to-the-lei).

## Common tasks

**Find the CIK of a company by ticker or name.** `registrants` returns a list of dicts with `cik` (padded), `ticker` and `name`:

```python
listed = sec.registrants(contact="me@example.org")
[r for r in listed if r["ticker"] == "NVDA"]
```

**Collect several registrants.** Pass their keys; padded or not, with or without a `CIK` prefix, all work:

```python
sec.update_sec(spark, "paths.yaml", ["1045810", "320193", "CIK0000789019"],
               contact="me@example.org")
```

**Collect the whole population.** The Commission asks that a client wanting many registrants take the bulk archive (about 1.4 GB) rather than a request each, which is what `bulk=True` does. With `ciks` as well, the archive is taken and only those registrants are kept:

```python
sec.update_sec(spark, "paths.yaml", bulk=True, contact="me@example.org")
```

**Keep every form, not only the periodic reports.** A company file also holds the facts of an `8-K` announcing a result and of registration documents that tag a figure or two. `forms=None` keeps them all:

```python
sec.update_silver_sec(spark, "paths.yaml", forms=None)
```

**Check the figures and see what was found.**

```python
from fincommons.dataquality import describe_check, issue_counts, with_issues

sec.update_silver_sec_quality(spark, "paths.yaml")

issues = sec.connect_to_sec(spark, "paths.yaml", "issues")
issue_counts(issues, dataset="sec").show()            # one row per check
print(describe_check("RESTATED_MATERIALLY"))          # what a code means

facts = sec.connect_to_sec(spark, "paths.yaml", "facts")
with_issues(facts, issues, ["cik", "filing", "fact"]) # label the records
```

**See which figures a later report restated.**

```python
(issues.where("issue = 'RESTATED_MATERIALLY'")
       .groupBy("concept").count().orderBy("count", ascending=False).show())
```

**Look up what a column means, or run the contract's rules.**

```python
import fincommons
from fincommons.contracts import check_quality, load_contract

print(fincommons.describe_column("fp", dataset="sec"))
results = check_quality(filings, load_contract("fincommons.sec.silver.filings"))
```

## Running it on a schedule

`update_sec` is the call a job scheduler makes: it collects the company files into the Bronze layer (the files as downloaded) and then rebuilds the two Silver tables. No module of the package opens a Spark session, so a scheduler runs a script around that call:

```python
# update-sec.py -- the script a job scheduler runs
import os
import sys

# Spark's Python workers must use this interpreter; set before the session.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from fincommons.financialreport import sec
from fincommons.log import configure

configure("INFO")                                        # log to standard error

builder = (SparkSession.builder
           .master("local[4]")
           .appName("fincommons-sec")
           .config("spark.driver.memory", "4g")
           .config("spark.local.dir", "/data/spark-tmp")  # not a tmpfs /tmp
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    sec.update_sec(spark, "paths.yaml", ["0001045810", "0000320193"],
                   contact="me@example.org")
    sec.update_silver_sec_quality(spark, "paths.yaml")
finally:
    spark.stop()
```

What a run does, in order, and what happens when part of it fails:

1. **Collect** (`update_bronze_sec`). Each company file is asked for with the `Last-Modified` of the last answer as `If-Modified-Since`, so a company whose filings have not changed comes back as `304 Not Modified` without a byte. `force=True` asks again regardless. A registrant that has never tagged a filing is answered with `404` and counted as absent, not as a failure.
2. **Retry politely.** The Commission limits a client to ten requests a second and answers one that asks faster with `429 Too Many Requests`. A request turned away with `429` or `503` is made again up to `RETRIES` (5) times, waiting at most `MAX_WAIT` (120) seconds.
3. **Fail per file, not per run.** A company file that fails -- a download cut off, an answer that is not a company file -- is logged with its traceback and counted, and the others carry on. At the end the stage raises `RuntimeError` naming up to ten of the failed keys, so the scheduler sees a non-zero exit code.
4. **Rebuild** (`update_silver_sec`). Both tables are rebuilt in full from every company file the Bronze layer holds (or the `ciks` given). A file that cannot be read is logged and counted and does not fail the run.
5. **Check** (`update_silver_sec_quality`, optional). With `ciks`, only the filed years those registrants touch are replaced and every other year is carried forward.

The run is idempotent: running it again collects only what changed and rebuilds the same tables.

## How it works

### The Commission's two publications

**This module reads the Commission's own parsed facts, not the filings, and needs no XBRL parser.** The Commission publishes the tagged figures twice over: as the filings themselves, on EDGAR, and as one JSON file per company on `data.sec.gov` holding the facts already parsed out of them. This module reads the second, as [`esef`](../esef/README.md) reads the xBRL-JSON that filings.xbrl.org publishes beside the ESEF report packages. Neither module parses inline XBRL, and neither needs a parser installed.

### Reconstructing a filing

**A filing is rebuilt exactly by grouping the facts on their accession number.** A company file is organised by concept and not by filing: under each taxonomy a concept, under each concept a unit, and under each unit every value the registrant has ever reported for it, each carrying the accession number of the filing it came from.

The reconstruction is exact rather than approximate: a figure a later filing restates appears again under the accession of the filing that restated it rather than replacing the first, so each filing goes on saying what it said. Of the twelve thousand eight hundred concept and period combinations of one registrant, more than half were reported by more than one of its filings, which is the comparative figures of each report standing beside the period the report is about.

### The Bronze and Silver layers

**Bronze keeps the files as published; Silver is rebuilt in full from them on every run.** The Bronze layer holds `companies/CIK<key>.json`, one company file per registrant, and `collections/CIK<key>.json`, a small record of when each was collected and what the server said, which is how the next run knows what it need not download again. A file is written under a temporary name and put in place once complete, so a run that dies leaves the previous file rather than half of the new one.

There is no watermark in the Silver layer because there is nothing to resume from: a company file is republished whole whenever any of the registrant's filings change, so a file that changed changes rows of every year it covers. What a run avoids repeating is the collection, not the reading. The reading happens on the executors, one company file at a time, so the file is the unit of parallelism and a run scales with the number of companies collected.

A file that came out of the bulk archive gets no `Last-Modified` record, because the archive's date is not the file's: the next run asking for that company on its own would be told it had not changed when it may have.

### Why the contact is required

**A request without a contact gets a `403` page, which a careless run would store as data.** The Commission answers an anonymous request with `403 Forbidden` -- a page of HTML that a run which did not check the status would store as the data. `user_agent` therefore refuses to build a request without a contact, before the first request is made, and every answer is checked to be a company file before it is stored. This is the one place this module is stricter than `esef`: filings.xbrl.org asks, and the Commission refuses.

### Data quality

**Nothing is dropped and nothing is corrected.** `quality` holds the sixteen checks the figures are held to, and `update_silver_sec_quality` writes what they found into a third Silver table, `issues`, beside the two the module collects. A check writes a row and leaves the figure where it is: the tables are what the Commission published, and a reader who wants to know what is wrong with a figure before using it joins the issues on. A filing with nothing wrong with it has no row at all -- the absence of a code is what says a record passed. The machinery is shared with the ESEF module in [`fincommons.dataquality`](../../dataquality.py); what lives here are the checks themselves, because a check knows the data set it is made of.

**The arithmetic checks are the valuable ones.** They are the relations a statement states on both sides: total assets against the liabilities and the equity, revenue against the cost of revenue and the gross profit, the income before tax against the tax and the net income, the three cash flows against the movement in cash. A difference there is a tagging fault rather than a difference of opinion, because the scale of a number is an attribute of the tag and invisible on the page. A difference within `TOLERANCE` (0.1 per cent of the total) is rounding. They run over the annual forms only (`ANNUAL_FORMS`: `10-K`, `20-F`, `40-F`), because a quarterly report states an income statement of the quarter and of the year to date.

The rest are structural: a period that ends before it begins, a figure tagged twice with two values, a concept in two units, a line the comparison looked for and did not find.

| code | finds |
| --- | --- |
| `IMBAL_BALANCE_SHEET`, `IMBAL_TOTALS`, `IMBAL_INCOME`, `IMBAL_PROFIT`, `IMBAL_CASHFLOW` | a statement that does not add up |
| `IMPL_NEGATIVE_ASSETS`, `IMPL_PERIOD`, `IMPL_FILED_BEFORE_PERIODEND`, `IMPL_FY_PERIODEND` | an implausible figure, period or date |
| `MISS_ASSETS`, `MISS_REVENUE`, `MISS_PROFIT`, `MISS_FRAMEWORK` | a line or framework looked for and not found |
| `DUPL_FACT_VALUE`, `MIXED_UNIT` | a figure tagged twice with two values; a concept in two units |
| `RESTATED_MATERIALLY` | a figure a later annual report states differently |

**One check has no counterpart in the ESEF module, and it is the one the company files make possible.** `RESTATED_MATERIALLY` sets what a year's own report said beside what a later report says about that same year, which a company file allows because it carries every filing's account of every period. A move of more than `RESTATEMENT_TOLERANCE` (1 per cent) is reported. On NVIDIA's 69 filings it finds 149 restatements. Share counts and per-share amounts (`SPLIT_RESTATED_UNITS`) are passed over: a split restates those by arithmetic rather than by judgement, and NVIDIA's two splits accounted for 69 further rows that said nothing about the accounts.

**The catalogue of checks is data, not code.** What each code means, how severe it is and what to do about it are rows of `data/dataquality_checks.csv` rather than branches in the code, so that the people most able to find a check wrong can read it. Nothing in it is authoritative; `describe_check` prints a row, and a deployment that keeps checks of its own ships a catalogue of the same shape and names it.

**Two findings on NVIDIA looked like faults, and only one was.** A US balance sheet presents mezzanine equity between the liabilities and the equity, so leaving `TemporaryEquity...` out of the identity reported NVIDIA's 2016 and 2017 reports as unbalanced by exactly the 87 and 31 million they hold. And three of its filings are reported as made before the period they report on had ended: the period end of a filing is the latest period any of its facts reports on, so a fact looking forward -- in these three, an accelerated share repurchase settling the next quarter -- carries it past the day the filing was made. That is a property of how `period_end` is derived and not of the filings.

### A factsheet on one registrant

**`templates/sec-analyst-factsheet.qmd` renders a factsheet of some twenty pages on one registrant, from its own reports and nothing else.** It is written in R and rendered with Quarto to a PDF, with **NVIDIA Corporation** as its example. It is the counterpart of the esef module's factsheet and is built the same way: it reads the two Silver tables as Parquet.

| page | what it holds |
| --- | --- |
| at a glance | the latest financial year against the one before it, in amounts, margins and returns |
| statement of operations | revenue to net income, what revenue is used for, and the growth of each line |
| margins | the margins and the cost structure as a share of revenue |
| balance sheet | both sides, with the non-current side read as the remainder |
| debt | net debt, leverage, interest cover and liquidity |
| working capital | the balances and the days of each, and the cash conversion cycle |
| returns | return on assets, equity, capital employed and invested capital, and DuPont |
| cash flow | the three flows, free cash flow, and the quality of the result |
| per share | earnings, dividends and book value per share, and what was returned to the holders of the shares |
| screening | the Piotroski F-score and Altman's Z''-score |
| coverage | what the company file carried, which lines were looked for and found, and the checks on the figures |

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sec-analyst-factsheet.qmd
```

The registrant is set by its central index key in the first chunk (`issuer_cik`), padded to ten digits as the tables spell it, together with the number of years (`n_years`). The Silver layer must be Parquet: a Delta table keeps the files of earlier versions, which the template would read as well, so it refuses one. The corporate design is in `r-corporate-design-functions-ggplot2.R`, one file for the whole package in `src/fincommons/templates`, three levels up from a template, which the esef templates source as well, so that a factsheet of either regime looks like the other without the file being kept in two places.

Four things are worth knowing before the template is pointed at another registrant.

**The basis.** `basis` decides which filing a figure is taken from where several report the same year.

- `"as-reported"`, the default: the figures of a year are the ones its own annual report states for it, as the esef factsheet does, so that a figure in the column of a year is the one the market saw when that year was reported.
- `"latest"`: they are taken from the most recently filed annual report that states the year, which shows a restated figure restated and fills a line the year's own report did not tag under a concept of the taxonomy; every figure that is not the year's own then carries a dagger. The second basis is something the esef module cannot offer, and it falls straight out of the company file carrying every filing's account of every period.

**Share splits.** A per-share figure of one year cannot be set beside another's unless both rest on the same number of shares. NVIDIA split its stock four for one in 2021 and ten for one in 2024, so its diluted earnings per share as each report states them run 4.52, 6.90, 3.85, 1.74, 11.93, 2.94, 4.90 -- a series that says nothing about the company.

The template brings them onto the share basis of the latest year by a factor it reads out of the filings themselves: the report of the following year states the share count of this year over again, and the ratio of the two is what happened to the shares in between. Chained back from the latest year, that turns the series into 0.11, 0.17, 0.39, 0.17, 1.19, 2.94, 4.90. The factor belongs to the report a figure came from and not to the year it is about, so the adjustment is right under either basis; it is shown with the per-share figures, and `split_adjust <- FALSE` turns it off.

**The length of a financial year.** A registrant on a 52/53-week calendar does not close on the same date every year: NVIDIA's financial year 2026 runs 363 days and its 2021, a 53-week year, runs 370. The flows of a year are therefore found by a window of 340 to 380 days (`MIN_DAYS`, `MAX_DAYS` in `quality`) and not by a fixed 364 or 365, which would find nothing at all for such a registrant.

**The non-current side of the balance sheet.** A US GAAP balance sheet subtotals the current assets and the current liabilities and leaves the non-current side to be read as the remainder, which is what the template does. The taxonomy's `NoncurrentAssets` concept is deliberately not read: registrants tag it with the long-lived assets of the geographical note, not with the other side of the balance sheet. NVIDIA's 2026 report states it as 10,383 million against total assets of 206,803 million and current assets of 125,605 million.

**What is missing compared with the esef factsheet.** Two things, both because the company files do not carry them: there is no page of the issuer's own description of its activities, the narrative facts being absent, and there is no mark for a figure read from a concept of the registrant's own making, those facts being absent too. What the registrant tagged its own way is simply a line the factsheet cannot show, and the coverage page names each one it looked for. The one market figure in the document is the value of the shares held by non-affiliates, which a registrant states on the cover of its own annual report.

**The disclaimer.** The last page is a disclaimer, and the cover says in its first line and in its small print what that page states in full: the document is produced by a template and the registrant is an example, no warranty is given for the correctness or the completeness of a figure, nobody who made it is associated with the registrant or with the Commission, nothing in it is advice, and liability is disclaimed as far as the law allows, in the terms of Articles 7 and 8 of the EUPL the project is licensed under. Whoever renders the factsheet for a purpose of their own is responsible for what they publish: the disclaimer is written for a template published as an example, and the paragraph on the example, the line on the cover and the reference to the licence are to be replaced by terms that fit the use.

### A data quality report on the findings

**`templates/sec-quality-report.qmd` reads the table of issues over the whole record rather than filing by filing.** It is the counterpart of the esef module's quality report and is built the same way -- it reads the three Silver tables as Parquet and renders with Quarto to a PDF of some fifteen pages. **No number in it is an amount out of a filing**; every figure is a count of filings, findings or checks.

| Section | Holds |
|---------|-------|
| the record checked | what was checked per form and per year filed, which years the checks have run over at all, the frameworks the filings tag against, and the table of the checks with the population each is asked of |
| what the checks found | the findings by severity, every check ranked by the share of filings it named, the bands of findings per filing, and the same by year filed and by registrant |
| the record that holds up | the share of annual reports free of errors and the share with no finding at all, the five identities read together, what a silent pass is, what a later report says about an earlier one, and the filings with the most found in them |
| what this report cannot say | the seven things a count of findings does not settle |
| the checks in full | every entry of the catalogue -- what it finds, what a finding means, what to do about it -- with the count it found in this record |
| the run | what was read, how to run the checks again, the sources and a disclaimer |

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render sec-quality-report.qmd
```

The registrants and the years are set in the first chunk (`report_ciks`, `report_years`, `n_years`) and default to every registrant the lake holds and the last six years in it. The catalogue of checks is read from `data/dataquality_checks.csv`, the same file the module reads, so a check added to the module appears in section 5 without the template being touched; a code in the table of issues that the catalogue does not describe is refused rather than printed bare.

Three things decide what the report says, and it states all three rather than leaving them to be inferred.

**The population.** A share is only readable against the right denominator. A check that reads a balance sheet could only fire on a filing that carries one, which is a filing made on `10-K`, `20-F` or `40-F`, amendment or not; the checks that read a date or a unit could fire on every filing. Since the one population sits inside the other, the annual reports are the only population every check was asked of, and every rate in the report -- the share free of errors, the shares by year and by registrant, the bands of findings per filing -- is taken over those. Over every filing instead, a record of which the quarterly reports are the bulk would read as sound because the checks that read a balance sheet had never looked at them. The per-check table keeps each check's own denominator and names it.

**Findings against filings.** A check names the records that failed it: the filing, where the fault is a fault of the filing as a whole, and the fact, where one figure is at fault. A filing therefore has as many rows as it has bad facts, and the two counts are different things. The report names them apart throughout -- findings are rows, filings are the distinct filings those rows name -- and no table adds one to the other. A single noisy filing carrying a third of the findings is the ordinary case, which is why the share of annual reports free of errors and not the count of findings is the report's headline.

**Why the years are the years filed.** A company file is organised by concept and carries every filing's account of every period, and the year a filing was made is the one column of it that only ever moves forward. A registrant that changes its year end reports two fiscal years or none, so a report counted by fiscal year would gain and lose filings for a reason that has nothing to do with their quality. The years are therefore the years the filings were made, which is also what the tables of facts and of issues are partitioned by. The table of filings carries no such column and the template derives it from `filed`, exactly as the module does.

The report gives the restatement check a section of its own, because it is the one check with no counterpart in the esef module and the one that most needs reading rather than counting: it names the facts a later annual report states differently for a period an earlier one already reported, and a restatement, a reclassification and a correction all look the same in the figures. The section ranks the concepts restated most often, which for NVIDIA's own filings is dominated by the accrued liabilities and the sundry non-current lines.

A year with no row in the table of issues is either a year every check passed or one the checks have never run over, and nothing in the tables tells the two apart. The report names those years rather than counting them as sound. A Delta Silver layer is refused for the same reason as in the factsheet.

### No mapping to the LEI

**The central index key cannot be turned into an LEI, so both are carried side by side in the combined tables.** Neither the Commission nor GLEIF publishes a mapping from a central index key to a legal entity identifier. The closest thing is a by-product of the LEI records themselves, which name the register an entity is registered in: some tens of thousands name EDGAR, and a few of those carry a central index key rather than a fund series identifier. It is a register of funds, in effect.

An operating company's record points at its state register instead -- NVIDIA's at Delaware's Division of Corporations, by file number `2862596`, which is not its central index key `0001045810`. Both columns are therefore carried in the combined tables and neither is derived from the other. This module invents nothing.

## Reference

### Configuration

The `sec` section of `paths.yaml`:

| key | holds |
| --- | --- |
| `bronze` | directory of the company files and the records of their collection; always a directory |
| `silver` | directory of the Silver tables, or a catalogue schema (see below) |

The Silver layer may name a schema in a catalogue instead of a directory, which puts the tables within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the record of what was collected is files:

```yaml
sec:
    bronze: /Volumes/fincommons/sec/lake/bronze
    silver:
        table: fincommons.financialreport.sec
        path: /Volumes/fincommons/sec/lake/silver
```

The tables are then `fincommons.financialreport.sec.filings`, `.facts` and `.issues`, always Delta. A Silver layer addressed by path is stored as Parquet unless `file_format="delta"` is passed, and a table that exists keeps its format; see `fincommons.storage.silver_format`.

### Public functions

All are importable from `fincommons.financialreport.sec`.

| function | what it does |
| --- | --- |
| **Pipeline** | |
| `update_sec(spark, config_path, ciks=None, contact=None, bulk=False, forms=PERIODIC_FORMS, file_format=None, force=False)` | collect the company files and rebuild `filings` and `facts` |
| `update_bronze_sec(config_path, ciks=None, contact=None, bulk=False, force=False)` | collect the company files only |
| `update_silver_sec(spark, config_path, ciks=None, forms=PERIODIC_FORMS, file_format=None, chunk_rows=CHUNK_ROWS)` | rebuild `filings` and `facts` from the Bronze layer; `forms=None` keeps every form |
| `update_silver_sec_quality(spark, config_path, ciks=None, file_format=None)` | run the checks and rebuild `issues` |
| **Read** | |
| `connect_to_sec(spark, config_path, table="facts")` | open `"facts"`, `"filings"` or `"issues"` |
| `registrants(contact)` | the Commission's list of registrants, as dicts of `cik`, `ticker`, `name` |
| `collected_companies(config_path)` | the CIKs the Bronze layer holds |
| `find_issues(spark, config_path, ciks=None)` | run the checks and return the issues without writing them |
| **Collect one piece** | |
| `collect_company(config_path, cik, contact, force=False)` | one company file; returns `'collected'`, `'unchanged'` or `'absent'` |
| `collect_bulk(config_path, contact, ciks=None)` | the bulk archive; returns how many files were written |
| `fetch(url, contact, headers=None)` | one request with the contact and the retries |
| `user_agent(contact)` | the user agent string; raises `ValueError` without a contact |
| **Parse (plain Python, no Spark)** | |
| `read_company(document, forms=PERIODIC_FORMS)` | filings and facts of one company file |
| `filings_from_company`, `facts_from_company` | the two halves of `read_company` |
| `padded_cik(value)` | a CIK padded to ten digits; accepts a `CIK` prefix, refuses anything not a number |
| `is_accession(value)`, `is_periodic(form, forms=PERIODIC_FORMS)` | tests on an accession number and a form |
| `build(spark, config_path, what, ciks=None, forms=PERIODIC_FORMS, ...)` | the parsed rows of `"filings"` or `"facts"`, before writing |
| `annual_figures(facts, filings)` | the wide table of annual figures the arithmetic checks read |
| **Layout** | |
| `create_directory_tree(config_path)` | create the local directories; on Databricks they are provisioned outside the pipeline |
| `path_bronze_*`, `path_silver_*`, `ref_silver_*`, `path_company_record` | where each part of the lake lives |

Constants: `BASE_URL`, `COMPANY_URL`, `BULK_URL`, `TICKERS_URL`, `SOURCE`, `PERIODIC_FORMS`, `FRAMEWORKS`, `RETRIES`, `MAX_WAIT`, `CHUNK_ROWS`, and of the checks `CHECKS`, `CONCEPTS`, `ANNUAL_FORMS`, `TOLERANCE`, `RESTATEMENT_TOLERANCE`, `SPLIT_RESTATED_UNITS`, `MIN_DAYS`, `MAX_DAYS`, `KEYS`, `DATASET`. Schemas: `filings_schema`, `facts_schema`, `issues_schema`.

### Data contracts and catalogues

- Data contracts in the Open Data Contract Standard: `data/contract_sec_silver_filings.yaml`, `data/contract_sec_silver_facts.yaml` and `data/contract_sec_silver_issues.yaml` (identifiers `fincommons.sec.silver.filings`, `.facts`, `.issues`), which `data/product_sec.yaml` names as the output ports of the module. They are the one place the columns are described -- `fincommons.describe_column("fp", dataset="sec")` reads its answer from there -- and the declared schemas of the module are held to them by the tests.
- A contract states the key of its table (the accession number; the accession number and the fact) and the rules the table is held to: one row per filing, per fact and per issue, a CIK of ten digits, an accession number in its form, a framework of IFRS or US-GAAP, a unit for every value, and a partition that is the year the filing was made. `fincommons.contracts.check_quality` runs them; the checks that name each figure at fault are those of [Data quality](#data-quality), which the contract of the facts points at.
- The check catalogue: `data/dataquality_checks.csv`.

### Source and terms

The company files are downloaded from `data.sec.gov` when the pipeline runs. **No filing is shipped with fincommons**: the package holds the code that collects them and two made-up documents in its tests. Works of the United States Government are not subject to copyright protection in the United States.

## Troubleshooting

- **`ValueError: The Commission requires a client to identify itself`** → no `contact` was given → pass `contact="you@example.org"` to `update_sec`, `update_bronze_sec`, `registrants` or `collect_company`.
- **`ValueError: Name the registrants to collect, or ask for bulk=True`** → `update_sec` was called with neither `ciks` nor `bulk=True` → pass one of them.
- **`ValueError: '...' is not a central index key`** → a CIK that is not a number (a ticker, for instance) → look the key up with `registrants`.
- **`RuntimeError: N company files could not be collected`** → some downloads failed; the others were stored → read the log for the tracebacks and run again; files already collected are not downloaded again.
- **A query for a breakdown by segment, a text block or a company-specific concept returns nothing** → the company files do not carry them → see [What a company file leaves out](#what-a-company-file-leaves-out).
- **A filing appears to be filed before its period ended** → a forward-looking fact moved its `period_end`; see the NVIDIA example under [Data quality](#data-quality).
- **A template stops with "is a Delta table"** → the templates read Parquet only → store the Silver layer as Parquet for rendering.
