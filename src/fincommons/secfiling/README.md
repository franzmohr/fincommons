# SEC filings

## Introduction

Module `secfiling` brings the tagged figures of the registrants of the U.S. Securities and Exchange Commission into the data lake, fact by fact. A company whose securities are registered with the Commission files its periodic reports -- the annual `10-K`, the quarterly `10-Q`, the `20-F` and `40-F` of a foreign private issuer -- with the financial statements tagged in Inline XBRL, which [Release 33-10514](https://www.sec.gov/rules/final/2018/33-10514.pdf) has required of every filer since the phase-in ended in 2021.

The Commission publishes those figures twice over: as the filings themselves, on EDGAR, and as one JSON file per company on `data.sec.gov` holding the facts already parsed out of them. This module reads the second, as [`esef`](../esef/README.md) reads the xBRL-JSON that filings.xbrl.org publishes beside the ESEF report packages. Neither module parses inline XBRL, and neither needs a parser installed.

A company file is organised by concept and not by filing: under each taxonomy a concept, under each concept a unit, and under each unit every value the registrant has ever reported for it, each carrying the accession number of the filing it came from. A filing is therefore reconstructed by grouping on that accession, and the reconstruction is exact rather than approximate: a figure a later filing restates appears again under the accession of the filing that restated it rather than replacing the first, so each filing goes on saying what it said. Of the twelve thousand eight hundred concept and period combinations of one registrant, more than half were reported by more than one of its filings, which is the comparative figures of each report standing beside the period the report is about.

### What a company file leaves out

This is worth stating plainly, because it decides what the data can be asked. A company file holds the **undimensioned numeric facts** of a filing and not the whole of it. Of a quarterly report of 2026 that states 941 facts, 480 of them without a dimension, the company file carries 321.

| in the file | not in the file |
| --- | --- |
| the consolidated figures of the primary statements | every breakdown by segment, product, class or geography |
| the numeric facts of the notes that carry no dimension | the narrative facts and the text blocks of the notes |
| the concepts of `us-gaap`, `ifrs-full`, `dei` and `srt` | every concept the registrant created for its own report |

The tables say so by having no column for a dimension or for text, rather than by having columns that are always empty. What is absent needs the filings themselves and a parser, and is a module of its own if it is ever wanted.

### Source and terms

The company files are downloaded from `data.sec.gov` when the pipeline runs. **No filing is shipped with fincommons**: the package holds the code that collects them and two made-up documents in its tests. Works of the United States Government are not subject to copyright protection in the United States.

The Commission asks that a client identify itself in the user agent of its requests, and answers one that does not with `403 Forbidden` -- a page of HTML that a run which did not check the status would store as the data. The `contact` argument is what puts an address there, and `user_agent` refuses to build a request without one. The Commission also asks that a client wanting many registrants take the bulk archive rather than a request each, which is what `bulk=True` does.

## Setting up

The paths of the data lake are read from the configuration file, which needs a `secfiling` section with the keys `bronze` and `silver`:

```yaml
secfiling:
    bronze: /data/secfiling/bronze
    silver: /data/secfiling/silver
```

The Silver layer may name a schema in a catalogue instead of a directory, which puts the tables within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the record of what was collected is files:

```yaml
secfiling:
    bronze: /Volumes/fincommons/secfiling/lake/bronze
    silver:
        table: fincommons.secfiling
        path: /Volumes/fincommons/secfiling/lake/silver
```

The tables are then `fincommons.secfiling.filings` and `fincommons.secfiling.facts`, always Delta.

## The module in five minutes

```python
from fincommons import secfiling

# Collect the company file of NVIDIA and read it into the tables. The key is
# the central index key, which the tables spell padded to ten digits.
outcome = secfiling.update_secfiling(
    spark, "paths.yaml", ["0001045810"], contact="me@example.org")

# What the registrant reported for its last financial year: the facts of its
# own annual report, for a period that ends on the period end of the filing.
filings = secfiling.connect_to_secfiling(spark, "paths.yaml", "filings")
facts = secfiling.connect_to_secfiling(spark, "paths.yaml", "facts")

revenue = (
    facts.where("CONCEPT = 'us-gaap:Revenues'")
         .join(filings.where("FORM = '10-K'").select("FILING", "PERIODEND"),
               "FILING")
         .where("PRDEND = PERIODEND")
         .select("PERIODEND", "VALUE"))
```

A registrant is found by ticker with `registrants(contact)`, which reads the Commission's own list of tickers and central index keys.

## The Silver tables

### `filings`

One row per filing the company file names, reconstructed from the facts. `FILING` is the accession number; `PERIODEND` is the latest period the filing reports on, which for a periodic report is the period it is about and not one of its comparatives; `FY` and `FP` are the fiscal year and period of the cover page, which is how a registrant whose financial year ends in January names it; `FRAMEWORK` is `US-GAAP` or `IFRS`, taken from the taxonomies the filing tagged against, so that a foreign private issuer reporting under IFRS is recognised as such rather than taken for a US GAAP filer because of where it filed.

### `facts`

One row per fact of a filing, partitioned by `FILEDYEAR`, the year the filing was made -- which is monotonic, as a fiscal year is not, since one filing states several of them. The columns a fact shares with `esef` are spelled as that module spells them, so that a module combining the two is combining two tables of the same shape rather than translating one into the other. There is no `DIMENSIONS`, no `TEXT`, no `DECIMALS` and no `LANG`, each absent because the company files do not carry it.

## A factsheet on one registrant

`templates/secfiling-analyst-factsheet.qmd` is a template for a factsheet on one registrant, written in R and rendered with Quarto to a PDF of some twenty pages, with **NVIDIA Corporation** as its example. It is the counterpart of the esef module's factsheet and is built the same way: it reads the two Silver tables as parquet and sets, from the facts of the registrant's own reports and from nothing else:

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
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render secfiling-analyst-factsheet.qmd
```

The registrant is set by its central index key in the first chunk, padded to ten digits as the tables spell it, together with the number of years. The corporate design is in `r-corporate-design-functions-ggplot2.R`, one file for the whole package in `src/fincommons/templates`, which the esef templates source as well, so that a factsheet of either regime looks like the other without the file being kept in two places.

Four things are worth knowing before the template is pointed at another registrant.

**The basis.** `basis` decides which filing a figure is taken from where several report the same year. Under `"as-reported"`, the default, the figures of a year are the ones its own annual report states for it, as the esef factsheet does, so that a figure in the column of a year is the one the market saw when that year was reported. Under `"latest"` they are taken from the most recently filed annual report that states the year, which shows a restated figure restated and fills a line the year's own report did not tag under a concept of the taxonomy; every figure that is not the year's own then carries a dagger. The second basis is something the esef module cannot offer, and it falls straight out of the company file carrying every filing's account of every period.

**Share splits.** A per-share figure of one year cannot be set beside another's unless both rest on the same number of shares. NVIDIA split its stock four for one in 2021 and ten for one in 2024, so its diluted earnings per share as each report states them run 4.52, 6.90, 3.85, 1.74, 11.93, 2.94, 4.90 -- a series that says nothing about the company. The template brings them onto the share basis of the latest year by a factor it reads out of the filings themselves: the report of the following year states the share count of this year over again, and the ratio of the two is what happened to the shares in between. Chained back from the latest year, that turns the series into 0.11, 0.17, 0.39, 0.17, 1.19, 2.94, 4.90. The factor belongs to the report a figure came from and not to the year it is about, so the adjustment is right under either basis; it is shown with the per-share figures, and `split_adjust <- FALSE` turns it off.

**The length of a financial year.** A registrant on a 52/53-week calendar does not close on the same date every year: NVIDIA's financial year 2026 runs 363 days and its 2021, a 53-week year, runs 370. The flows of a year are therefore found by a window of 340 to 380 days and not by a fixed 364 or 365, which would find nothing at all for such a registrant.

**The non-current side of the balance sheet.** A US GAAP balance sheet subtotals the current assets and the current liabilities and leaves the non-current side to be read as the remainder, which is what the template does. The taxonomy's `NoncurrentAssets` concept is deliberately not read: registrants tag it with the long-lived assets of the geographical note, not with the other side of the balance sheet. NVIDIA's 2026 report states it as 10,383 million against total assets of 206,803 million and current assets of 125,605 million.

Two things the esef factsheet has are missing here, and both because the company files do not carry them: there is no page of the issuer's own description of its activities, the narrative facts being absent, and there is no mark for a figure read from a concept of the registrant's own making, those facts being absent too. What the registrant tagged its own way is simply a line the factsheet cannot show, and the coverage page names each one it looked for. The one market figure in the document is the value of the shares held by non-affiliates, which a registrant states on the cover of its own annual report.

The last page is a disclaimer, and the cover says in its first line and in its small print what that page states in full: the document is produced by a template and the registrant is an example, no warranty is given for the correctness or the completeness of a figure, nobody who made it is associated with the registrant or with the Commission, nothing in it is advice, and liability is disclaimed as far as the law allows, in the terms of Articles 7 and 8 of the EUPL the project is licensed under. Whoever renders the factsheet for a purpose of their own is responsible for what they publish: the disclaimer is written for a template published as an example, and the paragraph on the example, the line on the cover and the reference to the licence are to be replaced by terms that fit the use.

## Reading the two regimes together

Module `financialreport` combines the output of this module with that of `esef` into one table of facts and one of filings, keyed by source. It reads both and neither reads it, so the tables of this module stand on their own and can be analysed without it.
