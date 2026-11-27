# EDINET filings

Module `edinet` brings the tagged figures of the filers of Japan's Financial Services Agency into the data lake, fact by fact: every figure and text of the annual, semi-annual and other reports Japanese companies and funds file, one row per fact, plus the agency's list of everyone who can file. Use it to read a Japanese company's five-year summary, its consolidated or parent-only statements, its workforce and its largest shareholders, or to put Japanese filers beside those of [`sec`](../sec/README.md) and [`esef`](../esef/README.md).

At a glance:

- **Source:** the API of EDINET, the agency's filing system -- a daily index of what was filed, and the agency's own conversion of each filing's XBRL into CSV -- and the list of filers on the disclosure site.
- **Update:** whenever you run it; by default the last thirty days. **Filings older than ten years can no longer be fetched**, so history not collected in time is lost.
- **Tables written:** `filings`, `facts`, `filers` and, if you run the checks, `issues`, all in the Silver layer (the cleaned tables analyses read).
- **Key:** the EDINET code of the filer (`E` and five digits) and the document management number of the filing.
- **Needs:** network access, Spark, and a **registered EDINET API key** (free). The list of filers alone needs no key.

## Quick start

> **You need an API key.** The API refuses a request that carries no registered key. Register at `disclosure2.edinet-fsa.go.jp` to get one, then make it available in one of these ways (first found wins): the `key` argument, the environment variable `FINCOMMONS_EDINET_KEY`, or a file named `edinet` in the directory `FINCOMMONS_VAULT` names. **Never put the key in `paths.yaml`**; see [The API key](#the-api-key) for why.

1. Make the key available, for instance as a file in a vault directory:

```bash
export FINCOMMONS_VAULT=/run/secrets        # one file per key, mode 600
printf '%s' "$EDINET_KEY" > /run/secrets/edinet
```

2. Add an `edinet` section to `paths.yaml`:

```yaml
edinet:
    bronze: /data/edinet/bronze
    silver: /data/edinet/silver
```

3. Start Spark and run the whole pipeline: collect the indexes of a range of calendar days, then the conversions of the filings those indexes say the agency converted, then rebuild the tables. Thirty days back by default, which is what keeping a lake up to date costs: one request per calendar day, plus one per filing converted. The full Spark session with Delta is under [Running it on a schedule](#running-it-on-a-schedule).

```python
from pyspark.sql import SparkSession
from fincommons.financialreport import edinet

spark = SparkSession.builder.master("local[4]").getOrCreate()   # see docs/spark-session.md for a scheduled run

edinet.create_directory_tree("paths.yaml")
edinet.update_edinet(spark, "paths.yaml")

filings = edinet.connect_to_edinet(spark, "paths.yaml", "filings")
facts = edinet.connect_to_edinet(spark, "paths.yaml", "facts")
filings.where("has_csv = '1'").select("filing", "edinet_code", "name",
                                      "doc_type", "period_end").show(5)
```

4. Ask your first question -- what one filer reported. The key is the EDINET code, the letter E and five digits; 7513 is Kojima's securities code and E03270 is its EDINET code. Kojima does not consolidate, so its figures are under `NonConsolidatedMember` (see [Consolidated and non-consolidated](#consolidated-and-non-consolidated)).

```python
revenue = (
    facts.where("edinet_code = 'E03270'")
         .where("concept = 'jpcrp_cor:NetSalesSummaryOfBusinessResults'")
         .where("dimensions = 'NonConsolidatedMember'")
         .select("prd_start", "prd_end", "value")
         .orderBy("prd_end"))
revenue.show()
```

You do not know the EDINET code? Look it up in the list of filers, which needs no key; see [Common tasks](#common-tasks).

## What you get

EDINET -- 金融商品取引法に基づく有価証券報告書等の開示書類に関する電子開示システム -- is the agency's filing system, the counterpart of EDGAR, and it has required the financial statements of a securities report to be tagged in XBRL since 2008.

What the columns of each table hold, and what a reader may rely on, is stated by its data contract (see [Reference](#reference)). `connect_to_edinet` returns each table in the order of its declared schema, whether it is stored as Parquet, which puts the partition column `filed_year` last when it is read, or as Delta.

Two things the contracts state that are easy to miss. `filed_year` is the year a document was submitted, not the year of the index that named it: a document an official corrected appears again in a later day's index, and six of 22,083 in one lake had an `index_date` a year after their submission. And the table of filers keeps a value the agency's list leaves blank as an empty string, not as null, as the other tables do: 4,177 of 11,471 filers have an empty corporate number, and the rules on the codes of a filer accept a blank.

### `filings` -- one row per document the indexes name

Partitioned by `filed_year`. Key: `filing`.

| column | meaning |
| --- | --- |
| `filing` | the 書類管理番号, the document management number the API addresses a filing by (`S100VWVY`), and the key of everything else here |
| `edinet_code` | EDINET code of the filer, `E` and five digits |
| `sec_code` | securities code: the four-digit ticker with a check digit appended (`'75130'` is ticker 7513); empty for an unlisted filer |
| `jcn` | corporate number (法人番号) of the National Tax Agency, thirteen digits |
| `name` | name of the filer, in Japanese |
| `fund_code` | the fund code, where the document is filed for an investment fund; empty for an operating company's report |
| `ordinance`, `form` | the ordinance and form codes, which together say which prescribed form it was filed on |
| `doc_type` | the kind of document: `'120'` annual securities report, `'130'` its correction, `'140'` quarterly, `'150'` its correction, `'160'` semi-annual, `'170'` its correction (`REPORT_TYPES`) |
| `period_start`, `period_end` | the period the index says the document reports on (see [The index's period is the agency's](#the-periods-are-resolved-and-not-reported)) |
| `submitted` | when the document was submitted |
| `filed_year` | the year it was submitted -- the year of `index_date` only for a filing without a submission time -- and the partition column |
| `index_date` | the calendar day whose index the row came from, which is what a run collects by |
| `description` | the description shown on the disclosure site, in Japanese |
| `parent` | the document this one belongs to, where it is part of another |
| `withdrawn` | `'1'` a withdrawal, `'2'` a document that was withdrawn, `'0'` neither |
| `edited` | whether an official of a finance bureau has corrected the information held about it |
| `disclosed` | whether an official has withheld it from disclosure |
| `legal` | `'1'` on public view, `'2'` in the extension period, `'0'` past both |
| `has_xbrl`, `has_csv`, `has_english` | `'1'` where the document carries XBRL, where the agency converted it to CSV, and where the filer lodged an English version (a document in its own right, not a translation) |

**`has_csv` is the column the module turns on.** The agency converts the XBRL of a filing where it can and says per filing whether it did, and a filing whose flag is not `'1'` contributes nothing to the table of facts -- exactly as an ESEF filing with no `json_url` does.

**Four states travel with a filing and all four are carried rather than acted on**: `withdrawn`, `edited`, `disclosed` and `legal`. A filing in the last state of `legal` is still named by an index collected earlier and can no longer be fetched, which is the thing this module's Bronze layer exists to survive.

**A filing is one row however often it is listed.** The agency lists a filing again, on the day and in the order of the change, whenever its details are edited -- a form code corrected the next day, a parent added. The table keeps the last listing, which is the filing's present account. Keeping every listing read the archive of such a filing once per listing and multiplied its facts in every table built on this one.

**Eight of the twenty-nine fields an index carries are deliberately absent.** `issuerEdinetCode`, `subjectEdinetCode`, `subsidiaryEdinetCode` and `currentReportReason` belong to the large-holding report, the takeover bid and the extraordinary report, and are empty on every document this module is about. `seqNumber` numbers a row within the day's index and means nothing once two days are in one table; `opeDateTime` is when an official touched the record, which `edited` already says; and `pdfFlag` and `attachDocFlag` offer forms of the filing this module does not read.

### `facts` -- one row per fact of the conversion

Partitioned by `filed_year`, the year the filing was made -- which only moves forward, as a financial year does not, since one report states five of them. Key: `filing` and `fact`.

| column | meaning |
| --- | --- |
| `edinet_code` | EDINET code of the filer |
| `filing` | document management number of the filing |
| `filed_year` | the year the filing was submitted, the partition column |
| `fact` | an identifier made for the fact: `filing:document:concept:context:occurrence` |
| `concept` | the element as the conversion writes it, prefix and name (`jppfs_cor:Assets`) |
| `label` | the Japanese name of the element |
| `context` | the context identifier, verbatim (`CurrentYearDuration`, `Prior1YearInstant_NonConsolidatedMember`) |
| `relative` | 相対年度, the period relative to the filing in words (`当期`, `前期`, `前々期`, `提出日時点`) |
| `basis` | 連結・個別: `連結` consolidated, `個別` the filer alone, `その他` other |
| `shape` | `期間` a flow over a period, `時点` a balance at a moment |
| `prd_start` | first day of the period, resolved; empty for an instant |
| `prd_end` | last day of the period or the day of an instant, resolved (see [How it works](#the-periods-are-resolved-and-not-reported)) |
| `dimensions` | the members of the context, the part of `context` after the period (`NonConsolidatedMember`) |
| `unit` | `JPY`, `shares`, `pure`, `JPYPerShares`, `tCO2e` ...; empty where the element has none |
| `value` | the number, as reported and without scaling; null where there is none |
| `text` | everything that is not a number |
| `doc_form` | the document of the filing the fact came from: `jpcrp030000-asr` the annual securities report, `jpaud-aai-cn` the audit report |

The columns a fact shares with the other two regimes are spelled as they spell them -- `filing`, `fact`, `concept`, `prd_start`, `prd_end`, `dimensions`, `unit`, `value`, `text` -- so that a module combining them is combining tables of one shape rather than translating between them. As for the other regimes, `value` is no measure.

**Three columns have no counterpart in `esef` or `sec`.** `label` is the Japanese name of the element, which the conversion states beside it and which is the only thing in these tables that says what a concept means to a reader who does not know the taxonomy; the ESEF labels live in an extension taxonomy the package does not read, and the Commission publishes none. `dimensions` carries the members of a context, which the company files of the Commission do not have at all. And `basis` is 連結・個別, the column that says whether a figure is the group's or the parent company's.

**A fact can have a unit and no number.** `value` holds a number and `text` everything else. The conversion writes `－` where a figure does not apply, forty-eight times in one report of 1,086 facts, and that becomes a null `value` rather than a zero.

**An extension is told from the taxonomy by its prefix.** `concept` is the element identifier as the conversion writes it, prefix and name. Four prefixes appear: `jppfs_cor` is the financial statements taxonomy, `jpcrp_cor` the disclosure ordinance, `jpdei_cor` the document and entity information, and a prefix built from the form and the filer's own EDINET code -- `jpcrp030000-asr_E03270-000` -- marks a concept the filer created for its own report.

**`fact` needs an occurrence number.** The element and the context do not identify a fact: a figure is often tagged in several places in a report, and the number of employees appears three times under one context in the same report. The document in the identifier is the form the archive names it by. A fund manager files one document per series of a fund under one form, with the series as the last part of the file name (`…_G03898-001`, `…_G03898-002`), and the same element in two series has two different values. For such a document the series is part of the identifier, as `form_series`. The main document of a filing, `-000`, which is every company's own report, is named by its form alone, so that the identifiers a company report already had do not change.

#### Consolidated and non-consolidated

**A Japanese report states its figures twice**, and no reader of this table should take the first row that matches. The consolidated accounts carry no dimension; the parent company's own carry `NonConsolidatedMember`. Kanagawa Chuo Kotsu's annual report states earnings per share of 294.99 for the group and 236.58 for the parent in the same table of the same report.

A filer that prepares no consolidated statements states its accounts under the member alone, so a query demanding an empty dimension returns nothing at all for it. Which it is, is in the filing: `jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI`. The agency's list of filers carries a flag of its own and the two can disagree -- for Kojima they do, the list saying the filer has consolidated accounts and the filing saying it prepared none.

A further member on top of the consolidation one is a breakdown and not a statement line. The statement of changes in equity tags the net assets once per component of equity, so one report states `NetAssets` under `NonConsolidatedMember_CapitalStockMember`, `..._RetainedEarningsMember` and seven other members besides the total.

### `filers` -- one row per filer the agency lists

From the zip the agency publishes on the disclosure site. Key: `edinet_code`. It is the counterpart of the Commission's list of registrants and, like it, is how a lake that wants companies by name or by ticker finds the codes to collect them by. The list as published in November 2026 is 11,467 rows, of which 3,816 are listed; the figures below are of that snapshot and the agency revises it continually.

| column | meaning |
| --- | --- |
| `edinet_code` | EDINET code |
| `kind` | the kind of filer (domestic legal person, foreign company, individual ...), in Japanese |
| `listed` | `上場` listed, `非上場` not |
| `consolidated` | `有` reports consolidated accounts, `無` not, as the agency records it |
| `capital` | stated capital in millions of yen, as published (text) |
| `year_end` | the day the financial year ends, as the agency writes it (`'5月31日'`) |
| `name`, `name_en`, `name_kana` | the name in Japanese, in Latin script, and its reading in katakana |
| `address`, `industry` | address and the agency's own industry classification, in Japanese |
| `sec_code` | securities code, four-digit ticker plus check digit |
| `jcn` | corporate number, thirteen digits |

**1,633 of the 3,816 listed filers do not close in March**, and a comparison that assumes the Japanese financial year ends there is wrong for every one of them. `year_end` is carried as the agency writes it and not as a date, there being no year in it. It is also why the field is text and not a parsed date: among those listed filers the agency writes the end of March two ways, `3月31日` for 2,029 of them and `3月末日` for 139, and thirteen more close on `3月20日`.

`sec_code` is non-blank for exactly the 3,816 filers the list marks as listed. `jcn` is non-blank for 7,289 of the rows -- an individual filer has none.

### `issues` -- one row per issue found

Written by `update_silver_edinet_quality`; see [Data quality](#data-quality). Columns `edinet_code`, `filed_year` (partition), `filing`, `fact` and `concept` (empty where the issue is about the filing as a whole), `issue` (the code of the check) and `checked` (the day of the run).

### Joining to other modules

- **To SEC and ESEF:** module `financialreport` puts the facts and filings of this module beside those of `esef` and `sec` in one table of facts and one of filings, keyed by source; see its [README](../combined/README.md). It reads this module and this module does not read it, so these tables stand on their own. A Japanese row there carries the EDINET code and the corporate number, and a `framework` of `JP-GAAP` or `IFRS` taken from the taxonomy where the taxonomy settles it and from the standard the filing states otherwise. The catalogue of measures, `financialreport_measures.csv`, has rows for the Japanese IFRS taxonomy for all eight of its measures and rows for Japan GAAP for all but the equity attributable to the parent, for which Japan GAAP has no element. A fact whose period could not be resolved is not carried there; `PERIOD_UNRESOLVED` counts them here.
- **To an LEI (legal entity identifier):** possible for some filers, through the corporate number `jcn`; see [The corporate number reaches the LEI](#the-corporate-number-reaches-the-lei).

## Common tasks

**Find a filer's EDINET code, without a key.** The list of filers is a zip on the disclosure site and needs no key, which is why a lake can name its filers before it can collect a filing from any of them. `collect_filers` stores it in the Bronze layer and `build_filers` reads it into a DataFrame:

```python
edinet.collect_filers("paths.yaml")                    # english=True for English names
filers = edinet.build_filers(spark, "paths.yaml")
filers.where("sec_code = '75130'").select("edinet_code", "name", "name_en").show()
```

Once `update_silver_edinet` (or `update_edinet`) has run, the same list is the stored table `edinet.connect_to_edinet(spark, "paths.yaml", "filers")`.

**Collect the history.** A run that wants more than the last month passes the day it wants to start from. The index API answers one calendar day per request, so a decade is some three thousand six hundred requests plus one per filing converted, and it is the only part of this module with a deadline on it:

```python
from fincommons.financialreport.edinet import earliest_retrievable

edinet.update_edinet(spark, "paths.yaml", start=earliest_retrievable())
```

**Run the steps apart.** `update_bronze_edinet` collects the indexes and the filer list, `collect_document` one filing, `update_silver_edinet` rebuilds the tables. `update_edinet(..., documents=False)` brings the indexes up to date and reads whatever was collected before:

```python
edinet.update_bronze_edinet("paths.yaml", start="2026-11-01", end="2026-11-30")
edinet.collect_document("paths.yaml", "S100VWVY")      # kind="csv" by default
edinet.update_silver_edinet(spark, "paths.yaml")
```

**Take the annual reports of operating companies only.** Most of what an index names is not a set of statements. Every report is accompanied by a 確認書, the management confirmation, which the index codes `'135'`; of the 2,118 documents filed on 13 November 2026, 868 were those and 894 were semi-annual reports. And a report filed for an investment fund carries a `fund_code` and reports on the fund rather than on the filer, which is most of the semi-annual reports of any given day. A lake after the statements of operating companies wants the report codes *and* an empty fund code:

```python
annual = filings.where(
    "doc_type IN ('120', '130') AND (fund_code IS NULL OR fund_code = '')")
```

**Take one basis, consolidated or not.** Consolidated figures carry no dimension; the parent's own carry `NonConsolidatedMember`. Use the filing's own statement of which it prepared:

```python
consolidated = facts.where("dimensions IS NULL")
parent_only = facts.where("dimensions = 'NonConsolidatedMember'")
prepares = facts.where(
    "concept = 'jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI'")
```

**Check the figures and see what was found.**

```python
from fincommons.dataquality import describe_check, issue_counts, with_issues

edinet.update_silver_edinet_quality(spark, "paths.yaml")

issues = edinet.connect_to_edinet(spark, "paths.yaml", "issues")
issue_counts(issues, dataset="edinet").show()     # one row per check
print(describe_check("UNIT_NOT_A_RATIO"))         # what a code means

facts = edinet.connect_to_edinet(spark, "paths.yaml", "facts")
with_issues(facts, issues, ["edinet_code", "filing", "fact"])
```

**Look up what a column means.**

```python
import fincommons

print(fincommons.describe_column("relative", dataset="edinet"))
```

## Running it on a schedule

`update_edinet` is the call a job scheduler makes. No module of the package opens a Spark session, so a scheduler runs a script around that call, with the key in the environment or the vault:

```python
# update-edinet.py -- the script a job scheduler runs
# FINCOMMONS_EDINET_KEY or FINCOMMONS_VAULT must be set in its environment.
import os
import sys

# Spark's Python workers must use this interpreter; set before the session.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from fincommons.financialreport import edinet
from fincommons.log import configure

configure("INFO")                                        # log to standard error

builder = (SparkSession.builder
           .master("local[4]")
           .appName("fincommons-edinet")
           .config("spark.driver.memory", "4g")
           .config("spark.local.dir", "/data/spark-tmp")  # not a tmpfs /tmp
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    edinet.update_edinet(spark, "paths.yaml")            # the last thirty days
    edinet.update_silver_edinet_quality(spark, "paths.yaml")
finally:
    spark.stop()
```

What a run does, in order, and what happens when part of it fails:

1. **Indexes** (`update_bronze_edinet`). One request per calendar day from `start` (default thirty days before `end`) to `end` (default today), plus the list of filers (`filers=True`, no key). A day the Bronze layer already holds is not asked for again unless `force=True`; an index of a past day is revised by the agency once a day, so a lake that wants the revisions asks again, and one that wants the history does not. A day older than `earliest_retrievable()` is counted as expired rather than requested, and a warning says where the window cut the range off.
2. **Filings.** The table of filings is built from every index held, and every filing with `has_csv = '1'` whose conversion is not yet in the Bronze layer is collected (`outstanding`). A filing the agency never converted is never asked for. A filing it has no CSV for is counted as absent.
3. **Tables** (`update_silver_edinet`). The tables of filings and of filers are rebuilt in full; the table of facts is rewritten a year at a time, by the year a filing was submitted, so a run that collects this month's filings rewrites this year's partition and leaves the others alone. A filing listed with a conversion and not collected is counted and reported, because that is a gap a run can close.
4. **Checks** (`update_silver_edinet_quality`, optional). With `codes`, only the filed years those filers touch are replaced and every other year is carried forward.

A request turned away for the moment (`429`, `500` or `503`) is made again up to `RETRIES` (5) times, waiting at most `MAX_WAIT` (120) seconds. An index that fails is logged with its traceback and the other days carry on; at the end the stage raises `RuntimeError` naming up to ten failed days, so the scheduler sees a non-zero exit code. An archive that cannot be read is logged and counted and does not fail the run. The run is idempotent: running it again asks only for what is not held.

## How it works

### What arrives from the agency

**The agency converts each filing's XBRL to CSV itself, so this module needs no XBRL parser.** The API publishes three things: an index of what was filed on a given calendar day, the filing itself, and -- this is what makes a module like this one possible -- **the agency's own conversion of the filing's XBRL into CSV**. This module reads that conversion, as [`sec`](../sec/README.md) reads the company files the Commission publishes and [`esef`](../esef/README.md) the xBRL-JSON of filings.xbrl.org. None of the three parses XBRL, and none needs a parser installed.

What arrives for a filing is a zip holding one CSV per document of it -- the report, and the audit report, which the agency converts as well -- written in UTF-16 with a byte-order mark and separated by tabs. Nine columns, and the first line is the header:

```
要素ID  項目名  コンテキストID  相対年度  連結・個別  期間・時点  ユニットID  単位  値
```

The list of filers, by contrast, is a CSV in the Japanese industrial encoding (`cp932`) rather than UTF-8, and its first line is the day it was produced and the number of rows, not the header.

### A filing leaves EDINET

**The Bronze layer is the only lasting copy of what it holds; a year not collected is a year lost.** This is the one way the module differs in kind from the other two, and everything about its Bronze layer follows from it. A securities report is on public view for five years and retrievable for five more, ten in all (`RETENTION_YEARS`), and the specification says of what is older that it cannot be obtained: 「既に 10 年を経過した書類については取得できません」. The API answers for no day before 4 January 2013 at all (`FIRST_DATE`).

No parameter of the API reaches past that, and a request for a day outside the window returns an empty index rather than an error -- which is how a lake that believes it is collecting ten years of history quietly collects what is left of it. `earliest_retrievable()` turns the retention into a date and `update_bronze_edinet` says where the window cuts a requested range off.

EDGAR keeps a filing for ever and filings.xbrl.org keeps its archive. EDINET does not. **The Bronze layer is therefore not a convenience that saves a download**, and that is also why `collect_document` refuses a body that is not an archive instead of storing it: the agency signals a failure here by the content type rather than by the status, and an error page written into the Bronze layer would be the only copy of a filing that no longer exists.

The Bronze layer holds `indexes` (one JSON file per calendar day, the unit the index API answers in), `documents` (one archive per filing collected), `collections` (a small record of when each was collected and what the server said, with the key struck out of the address) and `filers` (the agency's list).

### What the conversion carries and what it leaves out

**Unlike the company files of the Commission, the conversion carries the whole of a report.** Worth stating plainly, because it decides what the data can be asked: every fact, with its dimensions, its text and the filer's own concepts.

| in the conversion | not in the conversion |
| --- | --- |
| every fact of the report, dimensioned or not | the dates of a context -- see below |
| the filer's own concepts, told apart by their prefix | the presentation and calculation linkbases |
| the Japanese label of every element | an English label of anything but the filer's name |
| the narrative facts and the text blocks | the rendered document, which is a separate form |
| the audit report, converted alongside | |

Kojima's annual report for the year to 31 August 2026 comes to 1,099 facts: 1,086 of the report and 13 of the audit report, of which 19 are under concepts the filer created for itself and 412 under the financial statements taxonomy.

### The periods are resolved and not reported

**No column of the conversion holds the dates of a fact's context**, and this is the thing to know before using the tables. `コンテキストID` names the period relative to the filing's own -- `CurrentYearDuration`, `Prior4YearDuration`, `FilingDateInstant` -- and `相対年度` says the same in words.

**A filing states its own financial year and the comparative one, and those dates are used as reported.** Two things turn a label into dates, and the better one comes first:

1. The document and entity information states the filer's own dates: `jpdei_cor:CurrentFiscalYearStartDateDEI` and the three beside it (`ANCHOR_ELEMENTS`). Those are used as reported, so the two periods nearly every use of these figures rests on are the filer's own dates rather than arithmetic.
2. Only where no anchor covers the label -- the year before the comparative, and everything further back -- is the period shifted by whole years from the anchor, which is exact only where the filer's years are of equal length (a filer on a 52- or 53-week calendar, or one that moved its year end, has years that are not a whole number of years apart).

A report stating five years of net sales is therefore anchored for two of them and shifted for three.

**A semi-annual report covers a half-year and belongs to a year**, and the two are kept apart. It states the year -- `CurrentFiscalYearStartDateDEI` to `CurrentFiscalYearEndDateDEI`, April 2026 to March 2027 -- and the end of the half-year it covers in `CurrentPeriodEndDateDEI`, September 2026, with the comparative half-year's end in `ComparativePeriodEndDateDEI`. Its half-year figures carry the contexts `InterimDuration` and `InterimInstant` and their `Prior1` forms, and are dated from the start of the year to the end of the half-year and the same a year before; its comparative balance sheet, `Prior1YearInstant`, is the one at the end of the previous year.

Until November 2026 the half-year contexts were left undated and the comparative balance sheet was dated to the end of the comparative half-year, because the period's end and the year's end were read as alternatives for one date: of the 1,171,065 facts of 2,414 semi-annual reports, 851,927 had no date, and 753 still have none: the half-year two years back, which no date the report states covers.

**The dates a filing states are read once for its whole archive.** The document and entity information is in the report, and a document filed with it states none of its own: the auditor's report, and the second and later series of a fund, which share the first series' accounting periods. Each document used to be resolved against its own information alone, so the auditor's report of a semi-annual report had every half-year fact undated, and the later series of a fund whose accounting period is six months long were resolved against the index and shifted by whole years -- their comparative period a year back rather than one period back, in 61 annual reports of funds. A document that states dates of its own still uses them.

**The index's period is the agency's, and it is kept.** For a semi-annual report the index states the year the half-year belongs to, not the half-year: a report on April to September 2026 is listed to 31 March 2027. The table of filings keeps the index's period as the agency states it, which is what `INDEX_PERIOD_DIFFERS` sets against the filing's own; the combined tables of `financialreport` take the period the filing states it reports on.

A label whose shape is not recognised yields no dates at all rather than a guess, and `context` and `relative` are carried beside the dates so that what each one rested on can always be read. The XBRL instance of the filing carries the true dates for every context; a lake that needs them for the older years needs the instance and a parser, which is not this module.

### The API key

**The API reports a refused key with HTTP status 200, so the module reads the status inside the body.** The API refuses a request that carries no registered key, and reports the refusal with HTTP status 200 and the real status inside the body, so a run that trusted the status would store the refusal as data. `fetch_json` reads the body's own status for that reason. A key is obtained by registering at `disclosure2.edinet-fsa.go.jp`; the agency answers a missing key, an unknown key and a key whose subscription is not active with the same 401.

Where the key comes from is [`fincommons.credentials`](../../credentials.py) and nowhere else: the `key` argument, then `FINCOMMONS_EDINET_KEY`, then a file named `edinet` in the directory `FINCOMMONS_VAULT` names.

**The key is never put in `paths.yaml`.** That file says where the tables live; it is committed with a deployment, it is handed to every module, and it is read by R as well -- every Quarto template of the package opens it -- so a key in it would travel with the repository and be readable by anything that renders a factsheet. `api_key` refuses to look there.

**The key travels as a query parameter and not as a header**, so the address of a request carries the secret and the address is the thing a pipeline logs. `redacted` exists for that, and the record of a collection keeps the true shape of the request with the key replaced rather than a key-free address nobody asked for.

The list of filers is the exception to all of this: it is a zip on the disclosure site and needs no key.

### Data quality

**Nothing is dropped and nothing is corrected.** `quality` holds the twenty-six checks the figures are held to, and `update_silver_edinet_quality` writes what they found into a fourth Silver table, `issues`, beside the three the module collects. A check writes a row and leaves the figure where it is: the tables are what the agency published, and a reader who wants to know what is wrong with a figure before using it joins the issues on. A filing with nothing wrong with it has no row at all -- the absence of a code is what says a record passed. The machinery is shared with the other two regimes in [`fincommons.dataquality`](../../dataquality.py); what lives here are the checks themselves, because a check knows the data set it is made of.

| code | finds |
| --- | --- |
| `IMBAL_BALANCE_SHEET`, `IMBAL_TOTALS`, `IMBAL_ASSETS`, `IMBAL_LIABILITIES`, `IMBAL_INCOME`, `IMBAL_ORDINARY`, `IMBAL_PRETAX`, `IMBAL_PROFIT`, `IMBAL_TAX`, `IMBAL_PROFIT_SPLIT` | a statement that does not add up |
| `IMPL_NEGATIVE_ASSETS`, `IMPL_PERIOD` | an implausible figure or period |
| `MISS_ASSETS`, `MISS_REVENUE`, `MISS_PROFIT` | a line looked for and not found |
| `MISS_CONSOLIDATED`, `MISS_FACT_VALUE` | a filing that says it consolidates and states no consolidated figure; a fact with neither a number nor a text |
| `SUMMARY_DISAGREES`, `PERIOD_SHIFT_INEXACT`, `BASIS_CONTRADICTS_DIMENSION`, `UNIT_NOT_A_RATIO` | the four checks found only here (below) |
| `PERIOD_UNRESOLVED`, `INDEX_PERIOD_DIFFERS` | a period that could not be resolved; an index period that differs from the filing's own |
| `DUPL_FACT_VALUE`, `MIXED_UNIT`, `HIGH_EXTENSION_SHARE` | a figure tagged twice; a concept in two units; a filing most of whose figures are tagged with concepts of its own (nothing wrong, but hard to compare) |

**The arithmetic checks are the valuable ones, and two of them exist only here.** As in the other two regimes, they are the relations a statement states on both sides. The two Japanese ones are the lines that make a Japanese profit and loss account what it is: the ordinary result, 経常利益, is the operating result and the recurring items outside it, and the result before tax is the ordinary result and the extraordinary items, 特別損益. Neither relation can be written for an IFRS or a US GAAP filer, because neither has the lines.

Four more checks have no counterpart anywhere in this package, and each falls out of something said further up this README.

- **The filer states its own year twice.** The five-year summary of the front matter and the financial statements both carry the revenue and the total assets of the year the report is about, in different elements of different taxonomies, so they have to agree. `SUMMARY_DISAGREES` is the only check in the package that tests a filing against another part of itself; a company file of the Commission holds one account of a period per filing, and an ESEF report states no summary at all.
- **The period is resolved and not reported.** Anything older than the comparative is shifted by whole years, which is exact only where the filer's years are of equal length -- and the filing states the first and last day of both of its two most recent years, so whether the shift can be trusted is a question the filing itself answers. `PERIOD_SHIFT_INEXACT` asks it, with three days of slack (`YEAR_LENGTH_SLACK`) so that a leap year is not mistaken for a 52-week calendar.
- **The conversion says twice whether a figure is the group's.** 連結・個別 is a column and `NonConsolidatedMember` is a dimension of the context, and the two cannot disagree. Over the reports read while the module was written the correspondence held for every one of 6,752 facts; `BASIS_CONTRADICTS_DIMENSION` is what notices if it ever stops.
- **An element named for a ratio has to be a pure number.** `UNIT_NOT_A_RATIO` is the only check of the four that has found anything in a real report, and it found it in Fujitsu's: `EquityToAssetRatioIFRSSummaryOfBusinessResults` tagged with 1,167.25 and the unit `JPYPerShares`, which is the equity attributable to the owners of the parent per share under the name of a ratio. A reader trusting the name prints an equity ratio of 1,167 per cent. The ratio itself is in the same report under `RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults` as 0.60.

Two things decide what every check sees, and both are settled once rather than by each check.

**The basis.** The figures of a filing are the ones it states for its own period end on the basis the filing itself says it reports on: the consolidated accounts where 連結決算の有無 is set, the parent company's own where it is not. A member on top of the consolidation one is a breakdown and not a statement line, so a fact carrying one is left out -- the statement of changes in equity tags the net assets once per component of equity, and a check that took the first match would be testing a figure against part of itself.

**A missing line is not a zero.** A component of a sum being tested has to be stated, and a difference that cannot be computed is not a difference. Treating an absent line as nothing is what turns a filing that presents its statements differently into a filing that fails arithmetic: an IFRS statement of financial position subtotals its current liabilities and leaves the non-current side to be read as the remainder, and the first draft of these checks reported Fujitsu's liabilities as short by 244,695 million because of it. Only a genuinely optional line is coalesced to nothing -- the extraordinary items a filer may not have had, the result of operations it discontinued -- and each one is named where it is added.

Run over four real annual securities reports -- Kojima, which does not consolidate; Kanagawa Chuo Kotsu twice, consolidated under Japan GAAP; Fujitsu, consolidated under IFRS -- the twenty-six checks report five issues, all five being Fujitsu's mis-tagged ratio.

**The catalogue of checks is data, not code.** What each code means, how severe it is and what to do about it are rows of `data/dataquality_checks.csv` rather than branches in the code, so that the people most able to find a check wrong can read it. Nothing in it is authoritative; `describe_check` prints a row, and a deployment that keeps checks of its own ships a catalogue of the same shape and names it.

### A factsheet on one filer

**`templates/edinet-analyst-factsheet.qmd` renders a factsheet of some twenty pages on one filer, from its own annual securities reports and nothing else.** It is written in R and rendered with Quarto to a PDF, with **Kojima Co., Ltd.** as its example. It reads the Silver tables as Parquet.

| page | what it holds |
| --- | --- |
| at a glance | five years of revenue, the two results, the three cash flows and the employee count |
| the filer | what the agency's list says and what the report says about itself, side by side |
| five-year summary | the filer's own 主要な経営指標等の推移, nothing in it computed here |
| statement of operations | revenue to comprehensive income, with the margins and the cost structure |
| balance sheet | both sides, the equity composed as the filer's standard composes it, and the working capital |
| debt | interest-bearing debt summed from the lines, net debt, leverage and liquidity |
| cash flow | the three flows over five years, and the statement's own detail beside them |
| the workforce | employees over five years, the average age, the length of service and the annual pay |
| the holders | the ten largest by name, the register by class of investor, and the total shareholder return |
| coverage | every line looked for and not found, every figure refused for its unit, and fourteen checks |

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render edinet-analyst-factsheet.qmd
```

The filer is set by its EDINET code in the first chunk (`filer_edinet_code`). The corporate design is in `r-corporate-design-functions-ggplot2.R`, one file for the whole package in `src/fincommons/templates`, three levels up from a template, which the esef and sec templates source as well, so that a factsheet of any regime looks like the others.

Six things are worth knowing before the template is pointed at another filer.

**Two spans, and they are not the same.** The front matter of a Japanese annual securities report carries a five-year summary of the headline figures while its statements carry the year and its comparative. **One report therefore affords five years** of revenue, the ordinary result, total assets, the three cash flows, the employee count and the filer's own ratios, which neither of the other two regimes can do from a single filing. The factsheet keeps the two spans apart and names what each covers rather than padding the short one out with dashes.

**The consolidation basis.** `consolidation` is `"auto"` by default and the filing's own 連結決算の有無 decides it; `"consolidated"` and `"nonconsolidated"` force it. Where the agency's list and the filing disagree the profile page says so rather than preferring one silently. Only ten lines may be taken from the other basis -- the stated capital, the shares issued, the dividend and its payout, the shareholder return, the workforce figures -- because those are properties of the company however it reports, and they are marked ‡ where they are. **Nothing else falls back.** Fujitsu reports its group under IFRS and its parent under Japan GAAP, so its parent's 経常利益 is in the same report, and an unrestricted fallback put 275,236 million of it on the cover of a group factsheet whose revenue is 3,502,971 million.

**A concept is matched on its local name and never on the prefix.** Three of the four prefixes of a conversion are fixed and the fourth is built from the form and the filer's own EDINET code, so an element a filer created sits under that one. Fujitsu's operating profit is `jpcrp030000-asr_E01766-000:OperatingProfitIFRSSummaryOfBusinessResults`: an element of the five-year summary carried by the filer's own prefix, the core taxonomy having no operating profit for an IFRS filer. Matching on the prefix as well would lose it, and lose it silently.

**Every line declares the unit class it accepts**, and a fact of another class is refused and reported on the coverage page. This is not belt and braces: it is Fujitsu's mis-tagged `EquityToAssetRatioIFRSSummaryOfBusinessResults` again (see `UNIT_NOT_A_RATIO` above). A factsheet trusting the name would print an equity ratio of 1,167 per cent, and the ratio is in the same report under `RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults` as 0.60. The same element therefore serves two keys and the unit decides which. The filer's own ratios arrive as fractions besides -- an equity ratio of 57.1 per cent is stated as 0.571 -- and are brought onto percentages.

**No per-share figure is adjusted for a split.** A Japanese filer restates the earlier years of its own five-year summary at its discretion and not always for every year of it: Fujitsu's report states 207.0, 207.0, 207.1, 2,071.1 and 1,739.8 million shares against earnings per share of 924.21, 110.76, 135.59, 120.93 and 254.83, which is four years restated onto the shares after the ten-for-one split of October 2024 and the fifth left on the shares before it. A factor derived from the share counts would divide the four that are already restated, so the year the count moved is named instead and the reader is told the series spans it. This is where the template parts company with the sec factsheet, which does derive such a factor.

**The Japanese text needs a font.** The page is set in the font the corporate design names, which covers no Japanese, and typst sets what a font lacks as empty boxes without saying so. `cjk_fonts` names the fallback it reaches for; any installed family covering JIS X 0208 will do. `labels` sets the tables in English, in the filer's own Japanese, or in both -- charts use the English names whatever it is set to, their font being the graphics device's.

Three sections of the factsheet exist only here, and each follows from what the Cabinet Office ordinance asks a filer for: the workforce, the holders of the shares with the ten largest by name, and the total shareholder return over five years against the index the filer names. That last one and the price-earnings ratio are the only market figures in a document that fetches no market data, both being the filer's own statements.

Fourteen checks close the document, among them one that tests the reading of both year axes at once: the summary and the statements must agree about the year they share. Rendered against three real filers -- Kojima, which does not consolidate; Kanagawa Chuo Kotsu, consolidated under Japan GAAP; Fujitsu, consolidated under IFRS -- thirteen, fourteen and thirteen of the fourteen hold, Fujitsu's one difference being an operating income it presents between its gross profit and its operating result.

The last page is a disclaimer, and the cover says in its first line and in its small print what that page states in full: the document is produced by a template and the filer is an example, no warranty is given for the correctness or the completeness of a figure, nobody who made it is associated with the filer or with the agency, nothing in it is advice, and liability is disclaimed as far as the law allows, in the terms of Articles 7 and 8 of the EUPL the project is licensed under. Whoever renders the factsheet for a purpose of their own is responsible for what they publish.

### The corporate number reaches the LEI

**It does, and this is the only identifier in this family of modules that does.** Neither the Commission nor GLEIF publishes a mapping from a central index key to a legal entity identifier, and ESMA publishes none from an LEI to anything the Commission uses; `financialreport` carries both columns and fills exactly one of them in any row for that reason. Japan is different, and the reason is that the corporate number is not an identifier of its own:

> The corporate number of the National Tax Agency is a check digit followed by the twelve-digit 会社法人等番号 of the commercial register, and GLEIF records that register number as the registration authority entity id of an LEI issued to a Japanese company.

So the join is arithmetic rather than a lookup table. Drop the leading check digit, hyphenate the rest as the register writes it, and ask GLEIF:

```python
jcn = "1020001071491"                     # JCN of 富士通株式会社
body = jcn[1:]                            # 020001071491
registered_as = f"{body[:4]}-{body[4:6]}-{body[6:]}"   # 0200-01-071491
# GLEIF: entity.registeredAs == "0200-01-071491" under RA000412
# -> LEI 3538000246DHJLRTUZ24, 富士通株式会社
```

`RA000412` is the 登記 register of the Legal Affairs Bureau. Every match found while this was checked named that one register and no other, which is what makes the join exact: it is two spellings of one register number and not a name comparison.

**How far it reaches.** Of a random sample of 200 of the 3,810 listed filers the agency's list gives a thirteen-digit corporate number for, **27 reached an LEI -- 13.5 per cent**. Ten of the misses were then looked up by name in GLEIF and not one of them had an LEI under any spelling, so the join is not failing on them: most Japanese listed companies simply are not in the LEI system, which is a population fact and not a defect of the mapping.

Of the 27 matches, 26 carry a legal name that agrees with the filer's. The twenty-seventh is the point of joining on a number rather than on a name: EDINET has E24558 as 株式会社ミライト・ワン and GLEIF still has the LEI under 株式会社ミライト・ホールディングス, the name before the group renamed itself. A number survives a rename and a name match does not.

**What it is good for, and what it is not.** It is enough to put a Japanese filer beside a European one **where both exist**, which is the thing the combined tables cannot otherwise do at all. It is not enough to key the combined tables by: seven of eight listed Japanese filers would carry no LEI, and an identifier that is null for most of a population is not the identifier that population is keyed by. The corporate number stays the key for Japan, as the central index key does for the Commission, and the LEI is a column that is filled where it can be.

Nothing in this module derives it. The join needs GLEIF, which is [`entitymasterdata`](../../entitymasterdata/README.md), and belongs wherever the regimes are put side by side rather than in the module that reads one of them.

## Reference

### Configuration

The `edinet` section of `paths.yaml`:

| key | holds |
| --- | --- |
| `bronze` | directory of the indexes, archives, collection records and filer list; always a directory |
| `silver` | directory of the Silver tables, or a catalogue schema (see below) |

The Silver layer may name a schema in a catalogue instead of a directory, which puts the tables within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the Bronze layer and the record of what was collected are files:

```yaml
edinet:
    bronze: /Volumes/fincommons/edinet/lake/bronze
    silver:
        table: fincommons.financialreport.edinet
        path: /Volumes/fincommons/edinet/lake/silver
```

The tables are then `fincommons.financialreport.edinet.filings`, `.facts`, `.filers` and `.issues`, always Delta. A Silver layer addressed by path is stored as Parquet unless `file_format="delta"` is passed, and a table that exists keeps its format; see `fincommons.storage.silver_format`.

The API key is not configured here; see [The API key](#the-api-key).

### Public functions

All are importable from `fincommons.financialreport.edinet`.

| function | what it does |
| --- | --- |
| **Pipeline** | |
| `update_edinet(spark, config_path, start=None, end=None, key=None, documents=True, file_format=None)` | collect the indexes and the conversions, and rebuild the tables |
| `update_bronze_edinet(config_path, start=None, end=None, key=None, filers=True, force=False)` | collect the indexes of a range of days, and the list of filers |
| `update_silver_edinet(spark, config_path, days=None, file_format=None, chunk_rows=CHUNK_ROWS, filers=True)` | rebuild `filings`, `facts` and `filers` from the Bronze layer |
| `update_silver_edinet_quality(spark, config_path, codes=None, file_format=None)` | run the checks and rebuild `issues`; `codes` are EDINET codes |
| **Read** | |
| `connect_to_edinet(spark, config_path, table="filings")` | open `"filings"`, `"facts"`, `"filers"` or `"issues"` |
| `earliest_retrievable(today=None)` | the oldest day the API can still be asked about |
| `find_issues(spark, config_path, codes=None)` | run the checks and return the issues without writing them |
| `collected_indexes(config_path)` | the days whose indexes the Bronze layer holds |
| `read_filings(config_path, days=None)`, `read_filers(config_path)` | the rows of the indexes and of the filer list, in plain Python |
| `build_filings(spark, config_path, days=None)`, `build_filers(spark, config_path)`, `build_facts(spark, config_path, filings, ...)` | the tables as DataFrames, before writing |
| `outstanding(config_path, filings)` | the filings with a conversion that were not collected |
| **Collect one piece** | |
| `collect_index(config_path, day, key=None, force=False)` | one day's index; returns `'collected'`, `'held'` or `'expired'` |
| `collect_document(config_path, doc, key=None, kind="csv", force=False)` | one form of one filing (`DOCUMENT_KINDS`: `main`, `pdf`, `attached`, `english`, `csv`); returns `'collected'`, `'held'` or `'absent'` |
| `collect_filers(config_path, english=False)` | the list of filers; no key |
| `fetch(url, params=None, headers=None)`, `fetch_json(url, params=None)` | one request with the retries; `fetch_json` raises on the error hidden in a 200 |
| `filings_from_index(document, day)` | the rows of one day's index |
| **Parse (plain Python, no Spark)** | |
| `facts_from_archive(data, filing, period_start=None, period_end=None, ...)`, `facts_from_csv(text, filing, ...)` | the facts of one conversion |
| `split_context(context)`, `period_of(context, period_start, period_end, filed=None, anchors=None)`, `anchors_of(rows)` | how a context label becomes dates |
| `is_extension(concept)` | whether a concept is one the filer created |
| **Layout** | |
| `create_directory_tree(config_path)` | create the local directories |
| `path_bronze_*`, `path_silver_*`, `ref_silver_*`, `path_index_record` | where each part of the lake lives |

Constants: `BASE_URL`, `INDEX_URL`, `DOCUMENT_URL`, `FILERS_URL`, `FILERS_URL_EN`, `SOURCE`, `DOCUMENT_KINDS`, `REPORT_TYPES`, `ANNUAL_TYPES`, `RETENTION_YEARS`, `FIRST_DATE`, `FILERS_ENCODING`, `RETRIES`, `MAX_WAIT`, `CHUNK_ROWS`, `CSV_COLUMNS`, `CSV_ENCODING`, `CSV_DELIMITER`, `CSV_FOLDER`, `FACT_COLUMNS`, `NOT_APPLICABLE`, `TAXONOMY_PREFIXES`, `ANCHOR_ELEMENTS`. Schemas: `filings_schema`, `facts_schema`, `filers_schema`, `issues_schema`. The other constants of the checks (`CHECKS`, `CONCEPTS`, `TOLERANCE` ...) are in `fincommons.financialreport.edinet.quality`, which the package imports by name only, because it declares a `TAXONOMY_PREFIXES` of its own.

### Data contracts and catalogues

- Data contracts in the Open Data Contract Standard: `data/contract_edinet_silver_filings.yaml`, `..._facts.yaml`, `..._filers.yaml` and `..._issues.yaml`, which `data/product_edinet.yaml` names as the output ports of the module. They are the one place the columns are described -- `fincommons.describe_column("relative", dataset="edinet")` reads its answer from there -- and the declared schemas of the module are held to them by the tests.
- A contract states the key of its table (the document management number; that and the fact; the EDINET code) and the rules the table is held to: one row per document, fact, filer and issue, the document management number, the EDINET code, the corporate number and the securities code in their forms, the three flags '0' or '1', a period that ends after it begins, `filed_year` the year the document was submitted, no fact both a number and a text, and a unit for every number. `fincommons.contracts.check_quality` runs them; the checks that name each figure at fault are those of [Data quality](#data-quality), which the contract of the facts points at.
- The check catalogue: `data/dataquality_checks.csv`.

### Source and terms

The index, the filings and the list of filers are downloaded from the agency when the pipeline runs. **No filing is shipped with fincommons**: the package holds the code that collects them and one real conversion, cut down, in its tests. The documents are publications of the Financial Services Agency and reuse of EDINET content is permitted under the terms the agency publishes on the disclosure site.

## Troubleshooting

- **`RuntimeError: ... answered 401 in the body of a 200`** → the key is missing, unknown or its subscription is not active (the agency answers all three the same) → check at `disclosure2.edinet-fsa.go.jp` that the key was issued and is active, and that `FINCOMMONS_EDINET_KEY` or `FINCOMMONS_VAULT` is set in the environment the job runs in.
- **A key placed in `paths.yaml` is not found** → `api_key` deliberately never looks there → use the argument, the environment variable or the vault.
- **A range of old days collects nothing** → the days are older than `earliest_retrievable()`; the API answers them with an empty index → nothing can be done for them; collect early.
- **`RuntimeError: The Bronze layer holds no index`** → `update_silver_edinet` ran before any index was collected → run `update_bronze_edinet` (needs the key) first.
- **`RuntimeError: No filing of the table has a conversion in the Bronze layer`** → indexes were collected but no conversion was (for instance only `documents=False` runs on a fresh lake) → run `update_edinet` with `documents=True` or `collect_document` first.
- **`RuntimeError: The Bronze layer holds no list of filers`** → run `collect_filers`, which needs no key.
- **A query for consolidated figures returns nothing for a filer** → it prepares no consolidated statements and states its accounts under `NonConsolidatedMember` → see [Consolidated and non-consolidated](#consolidated-and-non-consolidated).
- **A fact has a unit but a null `value`** → the conversion wrote `－`, the figure does not apply → read `text`.
- **`prd_start`/`prd_end` are null** → the context label was not recognised, so no date was guessed → read `context` and `relative`; `PERIOD_UNRESOLVED` counts these.
- **The factsheet shows empty boxes instead of Japanese** → no font covering JIS X 0208 is installed → install one or set `cjk_fonts`.
