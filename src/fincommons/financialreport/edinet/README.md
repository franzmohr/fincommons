# EDINET filings

## Introduction

Module `edinet` brings the tagged figures of the filers of Japan's Financial Services Agency into the data lake, fact by fact. EDINET -- 金融商品取引法に基づく有価証券報告書等の開示書類に関する電子開示システム -- is the agency's filing system, the counterpart of EDGAR, and it has required the financial statements of a securities report to be tagged in XBRL since 2008.

Its API publishes three things: an index of what was filed on a given calendar day, the filing itself, and -- this is what makes a module like this one possible -- **the agency's own conversion of the filing's XBRL into CSV**. This module reads that conversion, as [`sec`](../sec/README.md) reads the company files the Commission publishes and [`esef`](../esef/README.md) the xBRL-JSON of filings.xbrl.org. None of the three parses XBRL, and none needs a parser installed.

What arrives for a filing is a zip holding one CSV per document of it -- the report, and the audit report, which the agency converts as well -- written in UTF-16 with a byte-order mark and separated by tabs. Nine columns, and the first line is the header:

```
要素ID  項目名  コンテキストID  相対年度  連結・個別  期間・時点  ユニットID  単位  値
```

### A filing leaves EDINET

This is the one way the module differs in kind from the other two, and everything about its Bronze layer follows from it. A securities report is on public view for five years and retrievable for five more, ten in all, and the specification says of what is older that it cannot be obtained: 「既に 10 年を経過した書類については取得できません」. No parameter of the API reaches past that, and a request for a day outside the window returns an empty index rather than an error -- which is how a lake that believes it is collecting ten years of history quietly collects what is left of it. `earliest_retrievable()` turns the retention into a date and `update_bronze_edinet` says where the window cuts a requested range off.

EDGAR keeps a filing for ever and filings.xbrl.org keeps its archive. EDINET does not. **The Bronze layer is therefore not a convenience that saves a download, it is the only lasting copy**, and a year not collected is a year lost rather than a year postponed. That is also why `collect_document` refuses a body that is not an archive instead of storing it: the agency signals a failure here by the content type rather than by the status, and an error page written into the Bronze layer would be the only copy of a filing that no longer exists.

### What the conversion carries and what it leaves out

Worth stating plainly, because it decides what the data can be asked. Unlike the company files of the Commission, which hold the undimensioned numeric facts only, the agency's conversion carries **the whole of a report**: every fact, with its dimensions, its text and the filer's own concepts.

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

Two things turn a label into dates, and the better one comes first. **A filing states its own financial year and the comparative one among its facts**, in the document and entity information: `jpdei_cor:CurrentFiscalYearStartDateDEI` and the three beside it. Those are used as reported, so the two periods nearly every use of these figures rests on are the filer's own dates rather than arithmetic. Only where no anchor covers the label -- the year before the comparative, and everything further back -- is the period shifted by whole years from the anchor, which is exact only where the filer's years are of equal length. A report stating five years of net sales is therefore anchored for two of them and shifted for three.

A label whose shape is not recognised yields no dates at all rather than a guess, and `context` and `relative` are carried beside the dates so that what each one rested on can always be read. The XBRL instance of the filing carries the true dates for every context; a lake that needs them for the older years needs the instance and a parser, which is not this module.

### Source and terms

The index, the filings and the list of filers are downloaded from the agency when the pipeline runs. **No filing is shipped with fincommons**: the package holds the code that collects them and one real conversion, cut down, in its tests. The documents are publications of the Financial Services Agency and reuse of EDINET content is permitted under the terms the agency publishes on the disclosure site.

### The API key

The API refuses a request that carries no registered key, and **reports the refusal with HTTP status 200 and the real status inside the body**, so a run that trusted the status would store the refusal as data. `fetch_json` reads the body's own status for that reason. A key is obtained by registering at `disclosure2.edinet-fsa.go.jp`; the agency answers a missing key, an unknown key and a key whose subscription is not active with the same 401.

Where the key comes from is [`fincommons.credentials`](../../credentials.py) and nowhere else: the `key` argument, then `FINCOMMONS_EDINET_KEY`, then a file named `edinet` in the directory `FINCOMMONS_VAULT` names.

```bash
export FINCOMMONS_VAULT=/run/secrets        # one file per key, mode 600
printf '%s' "$EDINET_KEY" > /run/secrets/edinet
```

**The key is never put in `paths.yaml`.** That file says where the tables live; it is committed with a deployment, it is handed to every module, and it is read by R as well -- every Quarto template of the package opens it -- so a key in it would travel with the repository and be readable by anything that renders a factsheet. `api_key` refuses to look there.

The key travels as a **query parameter** and not as a header, so the address of a request carries the secret and the address is the thing a pipeline logs. `redacted` exists for that, and the record of a collection keeps the true shape of the request with the key replaced rather than a key-free address nobody asked for.

The list of filers is the exception to all of this: it is a zip on the disclosure site and needs no key, which is why a lake can name its filers before it can collect a filing from any of them.

## Setting up

The paths of the data lake are read from the configuration file, which needs an `edinet` section with the keys `bronze` and `silver`:

```yaml
edinet:
    bronze: /data/edinet/bronze
    silver: /data/edinet/silver
```

The Silver layer may name a schema in a catalogue instead of a directory, which puts the tables within reach of Unity Catalog's grants and lineage. It then needs a `path` beside the `table`, because the Bronze layer and the record of what was collected are files:

```yaml
edinet:
    bronze: /Volumes/fincommons/edinet/lake/bronze
    silver:
        table: fincommons.financialreport.edinet
        path: /Volumes/fincommons/edinet/lake/silver
```

The tables are then `fincommons.financialreport.edinet.filings`, `.facts` and `.filers`, always Delta.

## The module in five minutes

```python
from fincommons.financialreport import edinet

# The whole pipeline: collect the indexes of a range of calendar days, then
# the conversions of the filings those indexes say the agency converted, then
# rebuild the tables. Thirty days back by default, which is what keeping a
# lake up to date costs.
edinet.update_edinet(spark, "paths.yaml")

# What one filer reported. The key is the EDINET code, the letter E and five
# digits; 7513 is Kojima's securities code and E03270 is its EDINET code.
filings = edinet.connect_to_edinet(spark, "paths.yaml", "filings")
facts = edinet.connect_to_edinet(spark, "paths.yaml", "facts")

revenue = (
    facts.where("EDINETCODE = 'E03270'")
         .where("CONCEPT = 'jpcrp_cor:NetSalesSummaryOfBusinessResults'")
         .where("DIMENSIONS = 'NonConsolidatedMember'")
         .select("PRDSTART", "PRDEND", "VALUE")
         .orderBy("PRDEND"))
```

A filer is found by name, by ticker or by industry in the table of filers, which needs no key to collect:

```python
edinet.update_bronze_edinet("paths.yaml", filers=True, start="2026-11-01")
filers = edinet.connect_to_edinet(spark, "paths.yaml", "filers")
filers.where("SECCODE = '75130'").select("EDINETCODE", "NAME", "NAMEEN").show()
```

The three steps run on their own where a lake wants them apart -- `update_bronze_edinet` for the indexes and the filer list, `collect_document` for one filing, `update_silver_edinet` for the tables -- and `update_edinet(..., documents=False)` brings the indexes up to date and reads whatever was collected before.

**Collecting the history.** A run that wants more than the last month passes the day it wants to start from. The index API answers one calendar day per request, so a decade is some three thousand six hundred requests plus one per filing converted, and it is the only part of this module with a deadline on it:

```python
from fincommons.financialreport.edinet import earliest_retrievable

edinet.update_edinet(spark, "paths.yaml", start=earliest_retrievable())
```

## The Silver tables

### `filings`

One row per document the collected indexes name, partitioned by `filed_year`. `filing` is the 書類管理番号, the document management number the API addresses a filing by and the key of everything else here; `index_date` is the calendar day whose index the row came from, which is what a run collects by.

`has_csv` is the column the module turns on: the agency converts the XBRL of a filing where it can and says per filing whether it did, and a filing whose flag is not `'1'` contributes nothing to the table of facts -- exactly as an ESEF filing with no `json_url` does. `doc_type` says what kind of document it is, `'120'` being the annual securities report; see `REPORT_TYPES`.

Four states travel with a filing and all four are carried rather than acted on: `withdrawn` marks a filing taken back, `edited` one an official of a finance bureau has corrected, `disclosed` one withheld from disclosure, and `legal` whether it is on public view, in the extension period, or past both. A filing in the last state is still named by an index collected earlier and can no longer be fetched, which is the thing this module's Bronze layer exists to survive.

Eight of the twenty-nine fields an index carries are deliberately absent, and the schema's docstring says which and why.

### `facts`

One row per fact of the conversion, partitioned by `filed_year`, the year the filing was made -- which only moves forward, as a financial year does not, since one report states five of them. The columns a fact shares with the other two regimes are spelled as they spell them, so that a module combining them is combining tables of one shape rather than translating between them.

Three columns have no counterpart in `esef` or `sec`. `label` is the Japanese name of the element, which the conversion states beside it and which is the only thing in these tables that says what a concept means to a reader who does not know the taxonomy. `dimensions` carries the members of a context, which the company files of the Commission do not have at all. And `basis` is 連結・個別, the column that says whether a figure is the group's or the parent company's.

`value` holds a number and `text` everything else, and **a fact can have a unit and no number**: the conversion writes `－` where a figure does not apply, forty-eight times in one report of 1,086 facts, and that becomes a null `value` rather than a zero. `concept` is the element identifier as the conversion writes it, prefix and name; four prefixes appear, and one of them -- `jpcrp030000-asr_E03270-000` -- is built from the form and the filer's own EDINET code, which is how an extension is told from the taxonomy here.

#### Consolidated and non-consolidated

**A Japanese report states its figures twice**, and no reader of this table should take the first row that matches. The consolidated accounts carry no dimension; the parent company's own carry `NonConsolidatedMember`. Kanagawa Chuo Kotsu's annual report states earnings per share of 294.99 for the group and 236.58 for the parent in the same table of the same report.

A filer that prepares no consolidated statements states its accounts under the member alone, so a query demanding an empty dimension returns nothing at all for it. Which it is, is in the filing: `jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI`. The agency's list of filers carries a flag of its own and the two can disagree -- for Kojima they do, the list saying the filer has consolidated accounts and the filing saying it prepared none.

A further member on top of the consolidation one is a breakdown and not a statement line. The statement of changes in equity tags the net assets once per component of equity, so one report states `NetAssets` under `NonConsolidatedMember_CapitalStockMember`, `..._RetainedEarningsMember` and seven other members besides the total.

### `filers`

One row per filer the agency lists, from the zip it publishes on the disclosure site. It is the counterpart of the Commission's list of registrants and, like it, is how a lake that wants companies by name or by ticker finds the codes to collect them by. The list as published in November 2026 is 11,467 rows, of which 3,816 are listed; the figures below are of that snapshot and the agency revises it continually.

`year_end` is carried as the agency writes it -- `'5月31日'` for a filer closing on 31 May -- and not as a date, there being no year in it. It is worth having: **1,633 of the 3,816 listed filers do not close in March**, and a comparison that assumes the Japanese financial year ends there is wrong for every one of them. It is also why the field is text and not a parsed date: among those listed filers the agency writes the end of March two ways, `3月31日` for 2,029 of them and `3月末日` for 139, and thirteen more close on `3月20日`.

`sec_code` is the securities code: the four-digit ticker of a listed filer with a check digit appended, so `'75130'` is ticker 7513. In that snapshot it is non-blank for exactly the 3,816 filers the list marks as listed. `jcn` is the corporate number of the National Tax Agency, thirteen digits, non-blank for 7,289 of the rows -- an individual filer has none.

## Data quality

`quality` holds the checks the figures are held to, and
`update_silver_edinet_quality` writes what they found into a fourth Silver
table, `issues`, beside the three the module collects. The machinery is
shared with the other two regimes in
[`fincommons.dataquality`](../../dataquality.py); what lives here are the
twenty-six checks themselves, because a check knows the data set it is made
of.

```python
from fincommons.financialreport import edinet
from fincommons.dataquality import describe_check, issue_counts, with_issues

edinet.update_silver_edinet_quality(spark, "paths.yaml")

issues = edinet.connect_to_edinet(spark, "paths.yaml", "issues")
issue_counts(issues, dataset="edinet").show()     # one row per check
print(describe_check("UNIT_NOT_A_RATIO"))         # what a code means

facts = edinet.connect_to_edinet(spark, "paths.yaml", "facts")
with_issues(facts, issues, ["EDINETCODE", "FILING", "FACT"])
```

**Nothing is dropped and nothing is corrected.** A check writes a row and
leaves the figure where it is: the tables are what the agency published, and
a reader who wants to know what is wrong with a figure before using it joins
the issues on. A filing with nothing wrong with it has no row at all -- the
absence of a code is what says a record passed.

The arithmetic checks are the valuable ones, as in the other two regimes,
and they are the relations a statement states on both sides. **Two of them
exist only here**, and they are the two lines that make a Japanese profit and
loss account what it is: the ordinary result, 経常利益, is the operating
result and the recurring items outside it, and the result before tax is the
ordinary result and the extraordinary items, 特別損益. Neither relation can
be written for an IFRS or a US GAAP filer, because neither has the lines.

Four more checks have no counterpart anywhere in this package, and each
falls out of something said further up this README.

**The filer states its own year twice.** The five-year summary of the front
matter and the financial statements both carry the revenue and the total
assets of the year the report is about, in different elements of different
taxonomies, so they have to agree. `SUMMARY_DISAGREES` is the only check in
the package that tests a filing against another part of itself; a company
file of the Commission holds one account of a period per filing, and an ESEF
report states no summary at all.

**The period is resolved and not reported.** Anything older than the
comparative is shifted by whole years, which is exact only where the filer's
years are of equal length -- and the filing states the first and last day of
both of its two most recent years, so whether the shift can be trusted is a
question the filing itself answers. `PERIOD_SHIFT_INEXACT` asks it, with
three days of slack so that a leap year is not mistaken for a 52-week
calendar.

**The conversion says twice whether a figure is the group's.** 連結・個別 is
a column and `NonConsolidatedMember` is a dimension of the context, and the
two cannot disagree. Over the reports read while the module was written the
correspondence held for every one of 6,752 facts;
`BASIS_CONTRADICTS_DIMENSION` is what notices if it ever stops.

**An element named for a ratio has to be a pure number.** `UNIT_NOT_A_RATIO`
is the only check of the four that has found anything in a real report, and
it found it in Fujitsu's: `EquityToAssetRatioIFRSSummaryOfBusinessResults`
tagged with 1,167.25 and the unit `JPYPerShares`, which is the equity
attributable to the owners of the parent per share under the name of a ratio.
A reader trusting the name prints an equity ratio of 1,167 per cent. The
ratio itself is in the same report under
`RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults` as 0.60.

Two things decide what every check sees, and both are settled once rather
than by each check.

**The basis.** The figures of a filing are the ones it states for its own
period end on the basis the filing itself says it reports on: the
consolidated accounts where 連結決算の有無 is set, the parent company's own
where it is not. A member on top of the consolidation one is a breakdown and
not a statement line, so a fact carrying one is left out -- the statement of
changes in equity tags the net assets once per component of equity, and a
check that took the first match would be testing a figure against part of
itself.

**A missing line is not a zero.** A component of a sum being tested has to be
stated, and a difference that cannot be computed is not a difference.
Treating an absent line as nothing is what turns a filing that presents its
statements differently into a filing that fails arithmetic: an IFRS statement
of financial position subtotals its current liabilities and leaves the
non-current side to be read as the remainder, and the first draft of these
checks reported Fujitsu's liabilities as short by 244,695 million because of
it. Only a genuinely optional line is coalesced to nothing -- the
extraordinary items a filer may not have had, the result of operations it
discontinued -- and each one is named where it is added.

Run over four real annual securities reports -- Kojima, which does not
consolidate; Kanagawa Chuo Kotsu twice, consolidated under Japan GAAP;
Fujitsu, consolidated under IFRS -- the twenty-six checks report five issues,
all five being Fujitsu's mis-tagged ratio.

What each code means, how severe it is and what to do about it are rows of
`data/dataquality_checks.csv` rather than branches in the code, so that the
people most able to find a check wrong can read it. Nothing in it is
authoritative; `describe_check` prints a row, and a deployment that keeps
checks of its own ships a catalogue of the same shape and names it.

## A factsheet on one filer

`templates/edinet-analyst-factsheet.qmd` is a template for a factsheet on one filer, written in R and rendered with Quarto to a PDF of some twenty pages, with **Kojima Co., Ltd.** as its example. It reads the Silver tables as parquet and sets, from the facts of the filer's own annual securities reports and from nothing else:

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

The filer is set by its EDINET code in the first chunk. The corporate design is in `r-corporate-design-functions-ggplot2.R`, one file for the whole package in `src/fincommons/templates`, three levels up from a template, which the esef and sec templates source as well, so that a factsheet of any regime looks like the others.

Six things are worth knowing before the template is pointed at another filer.

**Two spans, and they are not the same.** The front matter of a Japanese annual securities report carries a five-year summary of the headline figures while its statements carry the year and its comparative. **One report therefore affords five years** of revenue, the ordinary result, total assets, the three cash flows, the employee count and the filer's own ratios, which neither of the other two regimes can do from a single filing. The factsheet keeps the two spans apart and names what each covers rather than padding the short one out with dashes.

**The consolidation basis.** `consolidation` is `"auto"` by default and the filing's own 連結決算の有無 decides it; `"consolidated"` and `"nonconsolidated"` force it. Where the agency's list and the filing disagree the profile page says so rather than preferring one silently. Only ten lines may be taken from the other basis -- the stated capital, the shares issued, the dividend and its payout, the shareholder return, the workforce figures -- because those are properties of the company however it reports, and they are marked ‡ where they are. **Nothing else falls back.** Fujitsu reports its group under IFRS and its parent under Japan GAAP, so its parent's 経常利益 is in the same report, and an unrestricted fallback put 275,236 million of it on the cover of a group factsheet whose revenue is 3,502,971 million.

**A concept is matched on its local name and never on the prefix.** Three of the four prefixes of a conversion are fixed and the fourth is built from the form and the filer's own EDINET code, so an element a filer created sits under that one. Fujitsu's operating profit is `jpcrp030000-asr_E01766-000:OperatingProfitIFRSSummaryOfBusinessResults`: an element of the five-year summary carried by the filer's own prefix, the core taxonomy having no operating profit for an IFRS filer. Matching on the prefix as well would lose it, and lose it silently.

**Every line declares the unit class it accepts**, and a fact of another class is refused and reported on the coverage page. This is not belt and braces. In Fujitsu's own five-year summary the element named `EquityToAssetRatioIFRSSummaryOfBusinessResults` carries 1,167.25 with the unit `JPYPerShares`: the equity attributable to the owners of the parent per share, under the name of a ratio. A factsheet trusting the name would print an equity ratio of 1,167 per cent, and the ratio is in the same report under `RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults` as 0.60. The same element therefore serves two keys and the unit decides which. The filer's own ratios arrive as fractions besides -- an equity ratio of 57.1 per cent is stated as 0.571 -- and are brought onto percentages.

**No per-share figure is adjusted for a split.** A Japanese filer restates the earlier years of its own five-year summary at its discretion and not always for every year of it: Fujitsu's report states 207.0, 207.0, 207.1, 2,071.1 and 1,739.8 million shares against earnings per share of 924.21, 110.76, 135.59, 120.93 and 254.83, which is four years restated onto the shares after the ten-for-one split of October 2024 and the fifth left on the shares before it. A factor derived from the share counts would divide the four that are already restated, so the year the count moved is named instead and the reader is told the series spans it. This is where the template parts company with the sec factsheet, which does derive such a factor.

**The Japanese text needs a font.** The page is set in the font the corporate design names, which covers no Japanese, and typst sets what a font lacks as empty boxes without saying so. `cjk_fonts` names the fallback it reaches for; any installed family covering JIS X 0208 will do. `labels` sets the tables in English, in the filer's own Japanese, or in both -- charts use the English names whatever it is set to, their font being the graphics device's.

Three sections of the factsheet exist only here, and each follows from what the Cabinet Office ordinance asks a filer for: the workforce, the holders of the shares with the ten largest by name, and the total shareholder return over five years against the index the filer names. That last one and the price-earnings ratio are the only market figures in a document that fetches no market data, both being the filer's own statements.

Fourteen checks close the document, among them one that tests the reading of both year axes at once: the summary and the statements must agree about the year they share. Rendered against three real filers -- Kojima, which does not consolidate; Kanagawa Chuo Kotsu, consolidated under Japan GAAP; Fujitsu, consolidated under IFRS -- thirteen, fourteen and thirteen of the fourteen hold, Fujitsu's one difference being an operating income it presents between its gross profit and its operating result.

The last page is a disclaimer, and the cover says in its first line and in its small print what that page states in full: the document is produced by a template and the filer is an example, no warranty is given for the correctness or the completeness of a figure, nobody who made it is associated with the filer or with the agency, nothing in it is advice, and liability is disclaimed as far as the law allows, in the terms of Articles 7 and 8 of the EUPL the project is licensed under. Whoever renders the factsheet for a purpose of their own is responsible for what they publish.

## The corporate number reaches the LEI

**It does, and this is the only identifier in this family of modules that
does.** Neither the Commission nor GLEIF publishes a mapping from a central
index key to a legal entity identifier, and ESMA publishes none from an LEI
to anything the Commission uses; `financialreport` carries both columns and
fills exactly one of them in any row for that reason. Japan is different,
and the reason is that the corporate number is not an identifier of its own:

> The corporate number of the National Tax Agency is a check digit followed
> by the twelve-digit 会社法人等番号 of the commercial register, and GLEIF
> records that register number as the registration authority entity id of an
> LEI issued to a Japanese company.

So the join is arithmetic rather than a lookup table. Drop the leading check
digit, hyphenate the rest as the register writes it, and ask GLEIF:

```python
jcn = "1020001071491"                     # JCN of 富士通株式会社
body = jcn[1:]                            # 020001071491
registered_as = f"{body[:4]}-{body[4:6]}-{body[6:]}"   # 0200-01-071491
# GLEIF: entity.registeredAs == "0200-01-071491" under RA000412
# -> LEI 3538000246DHJLRTUZ24, 富士通株式会社
```

`RA000412` is the 登記 register of the Legal Affairs Bureau. Every match
found while this was checked named that one register and no other, which is
what makes the join exact: it is two spellings of one register number and
not a name comparison.

### How far it reaches

Of a random sample of 200 of the 3,810 listed filers the agency's list gives
a thirteen-digit corporate number for, **27 reached an LEI -- 13.5 per
cent**. Ten of the misses were then looked up by name in GLEIF and not one of
them had an LEI under any spelling, so the join is not failing on them: most
Japanese listed companies simply are not in the LEI system, which is a
population fact and not a defect of the mapping.

Of the 27 matches, 26 carry a legal name that agrees with the filer's. The
twenty-seventh is the point of joining on a number rather than on a name:
EDINET has E24558 as 株式会社ミライト・ワン and GLEIF still has the LEI under
株式会社ミライト・ホールディングス, the name before the group renamed itself.
A number survives a rename and a name match does not.

### What it is good for, and what it is not

It is enough to put a Japanese filer beside a European one **where both
exist**, which is the thing the combined tables cannot otherwise do at all.
It is not enough to key the combined tables by: seven of eight listed
Japanese filers would carry no LEI, and an identifier that is null for most
of a population is not the identifier that population is keyed by. The
corporate number stays the key for Japan, as the central index key does for
the Commission, and the LEI is a column that is filled where it can be.

Nothing in this module derives it. The join needs GLEIF, which is
[`entitymasterdata`](../../entitymasterdata/README.md), and belongs wherever
the regimes are put side by side rather than in the module that reads one of
them.

## What is not here yet

This module is not yet in `financialreport`'s `SOURCES`, so its facts are not
in the combined tables. The entity question above is settled; what remains is
the mapping of the Japanese elements onto the measures of
`financialreport_measures.csv`, which carries IFRS and US GAAP rows and no
Japanese ones.
