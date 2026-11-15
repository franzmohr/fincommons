# Financial reports

## Introduction

Module `financialreport` puts the filings of the tagging regimes side by side. Three of them require the periodic reports of listed companies to be tagged: the [European Single Electronic Format](https://www.esma.europa.eu/issuer-disclosure/electronic-reporting) in the Union, the inline XBRL mandate of the U.S. Securities and Exchange Commission, and the XBRL that Japan's Financial Services Agency has required of a securities report since 2008. [`esef`](../esef/README.md), [`sec`](../sec/README.md) and [`edinet`](../edinet/README.md) each bring one of them into the lake as its publisher gives it out, in the shape that publisher gives it. This module reduces them to the same columns and writes one table of facts and one of filings, so that a question put to all three at once is put to one table rather than to three of different shapes.

It holds no data of its own and collects nothing. There is no Bronze layer here, nothing is downloaded, and both tables are derived entirely from tables that are themselves rebuilt -- so both are rebuilt in full on every run, with no watermark to keep and nothing to resume from.

### What it does, and what it deliberately does not

**It takes nothing away from the two source modules.** Their tables stay authoritative and complete, neither imports this one, and neither knows it exists. An ESEF-only lake builds nothing here and loses nothing by it; the factsheet and the coverage report that `esef` ships keep working untouched, as does the factsheet of `sec`. The dependency runs one way, and only one way.

**It does not join the regimes by entity, because it cannot.** ESEF prescribes the legal entity identifier; a filing with the Commission states a central index key; and neither body publishes a mapping between the two. What GLEIF does publish is the register an entity is registered in, and for some tens of thousands of records that register is EDGAR -- but nearly all of those are funds, named by a series identifier rather than by a central index key, while an operating company's record points at its state register instead. NVIDIA's names Delaware, by a file number that is not its central index key. The issuers these tables hold are exactly the ones it does not reach. Both columns are carried and exactly one of them is filled in any row. It is the one thing a lake holding both regimes would most like to do, and the honest shape of the table says so rather than filling a column with a guess.

Japan is the exception, and it is worth saying why rather than leaving it as a happy accident. The corporate number of the National Tax Agency, which [`edinet`](../edinet/README.md) carries as `JCN`, is not an identifier of its own: it is a check digit followed by the twelve-digit register number of the commercial register, and GLEIF records that same register number against an LEI issued to a Japanese company. The join is therefore arithmetic rather than a lookup table, and it is exact -- two spellings of one register number, not a comparison of names. It reaches about one listed Japanese filer in seven, the rest having no LEI at all, so it is enough to put a Japanese filer beside a European one where both exist and not enough to key anything by. The edinet README sets out the check and what it found.

**It reconciles the vocabularies one measure at a time, and only in a column of its own.** `MEASURE` carries the identifier of what a fact *is* -- the same for every regime that states the same figure -- beside the `CONCEPT` its filer tagged, which is never altered. It is filled for the measures `mapping.MATERIALISED_MEASURES` covers and null for everything else, which today is nearly the whole table.

That restraint is the design and not a stage of it. A materialised identifier is read by people who will not go back to the caveat: a query filtering on `MEASURE = 'assets'` is making the claim that the rows it gets back are comparable, so a measure earns the column only once its concepts are agreed to mean the same figure in every framework the catalogue defines it for.

It covers **total assets**, **total liabilities**, **total equity**, **revenue** and **cash and cash equivalents**. The first concept of each framework, one per taxonomy:

| measure | IFRS | US-GAAP | JP-GAAP | IFRS, as a Japanese filer tags it |
| --- | --- | --- | --- | --- |
| assets | `ifrs-full:Assets` | `us-gaap:Assets` | `jppfs_cor:Assets` | `jpigp_cor:AssetsIFRS` |
| liabilities | `ifrs-full:Liabilities` | `us-gaap:Liabilities` | `jppfs_cor:Liabilities` | `jpigp_cor:LiabilitiesIFRS` |
| equity | `ifrs-full:Equity` | `us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | `jppfs_cor:NetAssets` | `jpigp_cor:EquityIFRS` |
| revenue | `ifrs-full:Revenue` | `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` | `jppfs_cor:NetSales` | `jpigp_cor:RevenueIFRS` |
| cash and equivalents | `ifrs-full:CashAndCashEquivalents` | `us-gaap:CashAndCashEquivalentsAtCarryingValue` | `jppfs_cor:CashAndCashEquivalents` | `jpigp_cor:CashAndCashEquivalentsIFRS` |

What differs between the cells of a row is recognition rather than definition -- development costs are capitalised under IAS 38 and generally expensed under ASC 730, LIFO is permitted under US GAAP and prohibited by IAS 2 -- and that is on the catalogue row where a reader can find it rather than hidden by the shared name.

#### Only the first concept of each framework

Assets is the cleanest of the eight measures: one element per taxonomy and no alternatives at any priority. Revenue is not, and adding it is what made the rule explicit. The catalogue gives revenue seven rows and six concepts, ranking three of them below the first as approximations -- and each of those says in its own caveat that it is **not the same figure**:

| | |
| --- | --- |
| `us-gaap:Revenues` | "the last resort and not an equivalent of the IFRS total" |
| `us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax` | "larger than the IFRS measure by the assessed taxes it includes" |
| `ifrs-full:RevenueFromContractsWithCustomers` | "excludes revenue outside IFRS 15 that the total may include" |

The priorities exist for a caller resolving a measure at query time, who wants the best answer the filing affords and takes the last resort where there is nothing better. The column is held to the stricter bar, so only priority 1 is written down and a fact tagged with one of the others keeps a null. The cost is coverage rather than correctness -- a registrant tagging only the pre-ASC-606 `us-gaap:Revenues` carries no identifier -- which is the right way round for a name a query filters on. Every priority-2 and priority-3 row in the catalogue carries a caveat of the same kind, so the rule is one the catalogue already stated and this column now enforces.

It is also what keeps the column answerable. A filing may tag several concepts of one measure, which is why the ranking is there, and labelling all of them would put two different figures of one filing and one period under one name. At priority 1 the catalogue has one element per taxonomy per measure, so that cannot happen; `canonical_concepts` refuses a catalogue in which it could, and refuses one in which two measures claim a single concept.

#### The same figure is not always in the same place

Cash and cash equivalents is the first measure whose first concepts are not the same line of the same statement. IFRS, Japanese IFRS and US GAAP each present it on the balance sheet. **A Japanese balance sheet has no line that means it** -- it presents 現金及び預金, cash together with deposits, time deposits of more than three months among them, which are cash equivalents under none of the three. The figure that means what `ifrs-full:CashAndCashEquivalents` means is the closing balance of the cash flow statement, and that is what carries the identifier. One name over one figure, wherever its regime chose to present it, which is the case this column exists for.

Two things follow. The first concept of IFRS carries a caveat about the US total -- ASU 2016-18 puts restricted cash in the cash flow statement total and IAS 7 does not -- and that total is `us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents`, a *second* concept the column leaves out. The pair that does carry the identifier is IAS 7 against `us-gaap:CashAndCashEquivalentsAtCarryingValue`, which the catalogue calls the closer match. So the rule removes the mismatch its own caveat warns about.

The other is a cost, and the first the rule has imposed rather than inherited. `jppfs_cor:CashAndDeposits` sits at priority 2 expressly so that a filer stating no cash flow statement is reached at all. The column declines it, because a figure read from that element "should be read as cash and deposits rather than as cash and equivalents" -- which is exactly a thing that must not wear this name. Those filers carry a null here and are still reached by `measure_values`. That is the division of labour between the stored column and query-time resolution, and this is the measure that shows it clearly.

#### Equity, where the null is a skew and not only a gap

Every measure's first concept under US GAAP is a total the filer may not have tagged. For equity that absence is **not random**, and a benchmark has to know it.

The first concept is `us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest`, the total including the non-controlling interests, which is what `ifrs-full:Equity` means and what 純資産 means. Many registrants tag only `us-gaap:StockholdersEquity`, which excludes them. The catalogue ranks that second and says why in as many words: *because many filings tag no total including NCI, not because it is equivalent*.

For a registrant that has no non-controlling interests the two elements carry the same number. The element does not say whether it has none. So naming it `equity` would be right for most filings and wrong for exactly the groups a cross-regime comparison is about -- and the registrants this column does reach are more often the ones with non-controlling interests to report. That is a skew in the sample, not a hole in it. **Counting the rows carrying a measure says how many filings were named, never how many exist.**

This null is also the one that is temporary. `us-gaap:StockholdersEquity` is the *first* concept of `equity_attributable_to_parent`, so the day that measure is written down the same fact takes a name -- and the right one rather than this one. The two are really one decision, and only the first half of it is made here. Japan complicates the second half: Japan GAAP has no element for 自己資本 at all, so `equity_attributable_to_parent` would reach two regimes out of three by construction. That is set out further down.

#### Two kinds of null, and the column cannot tell them apart

Liabilities passes the rule as easily as assets -- four concepts, one per taxonomy, nothing ranked below them -- and what it adds is the second reason a fact carries no identifier.

Revenue's nulls are the rule declining to name something the filer did state. Liabilities' are different: **many filings with the Commission tag no total liabilities at all**, presenting only liabilities and equity together, so for a substantial part of the US population the figure was never stated. Both appear as a null in the same column, and neither is a zero.

The total is not derived from the balance sheet identity to fill the gap. Nothing in this family of modules invents a figure, and what the alternative costs is already on record: in the `edinet` checks, reading a missing total as zero made one filer's liabilities short by 244,695 million. So a count of the rows carrying a measure is a count of what was stated and named -- never a population, and never a denominator without saying which.

Growing the column is therefore one entry in `MATERIALISED_MEASURES` plus the checks it has to pass. All eight measures pass them today. That includes `equity` and `profit_loss`, which an earlier reading of the catalogue took to be blocked: `us-gaap:StockholdersEquity` is claimed by both `equity` and `equity_attributable_to_parent`, but as the *second* concept of the first and the first of the second, so the priority rule settles it -- it is the equity attributable to the parent, and the stand-in for equity only where a registrant states no wider total. That they pass is not a reason to add them; each is still a judgement to be made against real filings one at a time.

**The concept is never altered to carry the identifier.** `ifrs-full:Revenue`, `us-gaap:Revenues` and `jppfs_cor:NetSales` stand in `CONCEPT` as they were filed, whatever `MEASURE` says beside them, so that what a filer tagged is always in the table and the judgement about what it means is always separable from it. That judgement is `mapping`'s, recorded as data rather than as code; it reaches all three regimes, and the section on the catalogue below sets out what it does and does not settle.

### What each source covers

The three publishers do not give out the same thing, and the difference is a property of what each publishes rather than of this module. `coverage()` returns it as a list of dictionaries, so that a notebook can print it beside a result:

| | `esef` | `sec` | `edinet` |
| --- | --- | --- | --- |
| publisher | filings.xbrl.org | sec.gov | disclosure2.edinet-fsa.go.jp |
| facts | every fact of the report, as xBRL-JSON | the undimensioned numeric facts of the filing | every fact of the report, as the agency's own CSV |
| dimensions | yes | no | yes |
| text blocks and narrative | yes | no | yes |
| entity key | `LEI` | `CIK` | `EDINETCODE` |
| frameworks | IFRS | US-GAAP, IFRS | JP-GAAP, IFRS |

A figure for an entity as a whole is comparable across all three. A breakdown by segment or by class is available on two of them: a quarterly report stating 941 facts is carried in a company file as 321. A reader of the combined table is entitled to know that an absent breakdown may mean the company reported none or may mean the publisher does not give them out, and that which of the two it is depends on the source.

Two things about the Japanese rows have to be read before they are used, and both are properties of the regime rather than of this module.

**A report that consolidates states every figure twice**, the group's with no dimension and the parent company's own under `NonConsolidatedMember`, and both are in this table because both are in the source table. A question that does not say which it wants is asking for two sets of accounts at once. Kanagawa Chuo Kotsu states earnings per share of 294.99 for the group and 236.58 for the parent in one table of one report.

**`JP-GAAP` is not comparable with the other two line by line.** A Japanese profit and loss account has two results where an IFRS one has a single operating line: the operating result, and the ordinary result that adds the recurring items outside it. The extraordinary items below that are a presentation an IFRS statement does not have. `FRAMEWORK` is read from what each filing says it reports under rather than from the prefix of a fact, because a Japanese group that has adopted IFRS tags its statements against the Japanese IFRS taxonomy and its front matter against the same disclosure taxonomy every other filer uses — so the prefix does not settle it, and the filing does.

One kind of Japanese fact is not carried, and it is the only thing this module drops. The agency's conversion carries no dates, so the edinet module resolves a period from what the filing says about itself and leaves both dates empty for a context whose shape it does not recognise. `PRDEND` is the one column of the combined table of facts that may not be empty — a fact in no period can be compared with nothing, which is what this table is for — so such a fact stays in the edinet table of facts, where that module's `PERIOD_UNRESOLVED` check counts them.

## Setting up

The configuration file needs a `financialreport` section with a `silver` key, and nothing else, since there is no Bronze layer to name:

```yaml
financialreport:
    silver: /data/financialreport/silver
```

The Silver layer may name a schema in a catalogue instead of a directory:

```yaml
financialreport:
    silver:
        table: fincommons.financialreport.combined
```

Unlike `esef` and `sec`, no `path` is needed beside the `table`: those two keep marks on disk recording what their tables were built from, and this module keeps none.

The sections of the sources have to be there as well, because this module reads their tables. Which sources a run combines is decided by which of the two have tables to read, not by this module: a lake that has collected only ESEF filings gets a combined table of ESEF filings, which is the same question answered over the population there is.

## The module in five minutes

```python
from fincommons.financialreport import combined

# Rebuild both tables from whichever sources the lake holds.
combined.update_silver_financialreport(spark, "paths.yaml")

combined.available_sources(spark, "paths.yaml")          # ['esef', 'sec']

facts = combined.connect_to_financialreport(spark, "paths.yaml", "fact")
filings = combined.connect_to_financialreport(spark, "paths.yaml", "filing")

# The revenue of every filing that reports under IFRS, whichever regime it
# was filed under, taken by the best concept the filing states.
revenue = (
    combined.measure_values(facts.where("FRAMEWORK = 'IFRS'"), "revenue")
    .select("SOURCE", "LEI", "CIK", "PERIODEND", "CONCEPT", "UNIT", "VALUE"))

# What that rests on, before it is published.
print(combined.describe_measure("revenue"))
```

`update_silver_financialreport` is the call a job scheduler makes, after the source pipelines have run. It takes `sources` to restrict the run to one of them, and `file_format` to say how the tables are stored. A run that finds no source at all raises rather than writing empty tables; a run that finds one of the two writes that one and logs that it did.

## The Silver tables

### `fact`

One row per fact of a filing, partitioned by `SOURCE` and then `PERIODYEAR`: most questions are scoped to one regime, and a question about a figure is scoped to a year after that.

| Column | Meaning |
| --- | --- |
| `SOURCE` | which module the row came from, `esef` or `sec` |
| `FILING` | the filing, as its own regime identifies it |
| `FACT` | the identifier of the fact within its filing |
| `LEI`, `CIK` | the entity; one is filled and the other empty, and neither is derived from the other |
| `FRAMEWORK` | `IFRS` or `US-GAAP`, from the prefix the concept is tagged under |
| `PERIODEND`, `PERIODYEAR` | the period the filing reports on, and its year |
| `CONCEPT` | the concept as the filing tagged it, prefix and name, unresolved across frameworks |
| `PRDSTART`, `PRDEND` | the period of the fact; an instant has no `PRDSTART` |
| `UNIT`, `VALUE` | a number and its unit, as reported and in full units |
| `TEXT`, `DIMENSIONS` | filled for ESEF rows only |

`FRAMEWORK` is taken from the taxonomy the filing tagged against and not from the regime it was filed under. That is what makes the population where the two genuinely meet usable: a foreign private issuer reporting under IFRS files with the Commission and tags against the IFRS taxonomy, and belongs beside the European reports rather than beside the American ones it was filed with.

`TEXT` and `DIMENSIONS` are empty for every SEC row, and not because this module drops them: the company files of the Commission carry no narrative facts and no breakdowns at all. An empty `DIMENSIONS` means the fact is for the entity as a whole, which is true of every row the Commission contributes.

### `filing`

One row per filing.

| Column | Meaning |
| --- | --- |
| `SOURCE`, `FILING` | which module, and the filing |
| `LEI`, `CIK`, `NAME` | the entity |
| `FRAMEWORK` | `IFRS` or `US-GAAP`, from the concepts the filing tagged |
| `COUNTRY` | the member state the report was filed in; ESEF only |
| `FORM` | the form the filing was made on, e.g. `10-K`; SEC only |
| `PERIODEND`, `PERIODYEAR` | the period reported on, and its year |
| `FACTS` | how many facts the filing contributed |

`COUNTRY` and `FORM` are each empty where their regime states nothing: an ESEF filing goes to the mechanism of one member state and the regime names no form, while a filing with the Commission names a form and no country. Neither is given a value this module invented. For an ESEF filing the framework and the count come from the facts, which is where the evidence for both is -- the repository's index says nothing about what a report tagged against.

## The catalogue of measures

`mapping` is the only module in the package that knows IFRS from US GAAP, and the only one that may. Whether `us-gaap:Revenues` and `ifrs-full:Revenue` are the same figure is a judgement about accounting rather than about storage, and it is made here so that it is made once, in one place, where it can be read and disagreed with.

The judgement is data and not code: a CSV shipped in the data directory of the package, `financialreport_measures.csv`, beside the column catalogue and the FIRDS schemas. It currently defines **8 measures in 38 rows**, of which **29 carry a caveat**.

**It reaches all three regimes.** The Japanese rows were added last and are the reason the `framework` and `taxonomy` columns are separate: a Japanese group that has adopted IFRS reports under IFRS and tags against `jpigp_cor`, Japan's own taxonomy for it, so its revenue resolves under `framework = 'IFRS'` beside `ifrs-full:Revenue` without being the same element. A filer under Japan GAAP tags `jppfs_cor` and carries `framework = 'JP-GAAP'`.

Two of the Japanese judgements are worth reading before the figures are used, and both are recorded as caveats on the rows themselves.

**Japan GAAP has no element for the equity attributable to the parent**, so no `JP-GAAP` row defines `equity_attributable_to_parent` and `concepts_for('equity_attributable_to_parent', 'JP-GAAP')` returns nothing. 自己資本 is 株主資本 plus 評価・換算差額等 — a sum rather than a line. Taking `jppfs_cor:ShareholdersEquity` for it is short by the valuation and translation adjustments, which for one report checked here is 9,191 million of a correct 63,326 million, so the row is absent rather than wrong.

**A Japanese balance sheet does not present cash and cash equivalents.** It presents 現金及び預金, cash together with deposits, time deposits of more than three months among them. The figure that means what `ifrs-full:CashAndCashEquivalents` means is the closing balance of the cash flow statement, so that is the first row and the balance sheet line is the second, carried only so that a filer stating no cash flow statement is reached at all.

One gap is known and is a gap rather than a judgement: a filer whose revenue is 営業収益 rather than 売上高 — a railway, a utility, an insurer — states it under an element no row here reaches. None of the reports this catalogue was written against uses one, and an element nobody has seen in a conversion is not put in an accounting catalogue.

| Column | Meaning |
| --- | --- |
| `measure` | the name being defined, such as `revenue` |
| `framework` | the framework the row applies to, as the tables record it |
| `taxonomy`, `element` | the concept, as the prefix the filings bind and the element name -- `ifrs-full` and `Revenue` |
| `priority` | which concept wins where a filing states several, lowest first |
| `basis` | why this concept |
| `caveat` | where the two frameworks do not actually agree |
| `source` | who decided |

The namespace is deliberately not used: a published taxonomy takes a new one every year, and neither source table carries it. The prefix is what both tables hold -- the ESEF rows because xBRL-JSON names a concept that way, the SEC rows because the Commission keys its facts by taxonomy.

**Nothing in this catalogue is authoritative, and the module says so.** There is no official correspondence between IFRS and US GAAP: the convergence programme of the two boards did not produce one, and the data quality rules published for filings with the Commission do not cross frameworks. What is shipped is a starting point a deployment should review and extend, and the `source` column exists so that a row somebody has reviewed can be told from a row nobody has.

```python
from fincommons.financialreport import combined as fr

fr.measures()                        # the names defined
fr.measure_catalogue(measure="equity")       # the rows, as a data frame
fr.concepts_for("revenue", "US-GAAP")        # [('us-gaap', 'RevenueFromContract...'), ...]
print(fr.describe_measure("revenue"))        # basis, caveat and source, printable
fr.caveats()                                 # every row that carries one
```

`with_measures(facts)` labels a table of facts with `MEASURE` and `MEASURE_PRIORITY`, leaving both null for the great majority of facts, which no row of the catalogue names. `measure_values(facts, "revenue")` goes one step further and returns one value per filing and period, by the best concept the filing states; it logs a warning where the concepts it resolved through carry caveats, and it excludes facts reported under a dimension, since a measure is the figure for the entity as a whole and summing the parts beside the whole is the arithmetic that exclusion exists to prevent.

Both join on `CONCEPT` as the tables spell it, which means **the catalogue works without this module's tables**. An ESEF-only lake can resolve a measure over `fincommons.financialreport.esef` directly:

```python
from fincommons.financialreport import esef
from fincommons.financialreport.combined import measure_values

measure_values(esef.connect_to_esef(spark, "paths.yaml"), "assets")
```

A deployment that keeps its own judgements ships a catalogue of the same shape in the data directory of its own package and names it: `measure_catalogue(packages=("fincommons", "mycompany"))`. Its rows are read beside these, which is how a reviewed mapping is kept without editing the one this package ships.

The module stops at resolution. It says which concepts a measure is taken to be and labels facts accordingly; it does not define ratios, aggregate across entities or build a metrics layer. Those belong to whoever is asking the question, and to the package they ship, rather than to the infrastructure that holds the data.

### A comparison, honestly

A measure resolved across both regimes is comparable only as far as its caveat says, and the caveats are not decoration. Revenue is the clearest case: `ifrs-full:Revenue` may include revenue from sources other than contracts with customers, which ASC 606 reports separately; the US split between revenue excluding and including assessed tax has no IFRS counterpart at all, so a filer tagging the including variant reports a larger figure than an IFRS filer would for the same business; and `us-gaap:Revenues` predates ASC 606, is tagged inconsistently, and is the last resort rather than an equivalent of the IFRS total. Anyone publishing a cross-regime comparison should print `caveats()` beside it.
