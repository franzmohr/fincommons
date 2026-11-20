# Financial reports

## Introduction

Module `financialreport` puts the filings of the tagging regimes side by side. Three of them require the periodic reports of listed companies to be tagged: the [European Single Electronic Format](https://www.esma.europa.eu/issuer-disclosure/electronic-reporting) in the Union, the inline XBRL mandate of the U.S. Securities and Exchange Commission, and the XBRL that Japan's Financial Services Agency has required of a securities report since 2008. [`esef`](../esef/README.md), [`sec`](../sec/README.md) and [`edinet`](../edinet/README.md) each bring one of them into the lake as its publisher gives it out, in the shape that publisher gives it. This module reduces them to the same columns and writes one table of facts and one of filings, so that a question put to all three at once is put to one table rather than to three of different shapes.

It holds no data of its own and collects nothing. There is no Bronze layer here, nothing is downloaded, and both tables are derived entirely from tables that are themselves rebuilt -- so both are rebuilt in full on every run, with no watermark to keep and nothing to resume from.

### What it does, and what it deliberately does not

**It takes nothing away from the source modules.** Their tables stay authoritative and complete, neither imports this one, and neither knows it exists. An ESEF-only lake builds nothing here and loses nothing by it; the factsheet and the coverage report that `esef` ships keep working untouched, as do the factsheets of `sec` and `edinet`. The dependency runs one way, and only one way.

**It does not join the regimes by entity, because it cannot.** ESEF prescribes the legal entity identifier; a filing with the Commission states a central index key; and neither body publishes a mapping between the two. What GLEIF does publish is the register an entity is registered in, and for some tens of thousands of records that register is EDGAR -- but nearly all of those are funds, named by a series identifier rather than by a central index key, while an operating company's record points at its state register instead. NVIDIA's names Delaware, by a file number that is not its central index key. The issuers these tables hold are exactly the ones it does not reach. Both columns are carried and exactly one of them is filled in any row. It is the one thing a lake holding both regimes would most like to do, and the honest shape of the table says so rather than filling a column with a guess.

Japan is the exception, and it is worth saying why rather than leaving it as a happy accident. The corporate number of the National Tax Agency, which [`edinet`](../edinet/README.md) carries as `jcn`, is not an identifier of its own: it is a check digit followed by the twelve-digit register number of the commercial register, and GLEIF records that same register number against an LEI issued to a Japanese company. The join is therefore arithmetic rather than a lookup table, and it is exact -- two spellings of one register number, not a comparison of names. It reaches about one listed Japanese filer in seven, the rest having no LEI at all, so it is enough to put a Japanese filer beside a European one where both exist and not enough to key anything by. The edinet README sets out the check and what it found.

**It reconciles the vocabularies one measure at a time, and only in a column of its own.** `measure` carries the identifier of what a fact *is* -- the same for every regime that states the same figure -- beside the `concept` its filer tagged, which is never altered. It is filled for the measures `mapping.MATERIALISED_MEASURES` covers and null for everything else, which today is nearly the whole table.

That restraint is the design and not a stage of it. A materialised identifier is read by people who will not go back to the caveat: a query filtering on `measure = 'assets'` is making the claim that the rows it gets back are comparable, so a measure earns the column only once its concepts are agreed to mean the same figure in every framework the catalogue defines it for.

It covers all eight measures the catalogue defines. The first concept of each framework, one per taxonomy:

| measure | IFRS | US-GAAP | JP-GAAP | IFRS, as a Japanese filer tags it |
| --- | --- | --- | --- | --- |
| assets | `ifrs-full:Assets` | `us-gaap:Assets` | `jppfs_cor:Assets` | `jpigp_cor:AssetsIFRS` |
| liabilities | `ifrs-full:Liabilities` | `us-gaap:Liabilities` | `jppfs_cor:Liabilities` | `jpigp_cor:LiabilitiesIFRS` |
| equity | `ifrs-full:Equity` | `us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | `jppfs_cor:NetAssets` | `jpigp_cor:EquityIFRS` |
| equity attributable to the parent | `ifrs-full:EquityAttributableToOwnersOfParent` | `us-gaap:StockholdersEquity` | *no element* | `jpigp_cor:EquityAttributableToOwnersOfParentIFRS` |
| profit or loss | `ifrs-full:ProfitLoss` | `us-gaap:ProfitLoss` | `jppfs_cor:ProfitLoss` | `jpigp_cor:ProfitLossIFRS` |
| profit or loss attributable to the parent | `ifrs-full:ProfitLossAttributableToOwnersOfParent` | `us-gaap:NetIncomeLoss` | `jppfs_cor:ProfitLossAttributableToOwnersOfParent` | `jpigp_cor:ProfitLossAttributableToOwnersOfParentIFRS` |
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

#### Where the null is a skew, and the two regimes are not skewed alike

Equity and profit or loss share a shape, and it is the one that matters most for a benchmark.

The first concept of each is the total **including the non-controlling interests** -- `us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` and `us-gaap:ProfitLoss` on the American side. Many registrants tag only `us-gaap:StockholdersEquity` and `us-gaap:NetIncomeLoss`, which exclude them. The catalogue ranks both second and says why in as many words: *because many filings tag no total including NCI, not because it is equivalent.*

For a registrant with no non-controlling interests the two elements carry **the same number**. The element does not say whether it has none. So naming it would be right for most filings and wrong for exactly the groups a cross-regime comparison exists to look at, and the column declines.

What adding profit or loss makes plain is that **the resulting skew is not symmetric between regimes**. Take the same economic fact in two places -- a company with no non-controlling interests:

| | what it tags | what the column says |
| --- | --- | --- |
| Japan | `jppfs_cor:ProfitLoss`, 当期純利益, as every filer does | `profit_loss` |
| United States | `us-gaap:NetIncomeLoss` only | nothing |

The Japanese population under `measure = 'profit_loss'` is close to every filer. The American population is conditional on having non-controlling interests to report. The two samples are selected by different rules, so **a comparison of their medians is not a comparison of the two regimes**. The same holds for equity: 純資産 is tagged by consolidating and non-consolidating filers alike. Counting the rows carrying a measure says how many filings were named, never how many exist.

Both exclusions are temporary in the same way. `us-gaap:StockholdersEquity` and `us-gaap:NetIncomeLoss` are the *first* concepts of `equity_attributable_to_parent` and `profit_loss_attributable_to_parent`, so writing those measures down gives these facts a name -- the right one rather than this one. The two attributable measures differ in reach, and that is the thing to decide them on: profit or loss attributable to the parent has an element in all four taxonomies, Japan stating 親会社株主に帰属する当期純利益 as a line of its own, while **Japan GAAP has no element for 自己資本 at all**, so the equity counterpart would reach two regimes of three by construction. That gap is set out further down.

What differs between the frameworks in profit or loss is composition rather than definition. A Japanese profit and loss account reaches 当期純利益 through 経常利益 and the extraordinary items below it, which an IFRS statement does not have. The intermediate lines are not comparable; the bottom line is still the bottom line.

#### The pair does not repair the skew. It inverts it.

Adding the attributable measure puts a name on `us-gaap:NetIncomeLoss` -- exactly the facts `profit_loss` declines -- and it reaches every regime, Japan stating 親会社株主に帰属する当期純利益 as a line of its own. It does not make the population whole, and the reason is worth following.

The attributable line exists only where there are non-controlling interests to attribute away. A filer preparing no consolidated statements states none under IFRS or under Japan GAAP: its result is the one figure that is both measures at once. `us-gaap:NetIncomeLoss` is not like that. It is the ordinary bottom line of an American income statement, tagged by registrants with subsidiaries and without. So the two measures lean opposite ways:

| measure | IFRS | JP-GAAP | US-GAAP |
| --- | --- | --- | --- |
| `profit_loss` | most filers | most filers | needs non-controlling interests |
| `profit_loss_attributable_to_parent` | needs non-controlling interests | needs non-controlling interests | most filers |

Between them the two cover the population well. **Neither does alone, and they are skewed in opposite directions**, so switching from one to the other changes which filers you are looking at as much as which figure.

#### One gap is structural, and it is the only one

Every absence described so far depends on what a filer tagged. A registrant that states no total liabilities; one that reports no minorities to attribute away. Change the filer and the gap closes.

`equity_attributable_to_parent` has one that does not. **Japan GAAP has no element for 自己資本 at all.** It is 株主資本 plus 評価・換算差額等 -- a sum rather than a line -- so no `JP-GAAP` filing can carry this measure however it is prepared, and `concepts_for('equity_attributable_to_parent', 'JP-GAAP')` returns nothing. A query filtering on this measure **excludes Japan entirely and says nothing about having done so**, which is the one case in this column where a null says more about the taxonomy than about the filing.

The sum is not computed here to close it. Taking `jppfs_cor:ShareholdersEquity` instead is short by the valuation and translation adjustments -- 9,191 million of a correct 63,326 million on one report read for the catalogue -- and adding the components up would be this module inventing a figure, which it does not do.

So the equity pair stays lopsided where the profit pair's reach is not. `equity` reaches every regime; this reaches two of three. Within those two they lean opposite ways as the profit pair does: most American registrants tag `us-gaap:StockholdersEquity`, and an IFRS filer states the attributable line only where there is something to attribute.

#### Which to use, and when

This is the limit of a materialised identifier, and it is better reached honestly than papered over. The figure a population-level comparison wants is *the result attributable to the parent, falling back to the total where there are no minorities to separate*. That is a judgement per filing -- it depends on what the filing states -- and not a judgement per concept, so no concept-to-name map can express it.

- **`measure`** answers a precise question about named facts. This fact is the total including minorities; that one is the parent's share. Filter on it when you mean exactly one of those.
- **`measure_values`** answers a question about a population. It takes the catalogue's priorities, picks the best concept each filing actually states, and returns one row per filing and period. Use it for a benchmark.

The column does not replace the function, and the two pairs are where that stops being a technicality.

All eight measures of the catalogue are now materialised, and the gate stays. A measure added to the catalogue tomorrow is **not** materialised by being added: it is named in `MATERIALISED_MEASURES` or it carries no identifier. That tuple is written out rather than derived from the catalogue precisely so that writing a row and claiming a figure is comparable stay two separate acts.

#### Two kinds of null, and the column cannot tell them apart

Liabilities passes the rule as easily as assets -- four concepts, one per taxonomy, nothing ranked below them -- and what it adds is the second reason a fact carries no identifier.

Revenue's nulls are the rule declining to name something the filer did state. Liabilities' are different: **many filings with the Commission tag no total liabilities at all**, presenting only liabilities and equity together, so for a substantial part of the US population the figure was never stated. Both appear as a null in the same column, and neither is a zero.

The total is not derived from the balance sheet identity to fill the gap. Nothing in this family of modules invents a figure, and what the alternative costs is already on record: in the `edinet` checks, reading a missing total as zero made one filer's liabilities short by 244,695 million. So a count of the rows carrying a measure is a count of what was stated and named -- never a population, and never a denominator without saying which.

Growing the column is therefore one entry in `MATERIALISED_MEASURES` plus the checks it has to pass. All eight measures pass them today. That includes `equity` and `profit_loss`, which an earlier reading of the catalogue took to be blocked: `us-gaap:StockholdersEquity` is claimed by both `equity` and `equity_attributable_to_parent`, but as the *second* concept of the first and the first of the second, so the priority rule settles it -- it is the equity attributable to the parent, and the stand-in for equity only where a registrant states no wider total. That they pass is not a reason to add them; each is still a judgement to be made against real filings one at a time.

**The concept is never altered to carry the identifier.** `ifrs-full:Revenue`, `us-gaap:Revenues` and `jppfs_cor:NetSales` stand in `concept` as they were filed, whatever `measure` says beside them, so that what a filer tagged is always in the table and the judgement about what it means is always separable from it. That judgement is `mapping`'s, recorded as data rather than as code; it reaches all three regimes, and the section on the catalogue below sets out what it does and does not settle.

### What each source covers

The three publishers do not give out the same thing, and the difference is a property of what each publishes rather than of this module. `coverage()` returns it as a list of dictionaries, so that a notebook can print it beside a result:

| | `esef` | `sec` | `edinet` |
| --- | --- | --- | --- |
| publisher | filings.xbrl.org | sec.gov | disclosure2.edinet-fsa.go.jp |
| facts | every fact of the report, as xBRL-JSON | the undimensioned numeric facts of the filing | every fact of the report, as the agency's own CSV |
| dimensions | yes | no | yes |
| text blocks and narrative | yes | no | yes |
| entity key | `lei` | `cik` | `edinet_code` |
| frameworks | IFRS | US-GAAP, IFRS | JP-GAAP, IFRS |

A figure for an entity as a whole is comparable across all three. A breakdown by segment or by class is available on two of them: a quarterly report stating 941 facts is carried in a company file as 321. A reader of the combined table is entitled to know that an absent breakdown may mean the company reported none or may mean the publisher does not give them out, and that which of the two it is depends on the source.

Two things about the Japanese rows have to be read before they are used, and both are properties of the regime rather than of this module.

**A report that consolidates states every figure twice**, the group's with no dimension and the parent company's own under `NonConsolidatedMember`, and both are in this table because both are in the source table. A question that does not say which it wants is asking for two sets of accounts at once. Kanagawa Chuo Kotsu states earnings per share of 294.99 for the group and 236.58 for the parent in one table of one report.

**`JP-GAAP` is not comparable with the other two line by line.** A Japanese profit and loss account has two results where an IFRS one has a single operating line: the operating result, and the ordinary result that adds the recurring items outside it. The extraordinary items below that are a presentation an IFRS statement does not have. `framework` is read from what each filing says it reports under rather than from the prefix of a fact, because a Japanese group that has adopted IFRS tags its statements against the Japanese IFRS taxonomy and its front matter against the same disclosure taxonomy every other filer uses — so the prefix does not settle it, and the filing does.

One kind of Japanese fact is not carried, and it is the only thing this module drops. The agency's conversion carries no dates, so the edinet module resolves a period from what the filing says about itself and leaves both dates empty for a context whose shape it does not recognise. `prd_end` is the one column of the combined table of facts that may not be empty — a fact in no period can be compared with nothing, which is what this table is for — so such a fact stays in the edinet table of facts, where that module's `PERIOD_UNRESOLVED` check counts them.

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

Unlike the source modules, no `path` is needed beside the `table`: they keep marks on disk recording what their tables were built from, and this module keeps none.

The sections of the sources have to be there as well, because this module reads their tables. Which sources a run combines is decided by which of the three have tables to read, not by this module: a lake that has collected only ESEF filings gets a combined table of ESEF filings, which is the same question answered over the population there is.

## The module in five minutes

```python
from fincommons.financialreport import combined

# Rebuild both tables from whichever sources the lake holds.
combined.update_silver_financialreport(spark, "paths.yaml")

combined.available_sources(spark, "paths.yaml")          # ['esef', 'sec', 'edinet']

facts = combined.connect_to_financialreport(spark, "paths.yaml", "fact")
filings = combined.connect_to_financialreport(spark, "paths.yaml", "filing")

# The revenue of every filing that reports under IFRS, whichever regime it
# was filed under, taken by the best concept the filing states.
revenue = (
    combined.measure_values(facts.where("framework = 'IFRS'"), "revenue")
    .select("source", "lei", "cik", "edinet_code", "period_end", "concept",
            "unit", "value"))

# What that rests on, before it is published.
print(combined.describe_measure("revenue"))
```

`update_silver_financialreport` is the call a job scheduler makes, after the source pipelines have run. It takes `sources` to restrict the run to one of them, and `file_format` to say how the tables are stored. A run that finds no source at all raises rather than writing empty tables; a run that finds only some of the three writes those and logs which it left out.

## The Silver tables

### `fact`

One row per fact of a filing, partitioned by `source` and then `period_year`: most questions are scoped to one regime, and a question about a figure is scoped to a year after that.

| Column | Meaning |
| --- | --- |
| `source` | which module the row came from, `esef`, `sec` or `edinet` |
| `filing` | the filing, as its own regime identifies it |
| `fact` | the identifier of the fact within its filing |
| `lei`, `cik`, `edinet_code` | the entity; exactly one is filled, and none is derived from another |
| `jcn` | the corporate number of a Japanese filer, where the agency states one; empty for the other two regimes |
| `framework` | `IFRS`, `US-GAAP` or `JP-GAAP`; see below |
| `period_end`, `period_year` | the period the filing reports on, and its year |
| `concept` | the concept as the filing tagged it, prefix and name, unresolved across frameworks |
| `measure` | the measure of the catalogue the concept is the first choice for, where it is one; see below |
| `prd_start`, `prd_end` | the period of the fact; an instant has no `prd_start` |
| `unit`, `value` | a number and its unit, as reported and in full units |
| `text`, `dimensions` | filled for ESEF and EDINET rows; empty for every SEC row |

For ESEF and SEC rows `framework` is taken from the taxonomy the filing tagged against and not from the regime it was filed under. That is what makes the population where the two genuinely meet usable: a foreign private issuer reporting under IFRS files with the Commission and tags against the IFRS taxonomy, and belongs beside the European reports rather than beside the American ones it was filed with.

`text` and `dimensions` are empty for every SEC row, and not because this module drops them: the company files of the Commission carry no narrative facts and no breakdowns at all. An empty `dimensions` means the fact is for the entity as a whole, which is true of every row the Commission contributes.

For EDINET rows the taxonomy settles `framework` where it can -- `jpigp_cor` is IFRS and `jppfs_cor` is Japan GAAP -- and the standard the filing states settles the rest. A Japanese IFRS filer therefore has its group's statements under `IFRS` and its parent company's own statements under `JP-GAAP`, in one report. A filing that no accounting standard applies to, such as a large-holding report or a fund's report, states the conversion's mark for not applicable, and its `framework` is empty rather than that mark.

### `filing`

One row per filing.

| Column | Meaning |
| --- | --- |
| `source`, `filing` | which module, and the filing |
| `lei`, `cik`, `edinet_code`, `jcn`, `name` | the entity |
| `framework` | `IFRS`, `US-GAAP` or `JP-GAAP`: for ESEF and SEC from the concepts the filing tagged, for EDINET the standard the filing states; empty where none applies |
| `country` | the member state the report was filed in; ESEF only |
| `form` | the form the filing was made on: `10-K` and the like for SEC, the agency's document type for EDINET (`120` is the annual securities report) |
| `period_end`, `period_year` | the period reported on, and its year |
| `facts` | how many facts the filing contributed |

`country` and `form` are each empty where their regime states nothing: an ESEF filing goes to the mechanism of one member state and the regime names no form, while a filing with the Commission or the agency names a form and no country. Neither is given a value this module invented. For an ESEF filing the framework and the count come from the facts, which is where the evidence for both is -- the repository's index says nothing about what a report tagged against.

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

The namespace is deliberately not used: a published taxonomy takes a new one every year, and neither source table carries it. The prefix is what all three tables hold -- the ESEF rows because xBRL-JSON names a concept that way, the SEC rows because the Commission keys its facts by taxonomy, the EDINET rows because the agency's conversion writes the element identifier as prefix and name.

**Nothing in this catalogue is authoritative, and the module says so.** There is no official correspondence between IFRS and US GAAP: the convergence programme of the two boards did not produce one, and the data quality rules published for filings with the Commission do not cross frameworks. What is shipped is a starting point a deployment should review and extend, and the `source` column exists so that a row somebody has reviewed can be told from a row nobody has.

```python
from fincommons.financialreport import combined as fr

fr.measures()                        # the names defined
fr.measure_catalogue(measure="equity")       # the rows, as a data frame
fr.concepts_for("revenue", "US-GAAP")        # [('us-gaap', 'RevenueFromContract...'), ...]
print(fr.describe_measure("revenue"))        # basis, caveat and source, printable
fr.caveats()                                 # every row that carries one
```

`with_measures(facts)` labels a table of facts with `measure` and `measure_priority`, leaving both null for the great majority of facts, which no row of the catalogue names. `measure_values(facts, "revenue")` goes one step further and returns one value per filing and period, by the best concept the filing states; it logs a warning where the concepts it resolved through carry caveats, and it excludes facts reported under a dimension, since a measure is the figure for the entity as a whole and summing the parts beside the whole is the arithmetic that exclusion exists to prevent.

Both join on `concept` as the tables spell it, which means **the catalogue works without this module's tables**. An ESEF-only lake can resolve a measure over `fincommons.financialreport.esef` directly:

```python
from fincommons.financialreport import esef
from fincommons.financialreport.combined import measure_values

measure_values(esef.connect_to_esef(spark, "paths.yaml"), "assets")
```

A deployment that keeps its own judgements ships a catalogue of the same shape in the data directory of its own package and names it: `measure_catalogue(packages=("fincommons", "mycompany"))`. Its rows are read beside these, which is how a reviewed mapping is kept without editing the one this package ships.

The module stops at resolution. It says which concepts a measure is taken to be and labels facts accordingly; it does not define ratios, aggregate across entities or build a metrics layer. Those belong to whoever is asking the question, and to the package they ship, rather than to the infrastructure that holds the data.

### A comparison, honestly

A measure resolved across both regimes is comparable only as far as its caveat says, and the caveats are not decoration. Revenue is the clearest case: `ifrs-full:Revenue` may include revenue from sources other than contracts with customers, which ASC 606 reports separately; the US split between revenue excluding and including assessed tax has no IFRS counterpart at all, so a filer tagging the including variant reports a larger figure than an IFRS filer would for the same business; and `us-gaap:Revenues` predates ASC 606, is tagged inconsistently, and is the last resort rather than an equivalent of the IFRS total. Anyone publishing a cross-regime comparison should print `caveats()` beside it.

## Reports

Two report templates in `templates/` read the two tables as Parquet and render with Quarto to PDF. Both need the corporate design script of the package's `templates` directory and the catalogue of measures in its `data` directory, which they find from where they stand in the package, or beside them where they are copied out. Render with a UTF-8 locale.

```bash
FINCOMMONS_CONFIG=/path/to/paths.yaml quarto render financialreport-coverage-report.qmd
FINCOMMONS_CONFIG=/path/to/paths.yaml FINCOMMONS_YEAR=2024 quarto render financialreport-comparison-report.qmd
```

**`financialreport-coverage-report.qmd`** says what the lake holds: for each regime the filings listed and read, the issuers, the facts, the years and the frameworks; the forms read from the Commission and the agency; which of the eight measures the annual reports of each regime and framework state; how often the result is the total and how often only the parent's share; and the first concept of each measure under each framework, with the caveats counted.

**`financialreport-comparison-report.qmd`** compares companies across the regimes on three ratios of figures of the same report -- net margin, equity ratio and return on equity -- so that no currency is converted. It takes one annual report per company and year, the result as the total where it is stated and the parent's share where it is not, and prints the catalogue's caveats on the measures it uses. The year is the latest one in which at least two regimes hold ten companies, or `FINCOMMONS_YEAR`.

Both apply three rules the tables do not apply themselves, and each was found on a lake rather than assumed:

* **Investment funds are not companies.** A Japanese fund files its annual report under document type 120, as a company does, and in a lake that read a few weeks of the agency's indexes most annual reports are funds' -- 456 of 605 in the one the reports were written against. A fund's report tags the fund taxonomies `jpsps*` or the elements of the financial statements taxonomy whose names end in `FND`; the coverage report counts these apart, and neither report puts them in a statistic of the measures.
* **A Japanese filer without subsidiaries states its accounts under `NonConsolidatedMember`**, and is read from them, as `measure_values` reads it. Of 78 Japanese annual reports of companies, 14 state their measures only so.
* **A measure counts under the framework of the filing.** A Japanese group reporting under IFRS states its parent company's own accounts under Japan GAAP in the same report, and those are not the report's figures.

The comparison report also leaves out banks and insurers, found from the lines their own reports present: the IFRS and US-GAAP lines of the esef factsheets, and for a Japanese filer the industry elements whose names end in `BNK`, `INS` and `SEC`.
