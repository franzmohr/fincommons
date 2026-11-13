# Financial reports

## Introduction

Module `financialreport` puts the filings of both tagging regimes side by side. Two of them require the periodic reports of listed issuers to be tagged: the [European Single Electronic Format](https://www.esma.europa.eu/issuer-disclosure/electronic-reporting) in the Union, and the inline XBRL mandate of the U.S. Securities and Exchange Commission. [`esef`](../esef/README.md) and [`secfiling`](../secfiling/README.md) each bring one of them into the lake as its publisher gives it out, in the shape that publisher gives it. This module reduces the two to the same columns and writes one table of facts and one of filings, so that a question put to both at once is put to one table rather than to two of different shapes.

It holds no data of its own and collects nothing. There is no Bronze layer here, nothing is downloaded, and both tables are derived entirely from tables that are themselves rebuilt -- so both are rebuilt in full on every run, with no watermark to keep and nothing to resume from.

### Three things it deliberately does not do

**It takes nothing away from the two source modules.** Their tables stay authoritative and complete, neither imports this one, and neither knows it exists. An ESEF-only lake builds nothing here and loses nothing by it; the factsheet and the coverage report that `esef` ships keep working untouched, as does the factsheet of `secfiling`. The dependency runs one way, and only one way.

**It does not join the regimes by entity, because it cannot.** ESEF prescribes the legal entity identifier; a filing with the Commission states a central index key; and no authoritative mapping between the two is published by either body. Both columns are carried and exactly one of them is filled in any row. It is the one thing a lake holding both regimes would most like to do, and the honest shape of the table says so rather than filling a column with a guess.

**It does not reconcile the vocabularies in the tables.** `ifrs-full:Revenue` and `us-gaap:Revenues` stand as they were filed. What the two have in common is a judgement about accounting, which `mapping` records as data rather than as code, and which is applied per query by whoever has read what it rests on.

### What each source covers

The two publishers do not give out the same thing, and the difference is a property of what each publishes rather than of this module. `coverage()` returns it as a list of dictionaries, so that a notebook can print it beside a result:

| | `esef` | `secfiling` |
| --- | --- | --- |
| publisher | filings.xbrl.org | sec.gov |
| facts | every fact of the report, as xBRL-JSON | the undimensioned numeric facts of the filing |
| dimensions | yes | no |
| text blocks and narrative | yes | no |
| entity key | `LEI` | `CIK` |

A figure for an entity as a whole is therefore comparable across the two. A breakdown by segment or by class is available on one side only: a quarterly report stating 941 facts is carried in a company file as 321. A reader of the combined table is entitled to know that an absent breakdown may mean the issuer reported none or may mean the publisher does not give them out, and that which of the two it is depends on the source.

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
        table: fincommons.financialreport
```

Unlike `esef` and `secfiling`, no `path` is needed beside the `table`: those two keep marks on disk recording what their tables were built from, and this module keeps none.

The sections of the sources have to be there as well, because this module reads their tables. Which sources a run combines is decided by which of the two have tables to read, not by this module: a lake that has collected only ESEF filings gets a combined table of ESEF filings, which is the same question answered over the population there is.

## The module in five minutes

```python
from fincommons import financialreport

# Rebuild both tables from whichever sources the lake holds.
financialreport.update_silver_financialreport(spark, "paths.yaml")

financialreport.available_sources(spark, "paths.yaml")   # ['esef', 'secfiling']

facts = financialreport.connect_to_financialreport(spark, "paths.yaml", "fact")
filings = financialreport.connect_to_financialreport(spark, "paths.yaml", "filing")

# The revenue of every filing that reports under IFRS, whichever regime it
# was filed under, taken by the best concept the filing states.
revenue = (
    financialreport.measure_values(facts.where("FRAMEWORK = 'IFRS'"), "revenue")
    .select("SOURCE", "LEI", "CIK", "PERIODEND", "CONCEPT", "UNIT", "VALUE"))

# What that rests on, before it is published.
print(financialreport.describe_measure("revenue"))
```

`update_silver_financialreport` is the call a job scheduler makes, after the source pipelines have run. It takes `sources` to restrict the run to one of them, and `file_format` to say how the tables are stored. A run that finds no source at all raises rather than writing empty tables; a run that finds one of the two writes that one and logs that it did.

## The Silver tables

### `fact`

One row per fact of a filing, partitioned by `SOURCE` and then `PERIODYEAR`: most questions are scoped to one regime, and a question about a figure is scoped to a year after that.

| Column | Meaning |
| --- | --- |
| `SOURCE` | which module the row came from, `esef` or `secfiling` |
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

The judgement is data and not code: a CSV shipped in the data directory of the package, `financialreport_measures.csv`, beside the column catalogue and the FIRDS schemas. It currently defines **8 measures in 22 rows**, of which **13 carry a caveat**.

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
from fincommons import financialreport as fr

fr.measures()                        # the names defined
fr.measure_catalogue(measure="equity")       # the rows, as a data frame
fr.concepts_for("revenue", "US-GAAP")        # [('us-gaap', 'RevenueFromContract...'), ...]
print(fr.describe_measure("revenue"))        # basis, caveat and source, printable
fr.caveats()                                 # every row that carries one
```

`with_measures(facts)` labels a table of facts with `MEASURE` and `MEASURE_PRIORITY`, leaving both null for the great majority of facts, which no row of the catalogue names. `measure_values(facts, "revenue")` goes one step further and returns one value per filing and period, by the best concept the filing states; it logs a warning where the concepts it resolved through carry caveats, and it excludes facts reported under a dimension, since a measure is the figure for the entity as a whole and summing the parts beside the whole is the arithmetic that exclusion exists to prevent.

Both join on `CONCEPT` as the tables spell it, which means **the catalogue works without this module's tables**. An ESEF-only lake can resolve a measure over `fincommons.esef` directly:

```python
from fincommons import esef
from fincommons.financialreport import measure_values

measure_values(esef.connect_to_esef(spark, "paths.yaml"), "assets")
```

A deployment that keeps its own judgements ships a catalogue of the same shape in the data directory of its own package and names it: `measure_catalogue(packages=("fincommons", "mycompany"))`. Its rows are read beside these, which is how a reviewed mapping is kept without editing the one this package ships.

The module stops at resolution. It says which concepts a measure is taken to be and labels facts accordingly; it does not define ratios, aggregate across entities or build a metrics layer. Those belong to whoever is asking the question, and to the package they ship, rather than to the infrastructure that holds the data.

### A comparison, honestly

A measure resolved across both regimes is comparable only as far as its caveat says, and the caveats are not decoration. Revenue is the clearest case: `ifrs-full:Revenue` may include revenue from sources other than contracts with customers, which ASC 606 reports separately; the US split between revenue excluding and including assessed tax has no IFRS counterpart at all, so a filer tagging the including variant reports a larger figure than an IFRS filer would for the same business; and `us-gaap:Revenues` predates ASC 606, is tagged inconsistently, and is the last resort rather than an equivalent of the IFRS total. Anyone publishing a cross-regime comparison should print `caveats()` beside it.
