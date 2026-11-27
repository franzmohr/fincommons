# Financial reports

`fincommons.financialreport` brings the XBRL-tagged periodic reports of listed
companies into the data lake, fact by fact: revenue, assets, cash flows and
every other figure an issuer tagged, from Europe, the United States and Japan.
Use it to read what a company reported over the years, to compare companies
of one country or sector, or to put issuers of the three regimes side by side.

It is a namespace of four modules, each with its own README. Start with the
one for the regime you need; this page says which, how they fit together, and
what they share.

| Module | Regime | Publisher | README |
| --- | --- | --- | --- |
| `esef` | the European Single Electronic Format (ESEF), plus the UK and Ukraine | [filings.xbrl.org](https://filings.xbrl.org) | [esef](esef/README.md) |
| `sec` | the inline XBRL mandate of the U.S. Securities and Exchange Commission | `data.sec.gov` | [sec](sec/README.md) |
| `edinet` | the XBRL Japan's Financial Services Agency requires, published through EDINET | `disclosure2.edinet-fsa.go.jp` | [edinet](edinet/README.md) |
| `combined` | the three reduced to the same columns, in one table of facts and one of filings | none: it reads the other three | [combined](combined/README.md) |

## Which one to start with

| You want to | Start with |
| --- | --- |
| read the reports of EU-listed issuers, by country | [`esef`](esef/README.md) |
| read a US-listed company's 10-K and 10-Q figures, or see what a later report restated | [`sec`](sec/README.md) |
| read a Japanese company's securities report, including parent-only statements, workforce and largest shareholders | [`edinet`](edinet/README.md) |
| ask one question of all three -- total assets of European, American and Japanese issuers, revenue across IFRS, US GAAP and Japan GAAP | the source modules you need, then [`combined`](combined/README.md) |

## Quick start

1. Add a section for each regime you collect, and one for `combined` if you
   use it, to `paths.yaml`:

   ```yaml
   esef:
       bronze: /data/esef/bronze
       silver: /data/esef/silver
   sec:
       bronze: /data/sec/bronze
       silver: /data/sec/silver
   edinet:
       bronze: /data/edinet/bronze
       silver: /data/edinet/silver
   financialreport:              # combined: a Silver layer only
       silver: /data/financialreport/silver
   ```

2. Run the source pipelines you need, then the combining layer. `spark` is a
   session with the Delta extensions; see
   [Setting up a Spark session](../../../docs/spark-session.md).

   ```python
   from fincommons.financialreport import esef, sec, edinet, combined

   esef.update_esef(spark, "paths.yaml", ["AT"], contact="me@example.org")     # one country
   sec.update_sec(spark, "paths.yaml", ["0001045810"], contact="me@example.org")  # one registrant (CIK)
   edinet.update_edinet(spark, "paths.yaml")                                     # the last thirty days; needs an API key

   combined.update_silver_financialreport(spark, "paths.yaml")   # from whichever sources the lake holds
   facts = combined.connect_to_financialreport(spark, "paths.yaml", "fact")
   ```

   Each source module's README has the full quick start for that regime,
   including what a first run downloads.

## What each regime needs

| | `esef` | `sec` | `edinet` | `combined` |
| --- | --- | --- | --- | --- |
| Credentials | none | a **contact address** (`contact=`); requests without one get `403 Forbidden` | a free, **registered API key** (`key=`, `FINCOMMONS_EDINET_KEY`, or a file `edinet` in `FINCOMMONS_VAULT`; never in `paths.yaml`) | none |
| Unit of a run | a country | a registrant | a range of calendar days | everything the lake holds |
| Network | yes | yes | yes | no |
| History limit | what the repository holds | what the company file holds | **filings older than ten years can no longer be fetched** | that of the sources |

## How they differ

The three publishers give out different things, and the tables keep the
difference rather than hide it. As `combined.coverage()` states it:

| | `esef` | `sec` | `edinet` |
| --- | --- | --- | --- |
| Entity key | LEI | central index key (CIK), ten digits | EDINET code (`E` and five digits); corporate number `jcn` where stated |
| Facts | every fact of the report, as xBRL-JSON | the undimensioned numeric facts of the filing | every fact of the report, as the agency's own CSV |
| Dimensions and text | yes | **no** -- a quarterly report stating 941 facts is carried as 321: the breakdowns by segment and by class are absent, and the narrative with them | yes |
| Watch out for | the repository collects what it can from the national mechanisms, which is a large part of the population and not the whole of it | the missing breakdowns above | a report that consolidates states every figure twice: the group's without a dimension and the parent company's under `NonConsolidatedMember`; and the period of a fact older than the comparative is shifted by whole years rather than reported |

## Joining to other data

- **ESEF → entity master data.** The facts are keyed by LEI, which joins to
  [`entitymasterdata`](../entitymasterdata/README.md) and
  [`gleif`](../gleif/README.md).
- **SEC → LEI: not possible.** Neither the Commission nor GLEIF publishes a
  mapping from a central index key to an LEI; see the
  [sec README](sec/README.md#no-mapping-to-the-lei).
- **EDINET → LEI: for some filers**, through the corporate number. In a
  sample of 200 listed filers, 13.5 per cent reached an LEI -- most Japanese
  listed companies are not in the LEI system at all; see the
  [edinet README](edinet/README.md#the-corporate-number-reaches-the-lei).
- **Across regimes:** `combined` carries `lei`, `cik` and `edinet_code` side
  by side, exactly one filled per row, and derives none from another.
- **Amounts in one currency:** convert with
  [`exchangerate.convert2ccy`](../exchangerate/README.md); the esef README
  shows how.

## Comparing a figure across regimes

Each framework names the same figure differently -- `ifrs-full:Revenue`,
`us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax`,
`jppfs_cor:NetSales` -- and the concepts are close, not identical.
`combined` ships a catalogue of eight *measures* (assets, revenue, ...) that
says which concept of each framework is taken to be the same figure, on what
basis, and with which caveats:

```python
from fincommons.financialreport import combined

print(combined.describe_measure("revenue"))   # every concept, its basis and its caveat
combined.measure_values(facts.where("period_year = 2024"), "revenue")   # one value per filing and period
```

Read the caveats before publishing a comparison. The
[combined README](combined/README.md) explains the `measure` column and the
catalogue in full.

## What the modules share

Each source module follows the same pattern, so what you learn in one carries
over:

- **Layers:** a Bronze layer (the files as downloaded) and a Silver layer of
  `filings`, `facts` and, after the checks, `issues`. A source module creates
  its directories with `create_directory_tree`, or `update_esef` does so
  itself; each opens its tables with `connect_to_<module>`.
- **Incremental runs:** each run collects only what is new or changed, and is
  safe to repeat.
- **Data quality:** `find_issues` and `update_silver_<module>_quality` (for
  example `update_silver_sec_quality`) run the module's checks and write the
  `issues` table; each README lists its checks.
- **Reports:** Quarto templates in each module's `templates/` folder -- an
  analyst factsheet on one issuer, and for some modules a data quality,
  country, coverage or comparison report -- rendered to tagged PDF; each
  README says which it has and how to render them.

## Why a namespace

`fincommons.financialreport` imports none of the four modules, and that is
deliberate. Each regime stands on its own: a lake that collects Japanese
filings and nothing else imports `fincommons.financialreport.edinet`, and
neither the combining layer nor any other regime is loaded by doing so.

The dependency runs one way: `combined` reads the three, none of the three
reads `combined`, and none reads another. `tests/test_combined.py` holds it
to that. Import the module you need by its full name:

```python
from fincommons.financialreport import esef        # not: import fincommons.financialreport as fr; fr.esef
```
