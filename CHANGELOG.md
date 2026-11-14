# Changelog

The notable changes of each release. The project follows
[semantic versioning](https://semver.org/); while the version is 0.x, a minor
release may change the public API, which `tests/test_public_api.py` lists.

## Unreleased

### New

- `edinet` and `financialreport`: the entity question is settled, and the
  answer is yes. The corporate number of the National Tax Agency, which the
  table of filings carries as `JCN`, is not an identifier of its own: it is a
  check digit followed by the twelve-digit 会社法人等番号 of the commercial
  register, and GLEIF records that register number as the registration
  authority entity id of an LEI issued to a Japanese company. Dropping the
  check digit and hyphenating the rest -- `1020001071491` to `0200-01-071491`
  -- reaches Fujitsu's LEI `3538000246DHJLRTUZ24` exactly. The join is
  arithmetic rather than a lookup table and rests on a number rather than a
  name, which is what makes it survive a rename: EDINET has E24558 as
  株式会社ミライト・ワン and GLEIF still has the LEI under the name before the
  group renamed itself.
  How far it reaches was measured rather than assumed. Of a random sample of
  200 of the 3,810 listed filers the agency's list gives a thirteen-digit
  corporate number for, 27 reached an LEI -- 13.5 per cent -- and every match
  named the same register, `RA000412`, the 登記 register of the Legal Affairs
  Bureau. Ten of the misses were then looked up by name in GLEIF and not one
  had an LEI under any spelling, so the join is not failing on them: most
  Japanese listed companies are not in the LEI system at all. That is enough
  to put a Japanese filer beside a European one where both exist and not
  enough to key the combined tables by, so the corporate number stays the key
  for Japan and the LEI is a column filled where it can be.
  This is the only identifier in the family of modules that reaches the LEI.
  The Commission's central index key does not: an operating company's LEI
  record points at its state register, NVIDIA's at Delaware by a file number
  that is not its central index key. Both READMEs now say which of the three
  regimes can be joined by entity and which cannot.

- `edinet`: the checks the figures are held to, and a table of the issues
  they find: `src/fincommons/financialreport/edinet/quality.py`.
  `update_silver_edinet_quality` writes a fourth Silver table, `issues`,
  beside the three the module collects; no section of the configuration file
  has to be added for it, and a lake that never runs the checks has no such
  table. The machinery is the one `fincommons/dataquality.py` already holds
  and what is here are the twenty-six checks themselves. Nothing is dropped
  and nothing is corrected: a check writes a row and leaves the figure where
  it is, and a filing with nothing wrong with it has no row at all.
  Two of the arithmetic checks exist only here, and they are the two lines
  that make a Japanese profit and loss account what it is. `IMBAL_ORDINARY`
  sets the operating result and the recurring items outside it against
  経常利益; `IMBAL_PRETAX` sets the ordinary result and the extraordinary
  items, 特別損益, against the result before tax. Neither relation can be
  written for an IFRS or a US GAAP filer, neither having the lines.
  Four checks have no counterpart anywhere in the package. `SUMMARY_DISAGREES`
  sets the five-year summary of the front matter against the statements on
  the year they both state, which is the only check of the package that tests
  a filing against another part of itself. `PERIOD_SHIFT_INEXACT` asks whether
  the periods this module resolves can be trusted: anything older than the
  comparative is shifted by whole years, which is exact only where the filer's
  years are of equal length, and the filing states the first and last day of
  both of its two most recent years -- so the filing answers the question
  about itself, with three days of slack so that a leap year is not mistaken
  for a 52-week calendar. `BASIS_CONTRADICTS_DIMENSION` sets 連結・個別
  against the presence of `NonConsolidatedMember` in the context, a
  correspondence that held for every one of 6,752 facts of the reports read
  while the module was written. And `UNIT_NOT_A_RATIO` reports an element
  named for a ratio carrying a unit no ratio can have, which is the only one
  of the four that has found anything in a real report: Fujitsu tags
  `EquityToAssetRatioIFRSSummaryOfBusinessResults` with 1,167.25 and the unit
  `JPYPerShares` -- the equity per share, under the name of a ratio -- while
  the ratio itself is in the same report under
  `RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults` as 0.60.
  Two things are settled once rather than by each check. The figures of a
  filing are the ones it states for its own period end on the basis the
  filing itself says it reports on, and a fact carrying a member on top of
  the consolidation one is a breakdown rather than a statement line -- the
  statement of changes in equity tags the net assets once per component of
  equity. And a component of a sum being tested has to be stated: treating an
  absent line as nothing is what turns a filing that presents its statements
  differently into one that fails arithmetic, and the first draft reported
  Fujitsu's liabilities as short by 244,695 million because an IFRS statement
  subtotals its current liabilities and leaves the non-current side to be
  read as the remainder. Only a genuinely optional line is coalesced, and
  each is named where it is added.
  Run over four real annual securities reports -- Kojima, which prepares no
  consolidated statements; Kanagawa Chuo Kotsu twice, consolidated under
  Japan GAAP; Fujitsu, consolidated under IFRS -- the checks report five
  issues, all five being Fujitsu's mis-tagged ratio. Calibrating them against
  those reports removed four false positives, every one of them a line a
  filing legitimately does not state.
- `edinet`: a section of the module's README on the checks, and
  `connect_to_edinet(..., "issues")` to read the table they write.

- `edinet`: a README for the module, which it did not have:
  `src/fincommons/financialreport/edinet/README.md`. It sets out the one way
  this regime differs in kind from the other two -- a filing is retrievable
  for ten years and then it is not, so the Bronze layer is the only lasting
  copy and a year not collected is a year lost rather than a year postponed
  -- together with what the agency's conversion carries and leaves out, why
  the periods in the table of facts are resolved rather than reported, where
  the API key comes from and why it is never in `paths.yaml`, the three
  Silver tables, and the factsheet. Two sections say what a reader has to
  know before querying: that a Japanese report states its figures twice, the
  consolidated accounts under no dimension and the parent company's own under
  `NonConsolidatedMember`, so that a query taking the first row that matches
  mixes two sets of accounts; and that `YEAREND` is text because 1,633 of the
  3,816 listed filers do not close in March and the agency writes the end of
  March two ways.

- `edinet`: a template for a factsheet on one Japanese filer, in R and
  Quarto, with Kojima Co., Ltd. as its example:
  `src/fincommons/financialreport/edinet/templates/edinet-analyst-factsheet.qmd`.
  It renders a PDF of some twenty pages from the facts of the filer's own
  annual securities reports and from nothing else. Six things in it have no
  counterpart in the esef or the sec factsheet, and each follows from what
  the Japanese ordinance asks a filer for.
  The five-year summary of the front matter, 主要な経営指標等の推移, carries
  revenue, the ordinary result, the net result, total assets, equity, the
  three cash flows, the employee count and the filer's own equity ratio,
  return on equity, payout ratio and price-earnings ratio for five years in
  one report. A lake holding a single annual report therefore shows a
  five-year series, which neither of the other two regimes can do; the
  statements of that same report cover the year and its comparative, so the
  factsheet keeps two year axes and says which covers what rather than
  padding the short one out with dashes.
  The consolidation basis is the first thing a Japanese report asks to have
  decided, and it is not a formality. A filer that consolidates states every
  figure twice -- the group with no dimension and the parent company under
  `NonConsolidatedMember` -- and Kanagawa Chuo Kotsu reports earnings per
  share of 294.99 for the group and 236.58 for the parent in the same table
  of the same report. A filer that does not consolidate states its accounts
  under the member alone, so requiring an empty dimension would empty the
  factsheet. `consolidation` is `"auto"` by default and the filing's own
  連結決算の有無 decides; the agency's list of filers disagrees with the
  filing for Kojima, and the profile page says so rather than preferring one
  silently. Only ten lines may be taken from the other basis -- the stated
  capital, the shares issued, the dividend and its payout, the shareholder
  return, the workforce figures -- because those are properties of the
  company however it reports, and they are marked ‡ where they are. Nothing
  else falls back: Fujitsu reports its group under IFRS and its parent under
  Japan GAAP, and an unrestricted fallback put 275,236 million of
  parent-company 経常利益 on the cover of a group factsheet whose revenue is
  3,502,971 million.
  A concept is matched on its local name and never on the prefix, because
  three of the four prefixes of a conversion are fixed and the fourth is
  built from the form and the filer's own EDINET code. Fujitsu's operating
  profit is
  `jpcrp030000-asr_E01766-000:OperatingProfitIFRSSummaryOfBusinessResults`:
  an element of the five-year summary under the filer's own prefix, the core
  taxonomy having no operating profit for an IFRS filer. Matching on the
  prefix as well would lose it silently.
  Every line declares the unit class it accepts and a fact of another class
  is refused and reported. In Fujitsu's own summary the element
  `EquityToAssetRatioIFRSSummaryOfBusinessResults` carries 1,167.25 with the
  unit `JPYPerShares`: the equity per share, under the name of a ratio. A
  factsheet trusting the name would print an equity ratio of 1,167 per cent,
  and the ratio is in the same report under
  `RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults` as 0.60. So
  the same element serves two keys and the unit decides which.
  No per-share figure is adjusted for a share split. A Japanese filer
  restates the earlier years of its own five-year summary at its discretion
  and not always for every year of it: Fujitsu's report states 207.0, 207.0,
  207.1, 2,071.1 and 1,739.8 million shares against earnings per share of
  924.21, 110.76, 135.59, 120.93 and 254.83, which is four years restated
  onto the shares after the ten-for-one split of October 2024 and the fifth
  left on the shares before it. A factor derived from the share counts would
  divide the four that are already restated, so the year the count moved is
  named instead and the reader is told the series spans it.
  Three sections exist only here. The workforce -- the employee count over
  five years, the average age, the length of service and the annual pay --
  which the ordinance asks of the filer and neither other regime requires.
  The holders of the shares: the ten largest by name with their holdings, and
  the register by class of investor. And the total shareholder return over
  five years against the index the filer names, which with the
  price-earnings ratio is the only market figure in a document that fetches
  no market data, both being the filer's own statements.
  Fourteen checks close the document, among them one that tests the reading
  of both year axes at once: the summary and the statements must agree about
  the year they share. Thirteen of the fourteen hold for Kojima, fourteen for
  Kanagawa Chuo Kotsu and thirteen for Fujitsu, whose remaining difference is
  an operating income it presents between its gross profit and its operating
  result. `labels` sets the tables in English, in the filer's own Japanese or
  in both; `cjk_fonts` names the fallback typst reaches for the Japanese
  characters, without which it sets them as empty boxes and says nothing.
  The last page is a disclaimer, which the cover points at.

- `dataquality`, `esef.quality` and `sec.quality`: the checks the two
  regimes' figures are held to, and a table of the issues they find.
  `fincommons/dataquality.py` holds what does not depend on the data set --
  how a check is described, how its rows are gathered, which partitions a run
  still has to look at, what is written where it looked at part of the table
  only, and how the issues are counted and joined back onto the records --
  and each module holds its own checks and declares its own issue table,
  because the record a check points at is identified differently in each.
  `update_silver_esef_quality` and `update_silver_sec_quality` write a
  third Silver table, `issues`, beside the tables they are derived from; no
  section of the configuration file has to be added for it, and a lake that
  never runs the checks has no such table. Nothing is dropped and nothing is
  corrected: a check writes a row and leaves the figure where it is, and a
  filing with nothing wrong with it has no row at all.
  Thirty-four checks, eighteen for ESEF and sixteen for the Commission. The
  valuable ones are the relations a statement states on both sides -- the
  balance sheet, revenue against the cost and the gross profit, the result
  before tax against the tax and the result, the three cash flows against the
  movement in cash -- because the scale of a number is an attribute of the tag
  and invisible on the page. The rest are structural. One check belongs to the
  Commission alone: a company file carries every filing's account of every
  period, so `RESTATED_MATERIALLY` can set what a year's own report said
  beside what a later report says, which found 149 restatements across
  NVIDIA's 69 filings; share counts and per-share amounts are passed over,
  a split restating those by arithmetic. Two belong to the repository alone,
  which validates what it publishes and lists what it could not convert.
  What a code means, how severe it is and what to do about it are rows of
  `data/dataquality_checks.csv` rather than branches in the code, as the
  measures of `financialreport` are, so that the people most able to find a
  check wrong can read it; `describe_check` prints one, and a deployment that
  keeps checks of its own ships a catalogue of the same shape and names it.
  The checks were run over NVIDIA's 69 filings and six real voestalpine
  reports, which corrected three of them: mezzanine equity belongs to the
  balance sheet identity, the discontinued operations belong between the
  result before tax and the result, and whether the repository published any
  facts is a question for the index rather than for the table of facts -- the
  last reporting 24,174 filings nobody had downloaded instead of the 192 the
  repository could not convert.
- `esef` and `sec`: a template for a data quality report, one per
  regime, in R and Quarto, which reads the table of issues over the whole
  record rather than filing by filing. Sixteen pages for ESEF and fifteen for
  the Commission, built as a pair in the same way the factsheets are: what
  was checked and what the checks have not run over, every check ranked by
  the share of filings it named, the bands of findings per filing, the share
  of the record free of errors, the five identities read together, and every
  entry of the catalogue with what it found here. No number in either is an
  amount out of a filing.
  Two distinctions carry both reports and neither is left to be inferred. A
  share is only readable against the right denominator, so each report takes
  every rate over the one population every check was asked of -- the filings
  the table of facts holds facts for, the annual reports -- and the per-check
  table keeps each check's own. And a check names the records that failed it,
  the filing where the fault is the report's and the fact where one figure is
  at fault, so findings are rows and filings are the filings those rows name;
  no table adds one to the other, and the headline of each report is the share
  of the record free of errors rather than a count of findings. A partition
  the checks have never run over and one in which every check passed look the
  same in the two tables, and both reports name the former rather than count
  it as sound.
  The catalogue of checks is read from `data/dataquality_checks.csv`, the same
  file the module reads, so a check added to a module appears in the report
  without the template being touched; a code in the table of issues the
  catalogue does not describe is refused rather than printed bare, as is a
  Delta Silver layer. The sec report gives `RESTATED_MATERIALLY` a
  section of its own, it being the one check without a counterpart in the
  other regime and the one that needs reading rather than counting.
- `sec`: a template for a factsheet on one registrant, in R and Quarto,
  with NVIDIA Corporation as its example:
  `src/fincommons/sec/templates/sec-analyst-factsheet.qmd`. It is
  the counterpart of the esef factsheet and renders a PDF of some twenty
  pages from the facts of the registrant's own annual reports and from
  nothing else: the statement of operations, the balance sheet and the cash
  flow statement as filed and as a share of revenue, the margins and the cost
  structure, the growth, the net debt and the leverage, the liquidity and the
  cover of the debt, the working capital and the cash conversion cycle, the
  returns on assets, equity, capital employed and invested capital with the
  decomposition of the return on equity, the quality of the cash flow against
  the result, the comprehensive income, the per-share figures and what was
  returned to the holders of the shares, Piotroski's F-score and Altman's
  Z''-score, the coverage of the tags with checks that the statements add up,
  and the reports it was built from. Four things differ from the esef
  factsheet, and each follows from what the Commission publishes. `basis`
  decides which filing a figure is taken from where several report the same
  year: the year's own report, as the esef factsheet does, or the most
  recently filed one, which shows a restatement restated and fills a line the
  year's own report did not tag under a concept of the taxonomy -- a choice
  the company files allow because they carry every filing's account of every
  period. The per-share figures are brought onto the share basis of the
  latest year by a factor read out of the filings themselves, since a
  registrant that splits its stock restates its share count and NVIDIA's
  earnings per share as each report states them run 4.52, 6.90, 3.85, 1.74,
  11.93, 2.94, 4.90. A financial year is found by a window of 340 to 380 days
  rather than a fixed 364 or 365, because a registrant on a 52/53-week
  calendar does not close on the same date every year. EBITDA is derived,
  which the esef factsheet cannot do, depreciation being a line of the US
  GAAP cash flow statement under a concept of the taxonomy. There is no mark
  for a figure read from a concept of the registrant's own making and no page
  of the issuer's own prose, both being absent from the company files
  altogether. The last page is a disclaimer, which the cover points at.
- `sec`: a README for the module, which it did not have:
  `src/fincommons/sec/README.md`. It sets out what a company file
  carries and what it leaves out, the terms the Commission publishes under,
  the two Silver tables, and the factsheet.
- `financialreport`: a README for the module, which it did not have:
  `src/fincommons/financialreport/README.md`. It sets out the three things
  the module deliberately does not do -- take anything away from the two
  source modules, join the regimes by entity, or reconcile the vocabularies
  in the tables -- what each source covers and cannot, the two combined
  tables column by column, and the catalogue of measures: what its columns
  mean, that nothing in it is authoritative since the two boards published
  no correspondence, how a deployment keeps its own judgements beside it,
  and that the catalogue resolves a measure over either source table on its
  own, so an ESEF-only lake can use it without building anything here.
- `esef`: a template for a factsheet on one issuer, in R and Quarto, with
  voestalpine AG as its example:
  `src/fincommons/financialreport/esef/templates/esef-analyst-factsheet.qmd`. It renders a
  PDF of some twenty pages from the facts of the issuer's own reports and
  from nothing else: the income statement, the statement of financial
  position and the cash flow statement as filed and as a share of revenue,
  the margins, the growth, the capital structure and the net debt, the
  liquidity and the cover of the debt, the working capital and the cash
  conversion cycle, the returns on equity, assets, capital employed and
  invested capital with the decomposition of the return on equity, the
  quality of the cash flow against the result, the comprehensive income and
  the equity by component, the per-share figures, Piotroski's F-score and
  Altman's Z''-score, the coverage of the tags with checks that the
  statements add up, and the filings it was built from. A figure read from
  a concept of the issuer's own taxonomy, such as its capital expenditure,
  carries a dagger and is listed with the years it was found in; no EBITDA
  is derived, because depreciation is not a line of the statements under a
  concept of the IFRS taxonomy. The charts use the ten colours of Tableau,
  so that the lines of a balance sheet in one chart can be told apart. The
  last page is a disclaimer, which the cover points at: the document is
  produced by a template and the issuer is an example, no warranty is given
  for the correctness or the completeness of a figure, nobody who made it is
  associated with the issuer or with any other body named on the pages,
  nothing in it is advice, and liability is disclaimed as far as the law
  allows, in the terms of Articles 7 and 8 of the licence.
- `esef`: a template for a coverage report on the record, the counterpart of
  the factsheet, in R and Quarto:
  `src/fincommons/financialreport/esef/templates/esef-coverage-report.qmd`. It renders a PDF
  of some sixteen pages that counts what the repository lists and what the
  issuers tagged, and no amount out of a report: the filings, issuers,
  second filings of a year and filings without facts per country, the days
  from the period end to publication against the four months Article 4 of
  the Transparency Directive allows, what the repository's own validation
  found, the facts of a median report, the taxonomies the facts are tagged
  against, how much of a report sits under concepts of the issuer's own
  making, the concepts of the taxonomy that the most issuers tag, the local
  names the most issuers invent a concept for, the share of issuers tagging
  each line of the three primary statements, how many of those lines an
  issuer tags, and the share of issuers each indicator of the factsheet
  could be computed for. The lines looked for are the lines the factsheet
  reads, so the coverage of a line is the answer to how many issuers the
  factsheet could be built for, and the share for an indicator is the size
  of the panel a cross-sectional analysis of it could rest on. Where the
  configuration file names the `firds` module's Silver layer, the report
  also puts the record against the issuers of the shares FIRDS holds a
  record for, counted over the whole record the lake holds rather than over
  the countries and years of the report, since a report narrowed to one
  country would otherwise read as though the rest of the market had never
  filed. A page of its own states what the facts do not carry and what each
  absence puts out of reach: no presentation role, so the statements it
  groups its lines into are its own grouping; no anchoring and no
  calculation relationships, which live in the extension taxonomy of the
  report package, so it counts extensions without saying what they were
  anchored to and checks no statement for adding up; no labels, so two
  issuers count as having invented the same thing only where they spelt the
  local name the same way; and no text blocks, so nothing in it measures the
  tagging of the notes. The countries and the years are set in the first
  chunk and default to every country the lake holds and the last five years
  in it; the facts are read through four queries that arrow answers over the
  parquet files, so the table of facts is never collected.
- `firds`: a history can start in 2018. The full and the delta files ESMA
  published up to 2019-09-21 under drafts of their message definitions,
  `DRAFT13auth.017.001.01` and `DRAFT6auth.036.001.01`, are imported into
  folders of their own instead of being set aside as unrecognised, and the
  history starts from a full file of either definition;
  `update_landing_import(config_path, start_date="2018-12-29")` starts it
  from the last full file of 2018. The drafts state their timestamps without
  a time zone and are read in UTC, and they do not state the relevant
  trading venue.

### Fixed

- `esef` and `sec`: the link to `fincommons.dataquality` in each README,
  which the move of the two modules under `financialreport` had left
  pointing one directory too high.

### Changed

- `financialreport`: `edinet` is a third source of the combined tables.
  `SOURCES` names it, `facts_from_edinet` and `filings_from_edinet` reduce it
  to the columns the other two reduce to, and a lake that has collected only
  Japanese filings now gets a combined table of Japanese filings, as a lake
  with only one of the other two already did.
  Both combined schemas gain two columns, `EDINETCODE` and `JCN`. There are
  now four identifier columns, one regime's own in each, and a row has the
  column of its source filled and the others empty -- which is what the
  table said about two of them already. `JCN` is carried for a reason the
  other three are not: it is the one identifier in these tables that reaches
  an identifier another regime uses, being a check digit followed by the
  twelve-digit commercial register number that GLEIF records against a
  Japanese company's LEI. That join is not made here, needing GLEIF, which
  this module does not read.
  `FRAMEWORK` gains the value `JP-GAAP` and is read, for a Japanese filing,
  from what the filing says it reports under rather than from the prefix of a
  fact. The prefix does not settle it: a Japanese group that has adopted IFRS
  tags its statements against the Japanese IFRS taxonomy and its front matter
  against the same disclosure taxonomy every other filer uses, so the two
  kinds of fact of one filing would answer differently. `JP-GAAP` has no
  counterpart in the other two regimes and is not comparable with them line
  by line.
  One kind of fact is not carried, and it is the only thing this module
  drops. `PRDEND` may not be empty in the combined table of facts, a fact in
  no period being comparable with nothing; the edinet module leaves both
  dates empty for a context whose shape it does not recognise rather than
  inventing them, and such a fact stays in that module's table of facts,
  where its `PERIOD_UNRESOLVED` check counts them.
  Built over a lake of four real annual securities reports the combined
  tables hold 6,752 facts and 10,899 filings, 2,320 of the facts marked IFRS
  and 4,432 JP-GAAP, with the dimensions and the narrative carried as the
  agency gives them.
  What this does not yet do is let one question be put to all three regimes
  at once. `financialreport_measures.csv` defines its measures over IFRS and
  US GAAP and over no Japanese element, so `concepts_for(..., framework=
  'JP-GAAP')` returns nothing. The Japanese facts are queryable by concept
  today; the vocabulary that would reconcile them is a judgement about
  accounting of the same kind the existing rows record, and a harder one.
  Both READMEs say so.

- **Breaking.** The regimes that require a financial report tagged are now
  sub-modules of one namespace package, and `secfiling` is `sec`:

  | Was | Is |
  | --- | --- |
  | `fincommons.esef` | `fincommons.financialreport.esef` |
  | `fincommons.secfiling` | `fincommons.financialreport.sec` |
  | `fincommons.financialreport` | `fincommons.financialreport.combined` |

  Four regimes of one thing were four top-level modules with nothing saying
  they belonged together, and a root that grew a module per jurisdiction. The
  namespace says it. What it must not do is cost the property the arrangement
  rests on, which is that each regime stands alone: `combined` reads the
  regimes, no regime reads `combined`, and no regime reads another, so a lake
  that collects Japanese filings imports one sub-module and loads neither the
  combining layer nor the other three. `financialreport/__init__.py` is
  therefore a namespace and not a facade -- it imports nothing, and a test
  checks in a session of its own that reaching the namespace loads none of
  its children. Were it to import them, the lazy imports in
  `combined.silver` would have nothing left to defer.

  The guard test changed with it. It used to grep both regimes for the string
  `financialreport`, which every module's own dotted name now contains; it
  looks for an import of `combined` instead, under either spelling, and for
  an import of one regime by another.

  What this costs a deployment: the Python import paths above, the
  `secfiling:` section of `paths.yaml`, which is now `sec:`, and the names
  `update_secfiling`, `update_bronze_secfiling`, `update_silver_secfiling`,
  `update_silver_secfiling_quality` and `connect_to_secfiling`, which drop
  `filing` from the middle. The `esef:` and `financialreport:` sections of
  the configuration file are unchanged, and so is every data set name the
  column catalogue and `describe_column` use, `secfiling` excepted: a data
  set is still addressed as `esef` or `financialreport` while its code lives
  under the namespace, which `columns.MODULE_FOLDERS` records. The tables
  themselves, their columns and the layout of the lake are untouched, so
  nothing has to be rebuilt.

- The claim that no mapping exists between a central index key and a legal
  entity identifier is stated more carefully, in the docstrings of
  `sec` and `financialreport`, in their READMEs and in the two rows of
  the column catalogue. Neither the Commission nor GLEIF publishes such a
  mapping, which is what the decision to carry both columns and join neither
  rests on; but GLEIF's records do name the register an entity is registered
  in, and for some tens of thousands of them that register is EDGAR. Nearly
  all of those are funds, named by a series identifier rather than by a
  central index key, and an operating company's record points at its state
  register instead -- NVIDIA's at Delaware, by a file number that is not its
  central index key. The decision is unchanged; the reason for it is now
  stated as what it is.
- The corporate design the Quarto templates draw with is one file for the
  whole package, `src/fincommons/templates/r-corporate-design-functions-ggplot2.R`,
  rather than a copy beside the templates of each module. The three templates
  that draw with it resolve it themselves, preferring a copy beside the
  template where there is one, so that a template taken out of the package
  can carry a design of its own; where neither is there they stop and say so.
  A change to the design is now made once.
- `exchangerate`: `check_internal_rates` takes the base currency the rows of
  the internal file are held against as a `base` argument, so that a file
  kept against another base can be checked against that one. It defaults to
  `'EUR'`, the base `update_silver_exchangerate` combines the sources on, so
  the check refuses the same rows as before where it is left out.

- `firds`: `update_landing_import` with a `start_date` before the full file
  already held moves the history back to an earlier full file, where ESMA
  published one, and fetches every delta file after it. It used to keep the
  full file held and fetch only what was newer. The historical database is
  initialised from the full file the landing zone records, rather than the
  newest one in the Bronze layer, so that a history rebuilt after such a run
  starts from the earlier full file; a data lake without that record still
  starts from the newest.
- `firds`: `update_from_bronze_to_silver_main` builds the history again by
  itself where such a run has moved the start back. It used to apply only the
  reference dates after its watermark, and since the earlier full file and the
  delta files between it and the one the history stood on are all older than
  that, every one of them was passed over and the run reported that there was
  nothing to do. The rebuild replaces the database in one transaction, moves
  the day the history stands at back with it, and takes the days up to the
  earlier full file off the list of days published late. `watermark.json`
  gains `history_from`, the full file the history was built from, which the
  Silver layer records and `firds.record_history_from(config_path, day)` sets
  by hand; the transaction log is no substitute, since the commit naming that
  full file is cleaned out once it is older than the log retention. A history
  built before that record existed is left alone, with a warning that a start
  date moved back cannot be noticed until it is recorded.
- `firds`: the weekly `OPTIMIZE` of the historical database fills a file to
  `silver.OPTIMIZE_FILE_SIZE`, 128 MB, rather than to the gigabyte Delta aims
  at by default, which left the table holding gigabyte files beside the small
  ones of the days since the last compaction. A gigabyte is eight times
  `spark.sql.files.maxPartitionBytes`, so one task's share of the plan of a day
  unrolled to several hundred megabytes and the cache of the plan did not fit
  in the storage memory of a session that had plenty. Open-source Delta takes
  the size from the session rather than from the table, so
  `silver.compaction_file_size` holds the two settings around the compaction
  and restores them afterwards. Files already written at the old size are not
  eligible for a smaller bin and stay as they are until something rewrites
  them.
- `frenchdatalibrary` is renamed `famafrenchdata`, after the name the data
  is known by. Its functions follow: `update_famafrenchdata`,
  `connect_to_famafrenchdata`, `update_bronze_famafrenchdata`,
  `update_silver_famafrenchdata` and `path_silver_famafrenchdata`, and
  `python -m fincommons.famafrenchdata` runs the job. The section of the
  configuration file is `famafrenchdata`, and so is the Silver table: a data
  lake built under the old name renames the section and moves the table, or
  builds it again with `full=True`.

### Project

- A feature can be asked for: the issue templates gain *Feature*, for
  something the package does not do yet -- a function, a column, an option, an
  output -- and `CONTRIBUTING.md` says what such a request should carry and
  what happens to it. A request states what is needed, how a figure is defined
  where it is one, what analysis or process it serves and how that is done
  today without it; the use behind it is the argument for it, since what
  fincommons holds is decided by what the institutions using it need. A
  request that fits and that nobody has time for stays open and labelled
  rather than being closed, so that the next person who needs the same thing
  finds it. Asking obliges nobody to write it.

## 0.2.0

Two modules are new, `esef` and `frenchdatalibrary`, and `entitymasterdata`
now downloads every register itself. The installation requirements are
raised: Python 3.11, Spark 4 and pandas 2 are the oldest versions supported,
and `pypdf` is a new dependency.

### New

- `marketpricedata.curves`: risk-free zero curves and discount factors per
  currency from the overnight rates and OIS quotes of the Silver table, with
  log-linear interpolation in the discount factors, parallel shifts and
  forward rates.
- `frenchdatalibrary`: the Kenneth R. French Data Library -- the Fama-French
  factors, the portfolios they are built from and the breakpoints of the
  sorts -- with every file of the library in one long table. A file is
  downloaded only where the server says it has changed, its text is kept
  line by line in the Bronze layer, and the Silver table is rebuilt file by
  file. `python -m fincommons.frenchdatalibrary` runs it as a scheduled job.
- `esef`: the annual financial reports listed issuers file in the European
  Single Electronic Format, from filings.xbrl.org. The indexes of the
  repository are kept up to date with conditional requests, the reports of
  the countries asked for are downloaded once and again only where the
  repository processed them anew, and two Silver tables are built from them:
  every filing, and every tagged fact keyed by LEI, rebuilt country by
  country. `python -m fincommons.financialreport.esef --country AT` runs it as a scheduled
  job.
- `entitymasterdata`: the ECB's lists of financial vehicle corporations
  (FVCs) and of payment statistics relevant institutions (PSRIs) are
  downloaded and imported into the Bronze layer. They are not yet part of
  the entity master data set: where they rank in `SECTOR` is still to be
  decided.
- `storage.replace_silver_partitions`: replaces some partitions of a Silver
  table, as parquet or as Delta, and leaves the others as they are.

### Changed

- Requirements: Python 3.11 or later (3.10 reached its end of life), PySpark
  and delta-spark 4.0, pandas 2.0, pyarrow 11, polars 1.0, numpy 1.22,
  openpyxl 3.0.7 and, new, pypdf 6.18.1. Spark 4 runs on Java 17 and 21.
  Under pandas 3 the text columns of the French Data Library reach Spark as
  Python objects.
- `entitymasterdata`: `update_entitymasterdata` downloads every register on
  every run, so it needs access to the publishers' sites; a register whose
  download fails is logged and the one in the Bronze layer is used, and a
  register can still be stored in the landing zone by hand.
  - GLEIF: the latest golden copy is found through GLEIF's Golden Copy API
    and downloaded only where the Bronze layer holds none of its day. A link
    given to the update is still used instead.
  - ECB: the daily list of MFIs and the lists of investment funds are found
    on the ECB's pages, and of an archive only the lists the Bronze layer
    lacks are unpacked. The list of MFIs is imported as published, rather
    than converted to a workbook by hand.
  - EIOPA: the registers of insurance undertakings and of IORPs are exported
    through the form of their pages, as CSV. The Excel export stores an LEI
    of digits as a number, which corrupted 276 LEIs of the insurance
    register and 337 of the IORP register, several undertakings under one
    LEI; workbooks stored by hand are still read.
  - ESMA: the list of CCPs is read from ESMA's own lists, two pdf files,
    rather than from a copy kept by hand. A struck-through row is a
    withdrawn CCP, an LEI wrapped over two lines is joined by its check
    digits, and the Tier of a recognised third-country CCP is read off its
    row; this added six CCPs the copy lacked. A list read as naming too few
    CCPs is refused and the one in the Bronze layer kept.
  - Every column of the ECB and EIOPA registers is imported as text, since
    their workbooks mix numbers, dates and text in a column.
- `marketpricedata`: the Yahoo exchange rates leave out a rate that stands
  apart from both the days around it, and remove it from the Silver table if
  it was merged before (`silver_yahoo.isolated_prints`). It took out Yahoo's
  EUR/VND of 261.04 on 2019-11-08, a hundredth of the days around it.
- `exchangerate`: a rate is carried for at most a week after its quote
  (`MAX_FILL_DAYS`) rather than without limit, and every source counts up to
  its own last day, so a lagging BIS or internal file no longer drops the
  newest ECB rates. `convert2ccy` refuses a frame that already has the
  columns it adds.
- `firds`: a cancelled record is left out of the compact table, its text
  fields are sorted before they are joined, and a field no venue states is
  empty rather than the empty string. A record that begins before the
  version in force, or states no ISIN or trading venue, is refused. The
  compact table is written through `fincommons.storage`.
- `entitymasterdata`: the GLEIF files and the list of CCPs are read as text,
  so Namibia's country code `NA` is no longer taken as missing. Entities
  without an LEI get the same IDs on every run, and a failed download of the
  CCP list is logged rather than stopping the update.
- Bronze files of `exchangerate` and `entitymasterdata` are written under a
  temporary name and put in place once complete.

### Project

- A contributing guide, issue and pull request templates, and continuous
  integration that runs the tests on the oldest and newest supported Python
  and on the oldest declared dependencies, builds the distributions, checks
  licensing and reports coverage. `pytest -m "not spark"` runs the tests that
  need no Spark session in seconds.

## 0.1.0

The first release.

- `exchangerate`: the reference rates of the ECB and the BIS, combined into one
  table per calendar day and currency pair, and conversion of amounts in
  Spark, pandas and polars frames.
- `marketpricedata`: prices and rates from public publishers -- the ECB, the
  Federal Reserve Bank of New York, the Bank of England, the LBMA, Yahoo
  Finance, the Dhaka Stock Exchange, VNDirect and European power exchanges --
  in one table.
- `firds`: ESMA's FIRDS reference data, from the register's files to the
  historical database ESMA's instructions describe.
- `entitymasterdata`: who a legal entity is, from GLEIF and the registers of
  the ECB, EIOPA and ESMA.
- The infrastructure the modules share: addressing through `paths.yaml`,
  Silver tables rebuilt in full as Parquet or Delta, logging, the file layout
  of tables, declared schemas and a catalogue of what each column holds.
