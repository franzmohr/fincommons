# Changelog

The notable changes of each release. The project follows
[semantic versioning](https://semver.org/); while the version is 0.x, a minor
release may change the public API, which `tests/test_public_api.py` lists.

## Unreleased

### Added

- `financialreport.esef.quality`: `DUPL_FILING_COPY`, a note that one report
  is held more than once for the same period and language with the same
  facts in every copy. `DUPL_FILING_PERIOD` now warns only where the copies
  state different facts, which is a correction; of the 77 filings it named
  in the Austrian and Dutch reports, 42 were copies, most of them added to
  the repository within minutes of each other.

- `sts`: a Silver layer of six tables, built from one snapshot and rebuilt in
  full. `notification` holds one row per notification, keyed on the register's
  own document identifier because neither identifier the register publishes is
  unique; `assertion` holds the claims a public notification makes against the
  STS criteria, long rather than 130 columns wide, so that the revision of the
  template now in prospect is a vocabulary that grows rather than a migration;
  `tranche` and `party` hold the children, the four kinds of party under one
  column for the role; `event` folds what is known to have happened to a
  securitisation out of the register, the two frozen spreadsheets and the
  difference between two snapshots; and `synthetic` holds the list of
  synthetic STS securitisations, which are in no register. `update_sts`
  collects and builds in one call, and `connect_to_sts` opens a table. On the
  register of 20 November 2026 the tables are 1,147, 33,568, 2,275, 2,338,
  2,395 and 228 rows, and 1,364 of the 2,250 distinct tranche ISINs are in
  the FIRDS reference data.

- `sts`: `templates/sts-country-factsheet.qmd`, a factsheet on the
  securitisations of one country -- what is securitised there against the rest
  of the register, when they were notified, who originates them, where their
  vehicles are established, who verified them, and what has happened since.
  The only setting is the two-letter country code -- or `EU` for the Union as
  a whole, which adds the member states side by side and the securitisations
  whose vehicle is in another country than their originator -- in the first
  chunk or in `FINCOMMONS_COUNTRY`. It covers the public half of the register
  and says so, a private notification carrying no party to attach to a
  country; it reports counts rather than percentages for a country with fewer
  than ten securitisations; and it tells a country that hosts more vehicles
  than it originates, which is what a reader of Luxembourg or Ireland is
  after.

- `entitymasterdata`: `entities_as_of` combines the registers as they stood on a
  day -- the latest Bronze snapshot of each on or before it, read and combined
  by the rules of the public table -- and `sector_counts_over_time` counts
  the entities of every sector, subsector and investment policy on every date
  a register was published. `write_sector_counts` writes the counts beside
  the Silver layer, and the template `entitymasterdata-sector-report.qmd`
  reports banks, insurers and investment funds over time from them. On the
  Bronze layer of 20 November 2026 the funds reach back to March 2025; the
  MFI list and EIOPA's and ESMA's registers, published only as they stand,
  begin with the first download.

- `entitymasterdata`: the Silver build reads and combines the registers in two
  steps that can be called on their own. A `normalise_<register>` function per
  register -- `normalise_gleif_level_1`, `normalise_gleif_level_2`,
  `normalise_esma_ccp`, `normalise_eiopa_insurance_undertaking`,
  `normalise_eiopa_iorp`, `normalise_ecb_mfi`, `normalise_ecb_investment_fund`
  -- turns one Bronze snapshot into the rows that register states, and
  `combine_registers` applies the precedence between them, the fund
  characteristics and the GLEIF relationships.
  `update_silver_entitymasterdata_public` is the two applied to the latest
  snapshot of each register, so that a history built from every snapshot will
  apply the same rules. Built from the Bronze layer of 20 November 2026, the
  public table is identical before and after: 3,500,479 rows, the same columns
  in the same order, no row in one that is not in the other.

- `firds`: `vacuum_silver_main` deletes the files of the historical database
  that no version within `retention_hours` reads, a week by default as in
  Delta. Every merge and compaction leaves the files it replaced behind, so
  the table grew faster than the history in it: 39 gigabytes for 261 million
  versions in a lake built over eleven months. A backfill wants
  `retention_hours=0`, since everything it replaced is only hours old; Delta's
  check against a retention below the table's own is lifted for that vacuum
  alone and restored afterwards. The vacuum is refused while the latest commit
  of the update is a merge without its append, because that batch is resumed
  from the version before the merge, which a short retention would delete.

- The handbook has chapters on the SEC filings, the EDINET filings and the
  combined financial report tables, after the one on ESEF. The ESEF chapter
  covers the quality checks and the report templates.
- `sts`: a new module for ESMA's public register of the securitisations
  notified as simple, transparent and standardised, read through the
  machine-to-machine services of the registers portal, which need no key. The
  Bronze layer collects one complete, dated snapshot of the register per run
  and the four spreadsheets ESMA publishes beside it. A snapshot is never
  rewritten: the register is current state, a cancelled notification leaves it,
  and the spreadsheets that recorded removals were frozen in 2022, so the
  snapshots are the only record of what the register once held. Every
  collection reports which of the fields expected of every notification are
  missing and which have appeared. The Silver tables are being designed from
  the snapshots and are not here yet.
- `sts`: `templates/sts-register-report.qmd`, a report on one snapshot of the
  register. It reads the Bronze snapshots directly and sets out the two
  populations the register divides into, the underlying exposures, the
  notification dates, the identifier schemes and why neither identifier
  identifies a row, the tranches and the parties, the census of the fields,
  the checks the snapshot fails, and what arrived and left since the snapshot
  before — which is the only record of a departure there is. The snapshot is
  the latest the lake holds or the one named in `FINCOMMONS_SNAPSHOT`.

- `esef`: two factsheets on one country, `esef-country-quality-factsheet.qmd`
  and `esef-country-economic-factsheet.qmd`. Each takes the country from
  `FINCOMMONS_COUNTRY` and reads everything else from the lake. The quality
  factsheet ranks issuers by what to check before using their figures. Each
  check scores once per issuer and year. It adds screens that set an issuer's
  reports side by side: restated, scale and sign breaks in the prior-year
  figures, jumps, currency breaks, and reports filed under another entity's
  LEI. The economic factsheet gives like-for-like growth, margins, returns,
  the largest companies, the movers and the banks and insurers apart, and
  counts a group reported by a holding and its subsidiary once.

- `firds`: `update_from_bronze_to_silver_main` takes `batch`, the number of
  dates applied together, one by default. Each date applied on its own is a
  plan, a merge and an append, and the plan and the merge each read the whole
  history to find the versions in force the date's records meet, so a backfill
  read the history twice per date. A batch plans the records of its dates as
  one delta file is planned -- `history.prepare_incoming` orders them on the
  date of their file, in `history.FILE_DATE`, between the start date and the
  record type -- and applies them in one merge and one append, which leaves the
  history the dates build applied in turn. A batch ends early before a date
  whose file held no records and before a date the run stops at. Its commits
  are tagged with its first and last date, `firds-silver-main:<first>..<last>`,
  and a run that died within one resumes that same batch whatever `batch` it is
  then given, since a batch under another label would not find the commits it
  is resumed by. `history.check_file_order` refuses a batch in which a file
  begins a version before one an earlier file began, which applied one after
  another `history.check_supersession` refuses too. `silver.upsert_from_bronze`
  applies the dates of a batch; `upsert_from_bronze_daily` is it for one date.
  `batch = True` is refused, as `compact = True` is.

- `firds`: `update_from_bronze_to_silver_main` takes `compact`, which says how
  often the historical database is compacted while the dates are applied, and
  `firds.compact_silver_main` compacts it on its own. The default,
  `silver.COMPACT_WEEKLY`, is what the stage has always done -- the delta file
  of each Friday -- so a daily run is unchanged. A whole number compacts after
  every *n* dates the run applied records for, and `silver.COMPACT_NEVER` not
  at all. The argument exists for the backfill: a compaction rewrites the
  files whose deleted rows have accumulated, which is most of the table, so
  its cost follows the size of the history and not the number of dates since
  the last one. A daily run pays that once a week, which is the trade deletion
  vectors are there to make; a run applying a year of delta files paid it some
  forty times over a history growing the whole while. A run that names a
  cadence its last date did not fall on is expected to call
  `compact_silver_main` when it is done, since the merges of an uncompacted
  history read the rows they marked deleted. `compact = True` is refused
  although it is an `int`, because it reads as "yes, compact" and would mean
  the most expensive cadence there is.

- `esef`: the analyst factsheet takes its issuer from `FINCOMMONS_ISSUER`
  where that is set, as it takes the config file from `FINCOMMONS_CONFIG`.
  The LEI was stated in the template alone, so rendering the factsheet for
  one issuer after another meant editing the template between renders. The
  template keeps its LEI as the default, so a render that sets nothing is
  unchanged, and a value that is not twenty upper-case letters and digits
  stops the render where it is given rather than at a page of empty tables
  several chunks later.

- `esef`: the sector factsheet takes Japanese issuers. A panel row may be
  keyed by an `EDINET_CODE` beside an `LEI` or a `CIK`, the benchmark counts
  the annual securities report (`120`, and `130` amending it) as an annual
  report, and the cover, the notes and the cross-regime section name the
  regimes the panel holds rather than assuming the second is the
  Commission's. The cross-regime table already added amounts up within a
  regime, and its note now says that a Japanese row is in yen.

### Changed

- `esef-country-quality-factsheet.qmd`: where an arithmetic check fired, the
  detail pages give the two sides of the identity, the gap and its share of
  total assets or revenue, so that a gap of a tenth of a percent can be told
  from a figure tagged on the wrong line. An issuer presenting a bank's loans
  to other banks or deposits of its customers counts as a bank, which takes
  Oesterreichische Kontrollbank, whose interest income is under another
  concept, out of the ranking for missing revenue lines. `IMBAL_ASSETS` is
  not counted for a bank or an insurer either, since a group with a bank
  presents the bank's assets beside the current and the non-current ones;
  Österreichische Post had ranked first on that alone. The fourth tile of
  the cover is the share of issuer-years a screen flagged, and long concept
  names wrap in the tables instead of running off the page.

- `exchangerate`: the proprietary rates, `exchangerate.csv`, are headed
  `ref_date,base,quote,value,type,source`, the names of the Silver table they
  are combined into, and `internal_schema` declares them, as the proprietary
  price list of `marketpricedata` now is. The file is read by position and
  not by the names in its header, so a file still headed
  `REFDT,BASE,QUOTE,VALUE,TYPE,SOURCE` reads as before and need not be
  rewritten. `check_internal_rates` names the columns in its message as the
  file now spells them, `a base other than EUR` where it said `a BASE other
  than EUR`.

- `entitymasterdata`: the list of CCPs is kept per day. `download_esma_ccp`
  stores it as `YYYYMMDD.csv` under the day of the download, as EIOPA's
  registers are, and takes `today` as they do; the Bronze import keeps one
  `YYYYMMDD.parquet` per day, with `REFDT`, where it used to replace
  `ccp_list.parquet`. ESMA publishes only the current lists and dates
  neither, so a list once replaced could not be had again and the register
  could have no history. A list stored as `ccp_list` by an earlier version is
  renamed to the day it was last written (`date_undated_ccp_lists`), since
  `c` sorts after every digit and the undated list would otherwise be taken
  for the latest; a dated list of the same day stops the import rather than
  either being dropped.

- `entitymasterdata`: a fund described by the ECB's list of investment funds
  has the sector `IF`. What the sector used to say -- `UCIT`, `AIFD` or `INVF`,
  after what the list states about UCITS compliance -- is now its
  `subsector`, which used to repeat the investment policy that `if_policy`
  carries. A query that selected funds by `sector in ('UCIT', 'AIFD',
  'INVF')` selects them by `sector = 'IF'` now, and one that told UCITS from
  AIFs by the sector does so by the subsector.

- `marketpricedata`: the proprietary price list, `marketpricedata_internal.csv`,
  is headed `ref_date,instr_id,value`, the names of the Silver table it is
  merged into, and `internal_schema` declares them. The file is read by
  position and not by the names in its header, so a file still headed
  `REFDT,INSTRID,VALUE` reads as before and need not be rewritten.

- `marketpricedata.curves`: the curve segments are named in snake case.
  `build_curves`, `segment_frame` and `parallel_shift` return `currency`,
  `years_from`, `years_to`, `log_discount_from` and `forward_rate` where they
  returned `CCY`, `T_FROM`, `T_TO`, `LOGDF_FROM` and `FWD`, and
  `parallel_shift` takes its shifts as `currency`, `shock` and `shift`. Code
  that reads a segment by name, or passes `keys=["SHOCK"]` to
  `add_discount_factor`, has to use the new names.

### Fixed

- `financialreport.esef.quality`: `IMBAL_CASHFLOW` holds the three cash flows
  to the movement in cash before the effect of exchange rates, which IAS 7
  presents on a line of its own. It read the movement after that effect, so
  an issuer with cash in a second currency was out by its exchange
  differences: 59 of the 72 reports it found faulty in the Austrian and Dutch
  reports of the lake. The movement is also accepted as tagged, because some
  reports put the movement before the effect under the concept for the one
  after it, and their figures agree.
- `financialreport.esef.quality`: `IMBAL_ASSETS` takes a gap between total
  assets and the two subtotals as explained where it is the assets held for
  sale, which some issuers present below the current assets rather than among
  them, under either of the two concepts the taxonomy has for that line. That
  was 9 of the 12 reports it found faulty; the 3 left are Österreichische
  Post's, which presents the assets of bank99 as a third block.
- `financialreport.esef.quality`: `IMBAL_PROFIT` takes a gap as explained
  where it is the share of the profit of associates, which some issuers
  present after the tax: ASML its share of Carl Zeiss SMT. That was all 4
  reports it found faulty in the Austrian and Dutch reports of the lake.
- `financialreport.edinet`: the quality checks read the dates a filing states
  with `try_to_timestamp` and a cast rather than with `try_to_date`. The
  Python API gained the date function in PySpark 4.1 and Spark 4.0 does not
  register it as a SQL function either, so on the oldest PySpark the package
  declares -- and the declaration is deliberate, the package taking the Spark
  its runtime provides -- every date check raised an `AttributeError` instead
  of running. For text stating a date the two agree.

- `financialreport.edinet`: `update_silver_edinet_quality`, `find_issues` and
  `issues_schema` are importable from the package, as the README and the
  handbook call them, rather than only from `edinet.quality`.

- Documentation: the handbook and the READMEs brought back in line with the
  code. Among other things, the top-level README no longer names a `dart`
  module that does not exist, says that every register of `entitymasterdata`
  is downloaded and that Silver tables are parquet by default; the
  `marketpricedata` README and handbook chapter cover the DSE, VNDirect,
  electricity and Yahoo index sources and no longer describe a command line
  that was removed; directory trees in the handbook carry the snake_case file
  names; and the `firds` README shows its section of `paths.yaml`.

- `firds`: the writes to the historical database fill their Parquet row groups
  to `silver.ROW_GROUP_SIZE`, 32 megabytes, rather than to Parquet's default
  of 128. The default is a limit on the buffered size of a group, which the
  history's text columns more than triple once read: the compaction of a
  history of 260 million versions wrote groups of 2.7 million rows, 380
  megabytes read, with 136 megabytes in `full_name` alone, and a backfill
  reading them on eight cores exhausted a driver of 14 gigabytes, on the first
  batch of a run that had just started. Delta has no table property for the
  size, so `silver.row_group_size` holds `parquet.block.size` on the session
  around the merge, the append, the compaction, the initialising write and the
  decimal migration, and restores it afterwards; the restoring
  `compaction_file_size` did is shared with it. The groups already written
  keep their size until a compaction rewrites their file.

- `esef`: the README names the columns of a sector panel file as the files
  and the template spell them. It had them in the lower case the rename of
  0.5.0 gave the Silver columns -- `lei`, `country`, `name` -- where a panel
  is a list kept by hand and keeps its upper case, so a panel built from the
  README would have been refused for want of `SHORT` and `GROUP` or read
  with its identifiers empty. The README now also says which columns are
  required, which is `SHORT` and `GROUP` with an `LEI` or a `CIK` on every
  row, and why the two spellings are both right.
- `sec`: the quality report renders again. It reads the year a filing was
  made as `fy`, and the rename of 0.5.0 gave the fiscal year the registrant
  states that same name, so the report stopped on two columns called `fy`.
  The report never used the fiscal year and no longer reads it.
- `esef`: the sector and the analyst factsheet choose from the filings the
  lake holds facts for, not from those the index lists with a `json_url`,
  which says the repository publishes facts rather than that the pipeline
  collected them. On a lake collected with `years` the sector factsheet
  printed a year's sector total over the one issuer whose report of it was
  read, and said an issuer whose latest report was not collected had not
  filed it; the analyst factsheet counted filings it had not read.
- `esef`: the coverage report compares the latest year a report was read
  for, not the latest year a filing is listed for. That was the year now
  being filed, over which a lake collected with `years` had read nothing,
  and the comparison on the cover printed dashes and a bare per cent sign.
- `edinet`: the table of filings has one row per filing, as it says. The
  agency lists a filing again, on the day of the edit, whenever its details
  are edited -- S100Z1QI was filed as a large-holding report and corrected
  the next day to a report of change -- and every listing was a row. The last
  listing read is now the one kept. Each extra row had its archive read again
  as well, so its facts were in the table of facts twice, and the combined
  tables, which join the two on the document number, held them more often
  still: 49 filings in thirty days of indexes.
- `edinet`: the facts of the series of one fund no longer share an
  identifier. A fund manager files one document per series under one form,
  `jpsps040000-srs-001_G03898-001` and `..._G03898-002`, and the identifier
  named the form alone, so the same element in the same context of two
  series was one fact twice with two values; 232 filings in thirty days held
  such pairs. The identifier now names the series where the document is not
  the filing's main one, `-000`; a company's own report keeps the identifier
  it had.
- `edinet`: the quality checks run over a filing that states a date as not
  applicable. The conversion writes `－` there, and Spark's ANSI mode stops on
  it as a date where it was meant to be no date, so one such filing stopped
  all twenty-six checks.
- `financialreport`: a Japanese filing the accounting standard does not apply
  to, a large-holding report or a fund's, has no framework. It was given the
  conversion's mark for not applicable, `－`, as its framework.

## 0.5.2

The reports of `financialreport` render again, and a render that fails says
so. Every template set `execute: error: true`, which turns a failed chunk
into a clean exit over a PDF of error messages; naming `error: false` instead
is what brought the rest of this list to light. No report carries the path of
the lake it was read from any more.

### Changed

- The run table of the coverage report and of the two quality reports no
  longer names the Silver layer it read. The value is a path on the machine
  that rendered the report, which tells a reader nothing and travels with the
  PDF wherever it is sent; what the table is for is saying what was read, and
  the countries, the years and the counts say that. No report carries a local
  address now.

### Fixed

- `esef`: the analyst factsheet's `lines_dropped` answers to both of the
  arguments that ask for a line to be left out. It took only `core_only` and
  dropped the untagged lines whatever the caller asked, so
  `drop_empty = FALSE` with `core_only = TRUE` would have removed them and
  named them in the note. Every caller asks for both, so no rendered page
  was wrong.
- `esef`: a heading of the analyst factsheet that heads no lines is left out,
  which is what it was always meant to do. `seq.int(h + 1L, last)` counts
  down where the heading is the last row of a statement or is followed
  straight by another, so the block of lines under it came back with two
  rows instead of none and the heading survived. No statement the template
  ships has a heading in either place.
- `esef`: every count of a report read in the coverage report, and every
  rate taken over the reports read, is over the filings the lake holds facts
  for. It read the `json_url` of the index instead, which says the
  repository publishes facts for a filing and not that the pipeline has
  collected them; since `per_filing` joins the facts to that population and
  only the filings with facts survive the join, the numerators and the
  denominators were different populations. A lake of one country read as
  though the whole repository had been read: 16,764 reports read and 0.3 per
  cent listed without facts, where the facts came from 315 reports and 97.9
  per cent of what is listed was never collected. A lake holding every
  country the repository lists is unaffected, which is why nothing caught it.
- The categorical colour scales ask the palette for as many colours as the
  chart has groups. `scale_colour_corporate_design` and its fill counterpart
  handed `scale_*_manual` the ten colours of the palette, and a manual scale
  given ten values for an eleventh group is an error, so a chart of more
  groups than that halted the render: the coverage report colours a line per
  country and the repository lists twenty-seven.
  `palette_corporate_design` already ramps through the ten for any caller
  that passes `n`, and the scales now do.
- `esef`: the coverage report leaves the FIRDS comparison out where the
  `firds` module has a Silver layer that was never built, as it says it
  does. It tested the directory and `create_directory_tree` makes the
  directory of every table whether a run has filled it or not, so an empty
  one passed the test and `open_dataset` then had no columns to select.
- `esef`: the sector factsheet renders its panel again. Six references
  reached for a panel column by the lower-case name the rename of 0.5.0 gave
  it -- `lei` in four joins, `lei` and `country` in two more -- while the
  panel kept the upper-case names 0.5.1 restored to it. The first of them
  emptied every figure on the page; the last was never reached. A join
  between a panel and a Silver table now names both sides.
- `esef`: the analyst factsheet renders for an issuer whose extension members
  were renamed. The equity-by-component table takes the member of the
  components-of-equity axis for the identity of a component, and an issuer
  renames its extension members from one year to the next as it renames its
  extension concepts: PORR tags its other reserves and its hybrid capital
  under `porr:` to 2023 and under `PORR:` from 2024. One component then
  became two rows and `factor` was handed the same level twice, which halted
  the render. The label is the identity instead, so the prefix may change
  without splitting a component; where two members of one year do share a
  label the table shows both figures rather than choosing one in silence.
- `esef`: the columns of that same table are the financial years in order.
  `pivot_wider` names its columns in the order the rows reach it, which is
  the order the facts were read, so the years came out shuffled --
  2020, 2021, 2025, 2022, 2024, 2023 for PORR.
- A Quarto template whose chunk fails now fails the render. Every template of
  `financialreport` set `execute: error: true` in its header, which turns a
  failure into a clean exit: the coverage report over a lake written before
  the rename of 0.5.0 rendered a PDF whose pages were
  `object 'period_end' not found` and exited 0, and a scheduler only sees the
  exit code. The templates name `error: false` -- Quarto's default -- with the
  reason beside it, so the render now stops at the chunk that is at fault.
  Inline `r` never honoured the setting, so under `error: true` a broken chunk
  surfaced three sections downstream as a missing variable instead.

## 0.5.1

What 0.5.0 left behind in the files the test suite does not run: a template,
two handbook references and the text of a check.

### Fixed

- `esef`: the sector factsheet reads its panel file again. The panel is a
  list kept by hand -- the nine issuers stated in the template, or the csv
  named by `FINCOMMONS_PANEL` -- and like every other file kept by hand it
  carries the upper-case names it always had, `LEI`, `CIK`, `COUNTRY` and
  `NAME`. The rename of 0.5.0 lowered them in the template but not in the two
  panels shipped beside it, so the template looked for `lei` in a file whose
  header says `LEI` and every row of the panel went missing. Nothing caught
  it, because no test renders a Quarto template. The panel keeps its names,
  the Silver columns it is joined to keep theirs, and the two meet through
  the key built from them, as they did before.
- The data quality check `edinet.PERIOD_UNRESOLVED` tells the reader to read
  the `context` and `relative` columns rather than the names they had.
- The handbook calls the exchange rate column `imputed`, and the README of
  `entitymasterdata` calls the columns of the Silver table `lei` and
  `subsector` where it describes what the tests hold.

## 0.5.0

The columns of the Silver layer are renamed from the compressed upper case of
ISO 20022 to snake_case in English words. Every module is converted, which
breaks every Silver table the package writes. Building the FIRDS history also
costs a great deal less per reference date, which is the other half of the
release and changes no table at all.

### Faster

- `firds`: `update_from_bronze_to_silver_main` applies a reference date for
  less. Four things it did per date cost more than what they did was worth.
  The plan of a file no longer shuffles the history. `history.plan_changes`
  joins one day's records against the versions in force, and the versions in
  force are most of the table: far too large to be the broadcast side of that
  join, so Spark sorted and shuffled the whole of them, every date, to be met
  by a few thousand records. The open versions are now cut down first, by a
  semi-join against the keys the file carries, which a left semi-join may
  broadcast -- so the history is read once, three columns of it, probed
  against a hash table of the day's keys, and never shuffled. What survives is
  at most one row per key of the file and is broadcast in turn, which leaves
  the plan with no shuffle of the history in it at all. It is the same plan:
  the join could only ever have used an open version whose instrument the file
  names.
  `history.prepare_incoming` costs one shuffle instead of three. It opened
  with a `dropDuplicates` over every column of every record, and the step
  after it already kept one record per instrument and day -- which an exact
  duplicate cannot survive either -- so a full-width shuffle was finding what
  was removed anyway. That step and the chaining that follows are now taken on
  one window, and share its exchange and its sort -- the latter because the
  order the window reads the records in is a column of its own rather than an
  expression each use of the window computes again under a name of its own,
  which left Spark unable to see that two uses asked for the same order and
  sorting the records once per use. One exchange and one sort, against three
  of each.
  The two checks of a file's records are one pass. Each counted on its own,
  and a file was flattened into the analytics schema once per count and again
  for the plan; `check_incoming_records` counts both at once, the flattened
  records are held while the date is applied, and a file is refused with the
  message it was refused with before. The two checks remain, for a caller that
  wants one of them.
  Resuming a date reads the end of the transaction log rather than all of it.
  `committed_steps` asks for the tags of a date that a previous run left
  applied in part, and such a date is the one the table was written by last;
  it asked Delta for the whole history of the table, which reads every commit
  file in the log. The log grows by a few commits for every date applied, so a
  run that builds the history from the start of the sample read a log of
  thousands of commit files once per date and the reading of it alone grew
  with the square of the dates. It now reads a window of the most recent
  commits, `RESUME_COMMITS` of them, which cannot mistake a tag of a replaced
  history for one of the history as it stands: either the commit that wrote
  the table from scratch is in the window, and the walk stops at it as it did
  before, or it is older than every commit in the window. `tagged_commits`
  takes the window as `limit` and looks at the whole log without one, which is
  what `watermark_from_commits` still does.
  None of this changes the history a file produces, and the tests of section
  4.2 are unchanged.

### Changed

- The Silver columns of `exchangerate`, `entitymasterdata`, `famafrenchdata`,
  `marketpricedata`, `esef`, `sec`, `edinet`, `financialreport` and `firds` are
  snake_case: `ref_date`, `instr_id`, `value`, `period_end`, `prd_start`,
  `maturity_date`, `trading_venue`, `nominal_value_per_unit_currency` and so
  on. `docs/column_rename.csv` is the table of decisions, one row per column,
  with the state of each: 215 columns renamed, and 5 names kept because they
  belong to the Bronze layer.
- The Bronze layer does not follow. A Bronze table records what a publisher
  sent and keeps the names it sent, so the rename happens where Bronze is read
  into Silver: `entitymasterdata.with_silver_names` and
  `marketpricedata.with_silver_names` do it for the registers and the price
  deliveries, `esef.FILING_RECORD_KEYS` for the filing index, and the Silver
  stages of `exchangerate` and `marketpricedata` for the proprietary files.
- The files kept by hand keep upper case, because their header is an input
  contract: `exchangerate.csv`, `marketpricedata_internal.csv` and the
  proprietary entity list. So do the mapping tables shipped in `data/`
  (`isin_yahoo.csv` and the others, whose header is `ISIN` or `INSTRID`), and
  the Bronze partition columns `DATASET`, `LASTDT` and `DATADT`.
  The reference date of `exchangerate.csv` is now `REFDT` rather than
  `REFDATE`, so a file kept by hand needs that one header changed.
- `columns`: `SNAKE_CASE_MODULES` says which modules are converted, and
  `defined_columns` reads lower-case names as columns only for those. Upper
  case had been doing a second job -- marking a name as a column at all -- and
  in a converted module only the leading underscore is left to do it, so the
  intermediates are now `_next_date`, `_source_rank`, `_rows`, `_error`,
  `_count`, `_leis`, `_rn`, `_dates`, `_POS` and `_DELIVERY`.
- The column catalogue states a Bronze input and the Silver table it feeds as
  separate rows, since the two no longer share a name. It has 198 rows where
  it had 192.
- `entitymasterdata`: `INTERNAL_COLUMNS` and `INTERNAL_ID_COLUMNS` no longer
  follow `COLUMNS`, because the proprietary list keeps upper case while the
  table does not; `INTERNAL_ID_SILVER` is the same four columns as the table
  names them. `warn_of_malformed_leis` takes the name of the LEI column, so
  that the proprietary list can still be checked on its normalised value
  before it is read into the Silver names.
- The Quarto templates read the new names. Three of them set a parameter by
  hand whose name the rename would have collided with -- the issuer's LEI, a
  registrant's CIK, a filer's EDINET code -- and a collision there is silent:
  `filter(LEI == lei)` would have become `filter(lei == lei)`, which is true
  of every row. They are now `issuer_lei`, `issuer_cik` and
  `filer_edinet_code`, and the report language is `report_lang` for the same
  reason.
- The handbook argues the convention in section *Naming conventions*: why the
  ISO 20022 spelling was there, why provenance belongs in the catalogue's
  `source` column rather than in the name, why Bronze does not follow, and why
  the conversion is a reviewed table of decisions rather than a lowering of
  case.

- `firds` is converted too: `isin`, `trading_venue`, `maturity_date`,
  `notional_currency`, `valid_from`, `valid_to`, `latest_record` and the rest,
  including `ANALYTICS_COLUMNS` and the constants of `history`. `DATADT` keeps
  its name: it is the partition directory of the Bronze layer
  (`DATADT=<date>`), and although `silver.py` attaches it to the frame it
  reads, no Silver table carries it. The value `"ISIN"` of
  `interest_rate_floating_id_type` also keeps its case, being data rather than
  a column, and so stays beside its siblings `Index` and `Name`.
- `describe_column` folds case instead of upper-casing what it is asked for,
  since a snake_case column could never be found by upper-casing it, and
  `defined_columns` takes a `snake_case` argument so that a package built on
  this one can say how far its own modules have been converted.
- `NOT_COLUMNS` names the three lower-case aliases that are not columns of any
  table and cannot be marked with an underscore: `src` and `tgt`, the two
  sides of a Delta MERGE, and `checksum`, which belongs to the landing
  register and is read with Polars.

## 0.4.0

The combined tables of `financialreport` answer eight measures of a
catalogue, `edinet` is a third source beside ESEF and the Commission, and a
Japanese filer has a factsheet of its own. `esef` gains a factsheet on a
sector, which reaches past one regime. Two changes break: no module of the
package opens a Spark session any more, so the three `python -m` entry points
and the `build_session` they exported are gone, and the combined tables carry
a `MEASURE` column.

### New

- `esef`: the sector factsheet reaches past one regime. A panel row is keyed
  by the identifier its regime uses -- an LEI under ESEF, a central index key
  with the Commission -- and `templates/esef-sector-panel-crossregime.csv` is
  the example, 145 European issuers and 86 American ones. What crosses the
  frameworks is the eight measures of the catalogue and nothing else: no pair
  of concepts for an operating result is in it, so EBIT stays a European
  exhibit rather than being paired with `us-gaap:OperatingIncomeLoss`, which
  would be the template inventing a mapping the combined module declines to
  make.
  Four things the Commission's filings force, none of them visible in an
  ESEF-only lake. A filing may be quarterly -- three 10-Q for every 10-K --
  and a quarter's profit over a year's equity is not a return, so the
  benchmark keeps the annual forms. A filing carries no country, so a country
  filter now keeps a row that has none; without that, letting the countries
  default to the panel's own quietly emptied the American half of a pooled
  benchmark. An issuer is not keyed by its LEI, and grouping on one collapsed
  every American registrant into a single issuer. And the bank test needed
  the US-GAAP lines beside the IFRS ones, the sectoral registers being
  European and no American registrant being on one.
  The result and the equity a pooled question wants are the total where the
  filer states it and the parent's share where it does not: most ESEF filers
  state the total and most American registrants only the parent's share, so
  neither alone reaches both. Which was used is carried per filing and
  printed as a table of what each pooled figure is made of, because a median
  over a mixture has to be able to say what it mixed.
  The benchmark's read of the facts is now a filter arrow applies to the
  files rather than a condition R applies afterwards. A lake holding one
  regime has a few tens of thousands of measure-bearing facts and one holding
  the Commission's filings has millions, and collecting those first did not
  finish.

- `esef`: the sector factsheet was rendered rather than only run, and three
  things only rendering could show were fixed. A cover that said
  "Construction in Austria" over a hundred and forty-five issuers in
  seventeen countries, because the country was a setting and swapping the
  panel did not touch it; the countries the panel filed in now decide what
  the cover may claim. An index chart carried away by one issuer whose
  revenue went from 0.2 to 186 million between two filings, an index of
  ninety-six thousand that took the axis to 1e+05 and flattened the other
  hundred and twenty-eight lines onto it; the axis is cut at a quantile and
  the caption names the lines that leave. And a group label that wrapped onto
  two lines in every row of a five-hundred-row table.

- `esef`: a factsheet on a sector, `templates/esef-sector-factsheet.qmd`, the
  counterpart of the one on a single issuer. It reads the two Silver tables of
  the module for the panel's own lines and, where the lake holds them, the
  combined tables of `financialreport` for the benchmark and the rates of
  `exchangerate` for the conversion, and renders with Quarto to a PDF. Two
  examples ship: the Austrian construction sector in the template's own first
  chunk, nine issuers, and the same sector read across Europe in
  `templates/esef-sector-panel-europe.csv`, 145 issuers in seventeen
  countries, in five groups from the contractors through the developers, the
  materials makers and building technology to the merchants.
  The panel is a stated list and the template says so twice, in the chunk
  that sets it and in the disclaimer on the last page. An ESEF report carries
  no sector code, and `SECTOR` of `entitymasterdata` covers financial
  entities only, so which issuer stands for a sector is a judgement rather
  than a classification; a different panel gives different totals and
  different medians, and the panel is printed in full at the front of the
  rendered document for that reason. A panel of nine fits in the template and
  a panel of a hundred and forty-five does not, so it may instead be read
  from a csv named in `FINCOMMONS_PANEL`.
  What the repository holds limits any such panel, and the limit is invisible
  in the output unless it is said: **filings.xbrl.org holds no German and no
  Irish filings at all**. Hochtief, Bilfinger and Heidelberg Materials are
  not in it, and the Irish issuers that are stand under `GB`. A European
  panel built from this repository is Europe minus Germany.
  Amounts in more than one currency are converted by the `_DOMESTIC`
  convention of the EMIR and SFTR pipelines, which is the reason the columns
  are named as they are: the filed figure and its currency stay untouched,
  `<key>_FXRATE` beside them is the rate applied and is 1 where the amount
  was filed in the target currency, and `<key>_DOMESTIC` is the filed amount
  divided by that rate. An amount whose currency the table does not quote
  stays unconverted rather than being carried over or read as a nought, and a
  rate table holding nothing quoted against the target is refused outright --
  the check those pipelines make, so that a page of blanks cannot be mistaken
  for a sector that reported nothing. Which rate applies follows IAS 21: a
  balance at the closing rate, an income or expense at the average of the
  year. A ratio is not converted at all, the currency cancelling in a
  quotient of two amounts of one filing.
  Banks and insurers are shown apart rather than dropped, and two tests find
  them. The sectoral registers are the first -- the ECB's MFI register,
  EIOPA's two and ESMA's list of CCPs, read as a file of LEIs, with no need
  of GLEIF for this one question. The filings themselves are the second, for
  a filer whose primary statements carry interest revenue computed with the
  effective interest method or one of the IFRS 17 lines. The second exists
  because the first has a hole: a register lists the authorised entity and
  what files is usually the group above it -- BAWAG P.S.K. is on the register
  and BAWAG Group AG, which files, is not -- and a British bank has not been
  on a European register since 2020. Over the Austrian filings the registers
  alone reach 22 of the 27 the two tests together find.
  The benchmark is read through the materialised `MEASURE` column rather than
  by resolving a measure per filing, which is the division of labour the
  combined README argues for, and the page states the cost: only the first
  concept of each framework carries a measure, so a filing stating revenue as
  IFRS 15 revenue alone is in the tables of the panel and not in the
  quartiles, and every count beside a benchmark figure counts filings that
  stated and named the figure rather than a population.
  Two findings came out of building it, both now in the README. The net
  margin row for banks and insurers is empty rather than nought: no Austrian
  bank tags `ifrs-full:Revenue`, because a bank's income statement has no
  line that means it. And of the 601 Austrian filings read, 96 carry a total
  liabilities figure; the rest present non-current and current liabilities
  and never add them up. The two nulls look alike in one column and neither
  is a zero.
  The rule that picks one filing per issuer and year turned out not to be
  cosmetic either. PORR filed its report for 2024 twice, both in German and
  both validating without error, and the two differ by a factor of a
  thousand: one states revenue of 6,190,521 with the unit `iso4217:EUR` and
  the other 6,190,521,000. Taking the report the repository processed last is
  what picks the right one.

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

- `financialreport`: the catalogue of measures reaches Japan. Sixteen rows
  added, eight under `JP-GAAP` against the `jppfs_cor` taxonomy and eight
  under `IFRS` against `jpigp_cor`, which is Japan's own taxonomy for a group
  that has adopted IFRS -- and which is why the `framework` and `taxonomy`
  columns are separate: a Japanese IFRS filer's revenue resolves under
  `framework = 'IFRS'` beside `ifrs-full:Revenue` without being the same
  element. Every element was read off a real conversion before it was
  written down; the catalogue now defines 8 measures in 38 rows.
  Two judgements are recorded as caveats rather than as mappings. **Japan
  GAAP has no element for the equity attributable to the parent**, 自己資本
  being 株主資本 plus 評価・換算差額等 and so a sum rather than a line, so no
  `JP-GAAP` row defines `equity_attributable_to_parent`: taking
  `jppfs_cor:ShareholdersEquity` for it is short by the valuation and
  translation adjustments, 9,191 million of a correct 63,326 million for one
  report checked. And a Japanese balance sheet presents 現金及び預金, cash
  together with deposits, so the figure meaning what
  `ifrs-full:CashAndCashEquivalents` means is the closing balance of the cash
  flow statement; that is the first row and the balance sheet line the
  second.
  One gap is named rather than guessed at: a filer whose revenue is 営業収益
  rather than 売上高 states it under an element no row reaches, none of the
  reports this was written against using one.

- `financialreport`: the combined table of facts carries `MEASURE`, a
  canonical identifier for the figure a fact states, so that the same concept
  has the same name in all three regimes. It is stored and not resolved on
  demand: `ifrs-full:Assets`, `us-gaap:Assets`, `jppfs_cor:Assets` and
  `jpigp_cor:AssetsIFRS` all answer to `'assets'` in the table itself, and the
  concept the filer tagged stays beside it, so neither the agreed meaning nor
  what was actually said is lost.
  It starts with one measure and will grow one at a time.
  `MATERIALISED_MEASURES` names what has been agreed -- today `('assets',)` --
  and a measure joins it when its concepts have been read against real
  filings and found to mean the same figure. Most of the column is therefore
  null, which is the honest answer: an identifier given to a concept that
  only approximately matches would be worse than none, because nothing
  downstream could tell the two apart.
  `canonical_concepts` refuses to build a lookup in which two measures claim
  one concept, and two such claims already exist:
  `us-gaap:StockholdersEquity` is both `equity` and
  `equity_attributable_to_parent`, and `us-gaap:NetIncomeLoss` is both
  `profit_loss` and `profit_loss_attributable_to_parent`. Those are judgements
  about the catalogue and not about this column, so the column waits for them
  rather than picking a winner.
  `with_measures` keeps working as before and now supersedes the stored
  column for the query it is asked: the table stores the measures agreed so
  far, resolving asks about every measure the catalogue defines.

- `financialreport`: `revenue` is the second measure the combined table
  carries, and adding it settled the rule for every measure after it. **Only
  the first concept of each framework is written into `MEASURE`.** Assets
  needed no such rule, having one element per taxonomy and no alternatives at
  any priority. Revenue has seven catalogue rows and six concepts, three of
  them ranked below the first, and each of those says in its own caveat that
  it is not the same figure: `us-gaap:Revenues` is "the last resort and not
  an equivalent of the IFRS total", the including-assessed-tax element is
  "larger than the IFRS measure", `ifrs-full:RevenueFromContractsWithCustomers`
  "excludes revenue outside IFRS 15 that the total may include". The
  priorities exist for a caller resolving a measure at query time, who wants
  the best answer the filing affords; the column is held to the stricter bar,
  so a fact tagged with an approximation keeps a null. The cost is coverage
  rather than correctness -- a registrant tagging only the pre-ASC-606
  `us-gaap:Revenues` carries no identifier -- which is the right way round
  for a name a query filters on. The rule was read off the catalogue rather
  than imposed on it: every row ranked below the first carries a caveat of
  that kind, and a test now holds them to it.
  It is also what keeps the column answerable. A filing may tag several
  concepts of one measure -- a 10-Q states its revenue under the ASC 606
  element and under the older total for the same period -- and labelling both
  would put two different figures of one filing under one name. At the first
  concept the catalogue has one element per taxonomy per measure, so that
  cannot happen, and `canonical_concepts` now refuses a catalogue in which it
  could.
  **The two collisions reported with the previous entry are settled by this
  rule rather than blocking anything.** `us-gaap:StockholdersEquity` is named
  by `equity` and by `equity_attributable_to_parent`, but as the second
  concept of the first and the first of the second, so it is the equity
  attributable to the parent; `us-gaap:NetIncomeLoss` likewise. All eight
  measures of the catalogue now pass the checks. That is a statement about
  the catalogue and not a licence to materialise them: each is still a
  judgement to be made against real filings one at a time, and
  `MATERIALISED_MEASURES` still names two.

- `financialreport`: `liabilities` is the third measure the combined table
  carries. It passes the rule as easily as assets -- four concepts, one per
  taxonomy, nothing ranked below them -- and what it adds is **a second
  reason a fact carries no identifier, which the column cannot distinguish
  from the first**. Revenue's nulls are the rule declining to name something
  the filer did state. These are different: many filings with the Commission
  present liabilities and equity together and tag no total liabilities at
  all, so for a substantial part of the US population the figure was never
  stated. Both appear as a null in the same column and neither is a zero.
  The total is not derived from the balance sheet identity to fill the gap.
  Nothing in this family of modules invents a figure, and what the
  alternative costs is already on record: in the `edinet` checks, reading a
  missing total as zero made one filer's liabilities short by 244,695
  million. So a count of the rows carrying a measure is a count of what was
  stated and named -- never a population, and never a denominator without
  saying which. The README, the `fact_schema` docstring and the column
  catalogue all now say so, and a test holds the two kinds of null apart.

- `financialreport`: `cash_and_equivalents` is the fourth measure the
  combined table carries, and **the first whose first concepts are not the
  same line of the same statement**. IFRS, Japanese IFRS and US GAAP each
  present cash and cash equivalents on the balance sheet. A Japanese balance
  sheet has no line that means it -- it presents 現金及び預金, cash together
  with deposits, time deposits of more than three months among them, which
  are cash equivalents under none of the three -- so for `JP-GAAP` the
  identifier is given to the closing balance of the cash flow statement. One
  name over one figure, wherever its regime chose to present it, which is
  the case this column exists for.
  The first-concept rule earns its keep twice here. The IFRS first concept
  carries a caveat about the US total, ASU 2016-18 putting restricted cash in
  the cash flow statement total where IAS 7 does not; that total is
  `us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents`, a
  *second* concept the column leaves out, so the rule removes the mismatch
  its own caveat warns about. The pair that does carry the identifier is
  IAS 7 against `us-gaap:CashAndCashEquivalentsAtCarryingValue`, which the
  catalogue calls the closer match.
  It also costs something for the first time, rather than only inheriting a
  cost. `jppfs_cor:CashAndDeposits` sits at priority 2 expressly so that a
  filer stating no cash flow statement is reached at all, and the column
  declines it, a figure read from that element being cash and deposits
  rather than cash and equivalents. Those filers carry a null here and are
  still reached by `measure_values`, which is the division of labour between
  the stored column and query-time resolution.

- `financialreport`: `equity` is the fifth measure the combined table
  carries, and the one **whose nulls are a skew rather than a gap**. Its
  first concepts are the totals including the non-controlling interests --
  `ifrs-full:Equity`, `jpigp_cor:EquityIFRS`, `jppfs_cor:NetAssets` and
  `us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest`
  -- 純資産 being their counterpart, 株主資本 and the valuation and
  translation adjustments and the subscription rights and the
  non-controlling interests together.
  `us-gaap:StockholdersEquity` is left out, and the catalogue says why in as
  many words: it is ranked second "because many filings tag no total
  including NCI, not because it is equivalent". That makes this null unlike
  the one total liabilities leaves. A registrant with no non-controlling
  interests states the same number under that element as a total including
  them would be, so the figure is usually there and usually right; what the
  element does not say is whether this is such a registrant. Naming it would
  be correct for most filings and wrong for exactly the groups a
  cross-regime comparison is about.
  The consequence is a skew in the sample and not only a hole in it: the
  registrants this column reaches for equity are more often the ones with
  non-controlling interests to report. A benchmark drawn from it has to say
  so. Counting the rows carrying a measure says how many filings were named,
  never how many exist, and the README, the `fact_schema` docstring and the
  column catalogue now all say that.
  This null is the one that is temporary. `us-gaap:StockholdersEquity` is
  the *first* concept of `equity_attributable_to_parent`, so the day that
  measure is written down the same fact takes a name, and the right one.
  The two are really one decision and only the first half of it is made
  here; Japan complicates the second, having no element for 自己資本 at all,
  so that measure would reach two regimes of three by construction. Tests
  record both halves.

- `financialreport`: `profit_loss` is the sixth measure the combined table
  carries. It repeats equity's shape -- `ifrs-full:ProfitLoss`,
  `jpigp_cor:ProfitLossIFRS`, `jppfs_cor:ProfitLoss` and `us-gaap:ProfitLoss`
  are the results including the non-controlling interests, and
  `us-gaap:NetIncomeLoss` is left out as the one that excludes them. What
  differs between the frameworks is composition rather than definition: a
  Japanese profit and loss account reaches 当期純利益 through 経常利益 and
  the extraordinary items, which an IFRS statement does not have, and the
  bottom line is still the bottom line.
  Adding it showed **the skew of that exclusion to be a cross-regime one**,
  which equity alone had not made plain. Take the same economic fact in two
  places, a company with no non-controlling interests. In Japan it states
  当期純利益 under `jppfs_cor:ProfitLoss` as every filer does, and the
  column names it. In the United States it states only
  `us-gaap:NetIncomeLoss`, and the column names nothing, though the figure
  is the same one. The Japanese population under `MEASURE = 'profit_loss'`
  is close to every filer; the American population is conditional on having
  non-controlling interests to report. The two samples are selected by
  different rules, so a comparison of their medians is not a comparison of
  the two regimes, and the same holds for equity, 純資産 being tagged by
  consolidating and non-consolidating filers alike. The README section on
  this is now about both measures rather than about equity, and the
  `fact_schema` docstring and column catalogue say it too.
  The exclusion is temporary as equity's is, and the two differ in reach,
  which is the thing to decide them on: `profit_loss_attributable_to_parent`
  has an element in all four taxonomies, Japan stating 親会社株主に帰属する
  当期純利益 as a line of its own, while `equity_attributable_to_parent` has
  no `JP-GAAP` element at all. Materialising the first would reach every
  regime and would put a name on the American facts this measure declines.

- `financialreport`: `profit_loss_attributable_to_parent` is the seventh
  measure the combined table carries. It puts a name on
  `us-gaap:NetIncomeLoss`, exactly the facts `profit_loss` declines, and it
  reaches every regime, Japan stating 親会社株主に帰属する当期純利益 as a
  line of its own.
  **It does not repair the skew of `profit_loss`. It inverts it**, and that
  is the more useful thing to know. The attributable line exists only where
  there are non-controlling interests to attribute away, so a filer
  preparing no consolidated statements states none under IFRS or Japan
  GAAP: its result is the one figure that is both measures at once.
  `us-gaap:NetIncomeLoss` is not like that, being the ordinary bottom line
  of an American income statement, tagged by registrants with subsidiaries
  and without. So `profit_loss` reaches most IFRS and Japanese filers and
  only American ones with minorities, while the attributable measure
  reaches most American registrants and only IFRS and Japanese ones with
  minorities. Between them the two cover the population well; neither does
  alone, and switching from one to the other changes which filers are in
  view as much as which figure.
  That is the limit of a materialised identifier, and the READMEs now say
  which tool to use for what. The figure a population-level comparison
  wants is the result attributable to the parent falling back to the total
  where there are no minorities to separate -- a judgement per filing
  rather than per concept, which no concept-to-name map can express.
  **Filter on `MEASURE` to ask a precise question of named facts; resolve
  through `measure_values`, which picks the best concept each filing
  actually states, to ask about a population.** The column does not replace
  the function, and this pair is where that stops being a technicality.

- `financialreport`: `equity_attributable_to_parent` is the eighth measure
  and completes the catalogue. It names `us-gaap:StockholdersEquity` as the
  profit pair named `us-gaap:NetIncomeLoss`, and it is the weakest of the
  eight for a reason no other measure has.
  **Its absence under Japan GAAP is structural rather than conditional.**
  Every other gap in this column depends on what a filer tagged -- a
  registrant stating no total liabilities, one reporting no minorities to
  attribute away -- and changing the filer closes it. This one depends on
  nothing about the filer at all: Japan GAAP has no element for 自己資本,
  which is 株主資本 plus 評価・換算差額等, a sum rather than a line. No
  `JP-GAAP` filing can carry this measure however it is prepared, and
  `concepts_for('equity_attributable_to_parent', 'JP-GAAP')` returns
  nothing, so a query filtering on it excludes Japan entirely and says
  nothing about having done so. It is the one case in this column where a
  null says more about the taxonomy than about the filing.
  The sum is not computed to close the gap. Taking
  `jppfs_cor:ShareholdersEquity` instead is short by the valuation and
  translation adjustments, 9,191 million of a correct 63,326 million on one
  report read for the catalogue, and adding the components up would be this
  module inventing a figure. So the equity pair stays lopsided where the
  profit pair's reach is not: `equity` reaches every regime and this reaches
  two of three.
  All eight measures are now materialised, **and the gate stays**. A measure
  added to the catalogue tomorrow is not materialised by being added: it is
  named in `MATERIALISED_MEASURES` or it carries no identifier. That tuple
  is written out rather than derived from the catalogue precisely so that
  writing a row and claiming a figure is comparable stay two separate acts,
  and a test now holds that line by inventing a catalogue row and checking
  the column ignores it.

### Fixed

- `esef`: the captions of the sector factsheet's tables now reach the page.
  `knitr::kable` writes a caption as a `Table:` line above a pipe
  table, pandoc reads it into the caption of a float, and the Typst writer
  emits the float as a bare `#table(...)` -- so all seven captions were
  dropped, in silence, in every render since the template was written. They
  are not titles but reading instructions: which unit an amount is in, what
  the count in brackets counts, that one table's amounts are not converted
  at all. They are now written under the table as their own paragraph.

- `esef`: the sector factsheet reads as a two-regime document where it is
  one. A cover that said "a factsheet on 231 issuers, from their ESEF annual
  financial reports" over 86 American registrants, and listed the seventeen
  European countries of a panel spanning eighteen; a provenance paragraph
  that named only the ESEF tables; a currency note that counted eight
  currencies without the dollar and claimed every amount was converted when
  the cross-regime table converts none. Each now says what it is of. In the
  panel table an issuer that files with the Commission showed `NA-NA` years
  and `NA` reports, which reads as a filer that never filed rather than one
  this lake keys differently, and it put all 86 of them in the list of
  issuers that had stopped filing -- 127 names against 127 years, the reader
  left to zip the two lists together by position, and `NA` for the years
  that did not exist. The years are a dash with a sentence saying why, the
  list is the European issuers alone, and each carries its own last year.

- `esef`: the benchmark's read of the measures is bounded by the years the
  factsheet shows. The fact table is partitioned by `SOURCE` and
  `PERIODYEAR` and the read used neither, so it scanned 2.7 million rows for
  the 1.3 million of years any page shows: 5.7 seconds against 2.2. The
  bound is the first year shown and not one less, because `PERIODYEAR` is
  the calendar year the period ends in and the financial year of the page is
  that year or the one before. The second read, the one that finds the banks
  and insurers, is deliberately left unbounded: 788 of the 2,144 it finds
  are only visible in a filing older than the years shown, and bounding it
  would move those into the population the panel is compared with.

- `esef`: the sector factsheet renders about four times faster -- 3m45s to
  56s for the cross-regime panel, 2m17s to 30s for the Austrian one. Three
  `group_by(...) %>% slice(1)` dedupes were the whole of it: dplyr builds a
  group index per group, and over the hundreds of thousands of groups the
  combined table has, one of the three took 95 seconds against 0.5 for the
  `distinct(..., .keep_all = TRUE)` that keeps the same row. The arrow scans
  the slowness was blamed on cost eleven seconds together.

- `financialreport`: a Japanese filing's framework is now decided per fact
  and not per filing. The consolidated statements of a Japanese group that
  has adopted IFRS are IFRS and the parent company's own statements are Japan
  GAAP, both in one report, so a framework taken from the filing alone called
  the parent's revenue IFRS: Fujitsu states 3,502,971 million under
  `jpigp_cor` for the group and 1,692,618 million under `jppfs_cor` for the
  company, and they are not figures of one framework. The taxonomy decides it
  where the taxonomy is decisive and the filing decides it otherwise. One
  residue stays and is named in the code: the five-year summary of an IFRS
  filer states the parent's Japan GAAP series under the same disclosure
  taxonomy as the group's IFRS one.

- `financialreport`: `measure_values` no longer returns nothing for a filer
  that prepares no consolidated statements. It excludes a fact reported under
  a dimension, because a figure broken down by segment or by class is a part
  of an entity and summing the parts beside the whole is the arithmetic that
  exclusion exists to prevent -- but `NonConsolidatedMember` names a basis of
  preparation and not a part, and a filer that does not consolidate states
  its figures under it and under nothing else. Such a fact is now kept, and
  where a filing states both the consolidated figure wins. The new
  `BASIS_MEMBERS` holds the one member this applies to; a segment is still
  not a measure, which a test says.

- `esef` and `sec`: the link to `fincommons.dataquality` in each README,
  which the move of the two modules under `financialreport` had left
  pointing one directory too high.

### Changed

- `exchangerate`: a Silver update with `use_bis = True` ends the table on the
  latest date for which BIS data is available. The BIS publishes its bulk
  statistics several days late, and the days beyond it were described by the
  ECB and the proprietary source alone: a pair only the BIS quotes stopped
  days before the rest, and the day an amount was converted on decided which
  currencies the table covered. The cut keeps the whole table on the same
  footing, at the price of the few newest days of the other sources, which the
  next run picks up once the BIS has caught up. It is applied after the days
  without a quote are filled, so a day before the cut still carries the last
  rate quoted on or before it. A run without the BIS is unchanged: every
  source counts up to its own last day. Where the BIS data in the Bronze layer
  holds no daily average at all there is no day to cut back to, and the update
  is refused before anything is written rather than replacing the table with
  an empty one. The log line of the combination names the cut-off beside the
  last day of each source.

- No module of the package opens a Spark session. The three entry points that
  did -- `python -m fincommons.marketpricedata`, `python -m
  fincommons.famafrenchdata` and `python -m fincommons.financialreport.esef`
  -- are gone, with the `build_session` each of them exported. The session is
  the caller's: a cluster has one already, a workstation has to say how much
  of itself to give it, and a pipeline that opened its own was deciding that
  for every caller. What the commands did is now a script of a dozen lines
  that the operator owns -- the session, `fincommons.log.configure`, the one
  `update_*` call, and an exit code taken from the outcome's `failed` entry --
  and each module's README shows it, as the README of `firds`, which never
  had an entry point, already did. The pipelines themselves are unchanged:
  they took a session as their first argument before and they take one now.

- `firds`: `update_from_landing_to_bronze` takes `workers`, how many files it
  reads at a time, one by default as the stage has always worked. A file is
  read by a single task whatever the session is given -- the XML reader
  reports a file as one it cannot split, so a 164 MB file comes back as one
  partition and `local[1]` and `local[4]` take the same time over it -- so the
  cores of a session sit idle for all but one of them, and the way to use them
  is to have several files in flight rather than to give one file more.
  Measured on four cores over 394 MB of delta files, four workers took 14.7
  seconds against 33.3, and more workers than cores added nothing. The figure
  is bounded by memory rather than by cores, since each file being read is
  held in the driver heap.
  The records of a file are now written into a directory of their own and
  moved into the partition of their reference date afterwards, whatever
  `workers` says. Writing straight into the folder cannot be done by more than
  one file at a time: Spark's commit protocol stages under `_temporary` beside
  the partitions and removes that whole directory when a job finishes, which
  takes with it what every job still running had staged there, and the run
  does not fail cleanly on it -- the writes that lost their staging raise
  `FileNotFoundException`, the import quarantines those files as it does any
  other failure, and what is left is a Bronze layer missing whole files whose
  names sit in the `error` folder as though they had been malformed. Writing
  aside also makes the import of one file atomic, so a write that fails leaves
  the folder as it was rather than a partition half filled.
  Which of two copies of a file is kept is settled before any file is read, in
  the order the names sort in, so that it does not turn on the order the reads
  finish in.

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

### Project

- The READMEs no longer point at a `docker` directory. The repository holds
  none: the worked run of the FIRDS README named `docker/tools/run_firds.py`
  and a compose file, and the test section of the `marketpricedata` README
  sent a Windows workstation to a container described in `docker/README.md`,
  and neither has ever been in the tree. The FIRDS section is gone and the
  `marketpricedata` one now gives the one command that works. The two schema
  tools said they run on the workstation "and not in the Docker image"; they
  say "without Spark", which is the part of it that is true and useful.

## 0.3.0

Quarto templates are new: a factsheet on one filer and a report on
the coverage of the record for `esef`, a factsheet for `sec`, and a data
quality report for both, built on `dataquality`, a new module that holds the
checks. `firds` can start a history in 2018, from the drafts of the message
definitions ESMA published then. Two changes break: `frenchdatalibrary` is
renamed `famafrenchdata`, after the name the data is known by, and the regimes
that require a financial report to be tagged are stated anew.

### New

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

### Changed

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
