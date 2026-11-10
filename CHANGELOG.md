# Changelog

The notable changes of each release. The project follows
[semantic versioning](https://semver.org/); while the version is 0.x, a minor
release may change the public API, which `tests/test_public_api.py` lists.

## Unreleased

### New

- `esef`: a template for a factsheet on one issuer, in R and Quarto, with
  voestalpine AG as its example:
  `src/fincommons/esef/templates/esef-analyst-factsheet.qmd`. It renders a
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
  `src/fincommons/esef/templates/esef-coverage-report.qmd`. It renders a PDF
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
  country. `python -m fincommons.esef --country AT` runs it as a scheduled
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
