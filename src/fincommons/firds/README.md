# FIRDS data

## Introduction

Functionality:
* Download FIRDS data from ESMA's website
* Import FIRDS files from landing zone into Bronze layer
* Generate a historical FIRDS database as described by ESMA in [FIRDS - Instructions on access and download of full and delta reference data files](https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files)


```python
# Path to the configuration file naming the paths of the data lake
config_path = "paths.yaml"

# The stages report what they do through the standard library's logging
from fincommons.log import configure

configure("INFO")

# Start Spark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
                    .appName("fincommons") \
                    .config("spark.cores.max", "20") \
                    .config("spark.driver.memory", "15g") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load module
from fincommons import firds

# Make sure that all the necessary directories exist
firds.create_directory_tree(config_path)
```

## Download data into landing zone

Function `update_landing_import` downloads FIRDS data from ESMA's website into the landing zone. The function proceeds in the following way:
* Check if a full FIRDS sample is already available in the `finished` folder of the landing zone. This sample is treated as the basis of the FIRDS data set.
* If a full sample (FULINS) is not already available, it is downloaded for the date specified in argument `start_date` or - if not available - the next date, where such data is available on ESMA's website. Every part of it is fetched, so a part that failed before is fetched again. If no full sample was published from `start_date` on, the function raises.
* Every delta file (DLTINS) published after the full sample is downloaded, unless it is already held. What is fetched is decided by name rather than by resuming after the newest file held: resuming would pass over a part that failed while a later file succeeded, and the history would lack its records for good.
* The cancellations file (FULCAN) is downloaded as well, but only its most recent publication. ESMA publishes one every morning beside the delta file, and each is the consolidated list of every record cancelled so far rather than the change of one day: two of them a week apart were found to hold 854,571 and 854,573 pairs, differing by four added and two removed. An older one therefore says nothing the newest does not, and a publication already held is not fetched again.
* Download candidates, for which a file with the same name is already in the `import` or the `finished` folder of the landing zone, are skipped. A file in the `error` folder is not held and is downloaded again. The comparison relies on `finished` being kept: it is the record of what the Bronze layer has been built from.
* A file is downloaded under the name `<file>.part` and only renamed once it is complete, so an interrupted download never leaves a file that counts as held. An error answer from ESMA is raised rather than stored, a file whose MD5 digest differs from the `checksum` the register states for it is refused, as is an archive that does not open, and every request has a timeout. A failed download does not stop the others; the failures are raised together at the end.
* The register is read page by page, a thousand files at a time, sorted by publication date and identifier so that the pages do not overlap. A list that ends up holding fewer files than the register says it found is refused, since the files missing from it would otherwise not be noticed. A single request for ten thousand rows returns an arbitrary part of the list once the window holds more files than that, which at some three thousand files a year happens within about three years.
* The register is only asked about the days that may still be missing something. `watermark.json` in the landing zone records the last day up to which every part of the full file and of every delta file is in `finished`, and the next run asks from a week before that day, or from the full file where nothing is recorded yet. The day moves on only while every file up to it is in `finished`, so a file that failed keeps the window open until it has been imported. Without a full file in `finished` the record is ignored and the register is asked from `start_date`. The week of overlap is there because ESMA can publish a file after the day it is dated, which the answer the record was computed from did not mention.
* `watermark.json` also records which days the register listed a delta file for, which day the history has been built up to, and which days turned up late. The first tells the Silver layer that a day it holds no records for was published rather than never existing. The second is written by the Silver layer, because the landing zone cannot read the watermark of the historical database and its own record is no substitute: that one is written before a run downloads anything, so the history can stand further along than it. The third says that a day the history has passed has records after all; a day marked so that the history has not reached yet is applied in the ordinary way and the mark is taken off again. A file that cannot be read is treated as no record at all, so a truncated write does not stop the pipeline.

The results are stored in the `import` folder of the landing zone.


```python
firds.update_landing_import(config_path, start_date = "2026-01-01")
```

## Update data in Bronze layer

Function `update_from_landing_to_bronze` imports data from the landing zone into the Bronze layer. The main change in the data is the format. Not major changes to the schema are made.

Only files ending in `.xml` or `.zip` are picked up, so the `<file>.zip.part` a download that was interrupted hard leaves behind is passed over rather than taken for an archive. Where an archive and the XML extracted from it are both in the import folder, the XML is removed, so that the same records are not read twice.

A file whose name is already in the `finished` folder is not imported again but moved to `error` with a warning. The Bronze layer is only ever appended to, so a second import would put every record of the file into it twice.

The import decides what a file is from the message definition identifier in its header and writes it to a folder of its own, partitioned by the publication date:

| File | Schema | Bronze folder | Row element |
| --- | --- | --- | --- |
| Full file, FULINS | `auth.017.001.02` | `auth_017_001_02` | `RefData` |
| Delta file, DLTINS | `auth.036.001.03` | `auth_036_001_03` | `FinInstrm` |
| Delta file, DLTINS, published up to 2022-01-31 | `auth.036.001.02` | `auth_036_001_02` | `FinInstrm` |
| Cancellations file, FULCAN | `auth.102.001.01` | `auth_102_001_01` | `CxlData` |

A record of the cancellations file carries nothing but the instrument and the trading venue whose combination was cancelled, as `FinInstrmGnlAttrbts.Id` and `TradgVnRltdAttrbts.Id`.

The cancellations file is **not** an input to the historical database. Section 4.2 of ESMA65-8-5014 builds that from the full and the delta files alone, and a cancellation reaches it as a `<CancRcrd>` record of a delta file, which `update_from_bronze_to_silver_main` already applies. The cancellations file answers the different question of which records have ever been cancelled, and is kept in the Bronze layer as its own data set.


```python
firds.update_from_landing_to_bronze(spark, config_path)
```
                                                                                    

## Update data in Silver layer

### Main data

Function `update_from_bronze_to_silver_main` can be used to update the historic FIRDS database as described in ESMA's [FIRDS - Instructions on access and download of full and delta reference data files](https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files)


```python
firds.update_from_bronze_to_silver_main(spark, config_path)
```

The update follows section 4.2 of ESMA65-8-5014 rev 3. Every record carries the
validity dates `VLDFROM` / `VLDTO` and the flag `LATESTRECORD`, and an
instrument is identified by the pair `(ISIN, TRADGVN)`. On the first run the
records of the newest full file are registered with the start of their publication
period as `VLDFROM`, an empty `VLDTO` and `LATESTRECORD` set. Every delta file
that follows is then applied in chronological order, and each of its records
implies one of three operations:

What a record does is decided by the version in force rather than by the
element it was reported in, because what has to hold afterwards is that an
instrument has one version in force at a time:

| Condition | Operation |
|---|---|
| no version in force | insert the record. A `<NewRcrd>`, and paragraph 27.b.iii.2, a termination reported late |
| the version in force began earlier | close it on `VLDFROM - 1`, clear its `LATESTRECORD`, insert the record. Paragraph 27.b.ii, and paragraph 27.b.iii.3, a correction after termination |
| the version in force began on the same day | replace it. Paragraph 27.b.iii.4, an ordinary termination, and see below |

A `<NewRcrd>` for an instrument that already has a version in force supersedes
that version rather than being added beside it. The instructions do not
describe that case, since a new instrument is not supposed to exist already;
ESMA can report one all the same, for an instrument admitted again after it was
terminated, and a termination stays in force because it carries no `VLDTO`.
Inserting such a record would give the instrument two versions in force, which
makes `LATESTRECORD` ambiguous and, on the next delta file that touches the
instrument, matches the merge against two rows at once and stops the update
with an error until the table is repaired.

The algorithm is implemented in `fincommons.firds.history` as transformations
of data frames, independently of how the history is stored, and is covered by
`tests/test_firds_history.py`, which reproduces the worked example of section
4.1 of the same document. `silver.py` applies the resulting plan to the Delta
table: one merge closes the superseded versions and replaces the terminated
ones -- both look for the version in force under the same condition, and an
instrument is subject to at most one of them, so a second merge would only read
the table again -- and the remaining records are appended afterwards. The
append has to come last, because it would otherwise add a second open version
for the instruments the merge closes.

Both steps are planned from one fixed version of the table, the one before
the merge. Spark computes the plan again for the append, and a read that is not
pinned would then see the table as the merge left it: a version the merge has
replaced is no longer in force there, and the record that replaced it would be
inserted a second time.

Where a delta file happens to carry several changes for the same instrument,
the records are chained -- each is valid until the day before the next one
begins -- which is what the sequential description of the instructions amounts
to when applied to a whole file at once.

Of several records for one instrument that begin on the same day, only the
last in the order `NewRcrd`, `ModfdRcrd`, `CancRcrd`, `TermntdRcrd` is kept;
chaining them would close the earlier one on the day before it begins, leaving
a version valid on no day at all. Exact duplicates, as a file imported twice
produces, are dropped first.

The module also provides the queries of section 4.3: `history.latest_records`
returns the version in force for every instrument, and `history.records_as_of`
returns the reference data as it stood on a given day.

#### Reading the Bronze layer

The Bronze layer is read one partition at a time, through the paths the
translation to the analytics schema actually reads and no others, and every one
of those paths is cast to the type the historical database carries. Both are
there for the same reason: the schema a file was imported with can change under
the layer's feet.

The XML schemas in `fincommons/data` are derived from ESMA's XSD files, and
deriving them again can change a type: a money or rate field can be read as
`double` under one derivation and as `decimal` under another, and `<CancRcrd>`
as a full reference data record under one and as the reduced one the schema
states under another. The Bronze layer keeps the files it has already written,
so a folder comes to hold files of more than one vintage, and Parquet will not
hand a column written as `double` to a reader asking for `decimal` -- it raises
`SchemaColumnConvertNotSupportedException` and the whole daily run stops.

`read_bronze_partition` takes that apart in three steps:

* the files of the partition are grouped by the types they state for the paths
  the pipeline reads, and each group is read on its own, since a single read can
  ask for only one type per column. A partition of more than one group is
  reported with a warning;
* the read asks for the paths named in `SOURCE_FIELDS` and for nothing else, so
  a field the pipeline never uses can change its type without being noticed at
  all -- which is the common case, since most of the schema is never read;
* every path is then cast to the type named in `SOURCE_FIELDS`, so the frame the
  history is built from has one fixed schema whatever the partition held.

The types in `SOURCE_FIELDS` are the ones the historical database carries. The
money and rate fields are the `decimal` types the XML schema derives from the
XSD -- `decimal(23,5)` for the two amounts, `decimal(21,10)` for the fixed rate
and `decimal(35,17)` for the price multiplier -- rather than `double`, since an
amount of eighteen integer digits and five decimals is more than a double holds
exactly. A Bronze file that states them as `double` is cast up to them.

Reading the schema of every file costs one look at its footer. That is nothing
next to reading a delta file, of which a partition holds one to four, and it is
paid once for the forty files of a full one.

#### How the table is laid out

The historical database is a Delta table clustered by `(ISIN, TRADGVN)`, the
pair the merges match on, and it is not partitioned. `declare_silver_main_layout`
declares that when the database is created, and its statements are idempotent,
so it can be called again on a database that already carries the layout:

```python
firds.declare_silver_main_layout(spark, config_path)
```

Partitioning by `TRADGVN` does not work at this size: it
puts the table into one directory per venue, the partitions average well under a
megabyte, and it buys no pruning, because a delta file touches most venues at
once and the merge therefore reads every partition anyway. What it does buy is a
file per (writing task, venue) pair on every append, so the table grows
thousands of files of a few kilobytes a day and every later merge has to open
all of them. Clustering keeps the columns of the merge condition together in the
same files without cutting the table into directories, so the statistics of a
file can rule it out. The clustering key is `history.KEY_COLUMNS` itself, so
that it cannot drift away from the condition the merges are written with, and
`delta.autoOptimize.optimizeWrite` sizes the files of the appends that follow.

Deletion vectors (`delta.enableDeletionVectors`) are what keep the daily merge
from rewriting the history. Without them a merge rewrites every file it touches
in full, and since a delta file touches instruments all over the table, that is
every file. Measured on the history of 17 million rows built from the full file
of 2026-09-05, re-applying the delta files of two days to a copy of it:

| Delta file | Versions changed | Without | With |
| --- | ---: | ---: | ---: |
| 2026-09-07 | 172 | 54 s, 16,963,552 rows copied | 15 s, none copied |
| 2026-09-08 | 1,180,246 | 85 s, 15,783,502 rows copied | 55 s, none copied |

Without deletion vectors the cost follows the size of the history, which only
grows; with them it follows the size of the delta file. The weekly `OPTIMIZE`
rewrites the files whose deleted rows have accumulated. Enabling them raises
the reader version of the table to 3, so a reader outside Spark needs
deletion-vector support; `compact` is the table meant for such readers. A
reader that hands arrow the parquet files a Delta table holds has none: it
either stops with an error or returns the rows a deletion vector has deleted,
so `main` is not the table to point one at.

`declare_silver_main_layout` sets the property, which upgrades the protocol of
the table; Delta does not undo that by itself.

The scan of the versions in force that planning a delta file needs is not
where the time goes: it took 4 to 8 seconds on the same history, for a delta
file of 80 thousand records as for one of 1.3 million. Restricting it to the
instruments of the file with a semi join, so that the file's keys could be
broadcast, was measured and made no difference.

The files written since the previous run are clustered once a week, on the
delta file of each Friday. With deletion vectors on, `OPTIMIZE` also rewrites
the files whose deleted rows have accumulated, which is most of the table: it
took 87 seconds and rewrote all of the 17 million rows of the history built
from the full file of 2026-09-05. The weekly cost therefore follows the size of
the history, not the week; what deletion vectors move off the daily path they
put back here once a week, which is a seventh of the rewrites and the reason
the trade is worth making.

#### The watermark

How far the history has been built is recorded in a small Delta table of its
own, under `watermarks` in the metadata area, one row per table of the Silver
layer. `update_from_bronze_to_silver_main` applies every delta file newer than
that reference date and none that is older, which is what keeps a second run
over the same files from attributing the wrong validity periods to the versions.
The functions of `fincommons.firds.metadata` read and write it:

```python
firds.read_metadata(spark, config_path)                      # the whole table
firds.get_metadata_maxdate(spark, config_path, "MAIN")       # one reference date
firds.update_metadata_maxdate(spark, config_path,            # set it by hand
                              table = "MAIN", maxdate = "2026-09-01")
```

A history this module built says itself how far it has been built: the commits
below name the reference dates they applied, and where the watermark is missing
it is recovered from them, with a warning, rather than the database having to
be built again. That is the state a run leaves behind when it dies after
writing the history and before writing the watermark. A history that carries
neither a watermark nor such commits is refused rather than rebuilt, because
there is no way to tell which delta files it has already seen; setting the
watermark by hand is what lets it be updated.

The merge and the append of a reference date are two commits, and the
watermark follows them, so a run can die with a date applied in part. Each
commit carries `firds-silver-main:<date>:merge` or `...:append` as its
`userMetadata`, and a date is resumed rather than started again: if its append
was committed, only the watermark is recorded; if only its merge was, the plan
is made from the table version before that merge and only the append is made.
Only the commits since the table was last written from scratch count, so a
rebuilt history does not inherit the tags of the old one. Delta's
`txnAppId`/`txnVersion` is not used for this, because the versions it records
survive an overwrite and would silently skip every append of a rebuilt history.

#### Applying only complete days

Which dates are candidates is not read off the Bronze layer alone. A day whose
delta file was quarantined has no Bronze partition at all, and a day that was
never fetched leaves no trace in the data lake either, so the dates are
collected from the Bronze partitions, from the names in `finished` and `error`,
and from the days `watermark.json` records the register as having listed.

A reference date is applied only once every part of its delta file
(`DLTINS_<date>_<n>of<m>`) is in the `finished` folder of the landing zone.
The first date that is not ready stops the update: the dates before it are
applied, the run raises, and the watermark stays before the gap. It stops for
one of two reasons, and says which:

* parts are missing, i.e. the file has not been imported, in whole or in part;
* every part was imported but the Bronze layer holds no partition, not even an
  empty one, which means a partition has gone missing.

A file that carried no records at all is a third case and not an error: the
Bronze layer creates the partition as an empty directory to say that the date
was imported and had nothing in it, and the update passes the date with a
warning rather than stopping on it for ever.

A record that does not say which instrument it is about stops the update
before anything is written. An instrument is the pair `(ISIN, TRADGVN)` and
every comparison the history makes on it is an equality, which no null meets,
so such a record is never found to supersede anything and is inserted again by
every file that carries it: the instrument comes to have one version in force
per delivery and `LATESTRECORD` is set on all of them. `check_instrument_key`
refuses it, for the same reason `check_publication_period` refuses a record
without a publication period -- nothing fails while the fault is being made.

A record that begins before the version in force for its instrument stops the
update as well. Paragraph 27.b.ii closes that version on the day before the
incoming record begins, which says what to do on the assumption that the
incoming record begins later; where it begins earlier the arithmetic still
goes through and closes the superseded version before its own `VLDFROM`, so
that it is valid on no day and a point-in-time query answers for every later
day with the older record. `history.check_supersession` refuses it.

A day whose delta file ESMA publishes after the history has been built past it
stops the update as well. Its records are not in the history, and applying the
days after it would not put them there, so the run raises naming the day. The
history can be rebuilt from that day, or the gap accepted with
`firds.clear_late_day(config_path, day)`, after which the update carries on.
Where a day arrives late but before the history reaches it, it is applied in
order and the marker is dropped.

The history is not initialised from a full file whose parts are incomplete
either, and a missing full file is reported as such rather than leaving the run
to complain about the watermark it then has no way to write. Where the Bronze
layer holds several full files, the newest is the one the history starts from
and the others are passed over with a warning: a full file states the reference
data in full, so an older one only adds instruments the newest one no longer
carries. Parts are checked per CFI letter, and a letter of which not a single
part arrived leaves no trace among the others. The landing zone therefore
records the parts the register lists for the full file, under `full_sample` in
its record, and the history is not initialised until every one of them is in
the `finished` folder. A data lake filled by hand carries no such list; its
history is initialised from the parts that arrived, with a warning that the
check could not be made. The landing zone records the parts of the full file
it downloads, which is the oldest it holds, so that every delta file after it
is fetched; a newer full file put into the lake by hand is the one the history
starts from instead, and the recorded list then says nothing about it. The
warning names both days, so that the two cases are told apart from the log.

Every one of these refusals stops the daily job until someone acts. Where a
file is broken at the source and will never arrive, moving the watermark past
the day by hand with `firds.update_metadata_maxdate` is what lets the history
carry on, at the price of that day's records.

### Concentrated data `compact`

Since individual instruments can be traded on multiple trading venues, FIRDS data contains multiple entries per ISIN. This can lead to unwanted data explosions when the data is joined with other data sets. To address this, table `compact` is generated. It contains only one observation per ISIN. Text fields, where multiple entries are available in `main`, are concatenated with `+`. The maturity date, the expiry date and the price multiplier keep their types: they hold the value the trading venues agree on and are empty where they disagree. Joining them as text and parsing the result would make the whole rebuild fail under Spark 4, which raises on a string it cannot parse.

The values of a text field are sorted before they are joined, so the string an ISIN gets is decided by the set of values and not by the order a hash set happens to iterate in. `collect_set` defines no order, and the table is rebuilt in full on every run: without sorting, a venue added to an ISIN can land anywhere in the string rather than beside the others, and two lakes holding the same rows need not spell the column the same way.

A venue that states nothing is not a venue that disagrees: a null is left out, so one venue's value stands for an ISIN the others say nothing about. Where no venue states a value at all the field is empty rather than the empty string, which is what concatenating an empty list answers; the typed columns are empty in that situation too, and a reader should not have to tell `NULL` and `''` apart to ask the same question of two columns of one table.

A cancelled record is left out. The history carries it as the version in force of its instrument and venue, as section 4.2 of the Guide prescribes, but it states nothing about the instrument and what it does say is that the instrument was never admitted to that venue; counted, it would list that venue among the ones the instrument trades on. An ISIN whose every venue was cancelled therefore leaves the table altogether, which is the same statement one row further out.

Note that the current selection of variables is limited, but can be extended in the future.

The table is stored in `{path to the Silver Layer as specified in the configuration file}/compact`. It is rebuilt from `main` in full, so the write replaces it rather than adding to it.

```python
firds.update_from_silver_main_to_silver_compact(spark, config_path)
```

* `file_format`: The format the table is stored in, `'parquet'` or `'delta'`. The default is `'parquet'`, because it is the format every engine reads without a further dependency. It says nothing about `main`, which is always a Delta table: `main` is added to a delivery at a time, which is what a transaction log is for, while `compact` is rebuilt from it and so loses nothing by carrying no log.

Delta is worth asking for where the table may be queried while it is rebuilt: a Delta rebuild replaces it in one transaction, so a reader sees either the previous version in full or the new one, and the previous versions remain readable. A parquet rebuild deletes the table before it writes the new one, which leaves a window in which a reader finds it missing or half-written.

```python
firds.update_from_silver_main_to_silver_compact(spark, config_path, file_format = "parquet")
```

`connect_to_firds` reads the table in whichever of the two formats it is stored in, so the choice does not have to be repeated when the table is queried. A table that already stands keeps the format it is stored in, and asking for the other one raises rather than converting it: overwriting a Delta table as parquet deletes its transaction log along with the superseded files, which is not something to do as a side effect of naming a format, and Delta will not adopt parquet files it has no log for in the other direction. Delete the directory to change the format of a table that exists. The storage format is handled by `fincommons.storage`, which the other modules with a Silver table rebuilt in full share.

The cancellations file is not a table of the Silver layer and is not reached through `connect_to_firds`. It stays in the Bronze layer as its own data set, under the path `firds.path_bronze_auth_102_001_01(config_path)`, and is read as the partitioned parquet it is.
                                                                                   

## Accessing the data

Function `connect_to_firds` can be used to access the FIRDS data sets. To access the historic database as described in the ESMA Guide, i.e. the *main* table, the function must be specified in the following way:


```python
firds_main = firds.connect_to_firds(spark, config_path, "main")
```

The *compact* data set can be accessed using the following specification:


```python
firds_compact = firds.connect_to_firds(spark, config_path, "compact")
```

## Tests

The module is covered by tests that run against a local Spark session
built by `tests/conftest.py` and need no data beyond what they write
themselves. Nothing in them reaches ESMA's register or the network: where a
response is needed, the test supplies it, so that what is asserted is what the
module does with an answer rather than what ESMA happens to be publishing.

| File | Tests | Covers |
| --- | ---: | --- |
| `tests/test_firds_common.py` | 24 | the layout of the data lake, `connect_to_firds`, a check that every path helper the other modules call on `common` exists, and what the package offers a caller and what it does not |
| `tests/test_firds_landing.py` | 52 | the query put to the register and the answers it returns page by page, including empty, truncated and failed ones, how far back the register is asked, which days it records as published, applied and late, the parts it records for the full file, skipping files that are already held, downloading, checking the checksum, and what an interrupted or failed download leaves behind, which cancellations file is fetched, and which files an update fetches again |
| `tests/test_firds_utilities.py` | 27 | which files are candidates for import and what an interrupted download leaves among them, removing an archive that was already extracted, the reference date and the parts read from a file name, whether a day is complete, which listed files have not been imported, unpacking, and reading the schema out of the head of a file |
| `tests/test_firds_bronze.py` | 24 | identifying the schema of a file and the routing that follows from it: into `finished` when it was read, into `error` when it was not or was imported before, the empty partition a file without records leaves, for both archives and bare XML, and for all three schemas |
| `tests/test_firds_history.py` | 17 | the algorithm of section 4.2, against the worked example of the instructions, and records that begin on the same day |
| `tests/test_firds_silver.py` | 61 | the order the delta files are applied in, the decision whether the database has to be initialised, which full file it starts from and whether every part the register lists for it has been imported, the agreement of the merge with the reference implementation of `history`, resuming a date after a run that died, recovering a watermark that was never written, stopping at an incomplete day, at one whose partition has gone missing and at one that arrived after the history passed it, passing a day that held no records, a merge that copies no unchanged rows, the watermark it leaves behind, reading a partition whose files were written with two vintages of the XML schema and one that states a path the other does not, and the `compact` table with the format it is stored in |
| `tests/test_firds_metadata.py` | 10 | the watermark: reading it back, updating it in place and extending it with a second table |

The readers of the FIRDS XML are replaced in the Bronze tests. What is at stake
there is where a file ends up, not how the XML parses, and the parsing is
covered by the Silver and history tests, which work on the frames the readers
produce.

Run them, from the root of the repository, with

```
pytest tests/ -k firds
```

## A worked run

[`docker/tools/run_firds.py`](../../../docker/tools/run_firds.py) takes the
whole pipeline through one cycle on data ESMA has actually published: it builds
the directory tree, downloads one full file, the delta files of the publication
days that follow it and the most recent cancellations file, imports them into
the Bronze layer, builds the history from them and derives the compact table,
and then checks the state that leaves behind -- that every instrument has
exactly one open version, that the version in force is the open one, that no
record ends before it begins, and that `compact` holds one row per ISIN. It is
the place to look for what the stages report and what the tables hold
afterwards. The run takes the date of the full file to start from:

```
docker compose -f docker/compose.yaml run --rm firds 2026-09-05
```

[`docker/tools/run_firds_incremental.py`](../../../docker/tools/run_firds_incremental.py)
applies one further publication day to the history that leaves behind, which is
the path the daily job takes, and checks the same invariants against a history
that already holds millions of records:

```
docker compose -f docker/compose.yaml run --rm --entrypoint python firds tools/run_firds_incremental.py 2026-09-09
```
