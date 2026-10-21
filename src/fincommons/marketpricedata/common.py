"""Market price data: the layout of the data lake.

The Bronze and the Silver layer are addressed through
:mod:`fincommons.addressing`, which reads the configuration file, so that the
same code runs against a local directory tree, against a Unity Catalog volume
and against a schema in a catalogue. The helpers below are the only place those
addresses are spelled out, so that the other modules of the package reach the
data lake through them and never through a path written into the code.

Each publisher gets a directory of its own in the Bronze layer, so that a price
is always traceable to the source it was taken from. The Silver layer holds one
table, in which every source is reduced to the same three columns, and which is
kept as Delta because it is maintained by upsert rather than by overwrite; see
:func:`standard_upsert`. Where the configuration file gives that layer a
catalogue prefix, the table is addressed by name and the marks of
:func:`write_watermark` need a ``path`` beside it, since they are files.

The schema of the proprietary source is declared here as well, because it is
the only source whose shape the module does not learn from the file a publisher
sent.

The sources are downloaded through a temporary directory on the local disk of
the driver rather than through a landing area in the data lake, as in
:mod:`fincommons.exchangerate`: the CSV files are never meant to be kept, and
on Databricks the Bronze layer is a volume that should not be used as scratch
space.
"""

import json
import logging
import os
from contextlib import contextmanager

from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import DateType, DoubleType, StringType, StructType
from pyspark.sql.window import Window

from ..addressing import (
    delta_table,
    delta_table_exists,
    load_paths,
    read_delta,
    resolve,
    write_delta,
)
from ..log import context

logger = logging.getLogger(__name__)


internal_schema = StructType() \
    .add("REFDT", DateType(), False) \
    .add("INSTRID", StringType(), False) \
    .add("VALUE", DoubleType(), False)
"""The shape a proprietary delivery is read in.

It is declared here rather than learnt from the file, as in
:mod:`fincommons.exchangerate`, because the proprietary source is the only one
whose shape the module does not take from what a publisher sent: a delivery is
written by whoever maintains it, and a reference date read as text or a price
read as a string would reach the merge as a type the Silver table does not
hold. The three columns are the three columns of that table, in their order.
"""


#: How long a download waits for a publisher, in seconds, before giving up.
#:
#: It bounds each wait for a connection and for the next piece of an answer,
#: not the download as a whole, so a slow but moving answer is not cut off. It
#: is there for the other case: a request without one waits on a connection
#: that has gone quiet for as long as the operating system lets it, and a
#: scheduled run that hangs on one publisher never reaches the others or the
#: Silver table, and never says why.
HTTP_TIMEOUT = 120

#: The user agent a download sends. Several publishers serve their files
#: through a content delivery network that refuses a request without one, and
#: naming the package lets a publisher see who is asking.
USER_AGENT = "fincommons"


# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "marketpricedata", "bronze").path


def path_bronze_ecb(config_path):
    return os.path.join(path_bronze_base(config_path), "ecb")

def path_bronze_ecb_estr(config_path):
    return os.path.join(path_bronze_ecb(config_path), "estr")

def path_bronze_ecb_fm(config_path):
    return os.path.join(path_bronze_ecb(config_path), "fm")


def path_bronze_nyfed(config_path):
    return os.path.join(path_bronze_base(config_path), "nyfed")

def path_bronze_nyfed_sofr(config_path):
    return os.path.join(path_bronze_nyfed(config_path), "sofr")


def path_bronze_boe(config_path):
    return os.path.join(path_bronze_base(config_path), "boe")

def path_bronze_boe_sonia(config_path):
    return os.path.join(path_bronze_boe(config_path), "sonia")


def path_bronze_internal_marketpricedata(config_path):
    """Folder the proprietary market price data is picked up from.

    Nothing writes into it. Like the proprietary sources of the exchange rate
    and the entity master data modules, the file is placed there by whoever
    maintains the internal quotes, because where such quotes come from and how
    they are produced differs from one organisation to the next.
    """
    return os.path.join(path_bronze_base(config_path), "internal")


def path_bronze_yahoo_marketpricedata(config_path):
    return os.path.join(path_bronze_base(config_path), "yahoo")

def path_bronze_yahoo_volatility(config_path):
    """The volatility indices downloaded from Yahoo Finance.

    Kept apart from the commodities and the listed instruments for the reason
    they are kept apart from each other: each is named by a mapping table of its
    own and each carries on from its own partitions.
    """
    return os.path.join(path_bronze_base(config_path), "yahoo_volatility")


def path_bronze_yahoo_indices(config_path):
    """The stock indices downloaded from Yahoo Finance by name.

    The ten benchmark indices the instrument table carries are named by their
    published index ISIN; these are named directly, because an index ISIN that
    cannot be checked against reference data is not worth inventing.
    """
    return os.path.join(path_bronze_base(config_path), "yahoo_indices")


def path_bronze_electricity(config_path):
    return os.path.join(path_bronze_base(config_path), "electricity")

def path_bronze_electricity_dayahead(config_path):
    return os.path.join(path_bronze_electricity(config_path), "dayahead")


def path_bronze_yahoo_commodities(config_path):
    """The energy and industrial commodities downloaded from Yahoo Finance.

    They are kept apart from the listed instruments rather than written beside
    them, because the two are named by different mapping tables and identified
    differently: an instrument by its ISIN, a commodity by a name, since a
    commodity future has no ISIN.
    """
    return os.path.join(path_bronze_base(config_path), "yahoo_commodities")


def path_bronze_lbma(config_path):
    return os.path.join(path_bronze_base(config_path), "lbma")

def path_bronze_lbma_gold(config_path):
    return os.path.join(path_bronze_lbma(config_path), "gold")

def path_bronze_lbma_silver(config_path):
    return os.path.join(path_bronze_lbma(config_path), "silver")


def path_bronze_yahoo_fx(config_path):
    """The exchange rates downloaded from Yahoo Finance.

    Only the currencies the ECB does not fix a reference rate for, such as the
    Bangladeshi taka. They are named ``EUR/XXX`` like the ECB rates so that a
    reader converts with them the same way, and kept in a layer of their own so
    that a rate in the table stays traceable to who published it.
    """
    return os.path.join(path_bronze_base(config_path), "yahoo_fx")


def path_bronze_dse(config_path):
    """The Dhaka Stock Exchange, see :mod:`fincommons.marketpricedata.bronze_dse`."""
    return os.path.join(path_bronze_base(config_path), "dse")

def path_bronze_dse_dayend(config_path):
    return os.path.join(path_bronze_dse(config_path), "dayend")

def path_bronze_dse_summary(config_path):
    return os.path.join(path_bronze_dse(config_path), "summary")

def path_bronze_dse_news(config_path):
    return os.path.join(path_bronze_dse(config_path), "news")


def path_bronze_vndirect(config_path):
    """The Vietnamese market data of VNDirect, see
    :mod:`fincommons.marketpricedata.bronze_vndirect`."""
    return os.path.join(path_bronze_base(config_path), "vndirect")

def path_bronze_vndirect_prices(config_path):
    return os.path.join(path_bronze_vndirect(config_path), "prices")

def path_bronze_vndirect_indices(config_path):
    return os.path.join(path_bronze_vndirect(config_path), "indices")

def path_bronze_vndirect_listings(config_path):
    return os.path.join(path_bronze_vndirect(config_path), "listings")




# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer.

    ``None`` where the layer is a schema in a catalogue rather than a
    directory, which is what setting up the directory tree checks for.
    """
    return resolve(config_path, "marketpricedata", "silver").path


def path_silver_watermarks(config_path):
    """Where the table records how far each source has already been merged.

    A source whose Bronze layer is immutable once written -- the Yahoo Finance
    layers, which are appended to one download at a time and never rewritten --
    does not have to be merged from the beginning every run. What it does have
    to know is which of its deliveries have already been folded in, and nothing
    in the Silver table says: a row there carries the reference date of a price
    and not the day it was fetched, so the table cannot be asked where the last
    run stopped.

    The mark is kept beside the table rather than inside it for that reason. It
    is one small file per source, written after the merge has succeeded, so a
    run that fails leaves the mark where it was and the next run collects the
    same deliveries again -- which costs a little and is the safe way round: a
    mark written before the merge would skip data that never arrived.

    The marks are files, and a file needs a directory to be written to, which
    a schema in a catalogue is not. A configuration file that addresses the
    Silver layer by catalogue name therefore has to give a ``path`` beside the
    ``table`` for the marks, and is refused here where it gives none: a run
    that quietly went on without them would collect and merge the whole
    history of every incremental source again, every morning.
    """
    silver = resolve(config_path, "marketpricedata", "silver")
    if silver.path is None:
        raise ValueError(
            f"the 'silver' layer of 'marketpricedata' in {config_path!r} is "
            "addressed by catalogue name alone, and the marks that say how far "
            "each source has been merged are files. Give the layer a 'path' "
            "beside its 'table' for them to be written to."
        )
    return os.path.join(silver.path, "_watermarks")


class Delivery(str):
    """A watermark that also knows how many files its partition held.

    A Bronze partition is named by the day its download ran, and a second
    download on the same day -- tickers added to a mapping table after the
    morning's run -- appends to the partition the mark already names. The
    mark alone cannot tell that partition has grown since it was merged;
    the number of files in it can, so the mark records it. It is a ``str``
    so that every caller that passes a watermark on keeps working.
    """

    def __new__(cls, value, files=None):
        self = super().__new__(cls, str(value))
        self.files = files
        return self


def read_watermark(config_path, source):
    """How far a source has already been merged, or None where it never has.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    source: str
        Name of the source, as :func:`write_watermark` recorded it.
    """
    mark = os.path.join(path_silver_watermarks(config_path), f"{source}.json")
    if not os.path.isfile(mark):
        return None

    try:
        with open(mark, encoding="utf-8") as handle:
            return json.load(handle).get("watermark")
    except (ValueError, OSError):
        # A mark that cannot be read is treated as one that was never written.
        # The cost is a full merge, which is correct if slow; refusing to run
        # over a file that says how fast to go would not be.
        return None


def read_watermark_files(config_path, source):
    """How many files the last merged partition held, or None where unknown.

    A mark written before the count was recorded has none, and the partition
    it names is then treated as unchanged, which is how it was treated before.
    """
    mark = os.path.join(path_silver_watermarks(config_path), f"{source}.json")
    try:
        with open(mark, encoding="utf-8") as handle:
            return json.load(handle).get("files")
    except (ValueError, OSError):
        return None


def write_watermark(config_path, source, watermark, files=None):
    """Record how far a source has now been merged.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    source: str
        Name of the source.
    watermark: str
        The last delivery folded into the Silver table, as the Bronze layer
        names it -- for the Yahoo sources, the newest ``LASTDT`` partition. It
        is written as text whatever it arrives as, because a partition column
        read back from parquet is a ``datetime.date`` and the mark has to be a
        date the next run can compare against and a reader can read.
    files: int, optional
        How many files the partition named by ``watermark`` held when it was
        merged; see :class:`Delivery`, which carries it where it is not given.
    """
    if files is None:
        files = getattr(watermark, "files", None)
    if _batch is not None:
        # The merge has not happened yet, and a mark must never run ahead of
        # it. It is held until the batch closes and written there.
        _batch["marks"].append((source, str(watermark), files))
        return

    folder = path_silver_watermarks(config_path)
    os.makedirs(folder, exist_ok=True)
    mark = {"source": source, "watermark": str(watermark)}
    if files is not None:
        mark["files"] = int(files)
    with open(os.path.join(folder, f"{source}.json"), "w",
              encoding="utf-8") as handle:
        json.dump(mark, handle)


def ref_silver_marketpricedata(config_path):
    """Where the combined market price data lives.

    In a local data lake this is a directory in the Silver layer. Where the
    configuration file gives the layer a catalogue prefix instead, it is a
    table in that schema and the pipeline addresses it by name, which is what
    puts it within reach of Unity Catalog's grants and lineage.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return resolve(config_path, "marketpricedata", "silver", "marketpricedata")


def ref_silver_dse_corporate_actions(config_path):
    """Where the corporate actions of the DSE shares live.

    A table of its own beside the prices rather than rows in them, because an
    event is not a price: it has a kind and a factor, and a reader adjusts the
    closes with it or not as the question asks. See
    :mod:`fincommons.marketpricedata.dse_events`.
    """
    return resolve(config_path, "marketpricedata", "silver",
                   "dse_corporate_actions")


def connect_to_dse_corporate_actions(spark, config_path):
    """Opens the corporate actions of the DSE shares.

    One row per event: ``INSTRID`` as the price table names the share,
    ``EXDATE``, ``KIND`` (``bonus``, ``cash`` or ``rights``) and ``FACTOR``,
    the number a close before the ex-date is multiplied by to be comparable
    with one on or after it. Multiplying by the ``bonus`` and ``rights``
    factors gives a price history, and by all three a total return.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return read_delta(spark, ref_silver_dse_corporate_actions(config_path))


def path_silver_marketpricedata(config_path):
    """Path of the Delta table that holds the combined market price data.

    Kept for the tests and for a notebook that looks at the files themselves.
    It is ``None`` where the table is addressed by catalogue name, so the
    pipeline goes through :func:`ref_silver_marketpricedata` instead.
    """
    return ref_silver_marketpricedata(config_path).path




def migrate_folder_names(config_path):
    """Rename the hyphenated folders and files of the lake to snake_case.

    The marketpricedata module spells its folders and the files it writes in
    snake_case -- ``yahoo_indices`` rather than ``yahoo-indices`` -- since
    names travel into a SQL catalogue, where a hyphen has to be quoted, and it
    reads them under those names only. This renames the folders and files of
    the module that are spelled with hyphens to the snake_case names it reads.
    Partition directories and folders named by a date are left alone, and a
    name whose snake_case form exists as well is raised rather than merged,
    see :func:`fincommons.addressing.rename_folders_to_snake_case`. A lake
    already in snake_case is left as it is.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    list of tuple
        The folders and files renamed, as pairs of old and new path.
    """
    import logging

    from ..addressing import rename_files, rename_folders_to_snake_case
    from ..log import context

    renamed = rename_folders_to_snake_case(
        path_bronze_base(config_path),
        path_silver_base(config_path),
    )
    renamed += rename_files([
        (os.path.join(path_bronze_internal_marketpricedata(config_path), "marketpricedata-internal.csv"), os.path.join(path_bronze_internal_marketpricedata(config_path), "marketpricedata_internal.csv")),
    ])
    logging.getLogger(__name__).info(
        "Renamed the folders and files of the data lake to snake_case",
        extra=context(module="marketpricedata", rows=len(renamed)),
    )
    return renamed


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    Creates the basic directory tree which is used for market price data.

    Directories that already exist are left alone and missing parents are
    created along the way, so that the call can be repeated and so that a data
    lake configured below a directory that does not exist yet is set up in one
    go. It is meant for a local data lake; on Databricks the volume and the
    directories in it are provisioned outside the pipeline.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """

    folder_list = [path_bronze_base(config_path),
                   path_bronze_ecb(config_path),
                   path_bronze_ecb_estr(config_path),
                   path_bronze_ecb_fm(config_path),
                   path_bronze_nyfed(config_path),
                   path_bronze_nyfed_sofr(config_path),
                   path_bronze_boe(config_path),
                   path_bronze_boe_sonia(config_path),
                   path_bronze_internal_marketpricedata(config_path),
                   path_bronze_yahoo_marketpricedata(config_path),
                   path_bronze_yahoo_commodities(config_path),
                   path_bronze_yahoo_volatility(config_path),
                   path_bronze_yahoo_indices(config_path),
                   path_bronze_electricity(config_path),
                   path_bronze_lbma(config_path),
                   path_bronze_lbma_gold(config_path),
                   path_bronze_lbma_silver(config_path),
                   path_bronze_yahoo_fx(config_path),
                   path_bronze_dse(config_path),
                   path_bronze_dse_dayend(config_path),
                   path_bronze_dse_summary(config_path),
                   path_bronze_dse_news(config_path),
                   path_bronze_vndirect(config_path),
                   path_bronze_vndirect_prices(config_path),
                   path_bronze_vndirect_indices(config_path),
                   path_bronze_vndirect_listings(config_path)]

    silver = resolve(config_path, "marketpricedata", "silver")
    if not silver.is_named and silver.path is not None:
        folder_list.append(silver.path)

    for folder in folder_list:
        os.makedirs(folder, exist_ok=True)


def read_bronze(spark, parquet_path, source):
    """Open a parquet data set of the Bronze layer, or None where it holds none.

    A source whose download has never succeeded has no data set yet, or an
    empty directory that :func:`create_directory_tree` made for it, and its
    Silver stage then has nothing to merge. That is a run that did less than a
    reader would assume rather than one that failed, so it is a ``WARNING`` and
    the stage returns.

    Only Spark's refusal to analyse the read is taken to mean that: a path that
    does not exist, or a directory without a file to infer the schema from.
    Anything else -- a permission problem, a file that is not parquet -- is
    raised, so that the run sets the source aside with its traceback instead of
    reporting it as merely empty. The schema is asked for inside the check
    because under Spark Connect a read is not analysed until it is used.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    parquet_path: str
        Path of the data set.
    source: str
        Name of the source, for the log.

    Returns
    -------
    pyspark.sql.DataFrame or None
        The data set, or ``None`` where there is none to read.
    """
    try:
        bronze = spark.read.format("parquet").load(parquet_path)
        bronze.schema
    except AnalysisException:
        logger.warning(
            "The Bronze layer holds no data for the source, nothing is merged",
            extra=context(module="marketpricedata", layer="silver",
                          source=source, path=parquet_path),
        )
        return None
    return bronze


def connect_to_marketpricedata(spark, config_path):
    """Opens the combined market price data set of the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return read_delta(spark, ref_silver_marketpricedata(config_path))


#: The batch a run is collecting into, or None where each source merges on its
#: own. It is module state rather than an argument because every source reaches
#: the Silver table through :func:`standard_upsert` and none of them has to know
#: whether it is running inside a batch. One batch at a time: a run holds one
#: Spark session and merges into one table, and nesting them would mean two
#: answers to the question of which source wins.
_batch = None


@contextmanager
def collected_upserts(spark, config_path):
    """Collect the sources of a run and merge them into the table once.

    **A merge costs what it costs whatever it carries.** It rewrites the files
    it touches in a table of a million rows, and that fixed cost is most of the
    bill: measured here, a source of two thousand rows and a source of a
    hundred and sixty each took about fifteen seconds. Eleven sources merging
    one after another therefore cost eleven times that, however little each of
    them had to say, and a run that had nothing new to fold in cost almost as
    much as one that had a year.

    Inside this block :func:`standard_upsert` puts its rows aside instead of
    merging them, and the block merges all of them together when it closes.

    **The order the sources are collected in is what decides between them.** A
    reference date and an instrument delivered by two sources can only be one
    row, and a merge given that pair twice fails outright rather than choosing,
    so the collected rows are reduced to the last delivery of each pair before
    they are merged. Last, here, means latest collected, which is the rule
    that merging the sources one after another would give, each one
    overwriting what the one before it had written, and it is why
    :func:`update_silver_layer` runs the proprietary source last.

    The marks of :func:`write_watermark` are held back in the same way and
    written only once the merge has succeeded, for the reason they are written
    after a merge at all: a mark that claims a delivery the table never
    received would have the next run skip it for good.

    A source that fails inside the block has contributed nothing, because a
    stage puts its rows aside as its last act, so the block merges what the
    sources that succeeded delivered and the run carries on -- which is what it
    did when they merged separately.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> with collected_upserts(spark, config_path):
    ...     update_silver_ecb(spark, config_path)
    ...     update_silver_internal(spark, config_path)
    """
    global _batch

    if _batch is not None:
        raise RuntimeError(
            "A batch of upserts is already being collected. One run merges "
            "into one table, and two batches would each claim to decide which "
            "source wins."
        )

    _batch = {"rows": [], "marks": []}
    try:
        yield _batch
        collected = _batch
    finally:
        _batch = None

    flush_collected_upserts(spark, config_path, collected)


def flush_collected_upserts(spark, config_path, collected):
    """Merge what a batch collected, then record how far its sources got."""
    frames = collected["rows"]

    if frames:
        # The priority is the order the sources were collected in, and the last
        # delivery of a pair is the one that is kept. Without this the merge
        # would be handed the same instrument and reference date twice wherever
        # two sources quote it, and would fail rather than choose.
        combined = None
        for priority, frame in enumerate(frames):
            frame = frame.select("REFDT", "INSTRID", "VALUE") \
                         .withColumn("PRIORITY", lit(priority))
            combined = frame if combined is None else combined.unionByName(frame)

        sources = Window.partitionBy("INSTRID", "REFDT") \
                        .orderBy(col("PRIORITY").desc())
        combined = combined \
            .withColumn("DELIVERY", row_number().over(sources)) \
            .filter(col("DELIVERY") == 1) \
            .select("REFDT", "INSTRID", "VALUE")

        merge_into_silver(spark, config_path, combined)

    for source, watermark, files in collected["marks"]:
        write_watermark(config_path, source, watermark, files)


def standard_upsert(spark, config_path, bronze):
    """Standard upsert.

    Performs the standard upsert operation. Every source writes the Silver
    table through this function, which is why it is kept here beside the paths
    rather than with any one of them: a row is identified by its instrument and
    its reference date, and a source that delivers a date again replaces the
    row it delivered before instead of adding a second one.

    Inside a :func:`collected_upserts` block the rows are put aside and merged
    with the rest of the run's sources instead, which is what a scheduled run
    does; called on its own it merges them itself, which is what a single
    source brought up to date by hand does.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    bronze: pyspark.sql.DataFrame
        The rows to be merged into the Silver table.
    """
    if _batch is not None:
        _batch["rows"].append(complete_rows(bronze))
        return

    merge_into_silver(spark, config_path, bronze)


def complete_rows(bronze):
    """The rows of a delivery that can be merged: the three columns, all set.

    **A row without an instrument or a reference date is never merged.** The
    merge matches on those two columns, and a null does not equal anything, a
    null included -- so such a row matches nothing already in the table and is
    inserted afresh on every run, one more copy each time. A proprietary file
    with a date the reader cannot parse delivers exactly that.

    **A row without a value is not merged either.** A null is a publisher saying
    it has no observation for that day, and written into the table it would
    replace an observation that was there, or be taken as the newest quote by a
    reader looking for the last one on or before a date. The ECB publishes such
    gaps in its monthly series.

    It is applied before the sources of a batch are reduced to one delivery per
    instrument and day, so that a source with nothing to say about a day does
    not win that day over one that has a figure for it.
    """
    return bronze.select("REFDT", "INSTRID", "VALUE") \
                 .dropna(subset=["REFDT", "INSTRID", "VALUE"])


def merge_into_silver(spark, config_path, bronze):
    """Merge a set of rows into the Silver table.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    bronze: pyspark.sql.DataFrame
        The rows to be merged, holding each instrument and reference date once.
    """

    # The correct variables in the correct order, and only complete rows
    bronze = complete_rows(bronze)

    ref = ref_silver_marketpricedata(config_path)

    if delta_table_exists(spark, ref):

        silver = delta_table(spark, ref)

        # Upsert
        silver.alias("s") \
            .merge(bronze.alias("b"), "s.INSTRID = b.INSTRID AND s.REFDT = b.REFDT") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    else:
        logger.info(
            "The Silver table does not exist yet and is created",
            extra=context(module="marketpricedata", layer="silver"),
        )
        write_delta(bronze, ref)
