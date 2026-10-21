"""Exchange rates: the combined daily table in the Silver layer.

The sources overlap, and none of them covers every day. The Silver layer
settles both questions: where a pair is quoted by more than one source the
proprietary data takes precedence over the ECB and the ECB over the BIS, and
the days between two quotes carry the last rate that stood, and are marked as
such in IMPUTED. The result is one row per calendar day, base and quote
currency, which is what :func:`convert2ccy` converts an amount with.
"""

import logging
import os

import pandas
import pyspark.sql.functions as func
from pyspark.sql import SparkSession, Window

from ..log import context, pipeline_run
from .bronze import *
from .common import *

logger = logging.getLogger(__name__)


def connect_to_exchangerate(spark, config_path):
    """Connects to the exchange rate data set.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    return read_silver(spark, ref_silver_exchangerate_table(config_path))


def convert2ccy(data, config_path, date, amount, currency, target, spark = None):
    """Converts a column of amounts into the target currency.

    The conversion is done with Spark, against the exchange rate table. A
    pandas or polars frame is handed to Spark for it and comes back as a frame
    of its own kind, in its own row order and, for pandas, with its own index.

    Parameters
    ----------
    data : pyspark.sql.DataFrame, pandas.DataFrame or polars.DataFrame
        The frame holding the amounts, their currencies and their dates.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    date : str
        Name of the column, which contains the date. It may hold dates,
        timestamps or text such as ``2024-06-03``; an amount is converted at
        the rate of its calendar day, so a time of day does not matter.
    amount : str
        Name of the column, which contains the monetary amount.
    currency : str
        Name of the column, which contains the currency code.
    target : str
        A 3-digit ISO currency code.
    spark: pyspark.sql.session.SparkSession, optional
        The session a pandas or polars frame is converted with. Defaults to
        the active session, which is the one a Databricks notebook already
        provides. A Spark frame is always converted with its own session.

    Returns
    -------
    pyspark.sql.DataFrame, pandas.DataFrame or polars.DataFrame
        ``data``, as the kind of frame it was given as, with two columns
        added: ``<amount>_FXRATE``, the rate that was applied, and
        ``<amount>_<target>``, the converted amount. An amount already in
        ``target`` is converted at a rate of 1; one whose currency is
        missing, or not covered by the table on its day, gets neither.

    Raises
    ------
    TypeError
        If ``data`` is none of the three kinds of frame.
    RuntimeError
        If a pandas or polars frame is given and there is no Spark session.
    ValueError
        If the table holds no rate quoted against ``target`` and some amount
        is in another currency. The table is rebased as a whole to one
        currency, so converting into any other finds no rate at all; joined
        on the left, that would give every such amount null without saying
        why. A single currency the table does not cover still gives null for
        its own amounts.
    """
    names = dict(config_path = config_path, date = date, amount = amount,
                 currency = currency, target = target)

    # Asked of the frame rather than checked with isinstance, because a frame
    # of Spark Connect, which shared and serverless compute on Databricks
    # hand out, is not a pyspark.sql.DataFrame before pyspark 4.0.
    if hasattr(data, "sparkSession"):
        return _convert2ccy_spark(data, **names)

    kind = type(data)
    is_pandas = isinstance(data, pandas.DataFrame)
    # polars is recognised by name, so that it is not imported for a caller
    # who does not use it.
    is_polars = kind.__module__.split(".")[0] == "polars" and kind.__name__ == "DataFrame"
    if not (is_pandas or is_polars):
        raise TypeError(
            f"convert2ccy converts a Spark, pandas or polars data frame, got "
            f"{kind.__module__}.{kind.__name__}."
        )

    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "convert2ccy reads the exchange rate table with Spark. Pass a session "
            "with spark = ..., or create one before calling."
        )

    if is_polars:
        import polars
        return polars.from_pandas(_convert2ccy_pandas(data.to_pandas(), spark, **names))
    return _convert2ccy_pandas(data, spark, **names)


def _convert2ccy_pandas(data, spark, **names):
    """Convert a pandas frame with Spark and hand it back as pandas.

    A Spark join does not keep the order of the rows, so the position of each
    row is carried through the conversion and the rows are put back in it;
    the index, which Spark has no place for, is restored afterwards.
    """
    position = "_fincommons_row"
    frame = data.reset_index(drop = True)
    frame[position] = range(len(frame))
    result = _convert2ccy_spark(spark.createDataFrame(frame), **names) \
        .toPandas() \
        .sort_values(position) \
        .drop(columns = position)
    result.index = data.index
    return result


def _convert2ccy_spark(data, config_path, date, amount, currency, target):
    """Convert a Spark frame; see :func:`convert2ccy`."""
    spark = data.sparkSession

    fxrate_column_name = amount + "_FXRATE"
    new_amount_column_name = amount + "_" + target
    day_column = "_" + amount + "_FXDAY"

    fxdata = connect_to_exchangerate(spark, config_path) \
        .filter(func.col("BASE") == target) \
        .select(func.col("REFDATE").cast("date").alias(day_column),
                func.col("QUOTE").alias(currency),
                func.col("VALUE").alias(fxrate_column_name))
    if fxdata.limit(1).count() == 0 \
            and data.filter(~func.col(currency).eqNullSafe(target)).limit(1).count() > 0:
        raise ValueError(
            f"The exchange rate table holds no rate quoted against {target!r}. "
            f"The table is rebased as a whole to one currency rather than "
            f"carrying every pair, so converting into {target!r} needs it "
            f"built with rebase={target!r}; see update_exchangerate."
        )

    # The rates are matched by calendar day, so that an amount dated with a
    # time of day, 2024-06-03 10:00, meets the rate of 2024-06-03. try_cast
    # answers a date it cannot read with null rather than failing the whole
    # conversion under ANSI mode; such an amount gets no rate, like one whose
    # currency the table does not cover.
    rate = func.when(func.col(currency) == target, func.lit(1.0)) \
        .otherwise(func.col(fxrate_column_name))
    return data \
        .withColumn(day_column, func.expr(f"try_cast(`{date}` AS date)")) \
        .join(fxdata, on = [day_column, currency], how = "left") \
        .withColumn(fxrate_column_name, rate) \
        .withColumn(new_amount_column_name,
                    func.col(amount) / func.col(fxrate_column_name)) \
        .select(*[func.col(c) for c in data.columns],
                fxrate_column_name, new_amount_column_name)


def check_internal_rates(internal, source):
    """Refuse an internal file whose rows the combination would misread.

    The combination compares reference dates as text -- to find the date every
    source reaches, to cut the sources back to it, and to decide which source
    wins a day -- so a date the internal file writes differently from the ECB,
    ``2024-6-3`` or ``2024-06-03 00:00:00`` for ``2024-06-03``, is not the
    same day to it. Depending on the date, such a row was dropped without a
    word, or kept beside the ECB's quote of the same day as a second rate for
    it, and it could move the cutoff of the whole table. The other checks are
    of the same kind: a base other than the euro, which the rest of the
    pipeline is not built for and which a rebase drops; one day and pair
    quoted twice, of which the combination would keep either; and a rate that
    is missing or not positive, which would win its day over a real one.

    Parameters
    ----------
    internal: pyspark.sql.DataFrame
        The internal file, read with :data:`internal_schema`.
    source: str
        Where it was read from, for the message.

    Raises
    ------
    ValueError
        Naming each problem found, with a few of the rows that have it.
    """
    problems = {
        "a REFDATE that is not a date written as YYYY-MM-DD":
            ~func.col("REFDATE").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
            | func.to_date(func.col("REFDATE"), "yyyy-MM-dd").isNull(),
        "a BASE other than EUR": func.col("BASE") != "EUR",
        "a VALUE that is missing or not positive":
            func.col("VALUE").isNull() | (func.col("VALUE") <= 0),
    }
    found = []
    for problem, condition in problems.items():
        rows = internal.filter(func.coalesce(condition, func.lit(True)))
        count = rows.count()
        if count:
            examples = [tuple(row) for row in rows.limit(3).collect()]
            found.append(f"{count} rows with {problem}, e.g. {examples}")

    duplicated = internal.groupBy("REFDATE", "BASE", "QUOTE").count() \
        .filter(func.col("count") > 1)
    count = duplicated.count()
    if count:
        examples = [tuple(row)[:3] for row in duplicated.limit(3).collect()]
        found.append(f"{count} days and pairs quoted more than once, e.g. {examples}")

    if found:
        raise ValueError(
            f"The internal exchange rates in {source} cannot be combined as "
            f"they are: " + "; ".join(found) + "."
        )


def update_silver_exchangerate(spark, config_path, rebase = None, use_bis = False,
                               file_format = None):
    """Update the exchange rate data in the silver layer

    Reads the public exchange rate data from the BIS and the ECB from the Bronze layer
    and combines them. After dropping duplicate entries and interpolating values over weekends
    the result is saved in the silver layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    rebase : str, optional
        ISO currency code of the currency that should become the new base currency instead of EUR.
        The code must be contained in the EZB reference rate data set.
    use_bis : bool
        Whether the BIS rates in the Bronze layer are combined in as well.
    file_format: str, optional
        Format the table is written in, ``'parquet'`` or ``'delta'``. Where it
        is left out the configuration file decides: a Silver layer addressed
        by catalogue name is written as Delta, and one addressed by path keeps
        the format its table is stored in, or is written as parquet where
        there is no table yet. Parquet asked of a catalogue table, or a format
        other than the one the table is stored in, is refused before anything
        is read. See :func:`fincommons.storage.write_silver` for what the
        choice amounts to.

    Examples
    --------
    >>> update_silver_exchangerate("path/to/configuration/file")
    """
    file_format = silver_format(ref_silver_exchangerate_table(config_path), file_format)

    ecb_file = os.path.join(path_bronze_ecb_exchangerate(config_path), "ecb_exchangerate.parquet")
    ecb = spark.read.parquet(ecb_file) \
        .unpivot(ids = "Date", values = None, variableColumnName = "QUOTE", valueColumnName = "VALUE") \
        .filter(func.col("VALUE").isNotNull()) \
        .withColumn("BASE", func.lit("EUR")) \
        .withColumn("TYPE", func.lit("ref")) \
        .withColumn("SOURCE", func.lit("ECB")) \
        .withColumnRenamed("Date", "REFDATE") \
        .select("REFDATE", "BASE", "QUOTE", "VALUE", "TYPE", "SOURCE") \
        .sort(func.col("REFDATE"))

    ecb_max_date = ecb.select(func.max("REFDATE")).first()[0]

    # A rebase to a currency the ECB does not quote finds no euro leg to divide
    # by, and every row of the table would be dropped: the table would be
    # replaced by an empty one without a word. The euro itself is the base the
    # table already has. Both are refused before anything is written.
    if rebase is not None:
        if rebase == "EUR":
            raise ValueError(
                "The table is based on the euro already; leave rebase out "
                "rather than rebasing to 'EUR'."
            )
        if ecb.filter(func.col("QUOTE") == rebase).limit(1).count() == 0:
            raise ValueError(
                f"Cannot rebase to {rebase!r}: the ECB reference rates in the "
                f"Bronze layer hold no rate against it. The table was left as "
                f"it was."
            )

    if use_bis:
        bis_file = os.path.join(path_bronze_bis_exchangerate(config_path), "bis_exchangerate.parquet")
        bis = spark.read.parquet(bis_file) \
            .filter(func.col("FREQ:Frequency") == "D: Daily") \
            .filter(func.substring(func.col("COLLECTION:Collection"), 0, 1) == "A") \
            .withColumn("QUOTE", func.substring(func.col("CURRENCY:Currency"), 0, 3)) \
            .filter(func.col("QUOTE") != "EUR") \
            .withColumn("BASE", func.lit("USD")) \
            .withColumn("SOURCE", func.lit("BIS")) \
            .withColumnRenamed("TIME_PERIOD:Time period or range", "REFDATE") \
            .withColumnRenamed("OBS_VALUE:Observation Value", "VALUE") \
            .filter(func.col("VALUE").isNotNull()) \
            .withColumn("TYPE", func.lit("avg")) \
            .select("REFDATE", "BASE", "QUOTE", "VALUE", "TYPE", "SOURCE")
        
        bis_max_date = bis.select(func.max("REFDATE")).first()[0]
    else:
        bis_max_date = ecb.select(func.max("REFDATE")).first()[0]

    
    internal_file = os.path.join(path_bronze_internal_exchangerate(config_path), "exchangerate.csv")
    use_internal = os.path.exists(internal_file)

    if use_internal:
        internal = spark.read \
            .options(header='True', delimiter=',') \
            .schema(internal_schema) \
            .csv(internal_file) \
            .select("REFDATE", "BASE", "QUOTE", "VALUE", "TYPE", "SOURCE")
        check_internal_rates(internal, internal_file)
        internal_max_date = internal.select(func.max("REFDATE")).first()[0]
    else:
        internal_max_date = ecb_max_date

    cutoff_date = min([ecb_max_date, bis_max_date, internal_max_date])

    # The cutoff and the date every source reaches are reported together in one
    # record, because the cutoff is only interpretable next to the dates it was
    # taken from: which source is holding the table back is the question a
    # reader of the log asks.
    logger.info(
        "Combining the sources up to the date they all reach",
        extra=context(
            module="exchangerate",
            layer="silver",
            ecb=ecb_max_date,
            bis=bis_max_date if use_bis else None,
            internal=internal_max_date if use_internal else None,
            cutoff=cutoff_date,
        ),
    )
    
    # Filter datasets to this cutoff date
    ecb = ecb.filter(func.col("REFDATE") <= func.lit(cutoff_date))
    if use_internal:
        internal = internal.filter(func.col("REFDATE") <= func.lit(cutoff_date))

    # Convert BIS' USD-FX-rates into EUR-FX-rates
    if use_bis:
        bis = bis.filter(func.col("REFDATE") <= func.lit(cutoff_date))
        
        eur_usd = ecb \
            .filter((func.col("BASE") == "EUR") & (func.col("QUOTE") == "USD")) \
            .select("REFDATE", "VALUE") \
            .withColumnRenamed("VALUE", "FX")
        
        bis = bis \
            .join(eur_usd, on = "REFDATE", how = "leftouter") \
            .filter(func.col("FX").isNotNull()) \
            .withColumn("VALUE", func.col("VALUE") * func.col("FX")) \
            .drop("FX") \
            .withColumn("BASE", func.lit("EUR"))
    
    # Internal over ECB over BIS. The precedence is carried by an explicit rank
    # rather than by SOURCE, because the SOURCE values of the internal data set
    # are whatever the internal CSV happens to contain.
    window_spec = Window.partitionBy("REFDATE", "BASE", "QUOTE").orderBy(func.col("source_rank"))

    if use_internal:
        result = internal.withColumn("source_rank", func.lit(1)) \
            .union(ecb.withColumn("source_rank", func.lit(2)))
    else:
        result = ecb.withColumn("source_rank", func.lit(2))

    if use_bis:
        result = result.union(bis.withColumn("source_rank", func.lit(3)))
    
    result = result \
        .withColumn("row_ctrpty", func.row_number().over(window_spec)) \
        .filter(func.col("row_ctrpty") == 1) \
        .drop("row_ctrpty", "source_rank")
    
    # Generate sequences between dates. The fill follows the time series of
    # the pair and not that of the individual source, because the quote that
    # closes a gap need not come from the source that opened it.
    #
    # IMPUTED marks the days that carry a rate from an earlier day rather than
    # a rate quoted on them. Each quote opens a run of days that ends the day
    # before the next quote of the pair, and the first day of the run is the
    # quote itself, so its position in the run is all it takes. That is a
    # weekend or a TARGET closing day wherever the ECB is the source -- the ECB
    # does not publish on those -- and any other day without a quote as well,
    # whatever the reason for it.
    window_spec = Window.partitionBy("BASE", "QUOTE").orderBy("REFDATE")
    result = result \
        .withColumn("next_date", func.lead("REFDATE").over(window_spec)) \
        .withColumn("next_date", func.date_sub(func.col("next_date"), 1)) \
        .withColumn("next_date", func.when(func.col("next_date").isNull(), func.col("REFDATE")).otherwise(func.col("next_date"))) \
        .withColumn("dates", func.sequence(func.col("REFDATE"), func.col("next_date"))) \
        .select(func.posexplode(func.col("dates")).alias("POS", "REFDATE"),
                "BASE", "QUOTE", "VALUE", "TYPE", "SOURCE") \
        .withColumn("IMPUTED", func.col("POS") > 0) \
        .drop("POS")

    if rebase is not None:

        eur = result \
            .filter((func.col("BASE") == "EUR") & (func.col("QUOTE") == rebase)) \
            .withColumn("QUOTE", func.lit("EUR")) \
            .withColumn("BASE", func.lit(rebase)) \
            .withColumn("VALUE", 1 / func.col("VALUE"))
        
        eur_rebase = result \
            .filter((func.col("BASE") == "EUR") & (func.col("QUOTE") == rebase)) \
            .select("REFDATE", "VALUE", "IMPUTED") \
            .withColumnRenamed("VALUE", "FX") \
            .withColumnRenamed("IMPUTED", "FX_IMPUTED")
        
        result = result \
            .filter(func.col("BASE") == "EUR") \
            .join(eur_rebase, on = "REFDATE", how = "leftouter") \
            .filter(func.col("FX").isNotNull()) \
            .withColumn("VALUE", func.col("VALUE") / func.col("FX")) \
            .withColumn("IMPUTED", func.col("IMPUTED") | func.col("FX_IMPUTED")) \
            .drop("FX", "FX_IMPUTED") \
            .withColumn("BASE", func.lit(rebase)) \
            .filter(func.col("BASE") != func.col("QUOTE"))

        result = result.unionByName(eur)

    # Save the result. The write replaces the table rather than adding to it,
    # because the combination is rebuilt from the Bronze layer in full every
    # time it is run. As Delta that is one transaction; as parquet a reader
    # may find the table missing while it is written.
    write_silver(
        result, ref_silver_exchangerate_table(config_path), file_format = file_format
    )


def update_exchangerate(spark, config_path, rebase = None, use_bis = False,
                        file_format = None):
    """Updates the exchange rate data in the silver layer

    Updates the exchange rate data set by calling the neccessary update
    functions sequentially.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    rebase : str
        ISO currency code of the currency that should become the new base currency instead of EUR.
        The code must be contained in the EZB reference rate data set.
    use_bis : bool
        Whether the BIS rates are downloaded and combined in as well.
    file_format: str, optional
        Format the Silver table is written in, ``'parquet'`` or ``'delta'``.
        Where it is left out the configuration file decides, so that moving
        the Silver layer into a catalogue takes a change of the configuration
        file alone; see :func:`update_silver_exchangerate`.

    Examples
    --------
    >>> update_exchangerate("path/to/configuration/file")
    """
    file_format = silver_format(ref_silver_exchangerate_table(config_path), file_format)
    with pipeline_run(
        logger,
        "update_exchangerate",
        module="exchangerate",
        config_path=config_path,
        file_format=file_format,
    ):
        _update_exchangerate(spark, config_path, rebase, use_bis, file_format)


def _update_exchangerate(spark, config_path, rebase, use_bis, file_format):
    """Do the work of :func:`update_exchangerate`."""
    # Update bronze
    if use_bis:
        # BIS data
        update_bronze_bis_exchangerate(config_path)
    
    # ECB data
    update_bronze_ecb_exchangerate(config_path)
    
   # Update silver
    update_silver_exchangerate(spark = spark, config_path = config_path, rebase = rebase,
                               use_bis = use_bis, file_format = file_format)
