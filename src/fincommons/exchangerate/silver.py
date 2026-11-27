"""Exchange rates: the combined daily table in the Silver layer.

The sources overlap, and none of them covers every day. The Silver layer
settles both questions: where a pair is quoted by more than one source the
proprietary data takes precedence over the ECB and the ECB over the BIS, and
the days between two quotes carry the last rate that stood, for up to a week,
and are marked as such in IMPUTED. The result is one row per calendar day, base and quote
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

#: How many days after its quote a rate is carried at most. A week covers a
#: weekend, the TARGET closing days around Easter and Christmas, and a source
#: that is a few days late. A pair that is not quoted for longer -- the ECB
#: suspended the Icelandic krona from December 2008 to February 2018 -- is left
#: without a rate for the days beyond, rather than converted at a rate that
#: may be years old.
MAX_FILL_DAYS = 7


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
        If ``data`` already has a column the conversion adds, because it has
        been converted into ``target`` before. And if the table holds no rate
        quoted against ``target`` and some amount is in another currency. The table is rebased as a whole to one
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

    # The columns the conversion adds must not be in the frame already, as they
    # are where it has been converted into the same target before: the join
    # would bring in a second column of the same name and fail on it.
    taken = [c for c in (fxrate_column_name, new_amount_column_name, day_column)
             if c in data.columns]
    if taken:
        raise ValueError(
            f"The frame already has the column(s) {taken} that converting "
            f"{amount!r} into {target!r} adds. Drop them, or convert from a "
            f"frame that has not been converted into {target!r} before."
        )

    fxdata = connect_to_exchangerate(spark, config_path) \
        .filter(func.col("base") == target) \
        .select(func.col("ref_date").cast("date").alias(day_column),
                func.col("quote").alias(currency),
                func.col("value").alias(fxrate_column_name))
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


def check_internal_rates(internal, source, base = "EUR"):
    """Refuse an internal file whose rows the combination would misread.

    The combination compares reference dates as text -- to decide which source
    wins a day and to order the quotes of a pair -- so a date the internal file
    writes differently from the ECB, ``2024-6-3`` or ``2024-06-03 00:00:00``
    for ``2024-06-03``, is not the same day to it. Such a row would be kept
    beside the ECB's quote of the same day as a second rate for it, or be
    misplaced among the quotes of its pair. The other checks are
    of the same kind: a base other than the one the file is expected to quote
    against, which the rest of the pipeline is not built for and which a
    rebase drops; one day and pair quoted twice, of which the combination
    would keep either; and a rate that is missing or not positive, which would
    win its day over a real one.

    Parameters
    ----------
    internal: pyspark.sql.DataFrame
        The internal file, read with :data:`internal_schema`.
    source: str
        Where it was read from, for the message.
    base: str, optional
        ISO currency code every row is expected to quote against. The euro by
        default, which is the base :func:`update_silver_exchangerate` combines
        the sources on; name another one to check a file that is kept against
        a different base, outside that combination.

    Raises
    ------
    ValueError
        Naming each problem found, with a few of the rows that have it.
    """
    problems = {
        "a ref_date that is not a date written as YYYY-MM-DD":
            ~func.col("ref_date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
            | func.to_date(func.col("ref_date"), "yyyy-MM-dd").isNull(),
        f"a base other than {base}": func.col("base") != base,
        "a value that is missing or not positive":
            func.col("value").isNull() | (func.col("value") <= 0),
    }
    found = []
    for problem, condition in problems.items():
        rows = internal.filter(func.coalesce(condition, func.lit(True)))
        count = rows.count()
        if count:
            examples = [tuple(row) for row in rows.limit(3).collect()]
            found.append(f"{count} rows with {problem}, e.g. {examples}")

    duplicated = internal.groupBy("ref_date", "base", "quote").count() \
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
    the result is saved in the silver layer. Where the BIS is used, the table
    ends on the last day the BIS reaches rather than on the last day of the
    source that is furthest ahead.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    rebase : str, optional
        ISO currency code of the currency that should become the new base currency instead of EUR.
        The code must be contained in the EZB reference rate data set.
    use_bis : bool
        Whether the BIS rates in the Bronze layer are combined in as well.
        A run that uses them also ends the table on the last day the BIS
        reaches, so that no day of the table is described without them. The
        BIS lags a few days behind the other sources, and the newest days it
        does not cover yet are picked up by the next run once it has.
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
        .unpivot(ids = "Date", values = None, variableColumnName = "quote", valueColumnName = "value") \
        .filter(func.col("value").isNotNull()) \
        .withColumn("base", func.lit("EUR")) \
        .withColumn("type", func.lit("ref")) \
        .withColumn("source", func.lit("ECB")) \
        .withColumnRenamed("Date", "ref_date") \
        .select("ref_date", "base", "quote", "value", "type", "source")

    ecb_max_date = ecb.select(func.max("ref_date")).first()[0]

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
        if ecb.filter(func.col("quote") == rebase).limit(1).count() == 0:
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
            .withColumn("quote", func.substring(func.col("CURRENCY:Currency"), 0, 3)) \
            .filter(func.col("quote") != "EUR") \
            .withColumn("base", func.lit("USD")) \
            .withColumn("source", func.lit("BIS")) \
            .withColumnRenamed("TIME_PERIOD:Time period or range", "ref_date") \
            .withColumnRenamed("OBS_VALUE:Observation Value", "value") \
            .filter(func.col("value").isNotNull()) \
            .withColumn("type", func.lit("avg")) \
            .select("ref_date", "base", "quote", "value", "type", "source")
        
        bis_max_date = bis.select(func.max("ref_date")).first()[0]

        # The table ends on the BIS's last day, so there has to be one. A
        # Bronze file without a single daily average -- a download that
        # brought an empty or a changed file -- would otherwise cut every row
        # away and replace the table with an empty one without a word.
        if bis_max_date is None:
            raise ValueError(
                "Cannot combine the BIS rates: the BIS data in the Bronze "
                "layer holds no daily average rate, so there is no last day "
                "to cut the table back to. The table was left as it was."
            )

    internal_file = os.path.join(path_bronze_internal_exchangerate(config_path), "exchangerate.csv")
    use_internal = os.path.exists(internal_file)

    if use_internal:
        internal = spark.read \
            .options(header='True', delimiter=',') \
            .schema(internal_schema) \
            .csv(internal_file)
        check_internal_rates(internal, internal_file)
        internal_max_date = internal.select(func.max("ref_date")).first()[0]
        if internal_max_date is None:
            logger.warning(
                "The internal exchange rates hold no rows, only the public "
                "sources are combined",
                extra=context(module="exchangerate", layer="silver", file=internal_file),
            )

    # How far the table reaches. Where the sources overlap the rank below
    # decides, and a day only some of them quote is described by those that
    # do, which is what happens on any day a source is silent. Without the
    # BIS every source counts up to its own last day: cutting them all back to
    # the day the last of them reaches would only throw the newest rates of
    # the others away. A run with the BIS ends on the BIS's last day instead,
    # see the cut further down. The last day of each source is logged, so that
    # a source that has fallen behind is visible.
    logger.info(
        "Combining the sources, the table ending on the BIS's last day" if use_bis
        else "Combining the sources, each up to its own last day",
        extra=context(
            module="exchangerate",
            layer="silver",
            ecb=ecb_max_date,
            bis=bis_max_date if use_bis else None,
            internal=internal_max_date if use_internal else None,
            cutoff=bis_max_date if use_bis else None,
        ),
    )

    # Convert BIS' USD-FX-rates into EUR-FX-rates
    if use_bis:
        eur_usd = ecb \
            .filter((func.col("base") == "EUR") & (func.col("quote") == "USD")) \
            .select("ref_date", "value") \
            .withColumnRenamed("value", "_fx")
        
        bis = bis \
            .join(eur_usd, on = "ref_date", how = "leftouter") \
            .filter(func.col("_fx").isNotNull()) \
            .withColumn("value", func.col("value") * func.col("_fx")) \
            .drop("_fx") \
            .withColumn("base", func.lit("EUR"))
    
    # Internal over ECB over BIS. The precedence is carried by an explicit rank
    # rather than by source, because the source values of the internal data set
    # are whatever the internal CSV happens to contain.
    # Within one rank the order runs on through the value and the labels, so
    # that two rows of one source for the same day and pair -- the BIS carries
    # a series per reference area, and areas share currencies -- are settled
    # the same way on every run rather than by the order Spark reads them in.
    window_spec = Window.partitionBy("ref_date", "base", "quote").orderBy(
        func.col("_source_rank"), func.col("value"), func.col("source"), func.col("type"))

    if use_internal:
        result = internal.withColumn("_source_rank", func.lit(1)) \
            .union(ecb.withColumn("_source_rank", func.lit(2)))
    else:
        result = ecb.withColumn("_source_rank", func.lit(2))

    if use_bis:
        result = result.union(bis.withColumn("_source_rank", func.lit(3)))
    
    result = result \
        .withColumn("_row_ctrpty", func.row_number().over(window_spec)) \
        .filter(func.col("_row_ctrpty") == 1) \
        .drop("_row_ctrpty", "_source_rank")
    
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
    # whatever the reason for it. A run lasts at most MAX_FILL_DAYS after its
    # quote, so a pair that goes unquoted for longer has no rate on the days
    # beyond.
    window_spec = Window.partitionBy("base", "quote").orderBy("ref_date")
    result = result \
        .withColumn("_next_date", func.lead("ref_date").over(window_spec)) \
        .withColumn("_next_date", func.date_sub(func.col("_next_date"), 1)) \
        .withColumn("_next_date", func.when(func.col("_next_date").isNull(), func.col("ref_date")).otherwise(func.col("_next_date"))) \
        .withColumn("_next_date", func.least(
            func.col("_next_date"), func.date_add(func.col("ref_date"), MAX_FILL_DAYS))) \
        .withColumn("_dates", func.sequence(func.col("ref_date"), func.col("_next_date"))) \
        .select(func.posexplode(func.col("_dates")).alias("_POS", "ref_date"),
                "base", "quote", "value", "type", "source") \
        .withColumn("imputed", func.col("_POS") > 0) \
        .drop("_POS")

    # A run that uses the BIS ends where the BIS ends. The BIS publishes with
    # a lag of a few days, and the days beyond it would be described by the
    # ECB and the proprietary source alone: a pair only the BIS quotes would
    # stop days before the rest, and the day a caller converts an amount on
    # would decide which currencies the table covers. Cutting the table back
    # to the BIS's last day keeps the whole of it on the same footing, at the
    # price of the few newest days of the other sources, which the next run
    # picks up once the BIS has caught up.
    #
    # The cut is made after the fill, so a day before it still carries the
    # last quote on or before it rather than losing its fill along with the
    # quote beyond the cut that opened the run it belongs to.
    if use_bis:
        result = result.filter(
            func.col("ref_date") <= func.to_date(func.lit(bis_max_date)))

    if rebase is not None:

        eur = result \
            .filter((func.col("base") == "EUR") & (func.col("quote") == rebase)) \
            .withColumn("quote", func.lit("EUR")) \
            .withColumn("base", func.lit(rebase)) \
            .withColumn("value", 1 / func.col("value"))
        
        eur_rebase = result \
            .filter((func.col("base") == "EUR") & (func.col("quote") == rebase)) \
            .select("ref_date", "value", "imputed") \
            .withColumnRenamed("value", "_fx") \
            .withColumnRenamed("imputed", "_fx_imputed")
        
        result = result \
            .filter(func.col("base") == "EUR") \
            .join(eur_rebase, on = "ref_date", how = "leftouter") \
            .filter(func.col("_fx").isNotNull()) \
            .withColumn("value", func.col("value") / func.col("_fx")) \
            .withColumn("imputed", func.col("imputed") | func.col("_fx_imputed")) \
            .drop("_fx", "_fx_imputed") \
            .withColumn("base", func.lit(rebase)) \
            .filter(func.col("base") != func.col("quote"))

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
        Whether the BIS rates are downloaded and combined in as well. A run
        that uses them ends the table on the last day the BIS reaches; see
        :func:`update_silver_exchangerate`.
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
