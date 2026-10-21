"""The watermark of the historical FIRDS database.

The pipeline has to know how far the historical database has been built, so
that :func:`fincommons.firds.silver.update_from_bronze_to_silver_main` applies
every delta file that is newer than that point and none that is older. Applying
a delta file twice would attribute the wrong validity periods to the versions,
so this bookkeeping is what keeps the history correct.

The watermark lives in a small Delta table, one row per table of the Silver
layer. It is always Delta, because it is updated in place and that is what a
transaction log is for, but how it is addressed follows the configuration
file: a directory on a workstation or in a Unity Catalog volume, or a table in
a catalogue where the metadata layer names one. Everything here goes through
:func:`fincommons.firds.common.ref_metadata` and the helpers of
:mod:`fincommons.addressing`, so neither form is written into this module.
Nothing here touches the local filesystem directly -- every read and write
goes through Spark -- so the same code path serves both.

Off-cluster the ``delta-spark`` package has to be installed (it comes with the
``local`` extra) and the session has to be built with the Delta extensions, for
instance with :func:`delta.configure_spark_with_delta_pip`. On Databricks Delta
is part of the runtime and no set-up is needed.
"""

import pyspark.sql.functions as func
import pyspark.sql.types as types

from ..addressing import delta_table, delta_table_exists, read_delta, write_delta
from . import common


#: The columns of the watermark table. ``MAXDATE`` is the reference date of the
#: newest file that has been applied to ``NAME``, spelled the way
#: :func:`fincommons.firds.silver.format_reference_date` spells it, and
#: ``UPDATED`` records when that last happened.
METADATA_SCHEMA = types.StructType(
    [
        types.StructField("NAME", types.StringType(), nullable=False),
        types.StructField("MAXDATE", types.StringType(), nullable=False),
        types.StructField("UPDATED", types.TimestampType(), nullable=True),
    ]
)


def metadata_exists(spark, config_path):
    """Return whether the watermark table has been created yet.

    The check goes through Delta rather than through the filesystem, so that it
    also answers correctly for a table that lives in object storage.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    """
    return delta_table_exists(spark, common.ref_metadata(config_path))


def initialise_metadata(spark, config_path, maxdate):
    """Create the watermark table and record ``maxdate`` for ``MAIN``.

    An existing table is overwritten, which resets the bookkeeping the way
    dropping and recreating the table did before. Call this only together with
    a rebuild of the historical database itself -- resetting the watermark
    without rebuilding would make the pipeline reapply delta files it has
    already applied.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    maxdate: str
        The reference date the historical database has been built up to.
    """
    _write_single_row(spark, config_path, table="MAIN", maxdate=maxdate)


def _write_single_row(spark, config_path, table, maxdate):
    """Create the watermark table from scratch, holding one row."""
    rows = spark.createDataFrame([(table, maxdate, None)], schema=METADATA_SCHEMA)
    rows = rows.withColumn("UPDATED", func.current_timestamp())

    write_delta(
        rows,
        common.ref_metadata(config_path),
        options={"overwriteSchema": "true"},
    )


def update_metadata_maxdate(spark, config_path, table, maxdate):
    """Record that ``table`` has been built up to ``maxdate``.

    The row is inserted if the table has no watermark yet and updated if it
    has, so the function is also the way to set the watermark by hand -- for
    a table built outside the pipeline, for instance. If the watermark table
    does not exist at all it is created.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    table: str
        Name of the table of the Silver layer, e.g. ``'MAIN'``.
    maxdate: str
        The reference date that table has been built up to.
    """
    ref = common.ref_metadata(config_path)

    if not metadata_exists(spark, config_path):
        _write_single_row(spark, config_path, table=table, maxdate=maxdate)
        return

    incoming = spark.createDataFrame(
        [(table, maxdate, None)], schema=METADATA_SCHEMA
    )

    delta_table(spark, ref).alias("tgt").merge(
        incoming.alias("src"), "tgt.NAME = src.NAME"
    ).whenMatchedUpdate(
        set={"MAXDATE": "src.MAXDATE", "UPDATED": "current_timestamp()"}
    ).whenNotMatchedInsert(
        values={
            "NAME": "src.NAME",
            "MAXDATE": "src.MAXDATE",
            "UPDATED": "current_timestamp()",
        }
    ).execute()


def get_metadata_maxdate(spark, config_path, table):
    """Return the reference date ``table`` has been built up to.

    Returns ``None`` when the watermark table does not exist yet or carries no
    row for ``table``, which is how a database that has not been initialised is
    told apart from one that has.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    table: str
        Name of the table of the Silver layer, e.g. ``'MAIN'``.
    """
    if not metadata_exists(spark, config_path):
        return None

    rows = (
        read_delta(spark, common.ref_metadata(config_path))
        .where(func.col("NAME") == table)
        .select("MAXDATE")
        .limit(1)
        .collect()
    )

    return rows[0]["MAXDATE"] if rows else None


def read_metadata(spark, config_path):
    """Return the whole watermark table, for inspection.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    """
    if not metadata_exists(spark, config_path):
        return spark.createDataFrame([], schema=METADATA_SCHEMA)

    return read_delta(spark, common.ref_metadata(config_path))


#: What this module offers. The watermark of the historical database.
__all__ = [
    "METADATA_SCHEMA",
    "get_metadata_maxdate",
    "initialise_metadata",
    "metadata_exists",
    "read_metadata",
    "update_metadata_maxdate",
]
