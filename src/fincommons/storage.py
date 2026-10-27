"""How a Silver table that is rebuilt in full is stored.

Several modules keep tables in the Silver layer that every run rebuilds from
the layer below in full, such as the combined exchange rate table and the
entity master data set; the table of the French Data Library is rebuilt a
partition at a time instead, see :func:`replace_silver_partitions`. Such a
table may be stored as parquet or as Delta, and
it may be addressed by path or by catalogue name, which is what
:mod:`fincommons.addressing` decides from the configuration file. This module
is the one place that turns those two facts into a read and a write, so that
the modules agree on what each combination guarantees.

The guarantees differ, and the functions here say which one a write gives:

* A Delta table, at a path or in a catalogue, is replaced in one
  transaction. A reader sees either the previous version in full or the new
  one, and the previous versions remain readable.
* A parquet table is rebuilt in full as well, but the overwrite deletes the
  table before it writes the new one, so there is a window in which a reader
  finds it missing or half-written. It is the format every engine reads
  without a further dependency, which is why it remains the default for a
  table addressed by path.
* A table addressed by catalogue name is always Delta, because that is what a
  Unity Catalog managed table is. Asking for parquet there is refused rather
  than quietly answered with Delta.

A table that already exists keeps its format. A write that asks for the other
format is refused before anything is written, because neither direction ends
well on its own: Delta will not adopt parquet files it has no log for, and a
parquet overwrite of a Delta table silently throws its history away. Remove
the table first to change its format.
"""

from __future__ import annotations

import contextlib
import os
import re

from .addressing import TableRef, delta_table, delta_table_exists, sql_table

SILVER_FORMATS = ("parquet", "delta")
"""The storage formats a Silver table may be written in."""

DEFAULT_SILVER_FORMAT = "parquet"
"""The format a new table addressed by path is written in where none is asked for.

Parquet is the default because it is the format every engine reads without a
further dependency, which is what a table meant to be shared should be. Delta
buys two things parquet does not have and is worth choosing where they matter:
the overwrite replaces the table in one transaction rather than deleting it
first, so a concurrent reader never finds it missing or half-written, and the
previous versions of the table remain readable.
"""

OPTIMIZE_WRITE = "delta.autoOptimize.optimizeWrite"
"""The table property that has Delta size the files of every write.

A table rebuilt in full from a frame whose partitions follow the joins that
made it would otherwise be stored as many small files, which every later read
has to open.
"""


def _as_ref(ref) -> TableRef:
    """Accept a :class:`~fincommons.addressing.TableRef` or a plain path.

    The modules addressed their Silver tables by path before they could be
    addressed by name, and their public helpers took the path as a string.
    Those helpers now delegate here, so a string is still taken to be a path.
    """
    if isinstance(ref, TableRef):
        return ref
    if isinstance(ref, (str, os.PathLike)):
        return TableRef(path=os.fspath(ref))
    raise TypeError(
        f"a table is addressed by a TableRef or a path, got {type(ref).__name__}"
    )


def check_file_format(file_format: str) -> str:
    """Reject a storage format that is not one of :data:`SILVER_FORMATS`.

    Parameters
    ----------
    file_format:
        One of :data:`SILVER_FORMATS`.
    """
    if file_format not in SILVER_FORMATS:
        raise ValueError(
            f"File format {file_format!r} not recognised. Use "
            + " or ".join(repr(f) for f in SILVER_FORMATS)
            + "."
        )
    return file_format


_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]+:")
"""A URI scheme such as ``abfss:`` or ``dbfs:``.

Two characters at least, so that a Windows drive letter is not taken for one.
"""


def check_local_path(path: str) -> str:
    """Refuse a path that Python's file APIs cannot look into.

    The format of a table is read off the file system with :mod:`os`, which
    finds nothing at ``abfss://...``, ``s3://...`` or ``dbfs:/...``. A table
    there would look absent, so a parquet overwrite would be let through over
    a Delta table and throw its history away. Such a path is refused instead;
    a Unity Catalog volume (``/Volumes/...``) is the location that works.
    """
    if _URI_SCHEME.match(path):
        raise ValueError(
            f"The table path {path!r} is a URI, which the format of a table "
            f"cannot be read from. Point paths.yaml at a Unity Catalog volume "
            f"(/Volumes/...) or a local directory instead."
        )
    return path


def _holds_table(path: str) -> bool:
    """Whether there is anything at ``path`` that a write would replace.

    An empty directory is not a table: it is what setting up the directory
    tree of a local data lake leaves behind, and neither format can be read
    out of it.
    """
    check_local_path(path)
    return os.path.isdir(path) and bool(os.listdir(path))


def file_format_on_disk(path: str, default: str = DEFAULT_SILVER_FORMAT) -> str:
    """The format the table at ``path`` is stored in.

    A Delta table keeps its transaction log in a ``_delta_log`` directory
    beside the parquet files the table is made of, and that directory is what
    tells the two formats apart. The format is read off the table rather than
    asked of the caller because reading a Delta table as plain parquet does not
    fail: it returns the superseded files of earlier versions along with the
    current ones, and so quietly answers with rows that are no longer in the
    table.

    Where there is no table yet, ``default`` is returned. A directory that
    holds files but no log is a parquet table, whatever the default.

    Parameters
    ----------
    path:
        Path of the table.
    default:
        The format assumed where the path holds no table.
    """
    check_local_path(path)
    if os.path.isdir(os.path.join(path, "_delta_log")):
        return "delta"
    if _holds_table(path):
        return "parquet"
    return default


def silver_format(ref, file_format: str | None = None) -> str:
    """The format a write to ``ref`` uses, or why it may not be written.

    A table addressed by catalogue name is a Delta table, because that is what
    a Unity Catalog managed table is. There is no choice to make there, and an
    explicit ``file_format`` may only confirm it. This is what lets a change
    of the configuration file alone move a table into the catalogue: a caller
    that leaves ``file_format`` out is given Delta there and parquet at a path.

    A table addressed by path keeps the format it is stored in. Where it does
    not exist yet, it takes the format asked for, or
    :data:`DEFAULT_SILVER_FORMAT` where none is.

    The answer is reached from the configuration and the file system alone,
    without a Spark session, so an update can check it before it starts work.

    Parameters
    ----------
    ref: fincommons.addressing.TableRef or str
        Where the table lives. A string is a path.
    file_format: str, optional
        One of :data:`SILVER_FORMATS`.

    Raises
    ------
    ValueError
        If the format is not one of :data:`SILVER_FORMATS`, if parquet is
        asked of a table addressed by catalogue name, or if the format asked
        for is not the one the table at the path is stored in.
    """
    ref = _as_ref(ref)
    if file_format is not None:
        check_file_format(file_format)

    if ref.is_named:
        if file_format not in (None, "delta"):
            raise ValueError(
                f"A table addressed by catalogue name is a Delta table, so "
                f"{ref.name} cannot be written as {file_format!r}. Address the "
                f"Silver layer of the configuration file by path to write parquet, "
                f"or leave the file format out."
            )
        return "delta"

    on_disk = file_format_on_disk(ref.path, default=None)
    if on_disk is None:
        return file_format or DEFAULT_SILVER_FORMAT
    if file_format is not None and file_format != on_disk:
        raise ValueError(
            f"The table at {ref.path} is stored as {on_disk!r} and cannot be "
            f"overwritten as {file_format!r}. A table keeps its format; remove "
            f"the directory first to change it, or leave the file format out."
        )
    return on_disk


def silver_table_exists(spark, ref) -> bool:
    """Whether a Silver table stands at ``ref``, in either format.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session. It is only asked where the table is addressed
        by catalogue name.
    ref: fincommons.addressing.TableRef or str
        Where the table would live. A string is a path.
    """
    ref = _as_ref(ref)
    if ref.is_named:
        return spark.catalog.tableExists(ref.name)
    return _holds_table(ref.path)


def read_silver(spark, ref):
    """Read a Silver table in whichever form it is addressed and stored.

    A table addressed by path is read in the format it is stored in, which is
    read off the table rather than asked of the caller; see
    :func:`file_format_on_disk`. A table that does not exist is reported as
    such rather than left to Spark, whose answer -- that the path does not
    exist, or that no schema can be inferred from an empty directory -- is
    true but silent about the fact that it is the update of the Silver layer
    that would create it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    ref: fincommons.addressing.TableRef or str
        Where the table lives. A string is a path.

    Raises
    ------
    FileNotFoundError
        If there is no table at the path.
    LookupError
        If there is no table of the name in the catalogue.
    """
    ref = _as_ref(ref)
    if ref.is_named:
        if not spark.catalog.tableExists(ref.name):
            raise LookupError(
                f"No table {ref.name!r} in the catalogue. The Silver layer has "
                "to be updated before the table can be read."
            )
        return spark.read.table(ref.name)
    if not _holds_table(ref.path):
        raise FileNotFoundError(
            f"No table at {ref.path!r}. The Silver layer has to be updated "
            "before the table can be read."
        )
    return spark.read.format(file_format_on_disk(ref.path)).load(ref.path)


@contextlib.contextmanager
def _static_partition_overwrite(spark):
    """Force a full overwrite on the session for the duration of a write.

    Delta takes ``partitionOverwriteMode`` as an option on the write itself,
    but the parquet source reads it from the session alone, so for parquet the
    setting has to be made on the session and put back afterwards. Without it
    a session configured for dynamic partition overwrite, as some pipelines
    configure theirs, would replace only the partitions the new frame carries
    and keep the rest of the old table beside them.
    """
    key = "spark.sql.sources.partitionOverwriteMode"
    previous = spark.conf.get(key)
    spark.conf.set(key, "static")
    try:
        yield
    finally:
        spark.conf.set(key, previous)


def _ensure_optimize_write(spark, ref: TableRef) -> None:
    """Set :data:`OPTIMIZE_WRITE` on a Delta table that does not carry it yet.

    The property is set by a statement of its own rather than as an option of
    the write, because whether a writer option reaches the table properties
    depends on the version of Delta and on whether the table is created,
    overwritten or replaced in the catalogue. The statement is only issued
    where the property is missing, so a table that carries it is not given a
    further version on every run. It changes the metadata of the table and
    none of its rows.
    """
    properties = delta_table(spark, ref).detail().first()["properties"] or {}
    if str(properties.get(OPTIMIZE_WRITE, "")).lower() != "true":
        spark.sql(
            f"ALTER TABLE {sql_table(ref)} SET TBLPROPERTIES "
            f"('{OPTIMIZE_WRITE}' = 'true')"
        )


def write_silver(df, ref, file_format: str | None = None, partition_by=()):
    """Replace a Silver table with a data frame.

    The table is rebuilt in full rather than merged into or appended to, and
    which guarantee comes with that depends on the format:

    * Delta, at a path or in a catalogue, replaces the table in one
      transaction. A reader sees either the previous version in full or the
      new one, and the previous versions remain readable. A table addressed
      by catalogue name stays the same table throughout, so what is granted
      on it survives the write. The schema is replaced with the frame's, the
      overwrite is a full one whatever the session says about partitions, and
      the table carries :data:`OPTIMIZE_WRITE`.
    * Parquet deletes the table before it writes the new one, so there is a
      window in which a reader finds it missing or half-written, and nothing
      of the previous table remains. The overwrite is a full one whatever the
      session says about partitions.

    The format is decided by :func:`silver_format`, before anything is
    written: a table addressed by catalogue name is Delta, and a table at a
    path keeps the format it is stored in.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        The data frame to be written.
    ref: fincommons.addressing.TableRef or str
        Where the table lives. A string is a path.
    file_format: str, optional
        One of :data:`SILVER_FORMATS`. Where it is left out the addressing and
        the table on disk decide; see :func:`silver_format`.
    partition_by:
        Columns the table is partitioned by. Columns that are not present in
        the data frame are ignored.

    Returns
    -------
    fincommons.addressing.TableRef or str
        The table that was written to, in the form it was given.

    Raises
    ------
    ValueError
        If the format asked for conflicts with the addressing or with the
        table on disk; see :func:`silver_format`.
    """
    given = ref
    ref = _as_ref(ref)
    file_format = silver_format(ref, file_format)
    spark = df.sparkSession

    columns = [c for c in partition_by if c in df.columns]
    writer = df.write.format(file_format).mode("overwrite")
    if columns:
        writer = writer.partitionBy(*columns)

    if file_format == "delta":
        # Delta keeps the schema of the table across an overwrite unless it
        # is told otherwise. The partition overwrite mode is stated on the
        # write rather than left to the session, because a session configured
        # for dynamic partition overwrite has Delta refuse a write that may
        # change the schema.
        writer = writer.option("overwriteSchema", "true") \
            .option("partitionOverwriteMode", "static")
        # A table that exists already is given the property before the
        # overwrite, so that the overwrite itself is sized by it.
        if delta_table_exists(spark, ref):
            _ensure_optimize_write(spark, ref)
        if ref.is_named:
            writer.saveAsTable(ref.name)
        else:
            writer.save(ref.path)
        # A table the write created, or one the catalogue replaced, has the
        # property only once it is set afterwards.
        _ensure_optimize_write(spark, ref)
    else:
        # The parquet source takes the overwrite mode from the session rather
        # than from the write, so the full overwrite is arranged there.
        with _static_partition_overwrite(spark):
            writer.save(ref.path)
    return given


@contextlib.contextmanager
def _dynamic_partition_overwrite(spark):
    """Have an overwrite replace only the partitions the frame carries.

    The counterpart of :func:`_static_partition_overwrite`, for the parquet
    source, which reads the mode from the session alone.
    """
    key = "spark.sql.sources.partitionOverwriteMode"
    previous = spark.conf.get(key)
    spark.conf.set(key, "dynamic")
    try:
        yield
    finally:
        spark.conf.set(key, previous)


def _sql_literal(value) -> str:
    """A value as a SQL string literal, its quotes and backslashes escaped."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def replace_silver_partitions(df, ref, column: str, values,
                              file_format: str | None = None):
    """Replace some partitions of a Silver table and leave the others.

    A table that is rebuilt piece by piece rather than in full -- one file of
    a library at a time, say -- is partitioned by the column that names the
    piece, and each write replaces the partitions of the pieces it carries.
    ``df`` must hold rows of those partitions only, and of every one of them:
    a partition named in ``values`` that ``df`` has no row for is emptied as
    Delta and left as it was as parquet, so a caller that has nothing to
    write for a piece leaves it out of ``values``.

    The guarantee depends on the format, as for :func:`write_silver`:

    * Delta, at a path or in a catalogue, replaces the partitions in one
      transaction with ``replaceWhere``. A reader sees the table before the
      write or after it, and the previous versions remain readable.
    * Parquet rewrites the partitions in place with a dynamic partition
      overwrite. A reader of one of them during the write may find it missing
      or half-written; the other partitions are not touched.

    A table that does not exist yet is created, partitioned by ``column``.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        The rows of the partitions to be replaced.
    ref: fincommons.addressing.TableRef or str
        Where the table lives. A string is a path.
    column: str
        The partition column.
    values: list
        The values of ``column`` whose partitions are replaced.
    file_format: str, optional
        One of :data:`SILVER_FORMATS`; see :func:`silver_format`.

    Returns
    -------
    fincommons.addressing.TableRef or str
        The table that was written to, in the form it was given.
    """
    given = ref
    ref = _as_ref(ref)
    file_format = silver_format(ref, file_format)
    spark = df.sparkSession
    values = list(values)
    if not values:
        return given

    writer = df.write.format(file_format).mode("overwrite").partitionBy(column)
    if file_format == "delta":
        exists = delta_table_exists(spark, ref)
        if exists:
            _ensure_optimize_write(spark, ref)
            predicate = f"`{column}` IN (" + ", ".join(
                _sql_literal(v) for v in values) + ")"
            writer = writer.option("replaceWhere", predicate)
        if ref.is_named:
            writer.saveAsTable(ref.name)
        else:
            writer.save(ref.path)
        if not exists:
            _ensure_optimize_write(spark, ref)
    else:
        with _dynamic_partition_overwrite(spark):
            writer.save(ref.path)
    return given


__all__ = [
    "DEFAULT_SILVER_FORMAT",
    "OPTIMIZE_WRITE",
    "SILVER_FORMATS",
    "check_file_format",
    "check_local_path",
    "file_format_on_disk",
    "read_silver",
    "replace_silver_partitions",
    "silver_format",
    "silver_table_exists",
    "write_silver",
]
