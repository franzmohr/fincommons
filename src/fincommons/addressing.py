"""Where the tables of the data lake live.

Every data set in the package reads the layout of its data lake from a
configuration file, so that the same code runs against a directory tree on a
workstation and against Databricks. This module holds the part that is common
to all of them: reading the file, turning an entry in it into an address, and
reading and writing a Delta table at whichever kind of address came back.

Two kinds of address occur. A layer that holds files, which is everything up
to and including Bronze, is addressed by path, and its entry in the
configuration file is the directory it lives in. A layer that holds Delta
tables may instead be addressed by catalogue name, and its entry carries the
prefix that the tables of the layer hang off. Names are what Unity Catalog
governs: grants, row filters, lineage and Catalog Explorer all key off the
identifier, and a table addressed only by path is invisible to them. Paths
remain the right address for the file layers, where there is no table to name,
and for a local data lake that has no catalogue at all.

Which of the two a layer uses is decided by the configuration file alone,
entry by entry. A layer given as a string is a directory, so a configuration
file that names only directories describes a lake addressed entirely by path.
A layer given as a mapping may carry ``path``, ``table`` or both, and
carrying ``table`` is what moves it into the catalogue::

    exchangerate:
        bronze: /Volumes/fincommons/exchangerate/lake/bronze
        silver:
            table: fincommons.exchangerate

There is deliberately no setting that says which environment the pipeline runs
in. A configuration file describes one data lake and the pipeline believes
what it says, so a file that names a catalogue is a file meant for a place
where that catalogue exists. The prefix is written out in full rather than
assembled here, because how many parts it has depends on where it is used:
Unity Catalog takes ``catalog.schema``, while the session catalogue of a
plain Spark installation takes the schema on its own.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

import yaml


@dataclass(frozen=True)
class TableRef:
    """The address of one table, in whichever form its layer is addressed.

    Attributes
    ----------
    path:
        The directory the table lives in, or ``None`` where the layer is
        addressed by catalogue name and the configuration file gives no
        directory beside it.
    name:
        The qualified name the table is addressed by, or ``None`` where the
        layer is addressed by path.
    """

    path: str | None = None
    name: str | None = None

    def __post_init__(self):
        if self.path is None and self.name is None:
            raise ValueError("a table reference has to carry a path or a name")

    @property
    def is_named(self) -> bool:
        """Whether the table is addressed by name rather than by path.

        Where a layer carries both, the name wins. A configuration file that
        gives both is describing a catalogue table whose files it also wants
        to name, not a choice to be made at run time.
        """
        return self.name is not None

    def __str__(self) -> str:
        """The address, as it would be written in a log line."""
        return self.name if self.name is not None else self.path


@lru_cache(maxsize=None)
def _parse(config_path: str, stamp: tuple[int, int]) -> dict:
    """Parse a configuration file. The stamp is explained in :func:`load_paths`.

    The encoding is stated rather than left to the platform, because the
    default differs between a Windows workstation and the Linux image the
    pipelines run in, and a path with a non-ASCII character would then be
    read differently in the two places.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_paths(config_path: str = "paths.yaml") -> dict:
    """Read the configuration file.

    The accessors of every data set call this once for each address they hand
    out, and setting a data lake up asks for a dozen addresses in a row, so
    the parsed file is kept. The cache is keyed by the modification time and
    the size of the file as well as by its name, so that a file rewritten in
    place, which is what a test that varies the configuration does, is read
    again rather than served from the cache.

    The dictionary that comes back is shared between all callers and must be
    treated as read only.

    Parameters
    ----------
    config_path:
        Path to the configuration file. This must be a yaml-file.
    """
    stat = os.stat(config_path)
    return _parse(config_path, (stat.st_mtime_ns, stat.st_size))


def identifier(name: str) -> str:
    """Turn the name of a table into a legal unquoted SQL identifier.

    The tables of the data lake are named for what they hold, and some of
    those names are legal directories but not legal identifiers: Spark
    rejects ``balance-sheet`` outright and asks for it back quoted. Rather
    than carry a second set of names in the configuration file, the hyphen is
    translated here, so that a table is named once, in the code that already
    names it, and keeps its hyphen where it is a directory.
    """
    return name.replace("-", "_")


def resolve(
    config_path: str, dataset: str, layer: str, leaf: str | None = None
) -> TableRef:
    """Address one table of one layer, or the layer itself.

    Parameters
    ----------
    config_path:
        Path to the configuration file. This must be a yaml-file.
    dataset:
        The data set, which is a top level key of the configuration file,
        such as ``"exchangerate"``.
    layer:
        The layer within it, such as ``"bronze"`` or ``"silver"``.
    leaf:
        The table within the layer, such as ``"main"``. Where it is left out
        the layer itself is addressed, which is what creating the directory
        tree of a local data lake needs.

    Returns
    -------
    TableRef
        Where the table lives.

    Notes
    -----
    The leaf is joined onto the directory and onto the catalogue prefix alike,
    so that the same call gives ``.../silver/main`` against a local data lake
    and ``fincommons.firds.main`` against a catalogue.
    """
    config = load_paths(config_path)

    if not isinstance(config, dict) or dataset not in config:
        raise KeyError(
            f"the configuration file {config_path!r} describes no data set "
            f"{dataset!r}"
        )
    layers = config[dataset]
    if not isinstance(layers, dict) or layer not in layers:
        raise KeyError(
            f"the configuration file {config_path!r} gives no {layer!r} layer "
            f"for {dataset!r}"
        )
    entry = layers[layer]

    if isinstance(entry, str):
        entry = {"path": entry}
    elif not isinstance(entry, dict):
        raise TypeError(
            f"the {layer!r} layer of {dataset!r} in {config_path!r} has to be a "
            f"directory or a mapping, got {type(entry).__name__}"
        )

    path = entry.get("path")
    name = entry.get("table")
    if path is None and name is None:
        raise ValueError(
            f"the {layer!r} layer of {dataset!r} in {config_path!r} gives "
            "neither a path nor a table name"
        )

    if leaf is not None:
        if path is not None:
            path = os.path.join(path, leaf)
        if name is not None:
            name = f"{name}.{identifier(leaf)}"

    return TableRef(path=path, name=name)


def read_delta(spark, ref: TableRef):
    """Open the Delta table a reference points at.

    Parameters
    ----------
    spark:
        An open Spark session.
    ref:
        Where the table lives, from :func:`resolve`.
    """
    if ref.is_named:
        return spark.read.table(ref.name)
    return spark.read.format("delta").load(ref.path)


def read_delta_version(spark, ref: TableRef, version: int):
    """Open one version of the Delta table a reference points at.

    A plan made from the table has to be made from a fixed version of it, so
    that a step repeated after a partial run reads the table as it stood
    before that run touched it rather than as it left it.

    Parameters
    ----------
    spark:
        An open Spark session.
    ref:
        Where the table lives, from :func:`resolve`.
    version:
        The version of the table to read.
    """
    reader = spark.read.format("delta").option("versionAsOf", version)
    if ref.is_named:
        return reader.table(ref.name)
    return reader.load(ref.path)


def delta_table_exists(spark, ref: TableRef) -> bool:
    """Whether a Delta table stands at a reference.

    Asked directly rather than inferred from a read failing: catching every
    exception would treat a permission problem or an unreadable transaction
    log as an absent table, and the caller's next step is usually to build one
    from scratch over whatever is there.

    Parameters
    ----------
    spark:
        An open Spark session.
    ref:
        Where the table would live, from :func:`resolve`.
    """
    from delta.tables import DeltaTable

    if ref.is_named:
        return spark.catalog.tableExists(ref.name)
    return DeltaTable.isDeltaTable(spark, ref.path)


def delta_table(spark, ref: TableRef):
    """The :class:`delta.tables.DeltaTable` a reference points at.

    This is what the update and merge operations are written against, and it
    is the one place that has to know whether Delta is asked for the table by
    name or by path.

    Parameters
    ----------
    spark:
        An open Spark session.
    ref:
        Where the table lives, from :func:`resolve`.
    """
    from delta.tables import DeltaTable

    if ref.is_named:
        return DeltaTable.forName(spark, ref.name)
    return DeltaTable.forPath(spark, ref.path)


def sql_table(ref: TableRef) -> str:
    """How a reference is written in a SQL statement.

    A table addressed by catalogue name is named as it stands. One addressed
    by path is reached through Delta's path syntax, which is what ``ALTER
    TABLE`` and ``DESCRIBE HISTORY`` need where there is no name to give.

    Parameters
    ----------
    ref:
        Where the table lives, from :func:`resolve`.
    """
    if ref.is_named:
        return ref.name
    return f"delta.`{ref.path}`"


def write_delta(
    df,
    ref: TableRef,
    mode: str = "overwrite",
    partition_by=(),
    options: dict | None = None,
) -> TableRef:
    """Write a data frame into the Delta table a reference points at.

    Parameters
    ----------
    df:
        The data frame to be written.
    ref:
        Where to write it, from :func:`resolve`.
    mode:
        The write mode, as Spark understands it. The default replaces the
        table in one transaction, so that a reader either sees the previous
        version in full or the new one, and never the half-written files an
        overwrite in place would leave behind.
    partition_by:
        Columns the table is partitioned by. Columns that are not present in
        the data frame are ignored, so that the same call serves a table that
        carries the partition column and one that does not.
    options:
        Writer options, such as ``{"overwriteSchema": "true"}``. They are
        stated by the caller rather than here, because which of them a table
        wants depends on how that table is rebuilt.

    Returns
    -------
    TableRef
        The table that was written to, so that the caller can report it.
    """
    writer = df.write.format("delta").mode(mode)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)

    columns = [c for c in partition_by if c in df.columns]
    if columns:
        writer = writer.partitionBy(*columns)

    if ref.is_named:
        writer.saveAsTable(ref.name)
    else:
        writer.save(ref.path)
    return ref


def rename_folders_to_snake_case(*roots: str) -> list[tuple[str, str]]:
    """Rename the hyphenated folders below ``roots`` to snake_case.

    Folder names travel into a SQL catalogue, where a hyphen has to be quoted,
    so every folder of the lake is spelled in snake_case and the modules read
    their tables under those names only. A folder spelled with hyphens is
    renamed to the snake_case name the module reads.

    Only directories are renamed, and only those named by the pipeline, which
    begin with a letter. A partition directory, ``DATADT=2024-06-03``, is a
    name Spark writes and reads back, and its value keeps its hyphens; the walk
    does not descend into one, nor into a Delta log. A folder named by a date,
    ``2024-06-03``, is not a name the pipeline chose either, and is left as it
    is. A folder whose
    new name is taken already is not merged into it: both are left where they
    are and the conflict is raised, because which of the two holds the data
    to keep is not something to guess.

    Meant for a local data lake. Storage provisioned outside the pipeline is
    renamed the way it was provisioned.

    Parameters
    ----------
    *roots:
        The layer directories to walk, as the configuration file names them.
        A root that does not exist is skipped.

    Returns
    -------
    list of tuple
        The folders renamed, as pairs of old and new path.

    Raises
    ------
    FileExistsError
        If a folder and its snake_case name both exist.
    """
    renamed = []
    for root in roots:
        if not os.path.isdir(root):
            continue
        for current, directories, _ in os.walk(root, topdown=True):
            keep = []
            for name in directories:
                if "=" in name or name == "_delta_log":
                    continue
                if "-" in name and name[0].isalpha():
                    old = os.path.join(current, name)
                    name = name.replace("-", "_")
                    new = os.path.join(current, name)
                    if os.path.exists(new):
                        raise FileExistsError(
                            f"Both {old} and {new} exist, so the first was not "
                            f"renamed; decide which holds the data to keep and "
                            f"remove the other."
                        )
                    os.rename(old, new)
                    renamed.append((old, new))
                keep.append(name)
            # Descend into the folders under their new names only.
            directories[:] = keep
    return renamed


def rename_files(pairs) -> list[tuple[str, str]]:
    """Rename files to the names a module writes and reads them under.

    A file is renamed only where the name it is renamed from is there and
    the one it is renamed to is not; one that is not there is skipped, and
    one whose new name is taken already is left where it is and the conflict
    raised, as for the folders of :func:`rename_folders_to_snake_case`. Run
    that first, since the files are named by where they live after it.

    Parameters
    ----------
    pairs:
        The old and the new path of each file.

    Returns
    -------
    list of tuple
        The files renamed, as pairs of old and new path.

    Raises
    ------
    FileExistsError
        If a file and its new name both exist.
    """
    renamed = []
    for old, new in pairs:
        if not os.path.isfile(old):
            continue
        if os.path.exists(new):
            raise FileExistsError(
                f"Both {old} and {new} exist, so the first was not renamed; "
                f"decide which holds the data to keep and remove the other."
            )
        os.rename(old, new)
        renamed.append((old, new))
    return renamed


__all__ = [
    "TableRef",
    "delta_table",
    "delta_table_exists",
    "identifier",
    "load_paths",
    "read_delta",
    "read_delta_version",
    "rename_files",
    "rename_folders_to_snake_case",
    "resolve",
    "sql_table",
    "write_delta",
]
