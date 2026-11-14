"""Data quality: the checks a data set is held to, and the issues they find.

A pipeline brings a data set in as its publisher gave it out, and the Silver
tables say what was published rather than what is true. Whether what was
published is any good is a separate question, and this module is the shape of
the answer: every data set that is checked writes one table of issues, one row
per issue found, beside the tables it was found in.

An issue table rather than a filter is the point. A check that quietly dropped
a bad row would leave a reader with a table that looks clean and a population
that is not the published one; a check that only logged would leave nothing to
count. A row in the issue table says which record, which check, and on what
day the check was made, which is what lets the same question be asked of a
figure before it is used, of an issuer over several years, and of the
publisher over several deliveries.

What is shared and what is not
------------------------------

The machinery is here and the checks are not. A check knows the data set it is
made of -- which concept a balance sheet totals under, which column holds the
reporting party -- and belongs beside that data set:
:mod:`fincommons.esef.quality` and :mod:`fincommons.secfiling.quality` each
hold their own, and each declares its own issue table, because the record a
check points at is identified differently in each. What is here is everything
that does not depend on the data set: how a check is described, how its rows
are gathered, which partitions still need checking, and how the issues are
counted once they are written.

The description of a check is data
---------------------------------

Every check is a row of a catalogue shipped with the package, in the data
directory beside the column catalogue and the measures of
:mod:`fincommons.financialreport`. The code a row defines is what the issue
table stores; the row says what the check means, how severe it is, and what to
do about a record it names. That is deliberate: a reader of an issue table has
a code and needs the sentence, a person deciding what to fix needs the
severity, and neither should have to read the source to get them. A deployment
that keeps checks of its own ships a catalogue of the same shape in its own
package and names it, as :func:`check_catalogue` describes.

A check is not a judgement about an issuer. It is a judgement about a filing:
that two figures of one report do not add up, that a tag is missing, that a
figure was restated. Which of those matters is the reader's question, and the
severity in the catalogue is this package's opinion rather than anybody's
rule.
"""

import logging
from dataclasses import dataclass
from importlib import resources

import pandas as pd

logger = logging.getLogger(__name__)

#: How severe an issue is. ``error`` is a record that cannot be right --
#: a balance sheet that does not balance, a period that ends before it
#: begins. ``warning`` is a record that may be right and is worth a look --
#: a figure far from its neighbours, a restatement. ``info`` is a record
#: that is only worth counting -- a report that tags most of its lines with
#: concepts of its own is not wrong, it is just hard to compare.
SEVERITIES = ("error", "warning", "info")

#: The file the catalogue of checks is shipped as, in the data directory of
#: a package.
CHECK_CATALOGUE_FILE = "dataquality_checks.csv"

#: The columns of the catalogue.
#:
#: ``dataset``
#:     the module the check belongs to, as the column catalogue names it,
#: ``code``
#:     what the issue table stores, e.g. ``IMBAL_BALANCE_SHEET``,
#: ``severity``
#:     one of :data:`SEVERITIES`,
#: ``title``
#:     the check in a few words,
#: ``meaning``
#:     what a row carrying the code says about the record it names,
#: ``remedy``
#:     what somebody holding the record can do about it, which for published
#:     data is usually to read the filing rather than to change anything.
CHECK_CATALOGUE_COLUMNS = ("dataset", "code", "severity", "title", "meaning",
                           "remedy")

#: The column every issue table carries the code in.
ISSUE_COLUMN = "ISSUE"

#: The column every issue table carries the day the check was made in. The
#: issues of a partition are replaced as a whole when it is checked again, so
#: this says when the rows that are there were found and not when the problem
#: arose.
CHECKED_COLUMN = "CHECKED"


def _root(package):
    """The importable name of ``package``; this one as it was imported."""
    return __package__ if package == "fincommons" else package


def check_catalogue(dataset=None, severity=None, packages=("fincommons",)):
    """The catalogue of checks, or the part of it that is asked for.

    Parameters
    ----------
    dataset: str, optional
        Only the checks of this module, e.g. ``'esef'``.
    severity: str, optional
        Only the checks of this severity; one of :data:`SEVERITIES`.
    packages: iterable of str
        The packages whose catalogues are read. A deployment that keeps
        checks of its own ships a catalogue of the same shape in its data
        directory and names it here, and its rows are read beside these.

    Returns
    -------
    pandas.DataFrame
        The rows, in the order dataset, severity, code.
    """
    parts = []
    for package in packages:
        path = resources.files(_root(package)).joinpath(
            "data", CHECK_CATALOGUE_FILE)
        if not path.is_file():
            raise FileNotFoundError(
                f"{package} ships no {CHECK_CATALOGUE_FILE}. A package that "
                "keeps its own checks puts one in its data directory, with "
                f"the columns {', '.join(CHECK_CATALOGUE_COLUMNS)}."
            )
        with path.open(encoding="utf-8") as handle:
            parts.append(pd.read_csv(handle, dtype=str, keep_default_na=False))

    catalogue = pd.concat(parts, ignore_index=True)
    missing = [name for name in CHECK_CATALOGUE_COLUMNS
               if name not in catalogue.columns]
    if missing:
        raise ValueError(
            f"The check catalogue is missing the columns {', '.join(missing)}")

    unknown = sorted(set(catalogue["severity"]) - set(SEVERITIES))
    if unknown:
        raise ValueError(
            f"The check catalogue gives the severity {unknown[0]!r}, which is "
            f"none of {', '.join(SEVERITIES)}")

    if dataset is not None:
        catalogue = catalogue[catalogue["dataset"] == str(dataset).strip().lower()]
    if severity is not None:
        catalogue = catalogue[catalogue["severity"] == str(severity).strip().lower()]

    order = {name: i for i, name in enumerate(SEVERITIES)}
    catalogue = catalogue.assign(_rank=catalogue["severity"].map(order))
    return (catalogue.sort_values(["dataset", "_rank", "code"], kind="stable")
            .drop(columns="_rank").reset_index(drop=True))


@dataclass(frozen=True)
class CheckDescription:
    """What one check looks for, and what a row of its code says."""

    dataset: str
    code: str
    severity: str
    title: str
    meaning: str
    remedy: str

    def __str__(self) -> str:
        lines = [f"{self.code} ({self.severity}) -- {self.title}",
                 f"  data set  {self.dataset}",
                 f"  means     {self.meaning}"]
        if self.remedy:
            lines.append(f"  remedy    {self.remedy}")
        return "\n".join(lines)


def describe_check(code, dataset=None, packages=("fincommons",)):
    """Describe one check, by the code its issues carry.

    Two data sets may define the same code -- a balance sheet fails to
    balance under either framework -- and they are two checks, written
    against different concepts and meaning the same thing. Where a code is
    defined more than once, ``dataset`` says which is wanted; leaving it out
    asks rather than picking one, since the sentence the two give differs.

    Raises
    ------
    KeyError
        Where no catalogue defines a check of that code.
    ValueError
        Where several do and ``dataset`` does not say which is meant.
    """
    wanted = str(code).strip().upper()
    rows = check_catalogue(dataset=dataset, packages=packages)
    rows = rows[rows["code"] == wanted]
    if rows.empty:
        known = sorted(check_catalogue(packages=packages)["code"].unique())
        raise KeyError(f"{wanted!r} is no check of the catalogue. Defined: "
                       + ", ".join(known))
    if len(rows) > 1:
        raise ValueError(
            f"{wanted!r} is defined for " + ", ".join(sorted(rows["dataset"]))
            + "; say which with describe_check(code, dataset=...)")
    row = rows.iloc[0]
    return CheckDescription(row.dataset, row.code, row.severity, row.title,
                            row.meaning, row.remedy)


def checks_of(dataset, packages=("fincommons",)):
    """The codes the catalogue defines for one data set, severest first."""
    return list(check_catalogue(dataset=dataset, packages=packages)["code"])


def run_checks(frame, checks, keys, catalogue_dataset=None,
               packages=("fincommons",)):
    """Gather the rows every check finds into one frame of issues.

    Each check is a ``(code, function)`` pair. The function takes the frame
    and returns the rows it objects to, carrying at least ``keys``; this adds
    the code and keeps nothing else, so that a check cannot widen the issue
    table by returning columns of its own, and so that two checks objecting to
    the same record give two rows rather than one row with two codes.

    A check that returns no rows contributes nothing, which is the ordinary
    case and is not reported: an issue table holds what is wrong, and the
    absence of a code for a record is what says the record passed.

    Parameters
    ----------
    frame: pyspark.sql.DataFrame
        What the checks are run over, usually one partition of a Silver table.
    checks: iterable of (str, callable)
        The checks to run, in the order they are to be run.
    keys: iterable of str
        The columns that identify the record an issue is about.
    catalogue_dataset: str, optional
        Where given, every code run is checked against the catalogue of that
        data set first, so that a code the catalogue does not define fails the
        run rather than reaching the table as a string nobody can look up.
    packages: iterable of str
        The catalogues to look in; see :func:`check_catalogue`.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per issue: ``keys`` and :data:`ISSUE_COLUMN`.
    """
    import pyspark.sql.functions as func

    keys = list(keys)
    checks = list(checks)
    if not checks:
        raise ValueError("no checks were given to run")

    if catalogue_dataset is not None:
        defined = set(checks_of(catalogue_dataset, packages))
        unknown = [code for code, _ in checks if code not in defined]
        if unknown:
            raise ValueError(
                f"{unknown[0]!r} is run but no row of the check catalogue "
                f"defines it for {catalogue_dataset!r}. Add it to "
                f"{CHECK_CATALOGUE_FILE}, so that a reader of the issue table "
                "can look the code up."
            )

    gathered = None
    for code, function in checks:
        found = (function(frame)
                 .select(*keys)
                 .withColumn(ISSUE_COLUMN, func.lit(code)))
        gathered = found if gathered is None else gathered.unionByName(found)
    return gathered.distinct()


def outstanding_partitions(source, issues, column):
    """The values of ``column`` that are in the source and not in the issues.

    This is what makes a run incremental without a watermark of its own: the
    issues of a partition are written when it is checked, so a partition the
    issue table does not mention is one nothing has looked at yet. A partition
    whose records are all sound is mentioned by nothing, which is why a run
    that checks it finds no issues and the next run checks it again -- cheap,
    and the alternative is a second table recording that nothing was found.

    Both arguments are frames rather than paths, so that this works the same
    over a directory of parquet and over a table in a catalogue.

    Parameters
    ----------
    source: pyspark.sql.DataFrame
        The table being checked.
    issues: pyspark.sql.DataFrame or None
        The issue table, or ``None`` where it does not exist yet.
    column: str
        The partition column.

    Returns
    -------
    list
        The values still to check, sorted.
    """
    present = {row[0] for row in source.select(column).distinct().collect()}
    done = set()
    if issues is not None:
        done = {row[0] for row in issues.select(column).distinct().collect()}
    return sorted(present - done)


def merged_issues(found, existing, column, rechecked):
    """The issues to write, where a run looked at part of the table only.

    :func:`fincommons.storage.write_silver` replaces a table in full, and a
    run told to check two countries out of thirty must not thereby throw the
    issues of the other twenty-eight away. What it writes is its own findings
    plus the rows the table already holds for the partitions it did not look
    at, which leaves every partition carrying the findings of the last run
    that read it.

    Parameters
    ----------
    found: pyspark.sql.DataFrame
        What this run found.
    existing: pyspark.sql.DataFrame or None
        The issue table as it stands, or ``None`` where it does not exist yet.
    column: str
        The partition column.
    rechecked: iterable
        The partition values this run looked at. Their old rows are dropped,
        including where the run found nothing in them, which is how a problem
        that has been fixed stops being reported.

    Returns
    -------
    pyspark.sql.DataFrame
        The rows to write.
    """
    import pyspark.sql.functions as func

    if existing is None:
        return found
    kept = existing.where(~func.col(column).isin(list(rechecked)))
    return found.unionByName(kept.select(*found.columns))


def partitioning(issues, column):
    """How to partition a write of an issue table: by the column, or not at all.

    Parquet writes no file whatever for an empty frame that is partitioned,
    and a table with no file has no schema to read from. A lake whose filings
    are all sound would therefore write an issue table nobody could open --
    which is the one lake where everything is right. An empty frame is
    written flat instead, which leaves a single file carrying the schema and
    no rows, and says exactly the same thing.

    Parameters
    ----------
    issues: pyspark.sql.DataFrame
        The rows about to be written.
    column: str
        The partition column the table uses when it has rows.

    Returns
    -------
    tuple
        The argument to pass as ``partition_by``.
    """
    return () if issues.isEmpty() else (column,)


def issue_counts(issues, by=None, packages=("fincommons",), dataset=None):
    """How many records each check found, for reading the table as a whole.

    The point of writing issues to a table rather than to a log is to be able
    to count them, and this is the count: one row per code, with the severity
    and the title of the catalogue joined on, so that the result reads without
    a second lookup.

    Parameters
    ----------
    issues: pyspark.sql.DataFrame
        An issue table, or any part of one.
    by: iterable of str, optional
        Further columns to group by, e.g. the partition column or the year.
    packages: iterable of str
        The catalogues to describe the codes from.
    dataset: str, optional
        Restrict the catalogue to one data set, where two define the same code.

    Returns
    -------
    pyspark.sql.DataFrame
        ``by``, the code, the severity, the title and ``RECORDS``, ordered by
        severity and then by the count.
    """
    import pyspark.sql.functions as func
    import pyspark.sql.types as types

    by = list(by or [])
    spark = issues.sparkSession

    counted = (issues.groupBy(*by, ISSUE_COLUMN)
               .agg(func.count(func.lit(1)).alias("RECORDS")))

    catalogue = check_catalogue(dataset=dataset, packages=packages)
    rows = [(row.code, row.severity, row.title)
            for row in catalogue.itertuples()]
    described = spark.createDataFrame(rows, schema=types.StructType([
        types.StructField(ISSUE_COLUMN, types.StringType(), nullable=False),
        types.StructField("SEVERITY", types.StringType(), nullable=False),
        types.StructField("TITLE", types.StringType(), nullable=False),
    ]))

    rank = func.lit(len(SEVERITIES))
    for i, name in enumerate(SEVERITIES):
        rank = func.when(func.col("SEVERITY") == name, func.lit(i)).otherwise(rank)

    return (counted
            .join(func.broadcast(described), [ISSUE_COLUMN], "left")
            .withColumn("_rank", rank)
            .orderBy("_rank", func.col("RECORDS").desc(), ISSUE_COLUMN)
            .drop("_rank")
            .select(*by, ISSUE_COLUMN, "SEVERITY", "TITLE", "RECORDS"))


def with_issues(records, issues, keys, separator="+"):
    """Label records with the codes their issues carry, one column.

    A record with two issues is two rows of the issue table and one row here,
    with the codes joined, which is the shape a reader wants when looking at
    the records rather than at the checks. A record nothing objected to keeps
    a null, which is the great majority of them.

    Parameters
    ----------
    records: pyspark.sql.DataFrame
        The table the issues are about.
    issues: pyspark.sql.DataFrame
        Its issue table.
    keys: iterable of str
        The columns the two are joined on.
    separator: str
        What the codes of one record are joined with.
    """
    import pyspark.sql.functions as func

    keys = list(keys)
    gathered = (issues.groupBy(*keys)
                .agg(func.array_join(
                    func.array_sort(func.collect_set(ISSUE_COLUMN)),
                    separator).alias(ISSUE_COLUMN)))
    return records.join(func.broadcast(gathered), keys, "left")


def suppressed(issues, exceptions, keys=None):
    """The issues that remain once the known ones are taken out.

    A deployment watching a publisher learns that some issues are not worth
    looking at again: a filer that tags one line its own way every year, a
    check that is wrong about one data set. Rather than turning the check off
    for everybody, those are listed and taken out on the way to a report,
    which keeps the issue table a record of what the checks found and leaves
    the judgement where it belongs.

    Parameters
    ----------
    issues: pyspark.sql.DataFrame
        An issue table.
    exceptions: pyspark.sql.DataFrame or pandas.DataFrame
        The issues to take out. It carries :data:`ISSUE_COLUMN` and whichever
        of the issue table's own columns narrow it -- an entity, a filing --
        and nothing else.
    keys: iterable of str, optional
        The columns to match on. By default every column the two share.
    """
    from pyspark.sql import DataFrame

    # A pandas frame has a join of its own, so asking whether it can be
    # joined says nothing; what matters is whether Spark already holds it.
    if not isinstance(exceptions, DataFrame):
        exceptions = issues.sparkSession.createDataFrame(exceptions)
    keys = list(keys or [name for name in exceptions.columns
                         if name in issues.columns])
    if ISSUE_COLUMN not in keys:
        raise ValueError(
            f"the exceptions have to name the {ISSUE_COLUMN} they are "
            "exceptions to; one that matched on the record alone would "
            "suppress every check at once")
    return issues.join(exceptions.select(*keys).distinct(), keys, "left_anti")


#: What this module offers. The machinery of a data quality run, and the
#: catalogue that says what each check means. The checks themselves belong to
#: the data sets they are made of.
__all__ = [
    "CHECKED_COLUMN",
    "CHECK_CATALOGUE_COLUMNS",
    "CHECK_CATALOGUE_FILE",
    "CheckDescription",
    "ISSUE_COLUMN",
    "SEVERITIES",
    "check_catalogue",
    "checks_of",
    "describe_check",
    "issue_counts",
    "merged_issues",
    "outstanding_partitions",
    "partitioning",
    "run_checks",
    "suppressed",
    "with_issues",
]
