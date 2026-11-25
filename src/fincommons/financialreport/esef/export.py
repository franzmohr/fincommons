"""ESEF filings: one issuer's reports as a spreadsheet.

The Silver tables hold the facts of every report of every country collected,
one row per fact, which is the shape to compute over and the wrong shape to
read. A reader who wants to see what one issuer reported -- and, above all,
what its reports say about the same figure one beside the other -- wants a
spreadsheet: :func:`export_esef_reports` writes one, in the OpenDocument
format :mod:`fincommons.spreadsheet` writes, with four sheets.

``about``
    what the file is, which filings it covers, where the figures come from
    and what is not to be read into them.
``figures``
    one row per figure and period, one column per filing. **This is the sheet
    the export exists for**: a row is one concept over one period as every
    filing that states it states it, so the same field of different filings
    stands side by side, and a column is one report. A figure a later report
    restates is a row with two different values in it, and ``differs`` marks
    it.
``filings``
    one row per filing covered, with the column of ``figures`` it stands
    under, what the repository's validation found, and where its files are.
``facts``
    every fact of those filings as it stands in the table, dimensions, texts
    and all, for a reader who wants more than the totals.

Nothing is converted and nothing is corrected. A figure is the number the
issuer tagged, in the unit it tagged it in, which is why ``unit`` is a column
of ``figures`` rather than an assumption: an issuer that changed its reporting
currency has two units in one row, and summing across them is the reader's
mistake to avoid rather than the export's to make silently.
"""

from __future__ import annotations

import datetime
import logging
import os

import pyspark.sql.functions as func

from ...spreadsheet import SUFFIX, Sheet, write_ods
from .common import BASE_URL, SOURCE, connect_to_esef, connect_to_esef_filings
from .quality import TAXONOMY_PREFIXES

logger = logging.getLogger(__name__)

# The three constants below name a *sheet* rather than a table: ``parse``
# already exports ``FACT_COLUMNS`` for the columns of the table of facts, and
# the module puts every submodule's names in one namespace, so a second
# ``FACT_COLUMNS`` here would quietly replace it.

#: The columns of the ``figures`` sheet that name the figure, before the one
#: column per filing that holds the values.
FIGURE_SHEET_KEYS = ("concept", "concept_source", "prd_start", "prd_end",
                     "unit", "filings", "differs")

#: The columns of the ``facts`` sheet, in the order it holds them.
FACT_SHEET_COLUMNS = ("filing", "period_end", "concept", "prd_start",
                      "prd_end", "unit", "dimensions", "value", "text",
                      "decimals", "lang", "fact")

#: The columns of the ``filings`` sheet, in the order it holds them, before
#: the addresses of the files.
FILING_SHEET_COLUMNS = ("filing", "column", "period_end", "lang", "country",
                        "system", "name", "facts", "published", "added",
                        "processed", "errors", "warnings", "inconsistencies")

#: What the ``about`` sheet says about the figures, under "read it as".
CAVEATS = (
    "Every figure is the number the issuer tagged in its own report, in the "
    "unit it tagged it in. Nothing is converted, scaled, restated or "
    "corrected.",
    "A row of 'figures' is one concept over one period. Where two filings "
    "state it, the row holds both -- the later report's comparative figure "
    "beside the earlier report's own -- and 'differs' marks a row whose "
    "values are not all the same, which is a restatement or a tagging fault.",
    "A column of 'figures' is one filing. The period it reports on is the "
    "heading; the other periods in the column are the comparatives that "
    "filing carries.",
    "Where one filing tags a figure twice for one period, in the statements "
    "and again in the notes, the smaller value is taken, as the data quality "
    "checks take it.",
    "Only the totals are in 'figures': a fact with dimensions is a part of a "
    "total, and the parts are in 'facts' with the axes they are broken down "
    "by.",
    "A concept of the issuer's own making is marked 'extension' in "
    "'concept_source'. Its name can change from one report to the next, so "
    "two rows may be one figure under two names.",
    "Text blocks -- the notes, tagged whole -- are not in the tables and so "
    "not in this file. The report as filed is where to read them.",
    "No warranty is given for the correctness or the completeness of a "
    "figure, and nothing here is advice.",
)


def export_esef_reports(spark, config_path, lei, path, years=None, lang=None,
                        created=None):
    """Write one issuer's ESEF reports to an OpenDocument spreadsheet.

    The file holds four sheets -- ``about``, ``figures``, ``filings`` and
    ``facts`` -- and the one it is written for is ``figures``: one row per
    concept and period, one column per filing, so that the same field of
    different reports is read across a row. A figure a later report restates
    shows as a row holding two values, and the ``differs`` column marks it.

    The country of the issuer has to have been collected by the pipeline, and
    the filings whose reports were not collected contribute no facts: they are
    rows of ``filings`` with nothing under their column, which is the honest
    answer to what the lake holds about them.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    lei: str
        The LEI of the issuer, as :mod:`fincommons.entitymasterdata` keys it.
        Blanks are removed and letters put in upper case before it is looked
        for.
    path: str
        Where to write the file. ``.ods`` is appended where the name does not
        end in it, and a directory is written into under the LEI of the
        issuer.
    years: int, optional
        How many financial years to cover, the latest first. Every filing of
        the issuer by default.
    lang: str, optional
        The language of the reports to cover, where an issuer files in more
        than one. Every language by default, which puts one column per filing
        in ``figures``.
    created: datetime.datetime, optional
        What the file states as the moment it was written, and the moment
        ``about`` names. The current one by default.

    Returns
    -------
    str
        The path the file was written to.

    Raises
    ------
    ValueError
        Where the table of filings holds no filing of the issuer, or none in
        the language asked for. A spreadsheet of nothing is not written.
    """
    lei = "".join(str(lei).split()).upper()
    created = created or datetime.datetime.now()

    filings = _issuer_filings(spark, config_path, lei, years, lang)
    countries = sorted({row["country"] for row in filings})
    names = sorted({row["name"] for row in filings if row["name"]})
    issuer = names[-1] if names else lei

    facts = _issuer_facts(spark, config_path, lei, countries,
                          [row["filing"] for row in filings])
    columns = _columns_of(filings)
    figures = _figures(facts, columns)
    counted = _with_fact_counts(filings, columns, facts)

    path = _destination(path, lei)
    logger.info("writing %d filings and %d facts of %s to %s",
                len(filings), len(facts), lei, path)
    # write_ods adds the suffix where the name lacks it, and the path it
    # returns is the file that was written.
    path = write_ods(
        path,
        [
            Sheet("about", ("item", "detail"),
                  _about(issuer, lei, countries, counted, facts, created)),
            Sheet("figures", tuple(FIGURE_SHEET_KEYS) + tuple(
                columns[row["filing"]] for row in filings), figures),
            Sheet("filings", tuple(FILING_SHEET_COLUMNS) + ("report", "viewer",
                                                      "package"),
                  _filings_rows(counted)),
            Sheet("facts", FACT_SHEET_COLUMNS, _facts_rows(facts)),
        ],
        created=created,
        title=f"ESEF reports of {issuer}",
    )
    return path


# What is read
# --------------------------------------------------------------------------


def _issuer_filings(spark, config_path, lei, years, lang):
    """The filings of the issuer, the latest period first, as dicts.

    They are a few dozen rows at most -- one issuer files once a year, in one
    or two languages -- so they are read into Python and the sheets are built
    from them there.
    """
    # What is wrong with the call is said before the lake is read, so that a
    # mistake in an argument does not cost a query first.
    if years is not None:
        if isinstance(years, bool) or not isinstance(years, int):
            raise TypeError("years is the number of financial years to cover")
        if years < 1:
            raise ValueError("years has to be at least one")

    frame = (connect_to_esef_filings(spark, config_path)
             .where(func.col("lei") == lei))
    if lang is not None:
        frame = frame.where(func.lower(func.col("lang")) == str(lang).lower())

    rows = [row.asDict() for row in
            frame.orderBy(func.col("period_end").desc(), "lang", "filing")
            .collect()]
    if not rows:
        spoken = "" if lang is None else f" in {lang!r}"
        raise ValueError(
            f"the table of filings holds no filing of {lei!r}{spoken}. The "
            "indexes cover every country, so an issuer that is not in it has "
            "filed no ESEF report -- check the LEI, and that the pipeline has "
            "run."
        )

    if years is not None:
        kept = sorted({row["period_end"] for row in rows}, reverse=True)[:years]
        rows = [row for row in rows if row["period_end"] in kept]
    return rows


def _issuer_facts(spark, config_path, lei, countries, filings):
    """Every fact of the filings covered, as dicts.

    The country goes into the filter first, because it is what the table of
    facts is partitioned by, and the lei beside it, because a partition is a
    whole country of reports.
    """
    if not filings:
        return []
    frame = (connect_to_esef(spark, config_path)
             .where(func.col("country").isin(countries))
             .where(func.col("lei") == lei)
             .where(func.col("filing").isin(filings)))
    return [row.asDict() for row in
            frame.orderBy("concept", "prd_end", "prd_start", "dimensions",
                          "filing").collect()]


def _destination(path, lei):
    """The file to write, which may be named by a directory to write it in."""
    path = str(path)
    if os.path.isdir(path) or path.endswith(os.sep):
        return os.path.join(path, f"{lei}-esef{SUFFIX}")
    return path


# The sheets
# --------------------------------------------------------------------------


def _columns_of(filings):
    """The heading each filing's figures stand under, by filing.

    A filing is headed by the period it reports on, which is what a reader
    compares by. A period with more than one filing needs more than that --
    an issuer files in two languages, or the repository holds the report
    twice -- and what tells those apart differs: where the languages do,
    every column of the period carries its language; where they do not, as
    for a report filed twice in one language, the columns are numbered.
    Either way *every* column of such a period is marked, because naming only
    the second would leave the first looking like the period's one report,
    which is what a reader of two columns most needs not to believe. Which
    filing a column is stands in the ``filings`` sheet.
    """
    periods = {}
    for row in filings:
        periods.setdefault(row["period_end"], []).append(row)

    columns = {}
    for period, rows in periods.items():
        base = period.isoformat()
        if len(rows) == 1:
            columns[rows[0]["filing"]] = base
            continue
        languages = [row["lang"] for row in rows]
        distinct = all(languages) and len(set(languages)) == len(languages)
        for number, row in enumerate(rows, start=1):
            columns[row["filing"]] = f"{base} ({row['lang']})" if distinct \
                else f"{base} #{number}"
    return columns


def _is_total(fact):
    """Whether a fact is a total: a number, and no axis it is broken down by."""
    return fact["value"] is not None and not (fact["dimensions"] or "")


def _concept_source(concept):
    """Whether a concept is the taxonomy's or the issuer's own."""
    prefix = str(concept).split(":", 1)[0]
    return "taxonomy" if prefix in TAXONOMY_PREFIXES else "extension"


def _figures(facts, columns):
    """One row per figure and period, one column per filing.

    The same figure is often tagged twice in one report, in the statements and
    again in the notes; where a filing holds more than one value for one
    figure and period the smaller is taken, as
    :func:`~fincommons.financialreport.esef.quality.annual_figures` takes it.
    """
    gathered = {}
    for fact in facts:
        if not _is_total(fact):
            continue
        key = (fact["concept"], fact["prd_start"], fact["prd_end"],
               fact["unit"])
        column = columns[fact["filing"]]
        held = gathered.setdefault(key, {})
        if column not in held or fact["value"] < held[column]:
            held[column] = fact["value"]

    order = list(columns.values())
    rows = []
    for key in sorted(gathered, key=_figure_order):
        concept, prd_start, prd_end, unit = key
        values = gathered[key]
        stated = [values[column] for column in order if column in values]
        rows.append([concept, _concept_source(concept), prd_start, prd_end,
                     unit, len(stated),
                     "yes" if len(set(stated)) > 1 else None]
                    + [values.get(column) for column in order])
    return rows


def _figure_order(key):
    """The taxonomy's concepts first, then the issuer's own, then the period.

    A reader looks a figure up by its name, so the name orders the sheet; the
    issuer's own concepts go after the taxonomy's because they are the rows
    whose name may not mean the same thing in two reports.
    """
    concept, prd_start, prd_end, unit = key
    return (_concept_source(concept) == "extension", concept,
            prd_end or datetime.date.min, prd_start or datetime.date.min,
            unit or "")


def _with_fact_counts(filings, columns, facts):
    """The filings, each with its column and how many facts it contributed."""
    counts = {}
    for fact in facts:
        counts[fact["filing"]] = counts.get(fact["filing"], 0) + 1
    rows = []
    for row in filings:
        row = dict(row)
        row["column"] = columns[row["filing"]]
        row["facts"] = counts.get(row["filing"], 0)
        rows.append(row)
    return rows


def _filings_rows(filings):
    """One row per filing, with the addresses of its files spelled out."""
    rows = []
    for row in filings:
        rows.append([row.get(column) for column in FILING_SHEET_COLUMNS]
                    + [_url(row.get("report_url")), _url(row.get("viewer_url")),
                       _url(row.get("package_url"))])
    return rows


def _url(address):
    """The address of a file at the repository, which the index gives relative."""
    if not address:
        return None
    if address.startswith("http://") or address.startswith("https://"):
        return address
    return BASE_URL + address if address.startswith("/") else \
        f"{BASE_URL}/{address}"


def _facts_rows(facts):
    """Every fact as it stands in the table."""
    return [[fact.get(column) for column in FACT_SHEET_COLUMNS] for fact in facts]


def _about(issuer, lei, countries, filings, facts, created):
    """What the file is, what it covers and what is not to be read into it."""
    periods = sorted({row["period_end"] for row in filings})
    collected = [row for row in filings if row["facts"]]
    rows = [
        ("what this is",
         "The annual financial reports one issuer filed in the European "
         "Single Electronic Format, as the facts of those reports stand in "
         "a fincommons data lake."),
        ("issuer", issuer),
        ("lei", lei),
        ("country of filing", ", ".join(countries)),
        ("filings covered", len(filings)),
        ("of which hold facts", len(collected)),
        ("periods", f"{periods[0].isoformat()} to {periods[-1].isoformat()}"
         if periods else None),
        ("facts", len(facts)),
        ("written", created),
        ("source", SOURCE),
        ("source address", BASE_URL),
        ("sheet 'figures'",
         "One row per figure and period, one column per filing: read across "
         "a row to compare the same field between reports, down a column to "
         "read one report."),
        ("sheet 'filings'",
         "One row per filing covered: the column of 'figures' it stands "
         "under, what the repository's validation found, and where its files "
         "are."),
        ("sheet 'facts'",
         "Every fact of those filings as the table holds it, with the axes "
         "of the figures that are broken down and the texts."),
    ]
    rows += [("read it as", caveat) for caveat in CAVEATS]
    return [[item, detail] for item, detail in rows]


#: What this module offers: the one call that writes a workbook.
__all__ = ["CAVEATS", "FACT_SHEET_COLUMNS", "FIGURE_SHEET_KEYS", "FILING_SHEET_COLUMNS",
           "export_esef_reports"]
