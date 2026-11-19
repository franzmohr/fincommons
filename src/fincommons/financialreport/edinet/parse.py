"""EDINET: reading the agency's CSV conversion into facts.

Plain Python over the files the agency hands over, as
:mod:`fincommons.financialreport.sec.parse` is plain Python over the company
files of the Commission. Nothing here takes a Spark session: an archive is
read on a worker, one at a time, and the rows travel as rows.

What arrives for a filing is a zip holding one CSV per document of it -- the
report, and the audit report, which the agency converts as well -- written in
UTF-16 with a byte-order mark and separated by tabs. Nine columns, and the
first line is the header::

    要素ID  項目名  コンテキストID  相対年度  連結・個別  期間・時点  ユニットID  単位  値

**The period is the hard part.** No column of the conversion holds the dates
of a fact's context. ``コンテキストID`` names the period relative to the
filing's own -- ``CurrentYearDuration``, ``Prior4YearDuration``,
``CurrentYearInstant``, ``FilingDateInstant`` -- and ``相対年度`` says the same
in words.

Two things resolve a label into dates, and the better one comes first. A
filing states its own financial year and the comparative one among its facts,
in the document and entity information: ``CurrentFiscalYearStartDateDEI`` and
the three beside it. :func:`anchors_of` reads them, and
:func:`period_of` uses them as reported, so the two periods nearly every use
of these figures rests on are the filer's own dates and not arithmetic. Only
where no anchor covers the label -- the year before the comparative, and
everything further back -- is the period shifted by whole years from the
anchor, which is exact only where the filer's years are of equal length: a
filer on a 52- or 53-week calendar, or one that moved its year end, has years
that are not a whole number of years apart. A report stating five years of
net sales is therefore anchored for two of them and shifted for three. A
label whose shape is not recognised yields no dates at all rather than a
guess, and the label itself is carried into the table beside the dates so
that what each one rested on can be read.

Four things the real conversion does that a description of it would not have
told us, each of which is handled here because a report was read rather than
imagined. That a filing carries its own period dates at all is the first of
them, and it is why this module does not have to trust the index.

A fact with a unit can have no number. The conversion writes ``－`` where a
figure does not apply -- forty-eight times in one report of 1,086 facts -- so
a reader that trusted the unit and called ``float`` on the value would raise
on a sound filing.

The element and the context do not identify a fact. A figure is often tagged
in several places in one report, and the number of employees appeared three
times under one context with one value. An occurrence number is therefore
part of the identifier, so that every row of the conversion survives and a
check rather than this module decides what a figure tagged twice means.

The filer's own concepts carry a prefix built from the form and its EDINET
code, as in ``jpcrp030000-asr_E03270-000:SomeConcept``, which is what tells
an extension from the taxonomy here. :func:`is_extension` is the one place
that knows it.
"""

import csv
import io
import re
import zipfile
from collections import defaultdict
from datetime import date, timedelta

from .common import facts_schema

#: The columns of the conversion, in the order it writes them. The header is
#: checked against this: a file whose columns are not these is not the thing
#: this module reads, and is refused rather than read by position.
CSV_COLUMNS = ("要素ID", "項目名", "コンテキストID", "相対年度", "連結・個別",
               "期間・時点", "ユニットID", "単位", "値")

#: What the conversion is written in. UTF-16 with a byte-order mark and tabs
#: between the fields, which is neither what a CSV reader assumes nor what a
#: description of the file is likely to mention.
CSV_ENCODING = "utf-16"
CSV_DELIMITER = "\t"

#: Where in the archive the conversion puts its files.
CSV_FOLDER = "XBRL_TO_CSV/"

#: The prefixes of the taxonomy. A concept under any other prefix is one the
#: filer created for its own report; see :func:`is_extension`.
TAXONOMY_PREFIXES = ("jppfs_cor", "jpcrp_cor", "jpdei_cor", "jpigp_cor",
                     "jpctl_cor", "jpsps_cor", "jplvh_cor", "jpaud_cor",
                     "jppfs_cnt", "jpcrp_cnt")

#: What the conversion writes where a figure does not apply. A full-width
#: dash, not a hyphen, and not an empty string.
NOT_APPLICABLE = ("－", "-", "‐", "―", "")

#: The elements by which a filing states its own periods, in the document and
#: entity information. They are the anchors :func:`period_of` resolves a
#: context against where the filing states them, which makes the current year
#: and the comparative exact rather than shifted; see :func:`anchors_of`.
ANCHOR_ELEMENTS = {
    "current_start": ("jpdei_cor:CurrentFiscalYearStartDateDEI",),
    "current_end": ("jpdei_cor:CurrentFiscalYearEndDateDEI",
                    "jpdei_cor:CurrentPeriodEndDateDEI"),
    "prior_start": ("jpdei_cor:PreviousFiscalYearStartDateDEI",),
    "prior_end": ("jpdei_cor:PreviousFiscalYearEndDateDEI",
                  "jpdei_cor:ComparativePeriodEndDateDEI"),
}

#: The shape of a context identifier: a period relative to the filing's own,
#: whether it is a span or a moment, and then the dimensions of the fact.
_CONTEXT = re.compile(
    r"^(?P<when>Current|Prior(?P<back>\d+)|Interim|FilingDate|NextYear|"
    r"NextFiscalYear)?"
    r"(?P<span>Year|FiscalYear|Quarter|Interim|AccumulatedQ\d|Period)?"
    r"(?P<kind>Duration|Instant)"
    r"(?:_(?P<members>.+))?$"
)


def is_extension(concept):
    """Whether a concept is one the filer created for its own report."""
    prefix = str(concept).split(":")[0]
    return prefix not in TAXONOMY_PREFIXES


def _shift_years(day, back):
    """``day`` moved back whole years, keeping the day of the month.

    29 February has no counterpart in a year that is not a leap year, and the
    last day of February is what the shift gives for it.
    """
    if day is None:
        return None
    try:
        return day.replace(year=day.year - back)
    except ValueError:
        return day.replace(year=day.year - back, day=28)


def split_context(context):
    """The parts of a context identifier, or ``None`` where it is not one.

    Returns
    -------
    dict or None
        ``when`` (``'Current'``, ``'Prior'``, ``'FilingDate'``, ...),
        ``back`` (how many years, 0 where the label names none), ``kind``
        (``'Duration'`` or ``'Instant'``) and ``members``, the dimensions as
        the identifier spells them or ``None``.
    """
    found = _CONTEXT.match(str(context or ""))
    if not found:
        return None
    when = found.group("when") or "Current"
    return {
        # Without the digits the label carries: how many years back is the
        # business of `back`, and a caller asking what kind of period this is
        # should not have to match "Prior1" and "Prior4" separately.
        "when": "Prior" if when.startswith("Prior") else when,
        "back": int(found.group("back") or 0),
        "span": found.group("span"),
        "kind": found.group("kind"),
        "members": found.group("members"),
    }


def _as_date(text):
    """A date the conversion states as text, or ``None``."""
    text = (text or "").strip()
    if text in NOT_APPLICABLE:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def anchors_of(rows):
    """The periods a filing states for itself, read off its own facts.

    The conversion carries no dates on a context, but the filing states its
    own financial year and the comparative one in the document and entity
    information -- ``CurrentFiscalYearStartDateDEI`` and the three beside it.
    Where it does, the current year and the comparative need no shifting at
    all, which is what makes them exact for a filer on a 52- or 53-week
    calendar and for one that moved its year end.

    Parameters
    ----------
    rows: iterable of sequence
        The rows of the conversion, each at least ``要素ID`` then enough
        columns to reach ``値``.

    Returns
    -------
    dict
        The keys of :data:`ANCHOR_ELEMENTS` that the filing states, as dates.
    """
    wanted = {element: key for key, elements in ANCHOR_ELEMENTS.items()
              for element in elements}
    found = {}
    for row in rows:
        if len(row) < len(CSV_COLUMNS):
            continue
        key = wanted.get(row[0].strip())
        if key is None or key in found:
            continue
        day = _as_date(row[len(CSV_COLUMNS) - 1])
        if day is not None:
            found[key] = day
    return found


def period_of(context, period_start, period_end, filed=None, anchors=None):
    """The period a context names, as a pair of dates.

    Parameters
    ----------
    context: str
        The context identifier the conversion states.
    period_start, period_end: datetime.date or None
        The period the index states for the filing, which a relative label is
        resolved against where the filing states no anchor of its own.
    filed: datetime.date, optional
        The day the filing was submitted, which ``FilingDateInstant`` names.
    anchors: dict, optional
        The periods the filing states for itself, from :func:`anchors_of`.
        Where the current year or the comparative is among them, that date is
        used as reported instead of being shifted.

    Returns
    -------
    tuple
        ``(start, end)``. ``start`` is ``None`` for a moment rather than a
        span, as it is in the other regimes. Both are ``None`` where the
        label is not one this function knows, because a date invented here
        would be indistinguishable in the table from one the filer reported.

    Notes
    -----
    Where no anchor covers the label -- the year before the comparative, and
    everything further back -- the shift is by whole years and is exact only
    where the filer's years are of equal length. A report stating five years
    of net sales is anchored for two of them and shifted for three.
    """
    parts = split_context(context)
    if parts is None:
        return None, None

    anchors = anchors or {}
    period_start = anchors.get("current_start", period_start)
    period_end = anchors.get("current_end", period_end)

    if parts["when"] == "FilingDate":
        return (None, filed) if parts["kind"] == "Instant" else (None, None)

    if parts["when"] in ("NextYear", "NextFiscalYear"):
        # A forecast. The label says a year forward, which is the one
        # direction the index's own period cannot be shifted into reliably,
        # the following year's end not being known.
        return None, None

    if parts["when"] == "Interim" or parts["span"] in ("Interim",) \
            or (parts["span"] or "").startswith("AccumulatedQ") \
            or parts["span"] == "Quarter":
        # A half-year or a quarter of the filing's own year. Which months it
        # covers depends on the filer's calendar, which the conversion does
        # not state, so the dates are left out and the label kept.
        return None, None

    back = parts["back"]

    # The comparative year as the filing states it, rather than the current
    # one moved back twelve months.
    if back == 1 and parts["kind"] == "Instant" and "prior_end" in anchors:
        return None, anchors["prior_end"]
    if back == 1 and parts["kind"] == "Duration" \
            and {"prior_start", "prior_end"} <= set(anchors):
        return anchors["prior_start"], anchors["prior_end"]

    if parts["kind"] == "Instant":
        return None, _shift_years(period_end, back)
    return _shift_years(period_start, back), _shift_years(period_end, back)


def _number(value):
    """The value as a number, or ``None`` where it is not one."""
    text = (value or "").strip()
    if text in NOT_APPLICABLE:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _form_of(member):
    """The form a member of the archive is named by.

    ``XBRL_TO_CSV/jpcrp030000-asr-001_E03270-000_2026-08-31_01_...csv`` is the
    report; ``jpaud-aai-cn-001_...`` the audit report that came with it.
    """
    base = member.rsplit("/", 1)[-1]
    return re.sub(r"-\d+_.*$", "", base) or None


def _series_of(member):
    """The series a member of the archive belongs to, where it is not the main one.

    The name after the form is the code of the entity the document is about
    and a series number: ``..._E03270-000_...`` for a company's own report.
    A filing by a fund manager carries one document per series of a fund,
    ``..._G03898-001_...`` and ``..._G03898-002_...``, under the same form,
    and the series is then the only thing that tells their facts apart.
    ``None`` for the main document, ``-000``, which is what every report a
    company files is.
    """
    base = member.rsplit("/", 1)[-1]
    found = re.match(r"^[^_]+_([^_]+-\d+)_", base)
    if not found or found.group(1).endswith("-000"):
        return None
    return found.group(1)


def facts_from_csv(text, filing, member=None, period_start=None,
                   period_end=None, filed=None, edinet_code=None,
                   filed_year=None):
    """Return the facts of one converted document, as rows of ``facts_schema``.

    Parameters
    ----------
    text: str
        The CSV, already decoded.
    filing: str
        The document management number of the filing.
    member: str, optional
        The name of the file within the archive, which names the form.
    period_start, period_end: datetime.date, optional
        The period the index states for the filing; every relative context is
        resolved against it.
    filed: datetime.date, optional
        The day the filing was submitted.
    edinet_code: str, optional
        The filer.
    filed_year: int, optional
        The partition. The year of ``filed`` by default.

    Raises
    ------
    ValueError
        Where the header is not the nine columns of the conversion. Reading
        such a file by position would put a label in the value column and a
        context in the label column, and nothing downstream would notice.
    """
    rows = list(csv.reader(io.StringIO(text), delimiter=CSV_DELIMITER))
    if not rows:
        return []

    header = [cell.strip().lstrip("﻿") for cell in rows[0]]
    if tuple(header[:len(CSV_COLUMNS)]) != CSV_COLUMNS:
        raise ValueError(
            "the file is not one of the agency's conversions: its columns are "
            f"{header[:9]} rather than {list(CSV_COLUMNS)}"
        )

    form = _form_of(member) if member else None
    # The form names the document in the identifier, and the series with it
    # where the filing holds one document of that form per series.
    series = _series_of(member) if member else None
    document = f"{form or ''}_{series}" if series else (form or '')
    year = filed_year if filed_year is not None else (filed.year if filed else None)
    # Read once for the dates the filing states about itself, then again for
    # the facts: a context is resolved against what the filing says rather
    # than against arithmetic wherever it says anything.
    anchors = anchors_of(rows[1:])
    seen = defaultdict(int)
    found = []

    for row in rows[1:]:
        if len(row) < len(CSV_COLUMNS):
            continue
        concept, label, context, relative, basis, shape, unit, _, value = \
            (cell.strip() for cell in row[:len(CSV_COLUMNS)])
        if not concept or not context:
            continue

        seen[(concept, context)] += 1
        occurrence = seen[(concept, context)]
        parts = split_context(context)
        start, end = period_of(context, period_start, period_end, filed,
                               anchors)
        number = _number(value) if unit not in NOT_APPLICABLE else None

        found.append((
            edinet_code,
            filing,
            year,
            # The conversion gives a fact no identifier. This one is made of
            # what identifies it within the filing, plus an occurrence
            # number, the element and the context not being unique.
            f"{filing}:{document}:{concept}:{context}:{occurrence}",
            concept,
            label or None,
            context,
            relative or None,
            basis or None,
            shape or None,
            start,
            end,
            (parts or {}).get("members"),
            unit if unit not in NOT_APPLICABLE else None,
            number,
            # A value that is not a number is a text, and so is the dash the
            # conversion writes where a figure does not apply: it says the
            # figure is not applicable, which is worth keeping.
            None if number is not None else (value or None),
            form,
        ))
    return found


def facts_from_archive(data, filing, period_start=None, period_end=None,
                       filed=None, edinet_code=None, filed_year=None):
    """Return the facts of one collected archive, as rows of ``facts_schema``.

    Every converted document in the archive is read, the report and the audit
    report alike, and ``doc_form`` says which a fact came from.

    Parameters
    ----------
    data: bytes or str
        The archive as collected, or the path to it.
    filing: str
        The document management number.

    The remaining parameters are those of :func:`facts_from_csv`.
    """
    source = io.BytesIO(data) if isinstance(data, (bytes, bytearray)) else data
    found = []
    with zipfile.ZipFile(source) as archive:
        for member in sorted(archive.namelist()):
            if not member.lower().endswith(".csv"):
                continue
            text = archive.read(member).decode(CSV_ENCODING, errors="replace")
            found.extend(facts_from_csv(
                text, filing, member, period_start, period_end, filed,
                edinet_code, filed_year))
    return found


#: The columns the reading hands back, in the order of the table.
FACT_COLUMNS = [field.name for field in facts_schema.fields]


#: What this module offers. Reading the agency's conversion into facts.
__all__ = [
    "ANCHOR_ELEMENTS",
    "CSV_COLUMNS",
    "CSV_DELIMITER",
    "CSV_ENCODING",
    "CSV_FOLDER",
    "FACT_COLUMNS",
    "NOT_APPLICABLE",
    "TAXONOMY_PREFIXES",
    "anchors_of",
    "facts_from_archive",
    "facts_from_csv",
    "is_extension",
    "period_of",
    "split_context",
]
