"""ESEF filings: reading an index and a report, in plain Python.

An index file of filings.xbrl.org is a JSON:API document: ``data`` lists the
filings, each with its attributes, and ``included`` the entities that filed
them, by the repository's own number. :func:`filings_from_index` turns it
into one record per filing, the entity's LEI and name put beside it.

A report is the xBRL-JSON the repository converted the filing to: a
``documentInfo`` and a ``facts`` object, each fact a value with its
``dimensions`` -- the concept, the entity, the period, the unit, the language
of a text and the dimensions of the taxonomy, such as
``ifrs-full:ComponentsOfEquityAxis``. :func:`facts_from_report` turns it
into one row per fact.

**A text fact that holds markup is left out.** Most of a report, by size, is
its notes tagged as text blocks: the note on intangible assets of one
filing is half a megabyte of HTML, tagged again for goodwill and for
intangible assets and goodwill together. The concept does not say that it is
a text block -- that is a type of the taxonomy, which the report does not
carry -- but the value does, and a value with markup in it is the note as
the filing presents it rather than a figure to compare. The Bronze layer
keeps the whole report.

Nothing here needs Spark, so that the reading can be tested on its own and
run on the executors as it is.
"""

import datetime
import re

import pandas

#: The columns :func:`facts_from_report` returns, in order.
FACT_COLUMNS = ["FACT", "LEI", "CONCEPT", "PRDSTART", "PRDEND", "UNIT",
                "DIMENSIONS", "VALUE", "TEXT", "DECIMALS", "LANG"]

#: The dimensions every fact of xBRL-JSON may carry, which the table gives a
#: column of their own. The others are the dimensions of the taxonomy, which
#: go into ``DIMENSIONS``.
CORE_DIMENSIONS = {"concept", "entity", "period", "unit", "language", "noteId"}

#: What a filing and a country may look like. Both become names of files and
#: directories in the Bronze layer, and the index is read from the network,
#: so a value that could be more than a name -- a ``..``, a slash -- is
#: refused rather than used.
_FILING = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_COUNTRY = re.compile(r"^[A-Z]{2}$")

#: The language a report is in, as the name of its file ends:
#: ``...-2026-03-31-1-de.json``.
_LANGUAGE = re.compile(r"-([a-z]{2,3})\.(?:json|x?html?)$")

#: An element of markup, which is what tells a text block from a short text.
_MARKUP = re.compile(r"<[A-Za-z/!][^>]*>")


def is_filing(value):
    return isinstance(value, str) and bool(_FILING.match(value))


def is_country(value):
    return isinstance(value, str) and bool(_COUNTRY.match(value))


def language_of(*urls):
    """The language a filing's report is in, read off the name of its file.

    The first address that names one decides. ``None`` where none does, as for
    the filings of the United Kingdom, whose files end ``-T01.json``.
    """
    for url in urls:
        if url:
            match = _LANGUAGE.search(url)
            if match:
                return match.group(1)
    return None


def _date(value):
    if not value:
        return None
    return datetime.date.fromisoformat(str(value)[:10])


def _timestamp(value):
    if not value:
        return None
    return datetime.datetime.fromisoformat(str(value)).replace(tzinfo=None)


def _integer(value):
    return None if value is None else int(value)


def filings_from_index(document):
    """The filings an index file lists, one record per filing.

    Parameters
    ----------
    document: dict
        The index file, as :func:`json.load` reads it.

    Returns
    -------
    list of dict
        One record per filing, with the columns of
        :data:`fincommons.esef.common.filings_schema`. A filing whose
        identifier or country is not a plain name is left out, since it
        cannot be filed under it.

    Raises
    ------
    ValueError
        If the document is not an index: no ``data`` list. An error page sent
        with status 200 would otherwise be taken for a year without filings.
    """
    if not isinstance(document, dict) or not isinstance(document.get("data"), list):
        raise ValueError("The document is not an index of filings: it has no "
                         "'data' list.")
    entities = {
        entity.get("id"): entity.get("attributes") or {}
        for entity in document.get("included") or []
        if entity.get("type") == "entity"
    }
    records = []
    for filing in document["data"]:
        attributes = filing.get("attributes") or {}
        identifier = attributes.get("fxo_id")
        country = attributes.get("country")
        if not is_filing(identifier) or not is_country(country):
            continue
        entity_id = (((filing.get("relationships") or {}).get("entity") or {})
                     .get("data") or {}).get("id")
        entity = entities.get(entity_id, {})
        # The identifier of a filing begins with the LEI it was filed under,
        # which stands in where the entity is not among those included.
        lei = entity.get("identifier") or identifier.split("-")[0]
        parts = identifier.split("-")
        records.append({
            "FILING": identifier,
            "LEI": lei,
            "NAME": entity.get("name"),
            "COUNTRY": country,
            "SYSTEM": parts[-3] if len(parts) >= 3 else None,
            "PERIODEND": _date(attributes.get("period_end")),
            "LANG": language_of(attributes.get("json_url"),
                                attributes.get("report_url")),
            "PUBLISHED": _timestamp(attributes.get("published")),
            "ADDED": _timestamp(attributes.get("date_added")),
            "PROCESSED": _timestamp(attributes.get("processed")),
            "ERRORS": _integer(attributes.get("error_count")),
            "WARNINGS": _integer(attributes.get("warning_count")),
            "INCONSISTENCIES": _integer(attributes.get("inconsistency_count")),
            "PACKAGEURL": attributes.get("package_url"),
            "REPORTURL": attributes.get("report_url"),
            "VIEWERURL": attributes.get("viewer_url"),
            "JSONURL": attributes.get("json_url"),
            "PACKAGESHA256": attributes.get("sha256"),
        })
    return [r for r in records if r["PERIODEND"] is not None]


def _point(text, end):
    """One end of a period of xBRL-JSON as the day it stands for.

    xBRL-JSON writes a period as instants: ``2024-04-01T00:00:00`` is the
    start of the 1 April 2024, which as the end of a period, or as an
    instant, is the end of 31 March. A date without a time is the whole of
    that day, and a time within a day is that day.
    """
    if "T" not in text:
        return datetime.date.fromisoformat(text)
    stamp = datetime.datetime.fromisoformat(text)
    day = stamp.date()
    if end and stamp.time() == datetime.time(0, 0):
        day -= datetime.timedelta(days=1)
    return day


def parse_period(text):
    """The first and the last day of a period of xBRL-JSON.

    Parameters
    ----------
    text: str
        ``2024-04-01T00:00:00/2025-04-01T00:00:00`` for a duration,
        ``2025-04-01T00:00:00`` for an instant.

    Returns
    -------
    tuple
        ``(start, end)`` as dates, both days included; ``start`` is ``None``
        for an instant. The examples are ``(2024-04-01, 2025-03-31)`` and
        ``(None, 2025-03-31)``.
    """
    if "/" in text:
        start, end = text.split("/", 1)
        return _point(start, end=False), _point(end, end=True)
    return None, _point(text, end=True)


def is_text_block(value):
    """Whether a text value is a text block: a note with its markup."""
    return isinstance(value, str) and bool(_MARKUP.search(value))


def _decimals(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        # "INF", which says the value is exact.
        return None


def facts_from_report(document):
    """The facts of one report, one row per fact.

    Parameters
    ----------
    document: dict
        The report in xBRL-JSON, as :func:`json.load` reads it.

    Returns
    -------
    pandas.DataFrame
        The columns :data:`FACT_COLUMNS`. A numeric fact -- one with a unit
        -- has ``VALUE`` and no ``TEXT``, any other fact ``TEXT`` and no
        ``VALUE``; a value with a unit that is not a number is kept as
        ``TEXT``. A fact reported as nil, and a text block, are left out; see
        the module docstring. ``DIMENSIONS`` lists the dimensions of the
        taxonomy as ``axis=member``, sorted and joined by ``;``, and is empty
        where the fact has none.

    Raises
    ------
    ValueError
        If the document is not a report in xBRL-JSON: no ``facts`` object.
    """
    if not isinstance(document, dict) or not isinstance(document.get("facts"), dict):
        raise ValueError("The document is not a report in xBRL-JSON: it has no "
                         "'facts' object.")
    rows = []
    for fact_id, fact in document["facts"].items():
        dimensions = fact.get("dimensions") or {}
        value = fact.get("value")
        concept = dimensions.get("concept")
        period = dimensions.get("period")
        if value is None or not concept or not period:
            continue
        unit = dimensions.get("unit")
        if unit is None and is_text_block(value):
            continue
        start, end = parse_period(period)
        entity = dimensions.get("entity") or ""
        others = sorted(f"{axis}={member}" for axis, member in dimensions.items()
                        if axis not in CORE_DIMENSIONS)
        number = None
        if unit is not None:
            try:
                number = float(value)
            except (TypeError, ValueError):
                number = None
        rows.append({
            "FACT": fact_id,
            "LEI": entity.split(":", 1)[-1],
            "CONCEPT": concept,
            "PRDSTART": start,
            "PRDEND": end,
            "UNIT": unit,
            "DIMENSIONS": ";".join(others) or None,
            "VALUE": number,
            # A value with a unit that is not a number is kept as text rather
            # than lost.
            "TEXT": None if number is not None else str(value),
            "DECIMALS": _decimals(fact.get("decimals")),
            "LANG": dimensions.get("language"),
        })
    # The columns are built as Python objects and the numbers typed after.
    # pandas 3 would otherwise infer its string type for the text columns and
    # give a missing text as NaN where pandas 2 gives None.
    frame = pandas.DataFrame(rows, columns=FACT_COLUMNS, dtype=object)
    frame["VALUE"] = frame["VALUE"].astype("float64")
    frame["DECIMALS"] = frame["DECIMALS"].astype("Int32")
    return frame


__all__ = [
    "CORE_DIMENSIONS",
    "FACT_COLUMNS",
    "facts_from_report",
    "filings_from_index",
    "is_country",
    "is_filing",
    "is_text_block",
    "language_of",
    "parse_period",
]
