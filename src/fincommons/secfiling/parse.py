"""SEC filings: reading a company file into filings and facts.

Plain Python over the JSON the Commission publishes, as
:mod:`fincommons.esef.parse` is plain Python over xBRL-JSON. Nothing here
takes a Spark session: a company file is read on a worker, one file at a
time, and the rows travel as rows.

A company file is organised by concept and not by filing. Under each taxonomy
it holds a concept, under each concept a unit, and under each unit the list of
values the registrant has ever reported for that concept and unit, each
carrying the accession number of the filing it came from. A filing is
therefore reconstructed by grouping the facts on that accession, and the
reconstruction is exact rather than approximate: every fact of the file
carries one, and a figure restated by a later filing appears again under the
accession of the filing that restated it rather than replacing the first. Of
the twelve thousand eight hundred concept and period combinations of one
registrant, more than half were reported by more than one of its filings,
which is the comparatives of each report appearing beside the period it is
about.

What the file does not hold is as important as what it does. The facts are the
undimensioned numeric ones: a breakdown by segment or by class is not there,
and neither is a narrative fact or a text block. The table this module builds
says so by having no column for either, rather than by having columns that are
always empty.
"""

import re
from collections import defaultdict
from datetime import date, datetime

from .common import facts_schema, filings_schema

#: What an accession number looks like. It becomes part of nothing but a
#: column, yet a value read from the network that is not one is a sign that
#: the file is not what it is taken for, and is refused rather than carried.
_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")

#: What a central index key looks like once padded, as the file names it.
_CIK = re.compile(r"^\d{10}$")

#: The taxonomies that say which framework a filing was prepared under, and
#: what to call it. The cover-page taxonomies say nothing about it: every
#: registrant tags against ``dei`` whichever framework it reports under.
FRAMEWORKS = {"us-gaap": "US-GAAP", "ifrs-full": "IFRS"}

#: The periodic reports that carry financial statements, and their amendments.
#: A company file holds the facts of every filing that tagged anything, an
#: ``8-K`` announcing a result among them; a lake wanting the statements wants
#: these.
PERIODIC_FORMS = ("10-K", "10-Q", "20-F", "40-F")


def is_accession(value):
    """Whether ``value`` is an accession number of the Commission."""
    return bool(isinstance(value, str) and _ACCESSION.match(value))


def padded_cik(value):
    """Return a central index key padded to the ten digits the files use.

    Raises
    ------
    ValueError
        Where the value is not a key. It becomes the name of a file in the
        Bronze layer and part of an address asked of the network, so a value
        that could be more than a number is refused rather than used.
    """
    text = str(value).strip()
    if text.upper().startswith("CIK"):
        text = text[3:]
    if not text.isdigit():
        raise ValueError(f"{value!r} is not a central index key")
    return text.zfill(10)


def is_periodic(form, forms=PERIODIC_FORMS):
    """Whether a form is one of the periodic reports, amendment or not."""
    return form is not None and form.split("/")[0] in forms


def _date(value):
    """Return a date the file states, or ``None`` where it states none."""
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _framework(taxonomies):
    """Return the framework a set of taxonomies says, or ``None``."""
    for taxonomy, framework in FRAMEWORKS.items():
        if taxonomy in taxonomies:
            return framework
    return None


def facts_from_company(document, forms=PERIODIC_FORMS):
    """Return the facts of one company file, as rows of ``facts_schema``.

    Parameters
    ----------
    document: dict
        The company file, as published.
    forms: iterable of str, optional
        The forms to keep, by :func:`is_periodic`. ``None`` keeps every form,
        which includes the results announcements and the registration
        documents that tag a figure or two.

    Returns
    -------
    list of tuple
        One row per fact, in the order of
        :data:`fincommons.secfiling.common.facts_schema`.
    """
    cik = padded_cik(document["cik"])
    rows = []

    for taxonomy, concepts in (document.get("facts") or {}).items():
        for name, concept in concepts.items():
            qname = f"{taxonomy}:{name}"
            for unit, entries in (concept.get("units") or {}).items():
                for entry in entries:
                    accession = entry.get("accn")
                    form = entry.get("form")
                    if not is_accession(accession):
                        continue
                    if forms is not None and not is_periodic(form, forms):
                        continue
                    filed = _date(entry.get("filed"))
                    end = _date(entry.get("end"))
                    if filed is None or end is None:
                        continue
                    value = entry.get("val")
                    rows.append((
                        cik,
                        accession,
                        filed.year,
                        # A fact of a company file has no identifier of its
                        # own. One is made from what identifies it within the
                        # filing, so that the same fact read twice is the same
                        # row, which is what lets a company file be read again
                        # without the table growing.
                        f"{accession}:{qname}:{entry.get('start') or ''}:"
                        f"{entry.get('end')}:{unit}",
                        qname,
                        _date(entry.get("start")),
                        end,
                        unit,
                        float(value) if isinstance(value, (int, float)) else None,
                        form,
                        filed,
                        entry.get("frame"),
                    ))
    return rows


def filings_from_company(document, forms=PERIODIC_FORMS):
    """Return the filings of one company file, as rows of ``filings_schema``.

    A filing is what a group of facts sharing an accession number says it is.
    ``PERIODEND`` is the latest period the filing reports on under a reporting
    taxonomy, which for a periodic report is the period it is about; the
    earlier ones are its comparatives. The cover page is deliberately not
    counted: it states a date after the period closed. ``FRAMEWORK`` comes from the taxonomies the filing tagged
    against and not from the fact that it was filed with the Commission, so
    that a foreign private issuer reporting under IFRS is recognised as one.
    """
    cik = padded_cik(document["cik"])
    name = document.get("entityName")

    by_filing = defaultdict(lambda: {"taxonomies": set(), "ends": [], "facts": 0,
                                     "form": None, "filed": None,
                                     "fy": None, "fp": None})
    for taxonomy, concepts in (document.get("facts") or {}).items():
        for concept in concepts.values():
            for entries in (concept.get("units") or {}).values():
                for entry in entries:
                    accession = entry.get("accn")
                    form = entry.get("form")
                    if not is_accession(accession):
                        continue
                    if forms is not None and not is_periodic(form, forms):
                        continue
                    filed, end = _date(entry.get("filed")), _date(entry.get("end"))
                    if filed is None or end is None:
                        continue
                    row = by_filing[accession]
                    row["taxonomies"].add(taxonomy)
                    # Only the reporting taxonomies decide the period. The
                    # cover page states the shares outstanding at a date
                    # after the period closed -- a quarterly report of 2026
                    # ending 31 August carries a cover date of 23 September
                    # -- so counting it would make every filing look as
                    # though it reported on a period it does not.
                    if taxonomy in FRAMEWORKS:
                        row["ends"].append(end)
                    row["facts"] += 1
                    row["form"] = row["form"] or form
                    row["filed"] = row["filed"] or filed
                    row["fy"] = row["fy"] if row["fy"] is not None else entry.get("fy")
                    row["fp"] = row["fp"] or entry.get("fp")

    rows = []
    for accession, found in sorted(by_filing.items()):
        taxonomies = sorted(found["taxonomies"])
        rows.append((
            accession, cik, name, found["form"], found["filed"],
            max(found["ends"]) if found["ends"] else None,
            int(found["fy"]) if found["fy"] is not None else None,
            found["fp"],
            ";".join(taxonomies),
            _framework(found["taxonomies"]),
            found["facts"],
        ))
    return rows


def read_company(document, forms=PERIODIC_FORMS):
    """Return the filings and the facts of one company file.

    Parameters
    ----------
    document: dict
        The company file, as published.
    forms: iterable of str, optional
        The forms to keep; see :func:`facts_from_company`.
    """
    return filings_from_company(document, forms), facts_from_company(document, forms)


#: What this module offers. Reading a company file into filings and facts.
__all__ = [
    "FRAMEWORKS",
    "PERIODIC_FORMS",
    "facts_from_company",
    "filings_from_company",
    "is_accession",
    "is_periodic",
    "padded_cik",
    "read_company",
]
