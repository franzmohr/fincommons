"""STS: the register and the spreadsheets in the Bronze layer.

Two things are collected, and neither needs a key. The register is read through
the machine-to-machine services of ESMA's registers portal, which are a Solr
index queried by URL; the spreadsheets are files on esma.europa.eu.

**A snapshot is taken, not an update.** The register answers for what is STS
today and for nothing else: a cancelled notification simply leaves it, and
since ESMA froze its two history spreadsheets in 2022 nothing published records
a removal. So a collection writes a complete, dated copy of the register beside
the ones taken before it, and never rewrites one. What left the register
between two runs is then a diff of two snapshots, which is the only way the
question can be answered at all. A lake that compacts these snapshots away
destroys the only record it has.

**Paging is ordered.** Solr serves a page by counting ``start`` documents into
a result set, so two requests of one run see a consistent set only where the
order is fixed. Every request therefore carries ``sort=id asc``, and a
collection checks that it ended up with as many documents as the first answer
said there were -- a mismatch means the index moved under the run, and the
snapshot says so rather than looking complete.

**The shape is recorded, not enforced.** What comes back is stored as it came
back, down to the bytes. Beside it the record of the collection lists every
field name the notifications carried and how many carried it, which is a field
census per snapshot: it is what the Silver layer is designed from, and
comparing two of them is how a change to the register is noticed. A field that
every notification carried when this module was written and that no longer
appears at all is reported as missing and warned about, because the Silver
layer reads it.

A link that has gone stale is the one failure that does not look like one:
esma.europa.eu answers a missing file with a page of HTML and an HTTP status
that the download would otherwise store as a spreadsheet. The Brexit list does
exactly this today, which is why :func:`collect_sheet` checks the content type
and the first bytes of what arrives before it keeps it.
"""

import datetime
import json
import logging
import os
import time

import requests

from ..log import context, pipeline_run
from ..marketpricedata.common import HTTP_TIMEOUT, USER_AGENT
from .common import (CHILD_KINDS, CORE_FIELDS, DOCUMENT_KINDS, PAGE_SIZE,
                     PAGE_SORT, REGISTER_URL, SHEET_CONTENT_TYPE, SHEET_MAGIC,
                     SHEET_REFERENCES, SHEETS, SOURCE, path_bronze_page,
                     path_bronze_sheet, path_register_record,
                     path_sheet_record, path_bronze_snapshot, read_json, today,
                     write_bytes, write_json)

logger = logging.getLogger(__name__)

#: How often a request the portal turns away for the moment is made again, and
#: the longest it is waited for.
RETRIES = 5
MAX_WAIT = 120

#: The answers that say "not now" rather than "not at all".
_RETRY_STATUS = {429, 500, 502, 503, 504}

#: What a Solr response header says when all is well.
_OK = 0

#: The most pages one collection will ask for, whatever the totals say. A
#: guard against a register that answers a page without advancing, which would
#: otherwise be an endless run; at the default page size it allows a register
#: a hundred times the size of today's.
MAX_PAGES = 400


def _wait(response, attempt):
    """How long to wait before asking again: what the server says, or longer
    each time, but never more than :data:`MAX_WAIT` seconds."""
    try:
        wait = float(response.headers.get("Retry-After"))
    except (TypeError, ValueError):
        wait = 2.0 ** (attempt + 1)
    return min(max(wait, 1.0), MAX_WAIT)


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds")


def fetch(url, params=None, headers=None):
    """One answer of the portal: its status, its body and its headers.

    A request turned away for the moment is made again after the wait it asks
    for, up to :data:`RETRIES` times. This is the one seam the module has for
    the network, and what the tests stand in at.
    """
    wait = 1.0
    for attempt in range(RETRIES + 1):
        response = requests.get(
            url,
            params=params or {},
            headers={"User-Agent": USER_AGENT,
                     "Accept-Encoding": "gzip, deflate", **(headers or {})},
            stream=True,
            timeout=HTTP_TIMEOUT,
        )
        try:
            if response.status_code in _RETRY_STATUS and attempt < RETRIES:
                wait = _wait(response, attempt)
                logger.debug(
                    "The portal asked to wait, the request is made again",
                    extra=context(module="sts", layer="bronze", url=url,
                                  status=response.status_code, wait=wait),
                )
            else:
                body = b"".join(response.iter_content(chunk_size=1 << 16))
                return response.status_code, body, dict(response.headers)
        finally:
            response.close()
        time.sleep(wait)


def fetch_page(kind, start=0, rows=PAGE_SIZE):
    """One page of the register, as bytes and as a parsed document.

    Parameters
    ----------
    kind: str
        ``'parent'`` for the notifications or ``'child'`` for their tranches
        and parties; see :data:`fincommons.sts.common.DOCUMENT_KINDS`.
    start: int
        How many documents to count past before the page begins.
    rows: int
        How many documents the page holds at most.

    Returns
    -------
    tuple
        The body as it came back, and the same parsed. The body is what the
        Bronze layer stores, so that what is kept is what was published rather
        than what this module made of it.

    Raises
    ------
    ValueError
        Where the kind is not one of the two the core holds.
    RuntimeError
        Where the portal answers with an HTTP error, with something that is not
        JSON, or with a Solr header whose status is not zero.
    """
    if kind not in DOCUMENT_KINDS:
        raise ValueError(
            f"Kind {kind!r} not recognised. Use "
            + " or ".join(repr(k) for k in DOCUMENT_KINDS) + ".")

    params = {"q": f"type_s:{kind}", "fl": "*", "rows": rows, "start": start,
              "sort": PAGE_SORT, "wt": "json"}
    status, body, _ = fetch(REGISTER_URL, params)

    if status >= 300:
        raise RuntimeError(
            f"{SOURCE} answered {status} for the {kind} documents of the "
            f"register, at {start}")

    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise RuntimeError(
            f"{SOURCE} answered something that is not JSON for the {kind} "
            f"documents of the register, at {start}") from error

    header = document.get("responseHeader") or {}
    if header.get("status") != _OK:
        raise RuntimeError(
            f"{SOURCE} answered Solr status {header.get('status')!r} for the "
            f"{kind} documents of the register, at {start}")
    if "response" not in document:
        raise RuntimeError(
            f"{SOURCE} answered a document with no result set for the {kind} "
            f"documents of the register, at {start}")

    return body, document


def _shape(documents):
    """The field names the documents carry, and how often each appears."""
    counts = {}
    for document in documents:
        for field in document:
            counts[field] = counts.get(field, 0) + 1
    return counts


def collect_register(config_path, day=None, page_size=PAGE_SIZE, force=False):
    """Collect one snapshot of the register into the Bronze layer.

    Both kinds of document are paged through and every answer is stored as it
    came back. A snapshot already held is left alone unless ``force`` says
    otherwise: the point of a snapshot is the day it was taken, and collecting
    a second one on the same day would overwrite the first.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str, optional
        The day the snapshot is filed under; today in UTC by default.
    page_size: int
        How many documents one request asks for.
    force: bool
        Collect the snapshot again even where the Bronze layer holds one for
        that day, replacing its pages.

    Returns
    -------
    dict
        What happened: ``'outcome'`` is ``'collected'`` or ``'held'``, and a
        collection also reports the documents and pages per kind, the totals
        the portal stated, a census of the fields the notifications carried,
        and any field expected of every notification that has gone missing.

    Raises
    ------
    RuntimeError
        Where a page cannot be collected, or where the portal stops advancing
        through the result set.
    """
    day = (day or today())
    day = day if isinstance(day, str) else day.isoformat()
    record = path_register_record(config_path, day)

    if not force and read_json(record) is not None:
        return {"outcome": "held", "day": day}

    started = _now()
    documents = {}
    pages = {}
    totals = {}
    shape = {}

    for kind in DOCUMENT_KINDS:
        collected = 0
        page = 0
        while True:
            if page >= MAX_PAGES:
                raise RuntimeError(
                    f"The register did not end after {MAX_PAGES} pages of "
                    f"{kind} documents; the page size is {page_size} and "
                    f"{collected} documents were collected")
            body, document = fetch_page(kind, collected, page_size)
            result = document["response"]
            totals[kind] = result.get("numFound")
            rows = result.get("docs") or []
            write_bytes(path_bronze_page(config_path, day, kind, page), body)
            page += 1
            if not rows:
                break
            collected += len(rows)
            if kind == "parent":
                for field, count in _shape(rows).items():
                    shape[field] = shape.get(field, 0) + count
            if collected >= (totals[kind] or 0):
                break
        documents[kind] = collected
        pages[kind] = page

    missing = [f for f in CORE_FIELDS if f not in shape]
    short = {kind: (documents[kind], totals.get(kind))
             for kind in DOCUMENT_KINDS
             if totals.get(kind) is not None
             and documents[kind] != totals[kind]}

    outcome = {
        "outcome": "collected",
        "day": day,
        "started": started,
        "finished": _now(),
        "source": SOURCE,
        "url": REGISTER_URL,
        "page_size": page_size,
        "documents": documents,
        "pages": pages,
        "stated_totals": totals,
        "fields": len(shape),
        "fields_missing": missing,
        # The census of the snapshot: every field the notifications carried and
        # how many carried it. Two of these side by side say what the register
        # has changed; one of them says what the Silver layer has to read.
        "field_counts": dict(sorted(shape.items())),
        "child_kinds": sorted(set(CHILD_KINDS)),
    }
    write_json(record, outcome)

    if missing:
        logger.warning(
            "The register no longer carries fields this module expects of "
            "every notification, so the snapshot holds less than the Silver "
            "layer reads",
            extra=context(module="sts", layer="bronze", source=SOURCE,
                          day=day, fields=", ".join(missing)),
        )
    if short:
        logger.warning(
            "Fewer documents were collected than the register said it held, "
            "which happens where the index changed under the run; the "
            "snapshot is incomplete and its record says so",
            extra=context(module="sts", layer="bronze", source=SOURCE,
                          day=day,
                          counts=", ".join(f"{k}: {got} of {said}"
                                           for k, (got, said) in short.items())),
        )

    logger.info(
        "A snapshot of the register was collected",
        extra=context(module="sts", layer="bronze", source=SOURCE, day=day,
                      notifications=documents.get("parent"),
                      children=documents.get("child"),
                      fields=len(shape)),
    )
    return outcome


def collect_sheet(config_path, name, day=None, force=False):
    """Collect one of the spreadsheets ESMA publishes beside the register.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    name: str
        Which one; a key of :data:`fincommons.sts.common.SHEETS`.
    day: datetime.date or str, optional
        The day the copy is filed under; today in UTC by default.
    force: bool
        Collect it again even where that day's copy is held.

    Returns
    -------
    str
        What happened: ``'collected'``, ``'held'`` for a copy already there, or
        ``'unavailable'`` where the address no longer serves a spreadsheet.

    Raises
    ------
    ValueError
        Where the name is not one of the spreadsheets.
    """
    if name not in SHEETS:
        raise ValueError(
            f"Spreadsheet {name!r} not recognised. Use one of "
            + ", ".join(repr(k) for k in sorted(SHEETS)) + ".")

    day = (day or today())
    day = day if isinstance(day, str) else day.isoformat()
    target = path_bronze_sheet(config_path, day, name)

    if not force and os.path.exists(target):
        return "held"

    url = SHEETS[name]
    status, body, headers = fetch(url, headers={"Accept": SHEET_CONTENT_TYPE})
    content_type = (headers.get("Content-Type") or "").split(";")[0].strip()
    served = status < 300 and body[:4] == SHEET_MAGIC

    if not served:
        # A file ESMA has moved answers with a page of HTML, which a download
        # that trusted the status would store as a spreadsheet. The Brexit
        # list does this today.
        write_json(path_sheet_record(config_path, day, name), {
            "outcome": "unavailable",
            "day": day,
            "collected": _now(),
            "source": SOURCE,
            "name": name,
            "reference": SHEET_REFERENCES.get(name),
            "url": url,
            "status": status,
            "content_type": content_type,
            "bytes": len(body),
        })
        logger.warning(
            "The address no longer serves a spreadsheet, so nothing was "
            "stored for it",
            extra=context(module="sts", layer="bronze", source=SOURCE,
                          day=day, file=f"{name}.xlsx", status=status,
                          content_type=content_type),
        )
        return "unavailable"

    write_bytes(target, body)
    write_json(path_sheet_record(config_path, day, name), {
        "outcome": "collected",
        "day": day,
        "collected": _now(),
        "source": SOURCE,
        "name": name,
        "reference": SHEET_REFERENCES.get(name),
        "url": url,
        "status": status,
        "content_type": content_type,
        "bytes": len(body),
        "last_modified": headers.get("Last-Modified"),
    })
    logger.info(
        "A spreadsheet was collected",
        extra=context(module="sts", layer="bronze", source=SOURCE, day=day,
                      file=f"{name}.xlsx", bytes=len(body)),
    )
    return "collected"


def collect_sheets(config_path, names=None, day=None, force=False):
    """Collect the spreadsheets, and report what happened to each.

    One that cannot be collected does not stop the others: the four are
    independent sources and three of them are frozen, so a run that got two
    has two more than it had.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    names: iterable of str, optional
        Which ones; all of them by default.
    day: datetime.date or str, optional
        The day the copies are filed under; today in UTC by default.
    force: bool
        Collect one again even where that day's copy is held.

    Returns
    -------
    dict
        The outcome per spreadsheet, ``'failed'`` where the collection raised.
    """
    outcomes = {}
    for name in (names if names is not None else sorted(SHEETS)):
        try:
            outcomes[name] = collect_sheet(config_path, name, day, force)
        except Exception:
            outcomes[name] = "failed"
            logger.exception(
                "The spreadsheet could not be collected",
                extra=context(module="sts", layer="bronze", source=SOURCE,
                              file=f"{name}.xlsx"),
            )
    return outcomes


def update_bronze_sts(config_path, register=True, sheets=True, day=None,
                      page_size=PAGE_SIZE, force=False):
    """Collect the register and the spreadsheets into the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    register: bool
        Take a snapshot of the register.
    sheets: bool
        Collect the spreadsheets as well.
    day: datetime.date or str, optional
        The day everything collected is filed under; today in UTC by default.
        One day holds one snapshot, so a run that wants a second one on the
        same day passes ``force``.
    page_size: int
        How many documents one request to the register asks for.
    force: bool
        Collect again over what that day already holds.

    Raises
    ------
    RuntimeError
        Where the register could not be collected. A spreadsheet that could
        not be is reported in the outcome and does not raise, the four being
        independent of each other and three of them frozen.
    """
    day = (day or today())
    day = day if isinstance(day, str) else day.isoformat()

    with pipeline_run(
        logger,
        "update_bronze_sts",
        module="sts",
        layer="bronze",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        outcome["day"] = day

        if sheets:
            collected = collect_sheets(config_path, day=day, force=force)
            for name, what in collected.items():
                outcome[f"sheet_{name}"] = what

        if register:
            snapshot = collect_register(config_path, day, page_size, force)
            outcome["register"] = snapshot["outcome"]
            if snapshot["outcome"] == "collected":
                outcome["notifications"] = snapshot["documents"]["parent"]
                outcome["children"] = snapshot["documents"]["child"]
                outcome["fields"] = snapshot["fields"]
                if snapshot["fields_missing"]:
                    outcome["fields_missing"] = len(snapshot["fields_missing"])
            else:
                logger.info(
                    "The Bronze layer already holds a snapshot of the "
                    "register for this day, so none was taken",
                    extra=context(module="sts", layer="bronze", source=SOURCE,
                                  day=day,
                                  folder=path_bronze_snapshot(config_path, day)),
                )


#: What this module offers. Collecting the register and the spreadsheets.
__all__ = [
    "MAX_PAGES",
    "MAX_WAIT",
    "RETRIES",
    "collect_register",
    "collect_sheet",
    "collect_sheets",
    "fetch",
    "fetch_page",
    "update_bronze_sts",
]
