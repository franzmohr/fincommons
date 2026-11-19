"""EDINET: the indexes and the filings in the Bronze layer.

Three things are collected here, and they need different amounts of
permission. The list of filers is a zip on the disclosure site and needs no
key at all. The index of a calendar day and the filings themselves come from
the API, which refuses a request that carries no registered key.

The index is the unit of collection. The API answers for one calendar day,
not for one filer and not for a range, so a lake that wants a year asks three
hundred and sixty-five times and a lake that wants the retrievable history
asks some three and a half thousand. That is the shape of the thing, and it
is why the Bronze layer keeps every index it has fetched: the second run over
a year it already holds makes no request at all.

**Collect early.** A securities report leaves EDINET ten years after it was
filed and cannot be fetched afterwards, so the oldest year in reach is being
lost while a lake decides whether it wants it. :func:`collect_index` and
:func:`collect_document` both refuse a day or a filing the window no longer
covers, with a message that says so, rather than storing the empty answer the
API gives for one.

Two ways the API reports a failure, and neither is the HTTP status.

The index API answers an error with **status 200** and the error in the body:
a request with no key, with a key that does not exist and with a key that is
merely inactive all come back as ``200`` carrying
``{"StatusCode": 401, "message": "Access denied ..."}``. A run that trusted
the status would store that as the index of the day and report success.
:func:`fetch_json` therefore reads the body and raises on what it finds
there.

The document API answers an error with a **different content type**: a filing
comes back as an archive and a failure as JSON, which the specification names
as the way to tell them apart -- 「リクエストの成功/エラーに応じたレスポンスヘッダ
の Content-Type を設定しています」. :func:`collect_document` checks that
rather than the length or the first bytes.

The key travels as a query parameter, so the address of every request to the
API contains the secret. Nothing here logs or records an address as it
stands: :func:`fincommons.credentials.redacted` is applied first, and the
record a collection writes keeps the redacted form.
"""

import csv
import datetime
import io
import json
import logging
import os
import time
import zipfile

import requests

from ...credentials import api_key, redacted
from ...log import context, pipeline_run
from ...marketpricedata.common import HTTP_TIMEOUT, USER_AGENT
from .common import (DOCUMENT_KINDS, DOCUMENT_URL, FILERS_ENCODING,
                     FILERS_URL, FILERS_URL_EN, INDEX_URL, SOURCE,
                     earliest_retrievable, filers_schema, filings_schema,
                     path_bronze_document, path_bronze_filers,
                     path_bronze_index, path_bronze_indexes,
                     path_index_record, read_json, write_bytes, write_json)

logger = logging.getLogger(__name__)

#: How often a request the agency turns away for the moment is made again,
#: and the longest it is waited for.
RETRIES = 5
MAX_WAIT = 120

#: The answers that say "not now" rather than "not at all", whether they
#: arrive as an HTTP status or inside the body.
_RETRY_STATUS = {429, 500, 503}

#: What the index API says when all is well.
_OK = 200


def _wait(response, attempt):
    """How long to wait before asking again: what the server says, or longer
    each time, but never more than :data:`MAX_WAIT` seconds."""
    try:
        wait = float(response.headers.get("Retry-After"))
    except (TypeError, ValueError):
        wait = 2.0 ** (attempt + 1)
    return min(max(wait, 1.0), MAX_WAIT)


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def fetch(url, params=None, headers=None):
    """One answer of the agency: its status, its body and its headers.

    A request turned away for the moment is made again after the wait it asks
    for, up to :data:`RETRIES` times. Nothing is logged with the key in it.
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
                    "The agency asked to wait, the request is made again",
                    extra=context(module="edinet", layer="bronze",
                                  url=redacted(response.url),
                                  status=response.status_code, wait=wait),
                )
            else:
                body = b"".join(response.iter_content(chunk_size=1 << 16))
                return response.status_code, body, dict(response.headers)
        finally:
            response.close()
        time.sleep(wait)


def fetch_json(url, params=None):
    """One answer of the index API, checked for the error it hides in its body.

    Raises
    ------
    RuntimeError
        Where the answer is an error. The index API reports one with HTTP
        status 200 and the real status inside the body, so a caller that
        looked at the status alone would store the error as data. The message
        names the status the body gave and never the address, which carries
        the key.
    """
    status, body, _ = fetch(url, params)
    if status >= 300:
        raise RuntimeError(f"{SOURCE} answered {status} for {redacted(url)}")

    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise RuntimeError(
            f"{SOURCE} answered something that is not JSON for "
            f"{redacted(url)}") from error

    inner = (document.get("metadata") or {}).get("status") \
        or document.get("StatusCode")
    message = (document.get("metadata") or {}).get("message") \
        or document.get("message") or ""
    if inner is not None and int(inner) != _OK:
        hint = ""
        if int(inner) == 401:
            hint = (" The agency answers a missing key, an unknown key and a "
                    "key whose subscription is not active with the same 401, "
                    "so check at disclosure2.edinet-fsa.go.jp that the key "
                    "was issued and is active.")
        raise RuntimeError(
            f"{SOURCE} answered {inner} in the body of a 200: {message}{hint}")
    return document


def collect_index(config_path, day, key=None, force=False):
    """Collect the index of one calendar day.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str
        The calendar day, as the API's ``date`` parameter.
    key: str, optional
        The API key; see :func:`fincommons.credentials.api_key`.
    force: bool
        Collect the day again even where the Bronze layer holds it. An index
        of a past day is revised by the agency once a day, so a lake that
        wants the revisions asks again; one that wants the history does not.

    Returns
    -------
    str
        What happened: ``'collected'``, ``'held'`` for a day already in the
        Bronze layer, or ``'expired'`` for one the retention window no longer
        covers.
    """
    day = day if isinstance(day, str) else day.isoformat()
    target = path_bronze_index(config_path, day)

    if not force and os.path.exists(target):
        return "held"

    if datetime.date.fromisoformat(day) < earliest_retrievable():
        # The API answers for such a day with an empty list rather than an
        # error, which a run would store as "nothing was filed that day".
        return "expired"

    document = fetch_json(INDEX_URL, {
        "date": day, "type": 2,
        "Subscription-Key": api_key("edinet", key),
    })

    write_json(target, document)
    write_json(path_index_record(config_path, day), {
        "date": day,
        "collected": _now(),
        "count": ((document.get("metadata") or {}).get("resultset")
                  or {}).get("count"),
        # The request as it was made, with the key struck out. Recording the
        # address without the parameter at all would be safe and would also
        # be a record of a request nobody made; this one says a key was sent
        # and does not say which.
        "url": redacted(f"{INDEX_URL}?date={day}&type=2"
                        f"&Subscription-Key={api_key('edinet', key)}"),
    })
    return "collected"


def collected_indexes(config_path):
    """The calendar days whose indexes the Bronze layer holds."""
    folder = path_bronze_indexes(config_path)
    if not os.path.isdir(folder):
        return []
    return sorted(name[:-5] for name in os.listdir(folder)
                  if name.endswith(".json"))


def _date(value):
    """Return a date the index states, or ``None`` where it states none."""
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _timestamp(value):
    """Return a timestamp the index states, or ``None``."""
    if not value:
        return None
    for shape in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(str(value), shape)
        except ValueError:
            continue
    return None


def filings_from_index(document, day):
    """Return the filings one day's index names, as rows of ``filings_schema``.

    The four fields that belong to the large-holding report, the takeover bid
    and the extraordinary report are dropped here rather than carried empty;
    see :data:`fincommons.financialreport.edinet.common.filings_schema`.
    """
    indexed = _date(day) or datetime.date.fromisoformat(str(day)[:10])
    rows = []
    for entry in (document.get("results") or []):
        doc = entry.get("docID")
        if not doc:
            continue
        submitted = _timestamp(entry.get("submitDateTime"))
        rows.append((
            doc,
            entry.get("edinetCode"),
            entry.get("secCode"),
            entry.get("JCN"),
            entry.get("filerName"),
            entry.get("fundCode"),
            entry.get("ordinanceCode"),
            entry.get("formCode"),
            entry.get("docTypeCode"),
            _date(entry.get("periodStart")),
            _date(entry.get("periodEnd")),
            submitted,
            (submitted.year if submitted else indexed.year),
            indexed,
            entry.get("docDescription"),
            entry.get("parentDocID"),
            entry.get("withdrawalStatus"),
            entry.get("docInfoEditStatus"),
            entry.get("disclosureStatus"),
            entry.get("legalStatus"),
            entry.get("xbrlFlag"),
            entry.get("csvFlag"),
            entry.get("englishDocFlag"),
        ))
    return rows


def read_filings(config_path, days=None):
    """The filings of the indexes the Bronze layer holds.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    days: iterable, optional
        The calendar days to read. By default every one collected.

    Returns
    -------
    list of tuple
        One row per filing, in the order of
        :data:`fincommons.financialreport.edinet.common.filings_schema`, from
        the last listing of it the indexes hold. A day
        whose index cannot be read is skipped with a warning rather than
        failing the lot of them.
    """
    wanted = [d if isinstance(d, str) else d.isoformat() for d in days] \
        if days else collected_indexes(config_path)
    # The agency lists a filing again, on the day and in the order it was
    # changed, whenever its details are edited: a form corrected, a parent
    # added. The last listing read is its present account, days being read in
    # order and a day's index being in the order of operations.
    rows = {}
    for day in sorted(wanted):
        document = read_json(path_bronze_index(config_path, day))
        if document is None:
            logger.warning(
                "The index could not be read",
                extra=context(module="edinet", layer="bronze", source=SOURCE,
                              file=f"{day}.json"),
            )
            continue
        for row in filings_from_index(document, day):
            rows.pop(row[0], None)
            rows[row[0]] = row
    return list(rows.values())


def collect_document(config_path, doc, key=None, kind="csv", force=False):
    """Collect one form of one filing.

    The agency reports a failure here by the content type rather than by the
    status: a filing arrives as an archive and an error as JSON. A body that
    is not an archive is therefore refused rather than stored, which is what
    keeps an error page out of the Bronze layer of a lake whose only copy of
    a filing this is.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    doc: str
        The document management number, e.g. ``'S100VWVY'``.
    key: str, optional
        The API key; see :func:`fincommons.credentials.api_key`.
    kind: str
        Which form to take, by
        :data:`fincommons.financialreport.edinet.common.DOCUMENT_KINDS`.
        ``'csv'`` is the agency's own conversion of the XBRL, which is what
        the Silver layer reads.
    force: bool
        Collect it again even where the Bronze layer holds it.

    Returns
    -------
    str
        ``'collected'``, ``'held'``, or ``'absent'`` where the agency has no
        such form of that filing.
    """
    if kind not in DOCUMENT_KINDS:
        raise ValueError(f"{kind!r} is no form of a filing; use one of "
                         + ", ".join(sorted(DOCUMENT_KINDS)))

    target = path_bronze_document(config_path, doc, kind)
    if not force and os.path.exists(target):
        return "held"

    status, body, answered = fetch(DOCUMENT_URL.format(doc=doc), {
        "type": DOCUMENT_KINDS[kind],
        "Subscription-Key": api_key("edinet", key),
    })

    kind_of_answer = (answered.get("Content-Type") or "").lower()
    if status >= 300:
        raise RuntimeError(f"{SOURCE} answered {status} for {doc}")
    if "json" in kind_of_answer:
        # An error, which the agency sends as JSON where a filing would be an
        # archive. A filing the agency does not hold in this form is an
        # answer rather than a failure.
        try:
            said = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            said = {}
        inner = said.get("StatusCode") or (said.get("metadata") or {}).get("status")
        if inner is not None and int(inner) == 404:
            return "absent"
        raise RuntimeError(
            f"{SOURCE} refused {doc} as {kind}: {said.get('message') or inner}")

    # Checked before it is stored, for the same reason: what arrives has to be
    # the archive it claims to be.
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            if archive.testzip() is not None:
                raise zipfile.BadZipFile("a member of the archive is corrupt")
    except zipfile.BadZipFile as error:
        raise RuntimeError(
            f"the answer for {doc} as {kind} is not a readable archive"
        ) from error

    write_bytes(target, body)
    return "collected"


def collect_filers(config_path, english=False):
    """Collect the agency's list of everyone who can file.

    The one thing in this module that needs no key. It is published as a zip
    holding a single CSV, written in the Japanese industrial encoding rather
    than in UTF-8.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    english: bool
        Take the edition whose names are in English.
    """
    status, body, _ = fetch(FILERS_URL_EN if english else FILERS_URL)
    if status >= 300:
        raise RuntimeError(f"{SOURCE} answered {status} for the filers list")
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        if not any(name.lower().endswith(".csv") for name in archive.namelist()):
            raise ValueError("the filers list holds no CSV")
    write_bytes(path_bronze_filers(config_path), body)
    return len(body)


def read_filers(config_path):
    """The filers of the list the Bronze layer holds.

    Returns
    -------
    list of tuple
        One row per filer, in the order of
        :data:`fincommons.financialreport.edinet.common.filers_schema`.

    Notes
    -----
    The first line of the CSV is not the header. The agency writes the day it
    was produced and the number of rows there -- ``ダウンロード実行日,2026年11
    月14日現在,件数,11467件`` -- and the header is the second line, so a reader
    that takes the first gets one column called "download date" and no
    filers.
    """
    path = path_bronze_filers(config_path)
    if not os.path.exists(path):
        raise RuntimeError(
            "The Bronze layer holds no list of filers. Collect it with "
            "collect_filers, which needs no API key."
        )
    with zipfile.ZipFile(path) as archive:
        name = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        text = archive.read(name).decode(FILERS_ENCODING, errors="replace")

    rows = list(csv.reader(io.StringIO(text)))
    if len(rows) < 3:
        raise ValueError("the filers list is too short to hold a header")
    width = len(filers_schema.fields)
    found = []
    for row in rows[2:]:
        if not row or not row[0].strip():
            continue
        padded = (row + [None] * width)[:width]
        found.append(tuple(value.strip() if isinstance(value, str) else value
                           for value in padded))
    return found


def update_bronze_edinet(config_path, start=None, end=None, key=None,
                         filers=True, force=False):
    """Collect the indexes of a range of days into the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    start, end: datetime.date or str, optional
        The first and last calendar day. ``end`` defaults to today and
        ``start`` to thirty days before it, which is what keeping a lake up
        to date costs; a lake building its history passes the earliest day it
        wants and is told where the retention window cuts it off.
    key: str, optional
        The API key; see :func:`fincommons.credentials.api_key`.
    filers: bool
        Collect the list of filers as well, which needs no key.
    force: bool
        Collect a day again even where the Bronze layer holds it.
    """
    end = end or datetime.date.today()
    end = end if not isinstance(end, str) else datetime.date.fromisoformat(end)
    start = start or (end - datetime.timedelta(days=30))
    start = start if not isinstance(start, str) \
        else datetime.date.fromisoformat(start)
    if start > end:
        raise ValueError(f"the range {start} to {end} ends before it begins")

    with pipeline_run(
        logger,
        "update_bronze_edinet",
        module="edinet",
        layer="bronze",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        if filers:
            outcome["filers_bytes"] = collect_filers(config_path)

        window = earliest_retrievable()
        if start < window:
            logger.warning(
                "The retention window is shorter than the range asked for: a "
                "filing leaves EDINET ten years after it was filed and "
                "cannot be fetched afterwards",
                extra=context(module="edinet", layer="bronze", source=SOURCE,
                              asked=str(start), earliest=str(window)),
            )

        counts = {"collected": 0, "held": 0, "expired": 0}
        failed = []
        day = start
        while day <= end:
            try:
                counts[collect_index(config_path, day, key, force)] += 1
            except Exception:
                failed.append(day.isoformat())
                logger.exception(
                    "The index could not be collected",
                    extra=context(module="edinet", layer="bronze",
                                  source=SOURCE, file=f"{day}.json"),
                )
            day += datetime.timedelta(days=1)

        outcome.update(counts)
        outcome["failed"] = len(failed)

        if failed:
            raise RuntimeError(
                f"{len(failed)} indexes could not be collected: "
                + ", ".join(failed[:10])
            )


#: What this module offers. Collecting the indexes, the filings and the list
#: of filers.
__all__ = [
    "MAX_WAIT",
    "RETRIES",
    "collect_document",
    "collect_filers",
    "collect_index",
    "collected_indexes",
    "fetch",
    "fetch_json",
    "filings_from_index",
    "read_filers",
    "read_filings",
    "update_bronze_edinet",
]
