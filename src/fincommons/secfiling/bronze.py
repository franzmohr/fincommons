"""SEC filings: the company files in the Bronze layer.

The Commission publishes, for every registrant that has tagged anything, one
JSON file holding the facts of all its filings. The Bronze layer keeps those
files as they were published, one per registrant, and beside them a small
record of when each was collected and what the server said, which is how the
next run knows what it need not download again.

Two ways in, and which to use follows from how many registrants are wanted.
A lake interested in a few dozen companies asks for one file each. A lake
interested in the whole population takes the archive of all of them, which is
about 1.4 gigabytes and is what the Commission asks a client wanting many
companies to take: ten thousand requests for ten thousand companies is the
thing the archive exists to prevent.

The Commission requires a client to identify itself and enforces it. A
request that carries no contact address is answered with ``403 Forbidden``
and no file -- not with an error a careless run would notice, but with a body
a run that did not check the status would store as the data. ``contact`` is
therefore not optional here, which is the one place this module is stricter
than :mod:`fincommons.esef`: filings.xbrl.org asks, and the Commission
refuses.

Each company file is collected on its own. One that fails -- a download cut
off, an answer that is not a company file -- is logged with its traceback and
counted, and the others carry on.
"""

import datetime
import io
import json
import logging
import os
import time
import zipfile

import requests

from ..log import context, pipeline_run
from ..marketpricedata.common import HTTP_TIMEOUT, USER_AGENT
from .common import (BULK_URL, COMPANY_URL, SOURCE, TICKERS_URL,
                     path_bronze_companies, path_bronze_company,
                     path_company_record, read_json, write_bytes, write_json)
from .parse import padded_cik

logger = logging.getLogger(__name__)

#: How often a request the Commission turns away for the moment is made
#: again, and the longest it is waited for. The Commission limits a client to
#: ten requests a second and answers one that asks faster with ``429 Too Many
#: Requests``.
RETRIES = 5
MAX_WAIT = 120

#: The answers that say "not now" rather than "not at all".
_RETRY_STATUS = {429, 503}


def user_agent(contact):
    """The user agent of a request, naming the contact.

    Raises
    ------
    ValueError
        Where no contact is given. The Commission answers an anonymous
        request with 403 and no file, so a run without one collects nothing
        and would have to be told why; it is better told here, before the
        first request, than by an error page stored as a company file.
    """
    contact = (contact or "").strip()
    if not contact:
        raise ValueError(
            "The Commission requires a client to identify itself and refuses "
            "a request that does not. Give a contact address, for instance "
            "contact='reporting@example.org'."
        )
    return f"{USER_AGENT} ({contact})"


def _wait(response, attempt):
    """How long to wait before asking again: what the server says, or longer
    each time, but never more than :data:`MAX_WAIT` seconds."""
    try:
        wait = float(response.headers.get("Retry-After"))
    except (TypeError, ValueError):
        wait = 2.0 ** (attempt + 1)
    return min(max(wait, 1.0), MAX_WAIT)


def fetch(url, contact, headers=None):
    """One answer of the Commission: its status, its body and its headers.

    The body of an answer with an error status is not read. A request turned
    away for the moment, with ``429`` or ``503``, is made again after the wait
    it asks for, up to :data:`RETRIES` times.
    """
    agent = user_agent(contact)
    wait = 1.0
    for attempt in range(RETRIES + 1):
        response = requests.get(
            url,
            headers={"User-Agent": agent, "Accept-Encoding": "gzip, deflate",
                     **(headers or {})},
            stream=True,
            timeout=HTTP_TIMEOUT,
        )
        try:
            if response.status_code in _RETRY_STATUS and attempt < RETRIES:
                wait = _wait(response, attempt)
                logger.debug(
                    "The Commission asked to wait, the request is made again",
                    extra=context(module="secfiling", layer="bronze", url=url,
                                  status=response.status_code, wait=wait),
                )
            elif response.status_code >= 300:
                return response.status_code, b"", dict(response.headers)
            else:
                body = b"".join(response.iter_content(chunk_size=1 << 16))
                return response.status_code, body, dict(response.headers)
        finally:
            response.close()
        time.sleep(wait)


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def collect_company(config_path, cik, contact, force=False):
    """Collect the company file of one registrant.

    The file is asked for with the ``Last-Modified`` of the last answer as
    ``If-Modified-Since``, so a company whose filings have not changed comes
    back as ``304 Not Modified`` without a byte.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    cik: str or int
        The central index key of the registrant.
    contact: str
        A contact address; see :func:`user_agent`.
    force: bool
        Ask for the file even where the record says it has not changed.

    Returns
    -------
    str
        What happened: ``'collected'``, ``'unchanged'``, or ``'absent'`` for a
        registrant the Commission holds no facts for, which is every company
        that has never tagged a filing.
    """
    key = padded_cik(cik)
    target = path_bronze_company(config_path, key)
    record = read_json(path_company_record(config_path, key)) or {}

    headers = {}
    if not force and record.get("last_modified") and os.path.exists(target):
        headers["If-Modified-Since"] = record["last_modified"]

    status, body, answered = fetch(COMPANY_URL.format(cik=key), contact, headers)

    if status == 304:
        return "unchanged"
    if status == 404:
        # Not every registrant has tagged a filing. The Commission says so
        # with 404, which is an answer and not a failure.
        write_json(path_company_record(config_path, key),
                   {"cik": key, "status": status, "collected": _now()})
        return "absent"
    if status >= 300:
        raise RuntimeError(f"{SOURCE} answered {status} for CIK{key}")

    # Checked before it is stored: an answer that is not a company file is
    # the shape a server error takes when it comes back with status 200.
    document = json.loads(body.decode("utf-8"))
    if "facts" not in document or "cik" not in document:
        raise ValueError(f"the answer for CIK{key} is not a company file")

    write_bytes(target, body)
    write_json(path_company_record(config_path, key), {
        "cik": key,
        "status": status,
        "collected": _now(),
        "last_modified": answered.get("Last-Modified"),
        "bytes": len(body),
    })
    return "collected"


def collect_bulk(config_path, contact, ciks=None):
    """Collect every company file at once, from the archive of all of them.

    One request of about 1.4 gigabytes rather than one per registrant, which
    is what the Commission asks a client wanting many companies to make. The
    archive is unpacked straight from memory into the Bronze layer; where
    ``ciks`` is given only those are written, which is how a lake takes the
    archive once and keeps the part of it that it wants.

    No record of a ``Last-Modified`` is written for a file that came out of
    the archive, because the archive's date is not the file's: the next run
    asking for that company on its own would be told it had not changed when
    it may have.
    """
    wanted = {padded_cik(cik) for cik in ciks} if ciks else None
    status, body, _ = fetch(BULK_URL, contact)
    if status >= 300:
        raise RuntimeError(f"{SOURCE} answered {status} for the bulk archive")

    written = 0
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        for name in archive.namelist():
            base = os.path.basename(name)
            if not base.startswith("CIK") or not base.endswith(".json"):
                continue
            key = base[3:-5]
            if not key.isdigit() or (wanted is not None and key not in wanted):
                continue
            write_bytes(path_bronze_company(config_path, key), archive.read(name))
            write_json(path_company_record(config_path, key),
                       {"cik": key, "status": 200, "collected": _now(),
                        "source": "bulk"})
            written += 1
    return written


def collected_companies(config_path):
    """The central index keys of the company files the Bronze layer holds."""
    folder = path_bronze_companies(config_path)
    if not os.path.isdir(folder):
        return []
    return sorted(
        name[3:-5] for name in os.listdir(folder)
        if name.startswith("CIK") and name.endswith(".json")
    )


def registrants(contact):
    """The registrants the Commission lists, as central index key and name.

    How a lake that wants companies by ticker or by name finds their keys.
    """
    status, body, _ = fetch(TICKERS_URL, contact)
    if status >= 300:
        raise RuntimeError(f"{SOURCE} answered {status} for the list of registrants")
    listed = json.loads(body.decode("utf-8"))
    return [{"cik": padded_cik(row["cik_str"]), "ticker": row.get("ticker"),
             "name": row.get("title")}
            for row in listed.values()]


def update_bronze_secfiling(config_path, ciks=None, contact=None, bulk=False,
                            force=False):
    """Collect the company files into the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    ciks: iterable, optional
        The registrants to collect. Required unless ``bulk`` is asked for,
        which collects every registrant there is.
    contact: str
        A contact address; see :func:`user_agent`.
    bulk: bool
        Take the archive of every company file rather than one request per
        registrant. Where ``ciks`` is given as well, the archive is taken and
        only those registrants are kept.
    force: bool
        Collect a file even where the record says it has not changed.
    """
    with pipeline_run(
        logger,
        "update_bronze_secfiling",
        module="secfiling",
        layer="bronze",
        source=SOURCE,
        config_path=config_path,
    ) as outcome:
        if bulk:
            outcome["collected"] = collect_bulk(config_path, contact, ciks)
            return

        if not ciks:
            raise ValueError(
                "Name the registrants to collect, or ask for bulk=True to "
                "take every one of them in a single archive."
            )

        counts = {"collected": 0, "unchanged": 0, "absent": 0}
        failed = []
        for cik in ciks:
            key = padded_cik(cik)
            try:
                counts[collect_company(config_path, key, contact, force)] += 1
            except Exception:
                failed.append(key)
                logger.exception(
                    "The company file could not be collected",
                    extra=context(module="secfiling", layer="bronze",
                                  source=SOURCE, file=f"CIK{key}.json"),
                )
        outcome.update(counts)
        outcome["failed"] = len(failed)

        if failed:
            raise RuntimeError(
                f"{len(failed)} company files could not be collected: "
                + ", ".join(failed[:10])
            )


#: What this module offers. Collecting the company files of the Commission.
__all__ = [
    "MAX_WAIT",
    "RETRIES",
    "collect_bulk",
    "collect_company",
    "collected_companies",
    "fetch",
    "registrants",
    "update_bronze_secfiling",
    "user_agent",
]
