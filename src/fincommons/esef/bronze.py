"""ESEF filings: the indexes and the reports in the Bronze layer.

filings.xbrl.org is a repository of the annual financial reports that issuers
on European regulated markets file in the European Single Electronic Format,
run by XBRL International, together with the reports of the United Kingdom
and of Ukraine filed in the same way. For each filing it publishes the report
package as filed, the report in a viewer, and the facts the report tags as
xBRL-JSON. The Bronze layer keeps two of them as they were published.

**The indexes.** The repository lists its filings in one index file per year
of the period end, which it asks a client that keeps a copy of the index to
read rather than every page of its API, and to ask for with the
``Last-Modified`` of the last answer as ``If-Modified-Since``: an index that
has not changed comes back as ``304 Not Modified`` without a byte. Every
index of every year is collected, since all of them together are a few tens
of megabytes and the table of filings is the catalogue of what there is to
collect.

**The reports.** The xBRL-JSON of each filing of the countries asked for is
downloaded once and again only where the index says the repository has
processed the filing since, which is what it does when it converts a filing
anew. A report is between a few hundred kilobytes and some tens of
megabytes, most of it the notes as text blocks, so a country is the unit a
run is asked to collect rather than the whole repository. The report
packages, the filing as it was made, are collected only where they are asked
for.

The repository asks a client that uses it routinely to name a contact in its
user agent; ``contact`` is how a run does so.

Each report is collected on its own. One that fails -- a download cut off,
an answer that is not a report -- is logged with its traceback and counted,
and the others carry on, as in :mod:`fincommons.marketpricedata.pipeline`.
"""

import datetime
import hashlib
import json
import logging
import os
import time
from urllib.parse import urlparse

import requests

from ..log import context, pipeline_run
from ..marketpricedata.common import HTTP_TIMEOUT, USER_AGENT
from .common import (BASE_URL, FIRST_YEAR, INDEX_URL, SOURCE,
                     path_bronze_index, path_bronze_indexes,
                     path_bronze_packages,
                     path_bronze_report, path_index_record,
                     path_report_record, read_json, write_bytes, write_json)
from .parse import filings_from_index

logger = logging.getLogger(__name__)


def user_agent(contact=None):
    """The user agent of a request, naming the contact where one is given."""
    return f"{USER_AGENT} ({contact})" if contact else USER_AGENT


#: How often a request the repository turns away for the moment is made
#: again, and the longest it is waited for. The repository limits the rate of
#: requests, and answers a client that asks too quickly with ``429 Too Many
#: Requests``; the first collection into a real lake was turned away for six
#: of its nine indexes.
RETRIES = 5
MAX_WAIT = 120

#: The answers that say "not now" rather than "not at all".
_RETRY_STATUS = {429, 503}


def _wait(resp, attempt):
    """How long to wait before asking again: what the server says, or longer
    each time, but never more than :data:`MAX_WAIT` seconds."""
    try:
        wait = float(resp.headers.get("Retry-After"))
    except (TypeError, ValueError):
        wait = 2.0 ** (attempt + 1)
    return min(max(wait, 1.0), MAX_WAIT)


def fetch(url, contact=None, headers=None):
    """One answer of the repository: its status, its body and its headers.

    The body of an answer with an error status is not read. The request
    carries a user agent and a timeout, as every request of the package does;
    see :data:`fincommons.marketpricedata.common.HTTP_TIMEOUT`. A request the
    repository turns away for the moment, with ``429`` or ``503``, is made
    again after the wait it asks for, up to :data:`RETRIES` times.
    """
    for attempt in range(RETRIES + 1):
        resp = requests.get(
            url,
            headers={"User-Agent": user_agent(contact), **(headers or {})},
            stream=True,
            timeout=HTTP_TIMEOUT,
        )
        try:
            if resp.status_code in _RETRY_STATUS and attempt < RETRIES:
                wait = _wait(resp, attempt)
                logger.debug(
                    "The repository asked to wait, the request is made again",
                    extra=context(module="esef", layer="bronze", url=url,
                                  status=resp.status_code, wait=wait),
                )
            elif resp.status_code >= 300:
                return resp.status_code, b"", dict(resp.headers)
            else:
                body = b"".join(resp.iter_content(chunk_size=1 << 16))
                return resp.status_code, body, dict(resp.headers)
        finally:
            resp.close()
        time.sleep(wait)


def file_url(path):
    """The address of a file of a filing, which the index gives relative.

    Raises
    ------
    ValueError
        If the address leaves the repository. The index is read from the
        network, and what it points to is written into the Bronze layer.
    """
    url = BASE_URL + path if path.startswith("/") else path
    if urlparse(url).netloc != urlparse(BASE_URL).netloc:
        raise ValueError(f"The address {path!r} is not one of {SOURCE}.")
    return url


def current_year(now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return now.year


def collect_index(config_path, year, contact=None, now=None):
    """Bring the index of one year up to date in the Bronze layer.

    Returns
    -------
    str
        ``"collected"`` where the index was written, ``"unchanged"`` where the
        server says it has not changed since it was collected, or where its
        text is the one the Bronze layer holds, and ``"absent"`` where the
        repository has no index for the year.
    """
    now = now or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    target = path_bronze_index(config_path, year)
    previous = read_json(path_index_record(config_path, year))
    present = os.path.exists(target)

    headers = {}
    if present and previous and previous.get("last_modified"):
        headers["If-Modified-Since"] = previous["last_modified"]
    url = INDEX_URL.format(year=year)
    status, body, answer = fetch(url, contact, headers)
    if status == 304:
        return "unchanged"
    if status == 404:
        return "absent"
    if status >= 300:
        raise requests.HTTPError(f"{status} for {url}")

    # An answer that is not an index -- an error page sent with status 200 --
    # is refused before it is written over the index the layer holds.
    filings = filings_from_index(json.loads(body))
    sha256 = hashlib.sha256(body).hexdigest()
    record = {
        "year": year,
        "url": url,
        "last_modified": answer.get("Last-Modified"),
        "sha256": sha256,
        "filings": len(filings),
        "collected": now.isoformat(timespec="seconds"),
    }
    if present and previous and previous.get("sha256") == sha256:
        record["collected"] = previous["collected"]
        write_json(path_index_record(config_path, year), record)
        return "unchanged"

    write_bytes(target, body)
    # The record is written after the file, so that a run that dies between
    # the two collects the index again rather than trusting a record of one
    # that never arrived.
    write_json(path_index_record(config_path, year), record)
    logger.info(
        "Collected an index into the Bronze layer",
        extra=context(module="esef", layer="bronze", source=f"index/{year}",
                      filings=len(filings)),
    )
    return "collected"


def read_filings(config_path, years=None):
    """The filings the indexes of the Bronze layer list, by filing.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    years: list of int, optional
        The years of the period end whose index is read; every index the
        layer holds where left out.

    Returns
    -------
    dict
        The record of each filing, see
        :func:`fincommons.esef.parse.filings_from_index`, in order of the
        filing.
    """
    folder = path_bronze_indexes(config_path)
    if not os.path.isdir(folder):
        return {}
    wanted = None if years is None else {int(y) for y in years}
    filings = {}
    for name in sorted(os.listdir(folder)):
        if not (name.startswith("filings-") and name.endswith(".json")):
            continue
        year = name[len("filings-"):-len(".json")]
        if not year.isdigit() or (wanted is not None and int(year) not in wanted):
            continue
        with open(os.path.join(folder, name), encoding="utf-8") as handle:
            for record in filings_from_index(json.load(handle)):
                filings[record["FILING"]] = record
    return dict(sorted(filings.items()))


def _stamp(value):
    return None if value is None else value.isoformat()


def collect_report(config_path, filing, contact=None, now=None):
    """Bring the report of one filing up to date in the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    filing: dict
        The record of the filing, see :func:`read_filings`.
    contact: str, optional
        A contact the user agent names.
    now: datetime.datetime, optional
        The time of the collection, in UTC. The present where left out.

    Returns
    -------
    str
        ``"collected"`` where the report was written, ``"unchanged"`` where it
        was not because the repository has not processed the filing since it
        was collected.
    """
    now = now or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    name, country = filing["FILING"], filing["COUNTRY"]
    target = path_bronze_report(config_path, country, name)
    previous = read_json(path_report_record(config_path, name))
    processed = _stamp(filing["PROCESSED"])
    if (os.path.exists(target) and previous
            and previous.get("processed") == processed
            and previous.get("json_url") == filing["JSONURL"]):
        return "unchanged"

    url = file_url(filing["JSONURL"])
    status, body, _ = fetch(url, contact)
    if status >= 300:
        raise requests.HTTPError(f"{status} for {url}")
    document = json.loads(body)
    if not isinstance(document, dict) or not isinstance(document.get("facts"), dict):
        raise ValueError(
            f"The answer for {name} is not a report in xBRL-JSON. It is not "
            "written over the Bronze layer, which keeps the report it held."
        )

    write_bytes(target, body)
    write_json(path_report_record(config_path, name), {
        "filing": name,
        "country": country,
        "json_url": filing["JSONURL"],
        "processed": processed,
        "sha256": hashlib.sha256(body).hexdigest(),
        "facts": len(document["facts"]),
        "collected": now.isoformat(timespec="seconds"),
    })
    logger.info(
        "Collected a report into the Bronze layer",
        extra=context(module="esef", layer="bronze", source=name,
                      facts=len(document["facts"])),
    )
    return "collected"


def collect_package(config_path, filing, contact=None):
    """Download the report package of one filing, unless the layer holds it.

    The package is kept under the name of the filing and the extension the
    repository gives it, ``.zip`` or ``.xbri``, and is downloaded again where
    its SHA-256 in the index is not that of the file the layer holds.

    Returns
    -------
    str
        ``"collected"`` or ``"unchanged"``.
    """
    path = filing["PACKAGEURL"]
    extension = os.path.splitext(urlparse(path).path)[1] or ".zip"
    target = os.path.join(path_bronze_packages(config_path),
                          f"COUNTRY={filing['COUNTRY']}",
                          f"{filing['FILING']}{extension}")
    expected = filing.get("PACKAGESHA256")
    if os.path.exists(target) and expected:
        with open(target, "rb") as handle:
            if hashlib.sha256(handle.read()).hexdigest() == expected:
                return "unchanged"

    url = file_url(path)
    status, body, _ = fetch(url, contact)
    if status >= 300:
        raise requests.HTTPError(f"{status} for {url}")
    if expected and hashlib.sha256(body).hexdigest() != expected:
        raise ValueError(
            f"The package of {filing['FILING']} does not have the SHA-256 the "
            "index gives for it. It is not written into the Bronze layer."
        )
    write_bytes(target, body)
    return "collected"


def update_bronze_esef(config_path, countries, years=None, packages=False,
                       contact=None):
    """Collect the indexes and the reports of some countries into the Bronze layer.

    Every index is brought up to date, and then the report of each filing of
    the countries asked for whose report the Bronze layer does not hold, or
    holds from before the repository last processed the filing; see the
    module docstring. A report that fails is logged with its traceback at
    ``ERROR``, counted, and the next one runs.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    countries: list of str
        The countries whose reports are collected, as the repository codes
        them, ``["AT", "DE"]``.
    years: list of int, optional
        Collect only the reports of filings whose period ends in these years.
        Every year where left out. The indexes of every year are collected
        all the same.
    packages: bool
        Collect the report packages as well.
    contact: str, optional
        A contact the user agent names, an e-mail address or a web page, as
        the repository asks of a client that uses it routinely.

    Returns
    -------
    dict
        The outcome of the stage: ``indexes`` counts the indexes written,
        ``collected`` and ``unchanged`` the reports written and skipped,
        ``unavailable`` the filings the repository has no report for,
        ``failed`` the indexes and reports that failed and
        ``failed_sources`` names them as ``bronze/<name>``.

    Examples
    --------
    >>> outcome = update_bronze_esef("path/to/configuration/file", ["AT"])
    >>> outcome["failed"]
    0
    """
    countries = [str(c).upper() for c in countries]
    with pipeline_run(
        logger,
        "update_bronze_esef",
        module="esef",
        layer="bronze",
        config_path=config_path,
        countries=",".join(countries),
    ) as outcome:
        outcome.update(indexes=0, collected=0, unchanged=0, unavailable=0,
                       packages=0, failed=0, failed_sources=[])

        for year in range(FIRST_YEAR, current_year() + 1):
            try:
                if collect_index(config_path, year, contact) == "collected":
                    outcome["indexes"] += 1
            except Exception:
                logger.exception(
                    "An index failed and was set aside",
                    extra=context(module="esef", layer="bronze",
                                  source=f"index/{year}"),
                )
                outcome["failed"] += 1
                outcome["failed_sources"].append(f"bronze/index/{year}")

        wanted = None if years is None else {int(y) for y in years}
        for filing in read_filings(config_path).values():
            if filing["COUNTRY"] not in countries:
                continue
            if wanted is not None and filing["PERIODEND"].year not in wanted:
                continue
            name = filing["FILING"]
            try:
                if filing["JSONURL"]:
                    outcome[collect_report(config_path, filing, contact)] += 1
                else:
                    outcome["unavailable"] += 1
                if packages and filing["PACKAGEURL"]:
                    if collect_package(config_path, filing, contact) == "collected":
                        outcome["packages"] += 1
            except Exception:
                logger.exception(
                    "A filing failed and was set aside",
                    extra=context(module="esef", layer="bronze", source=name),
                )
                outcome["failed"] += 1
                outcome["failed_sources"].append(f"bronze/{name}")

        if outcome["failed"]:
            logger.warning(
                "Indexes or reports were set aside, the run did not update them",
                extra=context(module="esef", layer="bronze",
                              failed=outcome["failed"],
                              sources=",".join(outcome["failed_sources"])),
            )
    return outcome


__all__ = [
    "collect_index",
    "collect_package",
    "collect_report",
    "fetch",
    "file_url",
    "read_filings",
    "update_bronze_esef",
    "user_agent",
]
