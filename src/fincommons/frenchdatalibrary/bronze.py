"""The Kenneth R. French Data Library: the files in the Bronze layer.

The library offers some three hundred files, each a zipped CSV file linked
from one page. The Bronze layer keeps the text of every one of them as it was
published -- the preamble, the notes and the copyright line included, one row
per line -- because the preamble is where a file says what it holds: which
vintage of CRSP it was built from, how its portfolios are formed and which
value stands for a missing one. Nothing is read out of a file here; that is
the work of the Silver layer, which can be redone from what is kept here.

**A file that has not changed is not downloaded again.** The library is
rebuilt once a month, and the whole of it is about 150 MB zipped, so a run
that fetched everything every time would fetch the same 150 MB thirty times
a month. The server states when a file was last modified and gives it an
entity tag, and both are kept with the file; a run asks for them first, with
a ``HEAD`` request, and downloads the file only where they differ. Where the
server does not answer that question, the file is downloaded, and its text is
compared with the one kept by its SHA-256, so that an unchanged file is not
written again and does not send the Silver layer to parse it again.

Each file is collected on its own. One that fails -- a download cut off, an
archive that is not one -- is logged with its traceback and counted, and the
others carry on, as in :mod:`fincommons.marketpricedata.pipeline`.
"""

import datetime
import email.utils
import hashlib
import logging
import os
import re
import zipfile
from urllib.parse import urljoin, urlparse

import pyarrow
import pyarrow.parquet
import requests

from ..log import context, pipeline_run
from ..marketpricedata.common import HTTP_TIMEOUT, USER_AGENT
from ..marketpricedata.download import downloaded
from .common import (LIBRARY_URL, SOURCE, path_bronze_collections,
                     path_bronze_dataset, read_collection, write_json)
from .parse import decode, split_lines

logger = logging.getLogger(__name__)

#: A link to a file of the library, as the page writes it:
#: ``href="ftp/F-F_Research_Data_Factors_CSV.zip"``. Each file is offered as
#: CSV and as text; only the CSV is collected.
_LINK = re.compile(r'href\s*=\s*"([^"]*?([^/"]+)_CSV\.zip)"', re.IGNORECASE)

#: What a name taken from the page may look like. The name becomes the name of
#: a directory in the Bronze layer and a value in a SQL predicate of the
#: Silver write, and the page is read from the network, so a name that could
#: be more than a name -- a ``..``, a slash, a quote -- is refused rather than
#: used.
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")


def parse_index(html, base_url=LIBRARY_URL):
    """The files the page of the library links, by name.

    Parameters
    ----------
    html: str
        The page.
    base_url: str
        The address of the page, which the links are relative to.

    Returns
    -------
    dict
        The address of each zipped CSV file, by the name of the file without
        the ``_CSV.zip`` suffix, ``F-F_Research_Data_Factors``, in order of
        the name.

    Raises
    ------
    ValueError
        If the page links no file. That is not a library that has been
        emptied but a page that is not the library -- an error page served
        with status 200, a page that has moved -- and a run that took it at
        its word would report success having collected nothing.
    """
    files = {}
    for href, name in _LINK.findall(html):
        if not _NAME.match(name):
            logger.warning(
                "A link of the library names a file that is not a plain name, "
                "it is left out",
                extra=context(module="frenchdatalibrary", layer="bronze",
                              file=name),
            )
            continue
        url = urljoin(base_url, href)
        if urlparse(url).netloc != urlparse(base_url).netloc:
            logger.warning(
                "A link of the library points to another server, it is left out",
                extra=context(module="frenchdatalibrary", layer="bronze",
                              file=name, url=url),
            )
            continue
        files.setdefault(name, url)
    if not files:
        raise ValueError(
            f"The page {base_url} links no zipped CSV file of the library. It "
            "is not taken for a library without files; nothing was collected."
        )
    return dict(sorted(files.items()))


def library_index(url=LIBRARY_URL):
    """Read the page of the library and return the files it links.

    See :func:`parse_index`.
    """
    with downloaded(url, SOURCE, file_name="data_library.html") as path:
        with open(path, "rb") as handle:
            html = decode(handle.read())
    return parse_index(html, url)


def probe(url):
    """The ``Last-Modified`` and ``ETag`` the server gives for a file.

    Asked with a ``HEAD`` request, which costs a few hundred bytes where the
    file costs up to sixteen megabytes. ``(None, None)`` where the server does
    not answer the request or names neither, which sends the caller to
    download the file and compare its text instead; a server that cannot be
    reached is found out by the download that follows.
    """
    try:
        resp = requests.head(url, headers={"User-Agent": USER_AGENT},
                             timeout=HTTP_TIMEOUT, allow_redirects=True)
    except requests.RequestException:
        return None, None
    try:
        if resp.status_code >= 400:
            return None, None
        return resp.headers.get("Last-Modified"), resp.headers.get("ETag")
    finally:
        resp.close()


def unchanged(previous, last_modified, etag):
    """Whether the server's answer says a file is the one collected before.

    The entity tag decides where both sides have one, because it changes with
    the content; the time of the last modification decides where there is no
    tag. Where neither is known the answer is no, and the file is downloaded.
    """
    if not previous:
        return False
    if etag and previous.get("etag"):
        return etag == previous["etag"]
    if last_modified and previous.get("last_modified"):
        return last_modified == previous["last_modified"]
    return False


def read_archive(path, dataset):
    """The text of the one CSV file a zip archive of the library holds.

    Raises
    ------
    ValueError
        If the archive does not hold exactly one CSV file. The library packs
        one file per archive, and an archive with none or several is not one
        the Bronze layer knows how to file under the name it was linked as.
    """
    try:
        with zipfile.ZipFile(path) as archive:
            members = [m for m in archive.namelist()
                       if m.lower().endswith(".csv") and not m.endswith("/")]
            if len(members) != 1:
                raise ValueError(
                    f"The archive of {dataset} holds {len(members)} CSV files "
                    f"{members}, where one was expected."
                )
            return decode(archive.read(members[0]))
    except zipfile.BadZipFile as err:
        raise ValueError(
            f"The answer for {dataset} is not a zip archive. It is not written "
            "over the Bronze layer, which keeps the file it held."
        ) from err


def http_date(value):
    """An HTTP date as a naive UTC timestamp, or None where there is none."""
    if not value:
        return None
    try:
        stamp = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if stamp.tzinfo is not None:
        stamp = stamp.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return stamp


def write_dataset(config_path, dataset, lines, last_modified, collected):
    """Replace the partition of one file in the Bronze layer with its lines.

    The partition is one parquet file, written under a name that readers pass
    over -- Spark and Arrow skip a file whose name begins with a dot -- and
    put in place once complete, so that a run that dies while writing leaves
    the file collected before rather than half of the new one.

    The name of the file is the name of the partition directory,
    ``DATASET=...``, and is not repeated inside it: Spark refuses a column
    that is both in the files and in the directory names.
    """
    folder = path_bronze_dataset(config_path, dataset)
    os.makedirs(folder, exist_ok=True)
    stamp = http_date(last_modified)
    table = pyarrow.table({
        "LINENO": pyarrow.array(range(1, len(lines) + 1), pyarrow.int32()),
        "LINE": pyarrow.array(lines, pyarrow.string()),
        "LASTMODIFIED": pyarrow.array([stamp] * len(lines), pyarrow.timestamp("us")),
        "COLLECTDT": pyarrow.array([collected.date()] * len(lines), pyarrow.date32()),
    })
    target = os.path.join(folder, "part-00000.parquet")
    partial = os.path.join(folder, ".part-00000.parquet.partial")
    try:
        pyarrow.parquet.write_table(table, partial)
        os.replace(partial, target)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


def collect_dataset(config_path, dataset, url, now=None):
    """Bring one file of the library up to date in the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    dataset: str
        The name of the file, ``F-F_Research_Data_Factors``.
    url: str
        Its address.
    now: datetime.datetime, optional
        The time of the collection, in UTC. The present where left out.

    Returns
    -------
    str
        ``"collected"`` where the file was written, ``"unchanged"`` where it
        was not because it is the one the Bronze layer holds.
    """
    now = now or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    previous = read_collection(config_path, dataset)
    present = os.path.isdir(path_bronze_dataset(config_path, dataset))

    last_modified, etag = probe(url)
    if present and unchanged(previous, last_modified, etag):
        logger.debug(
            "The file has not changed since it was collected, it is skipped",
            extra=context(module="frenchdatalibrary", layer="bronze",
                          source=dataset),
        )
        return "unchanged"

    with downloaded(url, SOURCE, file_name=f"{dataset}_CSV.zip") as path:
        text = read_archive(path, dataset)
    lines = split_lines(text)
    if not any(line.strip() for line in lines):
        raise ValueError(
            f"The file {dataset} holds no text. It is not written over the "
            "Bronze layer, which keeps the file it held."
        )
    sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()

    record = {
        "dataset": dataset,
        "url": url,
        "last_modified": last_modified,
        "etag": etag,
        "sha256": sha256,
        "lines": len(lines),
        "collected": now.isoformat(timespec="seconds"),
    }
    if present and previous and previous.get("sha256") == sha256:
        # The same text under a new date or tag. The record is brought up to
        # date, so that the next run's HEAD request finds it unchanged, but
        # the text and the time it was collected stay what they were: the
        # Silver layer has nothing new to read.
        record["collected"] = previous["collected"]
        write_json(os.path.join(path_bronze_collections(config_path),
                                f"{dataset}.json"), record)
        return "unchanged"

    write_dataset(config_path, dataset, lines, last_modified, now)
    # The record is written after the file, so that a run that dies between
    # the two leaves a record of the previous collection, and the next run
    # collects the file again rather than trusting a record of one that never
    # arrived.
    write_json(os.path.join(path_bronze_collections(config_path),
                            f"{dataset}.json"), record)
    logger.info(
        "Collected a file into the Bronze layer",
        extra=context(module="frenchdatalibrary", layer="bronze",
                      source=dataset, rows=len(lines),
                      last_modified=last_modified),
    )
    return "collected"


def update_bronze_frenchdatalibrary(config_path, datasets=None, url=LIBRARY_URL):
    """Collect the files of the library into the Bronze layer.

    The page of the library is read for the files it offers, and each of them
    that has changed since it was last collected is downloaded; see the module
    docstring. A file that fails is logged with its traceback at ``ERROR``,
    counted, and the next one runs.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    datasets: list of str, optional
        The files to collect, by name, ``["F-F_Research_Data_Factors"]``.
        Every file the page links where left out. A name the page does not
        link is counted as failed, since it is a file that was asked for and
        not collected.
    url: str
        The page of the library.

    Returns
    -------
    dict
        The outcome of the stage: ``collected`` and ``unchanged`` count the
        files written and skipped, ``failed`` the files that failed and
        ``failed_sources`` names them as ``bronze/<name>``.

    Raises
    ------
    ValueError
        If the page links no file; see :func:`parse_index`.

    Examples
    --------
    >>> outcome = update_bronze_frenchdatalibrary("path/to/configuration/file")
    >>> outcome["failed"]
    0
    """
    with pipeline_run(
        logger,
        "update_bronze_frenchdatalibrary",
        module="frenchdatalibrary",
        layer="bronze",
        config_path=config_path,
    ) as outcome:
        outcome.update(collected=0, unchanged=0, failed=0, failed_sources=[])
        files = library_index(url)
        wanted = list(files) if datasets is None else list(datasets)

        for dataset in wanted:
            try:
                if dataset not in files:
                    raise LookupError(
                        f"The library offers no file {dataset!r}. Its page "
                        f"{url} links {len(files)} files."
                    )
                outcome[collect_dataset(config_path, dataset, files[dataset])] += 1
            except Exception:
                logger.exception(
                    "A file failed and was set aside",
                    extra=context(module="frenchdatalibrary", layer="bronze",
                                  source=dataset),
                )
                outcome["failed"] += 1
                outcome["failed_sources"].append(f"bronze/{dataset}")

        if outcome["failed"]:
            logger.warning(
                "Files were set aside, the run did not update them",
                extra=context(module="frenchdatalibrary", layer="bronze",
                              failed=outcome["failed"],
                              sources=",".join(outcome["failed_sources"])),
            )
    return outcome


__all__ = [
    "collect_dataset",
    "library_index",
    "parse_index",
    "probe",
    "read_archive",
    "update_bronze_frenchdatalibrary",
]
