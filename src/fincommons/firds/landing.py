
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
import polars as pl
import requests
import zipfile

from . import common
from .utilities import parse_file_name
from ..log import context, pipeline_run

logger = logging.getLogger(__name__)


#: How many files the register is asked for at a time.
PAGE_SIZE = 1000

#: The fields of the register's answer the module keeps. ``checksum`` is the
#: MD5 digest of the file.
COLUMNS = ["file_type", "publication_date", "id", "file_name", "download_link", "checksum"]


def url_for_available_files(date_from, date_to, start=0, rows=PAGE_SIZE):
    """Create the URL to query the available files.

    Generates an URL for one page of the answer. The answer is sorted by
    publication date and identifier, so that the pages follow one another
    without overlapping.

    Parameters
    ----------
    date_from: str
        First date to be considered for download. Must have the format YYYY-MM-DD.
    date_to: str
        Last date to be considered for download. Must have the format YYYY-MM-DD.
    start: int
        Position of the first file of the page.
    rows: int
        Number of files on the page.
    """
    result = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B" + date_from + "T00:00:00Z+TO+" + date_to + "T23:59:59Z%5D&wt=json&sort=publication_date+asc,id+asc" + f"&start={start}&rows={rows}"
    return result


#: How long a request to ESMA may wait, in seconds: to connect, and between
#: two chunks of an answer. Without a limit a stalled connection holds the
#: daily job indefinitely.
TIMEOUT = (30, 300)

#: The suffix a file carries while it is being downloaded. The name the file
#: is finally stored under is only given to it once it is complete.
PARTIAL_SUFFIX = ".part"

#: How many days before the watermark the register is asked about again.
#:
#: The watermark is the last day up to which every file needed has been
#: imported, and the days before it are settled -- as far as the register said
#: at the time. A file published late, after the day it is dated, is not among
#: the answer the watermark was computed from, so without looking back the
#: window would begin after it and it would never be fetched. Looking back a
#: week costs one page of the answer and catches a file published up to that
#: late; one published later than that is beyond what this can repair.
LOOKBACK_DAYS = 7


def get_downloadable_files(date_from, date_to):
    """Get list of downloadable files.

    Extracts a list of downloadable content, reading the register's answer
    page by page.

    Parameters
    ----------
    date_from: str
        First date to be considered for download. Must have the format YYYY-MM-DD.
    date_to: str
        Last date to be considered for download. Must have the format YYYY-MM-DD.
    """
    docs = []
    while True:
        url = url_for_available_files(date_from, date_to, start=len(docs))
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        body = response.json()["response"]
        page = body["docs"]
        found = body.get("numFound", len(docs) + len(page))
        docs.extend(page)
        if len(page) == 0 or len(docs) >= found:
            break
    # Deciding what is missing from a truncated list would pass over whatever
    # the register left out, so a list that does not hold everything the
    # register found is refused.
    if found > len(docs):
        raise RuntimeError(
            f"The register found {found} files between {date_from} and "
            f"{date_to} but returned {len(docs)}; the list is incomplete."
        )
    if len(docs) == 0:
        return pl.DataFrame(schema={name: pl.String for name in COLUMNS})
    docs = pl.DataFrame(docs)
    if "checksum" not in docs.columns:
        docs = docs.with_columns(pl.lit(None, dtype=pl.String).alias("checksum"))
    docs = docs \
        .unique(subset=["file_name"], keep="first", maintain_order=True) \
        .with_columns(pl.col("publication_date").str.to_datetime("%Y-%m-%dT%H:%M:%SZ")) \
        .with_columns(pl.col("publication_date").dt.date()) \
        .with_columns(pl.col("publication_date").dt.to_string("%Y-%m-%d")) \
        .select(COLUMNS) \
        .sort(["publication_date", "file_type"])
    return docs


def files_in_landing_import(config_path):
    """Get list of files in the import folder.

    Returns the names of the files, which are currently in the import folder of the landing zone.
    If the folder does not exist yet, an empty list is returned.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    dir_import = common.path_landing_import(config_path)
    if not os.path.exists(dir_import):
        return []
    return os.listdir(dir_import)


def files_in_landing_finished(config_path):
    """Get list of files that have been imported into the Bronze layer.

    If the folder does not exist yet, an empty list is returned.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    dir_finished = common.path_landing_finished(config_path)
    if not os.path.exists(dir_finished):
        return []
    return os.listdir(dir_finished)


def drop_imported_files(config_path, docs):
    """Drop download candidates, which are already held.

    Removes those entries from a list of downloadable files, for which a file with the same name is
    already in the import folder, waiting to be imported, or in the finished folder, having been
    imported. A file in the error folder is not held: it is downloaded again, which is what repairs
    a download that failed.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    docs: polars.DataFrame
        Polars data frame containing a list of available files for download. Usually, the result of a
        call to get_downloadable_files.
    """
    held = set(files_in_landing_import(config_path)) \
        | set(files_in_landing_finished(config_path))
    if len(docs) == 0 or len(held) == 0:
        return docs
    skipped = docs.filter(pl.col("file_name").is_in(held))
    names = skipped["file_name"].to_list()
    for file_i in names:
        logger.debug(
            "File is already held, download skipped",
            extra=context(module="firds", layer="landing", file=file_i),
        )
    if names:
        logger.info(
            "Skipped files already held in the landing zone",
            extra=context(module="firds", layer="landing", files=len(names)),
        )
    return docs.filter(~pl.col("file_name").is_in(held))


def download_file(url, target, checksum=None):
    """Download ``url`` into the file ``target``.

    The data is written to ``target`` with :data:`PARTIAL_SUFFIX` appended and
    only renamed once the download is complete, so an interrupted download
    never leaves a file under the name that marks it as held. An error answer
    from the server is raised rather than written, a file whose MD5 digest
    differs from ``checksum`` is refused, and so is an archive that does not
    open.
    """
    partial = target + PARTIAL_SUFFIX
    digest = hashlib.md5()
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT) as response:
            response.raise_for_status()
            with open(partial, mode="wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    file.write(chunk)
                    digest.update(chunk)
        if checksum and digest.hexdigest() != checksum.lower():
            raise ValueError(
                f"{url} has the MD5 digest {digest.hexdigest()}, the register "
                f"states {checksum}"
            )
        if target.lower().endswith(".zip") and not zipfile.is_zipfile(partial):
            raise zipfile.BadZipFile(f"{url} did not return a readable archive")
        os.replace(partial, target)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


def download_files(config_path, docs):
    """Download files.

    Based on the output of function get_downloadable_files, downloads data from ESMA's website.

    A file that fails to download is logged and the others are still
    downloaded; the failures are raised together at the end, so that the run
    is marked as failed. Nothing is left behind for a failed file, so the next
    run fetches it again.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    docs: polars.DataFrame
        Polars data frame containing a list of available files for download. Usually, the result of a
        call to get_downloadable_files.
    """
    if len(docs) == 0:
        logger.warning(
            "No data available for download",
            extra=context(module="firds", layer="landing"),
        )
        return

    dir_import = common.path_landing_import(config_path)
    os.makedirs(dir_import, exist_ok=True)
    failed = []
    for row in docs.iter_rows(named=True):
        file_i = row["file_name"]
        try:
            download_file(
                row["download_link"],
                os.path.join(dir_import, file_i),
                checksum=row.get("checksum"),
            )
        except Exception:
            failed.append(file_i)
            logger.exception(
                "Download failed",
                extra=context(module="firds", layer="landing", file=file_i),
            )
            continue
        logger.info(
            "Downloaded file into the landing zone",
            extra=context(
                module="firds", layer="landing", file=file_i,
                datadt=row["publication_date"],
            ),
        )

    if failed:
        raise RuntimeError(
            f"{len(failed)} of {len(docs)} downloads failed: {', '.join(failed)}"
        )


def read_landing_state(config_path):
    """Return what the landing zone knows, as it last recorded it.

    ``complete_through`` is the day up to which every file needed has been
    imported, ``applied_through`` the day up to which the Silver layer has
    built the history, ``published`` the days after ``complete_through`` for
    which the register listed a delta file, and ``late`` the days whose delta
    file only appeared after that day had been left behind. The second is what lets the Silver layer
    tell a day that ESMA never published from one that was published and has
    not arrived: neither leaves a trace in the data lake. The third is what
    tells it that a day it may already have passed has more records after all.

    ``full_sample`` is the full file the history starts from as the register
    listed it: ``{"date": "YYYY-MM-DD", "files": [...]}``, or ``None`` where
    the landing zone has not seen that listing, as in a data lake filled by
    hand. The full file is published in parts per first letter of the CFI
    code, and a letter of which no part arrived leaves no trace among the
    parts that did; the listing is what tells the Silver layer that it is
    missing.

    A file that cannot be read is treated as no record at all, so that a
    truncated write does not stop the pipeline; the next run writes it again.
    """
    path = common.path_landing_watermark(config_path)
    empty = {"complete_through": None, "applied_through": None,
             "published": [], "late": [], "full_sample": None}
    if not os.path.exists(path):
        return empty
    try:
        with open(path, encoding="utf-8") as handle:
            state = json.load(handle)
    except (ValueError, OSError):
        logger.warning(
            "The landing zone's record could not be read and is taken as empty",
            extra=context(module="firds", layer="landing", path=path),
        )
        return empty
    return {
        "complete_through": state.get("complete_through"),
        "applied_through": state.get("applied_through"),
        "published": state.get("published", []),
        "late": state.get("late", []),
        "full_sample": state.get("full_sample"),
    }


def read_landing_watermark(config_path):
    """Return the day up to which every file needed has been imported.

    Returns ``None`` where no such day has been recorded.
    """
    return read_landing_state(config_path)["complete_through"]


def record_applied_through(config_path, day):
    """Record the day up to which the history has been built.

    The Silver layer calls this after applying a reference date. The landing
    zone cannot read the watermark of the historical database, which lives in
    a Delta table, and needs to know it: a delta file for a day the history
    has already passed is one whose records will not reach it, and only the
    Silver layer knows how far it has come. Its own record of what has been
    imported is no substitute, because it is written before a run downloads
    anything and so names an earlier day than the history may stand at.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    day: str
        The day the history has been built up to, spelled ``'YYYY-MM-DD'``.
    """
    state = read_landing_state(config_path)
    if state["applied_through"] is not None and state["applied_through"] >= day:
        return
    write_landing_watermark(
        config_path,
        state["complete_through"],
        published=state["published"],
        late=state["late"],
        applied_through=day,
        full_sample=state["full_sample"],
    )


def write_landing_watermark(config_path, day, published=(), late=(), applied_through=None,
                            full_sample=None):
    """Record the day everything is imported up to, and the days still open.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    day: str
        The day up to which every file needed has been imported.
    published: list of str
        The days after ``day`` for which the register listed a delta file.
    late: list of str
        The days whose delta file appeared after the day had been left behind.
    applied_through: str
        The day up to which the history has been built.
    full_sample: dict
        The full file the history starts from as the register listed it,
        ``{"date": "YYYY-MM-DD", "files": [...]}``; see
        :func:`read_landing_state`.
    """
    path = common.path_landing_watermark(config_path)
    with open(path + ".tmp", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "complete_through": day,
                "applied_through": applied_through,
                "published": sorted(published),
                "late": sorted(late),
                "full_sample": full_sample,
            },
            handle,
        )
    os.replace(path + ".tmp", path)


def clear_late_day(config_path, day):
    """Take ``day`` off the list of days that were published late.

    The Silver layer calls this for a day it has applied after all. It is also
    the way to accept a day the history has passed: the records of that day
    stay out of the history, and the update carries on.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be
        a yaml-file.
    day: str
        The day to take off the list, spelled ``'YYYY-MM-DD'``.
    """
    state = read_landing_state(config_path)
    write_landing_watermark(
        config_path,
        state["complete_through"],
        published=state["published"],
        late=[recorded for recorded in state["late"] if recorded != day],
        applied_through=state["applied_through"],
        full_sample=state["full_sample"],
    )


def complete_through(needed, finished):
    """Return the last day up to which every file in ``needed`` is finished.

    ``needed`` is a frame of files with their publication dates. The days are
    walked in order and the walk stops at the first day with a file that is
    not in ``finished``. Returns ``None`` where already the first day is
    incomplete, or where nothing is needed.
    """
    through = None
    for (day,), files in needed.sort("publication_date").group_by(
        "publication_date", maintain_order=True
    ):
        if not set(files["file_name"].to_list()) <= finished:
            break
        through = day
    return through


def update_landing_import(config_path, start_date, end_date=None):
    """Downloading FIRDS data

    Downloads FIRDS data from ESMA's website and stores it in the landing zone.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    start_date: str
        Date for which a full FIRDS sample should be downloaded in the format 'YYYY-MM-DD'.
    end_date: str, optional
        Last publication date to download, in the format 'YYYY-MM-DD'. The
        daily job leaves it out and takes everything published up to
        yesterday. Naming it closes the window, which is what rebuilding the
        history for a fixed period needs: without it a run started at a date
        years back fetches every file published since, and on a workstation
        that is neither the intended period nor an amount of data the machine
        is likely to hold.

        A run that closes the window records its landing watermark at the end
        of that window like any other, so a later run without an end date
        continues from there rather than starting again.
    """
    with pipeline_run(
        logger,
        "update_landing_import",
        module="firds",
        layer="landing",
        config_path=config_path,
    ):
        _update_landing_import(config_path, start_date, end_date)


def _update_landing_import(config_path, start_date, end_date=None):
    """Do the work of :func:`update_landing_import`.

    What is fetched is decided by name against the files already held, not by
    resuming after the newest one. Resuming would pass over a part that failed
    while a later file succeeded, and the history built from the delta files
    would then lack that part for good. Comparing names fetches every file that
    is not held, whatever went wrong with it before.
    """
    # The files published up to and including yesterday. Today is left out
    # because ESMA publishes through the day and the set of a day still
    # running may be incomplete.
    #
    # The day is taken in UTC, which is the zone ESMA dates its publications
    # in, rather than off the local clock of whatever machine the job happens
    # to run on. Read locally, a run started shortly after midnight east of
    # Greenwich lands on the previous UTC day and so asks for a window ending
    # two days back: nothing is lost, because the next run picks the files up
    # again, but the run reports that there was nothing to download when in
    # fact there was.
    yesterday = str(datetime.now(timezone.utc).date() - timedelta(days=1))

    # The end of the window the register is asked about. Yesterday where the
    # caller named no end, and never later than that whatever it named: how
    # far the published record can be trusted is a property of ESMA's
    # publishing and not of what was asked for.
    until = yesterday
    if end_date is not None:
        if end_date < start_date:
            raise ValueError(
                f"The end date {end_date!r} is before the start date "
                f"{start_date!r}, so the window holds no day at all."
            )
        if end_date > yesterday:
            logger.warning(
                "The end date lies beyond the last settled day and was cut back to it",
                extra=context(
                    module="firds", layer="landing",
                    requested=end_date, datadt=yesterday,
                ),
            )
        until = min(end_date, yesterday)

    finished = set(files_in_landing_finished(config_path))

    # The full file the history starts from: the one already held, or else
    # the first one published from the start date on.
    held_fulins = sorted(
        parsed["date"]
        for parsed in map(parse_file_name, finished)
        if parsed is not None and parsed["kind"] == "FULINS"
    )

    # The register is asked only for the days that may still be missing
    # something: from the last day up to which every file needed has been
    # imported, or from the full file where no such day is recorded. Asking
    # from the start date every time would make the answer grow by some three
    # thousand files a year. The day itself is asked for again, so that the
    # most recent cancellations file is among the answer.
    watermark = read_landing_watermark(config_path) if held_fulins else None
    if held_fulins:
        # The days just before the watermark are asked about again, so that a
        # file published late is still fetched; see LOOKBACK_DAYS.
        since = ""
        if watermark is not None:
            since = str(
                datetime.strptime(watermark, "%Y-%m-%d").date()
                - timedelta(days=LOOKBACK_DAYS)
            )
        date_from = max(str(held_fulins[0]), since)
    else:
        date_from = start_date
    avail = get_downloadable_files(date_from = date_from, date_to = until)

    docs_fulins = avail.filter(pl.col("file_type") == "FULINS")
    if held_fulins:
        base_date = str(held_fulins[0])
    else:
        if len(docs_fulins) == 0:
            raise RuntimeError(
                f"No full file (FULINS) was published between {start_date} and "
                f"{until}, so there is nothing to start the history from."
            )
        base_date = min(docs_fulins["publication_date"].to_list())
        if base_date > start_date:
            logger.warning(
                "Start date was reset to the first available publication date",
                extra=context(
                    module="firds",
                    layer="landing",
                    requested=start_date,
                    datadt=base_date,
                ),
            )
    # Every part of that full file, so that a part that failed is fetched
    # again; the parts already held are dropped below.
    docs_fulins = docs_fulins.filter(pl.col("publication_date") == base_date)

    # Every delta file published after the full file.
    docs_dltins = avail \
        .filter(pl.col("file_type") == "DLTINS") \
        .filter(pl.col("publication_date") > base_date)

    # The cancellations file. ESMA publishes one every morning beside the
    # delta file, but unlike a delta file it is not the change of one day: it
    # is the consolidated list of every record cancelled so far. Two of them a
    # week apart were found to hold 854,571 and 854,573 pairs, differing by
    # four added and two removed, so an older one says nothing the newest does
    # not already say. Only the most recent publication is therefore fetched.
    docs_fulcan = avail.filter(pl.col("file_type") == "FULCAN")
    if len(docs_fulcan) > 0:
        latest = max(docs_fulcan["publication_date"].to_list())
        docs_fulcan = docs_fulcan.filter(pl.col("publication_date") == latest)

    # What the landing zone knows is recorded before the downloading: the day
    # up to which every full and delta file is in the finished folder, and the
    # days after it for which the register lists a delta file. The second is
    # what tells the Silver layer that a day it has no records for was
    # published rather than never existing; older cancellations files are not
    # needed and count for neither.
    through = complete_through(pl.concat([docs_fulins, docs_dltins]), finished)
    if watermark is not None and (through is None or through < watermark):
        through = watermark
    published = sorted(
        {day for day in docs_dltins["publication_date"].to_list()
         if through is None or day > through}
    )

    # All kinds are fetched in one call, so that the record saying nothing was
    # available speaks for the run as a whole rather than for one kind of file.
    docs = pl.concat([docs_fulins, docs_dltins, docs_fulcan])
    docs = drop_imported_files(config_path = config_path, docs = docs)

    # A delta file for a day that has been left behind. ESMA published it
    # after the day it is dated, and where the Silver layer has built the
    # history past that day its records will not reach it: the update only
    # ever looks at days after its watermark. The day is recorded so that the
    # Silver layer can say so instead of passing over it.
    #
    # The comparison is against the later of the two days the landing zone
    # knows: how far the history has been built, which the Silver layer
    # records here, and how far this zone has imported. A day marked here that
    # the history has not reached yet is applied in the ordinary way, and the
    # Silver layer takes the mark off again.
    state = read_landing_state(config_path)
    late = set(state["late"])
    settled = max(
        [day for day in (through, state["applied_through"]) if day is not None],
        default=None,
    )
    if settled is not None:
        appearing = {
            row["publication_date"] for row in docs.iter_rows(named=True)
            if row["file_type"] == "DLTINS" and row["publication_date"] <= settled
        }
        for day in sorted(appearing - late):
            logger.error(
                "A delta file was published for a day that was left behind",
                extra=context(module="firds", layer="landing", datadt=day),
            )
        late |= appearing

    # The parts of the full file as the register lists them, for the Silver
    # layer to check the imported parts against; see read_landing_state. A
    # run that asked the register only about later days has not seen them and
    # keeps the list an earlier run recorded.
    full_sample = state["full_sample"]
    if len(docs_fulins) > 0:
        full_sample = {
            "date": base_date,
            "files": sorted(docs_fulins["file_name"].to_list()),
        }

    write_landing_watermark(
        config_path, through, published=published, late=sorted(late),
        applied_through=state["applied_through"], full_sample=full_sample,
    )
    if through is not None and (watermark is None or through > watermark):
        logger.info(
            "Every file needed has been imported up to this day",
            extra=context(module="firds", layer="landing", datadt=through),
        )

    download_files(config_path = config_path, docs = docs)


#: What this module offers. Downloading the files ESMA publishes into the landing zone.
__all__ = [
    "COLUMNS",
    "PAGE_SIZE",
    "PARTIAL_SUFFIX",
    "TIMEOUT",
    "clear_late_day",
    "complete_through",
    "download_file",
    "download_files",
    "drop_imported_files",
    "files_in_landing_finished",
    "files_in_landing_import",
    "get_downloadable_files",
    "read_landing_state",
    "record_applied_through",
    "read_landing_watermark",
    "update_landing_import",
    "url_for_available_files",
    "write_landing_watermark",
]
