"""GLEIF's mapping of market identifier codes to LEIs, and a history of it.

A market identifier code (MIC, ISO 10383) names a trading venue or a segment
of one, and GLEIF links every MIC to the LEI of the entity that operates the
market. It publishes the mapping about weekly, as a zipped csv file of two
columns, ``LEI`` and ``MIC``, named after its day: ``LEI-MIC-20261116.zip``.
Its mapping API lists every file it still holds, back to August 2022.

The pipeline is apart from the one of the golden copy, because nothing about
it follows the publications of the golden copy: the files come on a day of
their own, about weekly, and are a few dozen kilobytes each.

``list_mic_lei_files``
    which files GLEIF holds,
``download_mic_lei_file`` and ``import_mic_lei_file``
    a file into the landing zone, and from there into the Bronze layer, one
    parquet file per day,
``mic_lei_versions``
    the rules a history is built by, independently of its storage, and
``update_gleif_mic_lei``
    the pipeline, which brings in every file the Bronze layer lacks and
    rebuilds the history from the files it holds.

A version of a MIC holds from the day of the first file that links it to its
LEI to the day before the first file that no longer does -- because it names
another LEI or does not name the MIC at all. The files carry no dates of their
own, so a change is dated by the file it was first read in, which is up to a
week after GLEIF made it. Before the first file the Bronze layer holds nothing
is known.

The history is rebuilt from the Bronze layer on every run rather than merged
into, since all the files together hold some eighty thousand rows. A rebuild
gives the same history as the files did whatever order they were brought in.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas

from . import common, history
from ..log import context, pipeline_run

logger = logging.getLogger(__name__)

#: GLEIF's mapping API: the MIC-to-LEI files it holds, newest first.
MIC_LEI_URL = "https://mapping.gleif.org/api/v2/mic-lei"

#: How many files the API is asked for per page; it holds some eighty.
MIC_LEI_PAGE_SIZE = 100

#: How often a request GLEIF turns away as too many is made, and the longest
#: wait between two attempts, in seconds. The mapping API allows 60 requests a
#: minute, and the first run downloads some eighty files; an answer of 429
#: says in ``Retry-After`` how long to wait, which is what is waited.
MIC_LEI_ATTEMPTS = 4
MIC_LEI_MAX_WAIT = 90

#: How GLEIF names a MIC-to-LEI file.
MIC_LEI_FILE_NAME = re.compile(r"^LEI-MIC-(?P<day>\d{8})\.zip$", re.IGNORECASE)

#: The columns of a MIC-to-LEI file, by GLEIF's names.
MIC_LEI_SOURCE_COLUMNS = ["LEI", "MIC"]

#: The key of the history: a MIC is linked to one LEI at a time.
MIC_LEI_KEY = ("mic",)

#: The columns of the history, in the order they are stored in.
MIC_LEI_COLUMNS = ["mic", "lei", history.VALID_FROM, history.PUBLICATION,
                   history.VALID_TO, history.LATEST_RECORD]

#: The history in Spark's types, as its contract states it.
MIC_LEI_SCHEMA = common.mic_lei_schema


@dataclass(frozen=True)
class MicLeiFile:
    """One MIC-to-LEI file GLEIF holds."""

    day: date
    name: str
    url: str

    @property
    def label(self):
        """``YYYYMMDD``, the day in the file's name."""
        return self.day.strftime("%Y%m%d")


def mic_lei_day(name):
    """The day a MIC-to-LEI file is of, read off its name, or ``None``."""
    found = MIC_LEI_FILE_NAME.match(name)
    if not found:
        return None
    return datetime.strptime(found.group("day"), "%Y%m%d").date()


def _get(session, url, **options):
    """``session.get``, made again where GLEIF answers that it is too many.

    The answer is returned as it is otherwise, for the caller to raise on.
    """
    for attempt in range(1, MIC_LEI_ATTEMPTS + 1):
        response = session.get(url, **options)
        if response.status_code != 429 or attempt == MIC_LEI_ATTEMPTS:
            return response
        try:
            wait = int((response.headers or {}).get("Retry-After", ""))
        except ValueError:
            wait = 15 * attempt
        response.close()
        wait = max(1, min(wait, MIC_LEI_MAX_WAIT))
        logger.info("Waiting for GLEIF's rate limit",
                    extra=context(module="gleif", table=common.MIC_LEI, wait=wait))
        time.sleep(wait)


def list_mic_lei_files(session=None, timeout=60):
    """The MIC-to-LEI files GLEIF holds, oldest first.

    A file GLEIF marks as not valid or not yet processed is left out, and so
    is one not named as its files are, since its day could not be read. Where
    two files are of one day, the one uploaded last is taken.

    Parameters
    ----------
    session: requests.Session, optional
        The session to read the API with.

    Raises
    ------
    ValueError
        If the API names no file at all, which is what an API that has changed
        its shape answers.
    """
    import requests

    from ..marketpricedata.common import USER_AGENT

    session = session or requests.Session()
    url = f"{MIC_LEI_URL}?page%5Bsize%5D={MIC_LEI_PAGE_SIZE}"
    entries = []
    while url:
        with _get(session, url, headers={"User-Agent": USER_AGENT},
                  timeout=timeout) as answer:
            answer.raise_for_status()
            page = answer.json()
        entries += page.get("data") or []
        url = (page.get("links") or {}).get("next")

    latest = {}
    for entry in entries:
        attributes = entry.get("attributes") or {}
        day = mic_lei_day(attributes.get("fileName") or "")
        if (day is None or not attributes.get("downloadLink")
                or attributes.get("valid") is False
                or attributes.get("processed") is False):
            continue
        uploaded = attributes.get("uploadedAt") or ""
        if day not in latest or uploaded > latest[day][0]:
            latest[day] = (uploaded, MicLeiFile(day, attributes["fileName"],
                                                attributes["downloadLink"]))
    if not latest:
        raise ValueError(
            f"GLEIF's mapping API at {MIC_LEI_URL} names no MIC-to-LEI file. "
            "GLEIF may have changed the API; a file can still be placed into "
            "the landing zone by hand under the name GLEIF gives it."
        )
    return [latest[day][1] for day in sorted(latest)]


def download_mic_lei_file(config_path, file, session=None, timeout=120):
    """Download one MIC-to-LEI file into the landing zone.

    The file keeps the name GLEIF gave it, which the Bronze import reads its
    day off. It is written under a temporary name and put in place once it is
    complete and opens as a zip archive, so an interrupted download leaves
    nothing the import would read. A file already in the landing zone is not
    downloaded again.

    Returns
    -------
    str
        The path of the file.
    """
    import requests

    session = session or requests.Session()
    folder = common.path_landing_mic_lei(config_path)
    os.makedirs(folder, exist_ok=True)
    target = os.path.join(folder, file.name)
    if os.path.exists(target):
        return target
    partial = target + ".partial"
    try:
        with _get(session, file.url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            # As with the golden copy, the raw stream keeps a gzip transfer
            # encoding unless told to undo it.
            response.raw.decode_content = True
            with open(partial, "wb") as out:
                shutil.copyfileobj(response.raw, out, length=1 << 20)
        if not zipfile.is_zipfile(partial):
            raise ValueError(
                f"the MIC-to-LEI file {file.name} that GLEIF sent is not a zip "
                f"archive: {file.url}"
            )
        os.replace(partial, target)
    finally:
        if os.path.exists(partial):
            os.remove(partial)
    logger.info("Downloaded", extra=context(module="gleif", layer="landing",
                                            table=common.MIC_LEI, file=file.name))
    return target


def bronze_mic_lei_path(config_path, day):
    """The Bronze file of the MIC-to-LEI file of one day."""
    return os.path.join(common.path_bronze_mic_lei(config_path),
                        day.strftime("%Y%m%d") + ".parquet")


def bronze_mic_lei_files(config_path):
    """The MIC-to-LEI files the Bronze layer holds, oldest first.

    Returns
    -------
    list of tuple
        ``(day, path)``.
    """
    folder = common.path_bronze_mic_lei(config_path)
    if not os.path.isdir(folder):
        return []
    found = []
    for name in os.listdir(folder):
        if re.fullmatch(r"\d{8}\.parquet", name):
            day = datetime.strptime(name[:8], "%Y%m%d").date()
            found.append((day, os.path.join(folder, name)))
    return sorted(found)


def import_mic_lei_file(config_path, path):
    """Import one MIC-to-LEI file of the landing zone into the Bronze layer.

    The two columns are kept as GLEIF writes them, as text. A file already
    imported is not imported again, and the table is written under another
    name and renamed once complete. The file stays in the landing zone, as the
    golden copies do, which is what keeps it from being downloaded again.

    Returns
    -------
    str
        The path of the Bronze file.
    """
    name = os.path.basename(path)
    day = mic_lei_day(name)
    if day is None:
        raise ValueError(
            f"{path!r} is not named as a MIC-to-LEI file of GLEIF is, "
            "LEI-MIC-YYYYMMDD.zip"
        )
    target = bronze_mic_lei_path(config_path, day)
    if os.path.exists(target):
        return target
    os.makedirs(os.path.dirname(target), exist_ok=True)
    # Every field as text, and only an empty one as missing: 'NA' is a code.
    frame = pandas.read_csv(path, compression="zip", dtype=str,
                            keep_default_na=False, na_values=[""])
    missing = [c for c in MIC_LEI_SOURCE_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"{path!r} carries no column {missing}")
    partial = target + ".partial"
    try:
        frame[MIC_LEI_SOURCE_COLUMNS].to_parquet(partial, index=False)
        os.replace(partial, target)
    finally:
        if os.path.exists(partial):
            os.remove(partial)
    logger.info("Imported", extra=context(module="gleif", layer="bronze",
                                          table=common.MIC_LEI, file=name,
                                          rows=len(frame)))
    return target


def _normalised(values):
    """Codes written without blanks and in upper case, as ISO writes them."""
    return values.fillna("").astype(str) \
        .str.replace(r"[\s  -​  　]", "", regex=True) \
        .str.upper()


def mic_lei_mapping(frame, label=""):
    """The MIC-to-LEI file of one day as a dict of each MIC's LEI.

    The codes are written without blanks and in upper case, and a row missing
    either is left out. A MIC a file links to two LEIs -- which no file GLEIF
    published up to November 2026 did -- is given the first of them in
    order, so that the history is the same on every run, and is named in a
    warning.
    """
    codes = pandas.DataFrame({"mic": _normalised(frame["MIC"]),
                              "lei": _normalised(frame["LEI"])})
    codes = codes[(codes.mic != "") & (codes.lei != "")].drop_duplicates()
    codes = codes.sort_values(["mic", "lei"])
    doubled = sorted(codes.mic[codes.mic.duplicated()].unique())
    if doubled:
        logger.warning(
            "MICs the file links to more than one LEI, the first is taken",
            extra=context(module="gleif", table=common.MIC_LEI, file=label,
                          count=len(doubled), mics="|".join(doubled[:5])),
        )
    codes = codes.drop_duplicates("mic", keep="first")
    return dict(zip(codes.mic, codes.lei))


def mic_lei_versions(snapshots):
    """The history of the MIC-to-LEI mapping from the files of every day.

    Parameters
    ----------
    snapshots: list of tuple
        ``(day, frame)``: the day of a file and its rows under GLEIF's names,
        in any order.

    Returns
    -------
    pandas.DataFrame
        One row per version of a MIC, in :data:`MIC_LEI_COLUMNS`: the LEI it
        was linked to, the first and the last day the link held -- the day of
        the first file that stated it, and the day before the first file that
        no longer did, or empty while the latest file still does -- the file
        it was first read from, ``YYYYMMDD``, and whether it is the latest
        version of its MIC, which a MIC GLEIF no longer lists keeps.
    """
    rows, open_ = [], {}
    for day, frame in sorted(snapshots, key=lambda snapshot: snapshot[0]):
        label = day.strftime("%Y%m%d")
        mapping = mic_lei_mapping(frame, label)
        for mic in sorted(open_):
            lei, start, read_from = open_[mic]
            if mapping.get(mic) != lei:
                rows.append((mic, lei, start, read_from, day - timedelta(days=1)))
                del open_[mic]
        for mic, lei in mapping.items():
            if mic not in open_:
                open_[mic] = (lei, day, label)
    rows += [(mic, lei, start, read_from, None)
             for mic, (lei, start, read_from) in open_.items()]

    versions = pandas.DataFrame(rows, columns=MIC_LEI_COLUMNS[:-1])
    versions = versions.sort_values(["mic", history.VALID_FROM], ignore_index=True)
    versions[history.LATEST_RECORD] = ~versions["mic"].duplicated(keep="last")
    return versions[MIC_LEI_COLUMNS]


def mic_lei_history(spark, config_path):
    """The history built from every MIC-to-LEI file the Bronze layer holds."""
    snapshots = [(day, pandas.read_parquet(path))
                 for day, path in bronze_mic_lei_files(config_path)]
    versions = mic_lei_versions(snapshots)
    # Spark takes a missing date from pandas as NaT only through objects.
    records = [
        (r.mic, r.lei, r.valid_from, r.publication,
         None if pandas.isna(r.valid_to) else r.valid_to, bool(r.latest_record))
        for r in versions.itertuples(index=False)
    ]
    return spark.createDataFrame(records, MIC_LEI_SCHEMA)


def write_mic_lei_history(spark, config_path):
    """Rebuild the history from the files the Bronze layer holds and write it.

    The Delta table is written over in one commit. A file placed into the
    landing zone by hand is brought in by :func:`import_mic_lei_file` first.

    Returns
    -------
    int
        The number of versions written.
    """
    from ..addressing import write_delta

    if not bronze_mic_lei_files(config_path):
        raise FileNotFoundError(
            f"The Bronze layer holds no MIC-to-LEI file in "
            f"{common.path_bronze_mic_lei(config_path)!r}."
        )
    versions = mic_lei_history(spark, config_path).cache()
    try:
        write_delta(versions, common.ref_silver_mic_lei(config_path), mode="overwrite")
        return versions.count()
    finally:
        versions.unpersist()


def update_gleif_mic_lei(spark, config_path, session=None):
    """Bring the history of the MIC-to-LEI mapping up to GLEIF's latest file.

    Every file GLEIF holds that the Bronze layer lacks is downloaded and
    imported, and the history is rebuilt from all the files the Bronze layer
    holds and written over the Delta table. The first run therefore begins
    the history with the oldest file GLEIF still holds, and every later run
    keeps what earlier ones brought in, whether or not GLEIF still holds it.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session with Delta Lake.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    session: requests.Session, optional
        The session to read GLEIF's API and files with.

    Returns
    -------
    list of str
        The days of the files brought in, ``YYYYMMDD``.
    """
    common.create_directory_tree(config_path)
    with pipeline_run(logger, "update_gleif_mic_lei", module="gleif",
                      layer="silver", table=common.MIC_LEI,
                      config_path=config_path) as outcome:
        held = {day for day, _ in bronze_mic_lei_files(config_path)}
        added = []
        for file in list_mic_lei_files(session=session):
            if file.day in held:
                continue
            path = download_mic_lei_file(config_path, file, session=session)
            import_mic_lei_file(config_path, path)
            added.append(file.label)
        outcome["versions"] = write_mic_lei_history(spark, config_path)
        outcome["imported"] = len(added)
        outcome["files"] = len(held) + len(added)
    return added


def connect_to_mic_lei(spark, config_path):
    """Open the history of the MIC-to-LEI mapping, every version of every MIC."""
    from ..addressing import read_delta

    return read_delta(spark, common.ref_silver_mic_lei(config_path))


def mic_lei_held(spark, config_path):
    """Whether the history of the MIC-to-LEI mapping is there to read."""
    from ..addressing import delta_table_exists

    try:
        ref = common.ref_silver_mic_lei(config_path)
    except KeyError:
        return False
    return delta_table_exists(spark, ref)


def mic_lei_as_of(spark, config_path, day):
    """Which LEI operated which MIC on ``day``, one row per MIC."""
    return history.records_as_of(connect_to_mic_lei(spark, config_path), day)


def current_mic_lei(spark, config_path):
    """The MICs the latest file links to an LEI, one row per MIC."""
    from pyspark.sql import functions as F

    return connect_to_mic_lei(spark, config_path).where(
        F.col(history.VALID_TO).isNull())


__all__ = [
    "MIC_LEI_ATTEMPTS",
    "MIC_LEI_COLUMNS",
    "MIC_LEI_MAX_WAIT",
    "MIC_LEI_FILE_NAME",
    "MIC_LEI_KEY",
    "MIC_LEI_PAGE_SIZE",
    "MIC_LEI_SCHEMA",
    "MIC_LEI_SOURCE_COLUMNS",
    "MIC_LEI_URL",
    "MicLeiFile",
    "bronze_mic_lei_files",
    "bronze_mic_lei_path",
    "connect_to_mic_lei",
    "current_mic_lei",
    "download_mic_lei_file",
    "import_mic_lei_file",
    "list_mic_lei_files",
    "mic_lei_as_of",
    "mic_lei_day",
    "mic_lei_held",
    "mic_lei_history",
    "mic_lei_mapping",
    "mic_lei_versions",
    "update_gleif_mic_lei",
    "write_mic_lei_history",
]
