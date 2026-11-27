"""Which of GLEIF's publications a history needs, and their download.

GLEIF publishes the golden copy three times a day, at 00:00, 08:00 and 16:00
UTC, and keeps every publication since February 2018 downloadable as a full
copy. Beside each recent one it publishes delta files -- the records changed
in the 8 hours, the day, the week or the month before it -- but only for some
two months back.

A history is therefore read in two ways. From the start date the user sets,
it is begun from the full copy of the first publication on or after that day
and carried forward one full copy per interval -- a month by default -- for as
long as no delta files exist. Once they do, it is carried forward one day at
a time by the delta of the day before each publication, at the hour of day
the history was begun at, so that each delta covers exactly the 24 hours since
the one before it. Where a publication of that hour is missing, or offers no
delta, the next full copy closes the gap.

Every version begins on the day of its record's last update (see
:mod:`fincommons.gleif.history`), so the coarser steps of the first stretch
lose only the intermediate states of a record that changed more than once
within one step, not the dates of the changes they keep.
"""

from __future__ import annotations

import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from . import common
import logging

from ..log import context

logger = logging.getLogger(__name__)

#: The list of publications, newest first.
PUBLISHES_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"

#: How many publications the API returns per page; it refuses more.
PAGE_SIZE = 10

#: How the files GLEIF publishes are named.
FILE_NAME = re.compile(
    r"^(?P<day>\d{8})-(?P<hour>\d{4})-gleif-goldencopy-(?P<type>lei2|rr)-"
    r"(?P<kind>golden-copy|last-day)\.csv\.zip$")

#: The kind of file, by how GLEIF names it and how this module does.
KINDS = {"golden-copy": common.FULL, "last-day": common.DELTA}


@dataclass(frozen=True)
class Publication:
    """One publication of the golden copy, and the files of it the history reads."""

    when: datetime
    full: dict
    delta: dict

    @property
    def label(self):
        """``YYYYMMDD-HHMM``, as GLEIF names its files."""
        return self.when.strftime("%Y%m%d-%H%M")

    @property
    def day(self):
        return self.when.date()


def _parse(entry):
    """A :class:`Publication` from one entry of the API's list."""
    when = datetime.strptime(entry["publish_date"], "%Y-%m-%d %H:%M:%S")
    full, delta = {}, {}
    for level, kind in common.LEVELS.items():
        files = entry.get(kind) or {}
        url = (((files.get("full_file") or {}).get("csv")) or {}).get("url")
        if url:
            full[level] = url
        url = ((((files.get("delta_files") or {}).get("LastDay") or {})
                .get("csv")) or {}).get("url")
        if url:
            delta[level] = url
    return Publication(when, full, delta)


def list_publications(until, session=None, timeout=60):
    """GLEIF's publications from the latest back to the first on or after ``until``.

    The list is read page by page, newest first, and the reading stops at the
    first page that reaches back past ``until``, so a daily run reads a page
    or two and only a history begun years back reads the whole list.

    Parameters
    ----------
    until: datetime.date
        The earliest day of interest.
    session: requests.Session, optional
        The session to read with; a new one by default.

    Returns
    -------
    list of Publication
        Oldest first.
    """
    import requests

    session = session or requests.Session()
    found = []
    page = 1
    while True:
        response = session.get(PUBLISHES_URL, timeout=timeout,
                               params={"page": page, "per_page": PAGE_SIZE})
        response.raise_for_status()
        body = response.json()
        entries = body.get("data") or []
        for entry in entries:
            publication = _parse(entry)
            if publication.day < until:
                return sorted(found, key=lambda p: p.when)
            found.append(publication)
        pages = ((body.get("meta") or {}).get("pagination") or {}).get("total_pages")
        if not entries or (pages is not None and page >= pages):
            return sorted(found, key=lambda p: p.when)
        page += 1


def _add_months(day, months):
    month = day.month - 1 + months
    year = day.year + month // 12
    month = month % 12 + 1
    from calendar import monthrange
    return day.replace(year=year, month=month,
                       day=min(day.day, monthrange(year, month)[1]))


#: The day of the week a history fed by delta files reads a full copy instead,
#: Sunday. The delta files do not report every change: a relationship GLEIF
#: drops can leave them without a deletion, and 570 of half a million had,
#: after a month of deltas. A full copy closes what it no longer lists, so a
#: weekly one bounds how long such a record stays open to a week.
FULL_WEEKDAY = 6


def plan_publications(publications, start_date, applied=None, months=1,
                      full_weekday=FULL_WEEKDAY):
    """The publications to read, in order, and whether each as a full copy.

    Parameters
    ----------
    publications: list of Publication
        Oldest first, from :func:`list_publications`.
    start_date: datetime.date
        The first day the history is to cover.
    applied: str, optional
        The label of the last publication the history was carried to, or
        ``None`` for a history not yet begun.
    months: int
        The step between two full copies while no delta files exist.
    full_weekday: int or None
        The day of the week, Monday 0, on which a full copy is read instead of
        the delta, to catch what the deltas do not report; ``None`` for never.
        See :data:`FULL_WEEKDAY`.

    Returns
    -------
    list of tuple
        ``(publication, kind)``, where ``kind`` is
        :data:`~fincommons.gleif.common.FULL` or
        :data:`~fincommons.gleif.common.DELTA`.
    """
    if months < 1:
        raise ValueError(f"the step between two full copies is a month or more, got {months}")
    by_when = {p.when: p for p in publications}
    ordered = sorted(publications, key=lambda p: p.when)
    plan = []

    if applied is None:
        first = next((p for p in ordered if p.day >= start_date), None)
        if first is None:
            return []
        plan.append((first, common.FULL))
        current = first.when
    else:
        current = datetime.strptime(applied, "%Y%m%d-%H%M")

    latest = ordered[-1].when if ordered else current
    while current < latest:
        following = by_when.get(current + timedelta(days=1))
        if following is not None and set(following.delta) == set(common.LEVELS):
            weekly = (full_weekday is not None
                      and following.when.weekday() == full_weekday)
            plan.append((following, common.FULL if weekly else common.DELTA))
            current = following.when
            continue
        # No delta of the next day: a full copy. A step further on while that
        # step falls before the first publication of the same hour that has
        # deltas, and that publication once it does, from which the deltas
        # carry the history on. Where the publication of that hour is not out
        # yet, the history waits for it rather than moving to another hour.
        later = [p for p in ordered if p.when > current]
        if not later:
            break
        with_deltas = next((p for p in later
                            if p.when.time() == current.time()
                            and set(p.delta) == set(common.LEVELS)), None)
        step = datetime.combine(_add_months(current.date(), months), current.time())
        if with_deltas is not None and with_deltas.when <= step:
            target = with_deltas
        else:
            target = next((p for p in later
                           if p.when >= step and p.when.time() == current.time()),
                          None)
            if target is None:
                target = with_deltas
            if target is None and step < later[-1].when:
                # GLEIF's hours of publication have not always been the same;
                # before the deltas, the next copy at any hour will do.
                target = next(p for p in later if p.when >= step)
            if target is None:
                break
        plan.append((target, common.FULL))
        current = target.when
    return plan


def landing_name(publication, level, kind):
    """The name the file of one level of a publication is downloaded under."""
    gleif = "golden-copy" if kind == common.FULL else "last-day"
    return (f"{publication.label}-gleif-goldencopy-{common.LEVELS[level]}-"
            f"{gleif}.csv.zip")


def download_publication(config_path, publication, kind, session=None,
                         timeout=600):
    """Download the files of both levels of one publication into the landing zone.

    A file is written under a temporary name and put in place once complete,
    so an interrupted download leaves nothing the Bronze import would read.
    A file already in the landing zone is not downloaded again.

    Returns
    -------
    dict
        The path of each level's file.
    """
    import requests

    session = session or requests.Session()
    urls = publication.full if kind == common.FULL else publication.delta
    paths = {}
    for level in common.LEVELS:
        if level not in urls:
            raise ValueError(
                f"the publication {publication.label} offers no {kind} file of "
                f"{level}"
            )
        folder = common.path_landing(config_path, level)
        os.makedirs(folder, exist_ok=True)
        target = os.path.join(folder, landing_name(publication, level, kind))
        paths[level] = target
        if os.path.exists(target):
            continue
        partial = target + ".partial"
        with session.get(urls[level], stream=True, timeout=timeout) as response:
            response.raise_for_status()
            # GLEIF's server sends the archives with a gzip transfer encoding,
            # which the raw stream keeps unless told to undo it: copied as it
            # comes, the file is the zip gzipped once more.
            response.raw.decode_content = True
            with open(partial, "wb") as out:
                shutil.copyfileobj(response.raw, out, length=1 << 20)
        if not zipfile.is_zipfile(partial):
            os.remove(partial)
            raise ValueError(
                f"the {kind} file of {level} of {publication.label} that GLEIF "
                f"sent is not a zip archive: {urls[level]}"
            )
        os.replace(partial, target)
        logger.info("Downloaded", extra=context(module="gleif", layer="landing",
                                         file=os.path.basename(target)))
    return paths


def parse_landing_name(name):
    """The publication label, level and kind a landing file's name states.

    ``None`` for a name that is not one of GLEIF's.
    """
    found = FILE_NAME.match(name)
    if not found:
        return None
    level = next(k for k, v in common.LEVELS.items() if v == found.group("type"))
    return (f"{found.group('day')}-{found.group('hour')}", level,
            KINDS[found.group("kind")])


__all__ = [
    "FILE_NAME",
    "PAGE_SIZE",
    "FULL_WEEKDAY",
    "PUBLISHES_URL",
    "Publication",
    "download_publication",
    "landing_name",
    "list_publications",
    "parse_landing_name",
    "plan_publications",
]
