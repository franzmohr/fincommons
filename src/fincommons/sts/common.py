"""STS: the layout of the data lake.

The Bronze and the Silver layer are addressed through
:mod:`fincommons.addressing`, which reads the ``sts`` section of the
configuration file, so that the same code runs against a local directory tree,
against a Unity Catalog volume and against a schema in a catalogue::

    sts:
        bronze: /data/sts/bronze
        silver: /data/sts/silver

ESMA keeps the public register of the securitisations notified to it as simple,
transparent and standardised, and exposes it through the machine-to-machine
services of its registers portal: a Solr index queried by URL, documented at
https://registers.esma.europa.eu/publication/helpApp. **It needs no key and no
registration**, which is what makes this module possible and what separates the
register from the reported securitisation data an authority or an investor gets
from a securitisation repository.

**The register is current state.** A cancelled notification leaves it: of the
109 cancellations ESMA published up to May 2022, 107 are no longer in the
register. The two spreadsheets that record the history were frozen in 2022, so
for anything that happened after that the dated snapshots of this Bronze layer
are the only record of what the register once held. That is why
:func:`fincommons.sts.bronze.collect_register` writes a snapshot per run and
why nothing here ever rewrites one.

**The register holds two populations**, and which one a notification falls in
decides what it says. Of the 1,147 notifications held on 20 November 2026, 502
are public and carry the whole STS template, their tranches and their parties;
645 are private and carry sixteen to eighteen fields and no child documents at
all. The securitisation identifier is a real identifier only for the public
half -- a private notification carries a synthetic ``Private_2020_12_18_01`` --
so a join to reported securitisation data reaches about five hundred of them.

``common``
    the layout of the data lake, the addresses of the sources, and what the
    register is expected to hold,
``bronze``
    collecting the register and the spreadsheets, as published.

The Silver layer waits on :mod:`fincommons.sts.bronze` having collected
something real: the tables, their schemas and the reader are not declared here
until they are built from snapshots rather than from a description of them.

Five sources are collected. The register answers for the notifications and for
their tranches and parties; four spreadsheets hold what it does not. The
traditional list (ESMA33-128-760) is the notifications of 2019 to 14 January
2022 and holds 222 identifiers the register has since dropped. The cancelled
list (ESMA33-128-761) is the only published record of a removal, with ESMA's
own reasons for it. The synthetic list (ESMA82-402-39) is a data set of its own
-- its own columns, its own ``SYSEC_`` identifiers, no overlap with anything --
and is the only public record of synthetic STS, which ESMA still takes by
e-mail. The register of securitisation repositories is two rows, and is worth
having because it gives each repository's legal entity identifier, which the
register names only as a string.
"""

import datetime
import json
import os

from ..addressing import resolve

#: Where the registers portal answers. The core is named in the address, so
#: the one constant serves both the notifications and their children.
REGISTER_URL = "https://registers.esma.europa.eu/solr/esma_registers_stsre/select"

#: The Solr core the STS register is held in, as the help page names it.
REGISTER_CORE = "esma_registers_stsre"

#: The name the publisher is given in log records and messages.
SOURCE = "registers.esma.europa.eu"

#: The two kinds of document the core holds. A notification is a ``parent``
#: and its tranches and parties are ``child`` documents hanging off it by
#: ``_root_``, which is the parent's ``id``.
DOCUMENT_KINDS = ("parent", "child")

#: How many documents are asked for in one request. The help page's own
#: examples use a thousand and state no maximum; the whole register came back
#: in one answer at two thousand. Five hundred is a page small enough to be
#: retried cheaply and large enough that the register costs three requests.
PAGE_SIZE = 500

#: The order documents are paged in. Solr paging with ``start`` and no sort is
#: not stable -- a document can be served twice or skipped as the index shifts
#: under a run -- and ``id`` is the one field every document in the core
#: carries. It sorts as a string, which is immaterial: what matters is that the
#: order is total and does not change between two requests of one run.
PAGE_SORT = "id asc"

#: What a Solr response header says when all is well.
_OK = 0

#: The fields every notification in the register carried on 20 November 2026,
#: all 1,147 of them. A field that stops appearing altogether is a change to
#: the register rather than to a notification, so
#: :func:`fincommons.sts.bronze.collect_register` reports its absence and warns.
#: The other 245 fields a snapshot holds are not listed here: the template
#: fields are on the public half only, and which of them a notification carries
#: is a fact about that notification rather than about the register. Each
#: snapshot's record carries the census of all of them.
CORE_FIELDS = (
    "id",
    "entity_type",
    "type_s",
    "stsre_compl_stat_desc",
    "stsre_init_notif_dt",
    "stsre_notif_dt",
    "stsre_notif_id",
    "stsre_sanc_status",
    "stsre_sec_id",
    "stsre_secclass_code",
    "stsre_secclass_desc",
    "stsre_sectype_code",
    "stsre_sectype_desc",
)

#: The kinds of child document, by the ``entity_type`` they carry, with the
#: field that identifies the party or the instrument. The sponsor, the
#: originator and the original lender come with a legal entity identifier; the
#: special purpose entity comes with a country and nothing else, which is a gap
#: in the register rather than one in the reading of it.
CHILD_KINDS = {
    "issins": "stsre_isin_instrument",
    "originator": "originator_lei",
    "sspe": "sspe_country_code",
    "lender": "lender_lei",
    "sponsor": "sponsor_lei",
}

#: The spreadsheets ESMA publishes beside the register, by the name this module
#: files them under. The first two are frozen, the third is current and the
#: fourth is two rows of reference data.
SHEETS = {
    "traditional": (
        "https://www.esma.europa.eu/sites/default/files/library/"
        "esma33-128-760_securitisations_designated_as_sts_as_from_01_01_2019"
        "_regulation_2402_2017.xlsx"),
    "cancelled": (
        "https://www.esma.europa.eu/sites/default/files/library/"
        "esma33-128-765_sts_notifications_cancelled_as_from_01_01_2019"
        "_regulation_2402_2017.xlsx"),
    "synthetic": (
        "https://www.esma.europa.eu/sites/default/files/"
        "esma82-402-39_synthetic_securitisations_designated_as_sts_as_from"
        "_09_04_2021_regulation_2021_557.xlsx"),
    "repositories": (
        "https://www.esma.europa.eu/sites/default/files/library/"
        "esma_register_secr.xlsx"),
}

#: The ESMA reference of each spreadsheet, for the record a collection writes.
#: The register of repositories carries none. The reference is the one the
#: workbook states on its own first sheet: the cancelled list says
#: ESMA33-128-761 although ESMA files it under a name beginning esma33-128-765.
SHEET_REFERENCES = {
    "traditional": "ESMA33-128-760",
    "cancelled": "ESMA33-128-761",
    "synthetic": "ESMA82-402-39",
    "repositories": None,
}

#: The file type the spreadsheets are published as. A link that has gone stale
#: answers with a page of HTML rather than with a status, so what came back is
#: checked against this before it is stored; see
#: :func:`fincommons.sts.bronze.collect_sheet`.
SHEET_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

#: The first bytes of a zip, which is what an ``.xlsx`` is.
SHEET_MAGIC = b"PK\x03\x04"

#: The list of the securitisations that lost STS status in the Union through
#: Brexit. ESMA's securitisation page still links it and the link answers 404
#: with a page of HTML, so it is named here, and not in :data:`SHEETS`, until
#: an address that resolves is found.
BREXIT_SHEET_URL = (
    "https://www.esma.europa.eu/sites/default/files/library/"
    "esma33-128-765_securitisations_no_longer_eligible_for_sts_status_in_the_eu"
    "_due_to_brexit.xlsx")


def today():
    """The day a collection is filed under, in UTC.

    Stated here rather than taken per call so that every file one run writes
    lands in one snapshot, even where the run crosses midnight.
    """
    return datetime.datetime.now(datetime.timezone.utc).date()


# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "sts", "bronze").path


def path_bronze_register(config_path):
    """Folder holding one snapshot of the register per run that collected it."""
    return os.path.join(path_bronze_base(config_path), "register")


def path_bronze_snapshot(config_path, day):
    """Folder holding the answers of one collection of the register."""
    day = day if isinstance(day, str) else day.isoformat()
    return os.path.join(path_bronze_register(config_path), day)


def path_bronze_page(config_path, day, kind, page):
    """One answer of the register, as it came back.

    The kind is ``'parent'`` or ``'child'`` and the page is counted from zero,
    so that the files of a snapshot sort into the order they were fetched in.
    """
    return os.path.join(path_bronze_snapshot(config_path, day),
                        f"{kind}-{page:04d}.json")


def path_bronze_sheets(config_path):
    """Folder holding the spreadsheets, by the day they were collected."""
    return os.path.join(path_bronze_base(config_path), "sheets")


def path_bronze_sheet(config_path, day, name):
    """One spreadsheet, as ESMA published it.

    Filed under the day it was collected, as the register snapshots are. Two
    of the four are frozen and will never differ between two days; the
    synthetic list is current and changes, and a lake that kept one copy of it
    would overwrite the only record of what it said before.
    """
    day = day if isinstance(day, str) else day.isoformat()
    return os.path.join(path_bronze_sheets(config_path), day, f"{name}.xlsx")


def path_bronze_collections(config_path):
    """Folder holding the record of what was collected, and when."""
    return os.path.join(path_bronze_base(config_path), "collections")


def path_register_record(config_path, day):
    """The record of one collection of the register."""
    day = day if isinstance(day, str) else day.isoformat()
    return os.path.join(path_bronze_collections(config_path),
                        f"register-{day}.json")


def path_sheet_record(config_path, day, name):
    """The record of one collection of one spreadsheet."""
    day = day if isinstance(day, str) else day.isoformat()
    return os.path.join(path_bronze_collections(config_path),
                        f"sheet-{name}-{day}.json")


def collected_snapshots(config_path):
    """The days the Bronze layer holds a snapshot of the register for.

    In order, oldest first. Diffing two of them is how the lake tells what
    left the register between the two, which no published file has recorded
    since May 2022.
    """
    folder = path_bronze_register(config_path)
    if not os.path.isdir(folder):
        return []
    days = []
    for name in os.listdir(folder):
        if not os.path.isdir(os.path.join(folder, name)):
            continue
        try:
            datetime.date.fromisoformat(name)
        except ValueError:
            continue
        days.append(name)
    return sorted(days)


def snapshot_pages(config_path, day, kind="parent"):
    """The pages of one kind one snapshot holds, in the order fetched."""
    folder = path_bronze_snapshot(config_path, day)
    if not os.path.isdir(folder):
        return []
    return sorted(os.path.join(folder, name) for name in os.listdir(folder)
                  if name.startswith(f"{kind}-") and name.endswith(".json"))


# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer, or ``None`` where it is a schema."""
    return resolve(config_path, "sts", "silver").path


# Reading and writing what the Bronze layer holds
# --------------------------------------------------------------------------


def read_json(path):
    """A JSON file of the Bronze layer, or ``None`` where it cannot be read.

    A record that cannot be read is treated as one that was never written,
    which costs a collection and nothing else. A *page* that cannot be read is
    a different matter and is not read through here: a snapshot with an
    unreadable page is incomplete, and the record says how many documents it
    should hold.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def write_bytes(path, content):
    """Write a file under a temporary name and put it in place once complete.

    A run that dies while writing leaves the file that was there before rather
    than half of the new one. It matters here because a snapshot is the only
    record of what the register held on the day it was taken: half a page is
    worse than no page, since the record would count it as collected.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    partial = path + ".partial"
    try:
        with open(partial, "wb") as handle:
            handle.write(content)
        os.replace(partial, path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


def write_json(path, content):
    """Write a small JSON file under a temporary name and put it in place."""
    write_bytes(path, json.dumps(content, ensure_ascii=False,
                                 indent=1).encode("utf-8"))


def create_directory_tree(config_path):
    """Create an empty data lake directory tree.

    Directories that already exist are left alone and missing parents are
    created along the way. It is meant for a local data lake; on Databricks the
    volume and the directories in it are provisioned outside the pipeline.
    """
    folders = [path_bronze_base(config_path),
               path_bronze_register(config_path),
               path_bronze_sheets(config_path),
               path_bronze_collections(config_path)]

    silver = resolve(config_path, "sts", "silver")
    if not silver.is_named and silver.path is not None:
        folders.append(silver.path)

    for folder in folders:
        os.makedirs(folder, exist_ok=True)


#: What this module offers. The layout of the data lake and the addresses of
#: the sources it is filled from.
__all__ = [
    "BREXIT_SHEET_URL",
    "CHILD_KINDS",
    "CORE_FIELDS",
    "DOCUMENT_KINDS",
    "PAGE_SIZE",
    "PAGE_SORT",
    "REGISTER_CORE",
    "REGISTER_URL",
    "SHEETS",
    "SHEET_CONTENT_TYPE",
    "SHEET_MAGIC",
    "SHEET_REFERENCES",
    "SOURCE",
    "collected_snapshots",
    "create_directory_tree",
    "path_bronze_base",
    "path_bronze_collections",
    "path_bronze_page",
    "path_bronze_register",
    "path_bronze_sheet",
    "path_bronze_sheets",
    "path_bronze_snapshot",
    "path_register_record",
    "path_sheet_record",
    "path_silver_base",
    "read_json",
    "snapshot_pages",
    "today",
    "write_bytes",
    "write_json",
]
