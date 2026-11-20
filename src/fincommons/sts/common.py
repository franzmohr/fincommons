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
645 are private and carry sixteen to nineteen fields and no child documents at
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
import re

from pyspark.sql.types import (DateType, IntegerType, StringType, StructType)

from ..addressing import resolve
from ..storage import read_silver

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
#: The register of repositories carries none.
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


# The tables of the Silver layer
# --------------------------------------------------------------------------

#: The template fields of a notification begin with one of these. Everything
#: else a notification carries is a field of the notification itself. The two
#: sets of assertions are those of a non-ABCP and of an ABCP securitisation;
#: the register spells the second in upper case and the first in a mixture,
#: which is why the test is on the prefix and not on the whole name.
TEMPLATE_PREFIXES = ("non_abcp", "abcp")

#: What a template field's name ends in, by the part of an assertion it holds.
#: A field with none of these endings carries the assertion itself, as
#: ``non_abcp_Riskretention_and_RetainingRiskoption_Firstlosstranche`` does.
TEMPLATE_SUFFIXES = {
    "_flag": "flag",
    "_FLAG": "flag",
    "_comment": "comment",
    "_COMMENT": "comment",
    "_EXPL": "comment",
    "_explanation": "comment",
}

#: The composition of a securitisation identifier that the disclosure
#: templates use: the legal entity identifier of the reporting entity, a
#: letter for the class, and the period.
_LEI_IDENTIFIER = re.compile(r"^[A-Z0-9]{20}[A-Z][0-9]{6}$")


def identifier_scheme(value):
    """Which scheme a securitisation identifier follows.

    Only ``'lei'`` joins to anything outside the register: it is the
    identifier of the disclosure templates, and a securitisation reported
    through a securitisation repository is named by the same string. A
    private notification carries a placeholder that identifies nothing, and
    three notifications of the snapshot of 20 November 2026 carry a single
    space.

    Parameters
    ----------
    value: str or None
        The identifier as the register published it, leading space and all.

    Returns
    -------
    str
        ``'lei'``, ``'private'``, ``'synthetic'``, ``'missing'`` or
        ``'other'``.
    """
    trimmed = (value or "").strip()
    if not trimmed:
        return "missing"
    if _LEI_IDENTIFIER.match(trimmed):
        return "lei"
    upper = trimmed.upper()
    if upper.startswith("SYSEC"):
        return "synthetic"
    if upper.startswith("PRIVATE"):
        return "private"
    return "other"


notification_schema = StructType() \
    .add("id", StringType(), False) \
    .add("securitisation_identifier", StringType(), True) \
    .add("notification_identifier", StringType(), True) \
    .add("identifier_scheme", StringType(), False) \
    .add("securitisation_name", StringType(), True) \
    .add("securitisation_type", StringType(), True) \
    .add("securitisation_class", StringType(), True) \
    .add("type_code", StringType(), True) \
    .add("class_code", StringType(), True) \
    .add("compliance_status", StringType(), True) \
    .add("sanction_status", StringType(), True) \
    .add("underlying_exposure", StringType(), True) \
    .add("first_notified", DateType(), True) \
    .add("notified", DateType(), True) \
    .add("repository", StringType(), True) \
    .add("reporting_entity_name", StringType(), True) \
    .add("reporting_entity_lei", StringType(), True) \
    .add("reporting_entity_legal_name", StringType(), True) \
    .add("verifier_name", StringType(), True) \
    .add("verifier_country", StringType(), True) \
    .add("verifier_authority", StringType(), True) \
    .add("verifier_statement", StringType(), True) \
    .add("prospectus_country", StringType(), True) \
    .add("prospectus_identifier", StringType(), True) \
    .add("prospectus_issue_date", DateType(), True) \
    .add("notified_again", StringType(), True) \
    .add("notified_again_reason", StringType(), True) \
    .add("fields", IntegerType(), False) \
    .add("snapshot", DateType(), False)
"""One row per notification the register holds on the day of the snapshot.

``id`` is the register's own document identifier and the key of this table
and of every other table of the module. It is the key because **neither
identifier the register publishes is unique**: the 1,147 notifications of the
snapshot of 20 November 2026 carried 1,115 distinct securitisation
identifiers and 1,131 distinct notification identifiers, a securitisation
being notified again when it is restructured or re-notified.

``identifier_scheme`` says whether ``securitisation_identifier`` joins to
anything outside the register; see :func:`identifier_scheme`. Of that
snapshot, 502 notifications carry an identifier of the disclosure templates
and 645 a placeholder.

``reporting_entity_name`` and ``reporting_entity_lei`` are the entity that
made the notification, which the register names ``de_name`` and
``de_lei_priv`` and defines nowhere. It is the sole originator on 394 of the
502 notifications that carry it, one of several on 25, and on 74 it is not an
originator at all but the special purpose entity or a group entity reporting
on another's behalf. It is therefore carried as what it is -- the entity that
reported -- and the originators are in the table of parties.
``reporting_entity_legal_name`` is the same entity as its legal entity
identifier records it, which differs from the name the notification was filed
under: ``Lloyds Bank GmbH`` against ``LLOYDS BANK GMBH``.

``verifier_statement`` is what the authorised third party of Article 28 said
when it verified compliance, which 485 of the 502 public notifications of that
snapshot carry. The verifier's name is not normalised in the register -- one
of them appears under thirteen spellings -- so a count by verifier is a count
of spellings until they are mapped.

``fields`` is how many fields the document carried, which separates the two
populations at a glance: a public notification carries a median of 147 and a
private one 18.
"""

assertion_schema = StructType() \
    .add("id", StringType(), False) \
    .add("securitisation_identifier", StringType(), True) \
    .add("template", StringType(), False) \
    .add("requirement", StringType(), False) \
    .add("flag", StringType(), True) \
    .add("comment", StringType(), True) \
    .add("snapshot", DateType(), False)
"""One row per assertion a notification makes against the STS criteria.

The template is some 130 fields of a public notification, each a flag with an
explanatory comment beside it, and the private notifications carry none of
them. They are held long rather than wide for two reasons. As columns they
would be a table 130 wide and half empty, and the template is revised: the
review of the securitisation framework proposes a reduction of at least a
third in the fields of the public templates, which as rows is a vocabulary
that grows and as columns is a migration.

``template`` is ``'non_abcp'`` or ``'abcp'``, the two sets of criteria.
``requirement`` is the register's own field name with that prefix and the
``_flag`` or ``_comment`` ending removed, so that the flag and the comment of
one requirement meet in one row and nothing is renamed. The register's own
spelling is kept, typographical errors included -- the family of transparency
assertions is spelled ``requirementsOnTrasparency`` -- because a name invented
here would not be findable in what ESMA publishes.

``flag`` is the code as published. The register documents no domain for it;
the values seen are ``C``, ``N``, ``E``, ``A`` and ``Y``. It is therefore
carried and not interpreted.
"""

tranche_schema = StructType() \
    .add("id", StringType(), False) \
    .add("securitisation_identifier", StringType(), True) \
    .add("isin", StringType(), True) \
    .add("instrument_name", StringType(), True) \
    .add("snapshot", DateType(), False)
"""One row per tranche of a notification, by its ISIN.

The ISIN joins to :mod:`fincommons.firds`, which gives the instrument, its
issuer and the venues it trades on. The securitisation identifier is carried
beside it so that the join does not have to go through the notification.

Only public notifications have tranches: of the snapshot of 20 November 2026,
498 of the 502 public notifications carried 2,275 tranche documents between
them, and the private ones carried none. Ten of those documents carry no ISIN
and 267 no instrument name.
"""

party_schema = StructType() \
    .add("id", StringType(), False) \
    .add("securitisation_identifier", StringType(), True) \
    .add("role", StringType(), False) \
    .add("lei", StringType(), True) \
    .add("name", StringType(), True) \
    .add("country", StringType(), True) \
    .add("country_name", StringType(), True) \
    .add("snapshot", DateType(), False)
"""One row per party to a notification, whatever its role.

``role`` is ``'originator'``, ``'lender'`` for the original lender,
``'sponsor'`` or ``'sspe'`` for the securitisation special purpose entity.
The register carries each as a child document of its own with the same four
attributes under four prefixes, so they are one table with a column for the
role rather than four tables of one shape.

**The special purpose entity has no identifier and no name in the register**,
only a country, so its rows carry nulls in ``lei`` and ``name``. That is a gap
in the register and not in the reading of it. The other three roles carry a
legal entity identifier, which joins to :mod:`fincommons.entitymasterdata`.
"""

event_schema = StructType() \
    .add("securitisation_identifier", StringType(), False) \
    .add("identifier_scheme", StringType(), False) \
    .add("event", StringType(), False) \
    .add("event_date", DateType(), True) \
    .add("reason", StringType(), True) \
    .add("source", StringType(), False) \
    .add("source_as_of", DateType(), True)
"""One row per thing that is known to have happened to a securitisation.

The status of a securitisation is a fold over this table rather than a column
of the table of notifications, because no source states it. The register gives
the current state and no history; the two frozen spreadsheets give
notifications and cancellations with their dates and ESMA's reasons; and the
difference between two snapshots gives a departure with no reason at all.

``event`` is one of:

``notified``
    the securitisation was notified, from the register's first notification
    date or from the frozen list,
``updated``
    its notification moved, the register's notification date differing from
    its first,
``non_compliant``
    the register marks it as not compliant on the day of the snapshot,
``cancelled``
    the cancellation list records it, with ESMA's reason in ``reason``,
``left_register``
    it was in one snapshot and not in the next, which is the only record of a
    removal there is for anything that happened after May 2022.

``source`` names where the row came from and ``source_as_of`` the day that
source speaks for, because three of the sources are frozen and will never
move again. Contradictions are kept rather than resolved: two securitisations
cancelled in March 2021 are still in the register and still marked compliant,
and the ten the register marks non-compliant appear in no cancellation list.
"""

synthetic_schema = StructType() \
    .add("securitisation_identifier", StringType(), False) \
    .add("notification_number", IntegerType(), True) \
    .add("notified", DateType(), True) \
    .add("securitisation_type", StringType(), True) \
    .add("protection", StringType(), True) \
    .add("instrument", StringType(), True) \
    .add("underlying_exposure", StringType(), True) \
    .add("securitisation_name", StringType(), True) \
    .add("originator_country", StringType(), True) \
    .add("sanction_status", StringType(), True) \
    .add("source_as_of", DateType(), False)
"""One row per synthetic securitisation notified as STS.

A table of its own, and not rows of the table of notifications, because the
source is a different one and says different things. A synthetic STS
securitisation is notified to ESMA by electronic mail and appears in no
register; ESMA publishes the list as a spreadsheet, and that spreadsheet has
columns the register has not -- whether the credit protection is funded, and
whether it is a credit derivative or a financial guarantee -- and lacks
almost everything the register carries. Every row is private, none has an
ISIN, an originator country or a name, and the identifiers are a scheme of
their own, ``SYSEC_Private_2026_10_13_01``, which overlaps with nothing.
"""


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


#: The tables of the Silver layer, by the name they are read under.
SILVER_TABLES = ("notification", "assertion", "tranche", "party", "event",
                 "synthetic")


def ref_silver(config_path, table):
    """Where one table of the Silver layer lives."""
    if table not in SILVER_TABLES:
        raise ValueError(
            f"Table {table!r} not recognised. Use one of "
            + ", ".join(repr(t) for t in SILVER_TABLES) + ".")
    return resolve(config_path, "sts", "silver", table)


def path_silver(config_path, table):
    """Path of one table of the Silver layer, or ``None`` where it is named."""
    return ref_silver(config_path, table).path


def connect_to_sts(spark, config_path, table="notification"):
    """Open a table of the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    table: str
        Which table: ``'notification'``, ``'assertion'``, ``'tranche'``,
        ``'party'``, ``'event'`` or ``'synthetic'``. A lake that has never
        built the Silver layer has none of them.
    """
    return read_silver(spark, ref_silver(config_path, table))


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
    "SILVER_TABLES",
    "SOURCE",
    "TEMPLATE_PREFIXES",
    "TEMPLATE_SUFFIXES",
    "assertion_schema",
    "collected_snapshots",
    "connect_to_sts",
    "create_directory_tree",
    "event_schema",
    "identifier_scheme",
    "notification_schema",
    "party_schema",
    "path_bronze_base",
    "path_bronze_collections",
    "path_bronze_page",
    "path_bronze_register",
    "path_bronze_sheet",
    "path_bronze_sheets",
    "path_bronze_snapshot",
    "path_register_record",
    "path_sheet_record",
    "path_silver",
    "path_silver_base",
    "read_json",
    "ref_silver",
    "snapshot_pages",
    "synthetic_schema",
    "today",
    "tranche_schema",
    "write_bytes",
    "write_json",
]
