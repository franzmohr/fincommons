"""EDINET: the layout of the data lake.

The Bronze and the Silver layer are addressed through
:mod:`fincommons.addressing`, which reads the ``edinet`` section of the
configuration file, so that the same code runs against a local directory
tree, against a Unity Catalog volume and against a schema in a catalogue::

    edinet:
        bronze: /data/edinet/bronze
        silver: /data/edinet/silver

EDINET is the filing system of Japan's Financial Services Agency, the
counterpart of EDGAR, and it has required the financial statements of a
securities report to be tagged in XBRL since 2008. Its API publishes three
things: an index of what was filed on a given day, the filing itself, and --
this is what makes a module like this one possible -- the agency's own
conversion of the filing's XBRL into a CSV file. This module reads that
conversion, as :mod:`fincommons.financialreport.sec` reads the company files
of the Commission and :mod:`fincommons.financialreport.esef` the xBRL-JSON of
filings.xbrl.org. None of the three parses XBRL, and none needs a parser
installed.

**A filing leaves EDINET.** This is the one way the module differs in kind
from the other two, and everything about the Bronze layer follows from it. A
securities report is on public view for five years and retrievable for five
more, ten in all, and the specification says of what is older that it cannot
be obtained: 「既に 10 年を経過した書類については取得できません」. EDGAR keeps a
filing for ever and filings.xbrl.org keeps its archive; EDINET does not. The
Bronze layer is therefore not a convenience that saves a download, it is the
only lasting copy, and a year not collected is a year lost rather than a year
postponed. :data:`RETENTION_YEARS` is what the module knows about that, and
:func:`earliest_retrievable` turns it into a date.

The Bronze layer keeps what the agency publishes, as it was published.
``indexes`` holds one JSON file per calendar day, which is the unit the index
API answers in; ``documents`` holds one archive per filing collected; and
``collections`` a small record of when each was collected and what the server
said, which is how the next run knows what it need not download again. One
more file sits beside them: ``filers`` holds the agency's list of everyone who
can file, which is published as a zip on the disclosure site and is the one
thing here that needs no key.

The Silver layer holds the table of filings, one row per document the indexes
name. The table of facts belongs to the CSV conversion and waits on
:mod:`fincommons.financialreport.edinet.parse`; the schema is not declared
here until the layout of a real conversion has been read, rather than guessed
from a description of it.

The tables are keyed by the EDINET code of the filer. Two other identifiers
travel with it and neither is derived from the other: the securities code,
which is the ticker of a listed filer with a check digit appended, and the
corporate number of the National Tax Agency, which every Japanese legal
person has. The corporate number is worth naming because it is the first
identifier in this family of modules that a public register maps to the legal
entity identifier the ESEF tables are keyed by, and whether it reaches these
filers is a question for the data rather than for this docstring.
"""

import datetime
import json
import os

from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructType, TimestampType)

from ...addressing import resolve
from ...storage import read_silver

#: Where the API answers. Every request carries the key as a query parameter,
#: which is why the address of one must not be logged or recorded as it
#: stands; see :func:`fincommons.credentials.redacted`.
BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"

#: The index of what was filed on one calendar day. ``type=2`` asks for the
#: documents and the metadata; ``type=1`` for the metadata alone.
INDEX_URL = BASE_URL + "/documents.json"

#: One filing. ``type`` says which of its forms is wanted; see
#: :data:`DOCUMENT_KINDS`.
DOCUMENT_URL = BASE_URL + "/documents/{doc}"

#: The list of everyone who can file, as the agency publishes it on the
#: disclosure site: a zip holding one CSV. It needs no key, which is why a
#: lake can name its filers before it can collect a filing from them.
FILERS_URL = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip"

#: The same list with the filers' names in English.
FILERS_URL_EN = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelisteng/Edinetcode.zip"

#: The name the publisher is given in log records and messages.
SOURCE = "edinet-fsa.go.jp"

#: What the agency will hand over for a filing, by the ``type`` of a request.
#: ``csv`` is the agency's own conversion of the XBRL, which is what this
#: module reads; it is available only where the index says so.
DOCUMENT_KINDS = {
    "main": 1,     # 提出本文書及び監査報告書, with the XBRL
    "pdf": 2,      # the PDF the disclosure site offers
    "attached": 3,  # 代替書面・添付文書
    "english": 4,  # 英文ファイル, where the filer lodged one
    "csv": 5,      # the XBRL converted to CSV
}

#: The documents that carry a set of financial statements, by the code the
#: index states. A correction carries the code of its own kind rather than of
#: what it corrects, so both are named.
#:
#: Most of what an index names is not here, and the two largest absences are
#: worth naming. Every report is accompanied by a 確認書, the management
#: confirmation, which the index codes ``'135'``; of the 2,118 documents filed
#: on 13 November 2026, 868 were those and 894 were semi-annual reports. And
#: a report filed for an investment fund carries a ``FUNDCODE`` and reports on
#: the fund rather than on the filer, which is most of the semi-annual reports
#: of any given day. A lake after the statements of operating companies wants
#: these codes *and* an empty fund code.
REPORT_TYPES = {
    "120": "annual",            # 有価証券報告書
    "130": "annual-correction",  # 訂正有価証券報告書
    "140": "quarterly",         # 四半期報告書
    "150": "quarterly-correction",  # 訂正四半期報告書
    "160": "semiannual",        # 半期報告書
    "170": "semiannual-correction",  # 訂正半期報告書
}

#: The annual report and its corrections, which is what a lake wanting one
#: set of statements a year wants.
ANNUAL_TYPES = ("120", "130")

#: How long a filing stays retrievable. Five years on public view and five
#: more of the administrative extension; what is older is gone, and no
#: parameter of the API reaches it.
RETENTION_YEARS = 10

#: The first day the API answers for at all, whatever the retention would
#: allow: the system was renewed on 4 January 2023 and reaches back ten years
#: from a filing's own date, not from that one.
FIRST_DATE = datetime.date(2013, 1, 4)

#: The encoding of the filers list. The agency writes it in the Japanese
#: industrial encoding rather than in UTF-8, so a reader that assumes UTF-8
#: gets a file of replacement characters and no error.
FILERS_ENCODING = "cp932"


def earliest_retrievable(today=None):
    """The oldest day the API can still be asked about.

    A run that asks for a day older than this gets an empty index rather than
    an error, which is how a lake that believes it is collecting ten years of
    history quietly collects what is left of it. The caller is told the date
    instead.
    """
    today = today or datetime.date.today()
    try:
        bound = today.replace(year=today.year - RETENTION_YEARS)
    except ValueError:  # 29 February
        bound = today.replace(year=today.year - RETENTION_YEARS, day=28)
    return max(bound, FIRST_DATE)


filings_schema = StructType() \
    .add("FILING", StringType(), False) \
    .add("EDINETCODE", StringType(), True) \
    .add("SECCODE", StringType(), True) \
    .add("JCN", StringType(), True) \
    .add("NAME", StringType(), True) \
    .add("FUNDCODE", StringType(), True) \
    .add("ORDINANCE", StringType(), True) \
    .add("FORM", StringType(), True) \
    .add("DOCTYPE", StringType(), True) \
    .add("PERIODSTART", DateType(), True) \
    .add("PERIODEND", DateType(), True) \
    .add("SUBMITTED", TimestampType(), True) \
    .add("FILEDYEAR", IntegerType(), False) \
    .add("INDEXDATE", DateType(), False) \
    .add("DESCRIPTION", StringType(), True) \
    .add("PARENT", StringType(), True) \
    .add("WITHDRAWN", StringType(), True) \
    .add("EDITED", StringType(), True) \
    .add("DISCLOSED", StringType(), True) \
    .add("LEGAL", StringType(), True) \
    .add("HASXBRL", StringType(), True) \
    .add("HASCSV", StringType(), True) \
    .add("HASENGLISH", StringType(), True)
"""The shape of the table of filings: one row per document the indexes name.

``FILING`` is the 書類管理番号, the document management number the API
addresses a filing by, and the key of everything else here. ``INDEXDATE`` is
the calendar day whose index the row came from, which is what a run collects
by; ``FILEDYEAR`` is its year and the partition column, as it is on the table
of facts of the other two regimes.

``HASCSV`` is the column the module turns on. The agency converts the XBRL of
a filing to CSV where it can, and says per filing whether it did; a filing
whose flag is not ``'1'`` contributes nothing to the table of facts, exactly
as an ESEF filing with no ``JSONURL`` does. ``HASXBRL`` says the same of the
XBRL itself, and ``HASENGLISH`` whether the filer lodged an English version,
which is not a translation of the Japanese one but a document in its own
right.

Four states travel with a filing and all four are carried rather than acted
on. ``WITHDRAWN`` marks a filing taken back and the document that took it
back; ``EDITED`` a filing an official of a finance bureau has corrected;
``DISCLOSED`` one withheld from disclosure; and ``LEGAL`` whether it is on
public view, in the extension period, or past both. A filing in the last
state is still named by an index collected earlier and can no longer be
fetched, which is the thing this module's Bronze layer exists to survive.

Eight of the twenty-nine fields an index carries are deliberately absent,
for two reasons. ``issuerEdinetCode``, ``subjectEdinetCode``,
``subsidiaryEdinetCode`` and ``currentReportReason`` belong to the
large-holding report, the takeover bid and the extraordinary report, and are
empty on every document this module is about: a column that is always empty
would say nothing, while an absent one says it to anyone who looks at the
table. ``seqNumber`` numbers a row within the day's index and means nothing
once two days are in one table; ``opeDateTime`` is when an official of a
finance bureau touched the record, which ``EDITED`` already says; and
``pdfFlag`` and ``attachDocFlag`` offer forms of the filing this module does
not read.
"""

facts_schema = StructType() \
    .add("EDINETCODE", StringType(), True) \
    .add("FILING", StringType(), False) \
    .add("FILEDYEAR", IntegerType(), False) \
    .add("FACT", StringType(), False) \
    .add("CONCEPT", StringType(), False) \
    .add("LABEL", StringType(), True) \
    .add("CONTEXT", StringType(), False) \
    .add("RELATIVE", StringType(), True) \
    .add("BASIS", StringType(), True) \
    .add("SHAPE", StringType(), True) \
    .add("PRDSTART", DateType(), True) \
    .add("PRDEND", DateType(), True) \
    .add("DIMENSIONS", StringType(), True) \
    .add("UNIT", StringType(), True) \
    .add("VALUE", DoubleType(), True) \
    .add("TEXT", StringType(), True) \
    .add("DOCFORM", StringType(), True)
"""The shape of the table of facts: one row per fact of the agency's CSV.

The columns a fact shares with the other regimes are spelled as they spell
them -- ``FILING``, ``FACT``, ``CONCEPT``, ``PRDSTART``, ``PRDEND``,
``DIMENSIONS``, ``UNIT``, ``VALUE``, ``TEXT`` -- so that a module combining
them is combining tables of one shape rather than translating between them.

**The period is resolved rather than reported, and this is the one thing to
know before using it.** The agency's conversion does not carry the dates of a
fact's context. It carries a label saying which period the context is
relative to the filing's own -- ``CurrentYearDuration``,
``Prior4YearDuration``, ``FilingDateInstant`` -- and ``相対年度`` says the same
in words.

For the current year and the comparative, the dates are the filer's own: a
filing states both among its facts, in the document and entity information,
and those are used as reported. For anything further back the period is
shifted by whole years from that anchor, and a shift is exact only where the
filer's years are of equal length -- a filer on a 52- or 53-week calendar, or
one that moved its year end, has years that are not a whole number of years
apart. A report stating five years of net sales is therefore anchored for two
of them and shifted for three.

``CONTEXT`` and ``RELATIVE`` are carried verbatim beside the dates so that
what each one rested on can always be read, and a context whose shape is not
recognised leaves both dates null rather than taking a guess. The XBRL
instance of the filing carries the true dates for every context; a lake that
needs them for the older years needs the instance and a parser, which is not
this module.

``CONCEPT`` is the element identifier as the conversion writes it, prefix and
name. Four prefixes appear: ``jppfs_cor`` is the financial statements
taxonomy, ``jpcrp_cor`` the disclosure ordinance, ``jpdei_cor`` the document
and entity information, and a prefix built from the form and the filer's own
EDINET code -- ``jpcrp030000-asr_E03270-000`` -- marks a concept the filer
created for its own report, which is how an extension is told from the
taxonomy here.

``LABEL`` is the Japanese name of the element, which the conversion states
beside it. Neither of the other two regimes carries a label: the ESEF labels
live in the extension taxonomy the module does not read, and the Commission
publishes none. It is kept because it is the only thing in these tables that
says what a concept means to a reader who does not know the taxonomy.

``VALUE`` holds a number and ``TEXT`` everything else, and a fact can have a
unit and no number all the same: the conversion writes ``－`` where a figure
does not apply, forty-eight times in one report of 1,086 facts, and that
becomes a null ``VALUE`` rather than a zero or an error.

``DOCFORM`` says which member of the archive a fact came from, by the form
that names it: the report itself (``jpcrp030000-asr``) or the audit report
(``jpaud-aai-cn``), which the agency converts as well and which is where the
auditor and the opinion are.

``FACT`` is an identifier made for the fact, the conversion giving it none.
The element and the context do not identify one: a figure is often tagged in
several places in a report, and the number of employees appears three times
under one context in that same report. An occurrence number is therefore part
of it, which keeps every row of the conversion and lets a check rather than
the reader decide what to do about a figure tagged twice.
"""

filers_schema = StructType() \
    .add("EDINETCODE", StringType(), False) \
    .add("KIND", StringType(), True) \
    .add("LISTED", StringType(), True) \
    .add("CONSOLIDATED", StringType(), True) \
    .add("CAPITAL", StringType(), True) \
    .add("YEAREND", StringType(), True) \
    .add("NAME", StringType(), True) \
    .add("NAMEEN", StringType(), True) \
    .add("NAMEKANA", StringType(), True) \
    .add("ADDRESS", StringType(), True) \
    .add("INDUSTRY", StringType(), True) \
    .add("SECCODE", StringType(), True) \
    .add("JCN", StringType(), True)
"""The shape of the table of filers: one row per filer the agency lists.

The agency's own list, read from the zip it publishes on the disclosure site.
It is the counterpart of the Commission's list of registrants and, like it,
is how a lake that wants companies by name or by ticker finds the codes to
collect them by. It needs no API key.

``YEAREND`` is carried as the agency writes it -- ``'5月31日'`` for a filer
closing on 31 May -- and not as a date, there being no year in it. It is
worth having all the same: it says which filers close on a day other than
31 March, and a comparison that assumes the Japanese financial year ends in
March is wrong for every one of them. ``CAPITAL`` is likewise the figure as
published, in millions of yen.

``SECCODE`` is the securities code: the four-digit ticker of a listed filer
with a check digit appended, so ``'13760'`` is ticker 1376. ``JCN`` is the
corporate number of the National Tax Agency, thirteen digits, which every
Japanese legal person has and which is the identifier a register outside
EDINET is most likely to share.
"""


# The Bronze layer
# --------------------------------------------------------------------------


def path_bronze_base(config_path):
    return resolve(config_path, "edinet", "bronze").path


def path_bronze_indexes(config_path):
    """Folder holding one index file per calendar day, as published."""
    return os.path.join(path_bronze_base(config_path), "indexes")


def path_bronze_index(config_path, day):
    """The index of one calendar day."""
    return os.path.join(path_bronze_indexes(config_path), f"{day}.json")


def path_bronze_documents(config_path):
    """Folder holding one archive per filing collected."""
    return os.path.join(path_bronze_base(config_path), "documents")


def path_bronze_document(config_path, doc, kind="csv"):
    """One form of one filing, as the agency handed it over."""
    return os.path.join(path_bronze_documents(config_path), kind, f"{doc}.zip")


def path_bronze_filers(config_path):
    """The agency's list of everyone who can file."""
    return os.path.join(path_bronze_base(config_path), "filers",
                        "Edinetcode.zip")


def path_bronze_collections(config_path):
    """Folder holding the record of what was collected, and when."""
    return os.path.join(path_bronze_base(config_path), "collections")


def path_index_record(config_path, day):
    """The record of when one day's index was collected."""
    return os.path.join(path_bronze_collections(config_path), f"{day}.json")


# The Silver layer
# --------------------------------------------------------------------------


def path_silver_base(config_path):
    """Directory of the Silver layer, or ``None`` where it is a schema."""
    return resolve(config_path, "edinet", "silver").path


def ref_silver_facts(config_path):
    """Where the table of facts lives."""
    return resolve(config_path, "edinet", "silver", "facts")


def path_silver_facts(config_path):
    """Path of the table of facts, or ``None`` where it is named."""
    return ref_silver_facts(config_path).path


def ref_silver_filings(config_path):
    """Where the table of filings lives."""
    return resolve(config_path, "edinet", "silver", "filings")


def path_silver_filings(config_path):
    """Path of the table of filings, or ``None`` where it is named."""
    return ref_silver_filings(config_path).path


def ref_silver_filers(config_path):
    """Where the table of filers lives."""
    return resolve(config_path, "edinet", "silver", "filers")


def path_silver_filers(config_path):
    """Path of the table of filers, or ``None`` where it is named."""
    return ref_silver_filers(config_path).path


def ref_silver_issues(config_path):
    """Where the table of data quality issues lives.

    It sits in the Silver layer beside the tables it is derived from, which is
    where a table derived from a Silver table belongs: it is read from the
    tables of this module and from nothing else, and a lake that never runs
    the checks simply has no such table. No section of the configuration file
    has to be added for it.
    """
    return resolve(config_path, "edinet", "silver", "issues")


def path_silver_issues(config_path):
    """Path of the table of issues, or ``None`` where it is named."""
    return ref_silver_issues(config_path).path


def connect_to_edinet(spark, config_path, table="filings"):
    """Open a table of the Silver layer.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    table: str
        Name of the table, ``'facts'``, ``'filings'``, ``'filers'`` or
        ``'issues'`` -- the last being the table
        :mod:`fincommons.financialreport.edinet.quality` writes, which a lake
        that has never run the checks does not have.
    """
    if table == "facts":
        return read_silver(spark, ref_silver_facts(config_path))
    if table == "filings":
        return read_silver(spark, ref_silver_filings(config_path))
    if table == "filers":
        return read_silver(spark, ref_silver_filers(config_path))
    if table == "issues":
        return read_silver(spark, ref_silver_issues(config_path))
    raise ValueError(
        f"Table {table!r} not recognised. Use 'facts', 'filings', 'filers' "
        "or 'issues'.")


def read_json(path):
    """A small JSON file, or ``None`` where it cannot be read.

    A record that cannot be read is treated as one that was never written,
    which costs a download and nothing else.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def write_bytes(path, content):
    """Write a file under a temporary name and put it in place once complete.

    A run that dies while writing leaves the file that was there before,
    rather than half of the new one. It matters more here than in the other
    regimes: what this layer holds cannot be fetched again once a filing has
    left public view.
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
    created along the way. It is meant for a local data lake; on Databricks
    the volume and the directories in it are provisioned outside the pipeline.
    """
    folders = [path_bronze_base(config_path),
               path_bronze_indexes(config_path),
               path_bronze_documents(config_path),
               path_bronze_collections(config_path),
               os.path.dirname(path_bronze_filers(config_path))]

    silver = resolve(config_path, "edinet", "silver")
    if not silver.is_named and silver.path is not None:
        folders.append(silver.path)

    for folder in folders:
        os.makedirs(folder, exist_ok=True)


#: What this module offers. The layout of the data lake, and the tables read
#: from it.
__all__ = [
    "ANNUAL_TYPES",
    "BASE_URL",
    "DOCUMENT_KINDS",
    "DOCUMENT_URL",
    "FILERS_ENCODING",
    "FILERS_URL",
    "FILERS_URL_EN",
    "FIRST_DATE",
    "INDEX_URL",
    "REPORT_TYPES",
    "RETENTION_YEARS",
    "SOURCE",
    "connect_to_edinet",
    "create_directory_tree",
    "earliest_retrievable",
    "facts_schema",
    "filers_schema",
    "filings_schema",
    "path_bronze_base",
    "path_bronze_collections",
    "path_bronze_document",
    "path_bronze_documents",
    "path_bronze_filers",
    "path_bronze_index",
    "path_bronze_indexes",
    "path_index_record",
    "path_silver_base",
    "path_silver_facts",
    "path_silver_filers",
    "path_silver_filings",
    "path_silver_issues",
    "read_json",
    "ref_silver_facts",
    "ref_silver_filers",
    "ref_silver_filings",
    "ref_silver_issues",
    "write_bytes",
    "write_json",
]
