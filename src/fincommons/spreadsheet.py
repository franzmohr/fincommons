"""Tables written as an OpenDocument Spreadsheet.

A table that leaves the lake for a reader rather than for another program is
read in a spreadsheet, and the format it is written in is a decision that
outlives the file. This module writes `OpenDocument
<https://docs.oasis-open.org/office/OpenDocument/v1.3/>`_ spreadsheets,
``.ods``: an OASIS standard, published as ISO/IEC 26300, implemented by
LibreOffice, Gnumeric, Calligra, Excel and Google Sheets alike, and a format
whose reader is free. It is written here rather than taken from a library
because the whole of it is a handful of XML files in a zip archive, and a
package that keeps its dependencies to what a Databricks runtime already has
should not add one to write them.

What a writer of a workbook has to get right, and what this one does:

* **the types.** A number written as text is a number a spreadsheet will not
  sum, and a date written as text is one it will not sort. Each value is
  written with its OpenDocument value type -- ``float``, ``date``,
  ``boolean``, ``string`` -- so a column of figures arrives as figures.
* **the header.** The first row of a sheet is set in bold and declared a
  header row, which is the row a spreadsheet repeats at the top of every
  printed page and the one this module freezes on screen, so that the names
  of the columns stay in view in a sheet of ten thousand rows.
* **the widths.** A column is as wide as what stands in it, within reason, so
  that a sheet opens readable rather than as a row of ``###``.
* **no formulas.** A value is a value. A text that begins with ``=`` is
  written as a text, because a formula in OpenDocument is an attribute of its
  own and nothing a cell's content can turn into -- the injection a CSV
  export has to think about does not arise here.

One call writes a file::

    from fincommons.spreadsheet import Sheet, write_ods

    write_ods("revenue.ods", [
        Sheet("revenue", ["issuer", "period_end", "revenue"], [
            ("voestalpine AG", date(2026, 3, 31), 16_000_000_000.0),
            ("Issuer AG", date(2025, 12, 31), None),
        ]),
    ])

The rows of a sheet are any iterable of sequences and are read once; a value
of ``None``, an empty text and a ``NaN`` are all written as an empty cell,
which is what a spreadsheet means by a figure that is not there.
"""

from __future__ import annotations

import datetime
import math
import os
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence
from xml.etree import ElementTree as ElementTreeModule
from xml.etree.ElementTree import Element, SubElement, tostring

#: The media type of an OpenDocument spreadsheet, as the archive declares it.
MEDIA_TYPE = "application/vnd.oasis.opendocument.spreadsheet"

#: The suffix of the file.
SUFFIX = ".ods"

#: The version of the OpenDocument specification the files declare.
VERSION = "1.3"

#: The characters a sheet name may not hold, which a spreadsheet reads as the
#: syntax of a cell reference, and the character they are replaced by.
FORBIDDEN_IN_NAME = "[]*?:/\\'\""

#: How wide a column is allowed to get, and the narrowest it is written, in
#: centimetres, and what one character of content is taken to be worth.
MIN_WIDTH_CM = 1.8
MAX_WIDTH_CM = 14.0
WIDTH_PER_CHARACTER_CM = 0.21

_NAMESPACES = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
    "fo": "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0",
    "number": "urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0",
    "meta": "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "manifest": "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0",
    "config": "urn:oasis:names:tc:opendocument:xmlns:config:1.0",
    "dc": "http://purl.org/dc/elements/1.1/",
}

for _prefix, _uri in _NAMESPACES.items():
    ElementTreeModule.register_namespace(_prefix, _uri)


def _tag(name):
    """``office:value-type`` as the qualified name ElementTree wants."""
    prefix, local = name.split(":", 1)
    return f"{{{_NAMESPACES[prefix]}}}{local}"


@dataclass
class Sheet:
    """One sheet of a workbook: a name, a header and the rows.

    ``header`` may be ``None`` for a sheet whose first row is content, in
    which case nothing is set in bold, repeated or frozen. The rows are read
    once, so a generator may be passed.
    """

    name: str
    header: Sequence | None = None
    rows: Iterable[Sequence] = ()


def write_ods(path, sheets, *, created=None, title=None, generator=None):
    """Write sheets to an OpenDocument spreadsheet, and return its path.

    Parameters
    ----------
    path: str
        Where to write the file. ``.ods`` is appended where the name does not
        end in it, and the directories above it are created.
    sheets: Iterable[Sheet]
        The sheets, in the order the workbook holds them. A name is trimmed of
        what a sheet name may not hold and made unique, since a workbook with
        two sheets of one name is not a workbook.
    created: datetime.datetime, optional
        What the file states as the moment it was written. The current moment
        by default; a fixed one makes the bytes reproducible.
    title: str, optional
        The title in the metadata of the document.
    generator: str, optional
        What wrote the file, in the metadata. The package and its version by
        default.
    """
    sheets = _read(sheets)
    if not sheets:
        raise ValueError("a spreadsheet needs at least one sheet")

    path = str(path)
    if not path.lower().endswith(SUFFIX):
        path += SUFFIX
    folder = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)

    created = created or datetime.datetime.now()
    files = {
        "META-INF/manifest.xml": _manifest(),
        "content.xml": _content(sheets),
        "styles.xml": _styles(),
        "meta.xml": _meta(created, title, generator),
        "settings.xml": _settings(sheets),
    }

    stamp = created.timetuple()[:6]
    partial = path + ".partial"
    try:
        with zipfile.ZipFile(partial, "w", zipfile.ZIP_DEFLATED) as archive:
            # The media type is the first entry of the archive and is stored
            # uncompressed, which is how a reader recognises the format
            # without unpacking anything.
            first = zipfile.ZipInfo("mimetype", date_time=stamp)
            first.compress_type = zipfile.ZIP_STORED
            archive.writestr(first, MEDIA_TYPE.encode("ascii"))
            for name, content in files.items():
                entry = zipfile.ZipInfo(name, date_time=stamp)
                entry.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(entry, content)
        os.replace(partial, path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)
    return path


def _read(sheets):
    """The sheets with their rows read and their names made unique.

    The rows are read here and not again: they may be a generator, and the
    widths of the columns are decided by what stands in them.
    """
    read = []
    taken = set()
    for sheet in sheets:
        name = sheet_name(sheet.name, taken)
        taken.add(name.lower())
        header = None if sheet.header is None else list(sheet.header)
        read.append(Sheet(name, header, [list(row) for row in sheet.rows]))
    return read


def sheet_name(name, taken=()):
    """A sheet name a spreadsheet accepts, and not one already taken.

    What a name may not hold is replaced by a space, a name of nothing becomes
    ``sheet``, and one that is taken already is numbered. Names are compared
    without regard to case, as a spreadsheet compares them.
    """
    cleaned = "".join(" " if character in FORBIDDEN_IN_NAME else character
                      for character in str(name)).strip()
    cleaned = " ".join(cleaned.split())[:120] or "sheet"
    taken = {held.lower() for held in taken}
    if cleaned.lower() not in taken:
        return cleaned
    for number in range(2, 1000):
        candidate = f"{cleaned} ({number})"
        if candidate.lower() not in taken:
            return candidate
    raise ValueError(f"too many sheets named like {name!r}")


# The documents of the archive
# --------------------------------------------------------------------------


def _xml(root):
    """A document of the archive, as bytes."""
    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(
        root, encoding="utf-8", xml_declaration=False)


def _manifest():
    root = Element(_tag("manifest:manifest"),
                   {_tag("manifest:version"): VERSION})
    entries = [("/", MEDIA_TYPE), ("content.xml", "text/xml"),
               ("styles.xml", "text/xml"), ("meta.xml", "text/xml"),
               ("settings.xml", "text/xml")]
    for full_path, media_type in entries:
        attributes = {_tag("manifest:full-path"): full_path,
                      _tag("manifest:media-type"): media_type}
        if full_path == "/":
            attributes[_tag("manifest:version")] = VERSION
        SubElement(root, _tag("manifest:file-entry"), attributes)
    return _xml(root)


def _meta(created, title, generator):
    if generator is None:
        try:                                    # pragma: no cover - installed
            from importlib.metadata import version
            generator = f"fincommons/{version('fincommons')}"
        except Exception:                       # pragma: no cover - source tree
            generator = "fincommons"
    root = Element(_tag("office:document-meta"),
                   {_tag("office:version"): VERSION})
    meta = SubElement(root, _tag("office:meta"))
    SubElement(meta, _tag("meta:generator")).text = generator
    SubElement(meta, _tag("meta:creation-date")).text = created.isoformat(
        timespec="seconds")
    SubElement(meta, _tag("dc:date")).text = created.isoformat(
        timespec="seconds")
    if title:
        SubElement(meta, _tag("dc:title")).text = str(title)
    return _xml(root)


def _styles():
    """The styles of the document, which are the ones a reader may change.

    Everything this module sets is an automatic style of ``content.xml`` --
    the bold of a header, the format of a date, the width of a column -- so
    the document styles hold nothing but the frames the format asks for.
    """
    root = Element(_tag("office:document-styles"),
                   {_tag("office:version"): VERSION})
    SubElement(root, _tag("office:styles"))
    SubElement(root, _tag("office:automatic-styles"))
    SubElement(root, _tag("office:master-styles"))
    return _xml(root)


def _settings(sheets):
    """The view settings: the header row of each sheet frozen on screen."""
    root = Element(_tag("office:document-settings"),
                   {_tag("office:version"): VERSION})
    settings = SubElement(root, _tag("office:settings"))
    view_settings = SubElement(settings, _tag("config:config-item-set"),
                               {_tag("config:name"): "ooo:view-settings"})
    views = SubElement(view_settings, _tag("config:config-item-map-indexed"),
                       {_tag("config:name"): "Views"})
    view = SubElement(views, _tag("config:config-item-map-entry"))
    _item(view, "ViewId", "string", "view1")
    tables = SubElement(view, _tag("config:config-item-map-named"),
                        {_tag("config:name"): "Tables"})
    for sheet in sheets:
        if sheet.header is None:
            continue
        entry = SubElement(tables, _tag("config:config-item-map-entry"),
                           {_tag("config:name"): sheet.name})
        # 2 is the mode that freezes rather than splits, and the position is
        # the first row below the frozen one.
        _item(entry, "VerticalSplitMode", "short", "2")
        _item(entry, "VerticalSplitPosition", "int", "1")
        _item(entry, "PositionBottom", "int", "1")
    return _xml(root)


def _item(parent, name, type_name, value):
    item = SubElement(parent, _tag("config:config-item"),
                      {_tag("config:name"): name,
                       _tag("config:type"): type_name})
    item.text = value
    return item


def _content(sheets):
    root = Element(_tag("office:document-content"),
                   {_tag("office:version"): VERSION})
    automatic = SubElement(root, _tag("office:automatic-styles"))
    _date_styles(automatic)
    widths = _column_styles(automatic, sheets)
    body = SubElement(root, _tag("office:body"))
    spreadsheet = SubElement(body, _tag("office:spreadsheet"))
    for sheet in sheets:
        _table(spreadsheet, sheet, widths)
    return _xml(root)


def _date_styles(automatic):
    """The formats a date and a moment are shown in, and the bold header.

    A date cell carries the day it stands for and a format to show it in; the
    format written here is the one the rest of the package writes a date in,
    ``2026-03-31``, which sorts as it reads.
    """
    date_style = SubElement(automatic, _tag("number:date-style"),
                            {_tag("style:name"): "N-iso-date"})
    _date_parts(date_style)
    moment_style = SubElement(automatic, _tag("number:date-style"),
                              {_tag("style:name"): "N-iso-moment"})
    _date_parts(moment_style)
    SubElement(moment_style, _tag("number:text")).text = " "
    SubElement(moment_style, _tag("number:hours"),
               {_tag("number:style"): "long"})
    SubElement(moment_style, _tag("number:text")).text = ":"
    SubElement(moment_style, _tag("number:minutes"),
               {_tag("number:style"): "long"})
    SubElement(moment_style, _tag("number:text")).text = ":"
    SubElement(moment_style, _tag("number:seconds"),
               {_tag("number:style"): "long"})

    for name, data_style in (("ce-date", "N-iso-date"),
                             ("ce-moment", "N-iso-moment")):
        SubElement(automatic, _tag("style:style"),
                   {_tag("style:name"): name,
                    _tag("style:family"): "table-cell",
                    _tag("style:data-style-name"): data_style})

    header = SubElement(automatic, _tag("style:style"),
                        {_tag("style:name"): "ce-header",
                         _tag("style:family"): "table-cell"})
    SubElement(header, _tag("style:text-properties"),
               {_tag("fo:font-weight"): "bold"})


def _date_parts(style):
    SubElement(style, _tag("number:year"), {_tag("number:style"): "long"})
    SubElement(style, _tag("number:text")).text = "-"
    SubElement(style, _tag("number:month"), {_tag("number:style"): "long"})
    SubElement(style, _tag("number:text")).text = "-"
    SubElement(style, _tag("number:day"), {_tag("number:style"): "long"})


def _column_styles(automatic, sheets):
    """One style per width any column needs, and the name of each.

    A width is written once however many columns of however many sheets are
    that wide, because a style per column of a wide sheet is bytes for
    nothing.
    """
    names = {}
    for sheet in sheets:
        for width in _widths(sheet):
            if width in names:
                continue
            name = f"co{len(names) + 1}"
            names[width] = name
            style = SubElement(automatic, _tag("style:style"),
                               {_tag("style:name"): name,
                                _tag("style:family"): "table-column"})
            SubElement(style, _tag("style:table-column-properties"),
                       {_tag("style:column-width"): width,
                        _tag("style:use-optimal-column-width"): "false"})
    return names


def _widths(sheet):
    """The width of each column of a sheet, as a measurement."""
    columns = max([len(sheet.header or ())]
                  + [len(row) for row in sheet.rows] + [0])
    longest = [0] * columns
    for row in ([sheet.header] if sheet.header else []) + list(sheet.rows):
        for index, value in enumerate(row):
            longest[index] = max(longest[index], len(_shown(value)))
    widths = []
    for characters in longest:
        width = characters * WIDTH_PER_CHARACTER_CM
        widths.append(f"{min(max(width, MIN_WIDTH_CM), MAX_WIDTH_CM):.2f}cm")
    return widths


def _table(spreadsheet, sheet, widths):
    table = SubElement(spreadsheet, _tag("table:table"),
                       {_tag("table:name"): sheet.name})
    for width in _widths(sheet):
        SubElement(table, _tag("table:table-column"),
                   {_tag("table:style-name"): widths[width],
                    _tag("table:default-cell-style-name"): "Default"})
    if sheet.header is not None:
        header_rows = SubElement(table, _tag("table:table-header-rows"))
        _row(header_rows, sheet.header, style="ce-header")
    for row in sheet.rows:
        _row(table, row)


def _row(parent, values, style=None):
    row = SubElement(parent, _tag("table:table-row"))
    for value in values:
        _cell(row, value, style)
    return row


def _cell(row, value, style=None):
    """One cell, with the value type the value has."""
    attributes = {}
    if style:
        attributes[_tag("table:style-name")] = style
    texts = ()

    if _is_empty(value):
        pass
    elif isinstance(value, bool):
        attributes[_tag("office:value-type")] = "boolean"
        attributes[_tag("office:boolean-value")] = "true" if value else "false"
        texts = ("TRUE" if value else "FALSE",)
    elif isinstance(value, (int, float, Decimal)):
        shown = _shown(value)
        attributes[_tag("office:value-type")] = "float"
        attributes[_tag("office:value")] = shown
        texts = (shown,)
    elif isinstance(value, datetime.datetime):
        attributes[_tag("office:value-type")] = "date"
        attributes[_tag("office:date-value")] = value.isoformat(
            timespec="seconds")
        attributes.setdefault(_tag("table:style-name"), "ce-moment")
        texts = (_shown(value),)
    elif isinstance(value, datetime.date):
        attributes[_tag("office:value-type")] = "date"
        attributes[_tag("office:date-value")] = value.isoformat()
        attributes.setdefault(_tag("table:style-name"), "ce-date")
        texts = (_shown(value),)
    else:
        attributes[_tag("office:value-type")] = "string"
        texts = str(value).split("\n")

    cell = SubElement(row, _tag("table:table-cell"), attributes)
    for text in texts:
        # A line of its own per line of the text: a newline inside a paragraph
        # is whitespace, and a text of several lines is several paragraphs.
        SubElement(cell, _tag("text:p")).text = text
    return cell


def _is_empty(value):
    """Whether a value is one a spreadsheet shows as an empty cell."""
    if value is None or value == "":
        return True
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return True
    # What pandas and numpy put in place of a missing value, without importing
    # either: NaT and pandas.NA are neither None nor NaN.
    if value.__class__.__name__ in ("NaTType", "NAType"):
        return True
    return False


def _shown(value):
    """What a value reads as, which is also how wide its column has to be."""
    if _is_empty(value):
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        if value == int(value) and abs(value) < 1e15:
            return str(int(value))
        return repr(value)
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value)
    return max(text.split("\n"), key=len) if "\n" in text else text


#: What this module offers: a sheet, the call that writes a workbook, and the
#: rule a sheet name is held to.
__all__ = ["MEDIA_TYPE", "SUFFIX", "Sheet", "sheet_name", "write_ods"]
