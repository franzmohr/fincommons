"""Tables written as an OpenDocument spreadsheet.

The file is a zip archive of XML documents, so the tests read it back the way
a spreadsheet does: unpack the archive, parse ``content.xml`` and look at the
cells. What they are mostly about is the types -- a number has to arrive as a
number and a date as a date, or the sheet is a picture of a table rather than
a table.
"""

from __future__ import annotations

import datetime
import zipfile
from xml.etree import ElementTree

import pytest

from fincommons.spreadsheet import (MEDIA_TYPE, Sheet, sheet_name, write_ods)

NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "config": "urn:oasis:names:tc:opendocument:xmlns:config:1.0",
}


def cells_of(path, sheet=0):
    """Every row of a sheet, as the cells' attributes and text."""
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
    tables = content.findall(".//table:table", NS)
    rows = []
    for row in tables[sheet].iter(f"{{{NS['table']}}}table-row"):
        cells = []
        for cell in row.findall("table:table-cell", NS):
            texts = [paragraph.text or ""
                     for paragraph in cell.findall("text:p", NS)]
            cells.append(({key.split("}")[1]: value
                           for key, value in cell.attrib.items()},
                          "\n".join(texts)))
        rows.append(cells)
    return rows


def test_the_archive_declares_the_format_first_and_uncompressed(tmp_path):
    # How a reader tells an OpenDocument spreadsheet from any other zip.
    path = write_ods(tmp_path / "book", [Sheet("one", ["a"], [(1,)])])
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        assert names[0] == "mimetype"
        assert archive.read("mimetype").decode() == MEDIA_TYPE
        assert archive.getinfo("mimetype").compress_type == zipfile.ZIP_STORED
        for entry in ("META-INF/manifest.xml", "content.xml", "styles.xml",
                      "meta.xml", "settings.xml"):
            assert entry in names


def test_the_suffix_is_added_and_the_directories_made(tmp_path):
    path = write_ods(tmp_path / "below" / "book", [Sheet("one", None, [(1,)])])
    assert path.endswith("book.ods")
    assert zipfile.is_zipfile(path)


def test_a_number_arrives_as_a_number(tmp_path):
    path = write_ods(tmp_path / "book",
                     [Sheet("one", ["value"], [(1234.5,), (16_000_000_000.0,),
                                               (-3,)])])
    rows = cells_of(path)
    assert [cell[0]["value-type"] for row in rows[1:] for cell in row] \
        == ["float", "float", "float"]
    assert [cell[0]["value"] for row in rows[1:] for cell in row] \
        == ["1234.5", "16000000000", "-3"]


def test_a_date_arrives_as_a_date_and_a_moment_keeps_its_time(tmp_path):
    path = write_ods(tmp_path / "book", [
        Sheet("one", ["day", "moment"],
              [(datetime.date(2026, 3, 31),
                datetime.datetime(2026, 4, 1, 12, 30, 5))])])
    day, moment = cells_of(path)[1]
    assert day[0]["value-type"] == "date"
    assert day[0]["date-value"] == "2026-03-31"
    assert moment[0]["date-value"] == "2026-04-01T12:30:05"
    # Each carries the format it is shown in, or a reader shows a serial number.
    assert day[0]["style-name"] == "ce-date"
    assert moment[0]["style-name"] == "ce-moment"


def test_nothing_is_an_empty_cell(tmp_path):
    # A missing figure is an empty cell, whichever of the three ways it
    # reaches the writer: no value, no text, or a not-a-number.
    path = write_ods(tmp_path / "book",
                     [Sheet("one", ["a", "b", "c"], [(None, "", float("nan"))])])
    for attributes, text in cells_of(path)[1]:
        assert "value-type" not in attributes
        assert text == ""


def test_a_text_of_several_lines_keeps_its_lines(tmp_path):
    path = write_ods(tmp_path / "book",
                     [Sheet("one", ["note"], [("first\nsecond",)])])
    attributes, text = cells_of(path)[1][0]
    assert attributes["value-type"] == "string"
    assert text == "first\nsecond"


def test_a_text_that_looks_like_a_formula_stays_a_text(tmp_path):
    # OpenDocument holds a formula in an attribute of its own, so a text can
    # never become one; the test is here because the reverse is true of CSV.
    path = write_ods(tmp_path / "book", [Sheet("one", ["a"], [("=1+1",)])])
    attributes, text = cells_of(path)[1][0]
    assert attributes["value-type"] == "string"
    assert text == "=1+1"
    assert not any(key.startswith("formula") for key in attributes)


def test_a_boolean_arrives_as_a_boolean(tmp_path):
    path = write_ods(tmp_path / "book",
                     [Sheet("one", ["ok"], [(True,), (False,)])])
    rows = cells_of(path)
    assert [cell[0]["boolean-value"] for row in rows[1:] for cell in row] \
        == ["true", "false"]


def test_the_header_is_bold_repeated_and_frozen(tmp_path):
    path = write_ods(tmp_path / "book", [Sheet("one", ["a"], [(1,)])])
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
        settings = ElementTree.fromstring(archive.read("settings.xml"))
    header_rows = content.findall(".//table:table-header-rows", NS)
    assert len(header_rows) == 1
    assert header_rows[0].find(".//table:table-cell", NS).get(
        f"{{{NS['table']}}}style-name") == "ce-header"
    frozen = [item.text for item in settings.findall(".//config:config-item", NS)
              if item.get(f"{{{NS['config']}}}name") == "VerticalSplitMode"]
    assert frozen == ["2"]


def test_a_sheet_without_a_header_has_no_header_row(tmp_path):
    path = write_ods(tmp_path / "book", [Sheet("one", None, [("a",), ("b",)])])
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
    assert content.findall(".//table:table-header-rows", NS) == []
    assert len(cells_of(path)) == 2


def test_the_columns_are_as_wide_as_what_stands_in_them(tmp_path):
    path = write_ods(tmp_path / "book", [
        Sheet("one", ["a", "a longer heading than the other one"],
              [("x", "y")])])
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
    columns = content.findall(".//table:table-column", NS)
    assert len(columns) == 2
    widths = [column.get(f"{{{NS['table']}}}style-name") for column in columns]
    assert widths[0] != widths[1]


def test_a_sheet_name_is_trimmed_and_made_unique(tmp_path):
    assert sheet_name("a/b:c") == "a b c"
    assert sheet_name("", ()) == "sheet"
    assert sheet_name("facts", ["facts"]) == "facts (2)"
    path = write_ods(tmp_path / "book",
                     [Sheet("facts", None, [(1,)]),
                      Sheet("facts", None, [(2,)])])
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
    names = [table.get(f"{{{NS['table']}}}name")
             for table in content.findall(".//table:table", NS)]
    assert names == ["facts", "facts (2)"]


def test_the_rows_may_be_a_generator(tmp_path):
    path = write_ods(tmp_path / "book",
                     [Sheet("one", ["n"], ((number,) for number in range(3)))])
    assert len(cells_of(path)) == 4


def test_a_workbook_of_no_sheets_is_refused(tmp_path):
    with pytest.raises(ValueError):
        write_ods(tmp_path / "book", [])


def test_the_moment_it_states_can_be_fixed(tmp_path):
    # Two runs that state the same moment write the same bytes, which is what
    # lets a file be checked for having changed.
    created = datetime.datetime(2026, 11, 25, 9, 0, 0)
    sheets = [Sheet("one", ["a"], [(1,)])]
    first = write_ods(tmp_path / "first", sheets, created=created)
    second = write_ods(tmp_path / "second",
                       [Sheet("one", ["a"], [(1,)])], created=created)
    with open(first, "rb") as one, open(second, "rb") as two:
        assert one.read() == two.read()
    with zipfile.ZipFile(first) as archive:
        assert b"2026-11-25T09:00:00" in archive.read("meta.xml")
