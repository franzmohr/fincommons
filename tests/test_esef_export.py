"""One issuer's ESEF reports as a spreadsheet.

The lake these tests build holds two filings of one issuer, for two financial
years, and the later one carries the earlier year as its comparative figure --
which is the case the export exists for: the same field of two reports has to
end up in one row, so that a reader sees at once whether the two reports agree
about it.
"""

from __future__ import annotations

import datetime
import zipfile
from datetime import date, datetime as moment
from xml.etree import ElementTree

import pytest

from fincommons.financialreport.esef import common, export

LEI = "529900ZAXBMQDIWPNB72"
OTHER_LEI = "529900ARBU9SBJFNX419"
COUNTRY = "AT"
NS = {"table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
      "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0"}

THIS_YEAR = date(2025, 12, 31)
LAST_YEAR = date(2024, 12, 31)
FILING = f"{LEI}-2025-12-31-ESEF-AT-0"
EARLIER = f"{LEI}-2024-12-31-ESEF-AT-0"
PROCESSED = moment(2026, 4, 1, 12, 0, 0)


def fact(filing, concept, value, prd_end, prd_start=None, unit="iso4217:EUR",
         dimensions=None, text=None, lei=LEI, period_end=None):
    """One row of the table of facts."""
    period_end = period_end or (THIS_YEAR if filing == FILING else LAST_YEAR)
    return (COUNTRY, filing, lei, period_end,
            f"{filing}:{concept}:{prd_start or ''}:{prd_end}:{dimensions or ''}",
            concept, prd_start, prd_end, unit, dimensions,
            None if value is None else float(value), text, -3, "en")


def filing_row(filing=FILING, period_end=THIS_YEAR, lang="en", lei=LEI,
               jsonurl="/x.json", name="Issuer AG"):
    """One row of the table of filings."""
    return (filing, lei, name, COUNTRY, "ESEF", period_end, lang,
            PROCESSED, PROCESSED, PROCESSED, 0, 1, 2,
            "/p.zip", "/r.xhtml", "/v", jsonurl, "sha")


#: Two reports of one issuer. The later one restates the revenue of 2024 --
#: 480 where the 2024 report itself said 500 -- and repeats the assets
#: unchanged.
FACTS = [
    fact(FILING, "ifrs-full:Revenue", 600, THIS_YEAR, date(2025, 1, 1)),
    fact(FILING, "ifrs-full:Revenue", 480, LAST_YEAR, date(2024, 1, 1)),
    fact(FILING, "ifrs-full:Assets", 1000, THIS_YEAR),
    fact(FILING, "ifrs-full:Assets", 900, LAST_YEAR),
    fact(EARLIER, "ifrs-full:Revenue", 500, LAST_YEAR, date(2024, 1, 1)),
    fact(EARLIER, "ifrs-full:Assets", 900, LAST_YEAR),
]
FILINGS = [filing_row(), filing_row(EARLIER, LAST_YEAR)]


@pytest.fixture
def lake(spark, tmp_path):
    """A lake whose Silver tables this test writes directly."""
    import yaml

    from fincommons.storage import write_silver

    base = tmp_path / "lake"
    config_path = tmp_path / "paths.yaml"
    config_path.write_text(yaml.safe_dump(
        {"esef": {name: str(base / name) for name in ("bronze", "silver")}}))
    config_path = str(config_path)

    def write(facts=FACTS, filings=FILINGS):
        write_silver(
            spark.createDataFrame(facts, common.facts_schema),
            common.ref_silver_facts(config_path), partition_by=("country",))
        write_silver(
            spark.createDataFrame(filings, common.filings_schema),
            common.ref_silver_filings(config_path))
        return config_path

    return write


def sheets_of(path):
    """Every sheet of the file, as a name and its rows of text."""
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
    sheets = {}
    for table in content.findall(".//table:table", NS):
        rows = []
        for row in table.iter(f"{{{NS['table']}}}table-row"):
            rows.append(["\n".join(paragraph.text or "" for paragraph
                                   in cell.findall("text:p", NS))
                         for cell in row.findall("table:table-cell", NS)])
        sheets[table.get(f"{{{NS['table']}}}name")] = rows
    return sheets


def figures_of(path):
    """The ``figures`` sheet, as a dict from the row's keys to its values."""
    rows = sheets_of(path)["figures"]
    header = rows[0]
    keys = len(export.FIGURE_SHEET_KEYS)
    return header, {tuple(row[:keys]): row[keys:] for row in rows[1:]}


# The names
# --------------------------------------------------------------------------


def test_the_export_shadows_no_name_of_the_module():
    # Every submodule's names go into one namespace, so a constant named
    # after what another submodule already exports replaces it silently --
    # which is what a second FACT_COLUMNS beside the one of ``parse`` did.
    from fincommons.financialreport import esef
    from fincommons.financialreport.esef import (bronze, common, parse,
                                                 pipeline, quality, silver)

    for module in (common, parse, bronze, silver, quality, pipeline):
        shared = set(export.__all__) & set(getattr(module, "__all__", ()))
        assert shared == set(), f"export shadows {sorted(shared)} of {module.__name__}"
    for name in export.__all__:
        assert getattr(esef, name) is getattr(export, name)
    assert esef.FACT_COLUMNS == parse.FACT_COLUMNS


# The file
# --------------------------------------------------------------------------


def test_the_file_holds_the_four_sheets(spark, lake, tmp_path):
    config = lake()

    path = export.export_esef_reports(spark, config, LEI, tmp_path / "book")

    assert path.endswith(".ods")
    assert list(sheets_of(path)) == ["about", "figures", "filings", "facts"]


def test_a_directory_is_written_into_under_the_lei(spark, lake, tmp_path):
    config = lake()
    folder = tmp_path / "out"
    folder.mkdir()

    path = export.export_esef_reports(spark, config, LEI, folder)

    assert path.endswith(f"{LEI}-esef.ods")


def test_the_lei_is_read_as_it_is_keyed(spark, lake, tmp_path):
    # A lei copied out of a spreadsheet carries blanks and may be lower case.
    config = lake()

    path = export.export_esef_reports(spark, config, f" {LEI.lower()} ",
                                      tmp_path / "book")

    assert len(figures_of(path)[1]) == 4


def test_an_issuer_with_no_filing_is_refused(spark, lake, tmp_path):
    config = lake()

    with pytest.raises(ValueError, match="no filing"):
        export.export_esef_reports(spark, config, OTHER_LEI, tmp_path / "book")


# The comparison across filings, which is what the sheet is for
# --------------------------------------------------------------------------


def test_one_column_per_filing_headed_by_the_period_it_reports_on(spark, lake,
                                                                  tmp_path):
    config = lake()

    header, _ = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    assert header[:len(export.FIGURE_SHEET_KEYS)] == list(export.FIGURE_SHEET_KEYS)
    assert header[len(export.FIGURE_SHEET_KEYS):] == ["2025-12-31", "2024-12-31"]


def test_the_same_field_of_two_filings_stands_in_one_row(spark, lake, tmp_path):
    config = lake()

    _, figures = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    # The revenue of 2024 as the two reports state it: the 2025 report's
    # comparative figure beside the 2024 report's own.
    key = ("ifrs-full:Revenue", "taxonomy", "2024-01-01", "2024-12-31",
           "iso4217:EUR", "2", "yes")
    assert figures[key] == ["480", "500"]


def test_a_figure_the_two_reports_agree_on_is_not_marked(spark, lake, tmp_path):
    config = lake()

    _, figures = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    key = ("ifrs-full:Assets", "taxonomy", "", "2024-12-31", "iso4217:EUR",
           "2", "")
    assert figures[key] == ["900", "900"]


def test_a_figure_only_one_filing_states_stands_under_that_filing(spark, lake,
                                                                 tmp_path):
    config = lake()

    _, figures = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    key = ("ifrs-full:Revenue", "taxonomy", "2025-01-01", "2025-12-31",
           "iso4217:EUR", "1", "")
    assert figures[key] == ["600", ""]


def test_a_figure_tagged_twice_in_one_report_is_taken_once(spark, lake,
                                                           tmp_path):
    # The statements and the notes often carry one figure twice. The smaller
    # value is taken, as the data quality checks take it.
    config = lake(FACTS + [fact(FILING, "ifrs-full:Assets", 1000.4, THIS_YEAR)])

    _, figures = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    key = ("ifrs-full:Assets", "taxonomy", "", "2025-12-31", "iso4217:EUR",
           "1", "")
    assert figures[key] == ["1000", ""]


def test_the_issuers_own_concepts_come_after_the_taxonomys(spark, lake,
                                                           tmp_path):
    config = lake(FACTS + [fact(FILING, "voe:CapitalExpenditure", 70,
                                THIS_YEAR, date(2025, 1, 1))])

    _, figures = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))
    concepts = [key[0] for key in figures]

    assert concepts[-1] == "voe:CapitalExpenditure"
    assert [key[1] for key in figures][-1] == "extension"


def test_a_figure_with_dimensions_is_left_out_of_the_comparison(spark, lake,
                                                                tmp_path):
    # A figure broken down by segment is a part of a total, and the parts do
    # not belong in a sheet of totals.
    config = lake(FACTS + [fact(FILING, "ifrs-full:Revenue", 250, THIS_YEAR,
                                date(2025, 1, 1),
                                dimensions="ifrs-full:SegmentsAxis=voe:Steel")])

    path = export.export_esef_reports(spark, config, LEI, tmp_path / "book")
    _, figures = figures_of(path)
    facts = sheets_of(path)["facts"]

    assert [key for key in figures if key[0] == "ifrs-full:Revenue"
            and key[3] == "2025-12-31"] == [
        ("ifrs-full:Revenue", "taxonomy", "2025-01-01", "2025-12-31",
         "iso4217:EUR", "1", "")]
    assert any("ifrs-full:SegmentsAxis=voe:Steel" in row for row in facts)


def test_two_filings_of_one_period_are_told_apart_by_language(spark, lake,
                                                              tmp_path):
    german = f"{LEI}-2025-12-31-ESEF-AT-1"
    config = lake(FACTS + [fact(german, "ifrs-full:Assets", 1000, THIS_YEAR)],
                  FILINGS + [filing_row(german, THIS_YEAR, lang="de")])

    header, _ = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    # Both columns of the period carry their language: naming only the
    # second would leave the first looking like the period's one report.
    assert header[len(export.FIGURE_SHEET_KEYS):] == [
        "2025-12-31 (de)", "2025-12-31 (en)", "2024-12-31"]


# What is covered
# --------------------------------------------------------------------------


def test_two_filings_of_one_period_in_one_language_are_numbered(spark, lake,
                                                                tmp_path):
    # What the repository holds of some issuers: the same report twice in
    # one language, where the language tells the two filings apart no better
    # than the period does.
    again = f"{LEI}-2025-12-31-ESEF-AT-1"
    config = lake(FACTS + [fact(again, "ifrs-full:Assets", 1000, THIS_YEAR)],
                  FILINGS + [filing_row(again, THIS_YEAR, lang="en")])

    header, _ = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))

    assert header[len(export.FIGURE_SHEET_KEYS):] == [
        "2025-12-31 #1", "2025-12-31 #2", "2024-12-31"]


def test_the_years_are_the_latest_ones_asked_for(spark, lake, tmp_path):
    config = lake()

    header, figures = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book", years=1))

    assert header[len(export.FIGURE_SHEET_KEYS):] == ["2025-12-31"]
    assert all(key[5] == "1" for key in figures)


def test_a_language_can_be_asked_for(spark, lake, tmp_path):
    german = f"{LEI}-2025-12-31-ESEF-AT-1"
    config = lake(FACTS + [fact(german, "ifrs-full:Assets", 1000, THIS_YEAR)],
                  FILINGS + [filing_row(german, THIS_YEAR, lang="de")])

    header, _ = figures_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book", lang="de"))

    assert header[len(export.FIGURE_SHEET_KEYS):] == ["2025-12-31"]


def test_a_language_nothing_was_filed_in_is_refused(spark, lake, tmp_path):
    config = lake()

    with pytest.raises(ValueError, match="no filing"):
        export.export_esef_reports(spark, config, LEI, tmp_path / "book",
                                   lang="fr")


def test_the_years_have_to_be_a_number_of_years(spark, lake, tmp_path):
    config = lake()

    with pytest.raises(TypeError):
        export.export_esef_reports(spark, config, LEI, tmp_path / "book",
                                   years="2025")


def test_the_facts_of_another_issuer_are_not_in_the_file(spark, lake, tmp_path):
    other = f"{OTHER_LEI}-2025-12-31-ESEF-AT-0"
    config = lake(
        FACTS + [fact(other, "ifrs-full:Revenue", 42, THIS_YEAR,
                      date(2025, 1, 1), lei=OTHER_LEI)],
        FILINGS + [filing_row(other, THIS_YEAR, lei=OTHER_LEI,
                              name="Other AG")])

    path = export.export_esef_reports(spark, config, LEI, tmp_path / "book")

    assert not any(OTHER_LEI in cell for rows in sheets_of(path).values()
                   for row in rows for cell in row)


# The other sheets
# --------------------------------------------------------------------------


def test_the_filings_sheet_names_the_column_and_counts_the_facts(spark, lake,
                                                                  tmp_path):
    config = lake()

    rows = sheets_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))["filings"]

    assert rows[0][:4] == ["filing", "column", "period_end", "lang"]
    assert [row[1] for row in rows[1:]] == ["2025-12-31", "2024-12-31"]
    assert [row[7] for row in rows[1:]] == ["4", "2"]


def test_the_filings_sheet_spells_the_addresses_out(spark, lake, tmp_path):
    config = lake()

    rows = sheets_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))["filings"]

    assert rows[1][-3:] == [f"{common.BASE_URL}/r.xhtml",
                            f"{common.BASE_URL}/v",
                            f"{common.BASE_URL}/p.zip"]


def test_a_filing_whose_report_was_not_collected_is_still_a_row(spark, lake,
                                                                tmp_path):
    # The indexes cover every filing; a report nobody collected has no facts,
    # and saying so is the honest answer to what the lake holds.
    missing = f"{LEI}-2023-12-31-ESEF-AT-0"
    config = lake(FACTS, FILINGS + [filing_row(missing, date(2023, 12, 31),
                                               jsonurl=None)])

    path = export.export_esef_reports(spark, config, LEI, tmp_path / "book")
    rows = sheets_of(path)["filings"]
    header, figures = figures_of(path)

    assert [row[0] for row in rows[1:]][-1] == missing
    assert rows[-1][7] == "0"
    assert "2023-12-31" in header


def test_the_facts_sheet_holds_every_fact_including_the_texts(spark, lake,
                                                              tmp_path):
    config = lake(FACTS + [fact(FILING, "ifrs-full:NameOfReportingEntity",
                                None, THIS_YEAR, text="Issuer AG", unit=None)])

    rows = sheets_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))["facts"]

    assert rows[0] == list(export.FACT_SHEET_COLUMNS)
    assert len(rows) == 1 + 7
    assert any("Issuer AG" in row for row in rows[1:])


def test_the_about_sheet_says_what_the_file_is_and_what_it_is_not(spark, lake,
                                                                  tmp_path):
    config = lake()

    rows = sheets_of(export.export_esef_reports(
        spark, config, LEI, tmp_path / "book"))["about"]
    said = {row[0]: row[1] for row in rows[1:] if len(row) > 1}

    assert said["issuer"] == "Issuer AG"
    assert said["lei"] == LEI
    assert said["filings covered"] == "2"
    assert said["source"] == common.SOURCE
    assert said["periods"] == "2024-12-31 to 2025-12-31"
    # Every caveat is on the sheet, the disclaimer among them.
    assert len([row for row in rows[1:] if row[0] == "read it as"]) \
        == len(export.CAVEATS)


def test_the_figures_are_numbers_and_the_periods_dates(spark, lake, tmp_path):
    # A figure written as text is one a spreadsheet will not sum.
    config = lake()

    path = export.export_esef_reports(spark, config, LEI, tmp_path / "book")
    with zipfile.ZipFile(path) as archive:
        content = ElementTree.fromstring(archive.read("content.xml"))
    table = [sheet for sheet in content.findall(".//table:table", NS)
             if sheet.get(f"{{{NS['table']}}}name") == "figures"][0]
    rows = list(table.iter(f"{{{NS['table']}}}table-row"))
    types = [cell.get(
        "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}value-type")
        for cell in rows[1].findall("table:table-cell", NS)]

    assert types[2:5] == [None, "date", "string"]        # prd_start is empty
    assert types[len(export.FIGURE_SHEET_KEYS)] == "float"


def test_the_moment_it_states_can_be_fixed(spark, lake, tmp_path):
    config = lake()
    created = moment(2026, 11, 25, 9, 0, 0)

    path = export.export_esef_reports(spark, config, LEI, tmp_path / "book",
                                      created=created)

    with zipfile.ZipFile(path) as archive:
        assert b"2026-11-25T09:00:00" in archive.read("meta.xml")
