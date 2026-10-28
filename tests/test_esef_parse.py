"""Tests of reading an index and a report of filings.xbrl.org.

The reading is plain Python, so these tests need neither the network nor
Spark. The documents are the small ones in ``tests/data/esef``, made for the
tests rather than copied from the repository, and the facts a test is about
are written out in it.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import pandas
import pytest

import fincommons.esef as esef

FIXTURES = Path(__file__).resolve().parent / "data" / "esef"
LEI_AT = "5299000000000000AT01"
LEI_DE = "5299000000000000DE01"
YEAR = "2025-04-01T00:00:00/2026-04-01T00:00:00"

D = datetime.date


def load(name):
    with open(FIXTURES / name, encoding="utf-8") as handle:
        return json.load(handle)


def fact(concept, value, period=YEAR, unit=None, **dimensions):
    """One fact of xBRL-JSON; the keyword arguments are further dimensions."""
    dims = {"concept": concept, "entity": f"scheme:{LEI_AT}", "period": period}
    if unit is not None:
        dims["unit"] = unit
    dims.update(dimensions)
    return {"value": value, "dimensions": dims}


# The index
# --------------------------------------------------------------------------


def test_an_index_gives_one_record_per_filing_with_its_entity():
    records = esef.filings_from_index(load("filings-2025.json"))

    assert [r["FILING"] for r in records] == [
        "5299000000000000AT02-2025-12-31-ESEF-AT-0",
        f"{LEI_DE}-2025-12-31-ESEF-DE-0"]
    german = records[1]
    assert german["LEI"] == LEI_DE
    assert german["NAME"] == "Beispiel SE"
    assert german["COUNTRY"] == "DE"
    assert german["SYSTEM"] == "ESEF"
    assert german["PERIODEND"] == D(2025, 12, 31)
    assert german["LANG"] == "de"
    assert german["PROCESSED"] == datetime.datetime(2026, 8, 4, 16, 48, 35, 245129)
    assert german["WARNINGS"] == 2
    assert german["JSONURL"].endswith(f"{LEI_DE}-2025-12-31-1-de.json")
    assert records[0]["LANG"] == "en"


def test_the_lei_is_read_off_the_filing_where_its_entity_is_not_included():
    document = load("filings-2026.json")
    document["included"] = []

    records = esef.filings_from_index(document)

    assert records[0]["LEI"] == LEI_AT
    assert records[0]["NAME"] is None


def test_a_filing_whose_name_could_leave_its_folder_is_left_out():
    document = load("filings-2025.json")
    document["data"][0]["attributes"]["fxo_id"] = "../../etc"
    document["data"][1]["attributes"]["country"] = "D/E"

    assert esef.filings_from_index(document) == []


def test_an_answer_that_is_not_an_index_is_refused():
    with pytest.raises(ValueError, match="not an index"):
        esef.filings_from_index({"errors": [{"status": "503"}]})


def test_the_language_is_read_off_the_name_of_the_report():
    assert esef.language_of("/x/LEI-2026-03-31-1-de.json") == "de"
    assert esef.language_of(None, "/x/reports/LEI-2024-12-31-1-sv.xhtml") == "sv"
    # The filings of the United Kingdom name no language.
    assert esef.language_of("/x/LEI-2024-12-31-T01.json") is None


# The periods
# --------------------------------------------------------------------------


@pytest.mark.parametrize("text, expected", [
    # xBRL-JSON writes the end of a period as the start of the next day.
    ("2025-04-01T00:00:00/2026-04-01T00:00:00", (D(2025, 4, 1), D(2026, 3, 31))),
    ("2026-04-01T00:00:00", (None, D(2026, 3, 31))),
    # A date without a time is that whole day.
    ("2025-01-01/2025-12-31", (D(2025, 1, 1), D(2025, 12, 31))),
    ("2025-12-31", (None, D(2025, 12, 31))),
    # A time within a day is that day.
    ("2025-12-31T12:00:00", (None, D(2025, 12, 31))),
])
def test_a_period_is_the_days_it_covers(text, expected):
    assert esef.parse_period(text) == expected


# The facts
# --------------------------------------------------------------------------


def test_a_report_gives_one_row_per_fact_without_its_text_blocks():
    facts = esef.facts_from_report(
        load(f"{LEI_AT}-2026-03-31-1-de.json")).set_index("FACT")

    # f-4 is the note on goodwill, a text block.
    assert list(facts.index) == ["f-1", "f-2", "f-3"]

    revenue = facts.loc["f-1"]
    assert revenue["CONCEPT"] == "ifrs-full:Revenue"
    assert revenue["LEI"] == LEI_AT
    assert revenue["VALUE"] == 430569203.0
    assert revenue["TEXT"] is None
    assert revenue["UNIT"] == "iso4217:EUR"
    assert (revenue["PRDSTART"], revenue["PRDEND"]) == (D(2025, 4, 1), D(2026, 3, 31))
    assert revenue["DECIMALS"] == 0
    assert revenue["DIMENSIONS"] is None

    equity = facts.loc["f-2"]
    assert equity["PRDSTART"] is None and equity["PRDEND"] == D(2026, 3, 31)
    assert equity["DIMENSIONS"] == (
        "ifrs-full:ComponentsOfEquityAxis=ifrs-full:RetainedEarningsMember")
    assert pandas.isna(equity["DECIMALS"])  # INF, an exact value

    name = facts.loc["f-3"]
    assert name["TEXT"] == "Parent AG"
    assert name["LANG"] == "de"
    assert name["UNIT"] is None
    assert pandas.isna(name["VALUE"])


def test_the_dimensions_of_a_fact_are_sorted():
    document = {"facts": {"f": fact("ifrs-full:Revenue", "1", unit="iso4217:EUR",
                                    **{"b:Z": "m:2", "a:A": "m:1"})}}

    facts = esef.facts_from_report(document)

    assert facts.loc[0, "DIMENSIONS"] == "a:A=m:1;b:Z=m:2"


def test_a_nil_fact_is_left_out_and_a_unit_that_is_no_number_is_kept_as_text():
    document = {"facts": {
        "nil": fact("ifrs-full:Revenue", None, unit="iso4217:EUR"),
        "odd": fact("ifrs-full:Revenue", "n/a", unit="iso4217:EUR"),
    }}

    facts = esef.facts_from_report(document)

    assert list(facts["FACT"]) == ["odd"]
    assert facts.loc[0, "TEXT"] == "n/a"


def test_a_short_text_is_kept_and_a_note_with_markup_is_not():
    assert not esef.is_text_block("Erste Beispiel AG")
    assert not esef.is_text_block("Revenue < 5 % of total")
    assert esef.is_text_block("<p>13 Goodwill</p>")
    assert esef.is_text_block("Text <br/> more")


def test_an_answer_that_is_not_a_report_is_refused():
    with pytest.raises(ValueError, match="not a report"):
        esef.facts_from_report({"documentInfo": {}})


def test_a_report_without_facts_gives_an_empty_frame_with_the_columns():
    facts = esef.facts_from_report({"facts": {}})

    assert facts.empty
    assert list(facts.columns) == esef.FACT_COLUMNS
