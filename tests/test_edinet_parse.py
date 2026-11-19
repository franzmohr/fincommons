"""Reading the agency's CSV conversion into facts.

The fixture is cut from a real conversion rather than written by hand: the
header, the dates the filing states about itself, a five-year series, a figure
the filer marks as not applicable, and an element tagged more than once under
one context. Each of those is in it because the real file has it and because
a reader that assumed otherwise would be wrong on a sound filing.

Kojima's annual securities report for the year to 31 August 2026, document
S100ZH4D, as the agency converted it.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from pathlib import Path

import pytest

from fincommons.financialreport.edinet import common
from fincommons.financialreport.edinet.parse import (ANCHOR_ELEMENTS,
                                                     CSV_COLUMNS,
                                                     CSV_DELIMITER,
                                                     CSV_ENCODING, anchors_of,
                                                     facts_from_archive,
                                                     facts_from_csv,
                                                     is_extension, period_of,
                                                     split_context)

ARCHIVE = Path(__file__).parent / "data" / "edinet" / "S100ZH4D.zip"
FILING = "S100ZH4D"
#: What the index says about the filing, which is what a relative context
#: would be resolved against if the filing said nothing itself.
PERIOD = (date(2025, 9, 1), date(2026, 8, 31))
FILED = date(2026, 11, 13)

NAMES = [field.name for field in common.facts_schema.fields]


def read():
    """The facts of the fixture, as dictionaries."""
    rows = facts_from_archive(str(ARCHIVE), FILING, period_start=PERIOD[0],
                              period_end=PERIOD[1], filed=FILED,
                              edinet_code="E03270", filed_year=2026)
    return [dict(zip(NAMES, row)) for row in rows]


# The shape of what comes back
# --------------------------------------------------------------------------


def test_every_row_has_the_columns_of_the_table():
    rows = facts_from_archive(str(ARCHIVE), FILING)

    assert rows
    assert all(len(row) == len(NAMES) for row in rows)


def test_a_fact_is_identified_even_where_the_element_and_context_repeat():
    """The number of employees is tagged three times under one context in the
    real report, so the element and the context do not identify a fact."""
    facts = read()
    employees = [f for f in facts
                 if f["concept"] == "jpcrp_cor:NumberOfEmployees"]

    assert len({f["fact"] for f in facts}) == len(facts)
    if len(employees) > 1:
        assert len({f["fact"] for f in employees}) == len(employees)


def test_the_series_of_a_fund_tell_their_facts_apart():
    """A fund manager files one document per series of a fund, under one
    form, and the same element and context in each.

    The archive is built here from the shape of S100YWNP, a semi-annual
    report for two series of one fund.
    """
    header = CSV_DELIMITER.join(f'"{c}"' for c in CSV_COLUMNS)
    line = CSV_DELIMITER.join(f'"{c}"' for c in (
        "jppfs_cor:AccountsReceivableOther", "未収入金",
        "CurrentYearInstant_NonConsolidatedMember", "当期末", "個別", "時点",
        "JPY", "円", "{}"))
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        for series, value in (("000", "1"), ("001", "28564965"),
                              ("002", "2568320")):
            archive.writestr(
                f"XBRL_TO_CSV/jpsps040000-srs-001_G03898-{series}"
                "_2026-07-21_01_2026-10-21.csv",
                (header + "\n" + line.format(value) + "\n")
                .encode(CSV_ENCODING))

    facts = [dict(zip(NAMES, row))
             for row in facts_from_archive(data.getvalue(), "S100YWNP")]

    assert len(facts) == 3
    assert len({f["fact"] for f in facts}) == 3
    assert {f["doc_form"] for f in facts} == {"jpsps040000-srs"}
    assert sorted(f["value"] for f in facts) == [1, 2568320, 28564965]


def test_a_companys_own_report_keeps_the_identifier_it_had():
    """The main document of a filing is named by its form alone, as before
    the series were told apart."""
    fact = read()[0]["fact"]

    assert fact.startswith(f"{FILING}:jpcrp030000-asr:")


def test_the_document_a_fact_came_from_is_named():
    facts = read()

    assert {f["doc_form"] for f in facts} == {"jpcrp030000-asr"}


# The periods, which are resolved rather than reported
# --------------------------------------------------------------------------


def test_the_filing_states_its_own_year_and_the_comparative():
    with zipfile.ZipFile(ARCHIVE) as archive:
        member = next(n for n in archive.namelist() if n.endswith(".csv"))
        text = archive.read(member).decode(CSV_ENCODING)
    rows = list(csv.reader(io.StringIO(text), delimiter=CSV_DELIMITER))

    found = anchors_of(rows[1:])

    assert found["current_start"] == date(2025, 9, 1)
    assert found["current_end"] == date(2026, 8, 31)
    assert found["prior_start"] == date(2024, 9, 1)
    assert found["prior_end"] == date(2025, 8, 31)
    assert set(found) <= set(ANCHOR_ELEMENTS)


def test_the_filings_own_dates_beat_the_index():
    """Given a period the index got wrong, the dates the filing states win."""
    rows = facts_from_archive(str(ARCHIVE), FILING,
                              period_start=date(2025, 1, 1),
                              period_end=date(2025, 12, 31), filed=FILED)
    facts = [dict(zip(NAMES, row)) for row in rows]
    current = next(f for f in facts
                   if f["context"] == "CurrentYearDuration_NonConsolidatedMember")

    assert current["prd_start"] == date(2025, 9, 1)
    assert current["prd_end"] == date(2026, 8, 31)


def test_the_five_year_series_lands_on_the_filers_own_month_end():
    """Kojima closes in August, so every year of the series ends on 31
    August and none of them in March."""
    series = sorted(
        (f["prd_start"], f["prd_end"], f["value"]) for f in read()
        if f["concept"].endswith("NetSalesSummaryOfBusinessResults")
        and f["value"] is not None)

    assert len(series) == 5
    assert [str(end) for _, end, _ in series] == [
        "2022-08-31", "2023-08-31", "2024-08-31", "2025-08-31", "2026-08-31"]
    assert [str(start) for start, _, _ in series] == [
        "2021-09-01", "2022-09-01", "2023-09-01", "2024-09-01", "2025-09-01"]
    # And the figures are the ones the report states, in yen.
    assert series[-1][2] == pytest.approx(297_632_000_000)


def test_an_instant_has_no_start():
    facts = read()
    instants = [f for f in facts if f["shape"] == "時点"]

    assert instants
    assert all(f["prd_start"] is None for f in instants)


def test_the_filing_date_instant_is_the_day_it_was_submitted():
    assert period_of("FilingDateInstant", *PERIOD, FILED) == (None, FILED)


def test_a_context_that_is_not_recognised_yields_no_dates():
    """A date invented here would be indistinguishable in the table from one
    the filer reported."""
    assert period_of("SomethingElse", *PERIOD, FILED) == (None, None)
    assert split_context("SomethingElse") is None


def test_a_quarter_or_a_half_year_is_left_without_dates():
    """Which months they cover depends on a calendar the conversion does not
    state."""
    for context in ("CurrentQuarterDuration", "CurrentAccumulatedQ2Duration",
                    "InterimInstant"):
        assert period_of(context, *PERIOD, FILED) == (None, None)


def test_a_forecast_is_left_without_dates():
    assert period_of("NextYearDuration", *PERIOD, FILED) == (None, None)


def test_a_shift_lands_on_the_end_of_february_from_a_leap_day():
    start, end = period_of("Prior1YearDuration", date(2027, 3, 1),
                           date(2028, 2, 29))

    assert end == date(2027, 2, 28)
    assert start == date(2026, 3, 1)


def test_the_context_carries_the_dimensions():
    parts = split_context("Prior1YearInstant_NonConsolidatedMember")

    assert parts["when"] == "Prior"
    assert parts["back"] == 1
    assert parts["kind"] == "Instant"
    assert parts["members"] == "NonConsolidatedMember"

    assert split_context("CurrentYearDuration")["members"] is None
    assert split_context("CurrentYearDuration")["back"] == 0


def test_the_dimensions_reach_the_table():
    facts = read()
    members = {f["dimensions"] for f in facts if f["dimensions"]}

    assert "NonConsolidatedMember" in members


# Values, and what is not one
# --------------------------------------------------------------------------


def test_a_figure_marked_not_applicable_is_no_number_and_no_zero():
    """The conversion writes a full-width dash where a figure does not apply,
    forty-eight times in the real report, and a reader that trusted the unit
    and called float on it would raise on a sound filing."""
    facts = read()
    dashed = [f for f in facts
              if f["concept"].endswith(
                  "EquityInEarningsLossesOfAffiliates"
                  "IfEquityMethodIsAppliedSummaryOfBusinessResults")]

    assert dashed
    assert all(f["value"] is None for f in dashed)
    assert all(f["value"] != 0 for f in dashed)
    # What it said is kept, because it says the figure does not apply.
    assert all(f["text"] == "－" for f in dashed)


def test_a_number_and_a_text_are_never_both_set():
    for fact in read():
        assert fact["value"] is None or fact["text"] is None


def test_the_japanese_label_survives():
    facts = read()
    sales = next(f for f in facts
                 if f["concept"].endswith("NetSalesSummaryOfBusinessResults"))

    assert sales["label"] == "売上高、経営指標等"


# Extensions
# --------------------------------------------------------------------------


def test_the_filers_own_concepts_are_told_from_the_taxonomy():
    assert not is_extension("jppfs_cor:Assets")
    assert not is_extension("jpcrp_cor:NetSales")
    assert not is_extension("jpdei_cor:EDINETCodeDEI")
    # The prefix of an extension is built from the form and the EDINET code.
    assert is_extension("jpcrp030000-asr_E03270-000:SomeConcept")


# Refusing what is not a conversion
# --------------------------------------------------------------------------


def test_a_file_whose_columns_are_not_the_nine_is_refused():
    """Reading such a file by position would put a label in the value column
    and nothing downstream would notice."""
    with pytest.raises(ValueError, match="not one of the agency's conversions"):
        facts_from_csv("a\tb\tc\n1\t2\t3\n", FILING)


def test_an_empty_file_is_no_facts_rather_than_an_error():
    assert facts_from_csv("", FILING) == []


def test_the_header_is_the_nine_columns_the_conversion_writes():
    assert CSV_COLUMNS == ("要素ID", "項目名", "コンテキストID", "相対年度",
                           "連結・個別", "期間・時点", "ユニットID", "単位", "値")
