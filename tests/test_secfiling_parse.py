"""Reading a company file into filings and facts.

The company files are organised by concept and not by filing, so the filings
have to be reconstructed from the accession number each fact carries. The
tests here hold that reconstruction in place: that it is exact rather than
approximate, that a figure a later filing restates appears under both filings
rather than replacing the first, and that the period a filing reports on is
not taken from its cover page, which states a date after the period closed.

The fixtures are trimmed from a real company file of the Commission.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from fincommons.secfiling import parse
from fincommons.secfiling.common import facts_schema, filings_schema

DATA = Path(__file__).parent / "data" / "secfiling"
US = json.loads((DATA / "CIK0001177394.json").read_text(encoding="utf-8"))
IFRS = json.loads((DATA / "CIK0001114448.json").read_text(encoding="utf-8"))

ANNUAL = "0001628280-26-000001"
QUARTERLY = "0001628280-26-000002"


def rows(records, schema):
    return [dict(zip(schema.names, record)) for record in records]


# The central index key
# --------------------------------------------------------------------------


def test_a_key_is_padded_to_the_ten_digits_the_files_use():
    assert parse.padded_cik(1177394) == "0001177394"
    assert parse.padded_cik("1177394") == "0001177394"
    assert parse.padded_cik("CIK0001177394") == "0001177394"


def test_a_key_that_is_not_a_number_is_refused():
    """It becomes the name of a file in the Bronze layer and part of an
    address asked of the network."""
    for value in ("../etc/passwd", "1177394/..", "", "abc"):
        with pytest.raises(ValueError, match="central index key"):
            parse.padded_cik(value)


def test_an_accession_number_is_recognised_by_its_shape():
    assert parse.is_accession("0001628280-26-064256")
    assert not parse.is_accession("0001628280260642")
    assert not parse.is_accession(None)


# Reconstructing the filings
# --------------------------------------------------------------------------


def test_a_filing_is_reconstructed_from_the_accession_numbers():
    filings = rows(parse.filings_from_company(US), filings_schema)

    assert {f["FILING"] for f in filings} == {ANNUAL, QUARTERLY}


def test_a_filing_says_what_it_stated_and_how_much():
    filings = {f["FILING"]: f for f in rows(parse.filings_from_company(US),
                                            filings_schema)}

    annual = filings[ANNUAL]
    assert annual["FORM"] == "10-K"
    assert annual["FILED"] == date(2026, 1, 20)
    assert annual["FACTS"] == 2
    assert annual["NAME"] == "Demo Registrant Inc."
    assert annual["CIK"] == "0001177394"


def test_the_period_is_not_taken_from_the_cover_page():
    """The cover page states the shares outstanding at a date after the
    period closed -- here 23 September for a period ending 31 August -- so
    counting it would make the filing look as though it reported on it."""
    filings = {f["FILING"]: f for f in rows(parse.filings_from_company(US),
                                            filings_schema)}

    assert filings[QUARTERLY]["PERIODEND"] == date(2026, 8, 31)


def test_the_period_is_the_latest_the_filing_reports_on():
    """The quarterly report states the prior year end as a comparative; the
    period it is about is the later of the two."""
    quarterly = [f for f in rows(parse.filings_from_company(US), filings_schema)
                 if f["FILING"] == QUARTERLY][0]

    assert quarterly["PERIODEND"] == date(2026, 8, 31)
    assert quarterly["FP"] == "Q3"
    assert quarterly["FY"] == 2026


# The framework
# --------------------------------------------------------------------------


def test_the_framework_comes_from_the_taxonomies_the_filing_tagged_against():
    filings = rows(parse.filings_from_company(US), filings_schema)

    assert {f["FRAMEWORK"] for f in filings} == {"US-GAAP"}
    assert "dei;us-gaap" in {f["TAXONOMIES"] for f in filings}


def test_a_foreign_private_issuer_reporting_under_ifrs_is_recognised():
    """It files with the Commission and tags against the IFRS taxonomy.
    Reading the framework off the regulator would file it under US GAAP and
    make it incomparable with the European reports it is comparable to."""
    filings = rows(parse.filings_from_company(IFRS), filings_schema)

    assert [f["FRAMEWORK"] for f in filings] == ["IFRS"]


def test_the_cover_page_taxonomy_says_nothing_about_the_framework():
    """Every registrant tags against dei whichever framework it reports under."""
    assert parse._framework({"dei"}) is None


# The facts
# --------------------------------------------------------------------------


def test_a_fact_carries_the_filing_it_came_from():
    facts = rows(parse.facts_from_company(US), facts_schema)

    assert all(parse.is_accession(f["FILING"]) for f in facts)
    assert {f["CIK"] for f in facts} == {"0001177394"}


def test_a_restated_figure_appears_under_both_filings():
    """The company file does not collapse restatements, which is what makes
    the reconstruction exact: each filing says what it said."""
    facts = rows(parse.facts_from_company(US), facts_schema)
    assets = [f for f in facts
              if f["CONCEPT"] == "us-gaap:Assets" and f["PRDEND"] == date(2025, 11, 30)]

    assert {f["FILING"] for f in assets} == {ANNUAL, QUARTERLY}
    assert sorted(f["VALUE"] for f in assets) == [100.0, 101.0]


def test_a_duration_and_an_instant_are_told_apart():
    facts = {f["CONCEPT"]: f for f in rows(parse.facts_from_company(US),
                                           facts_schema)}

    assert facts["us-gaap:Revenues"]["PRDSTART"] == date(2024, 12, 1)
    assert facts["us-gaap:Revenues"]["PRDEND"] == date(2025, 11, 30)
    assert facts["dei:EntityCommonStockSharesOutstanding"]["PRDSTART"] is None


def test_a_fact_gets_an_identifier_that_is_the_same_on_a_second_reading():
    """A company file is republished whole and read again, and the table must
    not grow when it is."""
    first = [f[facts_schema.names.index("FACT")] for f in parse.facts_from_company(US)]
    again = [f[facts_schema.names.index("FACT")] for f in parse.facts_from_company(US)]

    assert first == again
    assert len(set(first)) == len(first)


def test_the_partition_is_the_year_the_filing_was_made():
    """The year a filing was made is monotonic; the fiscal year is not, since
    one filing states several of them."""
    facts = rows(parse.facts_from_company(US), facts_schema)

    assert all(f["FILEDYEAR"] == f["FILED"].year for f in facts)


# Which forms are kept
# --------------------------------------------------------------------------


def test_only_the_periodic_reports_are_kept_by_default():
    """A company file holds the facts of every filing that tagged anything,
    an 8-K announcing a result among them."""
    facts = rows(parse.facts_from_company(US), facts_schema)

    assert "8-K" not in {f["FORM"] for f in facts}
    assert "us-gaap:AnnouncedResult" not in {f["CONCEPT"] for f in facts}


def test_every_form_is_kept_where_the_run_asks_for_it():
    facts = rows(parse.facts_from_company(US, forms=None), facts_schema)

    assert "8-K" in {f["FORM"] for f in facts}


def test_an_amendment_counts_as_the_form_it_amends():
    assert parse.is_periodic("10-K/A")
    assert parse.is_periodic("20-F")
    assert not parse.is_periodic("424B7")
    assert not parse.is_periodic(None)


# Reading both at once
# --------------------------------------------------------------------------


def test_the_counts_of_a_filing_match_the_facts_it_yields():
    filings, facts = parse.read_company(US)
    counted = {f[filings_schema.names.index("FILING")]:
               f[filings_schema.names.index("FACTS")] for f in filings}

    actual = {}
    for fact in facts:
        key = fact[facts_schema.names.index("FILING")]
        actual[key] = actual.get(key, 0) + 1

    assert counted == actual
