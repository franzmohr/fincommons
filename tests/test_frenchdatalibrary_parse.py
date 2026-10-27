"""Tests of reading a file of the French Data Library.

The files are texts written for a person, and the reader recognises their
tables by shape: a header row that begins with a comma, the lines of text
above it as its title, rows keyed by a year, a month or a day. The tests run
it over small files written in the layouts the library uses -- they are in
``tests/data/frenchdatalibrary``, synthetic values in the library's shape --
and over the odd turns of the real files that the layouts alone do not show:
a title split over two lines, a lone carriage return before a header, a blank
line between a header and its first row, two tables titled alike. None of
them needs Spark or the network.

The last tests hold every title the library published in its 202609 edition
and check that each one is filed under a code of the vocabulary, so that a
change to the rules that loses one fails here rather than in a run.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy
import pytest

from fincommons.frenchdatalibrary import parse as P

FIXTURES = Path(__file__).resolve().parent / "data" / "frenchdatalibrary"


def lines_of(name):
    return P.split_lines(P.decode((FIXTURES / f"{name}.csv").read_bytes()))


def read(name):
    return P.parse_lines(name, lines_of(name))


def one(frame, **where):
    rows = frame
    for column, value in where.items():
        rows = rows[rows[column] == value]
    assert len(rows) == 1, f"expected one row for {where}, got {len(rows)}"
    return rows.iloc[0]


# The layouts
# --------------------------------------------------------------------------


def test_the_untitled_first_table_of_a_factor_file_is_its_monthly_factors():
    frame = read("F-F_Research_Data_Factors")

    monthly = frame[frame.FREQUENCY == "M"]
    assert set(monthly.TABLE) == {"FACTORS"}
    assert set(monthly.TITLE) == {""}
    assert list(dict.fromkeys(monthly.SERIES)) == ["Mkt-RF", "SMB", "HML", "RF"]
    # A month is dated by its last day.
    row = one(frame, FREQUENCY="M", SERIES="RF", REFDT=datetime(2024, 2, 29))
    assert row.VALUE == 0.4


def test_the_annual_factors_are_dated_at_the_end_of_the_year():
    frame = read("F-F_Research_Data_Factors")

    annual = frame[frame.FREQUENCY == "A"]
    assert set(annual.TABLE) == {"FACTORS"}
    assert set(annual.TITLE) == {"Annual Factors: January-December"}
    assert sorted(set(annual.REFDT)) == [datetime(2023, 12, 31),
                                         datetime(2024, 12, 31)]


def test_a_title_written_over_two_lines_is_joined():
    frame = read("F-F_Momentum_Factor")

    annual = frame[frame.FREQUENCY == "A"]
    assert set(annual.TITLE) == {"Annual Factors: January-December"}
    assert set(annual.TABLE) == {"FACTORS"}
    assert len(annual) == 2
    assert len(frame[frame.FREQUENCY == "M"]) == 24


def test_the_preamble_and_the_copyright_line_are_not_read_as_values():
    frame = read("F-F_Research_Data_Factors")

    # 24 months and 2 years of 4 factors, less the one missing value.
    assert len(frame) == 24 * 4 + 2 * 4 - 1
    assert not frame.isna().any().any()


def test_every_table_of_a_portfolio_file_is_filed_under_its_code():
    frame = read("6_Portfolios_2x3")

    tables = set(frame.groupby(["TABLE", "FREQUENCY"]).size().index)
    assert tables == {
        ("RET_VW", "M"), ("RET_EW", "M"), ("RET_VW", "A"), ("RET_EW", "A"),
        ("NFIRMS", "M"), ("AVG_SIZE", "M"), ("BEME_VW", "M"), ("BEME_JUN_VW", "M"),
    }
    vw = frame[(frame.TABLE == "RET_VW") & (frame.FREQUENCY == "M")]
    assert list(dict.fromkeys(vw.SERIES)) == [
        "SMALL LoBM", "ME1 BM2", "SMALL HiBM", "BIG LoBM", "ME2 BM2", "BIG HiBM"]
    assert set(vw.TITLE) == {"Average Value Weighted Returns -- Monthly"}
    # The long titles are kept, whitespace collapsed, for traceability.
    beme = frame[frame.TABLE == "BEME_VW"].TITLE.iloc[0]
    assert beme.startswith("For portfolios formed in June of year t Value Weight")
    assert "  " not in beme


def test_both_missing_values_are_left_out():
    frame = read("6_Portfolios_2x3")

    ew = frame[(frame.TABLE == "RET_EW") & (frame.FREQUENCY == "M")]
    nfirms = frame[frame.TABLE == "NFIRMS"]
    assert len(ew) == 24 * 6 - 1          # one -99.99
    assert len(nfirms) == 24 * 6 - 1      # one -999
    assert not frame.VALUE.isin(P.MISSING_VALUES).any()


def test_the_values_are_kept_as_published():
    frame = read("6_Portfolios_2x3")

    row = one(frame, TABLE="RET_VW", FREQUENCY="M", SERIES="SMALL LoBM",
              REFDT=datetime(2023, 1, 31))
    assert row.VALUE == -1.3149


def test_the_breakpoints_without_a_header_are_named():
    frame = read("ME_Breakpoints")

    assert set(frame.TABLE) == {"BREAKPOINTS"}
    assert set(frame.FREQUENCY) == {"M"}
    series = list(dict.fromkeys(frame.SERIES))
    assert series == ["N"] + [f"P{p:02d}" for p in range(5, 101, 5)]
    # The trailing comma of each row is not read as a value.
    assert len(frame) == 3 * 21


def test_the_breakpoints_keep_the_counts_their_header_names():
    frame = read("BE-ME_Breakpoints")

    series = list(dict.fromkeys(frame.SERIES))
    assert series[:3] == ["<= 0", ">0", "P05"]
    assert series[-1] == "P100"
    assert len(series) == 22
    assert set(frame.FREQUENCY) == {"A"}


def test_a_breakpoint_row_of_an_unknown_width_is_refused():
    with pytest.raises(ValueError, match="percentiles"):
        P.parse_lines("ME_Breakpoints", ["", "202407, 1, 2, 3, 4"])


def test_a_weekly_file_is_told_apart_from_a_daily_one_by_its_name():
    weekly = read("F-F_Research_Data_Factors_weekly")
    daily = read("F-F_Research_Data_Factors_daily")

    assert set(weekly.FREQUENCY) == {"W"}
    assert set(daily.FREQUENCY) == {"D"}
    # A week is dated by the day the file keys it by.
    assert weekly.REFDT.min() == datetime(2024, 1, 5)
    assert daily.REFDT.max() == datetime(2024, 1, 8)


# The odd turns of the real files
# --------------------------------------------------------------------------


def test_a_lone_carriage_return_does_not_cut_a_title_off_its_table():
    # 6_Portfolios_2x3 has one at the start of a header row. Split there, the
    # header would sit below a blank line and lose its title.
    text = ("Preamble.\n\n  Average Equal Weighted Returns -- Annual\n"
            "\r,Lo,Hi\n2024, 1.0, 2.0\n")
    frame = P.parse_lines("Example", P.split_lines(text))

    assert set(frame.TABLE) == {"RET_EW"}
    assert set(frame.TITLE) == {"Average Equal Weighted Returns -- Annual"}


def test_line_ends_are_cut_whichever_the_file_uses():
    assert P.split_lines("a\r\nb\nc\r\n") == ["a", "b", "c"]
    assert P.split_lines("a\n\rb") == ["a", "\rb"]


def test_a_blank_line_between_a_header_and_its_first_row_is_passed_over():
    # Portfolios_Formed_on_VAR has one.
    lines = ["", "  Value Weighted Returns -- Annual from January to December",
             ",Lo 20,Hi 20", "", "  1964,   15.70,   8.00", "  1965,   5.59,  49.32"]
    frame = P.parse_lines("Portfolios_Formed_on_VAR", lines)

    assert set(frame.TABLE) == {"RET_VW"}
    assert len(frame) == 4


def test_two_tables_titled_alike_are_numbered_rather_than_merged(caplog):
    # 6_Portfolios_2x3_weekly titles its first two tables alike.
    lines = ["", "  Average Value Weighted Returns -- Weekly", ",A,B",
             "20240105, 1.0, 2.0", "", "",
             "  Average Value Weighted Returns -- Weekly", ",A,B",
             "20240105, 3.0, 4.0"]
    with caplog.at_level(logging.WARNING, logger="fincommons"):
        frame = P.parse_lines("6_Portfolios_2x3_weekly", lines)

    assert sorted(set(frame.TABLE)) == ["RET_VW", "RET_VW_2"]
    assert one(frame, TABLE="RET_VW_2", SERIES="A").VALUE == 3.0
    assert any("numbered" in r.getMessage() for r in caplog.records)


def test_a_title_no_rule_matches_is_kept_under_a_code_made_from_it(caplog):
    lines = ["", "  Median Dividend Yield -- Monthly", ",Lo,Hi", "202401, 1, 2"]
    with caplog.at_level(logging.WARNING, logger="fincommons"):
        frame = P.parse_lines("Example", lines)

    assert set(frame.TABLE) == {"MEDIAN_DIVIDEND_YIELD_MONTHLY"}
    assert set(frame.TITLE) == {"Median Dividend Yield -- Monthly"}
    record = next(r for r in caplog.records if "matches no rule" in r.getMessage())
    assert record.levelno == logging.WARNING
    assert record.ctx["table"] == "MEDIAN_DIVIDEND_YIELD_MONTHLY"


def test_a_row_wider_than_its_header_is_refused():
    with pytest.raises(ValueError, match="Line 3 of Example holds 3 values"):
        P.parse_lines("Example", ["", ",A,B", "202401, 1, 2, 3"])


def test_a_value_that_is_not_a_number_is_refused():
    with pytest.raises(ValueError, match="Line 3 of Example holds 'n/a'"):
        P.parse_lines("Example", ["", ",A,B", "202401, 1, n/a"])


def test_a_file_without_a_table_gives_an_empty_frame_of_the_columns():
    frame = P.parse_lines("Example", ["Nothing to see.", "", "Copyright"])

    assert list(frame.columns) == P.COLUMNS
    assert frame.empty


def test_a_file_that_is_not_utf8_is_read_all_the_same():
    assert P.decode("firm’s".encode("utf-8")) == "firm’s"
    assert P.decode(b"firm\x92s") == "firm’s"


def test_the_reference_dates_have_no_time_of_day():
    frame = read("F-F_Research_Data_Factors_daily")

    assert (frame.REFDT == frame.REFDT.dt.normalize()).all()
    assert frame.REFDT.dtype == numpy.dtype("datetime64[ns]")


# The vocabulary
# --------------------------------------------------------------------------


#: Every title the library published in its 202609 edition, with the code it
#: is filed under. Untitled tables -- the first table of a factor file, the
#: breakpoints -- have no title and are tested above.
PUBLISHED_TITLES = [
    ('For portfolios formed in June of year t Value Weight Average of Accruals Calculated for June of t to June of t+1 as: Sum[ME(Mth) * AC/B / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1',
     'AC_VW'),
    ('Value-Weighted Average of ACCR',
     'AC_VW'),
    ('Average Firm Size',
     'AVG_SIZE'),
    ('Average Market Cap',
     'AVG_SIZE'),
    ('For portfolios formed in June of year t Value Weight Average of BE/ME Calculated for June of t to June of t+1 as: Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Jun t)] / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1 and BE(Fiscal Year t-1) is adjusted for net stock issuance to Jun t',
     'BEME_JUN_VW'),
    ('For portfolios formed in June of year t Value Weight Average of BE_FYt-1/ME_June t Calculated for June of t to June of t+1 as: Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Jun t)] / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1 and BE(Fiscal Year t-1) is adjusted for net stock issuance to Jun t',
     'BEME_JUN_VW'),
    ('Sum of BE / Sum of ME',
     'BEME_SUM'),
    ('For portfolios formed in June of year t Value Weight Average of BE/ME Calculated for June of t to June of t+1 as: Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Dec t-1)] / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1 and BE(Fiscal Year t-1) is adjusted for net stock issuance to Dec t-1',
     'BEME_VW'),
    ('Value Weight Average of BE / ME',
     'BEME_VW'),
    ('Value-Weighted Average of BE/ME',
     'BEME_VW'),
    ('For portfolios formed in June of year t Value Weight Average of Prior Beta Calculated for June of t to June of t+1 as: Sum[ME(Mth) * BETA / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1',
     'BETA_VW'),
    ('Value-Weighted Average of Prior Beta',
     'BETA_VW'),
    ('Sum of CF / Sum of ME',
     'CFP_SUM'),
    ('Value Weight Average of CF / ME',
     'CFP_VW'),
    ('Value Weight Average of CF/P when portfolio is formed',
     'CFP_VW'),
    ('Value Weight Average of D/P when portfolio is formed',
     'DP_VW'),
    ('Sum of E / Sum of ME',
     'EP_SUM'),
    ('Value Weight Average of E / ME',
     'EP_VW'),
    ('Value Weight Average of E/P when portfolio is formed',
     'EP_VW'),
    ('Annual Factors: January-December',
     'FACTORS'),
    ('For portfolios formed in June of year t Value Weight Average of investment (rate of growth of assets) Calculated as: Sum[ME(Mth) * Log(ASSET(t-1) / ASSET(t-2) / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1',
     'INV_VW'),
    ('Value Weight Average of Natural Log of INV',
     'INV_VW'),
    ('Number of Firms in Portfolios',
     'NFIRMS'),
    ('For portfolios formed in June of year t Value Weight Average of Net Share Issues Calculated for June of t to June of t+1 as: Sum[ME(Mth) * NI / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1',
     'NI_VW'),
    ('Value-Weighted Average of NI',
     'NI_VW'),
    ('For portfolios formed in June of year t Value Weight Average of OP Calculated as: Sum[ME(Mth) * OP(fiscal year t-1) / BE(fiscal year t-1)] / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1',
     'OP_VW'),
    ('Value Weight Average of OP',
     'OP_VW'),
    ('Equally-Weighted Average of Prior Returns',
     'PRIOR_EW'),
    ('Value-Weighted Average of Prior Returns',
     'PRIOR_VW'),
    ('Average Equal Weighted Returns -- Annual',
     'RET_EW'),
    ('Average Equal Weighted Returns -- Daily',
     'RET_EW'),
    ('Average Equal Weighted Returns -- Daily Small 2 3 4 Big',
     'RET_EW'),
    ('Average Equal Weighted Returns -- Daily Small Big',
     'RET_EW'),
    ('Average Equal Weighted Returns -- Monthly',
     'RET_EW'),
    ('Equal Weight Average Returns -- Annual',
     'RET_EW'),
    ('Equal Weight Average Returns -- Monthly',
     'RET_EW'),
    ('Equal Weight Returns -- Annual from January to December',
     'RET_EW'),
    ('Equal Weight Returns -- Monthly',
     'RET_EW'),
    ('Equal Weighted Returns -- Annual from January to December',
     'RET_EW'),
    ('Equal Weighted Returns -- Monthly',
     'RET_EW'),
    ('Aerage Value Weighted Returns -- Monthly',
     'RET_VW'),
    ('Average Value Weight Returns -- Monthly',
     'RET_VW'),
    ('Average Value Weighted Returns -- Annual',
     'RET_VW'),
    ('Average Value Weighted Returns -- Daily',
     'RET_VW'),
    ('Average Value Weighted Returns -- Daily Small 2 3 4 Big',
     'RET_VW'),
    ('Average Value Weighted Returns -- Daily Small Big',
     'RET_VW'),
    ('Average Value Weighted Returns -- Monthly',
     'RET_VW'),
    ('Average Value Weighted Returns -- Weekly',
     'RET_VW'),
    ('Value Weight Average Returns -- Annual',
     'RET_VW'),
    ('Value Weight Average Returns -- Monthly',
     'RET_VW'),
    ('Value Weight Returns -- Annual from January to December',
     'RET_VW'),
    ('Value Weight Returns -- Monthly',
     'RET_VW'),
    ('Value Weighted Returns -- Annual from January to December',
     'RET_VW'),
    ('Value Weighted Returns -- Monthly',
     'RET_VW'),
    ('For portfolios formed in June of year t Value Weight Average of Past Return Variance Calculated for June of t to June of t+1 as: Sum[ME(Mth) * VAR / Sum[ME(Mth)] Where Mth is a month from June of t to June of t+1',
     'VAR_VW'),
    ('Value-Weighted Average of Past Return Variance',
     'VAR_VW'),
]


def test_every_published_title_is_filed_under_a_code_of_the_vocabulary(caplog):
    with caplog.at_level(logging.WARNING, logger="fincommons"):
        codes = {title: P.table_code(title) for title, _ in PUBLISHED_TITLES}

    assert codes == dict(PUBLISHED_TITLES)
    assert set(codes.values()) <= set(P.TABLE_CODES)
    assert not caplog.records


def test_every_code_of_the_vocabulary_is_used_and_described():
    used = {code for _, code in PUBLISHED_TITLES} | {"FACTORS", "BREAKPOINTS"}

    assert set(P.TABLE_CODES) == used
    assert all(len(text) > 10 for text in P.TABLE_CODES.values())
