"""The Kenneth R. French Data Library: reading one of its CSV files.

Every file of the library is a text written for a person rather than for a
parser. It opens with a few lines of prose -- the vintage of the database it
was built from, how the portfolios are formed, which value stands for a
missing one -- and then holds one or more tables, each of them a title, a row
of column headers that begins with a comma and a row per period, keyed by the
year (``2025``), the month (``202509``) or the day (``20250930``). A file ends
with a copyright line. The breakpoint files have no header row at all: a row
there is a date, one or more counts of firms and the percentiles.

This module turns those lines into long records -- one per period, table and
series -- and is written in plain Python, without Spark, so that it can be
run over a downloaded file on its own and so that the Silver layer runs the
very same code on its executors. It recognises a table by its shape rather
than by a list of the files, because the library adds and renames files
without notice: whatever begins with a comma is a header, whatever begins
with a period is a row of the table above it, and the lines of text directly
above a header are its title.

The titles are published as prose and vary in their wording from one file to
the next -- ``Average Firm Size`` here, ``Average Market Cap`` there, and once
``Aerage Value Weighted Returns``. :func:`table_code` maps them onto a small
vocabulary of codes, :data:`TABLE_CODES`, so that the same kind of table is
found under the same code in every file. The title itself is kept beside the
code, so that what the code stands for can always be checked against what
the library says.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import numpy
import pandas

from ..log import context

logger = logging.getLogger(__name__)


#: The values the library writes for a missing one. Each file says which in
#: its preamble -- ``Missing data are indicated by -99.99 or -999.`` -- and
#: the two are the only ones that occur. A missing value is not written to the
#: Silver layer at all, rather than as a null: a reader asks for a series and
#: finds the periods it has.
MISSING_VALUES = (-99.99, -999.0)

#: A row of a table begins with its period: a year, a month or a day.
_PERIOD = re.compile(r"^\d{4}(?:\d{2}(?:\d{2})?)?$")

#: The frequency of a row, by the length of its period. A weekly file keys
#: its rows by the last day of the week, which looks like a day; see
#: :func:`frequency_of`.
_FREQUENCY_BY_LENGTH = {4: "A", 6: "M", 8: "D"}


# The vocabulary of tables
# --------------------------------------------------------------------------

#: The codes a table is filed under, and what each of them stands for. The
#: frequency is not part of the code: the monthly and the annual returns of a
#: file are both ``RET_VW``, told apart by the column ``FREQUENCY``. Nor is
#: the treatment of dividends, which the library publishes as files of their
#: own (``..._Wout_Div``), so it is told apart by ``DATASET``.
TABLE_CODES = {
    "FACTORS": "the factor returns of a factor file, in percent",
    "BREAKPOINTS": "the breakpoints of a sort: counts of firms and percentiles",
    "RET_VW": "value-weighted returns of the portfolios, in percent",
    "RET_EW": "equal-weighted returns of the portfolios, in percent",
    "NFIRMS": "the number of firms in each portfolio",
    "AVG_SIZE": "the average market capitalisation of the firms in each "
                "portfolio, in millions of dollars ('Average Firm Size' or "
                "'Average Market Cap')",
    "BEME_SUM": "the sum of book equity over the sum of market equity of each "
                "portfolio",
    "EP_SUM": "the sum of earnings over the sum of market equity of each "
              "portfolio",
    "CFP_SUM": "the sum of cash flow over the sum of market equity of each "
               "portfolio",
    "BEME_VW": "the value-weighted average of BE/ME, market equity taken at "
               "the end of December of the prior year where the title says",
    "BEME_JUN_VW": "the value-weighted average of BE/ME, market equity taken "
                   "at the end of June of the year of formation",
    "OP_VW": "the value-weighted average of operating profitability",
    "INV_VW": "the value-weighted average of investment, the (log) growth of "
              "assets",
    "EP_VW": "the value-weighted average of earnings to price (E/P, E/ME)",
    "CFP_VW": "the value-weighted average of cash flow to price (CF/P, CF/ME)",
    "DP_VW": "the value-weighted average of dividend yield",
    "AC_VW": "the value-weighted average of accruals",
    "NI_VW": "the value-weighted average of net share issues",
    "BETA_VW": "the value-weighted average of the prior market beta",
    "VAR_VW": "the value-weighted average of the variance of past returns",
    "PRIOR_VW": "the value-weighted average of the prior return the "
                "portfolios are formed on",
    "PRIOR_EW": "the equal-weighted average of the prior return the "
                "portfolios are formed on",
}

#: How a title is mapped onto a code: the first pattern that matches the
#: title, whitespace normalised and in lower case, decides. The patterns ask
#: for the words that carry the meaning and tolerate the rest, so that
#: ``Average Value Weighted Returns -- Monthly``, ``Value Weight Returns --
#: Annual from January to December`` and the misspelt ``Aerage Value Weighted
#: Returns`` all land on ``RET_VW``. The averages of a characteristic are
#: matched on the characteristic, and the two BE/ME averages on where market
#: equity is taken: in December of the prior year, which is the definition of
#: the factors, or in June of the year the portfolios are formed.
TABLE_RULES = (
    (r"annual factors", "FACTORS"),
    (r"value[- ]weight(ed)?( average)? returns", "RET_VW"),
    (r"equal(ly)?[- ]weight(ed)?( average)? returns", "RET_EW"),
    (r"number of firms", "NFIRMS"),
    (r"average firm size|average market cap", "AVG_SIZE"),
    (r"sum of be ?/ ?sum of me", "BEME_SUM"),
    (r"sum of e ?/ ?sum of me", "EP_SUM"),
    (r"sum of cf ?/ ?sum of me", "CFP_SUM"),
    (r"average of be_fyt-1/me_june|average of be/me .*me\(jun t\)", "BEME_JUN_VW"),
    (r"average of be ?/ ?me", "BEME_VW"),
    (r"average of op\b", "OP_VW"),
    (r"average of (natural log of )?inv(estment)?\b", "INV_VW"),
    (r"average of e ?/ ?(p|me)\b", "EP_VW"),
    (r"average of cf ?/ ?(p|me)\b", "CFP_VW"),
    (r"average of d ?/ ?p\b", "DP_VW"),
    (r"average of (accruals|accr)\b", "AC_VW"),
    (r"average of (net share issues|ni)\b", "NI_VW"),
    (r"average of prior beta", "BETA_VW"),
    (r"average of past (return )?variance", "VAR_VW"),
    (r"value[- ]weighted average of prior returns", "PRIOR_VW"),
    (r"equally[- ]weighted average of prior returns", "PRIOR_EW"),
)
_TABLE_RULES = tuple((re.compile(pattern), code) for pattern, code in TABLE_RULES)


def normalise_title(lines) -> str:
    """The title of a table as one line, its whitespace collapsed.

    A title is written over one line or several -- the momentum factor
    spells ``Annual Factors:`` and ``January-December`` on two -- and is
    indented and padded at will, so it is joined and its runs of whitespace
    are collapsed. Nothing else about it is changed.
    """
    if isinstance(lines, str):
        lines = [lines]
    return " ".join(" ".join(lines).split())


def slug(title: str) -> str:
    """A code made from a title no rule matches: upper case, words joined by ``_``."""
    words = re.findall(r"[A-Za-z0-9]+", title)
    return "_".join(words).upper()[:60] or "UNTITLED"


def table_code(title: str, breakpoints: bool = False) -> str:
    """The code of :data:`TABLE_CODES` a table with this title is filed under.

    A table of a breakpoint file is a list of breakpoints, and an untitled
    table is the monthly, weekly or daily factors a factor file opens with,
    since a portfolio file titles every one of its tables. Anything else is
    mapped by :data:`TABLE_RULES`.

    **A title no rule matches is not lost.** It is filed under a code made
    from the title itself, :func:`slug`, and a ``WARNING`` is logged, so that
    a table the library adds is in the Silver layer under a name that says
    what it is until a rule is written for it -- rather than missing, or
    under a code that says something it is not.

    Parameters
    ----------
    title: str
        The title as :func:`normalise_title` leaves it.
    breakpoints: bool
        Whether the table is one of a breakpoint file.
    """
    if breakpoints:
        return "BREAKPOINTS"
    if not title:
        return "FACTORS"
    lowered = title.lower()
    for pattern, code in _TABLE_RULES:
        if pattern.search(lowered):
            return code
    code = slug(title)
    logger.warning(
        "A table title matches no rule and is filed under a code made from it",
        extra=context(module="famafrenchdata", title=title, table=code),
    )
    return code


def frequency_of(period: str, weekly: bool = False) -> str:
    """``A``, ``M``, ``W`` or ``D``: the frequency of a row, from its period.

    A weekly file keys its rows by the last day of the week, in the same
    eight digits as a daily file, so the period alone cannot tell the two
    apart; the name of the file does, which is what ``weekly`` says.
    """
    frequency = _FREQUENCY_BY_LENGTH[len(period)]
    return "W" if weekly and frequency == "D" else frequency


def is_weekly(dataset: str) -> bool:
    """Whether a file of the library holds weekly rows, read off its name."""
    return "weekly" in dataset.lower()


# Breakpoints
# --------------------------------------------------------------------------

#: How many percentiles a row of a breakpoint file carries: every fifth NYSE
#: percentile, from the 5th to the 100th, which every breakpoint file of the
#: library says in its preamble.
PERCENTILES = tuple(f"P{p:02d}" for p in range(5, 101, 5))


def is_breakpoints(dataset: str) -> bool:
    """Whether a file of the library holds breakpoints, read off its name."""
    return "breakpoints" in dataset.lower()


def breakpoint_series(dataset: str, published, width: int) -> list[str]:
    """The names of the columns of a breakpoint file.

    A row there is one or more counts of firms followed by the twenty
    percentiles of :data:`PERCENTILES`. The files name the counts, where they
    name anything: the breakpoints of a ratio carry a header of two columns,
    ``<= 0`` and ``>0``, the number of firms whose ratio is not positive and
    is, and leave the percentiles unnamed. The ME and the prior-return
    breakpoints carry no header at all and one count, the number of NYSE
    firms, which is named ``N`` here. The percentiles are named ``P05`` to
    ``P100``.

    A row whose width fits none of this is refused rather than named wrongly.

    Parameters
    ----------
    dataset: str
        The name of the file, for the message.
    published: list of str
        The names the header gives, none where there is no header.
    width: int
        The number of values in a row, the date not counted.
    """
    counts = width - len(PERCENTILES)
    if published and len(published) == counts:
        return list(published) + list(PERCENTILES)
    if not published and counts == 1:
        return ["N"] + list(PERCENTILES)
    raise ValueError(
        f"A row of the breakpoints {dataset} holds {width} values under a "
        f"header of {len(published)} names, which is not one or more counts "
        f"followed by the {len(PERCENTILES)} percentiles the library publishes."
    )


# The tables of a file
# --------------------------------------------------------------------------


@dataclass
class Table:
    """One table of a file, as it is read: a title, columns and rows."""

    title: str
    columns: list[str]
    lineno: int
    breakpoints: bool = False
    periods: list[str] = field(default_factory=list)
    rows: list[list[float]] = field(default_factory=list)


def _values(fields, lineno, dataset):
    """The values of one row; an empty field is a missing value."""
    values = []
    for text in fields:
        text = text.strip()
        try:
            values.append(float(text) if text else numpy.nan)
        except ValueError:
            raise ValueError(
                f"Line {lineno} of {dataset} holds {text!r} where a number "
                "was expected."
            ) from None
    return values


def read_tables(dataset: str, lines) -> list[Table]:
    """Split the lines of a file into its tables.

    The preamble, the notes between the tables and the copyright line are
    passed over; they are kept in the Bronze layer, not here. The text of a
    file is read for its shape:

    * a line that begins with a comma is the header of a new table, and the
      lines of text directly above it, up to the blank line before them, are
      its title -- none at all for the first table of a factor file;
    * a line that begins with a period is a row of the table above it, or the
      first row of a table without a header where there is none above it,
      which is how a breakpoint file is laid out;
    * a line of text ends a table, and so does a blank line once the table
      has a row.

    Lines end at a line feed alone, see :func:`split_lines`.

    Parameters
    ----------
    dataset: str
        The name of the file, for the messages and the breakpoint columns.
    lines: iterable of str
        The lines of the file, in order, with or without their line ends.

    Raises
    ------
    ValueError
        If a row holds more values than its table has columns, or a value
        that is not a number. A row the reader cannot place is a sign that
        the layout has changed, and a table read on regardless would put
        values under the wrong series.
    """
    breakpoints = is_breakpoints(dataset)
    tables = []
    pending = []      # the lines of text since the last blank line
    current = None
    for lineno, line in enumerate(lines, start=1):
        text = line.strip()
        if not text:
            # A blank line ends a table, but not one that has a header and no
            # row yet: Portfolios_Formed_on_VAR has a blank line between the
            # header and the first row of a table.
            pending = []
            if current is not None and current.rows:
                current = None
            continue
        if text.startswith(","):
            current = Table(normalise_title(pending),
                            [c.strip() for c in text.split(",")[1:]], lineno,
                            breakpoints=breakpoints)
            tables.append(current)
            pending = []
            continue
        fields = text.split(",")
        period = fields[0].strip()
        if _PERIOD.match(period) and len(fields) > 1:
            values = _values(fields[1:], lineno, dataset)
            # A trailing comma leaves an empty field behind the last value.
            while values and numpy.isnan(values[-1]) and not fields[-1].strip():
                values.pop()
                fields.pop()
            if current is None:
                # A row without a header above it: the layout of the ME
                # breakpoints.
                current = Table(normalise_title(pending), [], lineno,
                                breakpoints=breakpoints)
                tables.append(current)
                pending = []
            if current.breakpoints and not current.periods:
                current.columns = breakpoint_series(dataset, current.columns,
                                                    len(values))
            width = len(current.columns)
            if len(values) > width:
                raise ValueError(
                    f"Line {lineno} of {dataset} holds {len(values)} values "
                    f"for a table of {width} columns, which begins at line "
                    f"{current.lineno}."
                )
            values = values + [numpy.nan] * (width - len(values))
            current.periods.append(period)
            current.rows.append(values)
            continue
        # Text: a title in the making, or a note after a table.
        current = None
        pending.append(text)
    return tables


#: The columns of the long records, in their order.
COLUMNS = ["REFDT", "TABLE", "FREQUENCY", "SERIES", "VALUE", "TITLE"]


def _refdt(periods):
    """The reference date of each period.

    A day or a week is the date it is keyed by. A month is its last day and a
    year the 31st of December, the end of the period a return is earned
    over, which is also the date a month of returns is usually joined on.
    """
    periods = pandas.Series(periods, dtype="object")
    refdt = pandas.Series(pandas.NaT, index=periods.index, dtype="datetime64[ns]")
    for length, fmt in ((8, "%Y%m%d"), (6, "%Y%m"), (4, "%Y")):
        selected = periods.str.len() == length
        if not selected.any():
            continue
        dates = pandas.to_datetime(periods[selected], format=fmt)
        if length == 6:
            dates = dates + pandas.offsets.MonthEnd(0)
        elif length == 4:
            dates = dates + pandas.offsets.YearEnd(0)
        refdt[selected] = dates
    return refdt


def parse_lines(dataset: str, lines) -> pandas.DataFrame:
    """The values of one file of the library, one row per period, table and series.

    Parameters
    ----------
    dataset: str
        The name of the file without its suffix, ``F-F_Research_Data_Factors``.
    lines: iterable of str
        The lines of the file, in order.

    Returns
    -------
    pandas.DataFrame
        The columns of :data:`COLUMNS`: ``REFDT`` (``datetime64``), ``TABLE``,
        ``FREQUENCY``, ``SERIES``, ``VALUE`` as published and ``TITLE``.
        Missing values (:data:`MISSING_VALUES`) are left out, and so is every
        row without a value.

    Raises
    ------
    ValueError
        If a row cannot be placed, see :func:`read_tables`.

    Notes
    -----
    Two tables of one file that land on the same code and frequency would be
    indistinguishable in the Silver layer, and the second would be read as
    more rows of the first. The later one is numbered instead, ``RET_VW_2``,
    and a ``WARNING`` is logged. It happens where the library titles two
    tables alike: ``6_Portfolios_2x3_weekly`` titles its first two tables
    ``Average Value Weighted Returns -- Weekly`` both, where the daily file
    of the same portfolios has value- and then equal-weighted returns. Which
    of the two the second one is, the file does not say, so it is not
    guessed here; the title is kept, as everywhere.
    """
    weekly = is_weekly(dataset)
    frames = []
    seen = set()
    for table in read_tables(dataset, lines):
        if not table.rows:
            continue
        code = table_code(table.title, table.breakpoints)
        values = numpy.asarray(table.rows, dtype="float64")
        periods = numpy.asarray(table.periods, dtype="object")
        frequencies = numpy.asarray(
            [frequency_of(p, weekly) for p in table.periods], dtype="object")

        present = set(frequencies)
        if any((code, f) in seen for f in present):
            number = 2
            while any((f"{code}_{number}", f) in seen for f in present):
                number += 1
            logger.warning(
                "Two tables of a file are filed under the same code, the "
                "later one is numbered",
                extra=context(module="famafrenchdata", dataset=dataset,
                              title=table.title, table=f"{code}_{number}",
                              line=table.lineno),
            )
            code = f"{code}_{number}"
        seen.update((code, f) for f in present)

        n, k = values.shape
        flat = values.reshape(-1)
        keep = ~numpy.isnan(flat)
        for missing in MISSING_VALUES:
            keep &= ~numpy.isclose(flat, missing, rtol=0.0, atol=1e-9)
        rows = numpy.repeat(numpy.arange(n), k)[keep]
        cols = numpy.tile(numpy.arange(k), n)[keep]
        refdt = _refdt(periods).to_numpy()
        frames.append(pandas.DataFrame({
            "REFDT": refdt[rows],
            "TABLE": code,
            "FREQUENCY": frequencies[rows],
            "SERIES": numpy.asarray(table.columns, dtype="object")[cols],
            "VALUE": flat[keep],
            "TITLE": table.title,
        }))
    if not frames:
        return pandas.DataFrame({c: pandas.Series(dtype="datetime64[ns]" if c == "REFDT"
                                                  else "float64" if c == "VALUE"
                                                  else "object")
                                 for c in COLUMNS})
    return pandas.concat(frames, ignore_index=True)[COLUMNS]


def decode(raw: bytes) -> str:
    """The text of a file of the library.

    The files are ASCII but for the odd typographic apostrophe in a note,
    which is written in UTF-8. A file that is not UTF-8 is read as Windows
    Latin-1, which every byte is a character of, so that the Bronze layer
    keeps whatever the library sent rather than failing over a character in
    a note.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def split_lines(text: str) -> list[str]:
    """The lines of a file, without their line ends.

    A line ends at a line feed, with or without a carriage return before it,
    and nowhere else. :meth:`str.splitlines` would also break a line at a
    lone carriage return, and the library has those: ``6_Portfolios_2x3``
    puts one at the start of a header row, which split there would leave a
    blank line between the table and its title, and the title would be lost.
    The carriage return is kept in the line instead, where stripping the
    line removes it.
    """
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [line[:-1] if line.endswith("\r") else line for line in lines]


__all__ = [
    "COLUMNS",
    "MISSING_VALUES",
    "PERCENTILES",
    "TABLE_CODES",
    "TABLE_RULES",
    "breakpoint_series",
    "decode",
    "frequency_of",
    "is_breakpoints",
    "is_weekly",
    "normalise_title",
    "parse_lines",
    "read_tables",
    "split_lines",
    "table_code",
]
