"""Market price data: the corporate actions of the Dhaka Stock Exchange.

The day-end archive of the DSE publishes the closes the shares traded at and
nothing else, so a bonus issue reaches it as a crash. City Bank closed at 22.80
taka on the 21st of May 2025 and at 19.00 on the next day it traded, which is
not a loss of a sixth of its value but a 12.5% stock dividend and a 12.5% cash
dividend going ex. Bonus issues are the ordinary way a Bangladeshi company pays
out -- most banks issue one every year -- so a return computed from these
closes without them is not merely noisy but wrong in one direction.

The exchange does not publish ex-dates, and its corporate action page carries
no feed yet. It does publish, in its news archive, the two notices that fix one:

* the **dividend declaration**, which names the cash and stock dividend as a
  percentage of the face value and the record date that decides who receives
  them, and
* the **resumption after record date**, which names the day trading resumes
  once the register has been drawn up. Trading in the share is suspended on the
  record date itself, so the day it resumes is the first day it trades ex.

This module reads those notices out of the news rows and turns them into one
row per event with the factor the closes before it have to be multiplied by.
It is plain pandas and knows nothing of Spark or of the data lake, so that it
can be tested on its own and so that a reader of the lake can run it over rows
it collected some other way.

Face values
    A cash dividend is declared as a percentage of the face value. The
    Securities and Exchange Commission set the face value of every listed share
    to ten taka in 2011, so ten is what is assumed; a share quoted with another
    face value would have its cash dividends misstated by the same proportion.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import pandas as pd

#: The face value a cash dividend percentage is taken of, in taka.
DSE_FACE_VALUE = 10.0

#: How long after a record date the resumption notice is looked for, in days.
#: A resumption follows its record date by one trading day; ten calendar days
#: covers Eid closures without reaching the next event.
RESUMPTION_WINDOW_DAYS = 10

#: How long before a rights record date the notice that sets its terms is
#: looked for, in days. The Commission's consent and the record date follow
#: the board's decision by months, not years.
RIGHTS_TERMS_WINDOW_DAYS = 400

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

_DATE = r"(\d{1,2})\s*[./\- ]\s*(\d{1,2}|[A-Za-z]{3,9})\s*[./\- ,]\s*(\d{4}|\d{2})"
_DATE_RE = re.compile(_DATE)
# "May 06, 2026", which some companies write instead.
_MONTH_FIRST_RE = re.compile(r"([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})")
_RECORD_DATE_RE = re.compile(r"record\s+date", re.IGNORECASE)
_RESUME_RE = re.compile(r"resume\s+on", re.IGNORECASE)

#: How far past its keyword a date is looked for, in characters. Far enough
#: for "Record Date for the entitlement of 5% Stock Dividend will be ...", not
#: so far that it reaches the next sentence's date.
_DATE_REACH = 120
# "15% Stock Dividend", and "Stock Dividend @ 3.00%" the other way round.
_STOCK_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*(?:final\s+|interim\s+)?(?:stock|bonus)"
    r"|(?:stock|bonus)\s+dividend\s*@\s*(\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE)
_CASH_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*(?:final\s+|interim\s+)?cash"
    r"|cash\s+dividend\s*@\s*(\d+(?:\.\d+)?)\s*%", re.IGNORECASE)
_RIGHTS_RATIO_RE = re.compile(
    r"(\d+)\s*\(?[A-Za-z ]*\)?\s*rights?\s+shares?\s+(?:against|for\s+every)\s+(\d+)",
    re.IGNORECASE)
_RIGHTS_PRICE_RE = re.compile(
    r"issue\s+price\s+of\s+(?:BDT|Tk\.?)\s*([\d,]+(?:\.\d+)?)", re.IGNORECASE)
_AT_PAR_RE = re.compile(r"at\s+par", re.IGNORECASE)


def parse_dse_date(text):
    """The first date written in a notice, or None.

    The notices write their dates in every way the exchange has ever used:
    ``17.11.2024``, ``17-May-2026``, ``07-Sep-2026`` and the occasional two
    digit year. A day and a month that cannot be a date are passed over rather
    than guessed at.
    """
    if not isinstance(text, str):
        return None
    candidates = [(m.start(), m.groups()) for m in _DATE_RE.finditer(text)]
    candidates += [(m.start(), (m.group(2), m.group(1), m.group(3)))
                   for m in _MONTH_FIRST_RE.finditer(text)]
    for _, groups in sorted(candidates):
        found = _to_date(*groups)
        if found is not None:
            return found
    return None


def _to_date(day, month, year):
    try:
        day = int(day)
        month = int(month) if month.isdigit() else _MONTHS.get(month[:3].lower())
        year = int(year)
        if year < 100:
            year += 2000
        if month is None:
            return None
        return date(year, month, day)
    except (ValueError, TypeError):
        return None


def _first(pattern, text):
    """The first date written after the first occurrence of a keyword."""
    for match in pattern.finditer(text or ""):
        found = parse_dse_date(text[match.end():match.end() + _DATE_REACH])
        if found is not None:
            return found
    return None


def join_continuations(news):
    """One text per notice, its continuations appended in the order filed.

    A notice longer than the archive allows is split into rows of the same
    code, day and summary, the later ones opening with ``(cont. news of ...)``,
    and the record date is often in the second of them. The rows are put back
    together by their identifier, which the exchange issues in sequence.

    Parameters
    ----------
    news: pandas.DataFrame
        News rows with the columns ``ID``, ``CODE``, ``FILEDAT``, ``SUMMARY``
        and ``BODY``.
    """
    if news.empty:
        return pd.DataFrame(columns=["ID", "CODE", "FILEDAT", "SUMMARY", "TEXT"])

    news = news.drop_duplicates("ID").copy()
    news["FILEDAT"] = pd.to_datetime(news["FILEDAT"]).dt.date
    news["BODY"] = news["BODY"].fillna("").str.replace(
        r"^\s*\((?:cont(?:inuation)?\.?\s*news\s+of\s+[^)]*)\)\s*:?\s*", "",
        regex=True, flags=re.IGNORECASE)
    news = news.sort_values("ID")
    joined = news.groupby(["CODE", "FILEDAT", "SUMMARY"], as_index=False) \
                 .agg(ID=("ID", "first"), BODY=("BODY", " ".join))
    joined["TEXT"] = joined["SUMMARY"].fillna("") + ". " + joined["BODY"]
    return joined[["ID", "CODE", "FILEDAT", "SUMMARY", "TEXT"]]


def resumption_dates(notices):
    """The day trading resumed after each record date, by code."""
    rows = []
    for notice in notices.itertuples():
        if not re.search(r"resumption\s+after\s+record\s+date",
                         notice.SUMMARY or "", re.IGNORECASE):
            continue
        resumed = _first(_RESUME_RE, notice.TEXT)
        if resumed is not None:
            rows.append({"CODE": notice.CODE, "RESUMED": resumed,
                         "ADJUSTED_OPEN": "adjusted open price" in notice.TEXT.lower()})
    return pd.DataFrame(rows, columns=["CODE", "RESUMED", "ADJUSTED_OPEN"])


def record_dates(notices):
    """The record dates trading was suspended for, and what each one was for."""
    rows = []
    for notice in notices.itertuples():
        if not re.search(r"suspension\s+for\s+record\s+date",
                         notice.SUMMARY or "", re.IGNORECASE):
            continue
        recorded = _first(_RECORD_DATE_RE, notice.TEXT) or \
            parse_dse_date(notice.TEXT)
        if recorded is not None:
            rows.append({"CODE": notice.CODE, "RECORD": recorded,
                         "RIGHTS": bool(re.search(r"rights?", notice.TEXT,
                                                  re.IGNORECASE))})
    return pd.DataFrame(rows, columns=["CODE", "RECORD", "RIGHTS"])


def dividend_declarations(notices):
    """The cash and stock dividends declared, one row per code and record date.

    A declaration is read from any notice that names a dividend percentage and
    a record date. Where one record date is named by more than one notice, the
    latest filed is kept: a stock dividend needs the Commission's consent, and
    the consent can come back with a different percentage than the board
    recommended, as Dhaka Bank's did in June 2025. A notice without a record
    date is not a declaration that can be placed and is left out.
    """
    rows = []
    for notice in notices.itertuples():
        text = notice.TEXT or ""
        if "dividend" not in text.lower():
            continue
        if re.search(r"disburse|credited", notice.SUMMARY or "", re.IGNORECASE):
            continue
        stock = _STOCK_RE.search(text)
        cash = _CASH_RE.search(text)
        if stock is None and cash is None:
            continue
        recorded = _first(_RECORD_DATE_RE, text)
        if recorded is None:
            continue
        rows.append({
            "CODE": notice.CODE,
            "RECORD": recorded,
            "FILEDAT": notice.FILEDAT,
            "STOCK_PCT": _percent(stock),
            "CASH_PCT": _percent(cash),
            "SOURCE": notice.ID,
        })
    frame = pd.DataFrame(rows, columns=["CODE", "RECORD", "FILEDAT",
                                        "STOCK_PCT", "CASH_PCT", "SOURCE"])
    return frame.sort_values(["CODE", "RECORD", "FILEDAT", "SOURCE"]) \
                .drop_duplicates(["CODE", "RECORD"], keep="last") \
                .reset_index(drop=True)


def _percent(match):
    """The percentage a dividend pattern matched, whichever way it was written."""
    if match is None:
        return 0.0
    return float(next(g for g in match.groups() if g is not None))


def rights_terms(notices):
    """The ratio and price of each rights issue announced, by code and day.

    The terms are written as ``1 Rights Share against 17 existing shares`` and
    ``issue price of BDT 1,110``; an issue ``at par`` is priced at the face
    value.
    """
    rows = []
    for notice in notices.itertuples():
        text = notice.TEXT or ""
        ratio = _RIGHTS_RATIO_RE.search(text)
        if ratio is None:
            continue
        price = _RIGHTS_PRICE_RE.search(text)
        if price is not None:
            subscription = float(price.group(1).replace(",", ""))
        elif _AT_PAR_RE.search(text):
            subscription = DSE_FACE_VALUE
        else:
            continue
        rows.append({"CODE": notice.CODE, "FILEDAT": notice.FILEDAT,
                     "NEW": int(ratio.group(1)), "HELD": int(ratio.group(2)),
                     "PRICE": subscription, "SOURCE": notice.ID})
    return pd.DataFrame(rows, columns=["CODE", "FILEDAT", "NEW", "HELD",
                                       "PRICE", "SOURCE"])


def _ex_date(code, recorded, resumptions, traded):
    """The first day a share traded after its record date.

    The resumption notice says so directly and is preferred. Without one, the
    first day the share traded after the record date is the same day, read off
    the prices instead.
    """
    later = resumptions[(resumptions["CODE"] == code)
                        & (resumptions["RESUMED"] > recorded)
                        & (resumptions["RESUMED"] <= recorded
                           + timedelta(days=RESUMPTION_WINDOW_DAYS))]
    if not later.empty:
        return later["RESUMED"].min(), "notice"
    days = traded.get(code)
    if days is not None:
        after = days[days > recorded]
        if len(after) and after[0] <= recorded + timedelta(days=RESUMPTION_WINDOW_DAYS):
            return after[0], "prices"
    return None, None


def _close_before(closes, code, ex_date):
    """The last traded close before an ex-date, the price the event is cut at."""
    series = closes.get(code)
    if series is None:
        return None
    before = series[series.index < ex_date]
    return float(before.iloc[-1]) if len(before) else None


def corporate_actions(news, prices):
    """The corporate actions of DSE shares, one row per event and kind.

    Parameters
    ----------
    news: pandas.DataFrame
        News rows as the archive delivers them, with the columns ``ID``,
        ``CODE``, ``FILEDAT``, ``SUMMARY`` and ``BODY``.
    prices: pandas.DataFrame
        Day-end rows with the columns ``REFDT``, ``CODE``, ``CLOSE`` and
        ``TRADES``. Only the days a share traded are read.

    Returns
    -------
    pandas.DataFrame
        One row per event with the columns

        ``CODE``, ``EXDATE``
            the share and the first day it traded ex,
        ``KIND``
            ``bonus``, ``cash`` or ``rights``,
        ``FACTOR``
            what a close before the ex-date is multiplied by to be comparable
            with a close on or after it,
        ``RECORD``, ``DETAIL``, ``SOURCE``, ``EXDATE_FROM``
            the record date, the terms as read, the notice they were read from,
            and whether the ex-date came from a notice or from the prices.

        A bonus issue of ``b`` per cent has the factor ``1 / (1 + b/100)``,
        exactly: it changes the number of shares and nothing else. A cash
        dividend of ``d`` taka against a close of ``P`` before it has the
        factor ``1 - d / P``, which is what reinvesting it on the ex-date comes
        to. A rights issue of ``n`` new shares for ``m`` held at a price ``S``
        has the factor of the theoretical ex-rights price,
        ``(m P + n S) / ((m + n) P)``, and none where ``S`` is above ``P``,
        since no one takes up a right to pay more than the market asks.

        Events whose record date lies beyond the last day traded are pending
        and are not returned; they reach the table once the share trades ex.
    """
    columns = ["CODE", "EXDATE", "KIND", "FACTOR", "RECORD", "DETAIL",
               "SOURCE", "EXDATE_FROM"]
    notices = join_continuations(news)
    if notices.empty or prices.empty:
        return pd.DataFrame(columns=columns)

    traded_rows = prices[(prices["TRADES"] > 0) & (prices["CLOSE"] > 0)].copy()
    traded_rows["REFDT"] = pd.to_datetime(traded_rows["REFDT"]).dt.date
    traded_rows = traded_rows.sort_values("REFDT")
    closes = {code: group.set_index("REFDT")["CLOSE"]
              for code, group in traded_rows.groupby("CODE")}
    traded = {code: series.index.to_numpy() for code, series in closes.items()}

    resumptions = resumption_dates(notices)
    rows = []

    for declared in dividend_declarations(notices).itertuples():
        ex_date, found_from = _ex_date(declared.CODE, declared.RECORD,
                                       resumptions, traded)
        if ex_date is None:
            continue
        detail = (f"{declared.CASH_PCT:g}% cash, {declared.STOCK_PCT:g}% stock, "
                  f"record date {declared.RECORD.isoformat()}")
        base = {"CODE": declared.CODE, "EXDATE": ex_date,
                "RECORD": declared.RECORD, "DETAIL": detail,
                "SOURCE": declared.SOURCE, "EXDATE_FROM": found_from}
        if declared.STOCK_PCT > 0:
            rows.append({**base, "KIND": "bonus",
                         "FACTOR": 1.0 / (1.0 + declared.STOCK_PCT / 100.0)})
        if declared.CASH_PCT > 0:
            before = _close_before(closes, declared.CODE, ex_date)
            amount = declared.CASH_PCT / 100.0 * DSE_FACE_VALUE
            if before and amount < before:
                rows.append({**base, "KIND": "cash",
                             "FACTOR": 1.0 - amount / before})

    terms = rights_terms(notices)
    for recorded in record_dates(notices).itertuples():
        if not recorded.RIGHTS or terms.empty:
            continue
        offered = terms[(terms["CODE"] == recorded.CODE)
                        & (terms["FILEDAT"] <= recorded.RECORD)
                        & (terms["FILEDAT"] > recorded.RECORD
                           - timedelta(days=RIGHTS_TERMS_WINDOW_DAYS))]
        if offered.empty:
            continue
        offer = offered.sort_values(["FILEDAT", "SOURCE"]).iloc[-1]
        ex_date, found_from = _ex_date(recorded.CODE, recorded.RECORD,
                                       resumptions, traded)
        if ex_date is None:
            continue
        before = _close_before(closes, recorded.CODE, ex_date)
        if not before:
            continue
        held, new, price = offer["HELD"], offer["NEW"], offer["PRICE"]
        factor = (held * before + new * price) / ((held + new) * before)
        if factor >= 1.0:
            continue
        rows.append({"CODE": recorded.CODE, "EXDATE": ex_date, "KIND": "rights",
                     "FACTOR": factor, "RECORD": recorded.RECORD,
                     "DETAIL": f"{new}R:{held} at {price:g} taka, record date "
                               f"{recorded.RECORD.isoformat()}",
                     "SOURCE": offer["SOURCE"], "EXDATE_FROM": found_from})

    events = pd.DataFrame(rows, columns=columns)
    return events.drop_duplicates(["CODE", "EXDATE", "KIND"], keep="last") \
                 .sort_values(["CODE", "EXDATE", "KIND"]) \
                 .reset_index(drop=True)
