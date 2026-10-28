"""Entity master data: the downloads into the landing zone.

Every source can be fetched over the network. GLEIF publishes its golden copy
files behind a link that changes with every publication, which its Golden
Copy API names, or which the caller hands over; ESMA's lists of CCPs are pdf
files at fixed addresses, which are read for the CCPs they name; the ECB's
lists of MFIs, investment funds, FVCs and PSRIs are linked from its pages
under names that change with each list, which are read off the pages; and
EIOPA's registers of insurance undertakings and of IORPs are exported by a
form on their pages, which :func:`download_eiopa_register` sends as the
page's "CSV" link would. A register can still be placed into the landing
zone by hand, as the module README describes, where a download does not
work. Nothing here removes a file from the landing zone: that is
the business of the Bronze layer, which does it once the file has been read.

A download either leaves the file it was asked for or leaves nothing at all.
The landing zone is not a scratch directory: the Bronze import reads the
reference date off the name of a file, so a file under the name of a register
is a claim that the register is there, and an answer that is not the register
must not be allowed to make that claim. What the server says is checked before
it is written, the bytes go to a name that marks them as incomplete, and the
name the Bronze import looks for is given to them only once they are all there
and, for an archive, once the archive opens.
"""

import datetime
import html
import io
import logging
import os
import re
import tempfile
import time
import zipfile
from urllib.parse import urljoin

import requests

from . import common
from ..log import context
from ..marketpricedata.common import USER_AGENT

logger = logging.getLogger(__name__)

#: How long a request may wait, in seconds: to connect, and between two chunks
#: of an answer. Without a limit a stalled connection holds the run
#: indefinitely. The second figure is generous because the GLEIF golden copies
#: are large, and it is the wait between chunks rather than the time the whole
#: download may take.
TIMEOUT = (30, 300)

#: ESMA's lists of CCPs, which it publishes under Article 88(1) of EMIR as pdf
#: files at fixed addresses: the CCPs authorised in the Union, and the
#: third-country CCPs recognised to offer their services in it, with the Tier
#: ESMA put each of them in.
ESMA_EU_CCP_URL = ("https://www.esma.europa.eu/sites/default/files/library/"
                   "ccps_authorised_under_emir.pdf")
ESMA_TC_CCP_URL = ("https://www.esma.europa.eu/sites/default/files/library/"
                   "third-country_ccps_recognised_under_emir.pdf")

#: The fewest CCPs a list may name before it is taken for a list that was not
#: read: ESMA's lists named 15 authorised and 41 recognised CCPs in October
#: 2026, and a layout the reader no longer understands yields few or none.
ESMA_CCP_MINIMUM = {"EU": 5, "TC": 10}

#: The suffix a file carries while it is being downloaded. The name the file is
#: finally stored under is only given to it once it is complete.
PARTIAL_SUFFIX = ".part"


def download_file(url, target_path, chunk_size = 1024 * 1024):
    """Download a file into the landing zone.

    The answer is written to ``target_path`` with :data:`PARTIAL_SUFFIX`
    appended and renamed only once the download is complete, so a download that
    is interrupted, or that answers with something other than the file, leaves
    nothing behind under the name the Bronze import reads.

    Three things are refused rather than written. An error answer from the
    server is raised, which is the case that matters most here: GLEIF changes
    the address of the golden copy with every release, so a link that has gone
    stale is the failure to expect, and its answer is an error page that would
    otherwise be stored under the name of the register. An archive that does not
    open is refused as well, for the same answer served with a success status.
    And a request that stalls is given up on rather than waited on forever, see
    :data:`TIMEOUT`.

    Parameters
    ----------
    url: str
        Address of the file.
    target_path: str
        Path the file should be written to. A path ending in ``.zip`` is taken
        to be an archive and the answer has to open as one.
    chunk_size: int
        Number of bytes read from the response at a time.
    """
    partial = target_path + PARTIAL_SUFFIX
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT) as response:
            response.raise_for_status()
            with open(partial, 'wb') as fd:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
        if target_path.lower().endswith(".zip") and not zipfile.is_zipfile(partial):
            raise zipfile.BadZipFile(
                f"{url} did not answer with a readable archive. Where the link "
                "was taken from the GLEIF download page some time ago, it has "
                "most likely been superseded; take the current one."
            )
        os.replace(partial, target_path)
    finally:
        # Nothing is left under the partial name either, so that a second
        # attempt starts from an empty landing zone rather than from the
        # remains of the first. The file is gone by now where the download
        # succeeded, and was never created where the folder does not exist.
        if os.path.exists(partial):
            os.remove(partial)


def download_gleif_level_1(config_path, url):
    """Download GLEIF Level 1 data into the landing zone.

    The link of a golden copy changes with every publication; the latest is
    found by :func:`download_latest_gleif`, and an earlier one can be taken
    from the download page and passed in. The file is stored under the name
    GLEIF gave it, which begins with the reference date; the Bronze import
    reads that date off the name.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    url: str
        URL of the csv link on the GLEIF website.
    """
    file_name = gleif_file_name(url)
    logger.info(
        "Downloading register into the landing zone",
        extra=context(module="entitymasterdata", layer="landing",
                      source="GLEIF_L1", file=file_name),
    )
    download_file(
        url, os.path.join(common.path_landing_gleif_level_1(config_path), file_name)
    )


def download_gleif_level_2(config_path, url):
    """Download GLEIF Level 2 data into the landing zone.

    The second GLEIF data set covers the ownership relationships between legal
    entities. As with the Level 1 data, the address of the RR-CDF csv file is
    found by :func:`download_latest_gleif` or passed in.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    url: str
        URL of the csv link on the GLEIF website.
    """
    file_name = gleif_file_name(url)
    logger.info(
        "Downloading register into the landing zone",
        extra=context(module="entitymasterdata", layer="landing",
                      source="GLEIF_L2", file=file_name),
    )
    download_file(
        url, os.path.join(common.path_landing_gleif_level_2(config_path), file_name)
    )


def gleif_file_name(url):
    """Return the name a GLEIF golden copy file is stored under.

    The published files are named
    ``20260724-0000-gleif-goldencopy-lei2-golden-copy.csv.zip``, and the Bronze
    import reads the reference date off the first eight characters of that
    name. The name is taken from the address rather than from a fixed offset
    into it, so that a link which carries a query string, a fragment or a
    different number of path segments still yields the file name GLEIF gave it.

    Parameters
    ----------
    url: str
        URL of the csv link on the GLEIF website.
    """
    name = url.split("?")[0].split("#")[0].rstrip("/").split("/")[-1]
    if not name.lower().endswith(".zip"):
        raise ValueError(
            f"The GLEIF link {url!r} does not end in the name of a zip file. "
            "Use the link of the csv file, which GLEIF serves as a zip archive."
        )
    return name


#: GLEIF's Golden Copy API: the files of the latest publication of the golden
#: copy, with a link to each data set in each format. GLEIF publishes three
#: times a day, and the link of a file changes with every publication, which
#: is why it used to be copied from the download page by hand.
GLEIF_LATEST_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"

#: The data sets of the golden copy the module reads, by the name the API
#: gives them: Level 1, who an entity is, and Level 2, its relationships.
GLEIF_DATA_SETS = {"lei2": "GLEIF_L1", "rr": "GLEIF_L2"}


def latest_gleif_urls():
    """The links of the latest golden copy, by data set.

    Returns
    -------
    dict
        The link of the zipped csv file of the full golden copy of Level 1,
        ``"lei2"``, and of Level 2, ``"rr"``, as GLEIF's Golden Copy API names
        them; see :data:`GLEIF_LATEST_URL`.

    Raises
    ------
    ValueError
        If the answer does not name both files, which is what an API that
        has changed its shape answers.
    """
    with requests.get(GLEIF_LATEST_URL, headers={"User-Agent": USER_AGENT},
                      timeout=TIMEOUT) as answer:
        answer.raise_for_status()
        latest = answer.json()
    urls = {}
    for data_set in GLEIF_DATA_SETS:
        try:
            urls[data_set] = latest["data"][data_set]["full_file"]["csv"]["url"]
        except (KeyError, TypeError) as err:
            raise ValueError(
                f"GLEIF's Golden Copy API at {GLEIF_LATEST_URL} names no csv "
                f"file of {data_set!r}. GLEIF may have changed the API; the link "
                "can still be given to the download, as the module README "
                "describes."
            ) from err
    return urls


def download_latest_gleif(config_path, data_sets=tuple(GLEIF_DATA_SETS)):
    """Download the latest golden copy the Bronze layer lacks into the landing zone.

    The links are asked of GLEIF's Golden Copy API, see
    :func:`latest_gleif_urls`, and a file is downloaded only where the Bronze
    layer holds no golden copy of its day: Level 1 is about half a gigabyte
    zipped, and GLEIF publishes three times a day, so a run on the same day
    as the last would otherwise fetch it again for a difference of hours.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    data_sets: tuple of str
        The data sets to download, ``"lei2"`` for Level 1 and ``"rr"`` for
        Level 2; both where left out.

    Returns
    -------
    list of str
        The names of the files put into the landing zone.
    """
    if not data_sets:
        return []
    urls = latest_gleif_urls()
    downloads = {
        "lei2": (download_gleif_level_1, common.path_bronze_gleif_level_1(config_path)),
        "rr": (download_gleif_level_2, common.path_bronze_gleif_level_2(config_path)),
    }
    stored = []
    for data_set in data_sets:
        download, bronze = downloads[data_set]
        name = gleif_file_name(urls[data_set])
        # The Bronze import files a golden copy under its day, the first eight
        # characters of the name.
        if os.path.exists(os.path.join(bronze, name[:8] + ".parquet")):
            continue
        download(config_path, urls[data_set])
        stored.append(name)
    return stored


def lei_is_valid(lei):
    """Whether a text is an LEI: its form and its check digits (ISO 17442)."""
    return (re.fullmatch(r"[0-9A-Z]{18}[0-9]{2}", lei) is not None
            and int("".join(str(int(c, 36)) for c in lei)) % 97 == 1)


def _matrix(a, b):
    """The product of two PDF transformation matrices."""
    return [a[0] * b[0] + a[1] * b[2], a[0] * b[1] + a[1] * b[3],
            a[2] * b[0] + a[3] * b[2], a[2] * b[1] + a[3] * b[3],
            a[4] * b[0] + a[5] * b[2] + b[4], a[4] * b[1] + a[5] * b[3] + b[5]]


def _page_items(page):
    """The pieces of text of a page with their position, and its thin bars.

    A piece of text comes as ``(text, x, y, size)`` in the coordinates of the
    page, ``y`` its baseline. A bar is a rectangle at most 1.2 points high,
    ``(x, y, width, height)``: the strike-through of a cell, and the rules
    of the table.
    """
    pieces, bars = [], []

    def text(value, cm, tm, font, size):
        if value and value.strip():
            m = _matrix(tm, cm)
            pieces.append((value.strip(), m[4], m[5], size * abs(m[3] or 1)))

    def operator(op, args, cm, tm):
        if op != b"re":
            return
        x, y, w, h = (float(a) for a in args)
        x0, y0 = x * cm[0] + y * cm[2] + cm[4], x * cm[1] + y * cm[3] + cm[5]
        w, h = w * cm[0], h * cm[3]
        if w < 0:
            x0, w = x0 + w, -w
        if h < 0:
            y0, h = y0 + h, -h
        if h <= 1.2 and w > 3:
            bars.append((x0, y0, w, h))

    page.extract_text(visitor_text=text, visitor_operand_before=operator)
    return pieces, bars


def _struck(x, y, size, bars):
    """Whether a bar runs through text at ``(x, y)`` rather than under it."""
    return any(bx <= x + 2 and bx + bw >= x + 6
               and y + 0.05 * size <= by <= y + 0.6 * size
               for bx, by, bw, bh in bars)


def ccps_from_pdf(content):
    """The CCPs of Table 1 of one of ESMA's lists, by LEI.

    The reader looks at the table the way a person does rather than at its
    words alone. A row ESMA has struck through is a CCP whose authorisation or
    recognition was withdrawn -- the British CCPs since Brexit, the Indian
    ones since 2023 -- which ESMA leaves in the table and crosses out. The
    strike-through is not a property of the text but a thin bar drawn through
    it, so it is found by comparing the bars of the page with the position of
    each LEI. A CCP withdrawn and recognised again appears twice, crossed out
    and not; it is current where one of its rows is not crossed out.

    The LEI column is narrow, and an LEI is often wrapped over two lines.
    The pieces are joined where they stand one below the other and only
    where the result has the check digits of an LEI, so that a wrong join
    yields no LEI rather than a false one. Only Table 1 is read: the tables
    after it list the same CCPs again for other purposes.

    Parameters
    ----------
    content: bytes
        The pdf file.

    Returns
    -------
    dict
        For each LEI of the table, ``current`` -- whether a row of it is not
        struck through -- and ``tier``, the Tier of a third-country CCP as
        its current row states it, ``"1"`` or ``"2"``, None where none is
        stated.
    """
    import pypdf

    ccps = {}
    state = "before"
    for page in pypdf.PdfReader(io.BytesIO(content)).pages:
        if state == "after":
            break
        pieces, bars = _page_items(page)
        # The lines of the page, top to bottom, which is where the headings
        # of the tables are told apart: a heading comes in several pieces.
        lines = {}
        for piece in pieces:
            lines.setdefault(round(piece[2]), []).append(piece)
        tokens = []
        for y in sorted(lines, reverse=True):
            line = sorted(lines[y], key=lambda p: p[1])
            heading = " ".join(p[0] for p in line)
            if state == "before" and re.match(r"Table 1\s*:", heading):
                state = "inside"
                continue
            if state == "inside" and re.match(r"Table (1 bis|2)\b", heading):
                state = "after"
                break
            if state == "inside":
                tokens += [p for p in line if re.fullmatch(r"[0-9A-Z]{2,20}", p[0])]
        for value, x, y, size in tokens:
            lei = value if len(value) == 20 and lei_is_valid(value) else None
            if lei is None and len(value) < 20:
                for rest, x2, y2, size2 in tokens:
                    if (y - 2.2 * size <= y2 <= y - 0.5 * size and abs(x2 - x) < 70
                            and len(value) + len(rest) == 20
                            and lei_is_valid(value + rest)):
                        lei = value + rest
                        break
            if lei is None:
                continue
            tier = None
            for piece in pieces:
                match = re.fullmatch(r"Tier\s*([12])", piece[0])
                if match and abs(piece[2] - y) < 1.5 * size:
                    tier = match.group(1)
            current = not _struck(x, y, size, bars)
            known = ccps.setdefault(lei, {"current": False, "tier": None})
            if current:
                known["current"] = True
                known["tier"] = tier
    return ccps


def esma_ccp_list(eu, tc):
    """The list of current CCPs from ESMA's two lists, as the Bronze import reads it.

    Parameters
    ----------
    eu, tc: bytes
        The pdf files of the CCPs authorised in the Union and of the
        recognised third-country CCPs.

    Returns
    -------
    pandas.DataFrame
        ``LEI``, ``SECTOR`` -- always ``CCP`` -- and ``SUBSECTOR``: ``EUCCP``
        for a CCP authorised in the Union, ``T1CCP`` or ``T2CCP`` for a
        recognised third-country CCP of Tier 1 or 2. A CCP whose rows are all
        struck through is left out.

    Raises
    ------
    ValueError
        If either list names fewer current CCPs than
        :data:`ESMA_CCP_MINIMUM`, which is a layout the reader does not
        understand rather than a market without CCPs, or a third-country CCP
        without a Tier.
    """
    import pandas

    rows = []
    for kind, content in (("EU", eu), ("TC", tc)):
        ccps = {lei: ccp for lei, ccp in ccps_from_pdf(content).items()
                if ccp["current"]}
        if len(ccps) < ESMA_CCP_MINIMUM[kind]:
            raise ValueError(
                f"ESMA's list of {kind} CCPs was read as naming {len(ccps)} current "
                "CCPs. ESMA may have changed the layout of the list; the list in "
                "the Bronze layer is kept."
            )
        for lei, ccp in sorted(ccps.items()):
            if kind == "EU":
                subsector = "EUCCP"
            elif ccp["tier"] in ("1", "2"):
                subsector = f"T{ccp['tier']}CCP"
            else:
                raise ValueError(
                    f"ESMA's list of third-country CCPs states no Tier for the "
                    f"current CCP {lei}. ESMA may have changed the layout of the "
                    "list; the list in the Bronze layer is kept."
                )
            rows.append((lei, "CCP", subsector))
    return pandas.DataFrame(rows, columns=["LEI", "SECTOR", "SUBSECTOR"]) \
        .drop_duplicates("LEI", keep="last")


def download_esma_ccp(config_path):
    """Download ESMA's lists of CCPs into the landing zone, as one list.

    ESMA publishes the CCPs authorised in the Union and the recognised
    third-country CCPs as pdf files; the two are read, see
    :func:`ccps_from_pdf`, and stored as ``ccp_list.csv`` with the columns
    ``LEI``, ``SECTOR`` and ``SUBSECTOR``, which the Bronze import reads.
    Nothing is stored where either list cannot be read; see
    :func:`esma_ccp_list`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        The number of current CCPs stored.
    """
    logger.info(
        "Downloading register into the landing zone",
        extra=context(module="entitymasterdata", layer="landing",
                      source="ESMA_CCP", file="ccp_list.csv"),
    )
    lists = []
    for url in (ESMA_EU_CCP_URL, ESMA_TC_CCP_URL):
        with requests.get(url, headers={"User-Agent": USER_AGENT},
                          timeout=TIMEOUT) as answer:
            answer.raise_for_status()
            if not answer.content.startswith(b"%PDF"):
                raise ValueError(f"{url} did not answer with a pdf file.")
            lists.append(answer.content)
    ccps = esma_ccp_list(*lists)
    _store(ccps.to_csv(index=False).encode("utf-8"),
           os.path.join(common.path_landing_esma_ccp(config_path), "ccp_list.csv"))
    return len(ccps)


# EIOPA
# --------------------------------------------------------------------------

#: The page of EIOPA's register of insurance undertakings.
EIOPA_INSURANCE_UNDERTAKING_URL = (
    "https://register.eiopa.europa.eu/registers/register-of-insurance-undertakings"
)

#: The page of EIOPA's register of institutions for occupational retirement
#: provision.
EIOPA_IORP_URL = (
    "https://register.eiopa.europa.eu/registers/"
    "register-of-institutions-for-occupational-retirement-provision"
)

#: How often an export EIOPA turns away is asked for, and how long is waited
#: in between, in seconds. EIOPA's gateway now and then answers an export
#: with ``403 Forbidden`` that it serves a moment later, as it did in the first
#: run of the download; each attempt starts again from the page, for a fresh
#: form.
EIOPA_ATTEMPTS = 3
EIOPA_WAIT = 30
EIOPA_RETRY_STATUS = {403, 429, 500, 502, 503, 504}

#: The name of the control behind a page's "CSV" link, after its last ``$``.
#: The CSV export is the one downloaded rather than the "Excel" one,
#: ``lkbtnExcelExport``: the workbook stores an LEI made of digits alone as a
#: number, which a twenty-digit LEI does not survive -- 276 LEIs of the
#: register of insurance undertakings came out as ``31570010000000000000``
#: and the like, several undertakings under one -- while the CSV export
#: writes every value as the register holds it.
CSV_EXPORT = "lkbtnExport"

_FORM = re.compile(r'<form\b[^>]*\baction="([^"]*)"[^>]*\bid="aspnetForm"', re.I)
_HIDDEN = re.compile(r"<input\b[^>]*>", re.I)
_ATTRIBUTE = re.compile(r'\b(type|name|value)="([^"]*)"', re.I)


def export_form(page, url, link=CSV_EXPORT):
    """The request a register page's export link makes, read off the page.

    EIOPA's registers are SharePoint pages, and their export is not a link to
    a file but a postback of the page's form: the browser sends the form back
    with its hidden fields -- the view state, the event validation -- and the
    name of the link as ``__EVENTTARGET``, and the server answers with the
    file. Sending the same form does what clicking the link does.

    Parameters
    ----------
    page: str
        The page, as the server sent it.
    url: str
        Its address, which the form's address is relative to.
    link: str
        The name of the link's control after its last ``$``, see
        :data:`CSV_EXPORT`.

    Returns
    -------
    tuple
        The address the form is sent to and its fields.

    Raises
    ------
    ValueError
        If the page has no form or not the link: a page EIOPA has changed, or
        one that is not the register.
    """
    form = _FORM.search(page)
    target = re.search(r"PostBackOptions\(&quot;([^&\"]*\$" + re.escape(link)
                       + r")&quot;", page)
    if form is None or target is None:
        raise ValueError(
            f"The page {url} has no form with the export {link!r}. EIOPA may "
            "have changed the page; the register can still be stored in the "
            "landing zone by hand, as the module README describes."
        )
    fields = {}
    for tag in _HIDDEN.findall(page):
        attributes = {k.lower(): html.unescape(v) for k, v in _ATTRIBUTE.findall(tag)}
        if attributes.get("type", "").lower() == "hidden" and "name" in attributes:
            fields[attributes["name"]] = attributes.get("value", "")
    fields["__EVENTTARGET"] = html.unescape(target.group(1))
    fields["__EVENTARGUMENT"] = ""
    return urljoin(url, html.unescape(form.group(1))), fields


def download_eiopa_register(url, folder, source, today=None):
    """Download one of EIOPA's registers into the landing zone.

    The page is read for its form, and the form sent back as the "CSV" link
    would send it; see :func:`export_form` and :data:`CSV_EXPORT`. The file is
    stored as ``YYYYMMDD.csv`` under the day of the download, which the Bronze
    import reads as the reference date, as for a workbook stored by hand. A
    second download on the same day replaces the first.

    The answer is checked before it is stored, as in :func:`download_file`:
    an error status is raised, and an answer that is not the register -- the
    page again, which is what a form the server no longer accepts gets -- is
    refused, so that nothing that is not the register is stored under its
    name.

    Parameters
    ----------
    url: str
        The page of the register.
    folder: str
        The folder of the register in the landing zone.
    source: str
        The name of the register in log records.
    today: datetime.date, optional
        The day the register is stored under. The present where left out.
    """
    today = today or datetime.date.today()
    target = os.path.join(folder, f"{today:%Y%m%d}.csv")
    logger.info(
        "Downloading register into the landing zone",
        extra=context(module="entitymasterdata", layer="landing",
                      source=source, file=os.path.basename(target)),
    )
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(EIOPA_ATTEMPTS):
        with requests.get(url, headers=headers, timeout=TIMEOUT) as page:
            page.raise_for_status()
            action, fields = export_form(page.text, url)
            # The cookies of the page keep the export on the server that
            # issued the form's view state.
            cookies = page.cookies
        with requests.post(action, data=fields, headers=headers, cookies=cookies,
                           timeout=TIMEOUT) as answer:
            if (answer.status_code in EIOPA_RETRY_STATUS
                    and attempt + 1 < EIOPA_ATTEMPTS):
                logger.warning(
                    "EIOPA turned the export away, it is asked for again",
                    extra=context(module="entitymasterdata", layer="landing",
                                  source=source, status=answer.status_code,
                                  wait=EIOPA_WAIT),
                )
                time.sleep(EIOPA_WAIT)
                continue
            answer.raise_for_status()
            content = answer.content
        break
    # The register is text in UTF-8, values in quotes and separated by
    # semicolons, and every register of EIOPA has a column "LEI".
    header = content[:4000].decode("utf-8", "replace").splitlines()[:1]
    if not header or '"LEI"' not in header[0].split(";"):
        raise ValueError(
            f"{url} did not answer the export with the register. EIOPA may have "
            "changed the page; the register can still be stored in the landing "
            "zone by hand, as the module README describes."
        )
    _store(content, target)


def download_eiopa_insurance_undertaking(config_path, today=None):
    """Download EIOPA's register of insurance undertakings into the landing zone.

    See :func:`download_eiopa_register`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    today: datetime.date, optional
        The day the register is stored under. The present where left out.
    """
    download_eiopa_register(
        EIOPA_INSURANCE_UNDERTAKING_URL,
        common.path_landing_eiopa_insurance_undertaking(config_path),
        "EIOPA_IC", today,
    )


def download_eiopa_iorp(config_path, today=None):
    """Download EIOPA's register of IORPs into the landing zone.

    See :func:`download_eiopa_register`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    today: datetime.date, optional
        The day the register is stored under. The present where left out.
    """
    download_eiopa_register(
        EIOPA_IORP_URL, common.path_landing_eiopa_iorp(config_path),
        "EIOPA_IORP", today,
    )


# ECB
# --------------------------------------------------------------------------

#: The ECB's page of the lists of financial institutions, which links the
#: yearly archives of the lists of investment funds.
ECB_LISTS_URL = ("https://www.ecb.europa.eu/stats/financial_corporations/"
                 "list_of_financial_institutions/html/index.en.html")

#: The ECB's page of the daily list of MFIs, which links the list of the day.
ECB_MFI_URL = ("https://www.ecb.europa.eu/stats/financial_corporations/"
               "list_of_financial_institutions/html/daily_list-MID.en.html")

#: The archive of the lists of investment funds of one year. The page also
#: links archives of several years, ``2019-2024``, which hold no list newer
#: than the archive of a single year.
_IF_ARCHIVE = re.compile(r'href="([^"?]*/ecb\.ifd_overview_(\d{4})\.en\.zip)[^"]*"')

#: A list of investment funds inside the archive, ``.../IF_2026_08.xlsx``.
_IF_WORKBOOK = re.compile(r"(?:^|/)(IF_\d{4}_(?:\d{2}|Q[1-4])\.xlsx)$")

#: The archive of the lists of FVCs of one year, as for the investment funds.
_FVC_ARCHIVE = re.compile(r'href="([^"?]*/ecb\.fvcd_overview_(\d{4})\.en\.zip)[^"]*"',
                          re.I)

#: A list of FVCs inside the archive, ``.../FVC_2026_Q3.xlsx``.
_FVC_WORKBOOK = re.compile(r"(?:^|/)(FVC_\d{4}_Q[1-4]\.xlsx)$")

#: The one archive of every list of PSRIs. The year is not in its name, so
#: the pattern gives a year of nought, which makes it the only candidate.
_PSRI_ARCHIVE = re.compile(r'href="([^"?]*/ecb\.psrid_overview\.en\.zip)()[^"]*"')

#: A list of PSRIs inside the archive, ``.../PSRIs_2021.xlsx`` or
#: ``.../PSRIs_2026_H1.xlsx``.
_PSRI_WORKBOOK = re.compile(r"(?:^|/)(PSRIs_\d{4}(?:_H[12])?\.xlsx)$")


#: The list of MFIs of the day, ``mfi_csv_261027.csv``; the page also links
#: the changes of the day, ``mfi_csv_update_...``, and compressed copies.
_MFI_LIST = re.compile(r'href="([^"?]*/mfi_csv_(\d{6})\.csv)"')


def _page(url):
    """A page of the ECB, as text."""
    with requests.get(url, headers={"User-Agent": USER_AGENT},
                      timeout=TIMEOUT) as page:
        page.raise_for_status()
        return page.text


def download_ecb_lists(archive, workbook, period_end, landing, bronze, source):
    """Download the ECB's lists of one sector the Bronze layer lacks into the landing zone.

    The ECB publishes the lists of investment funds, FVCs and PSRIs as
    workbooks gathered in zip archives, which its page of the lists of
    financial institutions links and which are replaced as new lists are
    published. The latest archive is downloaded -- of a single year, where the
    page links several -- and each list in it whose period the Bronze layer
    does not hold yet is put into the landing zone under the name the ECB
    gives it, which the Bronze import reads the period off. A list the Bronze
    layer holds is not unpacked again, so a run after the first puts only the
    lists published since into the landing zone.

    Parameters
    ----------
    archive: re.Pattern
        Matches a link of the page to an archive, its groups the address and
        the year.
    workbook: re.Pattern
        Matches a list inside the archive, its group the name of the file.
    period_end: callable
        Takes the name of a list and returns the last day of its period,
        which names its file in the Bronze layer.
    landing, bronze: str
        The folders of the sector in the landing zone and the Bronze layer.
    source: str
        The name of the register in log records.

    Returns
    -------
    list of str
        The names of the lists put into the landing zone.

    Raises
    ------
    ValueError
        If the page links no archive of the sector.
    """
    page = _page(ECB_LISTS_URL)
    archives = {int(year or 0): href for href, year in archive.findall(page)}
    if not archives:
        raise ValueError(
            f"The page {ECB_LISTS_URL} links no archive of the lists of {source}. "
            "The ECB may have changed the page; the lists can still be stored in "
            "the landing zone by hand, as the module README describes."
        )
    url = urljoin(ECB_LISTS_URL, archives[max(archives)])
    logger.info(
        "Downloading register into the landing zone",
        extra=context(module="entitymasterdata", layer="landing", source=source,
                      file=os.path.basename(url)),
    )
    stored = []
    with tempfile.TemporaryDirectory() as workspace:
        archive_path = os.path.join(workspace, "lists.zip")
        download_file(url, archive_path)
        with zipfile.ZipFile(archive_path) as lists:
            for member in sorted(lists.namelist()):
                match = workbook.search(member)
                if match is None:
                    continue
                name = match.group(1)
                if os.path.exists(os.path.join(bronze, period_end(name) + ".parquet")):
                    continue
                _store(lists.read(member), os.path.join(landing, name))
                stored.append(name)
    return stored


def download_ecb_investment_fund(config_path):
    """Download the lists of investment funds the Bronze layer lacks into the landing zone.

    The lists are monthly or quarterly, ``IF_2026_08.xlsx``, in one archive
    per year; see :func:`download_ecb_lists`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    list of str
        The names of the lists put into the landing zone.
    """
    from .bronze import end_of_investment_fund_period

    return download_ecb_lists(
        _IF_ARCHIVE, _IF_WORKBOOK, end_of_investment_fund_period,
        common.path_landing_ecb_investment_fund(config_path),
        common.path_bronze_ecb_investment_fund(config_path), "ECB_IF",
    )


def download_ecb_fvc(config_path):
    """Download the lists of FVCs the Bronze layer lacks into the landing zone.

    The lists are quarterly, ``FVC_2026_Q3.xlsx``, in one archive per year;
    see :func:`download_ecb_lists`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    list of str
        The names of the lists put into the landing zone.
    """
    from .bronze import end_of_period

    return download_ecb_lists(
        _FVC_ARCHIVE, _FVC_WORKBOOK, lambda name: end_of_period(name, "FVC"),
        common.path_landing_ecb_fvc(config_path),
        common.path_bronze_ecb_fvc(config_path), "ECB_FVC",
    )


def download_ecb_psri(config_path):
    """Download the lists of PSRIs the Bronze layer lacks into the landing zone.

    The lists are yearly until 2021, ``PSRIs_2021.xlsx``, and half-yearly
    since, ``PSRIs_2026_H1.xlsx``, all of them in one archive; see
    :func:`download_ecb_lists`. On the first run every list since 2014 is
    put into the landing zone.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    list of str
        The names of the lists put into the landing zone.
    """
    from .bronze import end_of_period

    return download_ecb_lists(
        _PSRI_ARCHIVE, _PSRI_WORKBOOK, lambda name: end_of_period(name, "PSRIs"),
        common.path_landing_ecb_psri(config_path),
        common.path_bronze_ecb_psri(config_path), "ECB_PSRI",
    )


def download_ecb_mfi(config_path):
    """Download the ECB's list of MFIs of the day into the landing zone.

    The page of the daily list links the list of the day as
    ``mfi_csv_<YYMMDD>.csv``, text separated by tabs and encoded in UTF-16,
    which the Bronze import reads as it is. It is stored as
    ``<YYYYMMDD>.csv`` under the day the ECB gives it, and not downloaded
    where the Bronze layer already holds the list of that day.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    str or None
        The name the list was stored under, or None where the Bronze layer
        holds it already.

    Raises
    ------
    ValueError
        If the page links no list, or the answer is not the list.
    """
    page = _page(ECB_MFI_URL)
    match = _MFI_LIST.search(page)
    if match is None:
        raise ValueError(
            f"The page {ECB_MFI_URL} links no list of MFIs. The ECB may have "
            "changed the page; the list can still be stored in the landing zone "
            "by hand, as the module README describes."
        )
    href, day = match.groups()
    name = f"20{day}.csv"
    if os.path.exists(os.path.join(common.path_bronze_ecb_mfi(config_path),
                                   f"20{day}.parquet")):
        return None
    url = urljoin(ECB_MFI_URL, href)
    logger.info(
        "Downloading register into the landing zone",
        extra=context(module="entitymasterdata", layer="landing", source="ECB_MFI",
                      file=name),
    )
    with requests.get(url, headers={"User-Agent": USER_AGENT},
                      timeout=TIMEOUT) as answer:
        answer.raise_for_status()
        content = answer.content
    try:
        header = content[:2000].decode("utf-16", "ignore")
    except UnicodeError:
        header = ""
    if not content.startswith(b"\xff\xfe") or "RIAD_CODE" not in header:
        raise ValueError(
            f"{url} did not answer with the list of MFIs. The ECB may have "
            "changed the file; the list can still be stored in the landing zone "
            "by hand, as the module README describes."
        )
    _store(content, os.path.join(common.path_landing_ecb_mfi(config_path), name))
    return name


def _store(content, target):
    """Write a file into the landing zone under a temporary name, then its own."""
    partial = target + PARTIAL_SUFFIX
    try:
        with open(partial, "wb") as handle:
            handle.write(content)
        os.replace(partial, target)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


#: What this module offers. Downloading the registers that can be fetched
#: over the network into the landing zone.
__all__ = [
    "ECB_LISTS_URL",
    "ECB_MFI_URL",
    "CSV_EXPORT",
    "EIOPA_INSURANCE_UNDERTAKING_URL",
    "EIOPA_IORP_URL",
    "ESMA_CCP_MINIMUM",
    "ESMA_EU_CCP_URL",
    "ESMA_TC_CCP_URL",
    "GLEIF_DATA_SETS",
    "GLEIF_LATEST_URL",
    "PARTIAL_SUFFIX",
    "TIMEOUT",
    "ccps_from_pdf",
    "download_ecb_fvc",
    "download_ecb_investment_fund",
    "download_ecb_lists",
    "download_ecb_mfi",
    "download_ecb_psri",
    "download_eiopa_insurance_undertaking",
    "download_eiopa_iorp",
    "download_eiopa_register",
    "download_esma_ccp",
    "download_file",
    "download_gleif_level_1",
    "download_gleif_level_2",
    "download_latest_gleif",
    "esma_ccp_list",
    "export_form",
    "gleif_file_name",
    "latest_gleif_urls",
    "lei_is_valid",
]
