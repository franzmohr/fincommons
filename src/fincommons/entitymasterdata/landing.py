"""Entity master data: the downloads into the landing zone.

Every source can be fetched over the network. GLEIF publishes its golden copy
files behind a link that changes with every publication, which its Golden
Copy API names, or which the caller hands over; the list of CCPs is served from a fixed address and
needs no argument; the ECB's lists of MFIs, investment funds, FVCs and PSRIs
are linked from its pages under names that change with each list, which are
read off the pages; and EIOPA's registers of insurance undertakings and of IORPs are
exported by a form on their pages, which :func:`download_eiopa_register`
sends as the page's "Excel" link would. A register can still be placed into
the landing zone by hand, as the module README describes, where a download
does not work. Nothing here removes a file from the landing zone: that is
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

#: The list of CCPs, at a fixed commit of the repository it is kept in. A
#: commit rather than the branch is named, so that what the pipeline imports
#: is the version that was reviewed: a change to the list reaches the data
#: lake only when this address is moved to a newer commit, in a change of its
#: own.
ESMA_CCP_URL = (
    "https://raw.githubusercontent.com/franzmohr/ccp-list/"
    "8dc0cf66bceaccac6b0c07252987f9e5b875538f/data/ccp-list.csv"
)

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


def download_esma_ccp(config_path):
    """Download the list of CCPs into the landing zone.

    ESMA publishes the central counterparties authorised under EMIR, and the
    recognised third-country ones, as pdf files, which cannot reasonably be
    read. Until that changes the list is taken from a public GitHub repository,
    where the same information is kept in a machine-readable form.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    logger.info(
        "Downloading register into the landing zone",
        extra=context(
            module="entitymasterdata",
            layer="landing",
            source="ESMA_CCP",
            file="ccp_list.csv",
        ),
    )
    download_file(
        ESMA_CCP_URL,
        os.path.join(common.path_landing_esma_ccp(config_path), "ccp_list.csv"),
    )


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
    "ESMA_CCP_URL",
    "GLEIF_DATA_SETS",
    "GLEIF_LATEST_URL",
    "PARTIAL_SUFFIX",
    "TIMEOUT",
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
    "export_form",
    "gleif_file_name",
    "latest_gleif_urls",
]
