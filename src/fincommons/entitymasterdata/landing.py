"""Entity master data: the downloads into the landing zone.

Two of the four sources can be fetched over the network. GLEIF publishes its
golden copy files behind a link that changes with every release, so the address
has to be handed over by the caller; the list of CCPs is served from a fixed
address and needs no argument.

The ECB registers and the EIOPA register are not downloaded. Both are served
from pages that hand out the workbook behind a form rather than from a stable
address, so the files are placed into the landing zone by hand; the module
README says where. Nothing here removes a file from the landing zone: that is
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

import logging
import os
import zipfile

import requests

from . import common
from ..log import context

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

    GLEIF does not publish the most recent golden copy behind a stable link, so
    the address of the LEI-CDF csv file has to be taken from the download page
    and passed in. The file is stored under the name GLEIF gave it, which
    begins with the reference date; the Bronze import reads that date off the
    name.

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
    entities. As with the Level 1 data, the address of the RR-CDF csv file has
    to be taken from the download page and passed in.

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


#: What this module offers. Downloading the two registers that are served from
#: an address into the landing zone.
__all__ = [
    "ESMA_CCP_URL",
    "PARTIAL_SUFFIX",
    "TIMEOUT",
    "download_esma_ccp",
    "download_file",
    "download_gleif_level_1",
    "download_gleif_level_2",
    "gleif_file_name",
]
