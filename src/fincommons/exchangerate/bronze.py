"""Exchange rates: the public sources in the Bronze layer.

The ECB publishes its reference rates, and the BIS its bulk exchange rate
statistics, as zipped CSV files. Both are downloaded, unpacked and rewritten
as parquet without any change to their content, so that the Bronze layer holds
what the source published and nothing else.
"""

import os
import tempfile
import zipfile

import pandas
import pyarrow
import pyarrow.parquet
import requests

from .common import *


#: How long a download waits on the server, in seconds: to connect, and then
#: between two chunks of the response. It is not a limit on the whole
#: download, which for the BIS file takes longer than either. Without one a
#: stalled connection holds the daily run indefinitely.
DOWNLOAD_TIMEOUT = (30, 120)


def unzip_and_get_names(folder_path, zip_file):
    """Unpack an archive into a folder and return the name of its first entry."""
    with zipfile.ZipFile(os.path.join(folder_path, zip_file), 'r') as zip_ref:
        zip_ref.extractall(folder_path)
        return zip_ref.namelist()[0]


#: The columns of the BIS bulk file the Silver layer reads. The file carries
#: every frequency and collection of the series and a dozen descriptive columns
#: besides, so it is read in these columns only.
BIS_COLUMNS = [
    "FREQ:Frequency",
    "COLLECTION:Collection",
    "CURRENCY:Currency",
    "TIME_PERIOD:Time period or range",
    "OBS_VALUE:Observation Value",
]


def download_zipped_csv(url, parquet_path, usecols=None):
    """Download a zipped CSV file and store it as parquet in the data lake.

    The archive is fetched and unpacked in a temporary directory on the local
    disk of the driver rather than in the data lake itself. On Databricks the
    Bronze layer is a Unity Catalog volume, and writing an archive of several
    megabytes there only to unpack it and delete it again is both slow and
    needlessly dependent on how the volume handles files that are never meant
    to be kept. Only the parquet file reaches the data lake.

    Parameters
    ----------
    url: str
        Address of the zipped CSV file.
    parquet_path: str
        Path of the parquet file that should be written.
    usecols: list of str, optional
        The columns to keep. All of them where it is left out.
    """
    with tempfile.TemporaryDirectory() as workspace:
        archive = os.path.join(workspace, "download.zip")

        resp = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
        try:
            # An error page is refused as what it is. Written to disk, it would
            # fail a moment later as a file that is not a zip archive, which
            # points at the archive rather than at the server.
            resp.raise_for_status()
            with open(archive, 'wb') as fd:
                for chunk in resp.iter_content(chunk_size=8192):
                    fd.write(chunk)
        finally:
            resp.close()

        file = unzip_and_get_names(workspace, archive)
        data = pandas.read_csv(os.path.join(workspace, file), na_values="NaN",
                               usecols=usecols)

    # The new file is written beside the old one and put in its place once it
    # is complete, so that a write that dies leaves the last good file rather
    # than a truncated one.
    partial = parquet_path + ".partial"
    try:
        pyarrow.parquet.write_table(
            pyarrow.Table.from_pandas(data, preserve_index=False), partial
        )
        os.replace(partial, parquet_path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


def update_bronze_bis_exchangerate(config_path):
    """Download BIS exchange rate data into bronze layer.

    Downloads public reference exchange rate data from the BIS's website
    into the bronze layer of the data lake.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_bis_exchangerate("path/to/configuration/file")
    """
    download_zipped_csv(
        'https://data.bis.org/static/bulk/WS_XRU_csv_flat.zip',
        os.path.join(path_bronze_bis_exchangerate(config_path), "bis_exchangerate.parquet"),
        usecols=BIS_COLUMNS,
    )


def update_bronze_ecb_exchangerate(config_path):
    """Download ECB exchange rate data into bronze layer.

    Downloads public reference exchange rate data from the ECBs website
    into the bronze layer of the data lake.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_bronze_ecb_exchangerate("path/to/configuration/file")
    """
    download_zipped_csv(
        'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip',
        os.path.join(path_bronze_ecb_exchangerate(config_path), "ecb_exchangerate.parquet"),
    )
