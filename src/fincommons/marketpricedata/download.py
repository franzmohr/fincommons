"""Market price data: the download the publishers that send one file share.

The ECB, the New York Fed, the Bank of England and the LBMA each publish a
series as one file, which is downloaded whole on every run and written over the
Bronze layer. The download is the same for all four -- a temporary directory on
the driver, one streamed request, the response closed whatever happens -- and so
is the danger it carries: a series that is overwritten whole is lost whole when
what is written over it is not the series. It is therefore written once, here,
together with the check that stands between an answer and the history it would
replace.
"""

import os
import tempfile
from contextlib import contextmanager

import requests

from .common import HTTP_TIMEOUT, USER_AGENT


@contextmanager
def downloaded(url, source, params=None, file_name="download"):
    """Download one answer into a temporary file and yield its path.

    The file is fetched into a temporary directory on the local disk of the
    driver rather than into the data lake itself, as in
    :mod:`fincommons.exchangerate`: it is deleted as soon as the block that
    reads it has finished, and on Databricks the Bronze layer is a Unity
    Catalog volume that should not be used as scratch space. Whatever reads the
    file has to have written its result before the block ends, because Spark
    reads lazily and the file is gone afterwards.

    The request carries a user agent and a timeout. Several of the publishers
    serve their files through a content delivery network that refuses a request
    without a user agent, and a request without a timeout waits on a connection
    that has gone quiet for as long as the operating system lets it; see
    :data:`HTTP_TIMEOUT`. The response is closed whether or not the download
    succeeds.

    **An answer with an error status or without a single byte is refused.**
    Neither can be the series, and both would otherwise reach the reader and,
    through it, the Bronze layer. An answer that has bytes but is still not the
    series -- an error page sent with status 200 -- can only be recognised once
    it has been parsed, which is what :func:`refuse_empty_answer` is for.

    Parameters
    ----------
    url: str
        Address of the file.
    source: str
        Name of the publisher, used in the message of a refusal.
    params: dict, optional
        Query parameters of the request.
    file_name: str
        Name the answer is written under in the temporary directory.

    Yields
    ------
    str
        Path of the downloaded file.
    """
    with tempfile.TemporaryDirectory() as workspace:
        file_path = os.path.join(workspace, file_name)

        resp = requests.get(
            url,
            params=params,
            headers={"User-Agent": USER_AGENT},
            stream=True,
            timeout=HTTP_TIMEOUT,
        )
        try:
            resp.raise_for_status()
            with open(file_path, 'wb') as fd:
                for chunk in resp.iter_content(chunk_size=8192):
                    fd.write(chunk)
        finally:
            resp.close()

        if os.path.getsize(file_path) == 0:
            raise ValueError(
                f"The answer of {source} to {url} is empty. It is not written "
                "over the Bronze layer, which keeps the series it held."
            )

        yield file_path


def refuse_empty_answer(frame, source, url):
    """Raise unless a parsed answer holds at least one observation.

    The series of these publishers are written over the Bronze layer whole, so
    an answer that parses to nothing would replace years of history with
    nothing -- and the run would report success, since the Silver stage then
    finds nothing to take and the table simply stops moving. That is how an
    error page served with status 200 arrives: a CSV reader given an HTML page
    returns a table of one column and no rows, and a JSON reader a record whose
    fields are all null.

    The check is made on the frame exactly as it is going to be written, and it
    is made before the write rather than by it. A file-based write in overwrite
    mode removes what the path held before it runs, so a parse that fails
    during the write would take the history with it. Where reading the answer
    fails here -- a date that does not parse under ANSI mode, for instance --
    the failure is raised as a refusal naming the source, with the error of the
    reader chained to it.

    Parameters
    ----------
    frame: pyspark.sql.DataFrame
        The answer as it is about to be written, observations without a value
        already removed.
    source: str
        Name of the publisher, used in the message.
    url: str
        Address the answer was downloaded from, used in the message.
    """
    try:
        empty = frame.limit(1).count() == 0
    except Exception as err:
        raise ValueError(
            f"The answer of {source} to {url} could not be read as the series. "
            "It is not written over the Bronze layer, which keeps the series "
            "it held."
        ) from err

    if empty:
        raise ValueError(
            f"The answer of {source} to {url} holds no observation. It is not "
            "written over the Bronze layer, which keeps the series it held."
        )
