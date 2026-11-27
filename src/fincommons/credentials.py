"""Where a key comes from, for the sources that will not answer without one.

Most of what fincommons reads is published to anyone who asks. Some sources
are not: the EDINET API of Japan's Financial Services Agency, which
:mod:`fincommons.financialreport.edinet` reads, refuses a request that
carries no registered key, as does, for instance, the OpenDART API of Korea's
Financial Supervisory Service, which no module of this package reads yet.
This module is the one place that says where such a key comes from, so that
a module reading one of them -- here or in a package built on fincommons --
does not invent its own answer.

Three places are looked in, in this order:

1. the ``key`` argument, so that a notebook or a test can pass one directly;
2. the environment, under ``FINCOMMONS_<NAME>_KEY``, which is what a
   scheduler sets and what a secret scope becomes on Databricks;
3. a file named ``<name>`` in the directory ``FINCOMMONS_VAULT`` names, which
   is the shape a container secret already has -- Kubernetes and Docker both
   mount a secret as a file of its own -- and which keeps the key out of the
   environment of every process in the image.

What is deliberately not looked in is the configuration file. ``paths.yaml``
says where the tables of a lake live; it is committed with a deployment, it
is handed to every module, and it is read by R as well -- every Quarto
template of the package opens it -- so a key in it would travel with the
repository and be readable by anything that renders a factsheet.

A key is never logged. :func:`redacted` exists because one of the two APIs
takes its key as a query parameter rather than as a header, so the address of
a request carries the secret and the address is the thing a pipeline logs.
"""

from __future__ import annotations

import os
import re

__all__ = ["KEY_ENVIRONMENT", "VAULT_ENVIRONMENT", "api_key", "redacted",
           "vault_path"]

#: The environment variable a key is read from, by the name of the source.
KEY_ENVIRONMENT = "FINCOMMONS_{name}_KEY"

#: The environment variable naming the directory that holds one file per key.
VAULT_ENVIRONMENT = "FINCOMMONS_VAULT"

#: The query parameters whose value is replaced in a logged address. Both of
#: the APIs that need a key name it in one of these.
SECRET_PARAMETERS = ("Subscription-Key", "subscription-key", "crtfc_key",
                     "api_key", "apikey", "key", "token")


def vault_path(name, vault=None):
    """The file a key would be read from, or ``None`` where no vault is set.

    Parameters
    ----------
    name: str
        The source, e.g. ``'edinet'``. It becomes a file name, so a value
        that could be more than a name is refused rather than used.
    vault: str, optional
        The directory holding one file per key. By default whatever
        :data:`VAULT_ENVIRONMENT` names.
    """
    if not re.fullmatch(r"[a-z0-9_-]+", str(name)):
        raise ValueError(
            f"{name!r} is not a source name. It becomes the name of a file "
            "in the vault directory, so it is limited to lower-case letters, "
            "digits, the hyphen and the underscore."
        )
    folder = vault if vault is not None else os.environ.get(VAULT_ENVIRONMENT)
    return os.path.join(folder, name) if folder else None


def api_key(name, key=None, env=None, vault=None):
    """The key for one source, from the argument, the environment or a vault.

    Parameters
    ----------
    name: str
        The source, e.g. ``'edinet'`` or ``'dart'``.
    key: str, optional
        The key itself. Given, it is used and nothing else is looked at,
        which is how a test passes one and how a caller that keeps its
        secrets its own way bypasses this module entirely.
    env: str, optional
        The environment variable to read instead of the default, which is
        :data:`KEY_ENVIRONMENT` with the name upper-cased.
    vault: str, optional
        The directory to read instead of whatever :data:`VAULT_ENVIRONMENT`
        names.

    Returns
    -------
    str
        The key, stripped of surrounding whitespace. A file written by an
        editor ends in a newline, and a newline in an HTTP header is worse
        than a wrong key.

    Raises
    ------
    ValueError
        Where none of the three holds one. The error names all three places,
        because a run that has no key collects nothing at all and the person
        reading the message is the person who has to put one somewhere.

    Examples
    --------
    >>> api_key("edinet", key="ZZZ")
    'ZZZ'
    """
    if key is not None and str(key).strip():
        return str(key).strip()

    variable = env or KEY_ENVIRONMENT.format(name=str(name).upper())
    found = os.environ.get(variable)
    if found and found.strip():
        return found.strip()

    path = vault_path(name, vault)
    if path and os.path.isfile(path):
        with open(path, encoding="utf-8") as handle:
            found = handle.read().strip()
        if found:
            return found

    raise ValueError(
        f"No API key for {name}. The publisher refuses a request without "
        f"one, so a run cannot collect anything. Pass key=..., or set "
        f"{variable}, or put the key in a file named {name!r} in the "
        f"directory {VAULT_ENVIRONMENT} names. Do not put it in paths.yaml: "
        "that file is committed with the deployment and is read by the "
        "Quarto templates as well."
    )


def redacted(url):
    """An address with the value of any key in it replaced, fit for a log.

    One of the two APIs takes its key as a query parameter, so the address of
    a request carries the secret while the address is exactly what a pipeline
    writes to its log and to the record it keeps of what it collected.

    Examples
    --------
    >>> redacted("https://example.org/a?date=2026-01-01&Subscription-Key=ZZZ")
    'https://example.org/a?date=2026-01-01&Subscription-Key=***'
    """
    text = str(url)
    for parameter in SECRET_PARAMETERS:
        text = re.sub(rf"({re.escape(parameter)}=)[^&#\s]*", r"\1***", text)
    return text
