"""ESEF filings: the annual financial reports of listed issuers, fact by fact.

Issuers whose securities are admitted to trading on a regulated market in the
European Union file their annual financial reports in the European Single
Electronic Format (ESEF), as XHTML documents whose figures are tagged in
Inline XBRL. filings.xbrl.org, run by XBRL International, collects them from
the national registers together with the reports of the United Kingdom and of
Ukraine filed in the same way, and publishes for each the report package as
filed and the tagged facts as xBRL-JSON. The module brings them into the data
lake in these stages:

``common``
    the layout of the data lake and the tables the result is read from,
``bronze``
    collecting the indexes of the repository, and the report of each filing
    of the countries asked for,
``parse``
    reading an index into filings and a report into facts, in plain Python,
``silver``
    the table of filings, and one table with every fact of every report
    collected, rebuilt country by country,
``pipeline``
    the whole pipeline in one call, :func:`update_esef`.

The facts are keyed by the LEI of the issuer, which is the key of
:mod:`fincommons.entitymasterdata` too.

The files are downloaded when the pipeline runs and are never shipped with
the package. The reports are the issuers' own publications, which they are
required to make public; the repository states no licence for its copies.
"""

from .common import *
from .parse import *
from .bronze import *
from .silver import *
from .pipeline import *
