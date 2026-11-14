"""EDINET: the tagged figures of the filers of Japan's Financial Services Agency.

EDINET is the agency's filing system, the counterpart of EDGAR, and it has
required the financial statements of a securities report to be tagged in XBRL
since 2008. Its API publishes an index of what was filed on a given day, the
filing itself, and the agency's own conversion of the filing's XBRL into CSV.
This module reads the conversion, as
:mod:`fincommons.financialreport.sec` reads the company files of the
Commission and :mod:`fincommons.financialreport.esef` the xBRL-JSON of
filings.xbrl.org. None of the three parses XBRL.

``common``
    the layout of the data lake and the tables the result is read from,
``bronze``
    collecting the index of a day, a filing, and the list of filers.

The reader of the CSV conversion and the Silver layer built from it are not
here yet: the layout of a conversion is read from one before it is relied on,
and that needs a key the agency issues.

**A filing leaves EDINET.** A securities report is retrievable for ten years
and then it is not, which is unlike EDGAR, where a filing stays for ever, and
unlike filings.xbrl.org, which keeps its archive. The Bronze layer of this
module is therefore the only lasting copy of what it holds, and the oldest
year within reach is being lost while a lake decides whether it wants it.

The API refuses a request without a registered key, and reports the refusal
with HTTP status 200 and the real status inside the body, so a run that
trusted the status would store the refusal as data. The list of filers is the
exception: it is a zip on the disclosure site and needs no key.

The files are downloaded when the pipeline runs and are never shipped with
the package. They are publications of the Financial Services Agency.
"""

from .common import *
from .bronze import *
