"""SEC filings: the tagged figures of the registrants of the Commission.

Registrants of the Securities and Exchange Commission tag the financial
statements of their periodic reports in Inline XBRL, and the Commission
publishes those figures twice over: as the filings themselves, and as one
JSON file per company holding the facts already parsed out of them. This
module reads the second, as :mod:`fincommons.esef` reads the xBRL-JSON that
filings.xbrl.org publishes beside the ESEF report packages. Neither parses
inline XBRL, and neither needs a parser installed.

``common``
    the layout of the data lake and the tables the result is read from,
``bronze``
    collecting the company file of each registrant asked for, or the archive
    of all of them,
``parse``
    reading a company file into filings and facts, in plain Python,
``silver``
    the table of filings and the table of facts, rebuilt from the company
    files collected,
``quality``
    the checks the figures are held to, and the table of issues they find,
``pipeline``
    the whole pipeline in one call, :func:`update_secfiling`.

A company file is organised by concept rather than by filing, and every fact
in it names the filing it came from. A filing is therefore reconstructed by
grouping on that accession number, and the reconstruction is exact: a figure
a later filing restates appears again under that filing rather than replacing
the first, so each filing says what it said.

What the company files hold is the undimensioned numeric facts, which is the
consolidated figures and not the whole of a filing: the breakdowns by segment
and by class are absent, and so are the narrative facts and the text blocks
of the notes. A quarterly report of 2026 stating 941 facts is carried as 321.
The tables say so by having no column for a dimension or for text, rather
than by having columns that are always empty.

The tables are keyed by the central index key of the registrant. Neither the
Commission nor GLEIF publishes a mapping from it to the legal entity
identifier :mod:`fincommons.esef` and :mod:`fincommons.entitymasterdata` are
keyed by. The closest thing is a by-product of the LEI records themselves,
which name the register an entity is registered in: some tens of thousands
name EDGAR, and a few of those carry a central index key. Nearly all of them
are funds, by their series identifier, and the record of an operating company
points at its state register instead -- NVIDIA's at Delaware, by a file
number that is not its central index key. There is nothing there for the
registrants these tables hold, and this module invents nothing.

The files are downloaded when the pipeline runs and are never shipped with
the package. They are publications of the Commission, a work of the United
States government.
"""

from .common import *
from .parse import *
from .bronze import *
from .silver import *
from .quality import *
from .pipeline import *
