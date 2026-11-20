"""STS: the public register of simple, transparent and standardised securitisations.

A securitisation that meets the criteria of Article 18 of the Securitisation
Regulation is notified to ESMA, which keeps the public register of those
notifications and exposes it through the machine-to-machine services of its
registers portal: a Solr index queried by URL, with no key and no registration
behind it. This module collects that register, and the four spreadsheets ESMA
publishes beside it.

``common``
    the layout of the data lake, the addresses of the sources, and what the
    register is expected to hold,
``bronze``
    collecting the register and the spreadsheets, as published, one dated
    snapshot per run.

**The register is current state, so the snapshots are the history.** A
cancelled notification leaves the register: of the 109 cancellations ESMA
published up to May 2022, 107 are no longer in it. The two spreadsheets that
recorded the removals were frozen in 2022 and nothing published has recorded
one since, so a lake's own dated snapshots are the only record of what the
register once held. Hence the shape of the Bronze layer -- a complete copy per
run, never rewritten -- and hence the one piece of advice about using it: do
not compact the snapshots away.

**Two populations, and the difference decides what a notification tells you.**
Of the 1,147 notifications held on 20 November 2026, 502 were public and
carried the whole STS template, their tranches and their parties; 645 were
private and carried sixteen to nineteen fields and no child documents at all.
A public notification's securitisation identifier is the one the disclosure
templates use -- the reporting entity's legal entity identifier, a letter for
the class and the period, ``2138001WO21Z3B8Y8K20N202602`` -- so it joins to
reported securitisation data as it stands. A private one carries a synthetic
``Private_2020_12_18_01``, which joins to nothing.

The tranches and the parties are child documents rather than fields: ``issins``
carries an ISIN, which joins to :mod:`fincommons.firds`, and ``originator``,
``lender`` and ``sponsor`` carry a legal entity identifier, which joins to
:mod:`fincommons.entitymasterdata`. The special purpose entity carries a
country and nothing else, which is a gap in the register.

Neither identifier in the register is unique: 1,147 notifications carried 1,115
distinct securitisation identifiers and 1,131 distinct notification
identifiers, a securitisation being notified again when it is restructured or
re-notified. The register's own document id is what identifies a row.

The files are downloaded when the pipeline runs and are never shipped with the
package. They are publications of the European Securities and Markets
Authority.
"""

from .common import *
from .bronze import *
