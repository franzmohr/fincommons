"""Financial reports: the filings of both regimes in one table.

Two regimes require the annual and periodic reports of listed issuers to be
tagged: the European Single Electronic Format in the Union, and the inline
XBRL mandate of the Securities and Exchange Commission in the United States.
:mod:`fincommons.esef` and :mod:`fincommons.secfiling` each bring one of them
into the lake, as its publisher gives it out. This module puts the two side by
side.

``common``
    the layout of the combined tables, and what each source does and does not
    cover,
``silver``
    reducing each source to the same columns and putting them one after the
    other, which :func:`update_silver_financialreport` does,
``mapping``
    the only place in the package where a concept of one framework is put
    beside a concept of the other, as a catalogue that says on what basis and
    with what caveat.

Three things this module deliberately does not do.

It does not take anything away from the two source modules. Their tables stay
authoritative and complete, neither imports this module, and neither knows it
exists: an ESEF-only lake builds nothing here and loses nothing by it. The
dependency runs one way.

It does not join the regimes by entity, because it cannot. ESEF prescribes the
legal entity identifier, a filing with the Commission states a central index
key, and no authoritative mapping between the two is published by either body.
Both columns are carried and one of them is empty in every row. It is the one
thing a lake holding both regimes would most like to do, and the honest shape
of the table says so.

It does not reconcile the vocabularies in the tables. ``ifrs-full:Revenue``
and ``us-gaap:Revenues`` stand as they were filed. What the two have in
common is a judgement about accounting, which ``mapping`` records as data
rather than as code, and which is applied per query by whoever has read what
it rests on.

The catalogue of measures is usable on its own. It joins on the concept as
both source tables spell it, so a lake that has only collected ESEF filings
can resolve a measure over :mod:`fincommons.esef` directly, without building
a combined table.
"""

from .common import *
from .silver import *
from .mapping import *
