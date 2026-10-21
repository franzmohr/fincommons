"""Entity master data: who the counterparties of a report are.

A report says what was agreed and with whom, but it identifies the counterparty
by a code alone. Anything asked of the data beyond a single contract -- which
sector is exposed, which group a set of entities belongs to, which country a
position sits in -- needs that code resolved into a description of the entity.
This module builds one table that does so, following the approach of `Lenoci
and Letizia (2021)
<https://link.springer.com/chapter/10.1007/978-3-030-66891-4_6>`_: GLEIF says
who the entity is and how it is related to other entities, and the registers of
the ECB, EIOPA and ESMA say what it does.

The pipeline runs in four stages:

``common``
    the layout of the data lake and the table the result is read from,
``landing``
    downloading the sources that can be fetched over the network,
``bronze``
    importing the registers of the landing zone unchanged, one file per
    reference date,
``silver``
    combining them into the entity master data set, first from the public
    sources alone and then with a proprietary list in front of them.

Both Silver tables carry a row per LEI. ``update_entitymasterdata`` runs the
whole pipeline; ``connect_to_entitymasterdata`` opens the result.
"""

from .common import *
from .landing import *
from .bronze import *
from .silver import *
