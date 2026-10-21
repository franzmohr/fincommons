"""Market price data: download and combination of price and rate data.

A supervisory data set records an amount, a notional or a price in the terms
that were agreed, and comparing those figures or revaluing a position means
holding the market data they are to be measured against. This module collects
that market data from the public sources in three stages:

``common``
    the layout of the data lake and the upsert the Silver table is kept by,
``bronze_*``
    downloading what each publisher offers, unchanged,
``download``
    the download the publishers that send one whole file share, which refuses
    an answer that is not the series before it replaces the history,
``silver_*``
    reducing every source to a reference date, an instrument and a value in one
    combined table, which :func:`connect_to_marketpricedata` opens,
``pipeline``
    running the sources of a layer in one call.

Every source writes the same three columns, so the instrument identifier is
what tells them apart: an ISIN where the source names one, a currency pair such
as ``EUR/USD`` for an exchange rate, and a name such as ``ON-ESTR`` for an
overnight rate.
"""

from .common import *
from .bronze_boe import *
from .bronze_dse import *
from .bronze_vndirect import *
from .bronze_ecb import *
from .bronze_electricity import *
from .bronze_lbma import *
from .bronze_nyfed import *
from .bronze_yahoo import *
from .silver_boe import *
from .silver_dse import *
from .silver_vndirect import *
from .silver_ecb import *
from .silver_electricity import *
from .silver_lbma import *
from .silver_exchangerate import *
from .silver_nyfed import *
from .silver_internal import *
from .silver_yahoo import *
from .dse_events import *
from .pipeline import *
from .describe import *
