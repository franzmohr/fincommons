"""Exchange rates: download, combination and conversion.

Under EMIR and SFTR an amount is reported in the currency it was agreed in, so
comparing amounts across reports means converting them first. ESMA builds the
figures for the ASR on European derivatives markets on the reference rates of
the ECB and augments them from further sources, and this module follows that
practice in three stages:

``common``
    the layout of the data lake and the schema of the proprietary source,
``bronze``
    downloading the reference rates published by the ECB and the BIS,
``silver``
    combining them with the proprietary data into one table with a row per
    calendar day and currency pair, and :func:`convert2ccy`, which converts a
    column of amounts with that table.

The euro is the base currency of the result unless ``rebase`` names another
one, which is what an organisation outside the euro area would do.
"""

from .common import *
from .bronze import *
from .silver import *
