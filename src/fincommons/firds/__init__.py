"""FIRDS: download, ingestion and historical database.

The module covers the pipeline for the public FIRDS reference data files:

``landing``
    downloading the files published by ESMA into the landing zone,
``bronze``
    importing them into the Bronze layer,
``silver``
    maintaining the historical database in the Silver layer, and
``history``
    the algorithm that historical database is built with, i.e. section 4.2 of
    ESMA's instructions on the use of full and delta reference data files.
"""

from .common import *
from .utilities import *
from .landing import *
from .bronze import *
from .silver import *
from . import history
