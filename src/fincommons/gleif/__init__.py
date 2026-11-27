"""GLEIF: the legal entity identifiers, and a history of what they say.

The module keeps a history of GLEIF's reference data in the Silver layer, of
the same shape as the FIRDS history of :mod:`fincommons.firds`: one version of
a record per state it held, with the days it held it. Level 1 says who an
entity is, Level 2 who owns or manages whom.

``landing``
    which of GLEIF's publications a history needs, and their download,
``bronze``
    the files as the Bronze layer keeps them,
``history``
    the rules a history is built by, independently of its storage, and
``silver``
    the Delta tables of the Silver layer and the pipeline that keeps them.

:mod:`fincommons.entitymasterdata` reads the entities and their parents from
here where the configuration file has a ``gleif`` section.
"""

from .common import *
from .landing import *
from .bronze import *
from .silver import *
from . import history
