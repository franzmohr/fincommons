"""The Kenneth R. French Data Library: the factors and portfolios of Fama and French.

Kenneth R. French publishes, at
https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html,
the factors of the Fama-French models and the returns of the portfolios they
are built from, for the US since 1926 and for international regions since
1990, along with the breakpoints of the sorts. Much of empirical asset
pricing rests on these files. They are published as some three hundred
zipped CSV files, each a text for a person to read, and the module brings
them into the data lake in these stages:

``common``
    the layout of the data lake and the table the result is read from,
``bronze``
    collecting each file that has changed, its text kept line by line,
``parse``
    reading the tables of a file into long records, in plain Python,
``silver``
    one table with every value of every file, rebuilt file by file, and
    :func:`series`, which turns a table of it back into the shape it was
    published in,
``pipeline``
    the whole pipeline in one call, :func:`update_frenchdatalibrary`.

The files are downloaded when the pipeline runs and are never shipped with
the package. They are copyright Eugene F. Fama and Kenneth R. French, and
the page states no licence: a use in research cites the library, and a use
beyond it is a question for its authors.
"""

from .common import *
from .parse import *
from .bronze import *
from .silver import *
from .pipeline import *
