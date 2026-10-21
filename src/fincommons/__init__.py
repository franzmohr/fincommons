"""fincommons: shared infrastructure for financial data work.

fincommons is the shared infrastructure that supervisors and financial
firms -- banks, insurers and investment firms alike -- can use together. It
holds the pipelines that work entirely with public data (exchange rates,
market prices, FIRDS reference data, entity master data) and the data lake
infrastructure they are built on: addressing through ``paths.yaml``,
logging, the file layout of tables and the declared schemas.

Pipelines on supervisory reporting data can build on this package; it knows
nothing of them.
"""

import logging

__version__ = "0.1.0"

# The package logs through the tree below this logger and leaves the
# configuration to the caller, see fincommons.log. The handler makes that
# silence deliberate: without it, logging would report that no handler could be
# found the first time a pipeline writes a record in an unconfigured process.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# The column catalogue needs neither Spark nor a data lake, so it is offered
# at the top of the package: fincommons.describe_column("TRADGVN").
from .columns import column_catalogue, describe_column  # noqa: E402,F401
