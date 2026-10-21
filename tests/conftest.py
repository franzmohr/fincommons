"""Configuration shared by the whole test suite.

The Spark session every test runs against is built by
:mod:`fincommons.testing`, which packages building on fincommons import as
well, so that every suite runs against the same configuration.
"""

from fincommons.testing import spark  # noqa: F401
