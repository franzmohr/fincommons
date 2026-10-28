"""Configuration shared by the whole test suite.

The Spark session every test runs against is built by
:mod:`fincommons.testing`, which packages building on fincommons import as
well, so that every suite runs against the same configuration.

Every test that needs the session, directly or through another fixture, is
marked ``spark`` as it is collected, so that the suite splits into two tiers
without anyone having to remember to mark a test:

* ``pytest -m "not spark"`` runs the tests that need no Spark session, in
  seconds, and is what to run while working;
* ``pytest`` runs everything, which takes the better part of an hour on a
  laptop and is what the continuous integration runs on every pull request.
"""

import pytest

from fincommons.testing import spark  # noqa: F401


def pytest_collection_modifyitems(items):
    for item in items:
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)
