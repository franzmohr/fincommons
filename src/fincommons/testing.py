"""The Spark session the test suites share.

The suites of fincommons and of the packages built on it run against one
local Spark session, which is built here rather
than in the individual test modules so that every module -- of either
package -- runs against the same configuration and a single session is
started for the whole run. A suite uses it by importing the fixture into its
``conftest.py``::

    from fincommons.testing import spark  # noqa: F401

The module is meant for test suites only: importing it sets the environment
variables described below.

The session carries the Delta extensions, because the watermark of the FIRDS
historical database is a Delta table addressed by path and
:mod:`fincommons.firds.metadata` cannot be exercised without them. Off-cluster
that needs the ``delta-spark`` package, which comes with the ``local`` and
``dev`` extras; where it is absent the session is built without them and only
the tests that touch Delta fail.

Spark launches its Python workers with whatever ``python3`` resolves to on the
PATH, which need not be the interpreter that runs the tests. Where the two
carry different versions of numpy or pandas, objects pickled by the driver fail
to unpickle in the worker with errors such as ``No module named
'numpy._core.numeric'``, and the tests that send Python objects to the workers
fail for a reason that has nothing to do with the code under test. Pointing
Spark at the interpreter that runs the tests removes the mismatch. The
variables are only filled in if they are not already set, so that a run can
still be aimed at another interpreter on purpose.

The same goes for the heap the driver is started with, which the whole suite
shares and which the default of 1 GB is not enough for; see
:data:`DRIVER_MEMORY`.
"""

from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

#: How much heap the driver is started with. In local mode the executors run
#: inside the driver JVM, so this is the whole of the memory the run has;
#: ``spark.executor.memory`` is ignored. The default of 1 GB is enough for any
#: single test and not enough for the whole suite: the session is shared by
#: every module, and a run of several hundred tests accumulates broadcast
#: relations, query plans and Delta log state in that one heap until it fails
#: with an OutOfMemoryError in whichever test happens to be running.
DRIVER_MEMORY = "4g"

# The driver heap cannot be set on the builder below. PySpark starts the JVM
# through spark-submit when the session is first built, and by the time the
# builder's configuration is applied that JVM is already running with the heap
# it was launched with. The launcher reads it from its own arguments, so it
# has to travel as one -- hence PYSPARK_SUBMIT_ARGS, whose value must end in
# "pyspark-shell". As with the interpreter above, a value already in the
# environment is left alone, so that a run can still be aimed elsewhere.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS", f"--driver-memory {DRIVER_MEMORY} pyspark-shell"
)


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")

    builder = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("fincommons-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        session = builder.getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
