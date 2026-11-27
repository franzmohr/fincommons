"""Schemas and lookup tables shipped as JSON in a package's ``data`` directory.

:func:`load_pyspark_schema` reads a Spark schema that was derived ahead of
time -- from the XSD of an ISO 20022 message, or from an answer of the ECB
data API, by the scripts in ``tools/`` -- so that a file is read against a
declared schema rather than one inferred from whatever sample it happens to
be. :func:`load_replacement_table` reads any other JSON table a package ships,
for instance one that maps the values of a column to replacements.

Both read from the ``data`` directory of this package by default, or of the
package named by ``package``, which is how a package built on fincommons reads
its own files. Only :func:`load_pyspark_schema` needs PySpark, and it imports
it when called, so reading a replacement table does not.
"""

import json
from importlib import resources

__all__ = ["load_pyspark_schema", "load_replacement_table"]


def _read_package_data(name, package=None):
    """Read a json file shipped in the data directory of a package.

    Parameters
    ----------
    name: str
        Name of the file without the file type.
    package: str, optional
        The package whose ``data`` directory holds the file. By default this
        package, located relative to this module rather than through a hard
        coded name, so that it works whether it is imported as ``fincommons``
        or, from the project root, as ``src.fincommons``. A package that builds
        on this one names itself to read its own files.
    """
    data = resources.files(package or __package__).joinpath("data", name + ".json")
    with data.open(encoding="utf-8") as json_file:
        return json.load(json_file)


def load_pyspark_schema(schema, package=None):
    """Get a Spark schema shipped as JSON in a package's ``data`` directory.

    The schemas are derived ahead of time by the scripts in ``tools/``: from
    the XSD of an ISO 20022 message (e.g. the FIRDS files) or from an answer
    of the ECB data API.

    Parameters
    ----------
    schema: str
        The name of the schema that should be loaded without the end of the
        file type, as the file in the ``data`` directory is spelled, e.g.
        ``firds_schema_017_001_02``.
    package: str, optional
        The package that ships the file; see :func:`_read_package_data`.

    Returns
    -------
    pyspark.sql.types.StructType
    """
    from pyspark.sql.types import StructType

    schema_json = _read_package_data(schema, package)

    return StructType.fromJson(schema_json)


def load_replacement_table(table, package=None):
    """Get the replacement table for a dataset.

    Needs no PySpark.

    Parameters
    ----------
    table: str
        The name of the table that should be loaded without the end of the file type.
    package: str, optional
        The package that ships the file; see :func:`_read_package_data`.
    """
    return _read_package_data(table, package)
