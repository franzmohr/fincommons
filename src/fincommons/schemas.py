
import json
from importlib import resources
from pyspark.sql.types import StructType


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
    """Get the reconstructed schema for a specific xml file.

    Parameters
    ----------
    schema: str
        The name of the schema that should be loaded without the end of the
        file type, as the file in the ``data`` directory is spelled, e.g.
        ``firds_schema_017_001_02``.
    package: str, optional
        The package that ships the file; see :func:`_read_package_data`.
    """
    schema_json = _read_package_data(schema, package)

    result = StructType.fromJson(schema_json)

    return result

def load_replacement_table(table, package=None):
    """Get the replacement table for a dataset.

    Parameters
    ----------
    table: str
        The name of the table that should be loaded without the end of the file type.
    package: str, optional
        The package that ships the file; see :func:`_read_package_data`.
    """
    return _read_package_data(table, package)
