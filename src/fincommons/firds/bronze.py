
import logging
import ntpath
import os
import pyspark.sql.functions as func
import shutil
import zipfile

from ..layout import rebalanced
from ..log import context, pipeline_run
from ..schemas import load_pyspark_schema
from .common import *
from .utilities import *

logger = logging.getLogger(__name__)


def read_xml_017_001_02(spark, file):
    """Reads FIRDS data from an XML file.
    
    Reads FIRDS data from an XML file with the schema auth.017.001.02.
    
    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    file: str
        File that should be imported.
    """
    
    reconstructed_schema = load_pyspark_schema("firds_schema_017_001_02")

    df = spark.read.format("xml") \
                .options(rowTag='RefData') \
                .schema(reconstructed_schema) \
                .load(file)
    
    return df


#: The schema both delta message definitions are read with.
#:
#: ESMA published the delta file as ``auth.036.001.02`` and revised it to
#: ``auth.036.001.03``; a history that reaches back far enough is therefore
#: built from files of both. They are read with the schema derived from the
#: newer XSD rather than one schema each. Every element path the older files
#: use is present in it, so nothing they state is dropped, and the elements it
#: adds come back empty for a file that does not state them, which is what an
#: explicit schema does for any absent element.
#:
#: The alternative, a schema of its own for the older definition, would give
#: the Bronze layer two shapes for one data set. The Silver layer reads that
#: layer through the named paths of
#: :data:`fincommons.firds.silver.SOURCE_FIELDS` precisely so that it need not
#: know which vintage a file was written with, and two shapes would put that
#: back on it for no gain.
DLTINS_SCHEMA = "firds_schema_036_001_03"


def _read_dltins(spark, file):
    """Read a delta file of either message definition.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    file: str
        File that should be imported.
    """
    reconstructed_schema = load_pyspark_schema(DLTINS_SCHEMA)

    df = spark.read.format("xml") \
                .options(rowTag='FinInstrm') \
                .schema(reconstructed_schema) \
                .load(file)

    return df


def read_xml_036_001_03(spark, file):
    """Reads FIRDS data from an XML file.

    Reads FIRDS data from an XML file with the schema auth.036.001.03.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    file: str
        File that should be imported.
    """
    return _read_dltins(spark, file)


def read_xml_036_001_02(spark, file):
    """Reads FIRDS data from an XML file.

    Reads FIRDS data from a delta file with the schema auth.036.001.02, which
    is the message definition ESMA published before ``auth.036.001.03``. A
    record has the same shape in both: it is a ``<FinInstrm>`` holding one of
    ``<NewRcrd>``, ``<ModfdRcrd>``, ``<TermntdRcrd>`` or ``<CancRcrd>``, and
    the file is read with :data:`DLTINS_SCHEMA` for the reasons given there.

    The reader is named after the older definition all the same, rather than
    the newer one being reused for both, so that the import says which
    definition a file was recognised as and so that a schema of its own can be
    given to the older files later without moving anything else.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    file: str
        File that should be imported.
    """
    return _read_dltins(spark, file)


def read_xml_102_001_01(spark, file):
    """Reads FIRDS cancellations data from an XML file.

    Reads FIRDS data from an XML file with the schema auth.102.001.01, the
    cancellations file. A record of such a file is a ``<CxlData>`` element and
    carries nothing but the instrument and the trading venue whose combination
    was cancelled.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    file: str
        File that should be imported.
    """

    reconstructed_schema = load_pyspark_schema("firds_schema_102_001_01")

    df = spark.read.format("xml") \
                .options(rowTag='CxlData') \
                .schema(reconstructed_schema) \
                .load(file)

    return df


def update_from_landing_to_bronze(spark, config_path):
    """FIRDS: Import raw data into Bronze layer.
    
    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    
    with pipeline_run(
        logger,
        "update_from_landing_to_bronze",
        module="firds",
        layer="bronze",
        config_path=config_path,
    ) as outcome:
        _update_from_landing_to_bronze(spark, config_path, outcome)


#: The schemas the import recognises, with the name of the reader, the Bronze
#: folder and the function reading the publication date off the file name.
#: The readers are named rather than referenced, so that they are looked up
#: when a file is read.
SCHEMAS = {
    "auth.017.001.02": ("read_xml_017_001_02", path_bronze_auth_017_001_02, get_fulins_data_date),
    "auth.036.001.03": ("read_xml_036_001_03", path_bronze_auth_036_001_03, get_dltins_data_date),
    # The delta message definition ESMA published before auth.036.001.03, up
    # to and including 2022-01-31. It has a folder of its own, named after it
    # like every other folder of the Bronze layer, so that the layer says
    # which message each file was published under. The Silver layer reads the
    # two folders together; see
    # :func:`fincommons.firds.common.paths_bronze_dltins`.
    "auth.036.001.02": ("read_xml_036_001_02", path_bronze_auth_036_001_02, get_dltins_data_date),
    "auth.102.001.01": ("read_xml_102_001_01", path_bronze_auth_102_001_01, get_fulcan_data_date),
}


def _base_name(name):
    """The name of a file without any of its extensions."""
    return ntpath.basename(name).split(".")[0]


def _route(file, zip_path, folder):
    """Move a file out of the import folder into ``folder``.

    For an archive, the archive is moved and the XML extracted from it is
    removed. Either may already be gone when a previous step failed half-way,
    which is not a reason to fail again. A file of the same name already in
    ``folder`` is replaced.
    """
    source = file
    if zip_path is not None:
        if file is not None and os.path.exists(file):
            os.remove(file)
        source = zip_path
    if os.path.exists(source):
        target = os.path.join(folder, ntpath.basename(source))
        shutil.move(source, target + ".moving")
        os.replace(target + ".moving", target)


def _update_from_landing_to_bronze(spark, config_path, outcome):
    """Do the work of :func:`update_from_landing_to_bronze`.

    A file that cannot be read is moved to the error folder and the import
    carries on with the next one, which is what a bulk import of files
    delivered by a third party has to do. That decision is deliberate and
    predates the logging: what the log adds is that the failure keeps its
    traceback, so that a malformed XML file and a failing write to the Bronze
    layer can be told apart afterwards, and that the closing record reports how
    many files were set aside. An import that quarantines everything is
    otherwise indistinguishable from a clean run.

    A file whose name is already in the finished folder is not imported again
    but set aside as well. The Bronze layer is only ever appended to, so a
    second import would put every record of the file into it twice.
    """
    remove_duplicates_from_landing_import_folder(config_path)
    files_in_folder = sorted(get_candidates_for_import(config_path))
    dir_import = path_landing_import(config_path)
    dir_finished = path_landing_finished(config_path)
    dir_error = path_landing_error(config_path)
    already_imported = {_base_name(name) for name in os.listdir(dir_finished)}

    outcome["candidates"] = len(files_in_folder)
    imported = 0
    quarantined = 0

    for name in files_in_folder:
        is_zip = ".zip" in name.lower()
        zip_path = os.path.join(dir_import, name) if is_zip else None
        file = None if is_zip else os.path.join(dir_import, name)

        if _base_name(name) in already_imported:
            _route(file, zip_path, dir_error)
            quarantined += 1
            logger.warning(
                "File was already imported, the copy was quarantined",
                extra=context(module="firds", layer="bronze", file=name),
            )
            continue

        if is_zip:
            try:
                file = os.path.join(dir_import, unzip_and_get_names(config_path, name))
            # Every way unpacking can fail is caught, not ValueError
            # alone: a truncated download raises zipfile.BadZipFile, which
            # derives from Exception directly, and a handler narrower than
            # this one lets it end the run instead of quarantining the
            # file.
            except Exception:
                logger.exception(
                    "Could not unpack the archive, the file was quarantined",
                    extra=context(module="firds", layer="bronze", file=name),
                )
                _route(None, zip_path, dir_error)
                quarantined += 1
                continue

        # The header is read inside the try, because reading it is one of
        # the ways a file fails: it is decoded as UTF-8 text, and a file
        # that is not text at all -- a truncated download, a stray binary
        # -- raises before the loop can quarantine it, which would leave
        # the files behind it stranded in the import folder.
        schema = None

        try:
            beginning_of_xml_file = read_first_n_chars(file, 2000)
            schema = extract_between_keywords(beginning_of_xml_file, "MsgDefIdr")
            known = next((key for key in SCHEMAS if schema and key in schema), None)

            if known is None:
                _route(file, zip_path, dir_error)
                quarantined += 1
                logger.warning(
                    "Schema not recognized, the file was quarantined",
                    extra=context(
                        module="firds", layer="bronze", file=name, schema=schema,
                    ),
                )
                continue

            reader, path_of, date_of = SCHEMAS[known]
            datadt = date_of(file)
            rebalanced(globals()[reader](spark, file)
                       .withColumn("DATADT", func.lit(datadt)), "DATADT") \
                .write.partitionBy("DATADT").mode("append").parquet(path_of(config_path))

            # A file that carries no records writes nothing, and the reference
            # date would then have no partition at all. The Silver layer takes
            # a date whose file has been imported but has no partition for a
            # partition that has gone missing, and refuses to carry on, so the
            # empty partition is created to say that the date was imported and
            # had nothing in it.
            partition = os.path.join(path_of(config_path), PARTITION_PREFIX + datadt)
            if not os.path.isdir(partition):
                os.makedirs(partition)
                logger.warning(
                    "The file holds no records, its reference date is empty",
                    extra=context(
                        module="firds", layer="bronze", file=name,
                        schema=known, datadt=datadt,
                    ),
                )
        except Exception:
            _route(file, zip_path, dir_error)
            quarantined += 1
            logger.exception(
                "Import failed, the file was quarantined",
                extra=context(
                    module="firds", layer="bronze", file=name, schema=schema,
                ),
            )
            continue

        # The records are in the Bronze layer from here on, so a failure to
        # move the file is not caught: quarantining it would have it
        # downloaded and imported a second time. The run stops instead, and
        # the file, which is still in the import folder, needs moving to the
        # finished folder by hand before the next one.
        _route(file, zip_path, dir_finished)
        already_imported.add(_base_name(name))
        imported += 1
        logger.info(
            "Imported file into the Bronze layer",
            extra=context(
                module="firds", layer="bronze", file=name,
                schema=known, datadt=datadt,
            ),
        )

    outcome["imported"] = imported
    outcome["quarantined"] = quarantined

    if quarantined:
        logger.warning(
            "Files were quarantined and are waiting in the error folder",
            extra=context(
                module="firds",
                layer="bronze",
                files=quarantined,
                path=dir_error,
            ),
        )


#: What this module offers. Reading the files of the landing zone into the Bronze layer.
__all__ = [
    "SCHEMAS",
    "read_xml_017_001_02",
    "read_xml_036_001_02",
    "read_xml_036_001_03",
    "read_xml_102_001_01",
    "update_from_landing_to_bronze",
]
