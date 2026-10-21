"""Entity master data: the registers in the Bronze layer.

Every source arrives as a file that a spreadsheet program or a zip archive
stands between: the GLEIF golden copies as zipped csv files, the ECB and EIOPA
registers as Excel workbooks, the list of CCPs as a plain csv file. This module
rewrites each of them as parquet, one file per reference date, without changing
what the register says. The reference date is read off the name of the file,
which is why the landing zone insists on those names.

A file is removed from the landing zone only after it has been written to the
Bronze layer. Where a file cannot be read the error is raised rather than
recorded and skipped, so that the run stops with the file still in place
instead of carrying on and leaving a gap in the Bronze layer that nothing
points at.
"""

import logging
import os

import pandas
import pyarrow
import pyarrow.parquet

from . import common
from ..log import context, pipeline_run

logger = logging.getLogger(__name__)

#: The workbooks the ECB and EIOPA registers are read from.
#:
#: The legacy ``.xls`` format is not among them. Reading it needs ``xlrd``,
#: which the package does not depend on, so a file in that format, were it
#: accepted by the listing, would fail on the import with a message about a
#: missing package rather than about the file. The README asks for the
#: workbook to be saved as ``.xlsx`` in every case, and a file of another kind
#: is named in a warning, see :func:`landing_files`.
EXCEL_EXTENSIONS = (".xlsx",)


def landing_files(folder, extensions):
    """Return the names of the files in ``folder`` worth importing, in order.

    The names are sorted, because they begin with the reference date and the
    import applies them oldest first, and :func:`os.listdir` promises no order
    of its own. Files of another kind are ignored, so that a note left next to
    the registers does not reach the reader, but they are named in a warning:
    the registers are placed in the landing zone by hand, so a file of the
    wrong kind is as likely to be the register saved in the wrong format as it
    is to be a note, and a register that is quietly not imported is the thing
    worth hearing about.

    Parameters
    ----------
    folder: str
        Folder of the landing zone that should be listed.
    extensions: tuple of str
        The file extensions to keep, lower case and with the dot.
    """
    present = [
        name for name in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, name))
    ]
    names = sorted(name for name in present if name.lower().endswith(extensions))
    ignored = sorted(set(present) - set(names))
    if ignored:
        logger.warning(
            "Files of the landing zone were not imported, they are not %s",
            " or ".join(extensions),
            extra=context(module="entitymasterdata", layer="bronze",
                          folder=folder, files="|".join(ignored)),
        )
    return names


def reference_date_from_file_name(file_name):
    """Return the reference date the name of a landing zone file begins with.

    Every register is stored under a name whose first eight characters are the
    reference date as ``YYYYMMDD``. The date is returned as ``YYYY-MM-DD``,
    which is the spelling the ``REFDATE`` column carries.

    Parameters
    ----------
    file_name: str
        Name of the file in the landing zone.
    """
    return pandas.to_datetime(file_name[0:8], format='%Y%m%d').strftime("%Y-%m-%d")


def end_of_investment_fund_period(file_name):
    """Return the last day of the period an ECB list of funds covers.

    The workbook is named ``IF_<year>_<period>.xlsx``, where the period is
    either a month, ``IF_2026_03.xlsx``, or a quarter, ``IF_2026_Q1.xlsx``. The
    reference date is the last day of it, because the list states the position
    at the end of the period rather than on a day within it.

    Parameters
    ----------
    file_name: str
        Name of the workbook in the landing zone.
    """
    period = file_name[3:7] + file_name[8:10]
    if "Q" in period:
        frequency = 'Q'
    elif period.isdigit():
        frequency = 'M'
    else:
        raise ValueError(
            f"The name {file_name!r} does not spell out the period the list covers. "
            "The module expects the name the ECB publishes the workbook under, "
            "'IF_<year>_<month>.xlsx' or 'IF_<year>_Q<quarter>.xlsx'."
        )
    return pandas.PeriodIndex([period], freq=frequency).to_timestamp(frequency).astype(str)[0]


def write_parquet(frame, path):
    """Write a data frame to the Bronze layer as parquet.

    A column of a register that currently carries no value at all reaches
    pyarrow as the null type, and Spark then reads it back as a column with no
    type of its own, which every comparison in the Silver layer has to be
    coerced through and which it resolves in ways that have nothing to do with
    what the register says. Such a column is text as far as the register is
    concerned, so it is written as text.
    """
    table = pyarrow.Table.from_pandas(frame, preserve_index=False)
    table = table.cast(pyarrow.schema([
        field.with_type(pyarrow.string()) if pyarrow.types.is_null(field.type) else field
        for field in table.schema
    ]))
    pyarrow.parquet.write_table(table, path)




# GLEIF
# --------------------------------------------------------------------------


def update_bronze_gleif_level_1(config_path):
    """Update GLEIF Level 1 data in the Bronze layer.

    Imports the zipped csv files holding GLEIF Level 1 data from the landing
    zone into the Bronze layer. Only the three columns the entity master data
    set draws on are read, because the golden copy carries some two hundred of
    them and the rest is never looked at.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of GLEIF Level 1 files imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_landing = common.path_landing_gleif_level_1(config_path)
    path_bronze = common.path_bronze_gleif_level_1(config_path)

    imported = 0
    for file_name in landing_files(path_landing, (".zip",)):
        path_i = os.path.join(path_landing, file_name)
        refdate = reference_date_from_file_name(file_name)

        raw = pandas.read_csv(
            path_i,
            compression = "zip",
            usecols = ["LEI", "Entity.LegalName", "Entity.LegalAddress.Country"],
        )
        raw = raw.rename(
            columns = {"Entity.LegalName": "NAME", "Entity.LegalAddress.Country": "CTRY"}
        )
        raw['REFDATE'] = refdate

        write_parquet(raw, os.path.join(path_bronze, file_name[0:8] + ".parquet"))
        os.remove(path_i)

        imported += 1
        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source="GLEIF_L1",
                          file=file_name, refdt=refdate, rows=len(raw)),
        )

    return imported


def update_bronze_gleif_level_2(config_path):
    """Update GLEIF Level 2 data in the Bronze layer.

    Imports the zipped csv files holding GLEIF Level 2 data, i.e. the reported
    relationships between legal entities, from the landing zone into the Bronze
    layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of GLEIF Level 2 files imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_landing = common.path_landing_gleif_level_2(config_path)
    path_bronze = common.path_bronze_gleif_level_2(config_path)

    imported = 0
    for file_name in landing_files(path_landing, (".zip",)):
        path_i = os.path.join(path_landing, file_name)
        refdate = reference_date_from_file_name(file_name)

        raw = pandas.read_csv(
            path_i,
            compression = "zip",
            usecols = ["Relationship.StartNode.NodeID", "Relationship.StartNode.NodeIDType",
                       "Relationship.EndNode.NodeID", "Relationship.EndNode.NodeIDType",
                       "Relationship.RelationshipType", "Relationship.RelationshipStatus"],
        )
        raw['REFDATE'] = refdate

        write_parquet(raw, os.path.join(path_bronze, file_name[0:8] + ".parquet"))
        os.remove(path_i)

        imported += 1
        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source="GLEIF_L2",
                          file=file_name, refdt=refdate, rows=len(raw)),
        )

    return imported




# ECB
# --------------------------------------------------------------------------


def update_bronze_ecb_investment_fund(config_path):
    """Update the ECB's list of investment funds in the Bronze layer.

    Imports the lists of investment funds published on the ECB website from the
    landing zone into the Bronze layer. The ECB publishes the list per month or
    per quarter and states the period in the name of the workbook, so the
    reference date is the last day of the period the name spells out rather
    than a date read off the name directly.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of investment funds imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_landing = common.path_landing_ecb_investment_fund(config_path)
    path_bronze = common.path_bronze_ecb_investment_fund(config_path)

    imported = 0
    for file_name in landing_files(path_landing, EXCEL_EXTENSIONS):
        path_i = os.path.join(path_landing, file_name)

        raw = pandas.read_excel(path_i, sheet_name=0)
        refdate = end_of_investment_fund_period(file_name)
        raw['REFDATE'] = refdate

        write_parquet(raw, os.path.join(path_bronze, refdate + ".parquet"))
        os.remove(path_i)

        imported += 1
        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source="ECB_IF",
                          file=file_name, refdt=refdate, rows=len(raw)),
        )

    return imported


def update_bronze_ecb_mfi(config_path):
    """Update the ECB's list of MFIs in the Bronze layer.

    Imports the lists of monetary financial institutions published on the ECB
    website from the landing zone into the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of MFIs imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_landing = common.path_landing_ecb_mfi(config_path)
    path_bronze = common.path_bronze_ecb_mfi(config_path)

    imported = 0
    for file_name in landing_files(path_landing, EXCEL_EXTENSIONS):
        path_i = os.path.join(path_landing, file_name)

        refdate = reference_date_from_file_name(file_name)

        # The postal columns are read as text from the workbook itself: a
        # postcode is not a number, and taking it for one dropped the leading
        # zero of '00187' and turned '1012 AB' into a missing value.
        raw = pandas.read_excel(path_i, sheet_name=0,
                                dtype={'BOX': "string", 'POSTAL': "string"})
        raw['REFDATE'] = refdate
        # Columns whose values look numeric in some rows and not in others
        # reach pyarrow as a mixed type it refuses. They are settled here, the
        # identifiers and the two postal columns as text. The
        # nullable string type is used rather than str, which would turn a
        # missing identifier into the text 'nan' and carry it into the joins of
        # the Silver layer as though it were an LEI.
        raw['LEI'] = raw['LEI'].astype("string")
        raw['HEAD_LEI'] = raw['HEAD_LEI'].astype("string")

        write_parquet(raw, os.path.join(path_bronze, file_name[0:8] + ".parquet"))
        os.remove(path_i)

        imported += 1
        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source="ECB_MFI",
                          file=file_name, refdt=refdate, rows=len(raw)),
        )

    return imported




# EIOPA
# --------------------------------------------------------------------------


def update_bronze_eiopa_insurance_undertaking(config_path):
    """Update EIOPA's list of insurance undertakings in the Bronze layer.

    Imports the lists of insurance undertakings published on EIOPA's website
    from the landing zone into the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of insurance undertakings imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_landing = common.path_landing_eiopa_insurance_undertaking(config_path)
    path_bronze = common.path_bronze_eiopa_insurance_undertaking(config_path)

    imported = 0
    for file_name in landing_files(path_landing, EXCEL_EXTENSIONS):
        path_i = os.path.join(path_landing, file_name)

        refdate = reference_date_from_file_name(file_name)

        raw = pandas.read_excel(path_i, sheet_name=0)
        raw['REFDATE'] = refdate
        # As with the MFI register, the columns that reach pyarrow as a mixed
        # type are settled first. EIOPA spells its dates day first.
        for column in ['Identification code', 'LEI', 'Street Name', 'Street Number', 'Post Code']:
            raw[column] = raw[column].astype("string")
        for column in ['Registration start date', 'Registration end date',
                       'Operation Start Date', 'Operation End Date']:
            raw[column] = pandas.to_datetime(raw[column], format='%d/%m/%Y').dt.strftime("%Y-%m-%d")

        write_parquet(raw, os.path.join(path_bronze, file_name[0:8] + ".parquet"))
        os.remove(path_i)

        imported += 1
        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source="EIOPA_IC",
                          file=file_name, refdt=refdate, rows=len(raw)),
        )

    return imported



def update_bronze_eiopa_iorp(config_path):
    """Update EIOPA's list of institutions for occupational retirement provision in the Bronze layer.

    Imports the lists of institutions for occupational retirement provision published on EIOPA's website
    from the landing zone into the Bronze layer.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of institutions for occupational retirement provision imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_landing = common.path_landing_eiopa_iorp(config_path)
    path_bronze = common.path_bronze_eiopa_iorp(config_path)

    imported = 0
    for file_name in landing_files(path_landing, EXCEL_EXTENSIONS):
        path_i = os.path.join(path_landing, file_name)

        refdate = reference_date_from_file_name(file_name)

        raw = pandas.read_excel(path_i, sheet_name=0)
        raw['REFDATE'] = refdate
        # As with the MFI register, the columns that reach pyarrow as a mixed
        # type are settled first. EIOPA spells its dates day first.
        for column in ['Name of NCA', 'Official name of the entity', 'Home Country', 'EU Country where the entity operates',
                       'Type of IORP', 'Street name', 'Street number', 'Post Code', 'City', 'Legal name of the managing entity',
                       'Address of the managing entity', 'Cross border status', 'Cross border activity', 'LEI',
                       'Identification Code', 'Website address']:
            raw[column] = raw[column].astype("string")
        for column in ['Registration Start Date', 'Registration End Date']:
            raw[column] = pandas.to_datetime(raw[column], format='%d/%m/%Y').dt.strftime("%Y-%m-%d")

        write_parquet(raw, os.path.join(path_bronze, file_name[0:8] + ".parquet"))
        os.remove(path_i)

        imported += 1
        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source="EIOPA_IORP",
                          file=file_name, refdt=refdate, rows=len(raw)),
        )

    return imported




# ESMA
# --------------------------------------------------------------------------


def update_bronze_esma_ccp(config_path):
    """Update the list of CCPs in the Bronze layer.

    Imports the list of central counterparties from the landing zone into the
    Bronze layer. Unlike the other registers the list is not kept per reference
    date: it names the CCPs currently authorised, and it is small enough that
    the file is simply replaced.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of CCPs imported, one where the landing zone holds the
        list and zero where it does not. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    path_i = os.path.join(common.path_landing_esma_ccp(config_path), "ccp_list.csv")

    # The list is downloaded rather than placed by hand, so a landing zone
    # without it means the download has not been run rather than that a file is
    # broken. The other registers import nothing from an empty folder without
    # comment; this one says so, because the list reaching the Bronze layer is
    # what a run of the Silver layer goes on to depend on.
    if not os.path.exists(path_i):
        logger.warning(
            "Nothing to import, the register is not in the landing zone",
            extra=context(module="entitymasterdata", layer="bronze", source="ESMA_CCP",
                          file=path_i),
        )
        return 0

    raw = pandas.read_csv(path_i, usecols=["LEI", "SECTOR", "SUBSECTOR"])

    write_parquet(
        raw, os.path.join(common.path_bronze_esma_ccp(config_path), "ccp_list.parquet")
    )
    # The file leaves the landing zone like every other register, which it used
    # not to do. Leaving it there was the more dangerous of the two: the list
    # is the one register whose name carries no reference date, so a copy left
    # behind is indistinguishable from one just downloaded, and a run whose
    # download failed would have imported the copy of the run before it and
    # reported it as the register.
    os.remove(path_i)

    logger.info(
        "Imported register into the Bronze layer",
        extra=context(module="entitymasterdata", layer="bronze", source="ESMA_CCP",
                      file="ccp_list.csv", rows=len(raw)),
    )

    return 1




def update_from_landing_to_bronze(config_path):
    """Entity master data: import the registers into the Bronze layer.

    Imports everything the landing zone currently holds, source by source. The
    downloads are not part of this: GLEIF needs a link that has to be looked up
    by hand, and the ECB and EIOPA registers are placed in the landing zone by
    hand as well.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Examples
    --------
    >>> update_from_landing_to_bronze("path/to/configuration/file")
    """
    with pipeline_run(
        logger,
        "update_from_landing_to_bronze",
        module="entitymasterdata",
        layer="bronze",
        config_path=config_path,
    ) as outcome:
        _update_from_landing_to_bronze(config_path, outcome)


def _update_from_landing_to_bronze(config_path, outcome):
    """Do the work of :func:`update_from_landing_to_bronze`.

    Each of the imports reports how many files it read, so that the closing
    record of the stage states what the run actually brought in. A run that
    imported nothing at all, because the landing zone was never filled, is
    otherwise indistinguishable from one that imported every register.
    """
    imported = 0

    # GLEIF data
    imported += update_bronze_gleif_level_1(config_path)
    imported += update_bronze_gleif_level_2(config_path)

    # ECB data
    imported += update_bronze_ecb_investment_fund(config_path)
    imported += update_bronze_ecb_mfi(config_path)

    # EIOPA data
    imported += update_bronze_eiopa_insurance_undertaking(config_path)
    imported += update_bronze_eiopa_iorp(config_path)

    # ESMA data
    imported += update_bronze_esma_ccp(config_path)

    outcome["imported"] = imported


#: What this module offers. Importing the registers of the landing zone into
#: the Bronze layer, one file per reference date.
__all__ = [
    "EXCEL_EXTENSIONS",
    "end_of_investment_fund_period",
    "landing_files",
    "reference_date_from_file_name",
    "update_bronze_ecb_investment_fund",
    "update_bronze_ecb_mfi",
    "update_bronze_eiopa_insurance_undertaking",
    "update_bronze_eiopa_iorp",
    "update_bronze_esma_ccp",
    "update_bronze_gleif_level_1",
    "update_bronze_gleif_level_2",
    "update_from_landing_to_bronze",
    "write_parquet",
]
