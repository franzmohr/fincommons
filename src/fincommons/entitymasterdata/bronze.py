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
import re

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

#: The registers the downloads of
#: :mod:`fincommons.entitymasterdata.landing` store as text -- the ECB's list
#: of MFIs and EIOPA's registers -- are read as they are published.
CSV_EXTENSIONS = (".csv",)


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


def end_of_period(file_name, prefix):
    """Return the last day of the period an ECB list of FVCs or PSRIs covers.

    The ECB names the workbook after the period: a quarter for the FVCs,
    ``FVC_2026_Q3.xlsx``, and a year or a half-year for the PSRIs,
    ``PSRIs_2021.xlsx`` until 2021 and ``PSRIs_2026_H1.xlsx`` since. The
    reference date is the last day of it, as for the investment funds.

    Parameters
    ----------
    file_name: str
        Name of the workbook in the landing zone.
    prefix: str
        What the name begins with, ``"FVC"`` or ``"PSRIs"``.
    """
    match = re.fullmatch(re.escape(prefix) + r"_(\d{4})(?:_(Q[1-4]|H[12]))?\.xlsx",
                         file_name)
    if match is None:
        raise ValueError(
            f"The name {file_name!r} does not spell out the period the list covers. "
            f"The module expects the name the ECB publishes the workbook under, "
            f"'{prefix}_<year>.xlsx', '{prefix}_<year>_Q<quarter>.xlsx' or "
            f"'{prefix}_<year>_H<half>.xlsx'."
        )
    year, period = match.groups()
    month = {None: 12, "H1": 6, "H2": 12}.get(period)
    if month is None:
        month = 3 * int(period[1])
    return (pandas.Timestamp(int(year), month, 1) + pandas.offsets.MonthEnd(0)) \
        .strftime("%Y-%m-%d")


def read_as_text(path):
    """The first sheet of a workbook, every column as text.

    The ECB's lists mix numbers and text in a column -- a postcode is a number
    in one row and ``1012 AB`` in the next -- which pyarrow refuses, and an
    identifier read as a number loses what makes it one.
    """
    raw = pandas.read_excel(path, sheet_name=0, dtype=object)
    for column in raw.columns:
        raw[column] = raw[column].map(
            lambda v: v if v is None or v != v else str(v)).astype("string")
    return raw


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
    # The file is written under a name the Silver layer does not read and
    # renamed once it is complete, so that an import that dies half-way leaves
    # no truncated file for it to take as the most recent register.
    partial = path + ".partial"
    try:
        pyarrow.parquet.write_table(table, partial)
        os.replace(partial, path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


#: How the csv registers are read: every value as text, and only an empty
#: field as missing. pandas otherwise reads 'NA', 'NULL', 'N/A', 'None' and
#: the like as missing, and 'NA' is the country code of Namibia.
CSV_AS_TEXT = {"dtype": str, "keep_default_na": False, "na_values": [""]}


def import_registers(path_landing, path_bronze, extensions, read, reference_date,
                     bronze_name, source):
    """Import the files of one folder of the landing zone into the Bronze layer.

    Every register goes through the same steps: its files are listed oldest
    first, each is read, given its reference date, written to the Bronze layer
    as parquet and only then removed from the landing zone. The registers
    differ in how a file is read and how its reference date is found, which
    the caller passes in.

    Two files that would be stored under the same Bronze name are refused
    before anything is imported, because the second would silently replace the
    first: a monthly and a quarterly list of investment funds that end on the
    same day, for instance.

    Parameters
    ----------
    path_landing: str
        Folder of the landing zone the files are read from.
    path_bronze: str
        Folder of the Bronze layer they are written to.
    extensions: tuple of str
        The file extensions to import, see :func:`landing_files`.
    read: callable
        Takes the path of a file and returns it as a pandas data frame.
    reference_date: callable
        Takes the name of a file and returns its reference date, ``YYYY-MM-DD``.
    bronze_name: callable
        Takes the name of a file and its reference date and returns the name of
        the Bronze file, without the extension.
    source: str
        Name of the register, for the log.

    Returns
    -------
    int
        Number of files imported.
    """
    files = [(name, reference_date(name)) for name in landing_files(path_landing, extensions)]

    targets = {}
    for name, refdate in files:
        targets.setdefault(bronze_name(name, refdate), []).append(name)
    clashes = sorted(names for names in targets.values() if len(names) > 1)
    if clashes:
        raise ValueError(
            f"Files of the landing zone in {path_landing!r} would replace one "
            f"another in the Bronze layer: {clashes}. Remove all but one of each."
        )

    for name, refdate in files:
        path_i = os.path.join(path_landing, name)
        raw = read(path_i)
        raw['REFDATE'] = refdate

        write_parquet(raw, os.path.join(path_bronze, bronze_name(name, refdate) + ".parquet"))
        os.remove(path_i)

        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source=source,
                          file=name, refdt=refdate, rows=len(raw)),
        )

    return len(files)


def dated_by_name(file_name, refdate):
    """The Bronze name of a register named after its reference date."""
    return file_name[0:8]


def dated_by_period(file_name, refdate):
    """The Bronze name of a register dated at the end of the period it covers."""
    return refdate




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
    def read(path):
        return pandas.read_csv(
            path,
            compression = "zip",
            usecols = ["LEI", "Entity.LegalName", "Entity.LegalAddress.Country"],
            **CSV_AS_TEXT,
        ).rename(
            columns = {"Entity.LegalName": "NAME", "Entity.LegalAddress.Country": "CTRY"}
        )

    return import_registers(
        common.path_landing_gleif_level_1(config_path),
        common.path_bronze_gleif_level_1(config_path),
        (".zip",), read, reference_date_from_file_name, dated_by_name, "GLEIF_L1",
    )


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
    def read(path):
        return pandas.read_csv(
            path,
            compression = "zip",
            usecols = ["Relationship.StartNode.NodeID", "Relationship.StartNode.NodeIDType",
                       "Relationship.EndNode.NodeID", "Relationship.EndNode.NodeIDType",
                       "Relationship.RelationshipType", "Relationship.RelationshipStatus"],
            **CSV_AS_TEXT,
        )

    return import_registers(
        common.path_landing_gleif_level_2(config_path),
        common.path_bronze_gleif_level_2(config_path),
        (".zip",), read, reference_date_from_file_name, dated_by_name, "GLEIF_L2",
    )




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
    return import_registers(
        common.path_landing_ecb_investment_fund(config_path),
        common.path_bronze_ecb_investment_fund(config_path),
        EXCEL_EXTENSIONS, lambda path: pandas.read_excel(path, sheet_name=0),
        end_of_investment_fund_period, dated_by_period, "ECB_IF",
    )


def update_bronze_ecb_fvc(config_path):
    """Update the ECB's list of financial vehicle corporations in the Bronze layer.

    Imports the quarterly lists of FVCs from the landing zone into the Bronze
    layer, each under the last day of its quarter; see :func:`end_of_period`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of FVCs imported.
    """
    return import_registers(
        common.path_landing_ecb_fvc(config_path),
        common.path_bronze_ecb_fvc(config_path),
        EXCEL_EXTENSIONS, read_as_text,
        lambda name: end_of_period(name, "FVC"), dated_by_period, "ECB_FVC",
    )


def update_bronze_ecb_psri(config_path):
    """Update the ECB's list of payment statistics relevant institutions in the Bronze layer.

    Imports the yearly and half-yearly lists of PSRIs from the landing zone
    into the Bronze layer, each under the last day of its period; see
    :func:`end_of_period`. The columns are kept as each list names them: the
    lists until 2021 spell ``Country of residence`` and carry a ``PSP Geo
    Scope``, the later ones spell ``Country of Residence`` and do not.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of PSRIs imported.
    """
    return import_registers(
        common.path_landing_ecb_psri(config_path),
        common.path_bronze_ecb_psri(config_path),
        EXCEL_EXTENSIONS, read_as_text,
        lambda name: end_of_period(name, "PSRIs"), dated_by_period, "ECB_PSRI",
    )


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
    def read(path):
        if path.lower().endswith(".csv"):
            # The list as the ECB publishes it, which is what the download
            # stores: text separated by tabs and encoded in UTF-16. Every
            # column of it is text.
            return pandas.read_csv(path, sep="\t", encoding="utf-16", dtype="string",
                                   keep_default_na=False, na_values=[""])
        # The postal columns are read as text from the workbook itself: a
        # postcode is not a number, and taking it for one would drop the
        # leading zero of '00187' and turn '1012 AB' into a missing value.
        raw = pandas.read_excel(path, sheet_name=0,
                                dtype={'BOX': "string", 'POSTAL': "string"})
        # Columns whose values look numeric in some rows and not in others
        # reach pyarrow as a mixed type it refuses. They are settled here, the
        # identifiers and the two postal columns as text. The
        # nullable string type is used rather than str, which would turn a
        # missing identifier into the text 'nan' and carry it into the joins of
        # the Silver layer as though it were an LEI.
        raw['LEI'] = raw['LEI'].astype("string")
        raw['HEAD_LEI'] = raw['HEAD_LEI'].astype("string")
        return raw

    return import_registers(
        common.path_landing_ecb_mfi(config_path),
        common.path_bronze_ecb_mfi(config_path),
        EXCEL_EXTENSIONS + CSV_EXTENSIONS, read, reference_date_from_file_name,
        dated_by_name, "ECB_MFI",
    )




# EIOPA
# --------------------------------------------------------------------------


#: The columns of EIOPA's registers that hold dates, by register.
EIOPA_DATES = {
    "EIOPA_IC": ["Registration start date", "Registration end date",
                 "Operation Start Date", "Operation End Date"],
    "EIOPA_IORP": ["Registration Start Date", "Registration End Date"],
}


def eiopa_dates(values):
    """EIOPA's dates as ``YYYY-MM-DD``, whichever way the register spells them.

    The CSV export writes most dates day first with a time,
    ``14/11/2013 00:00:00``, and some as ISO 8601, ``1990-12-10T00:00:00Z``,
    within the same column; a workbook holds dates, or text day first. Only
    the day is kept. A value that is none of these is refused rather than
    taken for a missing date.
    """
    text = values.astype("string").str.strip().replace("", pandas.NA)
    day = text.str.slice(0, 10)
    day_first = pandas.to_datetime(day, format="%d/%m/%Y", errors="coerce")
    iso = pandas.to_datetime(day, format="%Y-%m-%d", errors="coerce")
    dates = day_first.fillna(iso)
    unread = text[text.notna() & dates.isna()]
    if len(unread):
        raise ValueError(
            f"Dates of an EIOPA register that are not dates: "
            f"{sorted(set(unread))[:5]}"
        )
    return dates.dt.strftime("%Y-%m-%d").astype("string")


def read_eiopa_register(path, source):
    """One of EIOPA's registers as a frame of text, its dates as ``YYYY-MM-DD``.

    The register is read as the download stores it, the CSV export -- text in
    UTF-8, quoted and separated by semicolons -- or as a workbook stored by
    hand. Every column is text either way: the identifiers, the LEIs and the
    postcodes are not numbers, and a workbook mixes numbers, dates and text
    in a column -- a commercial name that is a date, a street number that is
    one -- which pyarrow refuses to write. The CSV export ends every line with
    a separator, which reads as an empty column without a name; it is
    dropped.

    Parameters
    ----------
    path: str
        The file in the landing zone.
    source: str
        ``"EIOPA_IC"`` or ``"EIOPA_IORP"``, which says the columns that hold
        dates, see :data:`EIOPA_DATES`.
    """
    if path.lower().endswith(".csv"):
        raw = pandas.read_csv(path, sep=";", encoding="utf-8", dtype="string",
                              keep_default_na=False, na_values=[""])
        unnamed = [c for c in raw.columns
                   if c.startswith("Unnamed:") and raw[c].isna().all()]
        raw = raw.drop(columns=unnamed)
    else:
        raw = pandas.read_excel(path, sheet_name=0, dtype=object)
    for column in EIOPA_DATES[source]:
        raw[column] = eiopa_dates(raw[column])
    for column in raw.columns:
        if raw[column].dtype != "string":
            raw[column] = raw[column].map(
                lambda v: v if v is None or v != v else str(v)).astype("string")
    return raw


def update_bronze_eiopa_insurance_undertaking(config_path):
    """Update EIOPA's list of insurance undertakings in the Bronze layer.

    Imports the lists of insurance undertakings published on EIOPA's website
    from the landing zone into the Bronze layer, as the download stores them
    or as workbooks stored by hand; see :func:`read_eiopa_register`.

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
    return import_registers(
        common.path_landing_eiopa_insurance_undertaking(config_path),
        common.path_bronze_eiopa_insurance_undertaking(config_path),
        EXCEL_EXTENSIONS + CSV_EXTENSIONS,
        lambda path: read_eiopa_register(path, "EIOPA_IC"),
        reference_date_from_file_name, dated_by_name, "EIOPA_IC",
    )


def update_bronze_eiopa_iorp(config_path):
    """Update EIOPA's list of institutions for occupational retirement provision in the Bronze layer.

    Imports the lists of institutions for occupational retirement provision
    published on EIOPA's website from the landing zone into the Bronze layer,
    as the download stores them or as workbooks stored by hand; see
    :func:`read_eiopa_register`.

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
    return import_registers(
        common.path_landing_eiopa_iorp(config_path),
        common.path_bronze_eiopa_iorp(config_path),
        EXCEL_EXTENSIONS + CSV_EXTENSIONS,
        lambda path: read_eiopa_register(path, "EIOPA_IORP"),
        reference_date_from_file_name, dated_by_name, "EIOPA_IORP",
    )


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

    raw = pandas.read_csv(path_i, usecols=["LEI", "SECTOR", "SUBSECTOR"], **CSV_AS_TEXT)

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
    downloads are not part of this; see
    :mod:`fincommons.entitymasterdata.landing`.

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
    imported += update_bronze_ecb_fvc(config_path)
    imported += update_bronze_ecb_psri(config_path)

    # EIOPA data
    imported += update_bronze_eiopa_insurance_undertaking(config_path)
    imported += update_bronze_eiopa_iorp(config_path)

    # ESMA data
    imported += update_bronze_esma_ccp(config_path)

    outcome["imported"] = imported


#: What this module offers. Importing the registers of the landing zone into
#: the Bronze layer, one file per reference date.
__all__ = [
    "CSV_AS_TEXT",
    "EXCEL_EXTENSIONS",
    "CSV_EXTENSIONS",
    "EIOPA_DATES",
    "dated_by_name",
    "dated_by_period",
    "eiopa_dates",
    "end_of_investment_fund_period",
    "end_of_period",
    "import_registers",
    "landing_files",
    "read_as_text",
    "read_eiopa_register",
    "reference_date_from_file_name",
    "update_bronze_ecb_fvc",
    "update_bronze_ecb_investment_fund",
    "update_bronze_ecb_mfi",
    "update_bronze_ecb_psri",
    "update_bronze_eiopa_insurance_undertaking",
    "update_bronze_eiopa_iorp",
    "update_bronze_esma_ccp",
    "update_bronze_gleif_level_1",
    "update_bronze_gleif_level_2",
    "update_from_landing_to_bronze",
    "write_parquet",
]
