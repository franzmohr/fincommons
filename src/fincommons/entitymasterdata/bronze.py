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

import datetime
import logging
import os
import re
import shutil
import zipfile

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
    which is the spelling the ``REFDT`` column carries.

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


def registers_to_import(path_landing, extensions, reference_date, bronze_name):
    """The files of a landing folder with their reference dates, clashes refused.

    Two files that would be stored under the same Bronze name are refused
    before anything is imported, because the second would silently replace the
    first: a monthly and a quarterly list of investment funds that end on the
    same day, for instance.

    Both importers go through this, so that the one that reads a register with
    Spark refuses the same files as the one that reads it with pandas.
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
    return files


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
    files = registers_to_import(path_landing, extensions, reference_date, bronze_name)

    for name, refdate in files:
        path_i = os.path.join(path_landing, name)
        raw = read(path_i)
        raw['REFDT'] = refdate

        write_parquet(raw, os.path.join(path_bronze, bronze_name(name, refdate) + ".parquet"))
        os.remove(path_i)

        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source=source,
                          file=name, refdt=refdate, rows=len(raw)),
        )

    return len(files)


def import_registers_with_spark(spark, path_landing, path_bronze, extensions,
                                columns, reference_date, bronze_name, source):
    """Import zipped csv registers through Spark, for files too large to hold at once.

    The golden copies of GLEIF are what this exists for. The Level 1 file is
    487 MB zipped and 4.74 GB as csv over some two hundred columns, and reading
    it into one pandas frame takes more memory than a small machine has:
    measured on a Raspberry Pi of 8 GB, where the kernel killed the import at
    7.5 GB resident. Spark reads the csv in splits and writes the parquet
    without ever holding the file, so what a run needs no longer follows the
    size of the register.

    Spark cannot read a zip, so the csv member is extracted beside the Bronze
    folder first, and deliberately not into the system temporary directory:
    there /tmp is often a tmpfs, and extracting into it would put the 4.74 GB
    back into the memory this function exists to save.

    Every column is read as text, as the pandas path reads them. The registers
    are text as far as they are concerned, and a reader left to infer types
    would make ``NA``, the country code of Namibia, into a missing value. Only
    the empty field is missing, which is what ``nullValue`` says here and what
    :data:`CSV_AS_TEXT` says there.

    The Bronze entry is a parquet directory rather than a single file, which is
    what a Spark write produces. :func:`fincommons.entitymasterdata.silver.latest_bronze_file`
    picks it up either way, since it selects on the name and the name still
    ends in ``.parquet``, and the directory is written under another name and
    renamed once complete, so an import that dies half-way leaves nothing the
    Silver layer would read.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    path_landing: str
        Folder of the landing zone the files are read from.
    path_bronze: str
        Folder of the Bronze layer they are written to.
    extensions: tuple of str
        The file extensions to import, see :func:`landing_files`.
    columns: dict
        The csv columns to keep, mapped to the names the Bronze layer gives
        them. Only these are read; the golden copy carries some two hundred.
    reference_date: callable
        Takes the name of a file and returns its reference date, ``YYYY-MM-DD``.
    bronze_name: callable
        Takes the name of a file and its reference date and returns the name of
        the Bronze entry, without the extension.
    source: str
        Name of the register, for the log.

    Returns
    -------
    int
        Number of files imported.
    """
    from pyspark.sql import functions as func

    files = registers_to_import(path_landing, extensions, reference_date, bronze_name)

    for name, refdate in files:
        path_i = os.path.join(path_landing, name)
        target = os.path.join(path_bronze, bronze_name(name, refdate) + ".parquet")
        unzipped = target + ".unzip"
        partial = target + ".partial"

        try:
            with zipfile.ZipFile(path_i) as archive:
                members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
                if len(members) != 1:
                    raise ValueError(
                        f"The archive {name!r} holds {len(members)} csv members, "
                        f"and one is expected: {members}"
                    )
                shutil.rmtree(unzipped, ignore_errors=True)
                os.makedirs(unzipped, exist_ok=True)
                archive.extract(members[0], unzipped)

            raw = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .option("nullValue", "")
                # Both of the next two are needed, and neither is enough. The
                # format doubles a quote inside a quoted field, so the escape
                # has to be the quote; but with that alone an empty field is
                # ambiguous -- `""` is either an empty value or an escaped
                # quote -- and the reader takes the second reading and
                # swallows the delimiters that follow. `multiLine` puts it on
                # the parser that resolves that correctly. The cost is that
                # the file is then read by one task rather than split across
                # the cores.
                #
                # Measured against pandas over the GLEIF Level 1 copy of
                # 18 November 2026, 3,498,374 rows, counting the countries
                # that are not two letters, of which pandas finds none:
                #
                #   default escape                        1394 wrong
                #   escape=quote                            19 wrong
                #   multiLine alone                       1394 wrong
                #   escape=quote and multiLine               0
                #
                # charToEscapeQuoteEscaping and the three
                # unescapedQuoteHandling settings leave the 19 in place, so
                # this pairing is the one that reproduces what the pandas
                # path read before it.
                .option("escape", chr(34))
                .option("multiLine", "true")
                .csv(os.path.join(unzipped, members[0]))
            )
            absent = [column for column in columns if column not in raw.columns]
            if absent:
                raise ValueError(
                    f"The register {name!r} carries no column {absent}, so the "
                    "Bronze layer cannot be built from it."
                )
            frame = raw.select(*[
                func.col(f"`{old}`").alias(new) for old, new in columns.items()
            ]).withColumn("REFDT", func.lit(refdate))

            shutil.rmtree(partial, ignore_errors=True)
            frame.write.mode("overwrite").parquet(partial)
            if os.path.exists(target):
                shutil.rmtree(target)
            os.rename(partial, target)
        finally:
            shutil.rmtree(unzipped, ignore_errors=True)
            shutil.rmtree(partial, ignore_errors=True)

        # Counted off the parquet that was written, which answers from its
        # footers, rather than from the frame, which would read the csv a
        # second time to say how many rows it had.
        rows = spark.read.parquet(target).count()
        os.remove(path_i)

        logger.info(
            "Imported register into the Bronze layer",
            extra=context(module="entitymasterdata", layer="bronze", source=source,
                          file=name, refdt=refdate, rows=rows),
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


#: The columns of the GLEIF Level 1 golden copy the entity master data set
#: draws on, as the Bronze layer names them. The copy carries some two hundred
#: and the rest is never looked at.
GLEIF_LEVEL_1_COLUMNS = {
    "LEI": "LEI",
    "Entity.LegalName": "NAME",
    "Entity.LegalAddress.Country": "CTRY",
}


def update_bronze_gleif_level_1(config_path, spark = None):
    """Update GLEIF Level 1 data in the Bronze layer.

    Imports the zipped csv files holding GLEIF Level 1 data from the landing
    zone into the Bronze layer. Only the three columns of
    :data:`GLEIF_LEVEL_1_COLUMNS` are read.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    spark: pyspark.sql.session.SparkSession, optional
        Session the golden copy is read with. Where it is not given the file
        is read into one pandas frame instead. **The golden copy is 4.74 GB as csv, and reading
        it in one frame needs more memory than a small machine has**: on a
        machine of 8 GB the kernel kills the import. A session therefore
        decides whether this stage is portable, which is why
        :func:`update_from_landing_to_bronze` passes one down; see
        :func:`import_registers_with_spark`.

    Returns
    -------
    int
        Number of GLEIF Level 1 files imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    if spark is not None:
        return import_registers_with_spark(
            spark,
            common.path_landing_gleif_level_1(config_path),
            common.path_bronze_gleif_level_1(config_path),
            (".zip",), GLEIF_LEVEL_1_COLUMNS,
            reference_date_from_file_name, dated_by_name, "GLEIF_L1",
        )

    def read(path):
        return pandas.read_csv(
            path,
            compression = "zip",
            usecols = list(GLEIF_LEVEL_1_COLUMNS),
            **CSV_AS_TEXT,
        ).rename(columns = GLEIF_LEVEL_1_COLUMNS)

    return import_registers(
        common.path_landing_gleif_level_1(config_path),
        common.path_bronze_gleif_level_1(config_path),
        (".zip",), read, reference_date_from_file_name, dated_by_name, "GLEIF_L1",
    )


#: The columns of the GLEIF Level 2 golden copy, i.e. of the reported
#: relationships between legal entities. The Bronze layer keeps their own
#: names, which the Silver layer reads by.
GLEIF_LEVEL_2_COLUMNS = {
    name: name for name in (
        "Relationship.StartNode.NodeID", "Relationship.StartNode.NodeIDType",
        "Relationship.EndNode.NodeID", "Relationship.EndNode.NodeIDType",
        "Relationship.RelationshipType", "Relationship.RelationshipStatus",
    )
}


def update_bronze_gleif_level_2(config_path, spark = None):
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
    if spark is not None:
        return import_registers_with_spark(
            spark,
            common.path_landing_gleif_level_2(config_path),
            common.path_bronze_gleif_level_2(config_path),
            (".zip",), GLEIF_LEVEL_2_COLUMNS,
            reference_date_from_file_name, dated_by_name, "GLEIF_L2",
        )

    def read(path):
        return pandas.read_csv(
            path,
            compression = "zip",
            usecols = list(GLEIF_LEVEL_2_COLUMNS),
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

    Imports the lists of central counterparties the landing zone holds into the
    Bronze layer, one file per day of download, as for EIOPA's registers; see
    :func:`fincommons.entitymasterdata.landing.download_esma_ccp`. ESMA
    publishes only the current list, so the lists kept are the only record of
    how it changed.

    A list stored under the undated name ``ccp_list`` by an earlier version of
    the module is dated first, see :func:`date_undated_ccp_lists`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    int
        Number of lists of CCPs imported. The closing record of
        :func:`update_from_landing_to_bronze` states the total.
    """
    date_undated_ccp_lists(config_path)
    path_landing = common.path_landing_esma_ccp(config_path)
    # The list is downloaded rather than placed by hand, so a landing zone
    # without it means the download has not been run rather than that a file is
    # broken. The other registers import nothing from an empty folder without
    # comment; this one says so, because the list reaching the Bronze layer is
    # what a run of the Silver layer goes on to depend on.
    if not landing_files(path_landing, CSV_EXTENSIONS):
        logger.warning(
            "Nothing to import, the register is not in the landing zone",
            extra=context(module="entitymasterdata", layer="bronze", source="ESMA_CCP",
                          file=path_landing),
        )
        return 0
    return import_registers(
        path_landing,
        common.path_bronze_esma_ccp(config_path),
        CSV_EXTENSIONS,
        lambda path: pandas.read_csv(
            path, usecols=["LEI", "SECTOR", "SUBSECTOR"], **CSV_AS_TEXT
        ),
        reference_date_from_file_name, dated_by_name, "ESMA_CCP",
    )


#: The name an earlier version of the module stored the list of CCPs under,
#: in the landing zone and in the Bronze layer, without a date.
UNDATED_CCP_LIST = "ccp_list"


def date_undated_ccp_lists(config_path):
    """Give a list of CCPs stored without a date the day it was last written.

    Before the list was kept per day it was stored as ``ccp_list.csv`` and
    imported as ``ccp_list.parquet``, each replacing the one before. The day
    the file was last written is the day of the download it holds, so it is
    renamed to that day, ``YYYYMMDD``, and joins the lists kept per day rather
    than being lost or, since ``c`` sorts after every digit, being taken for
    the latest list. A dated list of the same day already there is the newer
    of the two, and the undated one is refused rather than either being
    dropped.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.

    Returns
    -------
    list of tuple
        The files renamed, as pairs of old and new path.
    """
    renamed = []
    for folder, extension in (
        (common.path_landing_esma_ccp(config_path), ".csv"),
        (common.path_bronze_esma_ccp(config_path), ".parquet"),
    ):
        old = os.path.join(folder, UNDATED_CCP_LIST + extension)
        if not os.path.isfile(old):
            continue
        day = datetime.date.fromtimestamp(os.path.getmtime(old))
        new = os.path.join(folder, f"{day:%Y%m%d}{extension}")
        if os.path.exists(new):
            raise FileExistsError(
                f"{old!r} has no date and {new!r} holds the list of the day it "
                "was written. Remove the one that is not wanted."
            )
        os.rename(old, new)
        renamed.append((old, new))
        logger.warning(
            "Dated a list of CCPs stored without a date by the day it was written",
            extra=context(module="entitymasterdata", source="ESMA_CCP",
                          file=os.path.basename(old), renamed=os.path.basename(new)),
        )
    return renamed


def update_from_landing_to_bronze(config_path, spark = None):
    """Entity master data: import the registers into the Bronze layer.

    Imports everything the landing zone currently holds, source by source. The
    downloads are not part of this; see
    :mod:`fincommons.entitymasterdata.landing`.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    spark: pyspark.sql.session.SparkSession, optional
        Session the GLEIF golden copies are read with; the other registers are
        small enough to be read without one. Where it is not given they are
        read into one pandas frame instead, which a machine of 8 GB cannot do
        for the Level 1 copy -- so a session is what makes this stage portable.
        It is asked for rather than taken from whatever session happens to be
        active: no module of this package opens a session, and a stage whose
        reader depended on one another stage had left behind would behave
        differently for reasons its caller cannot see.

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
        _update_from_landing_to_bronze(config_path, outcome, spark)


def _update_from_landing_to_bronze(config_path, outcome, spark = None):
    """Do the work of :func:`update_from_landing_to_bronze`.

    Each of the imports reports how many files it read, so that the closing
    record of the stage states what the run actually brought in. A run that
    imported nothing at all, because the landing zone was never filled, is
    otherwise indistinguishable from one that imported every register.
    """
    imported = 0

    # GLEIF data
    imported += update_bronze_gleif_level_1(config_path, spark)
    imported += update_bronze_gleif_level_2(config_path, spark)

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
    "UNDATED_CCP_LIST",
    "dated_by_name",
    "dated_by_period",
    "date_undated_ccp_lists",
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
