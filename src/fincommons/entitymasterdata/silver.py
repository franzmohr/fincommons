"""Entity master data: the combined table in the Silver layer.

GLEIF says who a legal entity is; the registers of the ECB, EIOPA and ESMA say
what it does. The Silver layer puts the two together, in the manner of Lenoci
and Letizia (2021): the LEI is the key, GLEIF Level 1 supplies the name and the
country, the sectoral registers supply the sector, and GLEIF Level 2 supplies
the ultimate parent and, for a fund, the company that manages it. Only a
relationship the golden copy still marks as active is read, because the file
keeps the ones that have ended alongside them.

It is built in two steps. ``update_silver_entitymasterdata_public`` combines the
public sources alone, which is the table an organisation without a list of its
own would already query; ``update_silver_entitymasterdata`` puts a proprietary
list in front of it, so that an entity the organisation itself maintains is
described the way the organisation describes it. A rebuild replaces both
tables rather than adding to them. Where the configuration file addresses the
Silver layer by catalogue name they are Delta tables. Where it addresses it by
path they are written as parquet unless ``file_format`` asks for Delta, which
is worth asking for where a reader may be querying the table while it is
rebuilt: a Delta overwrite is one transaction, while a parquet overwrite
deletes the table before it writes the new one.
"""

import logging
import os
import zipfile

import pyspark.sql.functions as func
import requests
from pyspark.sql import Window

from . import common
from ..log import context, pipeline_run
from .bronze import *
from .landing import *

logger = logging.getLogger(__name__)


#: The columns of the entity master data set, in the order they are stored in.
COLUMNS = ["id", "lei",
           "internal_id_1", "internal_id_2", "internal_id_3", "internal_id_4",
           "mic", "name", "country", "sector", "subsector", "ultimate_parent_lei", "amc",
           "if_policy", "if_type", "if_ucits_compl"]

#: What a column of a Bronze register, or of the proprietary list, is called
#: in the Silver table. A Bronze table records what a publisher sent and keeps
#: the names it sent, so the Silver names are taken on where Bronze is read;
#: see :func:`with_silver_names`.
SILVER_NAMES = {
    "LEI": "lei", "NAME": "name", "CTRY": "country", "REFDT": "ref_date",
    "SECTOR": "sector", "SUBSECTOR": "subsector",
    "ULTPAR_LEI": "ultimate_parent_lei", "AMC": "amc",
    "IF_POLICY": "if_policy", "IF_TYPE": "if_type",
    "IF_UCITS_COMPL": "if_ucits_compl",
    "INTERNALID_1": "internal_id_1", "INTERNALID_2": "internal_id_2",
    "INTERNALID_3": "internal_id_3", "INTERNALID_4": "internal_id_4",
}

#: The columns a proprietary list contributes that the public sources do not.
#: The list is an input kept by hand, so it carries the upper-case names it
#: always has: it is read into the Silver names rather than written in them,
#: which is why these no longer follow :data:`COLUMNS`.
INTERNAL_ID_COLUMNS = ["INTERNALID_1", "INTERNALID_2", "INTERNALID_3",
                       "INTERNALID_4"]

#: The same four columns as the Silver table names them. The proprietary list
#: is held to :data:`INTERNAL_ID_COLUMNS`, which is what its header says; the
#: columns a frame is given are these, because by then it has been read into
#: the Silver names.
INTERNAL_ID_SILVER = [SILVER_NAMES[column] for column in INTERNAL_ID_COLUMNS]

#: The columns a proprietary list may carry: those of the entity master data
#: set except ``id``, which the update assigns. ``LEI`` is the one it must.
INTERNAL_COLUMNS = ["LEI"] + INTERNAL_ID_COLUMNS + [
    "NAME", "CTRY", "SECTOR", "SUBSECTOR", "ULTPAR_LEI", "AMC",
    "IF_POLICY", "IF_TYPE", "IF_UCITS_COMPL"]

#: What a well-formed LEI looks like after :func:`normalised_lei`: eighteen
#: letters or digits and two check digits (ISO 17442).
LEI_PATTERN = "^[A-Z0-9]{18}[0-9]{2}$"

#: The LEI an aggregation files everything under that it does not show on its
#: own, such as the reporting counterparties below the top fifteen.
LEI_OTHER = "XOTHERX0000000000064"

#: The LEI to file an entity under that could not be identified at all.
LEI_UNKNOWN = "XUNKNOWNX00000000025"

#: The entities the entity master data set carries that no register does, as
#: ``LEI: NAME``. They are there so that the two LEIs above resolve to a name
#: like any other, rather than dropping out of a join or leaving a legend with
#: an empty entry.
#:
#: Both LEIs have the form ISO 17442 gives an LEI, check digits included --
#: the part before them is padded with zeros to eighteen characters, and the
#: check digits make the whole number leave a remainder of 1 on division by
#: 97 -- so they pass wherever an LEI is validated, this module's own
#: :data:`LEI_PATTERN` among the checks, which a placeholder such as
#: ``XOTHERX`` would pass none of. The first four characters of an LEI
#: name the Local Operating Unit that issued it, and none has issued one
#: under ``XOTH`` or ``XUNK``: of the 3,424,918 LEIs of the GLEIF golden copy
#: of 8 September 2026, 521 begin with an ``X`` and none with either prefix.
#: That is a finding rather than a guarantee. Should a real entity ever be
#: given one of the two, the public row would win and the catch-all entity
#: would be gone from the table.
CATCH_ALL_ENTITIES = {LEI_OTHER: "Other", LEI_UNKNOWN: "Unknown"}

#: The sector each category of the ECB's list of MFIs stands for.
MFI_SECTORS = {
    "Central Bank": "CB",
    "Credit Institution": "CDTI",
    "Other Institution": "OFI",
    "Money Market Fund": "MMF",
}

#: The sector an institution on the list of MFIs is described by where its
#: category is not one :data:`MFI_SECTORS` covers.
#:
#: The register is downloaded and converted by hand, and a category the ECB
#: relabels, or a conversion that alters the spelling, would otherwise leave
#: the sector empty. An empty sector is worse than a coarse one here, because
#: the list of MFIs takes precedence over the list of investment funds: the row
#: that carries it wins, and an institution that also stands on the fund
#: register loses the description that register would have given it. Being on
#: the list of MFIs at all is a narrower statement than being a fund, so it is
#: the one that is recorded.
MFI_SECTOR_OTHER = "MFI"

#: The status a relationship of the GLEIF Level 2 data has to carry to be read
#: as current. The golden copy keeps a relationship after it has ended and
#: marks it ``INACTIVE``; an entity whose parent changed therefore stands in
#: two ``IS_ULTIMATELY_CONSOLIDATED_BY`` records, only one of which says who
#: consolidates it today.
ACTIVE_RELATIONSHIP = "ACTIVE"


def latest_bronze_file(folder):
    """Return the path of the most recent file in a folder of the Bronze layer.

    The registers are stored one file per reference date, named after that
    date, so the most recent one is the last name in order. An empty folder is
    reported as such rather than left to fail somewhere further down, because
    the message that a register has never been imported is the one worth
    reading.

    Parameters
    ----------
    folder: str
        Folder of the Bronze layer that holds one file per reference date.
    """
    names = sorted(name for name in os.listdir(folder) if name.endswith(".parquet"))
    if len(names) == 0:
        raise FileNotFoundError(
            f"No parquet file in {folder!r}. The Bronze layer has to be updated "
            "before the Silver layer can be built."
        )
    return os.path.join(folder, names[-1])


def text_columns(data, columns):
    """Select ``columns`` from ``data``, each of them as text.

    The registers reach the Bronze layer with whatever type the file they came
    in suggested, and a code that happens to be all digits in one release of a
    register and not in the next would otherwise arrive as a number one time
    and as text the other. Spelling the type out keeps the Silver table the
    same shape whatever the registers happened to hold, and keeps the unions
    and joins below off Spark's rules for reconciling two types.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        The data frame to select from.
    columns: list
        Names of the columns to keep.
    """
    return data.select(*[func.col(column).cast("string").alias(column) for column in columns])


def with_internal_id_columns(data):
    """Add the proprietary identifier columns a data frame does not carry.

    A proprietary list is not obliged to fill all four identifier columns, and
    a data lake without such a list carries none of them at all. The missing
    ones are added as empty text, so that the entity master data set has the
    same shape either way and a caller can select an identifier column without
    knowing what the organisation happens to keep.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        The data frame the columns are added to.
    """
    for column in INTERNAL_ID_SILVER:
        if column not in data.columns:
            data = data.withColumn(column, func.lit(None).cast("string"))
    return data


def normalised_lei(column):
    """Return the expression that writes an LEI the way GLEIF writes it.

    The registers are joined to GLEIF on the LEI as text, so an LEI a register
    spells differently is a different key: it matches nothing, and the entity
    it stands for is left without the sector that register gives it. The
    registers of the ECB and EIOPA are converted from spreadsheets by hand and
    the list of CCPs is typed by ESMA, and both kinds of slip occur in them:
    the list of CCPs of September 2026 carried six LEIs with a trailing space,
    which left KDPW_CCP and Euronext Clearing Athens without a sector, and the
    IORP register three in lower case, which did the same to three pension
    funds. An LEI holds neither blanks nor lower-case letters, so removing the
    one and raising the other changes no LEI that was well-formed. What is
    left empty becomes null, the absence of an LEI rather than a key.

    Parameters
    ----------
    column: str
        Name of the column holding the LEI.
    """
    # \p{Z} catches the non-breaking space a spreadsheet leaves, which \s
    # does not.
    cleaned = func.upper(
        func.regexp_replace(func.col(column).cast("string"), r"[\s\p{Z}]", "")
    )
    return func.when(cleaned != "", cleaned)


def with_normalised_lei(data, source, columns = ("lei",), lei="lei"):
    """Normalise the LEI columns of a register and log the ones still malformed.

    See :func:`normalised_lei` for what is normalised. An LEI that does not
    look like one even then is kept -- it may yet match a row of the same
    register elsewhere, and dropping it would decide more than the module
    knows -- but it is named in a warning with the register it came from,
    because it will not match GLEIF.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        The register.
    source: str
        Name of the register, for the log.
    columns: tuple
        The columns that hold an LEI. Only ``lei`` itself is checked.
    """
    for column in columns:
        if column in data.columns:
            data = data.withColumn(column, normalised_lei(column))
    warn_of_malformed_leis(data, source, lei)
    return data


def warn_of_malformed_leis(data, source, lei="lei"):
    """Log the LEIs of a register that do not match :data:`LEI_PATTERN`.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        A data frame with a normalised LEI column.
    source: str
        Name of the register, for the log.
    lei: str
        Name of that column. It is the Silver name, except for the
        proprietary list, which is checked before it is read into the Silver
        names so that a duplicate is found on the normalised value.
    """
    # The count and the examples are taken in one pass over the register,
    # which for the GLEIF golden copy is some millions of rows.
    found = data.filter(
        func.col(lei).isNotNull() & ~func.col(lei).rlike(LEI_PATTERN)
    ).agg(
        func.countDistinct(lei).alias("_count"),
        func.sort_array(func.collect_set(lei)).alias("_leis"),
    ).first()
    if not found["_count"]:
        return
    logger.warning(
        "LEIs of the register are malformed and will not match GLEIF",
        extra=context(module="entitymasterdata", layer="silver", source=source,
                      count=found["_count"], leis="|".join(found["_leis"][:5])),
    )


def check_internal_list(internal, source):
    """Refuse a proprietary list the combination would misread.

    The list is the one source whose shape the module does not learn from a
    publisher, and read by its header it is also the one whose columns are
    whatever the file says. Two things went wrong silently before. A column
    the entity master data set does not have -- ``Sector`` for ``SECTOR``, say
    -- was dropped when the table was written, and with it everything the list
    said under it. And an LEI the list carried twice left the choice between
    the two rows to the order Spark read them in wherever their
    ``INTERNALID_1`` agreed, which is not the organisation's choice to have
    made. Both are refused, and so is a list without an ``LEI`` column, which
    would otherwise displace nothing.

    Parameters
    ----------
    internal: pyspark.sql.DataFrame
        The proprietary list, its LEIs normalised.
    source: str
        Where it was read from, for the message.

    Raises
    ------
    ValueError
        Naming each problem found.
    """
    found = []
    if "LEI" not in internal.columns:
        found.append("no LEI column")
    unknown = [column for column in internal.columns if column not in INTERNAL_COLUMNS]
    if unknown:
        found.append(
            f"columns the entity master data set does not have, {unknown} "
            f"(it has {INTERNAL_COLUMNS})"
        )
    if not found:
        duplicated = internal.filter(func.col("LEI").isNotNull()) \
            .groupBy("LEI").count().filter(func.col("count") > 1)
        count = duplicated.count()
        if count:
            examples = sorted(row["LEI"] for row in duplicated.limit(3).collect())
            found.append(f"{count} LEIs described more than once, e.g. {examples}")
    if found:
        raise ValueError(
            f"The proprietary entity master data in {source} cannot be combined "
            f"as it is: " + "; ".join(found) + "."
        )


def with_silver_names(data):
    """Rename the Bronze columns of ``data`` to the Silver names.

    Whichever of :data:`SILVER_NAMES` the frame carries is renamed and the
    rest is left alone, so the same call serves a register that holds only an
    LEI and the GLEIF file that holds four of them. This is the one place the
    two layers meet: everything before it is spelled as the publisher spelled
    it, everything after it as the Silver table does.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        A frame read from the Bronze layer, or the proprietary list.
    """
    for bronze in SILVER_NAMES:
        if bronze in data.columns:
            data = data.withColumnRenamed(bronze, SILVER_NAMES[bronze])
    return data


def read_internal_list(spark, path):
    """Read, normalise and check the proprietary list.

    Every column is read as text. The proprietary identifiers are codes rather
    than numbers, and a code that happens to be all digits must not lose its
    leading zeros on the way in. The columns are taken from the header rather
    than declared by position, because the identifier columns a list does not
    use may be left out of it; :func:`check_internal_list` holds the header to
    :data:`INTERNAL_COLUMNS` instead.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    path: str
        Path of the proprietary list.
    """
    internal = spark.read.options(header="True", inferSchema="False").csv(path)
    internal = with_normalised_lei(
        internal, "INTERNAL", columns=("LEI", "ULTPAR_LEI", "AMC"), lei="LEI"
    ) if "LEI" in internal.columns else internal
    check_internal_list(internal, path)
    return with_internal_id_columns(with_silver_names(internal))


def first_per_lei(data, order):
    """Keep one row per LEI, the first under ``order``.

    Two rules make the result of this repeatable, and a caller has to keep to
    both of them, because Spark reports no error where they are broken -- it
    simply answers with one of the rows.

    ``order`` has to distinguish every pair of rows a partition can hold. Where
    two rows of an LEI are equal under it the row kept is whichever Spark read
    first, which depends on how the file was partitioned and not on what the
    register says, so the table can differ between two runs over identical
    input. Ordering by a column no one asked about is not arbitrary in the way
    that matters: it is arbitrary once, in the code, rather than afresh on
    every run.

    Nulls belong last, which is the opposite of what ``orderBy`` does on its
    own. A null is the absence of a statement, so a row that says something
    should be preferred to one that does not.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        A data frame with an ``lei`` column.
    order: list
        The columns the rows of one LEI are ordered by. The first of them is
        kept.
    """
    window_spec = Window.partitionBy("lei").orderBy(*order)
    return data \
        .withColumn("_rn", func.row_number().over(window_spec)) \
        .filter(func.col("_rn") == 1) \
        .drop("_rn")


def active_relationship(data, relationship_type, column):
    """Return the counterpart of every entity in a current relationship.

    The GLEIF Level 2 data reports the relationships between legal entities,
    each of them from the entity it concerns to the entity it points at. The
    golden copy is a record of what has been reported rather than of what holds
    today: a relationship that has ended stays in the file and is marked
    inactive, so an entity whose parent changed stands in two records of the
    same type. Only the one marked :data:`ACTIVE_RELATIONSHIP` says who the
    parent is now, and an entity whose only record is an inactive one is not in
    the relationship at all any more and is left without a counterpart.

    An entity reports at most one current relationship of a kind, so the rows
    that remain are already one per LEI in a well-formed file. They are ordered
    by the counterpart and the first is kept all the same, which makes a file
    that does carry two answer the same way every time rather than with
    whichever row Spark happened to read first.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        The GLEIF Level 2 data as the Bronze layer holds it.
    relationship_type: str
        The value of ``Relationship.RelationshipType`` to select, such as
        ``'IS_ULTIMATELY_CONSOLIDATED_BY'``.
    column: str
        Name the counterpart is returned under, such as ``'ultimate_parent_lei'``.

    Returns
    -------
    pyspark.sql.DataFrame
        A frame with an ``lei`` column and ``column``, one row per LEI.
    """
    related = data \
        .filter(func.col("`Relationship.RelationshipType`") == relationship_type) \
        .filter(func.col("`Relationship.RelationshipStatus`") == ACTIVE_RELATIONSHIP) \
        .withColumnRenamed("Relationship.StartNode.NodeID", "lei") \
        .withColumnRenamed("Relationship.EndNode.NodeID", column) \
        .transform(lambda frame: text_columns(frame, ["lei", column])) \
        .withColumn("lei", normalised_lei("lei")) \
        .withColumn(column, normalised_lei(column))
    return first_per_lei(related, [func.col(column).asc_nulls_last()])


def mfi_sector():
    """Return the expression that reads the sector off an MFI's category.

    The categories the register is known to use are mapped to the vocabulary of
    the entity master data set and anything else to :data:`MFI_SECTOR_OTHER`,
    so that an institution on the register is never left without a sector. See
    :data:`MFI_SECTOR_OTHER` for why the fallback is not simply nothing.
    """
    expression = func
    for category, sector in MFI_SECTORS.items():
        expression = expression.when(func.col("CATEGORY") == category, sector)
    return expression.otherwise(func.lit(MFI_SECTOR_OTHER))


def warn_of_unknown_mfi_categories(data):
    """Log the categories of the MFI register that have no sector of their own.

    The run stays valid -- the institutions keep :data:`MFI_SECTOR_OTHER` -- but
    the sector is coarser than the register would allow, which is worth a
    record naming what was not understood rather than a table that is quietly
    less precise than the one before it.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        The list of MFIs as the Bronze layer holds it.
    """
    known = func.col("CATEGORY").isin(list(MFI_SECTORS))
    # A category that is missing altogether makes ``isin`` answer with null
    # rather than with false, so the comparison is resolved before it is
    # negated. Such a row is reported too: it is described no more precisely
    # than one whose category is spelled in a way the module does not know.
    unknown = data.filter(~func.coalesce(known, func.lit(False))) \
        .select("CATEGORY").distinct().collect()
    if not unknown:
        return
    categories = sorted(str(row["CATEGORY"]) for row in unknown)
    logger.warning(
        "Categories of the register have no sector of their own, the "
        "institutions carrying them are described as %s",
        MFI_SECTOR_OTHER,
        extra=context(module="entitymasterdata", layer="silver", source="ECB_MFI",
                      categories="|".join(categories)),
    )


def update_silver_entitymasterdata_public(spark, config_path, file_format = None):
    """Update the public entity master data in the Silver layer.

    Combines the most recent version of every public register in the Bronze
    layer into one row per LEI, and writes it to the Silver layer. The result
    is what ``update_silver_entitymasterdata`` puts a proprietary list in front
    of.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    file_format: str, optional
        Format the table is written in, ``'parquet'`` or ``'delta'``. Where it
        is left out the configuration file decides: a Silver layer addressed
        by catalogue name is written as Delta, and one addressed by path keeps
        the format its table is stored in, or is written as parquet where
        there is no table yet. Parquet asked of a catalogue table, or a format
        other than the one the table is stored in, is refused before anything
        is read. See :func:`fincommons.storage.write_silver` for what the
        choice amounts to.

    Examples
    --------
    >>> update_silver_entitymasterdata_public(spark, "path/to/configuration/file")
    """
    file_format = common.silver_format(common.ref_silver_public(config_path), file_format)
    with pipeline_run(
        logger,
        "update_silver_entitymasterdata_public",
        module="entitymasterdata",
        layer="silver",
        table="public",
        config_path=config_path,
        file_format=file_format,
    ) as outcome:
        _update_silver_entitymasterdata_public(
            spark, config_path, file_format, outcome
        )


# Reading one register
# --------------------------------------------------------------------------
#
# Each register is read from one file of the Bronze layer -- one snapshot of
# it -- into the rows the combination below works with, the LEI normalised and
# every column named as the Silver layer names it. A function here says what a
# register states and nothing about which register wins: that is decided once,
# in :func:`combine_registers`, so that the table built from the latest
# snapshots and a history built from every snapshot apply the same rules.


def normalise_gleif_level_1(data):
    """The entities of one GLEIF Level 1 golden copy: who an LEI is."""
    return with_normalised_lei(data.transform(with_silver_names), "GLEIF_L1")


def normalise_gleif_level_2(data):
    """The current relationships of one GLEIF Level 2 golden copy, one row per LEI.

    The ultimate parent and, for a fund, the asset management company, which
    are the same reading of the same file under different relationship types.
    Both go through :func:`active_relationship`, so that neither can be read
    without the status of the relationship being taken into account.
    """
    data = data.transform(with_silver_names)
    return active_relationship(
        data, "IS_ULTIMATELY_CONSOLIDATED_BY", "ultimate_parent_lei"
    ).join(
        active_relationship(data, "IS_FUND-MANAGED_BY", "amc"),
        on="lei", how="fullouter",
    )


def normalise_gleif_mic_lei(data):
    """The market identifier codes GLEIF links to each LEI, one row per LEI.

    GLEIF links every MIC (ISO 10383) to the LEI of the entity that operates
    the market, and an operator often runs several: a market segment carries
    a MIC of its own beside that of the market, and a group runs a market per
    asset class. Each LEI is therefore given the list of its MICs, sorted and
    without repeats, so that the table stays one row per LEI and the list is
    the same on every run. A MIC is written in upper case without blanks, as
    ISO 10383 writes it, and the LEI as GLEIF writes it, see
    :func:`normalised_lei`.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        The links of MICs to LEIs, in columns ``mic`` and ``lei``: the
        versions of the history of :mod:`fincommons.gleif.mic_lei` that hold
        on one day.

    Returns
    -------
    pyspark.sql.DataFrame
        A frame with an ``lei`` column and ``mic``, an array of text.
    """
    data = with_normalised_lei(text_columns(data, ["lei", "mic"]), "GLEIF_MIC")
    mic = func.upper(func.regexp_replace(func.col("mic"), r"[\s\p{Z}]", ""))
    return data.withColumn("mic", func.when(mic != "", mic)) \
        .filter(func.col("lei").isNotNull() & func.col("mic").isNotNull()) \
        .groupBy("lei") \
        .agg(func.array_sort(func.collect_set("mic")).alias("mic"))


#: The type of the ``mic`` column, for a table built without the MIC-to-LEI
#: history.
MIC_TYPE = "array<string>"


def normalise_ecb_investment_fund(data):
    """The funds of one ECB list of investment funds.

    Every fund has the sector ``IF``. The subsector says what the register
    states about UCITS compliance: ``UCIT`` for a UCITS, ``AIFD`` for an
    alternative investment fund and ``INVF`` for a fund the register reports
    neither about. The characteristics of the fund -- ``if_policy``,
    ``if_type`` and ``if_ucits_compl`` -- are what
    :func:`combine_registers` treats apart from the sector; the investment
    policy is in ``if_policy``.
    """
    return data.transform(with_silver_names) \
        .transform(lambda data: with_normalised_lei(data, "ECB_IF")) \
        .filter(func.col("lei").isNotNull()) \
        .withColumn("sector", func.lit("IF")) \
        .withColumn("subsector", func.when(func.col("UCITS compliance") == "Non-UCITS compliant, AIF", "AIFD") \
                                     .when(func.col("UCITS compliance") == "UCITS compliant", "UCIT") \
                                     .otherwise(func.lit("INVF"))) \
        .withColumn("if_policy", func.col("Investment policy")) \
        .withColumnRenamed("Type of investment fund", "if_type") \
        .withColumn("if_type", func.when(func.col("if_type") == "Exchange traded fund (ETF)", "ETF") \
                                   .when(func.col("if_type") == "Private equity fund (PEF)", "PEF") \
                                   .when(func.col("if_type") == "Investment fund other than an Exchange traded fund (ETF) and a Private equity fund (PEF)", "Neither ETF nor PEF") \
                                   .otherwise(func.col("if_type"))) \
        .withColumnRenamed("UCITS compliance", "if_ucits_compl") \
        .withColumn("if_ucits_compl", func.when(func.col("if_ucits_compl") == "Non-UCITS compliant, AIF", "AIF") \
                                          .when(func.col("if_ucits_compl") == "UCITS compliant", "UCITS") \
                                          .otherwise(func.col("if_ucits_compl"))) \
        .transform(lambda data: text_columns(
            data, ["lei", "sector", "subsector", "if_policy", "if_type", "if_ucits_compl"]
        ))


def normalise_ecb_mfi(data):
    """The institutions of one ECB list of MFIs, with the sector their category gives.

    The register is read before the sector is derived from it, so that a
    category the module does not know can be named in the log.
    """
    register = data.transform(with_silver_names) \
        .transform(lambda data: with_normalised_lei(data, "ECB_MFI")) \
        .filter(func.col("lei").isNotNull())
    warn_of_unknown_mfi_categories(register)
    return register \
        .withColumn("sector", mfi_sector()) \
        .transform(lambda data: text_columns(data, ["lei", "sector"]))


def normalise_eiopa_insurance_undertaking(data):
    """The undertakings of one EIOPA register of insurance undertakings."""
    return data.transform(with_silver_names) \
        .transform(lambda data: with_normalised_lei(data, "EIOPA_INUN")) \
        .filter(func.col("lei").isNotNull()) \
        .withColumn("sector", func.lit("INUN")) \
        .transform(lambda data: text_columns(data, ["lei", "sector"]))


def normalise_eiopa_iorp(data):
    """The institutions of one EIOPA register of IORPs."""
    return data.transform(with_silver_names) \
        .transform(lambda data: with_normalised_lei(data, "EIOPA_IORP")) \
        .filter(func.col("lei").isNotNull()) \
        .withColumn("sector", func.lit("ORPI")) \
        .transform(lambda data: text_columns(data, ["lei", "sector"]))


def normalise_esma_ccp(data):
    """The CCPs of one ESMA list of CCPs."""
    return data.transform(with_silver_names) \
        .transform(lambda data: with_normalised_lei(data, "ESMA_CCP")) \
        .filter(func.col("lei").isNotNull()) \
        .transform(lambda data: text_columns(data, ["lei", "sector", "subsector"]))


# Combining the registers
# --------------------------------------------------------------------------


def combine_registers(gleif_1, relationships, ccp, inun, orpi, mfi, invfund,
                      mic_lei=None):
    """Combine the registers into the public entity master data, one row per LEI.

    The arguments are the registers as the ``normalise_`` functions of this
    module return them, each from one snapshot. Which snapshots they are is the
    caller's business: :func:`update_silver_entitymasterdata_public` passes the
    latest of each.

    Parameters
    ----------
    gleif_1: pyspark.sql.DataFrame
        :func:`normalise_gleif_level_1`.
    relationships: pyspark.sql.DataFrame
        :func:`normalise_gleif_level_2`.
    ccp, inun, orpi, mfi, invfund: pyspark.sql.DataFrame
        :func:`normalise_esma_ccp`, :func:`normalise_eiopa_insurance_undertaking`,
        :func:`normalise_eiopa_iorp`, :func:`normalise_ecb_mfi` and
        :func:`normalise_ecb_investment_fund`.
    mic_lei: pyspark.sql.DataFrame, optional
        :func:`normalise_gleif_mic_lei`. Where it is left out ``mic`` is empty
        for every entity, so the table has the same shape either way.
    """
    # The fund register makes two different kinds of statement, and only one of
    # them is contested. The sector is a claim about what the entity is, which
    # the registers below argue over and the narrowest of them wins. The fund
    # characteristics are facts about a fund that no other register carries, so
    # there is nothing for them to lose to, and they are joined on separately
    # rather than travelling with the sector.
    #
    # Were they to travel with it, a fund that also stands on a register
    # ranked above the fund register would lose them: the sector is decided
    # by keeping one row per LEI, the row kept is the one the winning register
    # contributed, and that row has no such columns. A money market fund is the
    # case that matters, because it stands on the list of MFIs by definition.
    fund_characteristics = first_per_lei(
        invfund.select("lei", "if_policy", "if_type", "if_ucits_compl"),
        [func.col("if_ucits_compl").asc_nulls_last(),
         func.col("if_type").asc_nulls_last(),
         func.col("if_policy").asc_nulls_last()],
    )
    invfund = invfund.select("lei", "sector", "subsector")

    # An entity can appear in more than one register: a central counterparty is
    # licensed as a credit institution in several member states, and a fund can
    # be listed as an MFI where it is a money market fund. The narrower
    # description wins. The precedence is carried by an explicit rank rather
    # than by the order in which the rows of the union arrive, which is not
    # specified.
    # The two EIOPA registers sit next to one another: each names one specific
    # kind of supervised undertaking, which is a narrower statement than the
    # category an institution carries on the MFI list or the fact that it is a
    # fund.
    #
    # They do overlap, and the order between them therefore decides something.
    # A handful of entities stand in both, being insurance undertakings that
    # also run occupational pension business -- the Pensionskassen constituted
    # as insurers, and the institutions a French or Irish group runs for
    # retirement provision. Nine of them did so in the registers of September
    # 2026. The insurance register wins, because it states the authorisation
    # the entity holds, while the IORP register states an activity carried on
    # under it. An organisation that would rather see the activity should swap
    # the two ranks.
    sector = ccp.withColumn("_source_rank", func.lit(1)) \
        .unionByName(inun.withColumn("_source_rank", func.lit(2)), allowMissingColumns=True) \
        .unionByName(orpi.withColumn("_source_rank", func.lit(3)), allowMissingColumns=True) \
        .unionByName(mfi.withColumn("_source_rank", func.lit(4)), allowMissingColumns=True) \
        .unionByName(invfund.withColumn("_source_rank", func.lit(5)), allowMissingColumns=True)

    # The rank settles which register describes the entity. Within one register
    # it settles nothing, and a register can carry an LEI twice: a central
    # counterparty authorised under more than one heading stands in the list of
    # CCPs once per heading, and an institution licensed in several member
    # states stands in the list of MFIs once per state. So the order runs on
    # past the rank, through every remaining column of the frame -- which after
    # the fund characteristics were taken out of it is just the sector and the
    # subsector.
    #
    # That the order covers every column is the point, not a detail: the row
    # kept is then fully determined, and the table is the same over two runs on
    # the same registers. Where the order stopped at the sector, the subsector
    # of a doubly authorised CCP came from whichever row Spark read first,
    # which follows from how the file happened to be partitioned. Between two
    # rows that do state a subsector the alphabetical one wins, which decides
    # nothing anyone asked about -- but it decides it once, here, rather than
    # afresh on every run.
    sector = first_per_lei(
        sector,
        [func.col("_source_rank"),
         func.col("sector").asc_nulls_last(),
         func.col("subsector").asc_nulls_last()],
    ).drop("_source_rank")

    # Enrich GLEIF Level 1 data with sector information
    result = gleif_1.join(sector, on = "lei", how = "leftouter")

    # And with what the fund register says about a fund, whichever register
    # ended up describing it. SUBSECTOR is not among these columns: it is the
    # refinement the winning register offers, so it stays that register's
    # statement rather than being filled in from another taxonomy. For a fund
    # that wins nothing is lost by that, because the investment policy the
    # sector step would have put in SUBSECTOR is what IF_POLICY carries.
    result = result.join(fund_characteristics, on = "lei", how = "leftouter")

    # And with the ultimate parent and the asset management company GLEIF
    # Level 2 reports for it.
    result = result.join(relationships, on = "lei", how = "leftouter")

    # And with the markets it operates, which GLEIF's MIC-to-LEI mapping
    # names. The mapping is joined onto GLEIF Level 1 rather than added to it:
    # a MIC whose LEI the golden copy does not know names no entity of the
    # table.
    if mic_lei is None:
        return result.withColumn("mic", func.lit(None).cast(MIC_TYPE))
    return result.join(mic_lei, on = "lei", how = "leftouter")


# The registers as they stood on a day
# --------------------------------------------------------------------------


#: The registers :func:`entities_as_of` combines, with the Bronze folder of
#: each and the ``normalise_`` function that reads one snapshot of it.
SNAPSHOT_REGISTERS = {
    "gleif_1": (common.path_bronze_gleif_level_1, normalise_gleif_level_1),
    "relationships": (common.path_bronze_gleif_level_2, normalise_gleif_level_2),
    "ccp": (common.path_bronze_esma_ccp, normalise_esma_ccp),
    "inun": (common.path_bronze_eiopa_insurance_undertaking,
             normalise_eiopa_insurance_undertaking),
    "orpi": (common.path_bronze_eiopa_iorp, normalise_eiopa_iorp),
    "mfi": (common.path_bronze_ecb_mfi, normalise_ecb_mfi),
    "invfund": (common.path_bronze_ecb_investment_fund, normalise_ecb_investment_fund),
}


def snapshot_dates(folder):
    """The Bronze snapshots of one register by their reference date, oldest first.

    A register is stored one file per reference date, named ``YYYYMMDD`` or
    ``YYYY-MM-DD``. A file whose name carries no date -- the list of CCPs an
    earlier version stored as ``ccp_list``, replacing it on every import -- is
    dated by the day it was last written, which is the day of the download it
    holds.

    Returns
    -------
    list of tuple
        Pairs of the reference date, :class:`datetime.date`, and the path.
    """
    import datetime as _datetime

    snapshots = []
    for name in sorted(os.listdir(folder)) if os.path.isdir(folder) else []:
        if not name.endswith(".parquet"):
            continue
        digits = "".join(c for c in name if c.isdigit())[:8]
        path = os.path.join(folder, name)
        try:
            day = _datetime.datetime.strptime(digits, "%Y%m%d").date()
        except ValueError:
            day = _datetime.date.fromtimestamp(os.path.getmtime(path))
        snapshots.append((day, path))
    return sorted(snapshots)


def snapshot_as_of(snapshots, day):
    """The latest of ``snapshots`` on or before ``day``, or ``None``."""
    held = [snapshot for snapshot in snapshots if snapshot[0] <= day]
    return held[-1] if held else None


# GLEIF from the history of fincommons.gleif
# --------------------------------------------------------------------------
#
# Where the configuration file has a ``gleif`` section and that module's
# history has been built, the entities and their parents are read from it
# rather than from the golden copies this module's Bronze layer holds: the
# history says what GLEIF said on any day since it was begun, where the
# snapshots say it only on the days they were downloaded. The frames are
# given names the normalisation reads, so that everything after the reading
# -- the normalisation, the combination -- is the same either way.


def gleif_history_held(spark, config_path):
    """Whether the GLEIF history of :mod:`fincommons.gleif` is there to read."""
    from ..addressing import delta_table_exists
    from ..gleif import common as gleif_common

    try:
        refs = [gleif_common.ref_silver(config_path, level)
                for level in gleif_common.LEVELS]
    except KeyError:
        return False
    return all(delta_table_exists(spark, ref) for ref in refs)


def gleif_from_history(spark, config_path, day=None):
    """GLEIF's entities and relationships, from the history, in this module's names.

    Parameters
    ----------
    day: datetime.date, optional
        The day to read them as of; the records in force now where left out.
        For a day before the history was begun the state it was begun with is
        read, as :func:`entities_as_of` reads the earliest golden copy where
        none of the day is held.

    Returns
    -------
    tuple
        The frames :func:`normalise_gleif_level_1` and
        :func:`normalise_gleif_level_2` read -- the entities under the Silver
        names, the relationships under GLEIF's -- and the day the history was
        read as of, which is the day it was begun where ``day`` is before it.
    """
    from datetime import datetime

    from ..gleif import common as gleif_common
    from ..gleif import history as gleif_history

    frames, read_as_of = [], None
    for level in ("level_1", "level_2"):
        versions = gleif_common.connect_to_gleif(spark, config_path, level)
        if day is None:
            current = versions.where(func.col(gleif_history.VALID_TO).isNull())
        else:
            begun = versions.agg(func.min(gleif_history.PUBLICATION)).first()[0]
            begun = datetime.strptime(begun, "%Y%m%d-%H%M").date()
            as_of = max(day, begun)
            read_as_of = as_of if read_as_of is None else max(read_as_of, as_of)
            if day < begun:
                # Before the history began, each record as it was begun with:
                # its first version, whatever day that version starts.
                first = Window.partitionBy(*gleif_history.KEYS[level]) \
                    .orderBy(func.col(gleif_history.VALID_FROM))
                current = versions.withColumn("_rank", func.row_number().over(first)) \
                    .where(func.col("_rank") == 1).drop("_rank")
            else:
                current = gleif_history.records_as_of(versions, as_of)
        frames.append(current)

    entities, relationships = frames
    # The Silver names already: normalise_gleif_level_1 renames the Bronze
    # names of a golden copy into these, and leaves a frame that has them.
    entities = entities.select(
        "lei",
        func.col("legal_name").alias("name"),
        func.col("legal_address_country").alias("country"),
    )
    relationships = relationships.select(
        func.col("start_lei").alias("Relationship.StartNode.NodeID"),
        func.col("end_lei").alias("Relationship.EndNode.NodeID"),
        func.col("relationship_type").alias("Relationship.RelationshipType"),
        func.col("relationship_status").alias("Relationship.RelationshipStatus"),
    )
    return entities, relationships, read_as_of


def entities_as_of(spark, config_path, day):
    """The public entity master data as the registers stood on ``day``.

    Every register is read from its latest snapshot on or before ``day`` and
    the registers are combined by :func:`combine_registers`, the rules the
    public table is built by. A sectoral register with no snapshot by then
    contributes nothing, which :func:`sector_counts_over_time` reports apart
    from a register that had no entity of a sector.

    GLEIF is read from the history of :mod:`fincommons.gleif` where it has
    been built, as of ``day``; see :func:`gleif_from_history`. Otherwise it
    is the exception among the snapshots. It is the frame the registers are
    joined onto rather than a statement about a sector, so where no golden
    copy of ``day`` or before is held the earliest one is used. A golden copy keeps every LEI
    ever issued, lapsed and retired ones included, so an entity a register
    names on ``day`` is in any later copy; what the later copy changes is the
    name and the relationships, which a count of sectors does not read.

    ``mic`` holds the MICs GLEIF linked to an LEI on ``day`` where the
    MIC-to-LEI history of :mod:`fincommons.gleif` has been built, and is empty
    otherwise; it is not among the snapshots reported, since it says nothing
    about a sector.

    Returns
    -------
    tuple
        The combined frame, and a dict of the snapshot each register was read
        from, its reference date or ``None``.
    """
    frames, used = {}, {}
    from_history = gleif_history_held(spark, config_path)
    if from_history:
        entities, relationships, read_as_of = gleif_from_history(
            spark, config_path, day)
        frames["gleif_1"] = normalise_gleif_level_1(entities)
        frames["relationships"] = normalise_gleif_level_2(relationships)
        used["gleif_1"] = used["relationships"] = read_as_of
    for name, (path_of, normalise) in SNAPSHOT_REGISTERS.items():
        if from_history and name in ("gleif_1", "relationships"):
            continue
        snapshots = snapshot_dates(path_of(config_path))
        if not snapshots:
            raise FileNotFoundError(
                f"The Bronze layer holds no dated snapshot of {name!r} in "
                f"{path_of(config_path)!r}."
            )
        snapshot = snapshot_as_of(snapshots, day)
        if snapshot is None and name in ("gleif_1", "relationships"):
            snapshot = snapshots[0]
        if snapshot is None:
            frames[name] = normalise(spark.read.parquet(snapshots[0][1]).limit(0))
            used[name] = None
        else:
            frames[name] = normalise(spark.read.parquet(snapshot[1]))
            used[name] = snapshot[0]
    from ..gleif import mic_lei

    if mic_lei.mic_lei_held(spark, config_path):
        frames["mic_lei"] = normalise_gleif_mic_lei(
            mic_lei.mic_lei_as_of(spark, config_path, day))
    return combine_registers(**frames), used


def sector_counts_over_time(spark, config_path, dates=None):
    """How many entities each sector held, on every date a register was published.

    For every date, :func:`entities_as_of` combines the registers as they
    stood on it and the entities are counted by ``sector``, ``subsector`` and
    ``if_policy``, so that a report can show a sector as a whole or broken
    down by what its register says beyond the sector. This is the history of
    the registers in miniature: one snapshot per register and date, read and
    combined by the same rules as the public table.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    dates: list of datetime.date, optional
        The dates to count on. Every reference date of a snapshot of a
        sectoral register where left out.

    Returns
    -------
    tuple of pandas.DataFrame
        The counts, with the columns ``date``, ``sector``, ``subsector``,
        ``if_policy`` and ``entities``; and the snapshots behind them, with
        ``date``, ``register`` and ``snapshot``, ``None`` where the register
        had no snapshot by then.
    """
    import pandas

    if dates is None:
        sectoral = [n for n in SNAPSHOT_REGISTERS if n not in ("gleif_1", "relationships")]
        dates = sorted({day for name in sectoral
                        for day, _ in snapshot_dates(SNAPSHOT_REGISTERS[name][0](config_path))})
    counts, coverage = [], []
    for day in dates:
        entities, used = entities_as_of(spark, config_path, day)
        frame = entities.filter(func.col("sector").isNotNull()) \
            .groupBy("sector", "subsector", "if_policy").count() \
            .withColumnRenamed("count", "entities").toPandas()
        frame.insert(0, "date", day)
        counts.append(frame)
        coverage += [{"date": day, "register": name, "snapshot": snapshot}
                     for name, snapshot in used.items()]
    columns = ["date", "sector", "subsector", "if_policy", "entities"]
    counts = pandas.concat(counts, ignore_index=True) if counts else pandas.DataFrame(columns=columns)
    return counts[columns], pandas.DataFrame(coverage, columns=["date", "register", "snapshot"])


def write_sector_counts(spark, config_path):
    """Write :func:`sector_counts_over_time` beside the Silver layer, for the sector report.

    The counts go to ``sector_counts.parquet`` and the snapshots behind them to
    ``sector_snapshots.parquet``, in the Silver folder of the module, which is
    where the template ``entitymasterdata-sector-report.qmd`` reads them.

    Returns
    -------
    str
        The folder written to.
    """
    folder = common.path_silver_base(config_path)
    if folder is None:
        raise ValueError("The sector counts are written as files, which a Silver "
                         "layer addressed by catalogue name has no folder for.")
    counts, coverage = sector_counts_over_time(spark, config_path)
    os.makedirs(folder, exist_ok=True)
    counts.to_parquet(os.path.join(folder, "sector_counts.parquet"), index=False)
    coverage.to_parquet(os.path.join(folder, "sector_snapshots.parquet"), index=False)
    logger.info(
        "Wrote the sector counts of the registers over time",
        extra=context(module="entitymasterdata", layer="silver",
                      dates=int(counts["date"].nunique()), rows=len(counts)),
    )
    return folder


def _update_silver_entitymasterdata_public(spark, config_path, file_format, outcome):
    """Do the work of :func:`update_silver_entitymasterdata_public`."""
    def latest(path):
        return spark.read.parquet(latest_bronze_file(path))

    # GLEIF from the history of fincommons.gleif where it has been built, and
    # from the latest golden copy of this module's Bronze layer otherwise.
    if gleif_history_held(spark, config_path):
        entities, relationships, _ = gleif_from_history(spark, config_path)
        outcome["gleif"] = "history"
    else:
        entities = latest(common.path_bronze_gleif_level_1(config_path))
        relationships = latest(common.path_bronze_gleif_level_2(config_path))
        outcome["gleif"] = "bronze"

    result = combine_registers(
        gleif_1=normalise_gleif_level_1(entities),
        relationships=normalise_gleif_level_2(relationships),
        ccp=normalise_esma_ccp(latest(common.path_bronze_esma_ccp(config_path))),
        inun=normalise_eiopa_insurance_undertaking(
            latest(common.path_bronze_eiopa_insurance_undertaking(config_path))),
        orpi=normalise_eiopa_iorp(latest(common.path_bronze_eiopa_iorp(config_path))),
        mfi=normalise_ecb_mfi(latest(common.path_bronze_ecb_mfi(config_path))),
        invfund=normalise_ecb_investment_fund(
            latest(common.path_bronze_ecb_investment_fund(config_path))),
        mic_lei=gleif_mic_lei(spark, config_path),
    )

    common.write_silver(
        result, common.ref_silver_public(config_path), file_format = file_format
    )

    outcome["rows"] = common.read_silver(
        spark, common.ref_silver_public(config_path)
    ).count()


def gleif_mic_lei(spark, config_path):
    """The MICs GLEIF links to each LEI now, from :mod:`fincommons.gleif`, or ``None``.

    The mapping is kept by the pipeline of its own in
    :mod:`fincommons.gleif.mic_lei`, which this module reads rather than
    downloads. Unlike a register, its absence does not stop the update: the
    MICs add an identifier to entities the table describes already, so where
    the configuration file has no ``gleif`` section, or the history has not
    been built, the table is built with ``mic`` empty, and a warning says so.
    """
    from ..gleif import mic_lei

    if not mic_lei.mic_lei_held(spark, config_path):
        logger.warning(
            "The MIC-to-LEI history of fincommons.gleif has not been built, "
            "mic is left empty",
            extra=context(module="entitymasterdata", layer="silver",
                          source="GLEIF_MIC"),
        )
        return None
    return normalise_gleif_mic_lei(mic_lei.current_mic_lei(spark, config_path))


def update_silver_entitymasterdata(spark, config_path, file_format = None):
    """Update the entity master data set in the Silver layer.

    Combines the public entity master data with the proprietary list, if one is
    provided, and writes the result to the Silver layer. This is the table
    :func:`fincommons.entitymasterdata.connect_to_entitymasterdata` opens.

    Where an LEI is described by both, the proprietary description wins in
    full: the public row is dropped rather than filled in from, because a list
    an organisation maintains itself states what that organisation holds the
    entity to be. Entities the proprietary list carries without an LEI are kept
    as they are, since there is nothing to match them on.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    file_format: str, optional
        Format the table is written in, ``'parquet'`` or ``'delta'``, decided
        as for :func:`update_silver_entitymasterdata_public` where it is left
        out. It says nothing about the format of the public table this one is
        built from, which is read in whichever format it was written in. See
        :func:`fincommons.storage.write_silver` for what the choice amounts
        to.

    Examples
    --------
    >>> update_silver_entitymasterdata(spark, "path/to/configuration/file")
    """
    file_format = common.silver_format(common.ref_silver_main(config_path), file_format)
    with pipeline_run(
        logger,
        "update_silver_entitymasterdata",
        module="entitymasterdata",
        layer="silver",
        table="main",
        config_path=config_path,
        file_format=file_format,
    ) as outcome:
        _update_silver_entitymasterdata(spark, config_path, file_format, outcome)


def _update_silver_entitymasterdata(spark, config_path, file_format, outcome):
    """Do the work of :func:`update_silver_entitymasterdata`."""
    path_internal = os.path.join(
        common.path_bronze_internal_entitymasterdata(config_path),
        "entitymasterdata_internal.csv",
    )

    public = common.read_silver(spark, common.ref_silver_public(config_path))

    # The MICs are GLEIF's statement of which markets an LEI operates, not a
    # description of the entity, so they are taken by LEI whichever source
    # describes it -- the proprietary list included, which has no column for
    # them. A public table built before the column was is read as without it.
    if "mic" in public.columns:
        gleif_mic = public.select("lei", "mic").filter(func.col("mic").isNotNull())
    else:
        gleif_mic = spark.createDataFrame([], f"lei string, mic {MIC_TYPE}")

    if os.path.exists(path_internal):
        internal = read_internal_list(spark, path_internal)

        described_internally = internal.filter(func.col("lei").isNotNull()).select("lei").distinct()
        public = public.join(described_internally, on = "lei", how = "left_anti")

        result = internal.withColumn("_source_rank", func.lit(1)) \
            .unionByName(public.withColumn("_source_rank", func.lit(2)), allowMissingColumns=True)
    else:
        result = with_internal_id_columns(public.withColumn("_source_rank", func.lit(2)))

    # The catch-all entities come last, so that a proprietary list that
    # describes one of them -- with a sector of its own, say -- has its way.
    catch_all = spark.createDataFrame(
        list(CATCH_ALL_ENTITIES.items()), "lei string, name string"
    ).withColumn("_source_rank", func.lit(3))
    result = result.unionByName(catch_all, allowMissingColumns=True)

    # An LEI describes one entity, so it may appear once. Rows without an LEI
    # cannot be matched and are all kept; they have to be set aside first,
    # because a window partitioned by LEI would gather them into a single group
    # and keep one of them. The proprietary list is refused where it carries an
    # LEI twice, and the public rows it describes are gone already, so this
    # finds one row per LEI in every table the update accepts; it stays as the
    # guarantee that the table is keyed by the LEI.
    without_lei = result.filter(func.col("lei").isNull())
    with_lei = first_per_lei(
        result.filter(func.col("lei").isNotNull()),
        [func.col("_source_rank"), func.col("internal_id_1").desc_nulls_last()],
    )
    result = with_lei.unionByName(without_lei).drop("_source_rank")
    result = result.drop("mic").join(gleif_mic, on = "lei", how = "leftouter")

    # Number the entities. The window spans the whole table, which sorts it on
    # one partition, but the number is meant to be a running one and dense.
    #
    # The number belongs to one build of the table: it is assigned afresh on
    # every rebuild, so an LEI added by a new release of a register moves the
    # number of every entity after it. It is for ordering and joining within
    # one build, and a reference meant to outlast a rebuild is kept by LEI.
    # Within a build it is repeatable: the rows without an LEI, which all tie
    # on it, are ordered by every other column, so the same input numbers them
    # the same way on every run.
    result = result.withColumn(
        "id", func.row_number().over(Window.orderBy(
            func.col("lei").asc_nulls_last(),
            *[func.col(column).asc_nulls_last()
              for column in COLUMNS if column not in ("id", "lei")],
        ))
    )

    common.write_silver(
        result.select(*COLUMNS),
        common.ref_silver_main(config_path),
        file_format = file_format,
    )

    outcome["internal"] = os.path.exists(path_internal)
    outcome["rows"] = common.read_silver(
        spark, common.ref_silver_main(config_path)
    ).count()


def update_entitymasterdata(spark, config_path, url_gleif_level_1 = None,
                            url_gleif_level_2 = None, file_format = None):
    """Update the entity master data set.

    Updates the entity master data set by calling the necessary update
    functions sequentially: the downloads that can be automated, the import of
    everything the landing zone holds into the Bronze layer, and the two steps
    of the Silver layer.

    Every register is downloaded on every run: the GLEIF golden copies from
    the links passed in, or else the latest publication the Bronze layer
    lacks, and the list of CCPs and the registers of the ECB and EIOPA. A
    register whose download fails is logged and the one the Bronze layer
    holds is used, except for a GLEIF link that was passed in, whose failure
    is raised; a register can be placed into the landing zone by hand as the
    module README describes.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    url_gleif_level_1: str, optional
        URL of the GLEIF Level 1 csv file. Where it is not given, the latest
        golden copy is downloaded unless the Bronze layer holds one of its day;
        see :func:`download_latest_gleif`.
    url_gleif_level_2: str, optional
        URL of the GLEIF Level 2 csv file, as for Level 1.
    file_format: str, optional
        Format both Silver tables are written in, ``'parquet'`` or
        ``'delta'``. Where it is left out the configuration file decides, for
        each table on its own, so that moving the Silver layer into a
        catalogue takes a change of the configuration file alone; see
        :func:`update_silver_entitymasterdata_public`.

    Examples
    --------
    >>> update_entitymasterdata(spark, "path/to/configuration/file")
    """
    # Both tables are checked before anything is downloaded, so that a format
    # either of them refuses does not surface only after the downloads.
    common.silver_format(common.ref_silver_public(config_path), file_format)
    common.silver_format(common.ref_silver_main(config_path), file_format)
    with pipeline_run(
        logger,
        "update_entitymasterdata",
        module="entitymasterdata",
        config_path=config_path,
    ):
        _update_entitymasterdata(
            spark, config_path, url_gleif_level_1, url_gleif_level_2, file_format
        )


def _update_entitymasterdata(spark, config_path, url_gleif_level_1,
                             url_gleif_level_2, file_format):
    """Do the work of :func:`update_entitymasterdata`."""
    # Update the landing zone. A GLEIF link that is given is downloaded, and
    # its failure stops the update, since it was asked for; a data set without
    # one is taken from the latest publication, as the other registers are.
    if url_gleif_level_1 is not None:
        download_gleif_level_1(config_path, url_gleif_level_1)
    if url_gleif_level_2 is not None:
        download_gleif_level_2(config_path, url_gleif_level_2)
    latest = tuple(data_set for data_set, url in
                   (("lei2", url_gleif_level_1), ("rr", url_gleif_level_2))
                   if url is None)
    # The list of CCPs and the registers of the ECB and EIOPA are the downloads
    # that run without being asked for, so an environment without access to
    # them -- a cluster without internet access, a host that is down, a page
    # that has changed -- must not stop the update. The Bronze layer keeps the
    # register it imported last, and the failure is recorded.
    unasked = (
        (lambda config_path: download_latest_gleif(config_path, latest),
         "GLEIF", "The latest GLEIF golden copy"),
        (download_esma_ccp, "ESMA_CCP", "The list of CCPs"),
        (download_ecb_mfi, "ECB_MFI", "The ECB's list of MFIs"),
        (download_ecb_investment_fund, "ECB_IF",
         "The ECB's lists of investment funds"),
        (download_ecb_fvc, "ECB_FVC", "The ECB's lists of FVCs"),
        (download_ecb_psri, "ECB_PSRI", "The ECB's lists of PSRIs"),
        (download_eiopa_insurance_undertaking, "EIOPA_IC",
         "EIOPA's register of insurance undertakings"),
        (download_eiopa_iorp, "EIOPA_IORP", "EIOPA's register of IORPs"),
    )
    for download, source, register in unasked:
        try:
            download(config_path)
        except (requests.RequestException, ValueError, zipfile.BadZipFile):
            logger.error(
                f"{register} could not be downloaded, the one in the Bronze "
                "layer is used",
                exc_info=True,
                extra=context(module="entitymasterdata", layer="landing",
                              source=source),
            )

    # Update the Bronze layer
    update_from_landing_to_bronze(config_path, spark)

    # Update the Silver layer
    update_silver_entitymasterdata_public(
        spark = spark, config_path = config_path, file_format = file_format
    )
    update_silver_entitymasterdata(
        spark = spark, config_path = config_path, file_format = file_format
    )


#: What this module offers. Combining the registers of the Bronze layer into
#: the entity master data set, and the pipeline that runs every stage.
__all__ = [
    "ACTIVE_RELATIONSHIP",
    "COLUMNS",
    "CATCH_ALL_ENTITIES",
    "INTERNAL_COLUMNS",
    "INTERNAL_ID_COLUMNS",
    "LEI_OTHER",
    "LEI_PATTERN",
    "LEI_UNKNOWN",
    "MFI_SECTORS",
    "MFI_SECTOR_OTHER",
    "MIC_TYPE",
    "active_relationship",
    "check_internal_list",
    "combine_registers",
    "first_per_lei",
    "latest_bronze_file",
    "with_silver_names",
    "SILVER_NAMES",
    "mfi_sector",
    "normalise_ecb_investment_fund",
    "normalise_ecb_mfi",
    "normalise_eiopa_insurance_undertaking",
    "normalise_eiopa_iorp",
    "normalise_esma_ccp",
    "gleif_from_history",
    "gleif_history_held",
    "normalise_gleif_level_1",
    "normalise_gleif_level_2",
    "normalise_gleif_mic_lei",
    "gleif_mic_lei",
    "normalised_lei",
    "read_internal_list",
    "text_columns",
    "update_entitymasterdata",
    "update_silver_entitymasterdata",
    "update_silver_entitymasterdata_public",
    "warn_of_malformed_leis",
    "warn_of_unknown_mfi_categories",
    "with_internal_id_columns",
    "INTERNAL_ID_SILVER",
    "with_normalised_lei",
    "write_sector_counts",
    "snapshot_dates",
    "snapshot_as_of",
    "sector_counts_over_time",
    "entities_as_of",
    "SNAPSHOT_REGISTERS",
]
