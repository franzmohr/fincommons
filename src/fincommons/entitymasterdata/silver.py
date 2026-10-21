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

import pyspark.sql.functions as func
from pyspark.sql import Window

from . import common
from ..log import context, pipeline_run
from .bronze import *
from .landing import *

logger = logging.getLogger(__name__)


#: The columns of the entity master data set, in the order they are stored in.
COLUMNS = ["ID", "LEI",
           "INTERNALID_1", "INTERNALID_2", "INTERNALID_3", "INTERNALID_4",
           "NAME", "CTRY", "SECTOR", "SUBSECTOR", "ULTPAR_LEI", "AMC",
           "IF_POLICY", "IF_TYPE", "IF_UCITS_COMPL"]

#: The columns a proprietary list contributes that the public sources do not.
INTERNAL_ID_COLUMNS = ["INTERNALID_1", "INTERNALID_2", "INTERNALID_3", "INTERNALID_4"]

#: The columns a proprietary list may carry: those of the entity master data
#: set except ``ID``, which the update assigns. ``LEI`` is the one it must.
INTERNAL_COLUMNS = [column for column in COLUMNS if column != "ID"]

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
    for column in INTERNAL_ID_COLUMNS:
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


def with_normalised_lei(data, source, columns = ("LEI",)):
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
        The columns that hold an LEI. Only ``LEI`` itself is checked.
    """
    for column in columns:
        if column in data.columns:
            data = data.withColumn(column, normalised_lei(column))
    warn_of_malformed_leis(data, source)
    return data


def warn_of_malformed_leis(data, source):
    """Log the LEIs of a register that do not match :data:`LEI_PATTERN`.

    Parameters
    ----------
    data: pyspark.sql.DataFrame
        A data frame with a normalised ``LEI`` column.
    source: str
        Name of the register, for the log.
    """
    malformed = data.filter(
        func.col("LEI").isNotNull() & ~func.col("LEI").rlike(LEI_PATTERN)
    ).select("LEI").distinct()
    examples = sorted(row["LEI"] for row in malformed.limit(6).collect())
    if not examples:
        return
    logger.warning(
        "LEIs of the register are malformed and will not match GLEIF",
        extra=context(module="entitymasterdata", layer="silver", source=source,
                      count=malformed.count(), leis="|".join(examples[:5])),
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
        internal, "INTERNAL", columns=("LEI", "ULTPAR_LEI", "AMC")
    ) if "LEI" in internal.columns else internal
    check_internal_list(internal, path)
    return with_internal_id_columns(internal)


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
        A data frame with an ``LEI`` column.
    order: list
        The columns the rows of one LEI are ordered by. The first of them is
        kept.
    """
    window_spec = Window.partitionBy("LEI").orderBy(*order)
    return data \
        .withColumn("rn", func.row_number().over(window_spec)) \
        .filter(func.col("rn") == 1) \
        .drop("rn")


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
        Name the counterpart is returned under, such as ``'ULTPAR_LEI'``.

    Returns
    -------
    pyspark.sql.DataFrame
        A frame with an ``LEI`` column and ``column``, one row per LEI.
    """
    related = data \
        .filter(func.col("`Relationship.RelationshipType`") == relationship_type) \
        .filter(func.col("`Relationship.RelationshipStatus`") == ACTIVE_RELATIONSHIP) \
        .withColumnRenamed("Relationship.StartNode.NodeID", "LEI") \
        .withColumnRenamed("Relationship.EndNode.NodeID", column) \
        .transform(lambda frame: text_columns(frame, ["LEI", column])) \
        .withColumn("LEI", normalised_lei("LEI")) \
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


def _update_silver_entitymasterdata_public(spark, config_path, file_format, outcome):
    """Do the work of :func:`update_silver_entitymasterdata_public`."""
    # GLEIF Level 1 data, which says who the entity is
    gleif_1 = spark.read.parquet(
        latest_bronze_file(common.path_bronze_gleif_level_1(config_path))
    )
    gleif_1 = with_normalised_lei(gleif_1, "GLEIF_L1")

    # GLEIF Level 2 data, which says how entities are related to one another
    gleif_2 = spark.read.parquet(
        latest_bronze_file(common.path_bronze_gleif_level_2(config_path))
    )

    # ECB investment funds
    invfund = spark.read.parquet(
            latest_bronze_file(common.path_bronze_ecb_investment_fund(config_path))
        ) \
        .transform(lambda data: with_normalised_lei(data, "ECB_IF")) \
        .filter(func.col("LEI").isNotNull()) \
        .withColumn("SECTOR", func.when(func.col("UCITS compliance") == "Non-UCITS compliant, AIF", "AIFD") \
                                  .when(func.col("UCITS compliance") == "UCITS compliant", "UCIT") \
                                  .otherwise(func.lit("INVF"))) \
        .withColumn("SUBSECTOR", func.col("Investment policy")) \
        .withColumn("IF_POLICY", func.col("Investment policy")) \
        .withColumnRenamed("Type of investment fund", "IF_TYPE") \
        .withColumn("IF_TYPE", func.when(func.col("IF_TYPE") == "Exchange traded fund (ETF)", "ETF") \
                                   .when(func.col("IF_TYPE") == "Private equity fund (PEF)", "PEF") \
                                   .when(func.col("IF_TYPE") == "Investment fund other than an Exchange traded fund (ETF) and a Private equity fund (PEF)", "Neither ETF nor PEF") \
                                   .otherwise(func.col("IF_TYPE"))) \
        .withColumnRenamed("UCITS compliance", "IF_UCITS_COMPL") \
        .withColumn("IF_UCITS_COMPL", func.when(func.col("IF_UCITS_COMPL") == "Non-UCITS compliant, AIF", "AIF") \
                                          .when(func.col("IF_UCITS_COMPL") == "UCITS compliant", "UCITS") \
                                          .otherwise(func.col("IF_UCITS_COMPL"))) \
        .transform(lambda data: text_columns(
            data, ["LEI", "SECTOR", "SUBSECTOR", "IF_POLICY", "IF_TYPE", "IF_UCITS_COMPL"]
        ))

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
        invfund.select("LEI", "IF_POLICY", "IF_TYPE", "IF_UCITS_COMPL"),
        [func.col("IF_UCITS_COMPL").asc_nulls_last(),
         func.col("IF_TYPE").asc_nulls_last(),
         func.col("IF_POLICY").asc_nulls_last()],
    )
    invfund = invfund.select("LEI", "SECTOR", "SUBSECTOR")

    # ECB MFIs. The register is read before the sector is derived from it, so
    # that a category the module does not know can be named in the log.
    mfi_register = spark.read.parquet(
            latest_bronze_file(common.path_bronze_ecb_mfi(config_path))
        ) \
        .transform(lambda data: with_normalised_lei(data, "ECB_MFI")) \
        .filter(func.col("LEI").isNotNull())
    warn_of_unknown_mfi_categories(mfi_register)
    mfi = mfi_register \
        .withColumn("SECTOR", mfi_sector()) \
        .transform(lambda data: text_columns(data, ["LEI", "SECTOR"]))

    # EIOPA insurance undertakings
    inun = spark.read.parquet(
            latest_bronze_file(common.path_bronze_eiopa_insurance_undertaking(config_path))
        ) \
        .transform(lambda data: with_normalised_lei(data, "EIOPA_INUN")) \
        .filter(func.col("LEI").isNotNull()) \
        .withColumn("SECTOR", func.lit("INUN")) \
        .transform(lambda data: text_columns(data, ["LEI", "SECTOR"]))

    # EIOPA institutions for occupational retirement provision
    orpi = spark.read.parquet(
            latest_bronze_file(common.path_bronze_eiopa_iorp(config_path))
        ) \
        .transform(lambda data: with_normalised_lei(data, "EIOPA_IORP")) \
        .filter(func.col("LEI").isNotNull()) \
        .withColumn("SECTOR", func.lit("ORPI")) \
        .transform(lambda data: text_columns(data, ["LEI", "SECTOR"]))

    # ESMA CCPs. The list is not kept per reference date, so the folder holds
    # the one file, but it is looked up the same way as the others so that a
    # Bronze layer the list has never reached says so in the same words.
    ccp = spark.read.parquet(
            latest_bronze_file(common.path_bronze_esma_ccp(config_path))
        ) \
        .transform(lambda data: with_normalised_lei(data, "ESMA_CCP")) \
        .filter(func.col("LEI").isNotNull()) \
        .transform(lambda data: text_columns(data, ["LEI", "SECTOR", "SUBSECTOR"]))

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
    sector = ccp.withColumn("source_rank", func.lit(1)) \
        .unionByName(inun.withColumn("source_rank", func.lit(2)), allowMissingColumns=True) \
        .unionByName(orpi.withColumn("source_rank", func.lit(3)), allowMissingColumns=True) \
        .unionByName(mfi.withColumn("source_rank", func.lit(4)), allowMissingColumns=True) \
        .unionByName(invfund.withColumn("source_rank", func.lit(5)), allowMissingColumns=True)

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
        [func.col("source_rank"),
         func.col("SECTOR").asc_nulls_last(),
         func.col("SUBSECTOR").asc_nulls_last()],
    ).drop("source_rank")

    # Enrich GLEIF Level 1 data with sector information
    result = gleif_1.join(sector, on = "LEI", how = "leftouter")

    # And with what the fund register says about a fund, whichever register
    # ended up describing it. SUBSECTOR is not among these columns: it is the
    # refinement the winning register offers, so it stays that register's
    # statement rather than being filled in from another taxonomy. For a fund
    # that wins nothing is lost by that, because the investment policy the
    # sector step would have put in SUBSECTOR is what IF_POLICY carries.
    result = result.join(fund_characteristics, on = "LEI", how = "leftouter")

    # Generate information on the ultimate parent based on GLEIF Level 2 data,
    # and on a fund's asset management company, which is the same reading of
    # the same file under a different relationship type. Both go through
    # :func:`active_relationship`, so that neither can be read without the
    # status of the relationship being taken into account.
    result = result.join(
        active_relationship(gleif_2, "IS_ULTIMATELY_CONSOLIDATED_BY", "ULTPAR_LEI"),
        on = "LEI", how = "leftouter",
    )
    result = result.join(
        active_relationship(gleif_2, "IS_FUND-MANAGED_BY", "AMC"),
        on = "LEI", how = "leftouter",
    )

    common.write_silver(
        result, common.ref_silver_public(config_path), file_format = file_format
    )

    outcome["rows"] = common.read_silver(
        spark, common.ref_silver_public(config_path)
    ).count()


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

    if os.path.exists(path_internal):
        internal = read_internal_list(spark, path_internal)

        described_internally = internal.filter(func.col("LEI").isNotNull()).select("LEI").distinct()
        public = public.join(described_internally, on = "LEI", how = "left_anti")

        result = internal.withColumn("source_rank", func.lit(1)) \
            .unionByName(public.withColumn("source_rank", func.lit(2)), allowMissingColumns=True)
    else:
        result = with_internal_id_columns(public.withColumn("source_rank", func.lit(2)))

    # The catch-all entities come last, so that a proprietary list that
    # describes one of them -- with a sector of its own, say -- has its way.
    catch_all = spark.createDataFrame(
        list(CATCH_ALL_ENTITIES.items()), "LEI string, NAME string"
    ).withColumn("source_rank", func.lit(3))
    result = result.unionByName(catch_all, allowMissingColumns=True)

    # An LEI describes one entity, so it may appear once. Rows without an LEI
    # cannot be matched and are all kept; they have to be set aside first,
    # because a window partitioned by LEI would gather them into a single group
    # and keep one of them. The proprietary list is refused where it carries an
    # LEI twice, and the public rows it describes are gone already, so this
    # finds one row per LEI in every table the update accepts; it stays as the
    # guarantee that the table is keyed by the LEI.
    without_lei = result.filter(func.col("LEI").isNull())
    with_lei = first_per_lei(
        result.filter(func.col("LEI").isNotNull()),
        [func.col("source_rank"), func.col("INTERNALID_1").desc_nulls_last()],
    )
    result = with_lei.unionByName(without_lei).drop("source_rank")

    # Number the entities. The window spans the whole table, which sorts it on
    # one partition, but the number is meant to be a running one and a caller
    # that joins on it expects it to be dense.
    result = result.withColumn(
        "ID", func.row_number().over(Window.orderBy(func.col("LEI").asc_nulls_last()))
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

    The ECB and EIOPA registers are not downloaded and the GLEIF golden copies
    only where their addresses are passed in, so the landing zone should be
    filled as the module README describes before this is called.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An active Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    url_gleif_level_1: str, optional
        URL of the GLEIF Level 1 csv file. Where it is not given, the Level 1
        data already in the landing zone is used.
    url_gleif_level_2: str, optional
        URL of the GLEIF Level 2 csv file. Where it is not given, the Level 2
        data already in the landing zone is used.
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
    # Update the landing zone
    if url_gleif_level_1 is not None:
        download_gleif_level_1(config_path, url_gleif_level_1)
    if url_gleif_level_2 is not None:
        download_gleif_level_2(config_path, url_gleif_level_2)
    download_esma_ccp(config_path)

    # Update the Bronze layer
    update_from_landing_to_bronze(config_path)

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
    "active_relationship",
    "check_internal_list",
    "first_per_lei",
    "latest_bronze_file",
    "mfi_sector",
    "normalised_lei",
    "read_internal_list",
    "text_columns",
    "update_entitymasterdata",
    "update_silver_entitymasterdata",
    "update_silver_entitymasterdata_public",
    "warn_of_malformed_leis",
    "warn_of_unknown_mfi_categories",
    "with_internal_id_columns",
    "with_normalised_lei",
]
