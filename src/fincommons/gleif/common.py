"""The layout of the GLEIF data lake and the columns each level is read in.

GLEIF publishes the reference data of every legal entity identifier in two
levels, and this module keeps a history of each:

``level_1`` -- *who is who*
    One record per LEI: the legal name, the addresses, the legal form, the
    jurisdiction, the status of the entity and of its registration. GLEIF
    calls the file ``lei2``.
``level_2`` -- *who owns whom*
    One record per reported relationship between two LEIs: which entity is
    directly or ultimately consolidated by which, which fund is managed by
    which company, and the periods of the relationship. GLEIF calls the file
    ``rr``.

Beside the two levels it keeps a history of one of GLEIF's mappings:

``mic_lei`` -- *which entity operates which market*
    One record per market identifier code (MIC, ISO 10383) and the LEI of
    the entity operating the market, from the files GLEIF publishes about
    weekly; see :mod:`fincommons.gleif.mic_lei`.

The configuration file names a ``gleif`` section with the three layers::

    gleif:
      landing: /data/gleif/landing
      bronze: /data/gleif/bronze
      silver: /data/gleif/silver
"""

from __future__ import annotations

import os

from pyspark.sql.types import BooleanType, DateType, StringType, StructType

from ..addressing import read_delta, resolve

#: The data set as the configuration file names it.
DATASET = "gleif"

#: The two levels, by the name this module gives them and the name GLEIF gives
#: the file.
LEVELS = {"level_1": "lei2", "level_2": "rr"}

#: The tables of the Silver layer, one history per level.
TABLES = {"level_1": "entities", "level_2": "relationships"}

#: The folder and the Silver table of the MIC-to-LEI mapping.
MIC_LEI = "mic_lei"

#: The kinds of file a publication offers that the history reads: the full
#: golden copy, and the delta of the records changed in the 24 hours before it.
FULL = "full"
DELTA = "delta"

#: The columns of the Level 1 golden copy the history keeps, by the name GLEIF
#: gives them and the name the Silver layer gives them. The file has some 340
#: columns, most of them repetitions of a group -- the fifth other name, the
#: third transliterated address -- that a history of who an entity is does not
#: need. Every other name and every further address line is left out, which
#: keeps a version from being opened by a change of a translated name alone.
LEVEL_1_COLUMNS = {
    "LEI": "lei",
    "Entity.LegalName": "legal_name",
    "Entity.LegalName.xmllang": "legal_name_language",
    "Entity.OtherEntityNames.OtherEntityName.1": "other_name",
    "Entity.OtherEntityNames.OtherEntityName.1.type": "other_name_type",
    "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1":
        "transliterated_name",
    "Entity.LegalAddress.FirstAddressLine": "legal_address_line",
    "Entity.LegalAddress.City": "legal_address_city",
    "Entity.LegalAddress.Region": "legal_address_region",
    "Entity.LegalAddress.Country": "legal_address_country",
    "Entity.LegalAddress.PostalCode": "legal_address_postal_code",
    "Entity.HeadquartersAddress.FirstAddressLine": "hq_address_line",
    "Entity.HeadquartersAddress.City": "hq_address_city",
    "Entity.HeadquartersAddress.Region": "hq_address_region",
    "Entity.HeadquartersAddress.Country": "hq_address_country",
    "Entity.HeadquartersAddress.PostalCode": "hq_address_postal_code",
    "Entity.RegistrationAuthority.RegistrationAuthorityID":
        "registration_authority_id",
    "Entity.RegistrationAuthority.RegistrationAuthorityEntityID":
        "registration_authority_entity_id",
    "Entity.LegalJurisdiction": "legal_jurisdiction",
    "Entity.EntityCategory": "entity_category",
    "Entity.EntitySubCategory": "entity_sub_category",
    "Entity.LegalForm.EntityLegalFormCode": "legal_form_code",
    "Entity.LegalForm.OtherLegalForm": "legal_form_other",
    "Entity.AssociatedEntity.AssociatedLEI": "associated_lei",
    "Entity.AssociatedEntity.type": "associated_entity_type",
    "Entity.EntityStatus": "entity_status",
    "Entity.EntityCreationDate": "entity_creation_date",
    "Entity.EntityExpirationDate": "entity_expiration_date",
    "Entity.EntityExpirationReason": "entity_expiration_reason",
    "Entity.SuccessorEntity.1.SuccessorLEI": "successor_lei",
    "Registration.InitialRegistrationDate": "initial_registration_date",
    "Registration.LastUpdateDate": "last_update_date",
    "Registration.RegistrationStatus": "registration_status",
    "Registration.NextRenewalDate": "next_renewal_date",
    "Registration.ManagingLOU": "managing_lou",
    "Registration.ValidationSources": "validation_sources",
    "Registration.ValidationAuthority.ValidationAuthorityID":
        "validation_authority_id",
    "Registration.ValidationAuthority.ValidationAuthorityEntityID":
        "validation_authority_entity_id",
    "ConformityFlag": "conformity_flag",
}

#: The Level 1 columns of the history, every one as text, as GLEIF writes it:
#: dates and timestamps stay the ISO 8601 text of the file, which is what a
#: comparison of two versions needs and what a reader parses where it wants a
#: date. The names are the values of :data:`LEVEL_1_COLUMNS`, in its order.
level_1_schema = StructType() \
    .add("lei", StringType(), True) \
    .add("legal_name", StringType(), True) \
    .add("legal_name_language", StringType(), True) \
    .add("other_name", StringType(), True) \
    .add("other_name_type", StringType(), True) \
    .add("transliterated_name", StringType(), True) \
    .add("legal_address_line", StringType(), True) \
    .add("legal_address_city", StringType(), True) \
    .add("legal_address_region", StringType(), True) \
    .add("legal_address_country", StringType(), True) \
    .add("legal_address_postal_code", StringType(), True) \
    .add("hq_address_line", StringType(), True) \
    .add("hq_address_city", StringType(), True) \
    .add("hq_address_region", StringType(), True) \
    .add("hq_address_country", StringType(), True) \
    .add("hq_address_postal_code", StringType(), True) \
    .add("registration_authority_id", StringType(), True) \
    .add("registration_authority_entity_id", StringType(), True) \
    .add("legal_jurisdiction", StringType(), True) \
    .add("entity_category", StringType(), True) \
    .add("entity_sub_category", StringType(), True) \
    .add("legal_form_code", StringType(), True) \
    .add("legal_form_other", StringType(), True) \
    .add("associated_lei", StringType(), True) \
    .add("associated_entity_type", StringType(), True) \
    .add("entity_status", StringType(), True) \
    .add("entity_creation_date", StringType(), True) \
    .add("entity_expiration_date", StringType(), True) \
    .add("entity_expiration_reason", StringType(), True) \
    .add("successor_lei", StringType(), True) \
    .add("initial_registration_date", StringType(), True) \
    .add("last_update_date", StringType(), True) \
    .add("registration_status", StringType(), True) \
    .add("next_renewal_date", StringType(), True) \
    .add("managing_lou", StringType(), True) \
    .add("validation_sources", StringType(), True) \
    .add("validation_authority_id", StringType(), True) \
    .add("validation_authority_entity_id", StringType(), True) \
    .add("conformity_flag", StringType(), True)


#: The history of the MIC-to-LEI mapping, as its contract states it: the MIC
#: and the LEI of the entity operating the market, as text, and the days the
#: link held; see :mod:`fincommons.gleif.mic_lei`.
mic_lei_schema = StructType() \
    .add("mic", StringType(), False) \
    .add("lei", StringType(), False) \
    .add("valid_from", DateType(), False) \
    .add("publication", StringType(), False) \
    .add("valid_to", DateType(), True) \
    .add("latest_record", BooleanType(), False)


#: How many of the numbered period and quantifier groups of a relationship the
#: Bronze layer keeps. GLEIF writes five of each; a relationship states up to
#: three periods -- of the relationship, of the accounts it rests on, of the
#: filing -- and the period of the relationship is the one the history reads.
RELATIONSHIP_GROUPS = 5

#: The columns of the Level 2 golden copy the Bronze layer keeps, by the name
#: GLEIF gives them. The Silver layer derives its own from these rather than
#: renaming them, because the period of the relationship is whichever of the
#: numbered periods is typed as one; see :mod:`fincommons.gleif.history`.
LEVEL_2_SOURCE_COLUMNS = (
    [
        "Relationship.StartNode.NodeID",
        "Relationship.StartNode.NodeIDType",
        "Relationship.EndNode.NodeID",
        "Relationship.EndNode.NodeIDType",
        "Relationship.RelationshipType",
        "Relationship.RelationshipStatus",
    ]
    + [f"Relationship.Period.{n}.{part}"
       for n in range(1, RELATIONSHIP_GROUPS + 1)
       for part in ("startDate", "endDate", "periodType")]
    + [f"Relationship.Quantifiers.{n}.{part}"
       for n in range(1, RELATIONSHIP_GROUPS + 1)
       for part in ("MeasurementMethod", "QuantifierAmount", "QuantifierUnits")]
    + [
        "Registration.InitialRegistrationDate",
        "Registration.LastUpdateDate",
        "Registration.RegistrationStatus",
        "Registration.NextRenewalDate",
        "Registration.ManagingLOU",
        "Registration.ValidationSources",
        "Registration.ValidationDocuments",
        "DeletedAt",
    ]
)

#: The columns of the Bronze layer of each level, by GLEIF's names.
SOURCE_COLUMNS = {
    "level_1": list(LEVEL_1_COLUMNS),
    "level_2": LEVEL_2_SOURCE_COLUMNS,
}


def _check_level(level):
    if level not in LEVELS:
        raise ValueError(
            f"{level!r} is not a level of the GLEIF data; it is one of "
            f"{sorted(LEVELS)}"
        )
    return level


def path_landing(config_path, level):
    """The folder of the landing zone the files of one level are downloaded to."""
    return os.path.join(resolve(config_path, DATASET, "landing").path,
                        _check_level(level))


def path_bronze(config_path, level):
    """The folder of the Bronze layer the files of one level are imported to."""
    return os.path.join(resolve(config_path, DATASET, "bronze").path,
                        _check_level(level))


def ref_silver(config_path, level):
    """Where the history of one level lives in the Silver layer.

    The history is a Delta table, by path in a local data lake or by name where
    the configuration file gives the layer a catalogue prefix; see
    :mod:`fincommons.addressing`.
    """
    return resolve(config_path, DATASET, "silver", TABLES[_check_level(level)])


def path_landing_mic_lei(config_path):
    """The folder of the landing zone the MIC-to-LEI files are downloaded to."""
    return os.path.join(resolve(config_path, DATASET, "landing").path, MIC_LEI)


def path_bronze_mic_lei(config_path):
    """The folder of the Bronze layer the MIC-to-LEI files are imported to."""
    return os.path.join(resolve(config_path, DATASET, "bronze").path, MIC_LEI)


def ref_silver_mic_lei(config_path):
    """Where the history of the MIC-to-LEI mapping lives in the Silver layer."""
    return resolve(config_path, DATASET, "silver", MIC_LEI)


def create_directory_tree(config_path):
    """Create the folders of the landing zone and the Bronze layer."""
    for level in LEVELS:
        os.makedirs(path_landing(config_path, level), exist_ok=True)
        os.makedirs(path_bronze(config_path, level), exist_ok=True)
    os.makedirs(path_landing_mic_lei(config_path), exist_ok=True)
    os.makedirs(path_bronze_mic_lei(config_path), exist_ok=True)


def connect_to_gleif(spark, config_path, level="level_1"):
    """Open the history of one level, every version of every record.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        An open Spark session.
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    level: str
        ``"level_1"`` for the entities, ``"level_2"`` for the relationships.
    """
    return read_delta(spark, ref_silver(config_path, level))


__all__ = [
    "DATASET",
    "DELTA",
    "FULL",
    "LEVELS",
    "LEVEL_1_COLUMNS",
    "LEVEL_2_SOURCE_COLUMNS",
    "MIC_LEI",
    "RELATIONSHIP_GROUPS",
    "SOURCE_COLUMNS",
    "TABLES",
    "connect_to_gleif",
    "level_1_schema",
    "mic_lei_schema",
    "create_directory_tree",
    "path_bronze",
    "path_bronze_mic_lei",
    "path_landing",
    "path_landing_mic_lei",
    "ref_silver",
    "ref_silver_mic_lei",
]
