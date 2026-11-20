"""STS: reading a snapshot and the spreadsheets into rows, in plain Python.

Nothing here needs Spark, so every rule by which the register becomes a table
can be tested on a document written into the test. The register is small -- the
snapshot of 20 November 2026 held 1,147 notifications and 4,613 child
documents -- so a snapshot is read on the driver and the rows are handed to
Spark whole, as the table of filings of
:mod:`fincommons.financialreport.edinet` is.

Three rules carry the reading, and each answers something the register does
not say.

**The identifier is trimmed here and nowhere earlier.** Three notifications of
that snapshot carry a single space in both identifier fields and eight more
carry a leading space. The Bronze layer keeps them as published, because it is
a record of what ESMA served; this layer is the prepared one, so it trims, and
an identifier that was nothing but space becomes null rather than a string that
looks like an identifier.

**A template field is told from a field of the notification by its prefix.**
The assertions against the STS criteria all begin with ``non_abcp`` or
``abcp``; everything else is a field of the notification itself. The ending
``_flag``, ``_comment``, ``_EXPL`` or their upper-case variants says which part
of an assertion a field holds, and what is left between the two is the
requirement, in the register's own spelling.

**A field this module does not know is carried into nothing, and said so.**
:func:`unmapped_fields` names the fields of a snapshot that neither become a
column nor an assertion, which is how a field the register adds is noticed
rather than silently dropped.
"""

import datetime
import json

from .common import (TEMPLATE_PREFIXES, TEMPLATE_SUFFIXES, identifier_scheme,
                     snapshot_pages)

#: The fields of a notification that become columns, by the column they become.
#: A field that is multivalued in the register is joined with a semicolon
#: rather than cut to its first value; the names of a securitisation and the
#: repositories it reports through are both multivalued.
NOTIFICATION_FIELDS = {
    "stsre_sec_name": "securitisation_name",
    "stsre_sectype_desc": "securitisation_type",
    "stsre_secclass_desc": "securitisation_class",
    "stsre_sectype_code": "type_code",
    "stsre_secclass_code": "class_code",
    "stsre_compl_stat_desc": "compliance_status",
    "stsre_sanc_status": "sanction_status",
    "stsre_undrlexp_desc": "underlying_exposure",
    "stsre_sec_repository": "repository",
    "de_name": "reporting_entity_name",
    "de_lei_priv": "reporting_entity_lei",
    "de_lei_name": "reporting_entity_legal_name",
    "authorised_third_party_name": "verifier_name",
    "authorised_third_party_country": "verifier_country",
    "authorised_third_party_competent_authority": "verifier_authority",
    "authorised_third_party_statement": "verifier_statement",
    "prospectus_country_name": "prospectus_country",
    "prospectus_identifier": "prospectus_identifier",
    "multiple_notif_flag": "notified_again",
    "multiple_notif_reason_desc": "notified_again_reason",
}

#: The fields a notification carries that this module reads but does not carry
#: into a column of its own: the identifiers and dates, which are read by name,
#: and the fields that duplicate something already held.
#:
#: ``sts_notification_information_notification_indentifier`` -- ESMA's
#: spelling -- equals ``stsre_notif_id`` on every notification that carries it,
#: and ``sts_notification_information_sts_compliance_status`` equals
#: ``stsre_compl_stat_desc``. ``concatenated_issins`` and
#: ``concatenated_originators`` are the ISINs and the originators of the child
#: documents, joined into a string; the children carry them better, with the
#: instrument name and the originator's country beside each, and the
#: ``first_five_`` fields are the same strings cut short. ``stsre_undrlexp_id``
#: is the code of the exposure type whose description is carried.
KNOWN_OTHER_FIELDS = frozenset({
    "id", "type_s", "entity_type", "_root_", "_version_", "timestamp",
    "stsre_sec_id", "stsre_notif_id", "stsre_init_notif_dt", "stsre_notif_dt",
    "prospectus_issue_date",
    "sts_notification_information_notification_indentifier",
    "sts_notification_information_sts_compliance_status",
    "concatenated_issins", "first_five_concatenated_issins",
    "concatenated_originators", "first_five_concatenated_originators",
    "stsre_undrlexp_id", "stsre_ntr_upd_description", "multiple_notif_comment",
    "prospectus_exemption", "comment",
})

#: The child documents, by the ``entity_type`` the register gives them, and
#: the prefix its fields carry. ``issins`` is the tranches and is read on its
#: own; the other four are parties and differ only in the prefix.
PARTY_KINDS = {
    "originator": "originator",
    "lender": "lender",
    "sponsor": "sponsor",
    "sspe": "sspe",
}


def text_of(value):
    """One value as text, as the register means it rather than as JSON holds it.

    A field the register serves as a number reaches Python as a float, and
    ``str`` then renders a prospectus identifier of 169403 as ``'169403.0'``.
    A float that is a whole number is therefore rendered without its
    fractional part, which is what every identifier and reference in the
    register is.
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def one(value):
    """One value of a field, whether the register gave one or several."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    text = text_of(value)
    return text if text else None


def joined(value, separator="; "):
    """Every value of a field, joined, so that none of a multivalued one is lost."""
    if value is None:
        return None
    values = value if isinstance(value, (list, tuple)) else [value]
    parts = [text_of(v) for v in values
             if v is not None and text_of(v) != ""]
    return separator.join(parts) if parts else None


def trimmed(value):
    """An identifier without the space around it, or ``None`` where that is all it was."""
    text = one(value)
    if text is None:
        return None
    text = text.strip()
    return text or None


def as_date(value):
    """The day of a date the register or a spreadsheet states.

    The register writes a date as ``2026-09-10T00:00:00Z`` in the fields it
    indexes and as ``2026-09-10 00:00:00.0`` in the prospectus issue date, so
    the day is taken from the front rather than the whole parsed. A
    spreadsheet gives a datetime, which carries its own day.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.date.fromisoformat(text[:10])
    except ValueError:
        return None


def requirement_of(field):
    """Split a template field into its template, requirement and part.

    Parameters
    ----------
    field: str
        The field name as the register spells it.

    Returns
    -------
    tuple or None
        ``(template, requirement, part)`` where the template is ``'non_abcp'``
        or ``'abcp'`` and the part is ``'flag'`` or ``'comment'``, or ``None``
        where the field is not a template field.
    """
    for template in TEMPLATE_PREFIXES:
        if field.startswith(template):
            rest = field[len(template):].lstrip("_")
            for ending, part in TEMPLATE_SUFFIXES.items():
                if rest.endswith(ending) and len(rest) > len(ending):
                    return template, rest[:-len(ending)], part
            # A field with no ending carries the assertion itself.
            return template, rest, "flag"
    return None


def documents(config_path, day, kind):
    """The documents of one kind the snapshot of a day holds.

    The pages are read in the order they were fetched, so that the documents
    come back in the order the register served them.
    """
    found = []
    for page in snapshot_pages(config_path, day, kind):
        with open(page, encoding="utf-8") as handle:
            answer = json.load(handle)
        found.extend((answer.get("response") or {}).get("docs") or [])
    return found


def notification_rows(config_path, day):
    """The notifications and their assertions, from one snapshot.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str
        The day of the snapshot.

    Returns
    -------
    tuple
        The rows of the table of notifications and the rows of the table of
        assertions, each in the order of its schema.
    """
    snapshot = as_date(day) if not isinstance(day, datetime.date) else day
    notifications = []
    assertions = []

    for document in documents(config_path, day, "parent"):
        identifier = trimmed(document.get("stsre_sec_id"))
        values = {column: None for column in NOTIFICATION_FIELDS.values()}
        for field, column in NOTIFICATION_FIELDS.items():
            values[column] = joined(document.get(field))

        notifications.append((
            one(document.get("id")),
            identifier,
            trimmed(document.get("stsre_notif_id")),
            identifier_scheme(identifier),
            values["securitisation_name"],
            values["securitisation_type"],
            values["securitisation_class"],
            values["type_code"],
            values["class_code"],
            values["compliance_status"],
            values["sanction_status"],
            values["underlying_exposure"],
            as_date(one(document.get("stsre_init_notif_dt"))),
            as_date(one(document.get("stsre_notif_dt"))),
            values["repository"],
            values["reporting_entity_name"],
            values["reporting_entity_lei"],
            values["reporting_entity_legal_name"],
            values["verifier_name"],
            values["verifier_country"],
            values["verifier_authority"],
            values["verifier_statement"],
            values["prospectus_country"],
            values["prospectus_identifier"],
            as_date(one(document.get("prospectus_issue_date"))),
            values["notified_again"],
            values["notified_again_reason"],
            len(document),
            snapshot,
        ))

        # The flag and the comment of one requirement meet in one row.
        made = {}
        for field, value in document.items():
            split = requirement_of(field)
            if split is None:
                continue
            template, requirement, part = split
            made.setdefault((template, requirement), {})[part] = one(value)
        for (template, requirement), parts in made.items():
            assertions.append((
                one(document.get("id")),
                identifier,
                template,
                requirement,
                parts.get("flag"),
                parts.get("comment"),
                snapshot,
            ))

    return notifications, assertions


def child_rows(config_path, day, identifiers=None):
    """The tranches and the parties, from one snapshot.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    day: datetime.date or str
        The day of the snapshot.
    identifiers: dict, optional
        The securitisation identifier of each notification, by the register's
        document identifier, so that a child carries the identifier of the
        notification it hangs off and a join to FIRDS or to the entity master
        data does not have to go through the table of notifications.

    Returns
    -------
    tuple
        The rows of the table of tranches and the rows of the table of
        parties, each in the order of its schema.
    """
    snapshot = as_date(day) if not isinstance(day, datetime.date) else day
    identifiers = identifiers or {}
    tranches = []
    parties = []

    for document in documents(config_path, day, "child"):
        parent = one(document.get("_root_"))
        identifier = identifiers.get(parent)
        kind = one(document.get("entity_type"))

        if kind == "issins":
            tranches.append((
                parent,
                identifier,
                trimmed(document.get("stsre_isin_instrument")),
                one(document.get("stsre_isin_name")),
                snapshot,
            ))
        elif kind in PARTY_KINDS:
            prefix = PARTY_KINDS[kind]
            # The special purpose entity has a country and nothing else, and
            # its country is under a different name from the other three.
            country = document.get(f"{prefix}_country")
            if country is None:
                country = document.get(f"{prefix}_country_code")
            parties.append((
                parent,
                identifier,
                kind,
                trimmed(document.get(f"{prefix}_lei")),
                one(document.get(f"{prefix}_name")),
                one(country),
                one(document.get(f"{prefix}_country_name")),
                snapshot,
            ))

    return tranches, parties


def unmapped_fields(config_path, day):
    """The fields of a snapshot that become neither a column nor an assertion.

    A field the register adds appears here rather than being dropped in
    silence. The count beside each is how many notifications carry it.
    """
    seen = {}
    for document in documents(config_path, day, "parent"):
        for field in document:
            if field in NOTIFICATION_FIELDS or field in KNOWN_OTHER_FIELDS:
                continue
            if requirement_of(field) is not None:
                continue
            seen[field] = seen.get(field, 0) + 1
    return dict(sorted(seen.items()))


# The spreadsheets
# --------------------------------------------------------------------------


def sheet_rows(path):
    """The rows of one of ESMA's spreadsheets, as dictionaries.

    Each sheet carries a block of disclaimer rows above its header -- the
    reference of the document, the day it was frozen and a link to the
    register that replaced it -- so the header is found as the widest row of
    the first twenty-five rather than assumed to be the first. A row below it
    that fills no more than one cell is a note rather than a record.
    """
    import pandas

    raw = pandas.read_excel(path, sheet_name=0, header=None, dtype=object)
    if raw.empty:
        return []

    filled = raw.notna().sum(axis=1)
    scan = filled.iloc[:min(25, len(filled))]
    header_at = int(scan.idxmax())
    header = [str(v).strip() if v is not None and v == v else f"col{i}"
              for i, v in enumerate(raw.iloc[header_at])]

    rows = []
    for _, record in raw.iloc[header_at + 1:].iterrows():
        values = list(record)
        if sum(1 for v in values if v is not None and v == v) <= 1:
            continue
        rows.append({name: (value if value is not None and value == value
                            else None)
                     for name, value in zip(header, values)})
    return rows


def sheet_identifier(row):
    """The securitisation identifier of a spreadsheet row, whatever it is called.

    The three lists name the column differently -- ``Unique Identifier`` on the
    frozen list of notifications, ``Unique Securitisation Identifier`` on the
    cancellations and ``Securitisation Unique Identifier`` on the synthetic
    list -- so the row is asked for each in turn.
    """
    for column in ("Unique Identifier", "Unique Securitisation Identifier",
                   "Securitisation Unique Identifier"):
        if column in row:
            value = trimmed(row[column])
            if value is not None:
                return value
    return None


def synthetic_rows(path, as_of):
    """The synthetic STS securitisations, from ESMA's list.

    Parameters
    ----------
    path: str
        The spreadsheet in the Bronze layer.
    as_of: datetime.date or str
        The day the copy was collected, which is what the list speaks for: it
        is current and ESMA republishes it.
    """
    as_of = as_date(as_of)
    rows = []
    for record in sheet_rows(path):
        identifier = sheet_identifier(record)
        if identifier is None:
            continue
        number = record.get("Notification number")
        rows.append((
            identifier,
            int(number) if isinstance(number, (int, float)) and number == number
            else None,
            as_date(record.get("Notification date to ESMA")),
            one(record.get("Private or Public")),
            one(record.get("Funded credit protection or unfunded credit "
                           "protection")),
            one(record.get("Credit derivatives or Financial guarantees")),
            one(record.get("Underlying assets")),
            one(record.get("Securitisation Name")),
            one(record.get("Originator Country")),
            one(record.get("Administrative sanctions present (YES/NO)")),
            as_of,
        ))
    return rows


# The events
# --------------------------------------------------------------------------


def register_events(notifications, snapshot):
    """What the register itself witnesses about the securitisations it holds.

    The register is read for three events. A notification was made, on the day
    it states as its first; it moved, where the day it last moved differs; and
    it is marked as not compliant, which the register states of ten of the
    1,147 notifications of the snapshot of 20 November 2026 and which no
    cancellation list records.
    """
    snapshot = as_date(snapshot)
    events = []
    for row in notifications:
        identifier, scheme = row[1], row[3]
        if identifier is None:
            continue
        status, first_notified, notified = row[9], row[12], row[13]
        if first_notified is not None:
            events.append((identifier, scheme, "notified", first_notified,
                           None, "register", snapshot))
        if notified is not None and notified != first_notified:
            events.append((identifier, scheme, "updated", notified, None,
                           "register", snapshot))
        if status is not None and status.strip().lower() != "compliant":
            events.append((identifier, scheme, "non_compliant", snapshot,
                           status, "register", snapshot))
    return events


def sheet_events(path, event, source, as_of, date_column, reason_column=None):
    """The events one of the frozen spreadsheets witnesses.

    Parameters
    ----------
    path: str
        The spreadsheet in the Bronze layer.
    event: str
        What the rows of this sheet say happened: ``'notified'`` or
        ``'cancelled'``.
    source: str
        The name the rows carry for where they came from, which is ESMA's
        reference for the document.
    as_of: datetime.date or str
        The day the sheet speaks for. Both of these were frozen in 2022 and
        will never move again, which is why it is recorded per row.
    date_column: str
        The column holding the day the event happened on.
    reason_column: str, optional
        The column holding ESMA's reason for it, where the sheet gives one.
    """
    as_of = as_date(as_of)
    events = []
    for record in sheet_rows(path):
        identifier = sheet_identifier(record)
        if identifier is None:
            continue
        reason = one(record.get(reason_column)) if reason_column else None
        events.append((
            identifier,
            identifier_scheme(identifier),
            event,
            as_date(record.get(date_column)),
            reason,
            source,
            as_of,
        ))
    return events


def departures(before, after, day):
    """The securitisations that one snapshot held and the next does not.

    This is the only record of a removal there is for anything that happened
    after May 2022, ESMA having frozen its cancellation list then and the
    register answering only for the day it is read. It says that a
    notification went and not why: the register gives no reason, and a
    departure is therefore recorded as a fact about two snapshots and nothing
    more.

    Parameters
    ----------
    before, after: iterable
        The rows of the table of notifications built from the earlier and the
        later snapshot.
    day: datetime.date or str
        The day of the later snapshot, which is when the departure was first
        seen and not necessarily when it happened.
    """
    day = as_date(day)
    held = {row[0] for row in after}
    events = []
    for row in before:
        if row[0] in held:
            continue
        identifier, scheme = row[1], row[3]
        if identifier is None:
            continue
        events.append((identifier, scheme, "left_register", day, None,
                       "snapshot", day))
    return events


#: What this module offers. Reading a snapshot and the spreadsheets into rows.
__all__ = [
    "KNOWN_OTHER_FIELDS",
    "NOTIFICATION_FIELDS",
    "PARTY_KINDS",
    "as_date",
    "child_rows",
    "departures",
    "documents",
    "joined",
    "notification_rows",
    "one",
    "register_events",
    "requirement_of",
    "sheet_events",
    "sheet_identifier",
    "sheet_rows",
    "synthetic_rows",
    "text_of",
    "trimmed",
    "unmapped_fields",
]
