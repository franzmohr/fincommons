"""Derive a spark-xml schema for an ISO 20022 message from its XSD.

The schemas in ``src/fincommons/data`` say how a regulatory XML file is to
be read into Spark. The older ones were inferred by spark-xml from sample
files, which is why their numeric types disagree between files: the same
field is a ``double`` in one and a ``long`` in another, depending on what the
sample happened to contain. This script derives the schema from the schema
definition instead, so that the result depends on the message rather than on
the sample, and so that a new message can be onboarded before any file of it
has been received.

An ISO 20022 XSD does not read as a tree. It declares one global element --
the root of the instance document -- and then a flat, alphabetically sorted
list of named types that refer to one another through ``type=``. The tree of
the XML file is recovered by starting at that global element and following
the type references down, which is what :meth:`Schema.type_of` does.

The output follows the conventions of the files already in the data
directory. The schema describes the *content* of the row element, so it is
used together with the matching ``rowTag``; attributes are prefixed with
``_``; the text of an element that also carries attributes is ``_VALUE``;
every field is nullable and carries an empty metadata dict; ``maxOccurs > 1``
becomes an array; and the branches of an ``xs:choice`` are flattened into
their parent struct as sibling nullable fields, because a file holds one
branch per record but the schema has to admit them all.

Usage, from the root of the repository::

    python tools/xsd_to_spark_schema.py path/to/auth.036.001.03.xsd \\
        --row-tag FinInstrm \\
        --out src/fincommons/data/firds_schema_036_001_03.json

The row tag is not derived automatically, because which element is a row is
a decision about the shape of the table rather than a fact about the
message. Run with ``--show-rows`` to print the candidates.
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET

XS = "{http://www.w3.org/2001/XMLSchema}"

#: Particles that group other particles without themselves being a level of
#: the XML tree, and are therefore flattened into the enclosing struct.
PARTICLES = ("sequence", "choice", "all", "complexContent", "extension",
             "restriction")

STRING_BASES = ("string", "normalizedString", "token", "NMTOKEN", "anyURI",
                "ID", "IDREF", "language", "Name", "NCName", "hexBinary",
                "base64Binary", "duration", "time", "gYear", "gMonth",
                "gDay", "gYearMonth", "gMonthDay")

INTEGER_BASES = ("integer", "nonNegativeInteger", "positiveInteger",
                 "negativeInteger", "nonPositiveInteger", "long",
                 "unsignedLong", "int", "unsignedInt", "short",
                 "unsignedShort", "byte", "unsignedByte")

#: The largest precision a Spark decimal can carry.
MAX_PRECISION = 38


def tag(elem):
    """The local name of an element of the schema document."""
    return elem.tag[len(XS):] if elem.tag.startswith(XS) else elem.tag


def field(name, spark):
    return {"name": name, "type": spark, "nullable": True, "metadata": {}}


def repeats(element):
    maximum = element.get("maxOccurs", "1")
    return maximum == "unbounded" or int(maximum) > 1


class Schema:
    """An XSD document, indexed by the name of each global declaration."""

    def __init__(self, path):
        self.root = ET.parse(path).getroot()
        self.simple = {}
        self.complex = {}
        self.elements = {}
        self.groups = {}
        for child in self.root:
            name = child.get("name")
            if name is None:
                continue
            if tag(child) == "simpleType":
                self.simple[name] = child
            elif tag(child) == "complexType":
                self.complex[name] = child
            elif tag(child) == "element":
                self.elements[name] = child
            elif tag(child) == "group":
                self.groups[name] = child

    # -- simple types ----------------------------------------------------
    def primitive(self, type_name, decimal_as):
        base, facets = self.resolve_simple(type_name)
        return self.spark_primitive(base, facets, decimal_as)

    def resolve_simple(self, type_name, facets=None, seen=None):
        """Follow a chain of simpleType restrictions down to an xs: base.

        ISO 20022 names every value type, and those names restrict one
        another rather than an ``xs:`` type directly, so the base and the
        facets that decide the Spark type are several steps away. The facets
        collected on the way down are the narrowest, because a restriction
        may only narrow what it restricts.
        """
        facets = {} if facets is None else facets
        seen = set() if seen is None else seen
        if type_name is None:
            return "string", facets
        if type_name.startswith("xs:"):
            return type_name[3:], facets
        if type_name in seen:
            return "string", facets
        seen.add(type_name)
        node = self.simple.get(type_name)
        if node is None:
            return "string", facets
        restriction = node.find(XS + "restriction")
        if restriction is None:
            # A union or a list; neither has a single Spark type.
            return "string", facets
        for facet in restriction:
            if tag(facet) in ("fractionDigits", "totalDigits"):
                facets.setdefault(tag(facet), int(facet.get("value")))
        return self.resolve_simple(restriction.get("base"), facets, seen)

    @staticmethod
    def spark_primitive(base, facets, decimal_as):
        if base in STRING_BASES:
            return "string"
        if base == "boolean":
            return "boolean"
        if base == "date":
            return "date"
        if base == "dateTime":
            return "timestamp"
        if base in INTEGER_BASES:
            return "long"
        if base in ("decimal", "float", "double"):
            if base == "decimal" and facets.get("fractionDigits") == 0:
                # Counters such as Number or Max20PositiveNumber are
                # decimals that are not allowed a fractional part.
                return "long"
            if base == "decimal" and decimal_as == "decimal":
                # The two XSD facets are independent maxima: a value may
                # carry up to totalDigits digits altogether and up to
                # fractionDigits of them after the point, so totalDigits
                # integer digits are legal when there is no fractional part.
                # Spark's precision counts the integer and the fractional
                # digits together, so the precision has to be the sum of the
                # two rather than totalDigits alone. Reading a plain amount
                # such as -1234567.89 against a decimal(25,19) derived the
                # naive way overflows, and because spark-xml reads
                # permissively the whole record is returned as nulls instead
                # of raising.
                fraction = facets.get("fractionDigits", 18)
                total = min(facets.get("totalDigits", 20) + fraction,
                            MAX_PRECISION)
                return "decimal(%d,%d)" % (total, min(fraction, total))
            return "double"
        return "string"

    # -- complex types ---------------------------------------------------
    def type_of(self, type_name, decimal_as, stack=()):
        if type_name in self.complex:
            return self.complex_type(type_name, decimal_as, stack)
        return self.primitive(type_name, decimal_as)

    def complex_type(self, type_name, decimal_as, stack):
        node = self.complex[type_name]
        simple_content = node.find(XS + "simpleContent")
        if simple_content is not None:
            return self.simple_content(simple_content, decimal_as)
        if type_name in stack:
            # A recursive definition: keep the branch as a string rather
            # than unrolling it for ever. None of the EMIR messages needs
            # this, but other ISO 20022 messages do.
            return "string"
        fields = []
        self.collect(node, fields, decimal_as, stack + (type_name,))
        if not fields:
            # An element with no content the schema describes, such as one
            # whose only child is xs:any.
            return "string"
        return {"type": "struct", "fields": fields}

    def simple_content(self, simple_content, decimal_as):
        """An element with a text value and attributes: _VALUE plus _attrs.

        This is how a monetary amount is written in ISO 20022: the number is
        the text of the element and the currency is an attribute of it.
        """
        holder = simple_content.find(XS + "extension")
        if holder is None:
            holder = simple_content.find(XS + "restriction")
        value = self.primitive(holder.get("base"), decimal_as)
        fields = [field("_" + attribute.get("name"),
                        self.primitive(attribute.get("type"), decimal_as))
                  for attribute in holder.findall(XS + "attribute")]
        if not fields:
            return value
        fields.append(field("_VALUE", value))
        return {"type": "struct", "fields": fields}

    def collect(self, node, fields, decimal_as, stack):
        """Walk a particle tree, flattening sequences, choices and groups."""
        for child in node:
            name = tag(child)
            if name in PARTICLES:
                self.collect(child, fields, decimal_as, stack)
            elif name == "element":
                self.element(child, fields, decimal_as, stack)
            elif name == "attribute":
                self.append(fields, field("_" + child.get("name"),
                                          self.primitive(child.get("type"),
                                                         decimal_as)))
            elif name == "group":
                group = self.groups.get(child.get("ref"))
                if group is not None:
                    self.collect(group, fields, decimal_as, stack)

    def element(self, element, fields, decimal_as, stack):
        ref = element.get("ref")
        if ref is not None:
            element = self.elements[ref]
        type_name = element.get("type")
        if type_name is not None:
            spark = self.type_of(type_name, decimal_as, stack)
        else:
            spark = self.inline_type(element, decimal_as, stack)
        if repeats(element):
            spark = {"type": "array", "elementType": spark,
                     "containsNull": True}
        self.append(fields, field(element.get("name"), spark))

    def inline_type(self, element, decimal_as, stack):
        inline = element.find(XS + "complexType")
        if inline is None:
            simple = element.find(XS + "simpleType")
            if simple is None:
                return "string"
            restriction = simple.find(XS + "restriction")
            base = restriction.get("base") if restriction is not None else None
            return self.primitive(base, decimal_as)
        simple_content = inline.find(XS + "simpleContent")
        if simple_content is not None:
            return self.simple_content(simple_content, decimal_as)
        nested = []
        self.collect(inline, nested, decimal_as, stack)
        return {"type": "struct", "fields": nested} if nested else "string"

    @staticmethod
    def append(fields, new):
        """Keep the first of two same named siblings, as spark-xml does."""
        if not any(existing["name"] == new["name"] for existing in fields):
            fields.append(new)

    # -- finding the row -------------------------------------------------
    def rows(self):
        """Every element reachable from a global element, breadth first.

        Yields ``(depth, path, name, type_name, repeating)``. Searching from
        the root rather than over the flat list of complex types matters
        because the same element name is declared in several of them, and
        only the position in the tree tells them apart.
        """
        queue = [(0, (), name, element.get("type"))
                 for name, element in self.elements.items()]
        seen = set()
        while queue:
            depth, path, name, type_name = queue.pop(0)
            node = self.complex.get(type_name)
            if node is None or type_name in seen:
                continue
            seen.add(type_name)
            for child in node.iter(XS + "element"):
                child_name = child.get("name")
                yield (depth + 1, path + (name,), child_name,
                       child.get("type"), repeats(child))
                queue.append((depth + 1, path + (name,), child_name,
                              child.get("type")))

    def row_element(self, row_tag):
        """The shallowest element named ``row_tag``, and its type name."""
        if row_tag in self.elements:
            return self.elements[row_tag].get("type"), ()
        for depth, path, name, type_name, _ in self.rows():
            if name == row_tag:
                return type_name, path
        return None, ()

    def ambiguous(self, row_tag):
        """Places other than the row where ``row_tag`` is also declared.

        A reused name is worth reporting, because the row tag decides where
        one record of the table ends. A name nested *inside* the row is
        harmless, which covers every case in these messages: the reader
        matches the outer element and consumes the file to its closing tag,
        so ``rowTag="Tx"`` gives one row per ``Tx`` of ``FinInstrmRptgTxRpt``
        even though auth.016.001.01 declares another ``Tx`` inside ``New``,
        and ``rowTag="Rpt"`` gives one row per counterparty in
        auth.106.001.01 even though ``Rpt`` recurs under ``MssngValtn``,
        ``MssngMrgnInf`` and ``AbnrmlVals``. The case that would not be
        harmless is a name reused on a branch the row does not contain, and
        that is what the warning asks to be checked.
        """
        places = [path + (name,) for _, path, name, _, _ in self.rows()
                  if name == row_tag]
        return places[1:]


def sort_fields(spark):
    """Order the fields of every struct alphabetically."""
    if not isinstance(spark, dict):
        return
    if spark["type"] == "struct":
        spark["fields"].sort(key=lambda f: f["name"])
        for child in spark["fields"]:
            sort_fields(child["type"])
    elif spark["type"] == "array":
        sort_fields(spark["elementType"])


def show_rows(schema):
    """Print the elements that could be a row, shallowest first."""
    print("%-6s %-34s %-44s %s" % ("depth", "path", "type", "repeating"))
    for depth, path, name, type_name, repeating in schema.rows():
        if depth > 3:
            continue
        print("%-6d %-34s %-44s %s" % (
            depth, "/".join(path + (name,))[-34:], type_name,
            "yes" if repeating else ""))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("xsd", help="the XSD of the message")
    parser.add_argument("--row-tag",
                        help="the element one row of the table is read from")
    parser.add_argument("--show-rows", action="store_true",
                        help="print the candidate row elements and exit")
    parser.add_argument("--decimal-as", choices=["double", "decimal"],
                        default="decimal",
                        help="how to read an xs:decimal (default: decimal, "
                             "which keeps the precision the XSD declares)")
    parser.add_argument("--sort", action="store_true",
                        help="order the fields of every struct alphabetically "
                             "instead of in the order of the XSD")
    parser.add_argument("--out", help="where to write the json; "
                                      "the default is standard output")
    arguments = parser.parse_args(argv)

    schema = Schema(arguments.xsd)

    if arguments.show_rows:
        show_rows(schema)
        return 0
    if arguments.row_tag is None:
        parser.error("--row-tag is required unless --show-rows is given")

    type_name, path = schema.row_element(arguments.row_tag)
    if type_name is None:
        parser.error("row tag %r is not declared in this schema"
                     % arguments.row_tag)
    for place in schema.ambiguous(arguments.row_tag):
        nested = "/".join(place).startswith("/".join(path + (row,)))             if (row := arguments.row_tag) else False
        print("%s: %r is also declared at %s%s"
              % ("note" if nested else "warning", arguments.row_tag,
                 "/".join(place),
                 ", inside the row itself, which the reader handles"
                 if nested else
                 ", which is not inside the row; check that one record of "
                 "the table really ends where it should"),
              file=sys.stderr)

    spark = schema.type_of(type_name, arguments.decimal_as)
    if not isinstance(spark, dict) or spark["type"] != "struct":
        parser.error("row tag %r has no fields of its own" % arguments.row_tag)
    if arguments.sort:
        sort_fields(spark)

    text = json.dumps(spark, indent=4) + "\n"
    if arguments.out:
        with open(arguments.out, "w", encoding="utf-8") as out:
            out.write(text)
        print("%s: %s (%s) -> %s" % (
            arguments.row_tag, "/".join(path + (arguments.row_tag,)),
            type_name, arguments.out), file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
