"""Derive a Spark schema for an ECB data API answer from a real answer.

``xsd_to_spark_schema.py`` beside this file derives a schema from the schema
definition of a message, which is the better way round and is what the
regulatory XML sources of the package use. The ECB data API has no such
definition to derive from: it answers a ``format=csvdata`` request with an
SDMX flat file whose columns are the dimensions and attributes of the flow
that was asked for, so the set of columns and the order they come in are a
property of the flow and are only learned from an answer.

This script therefore takes an answer -- fetched here, or one saved earlier --
and writes the schema of it. What it reads off the answer is the *shape*: the
names of the columns and the order they appear in. The types come from a rule
rather than from the values, because an SDMX answer leaves the attributes it
has nothing to say about empty -- in the yield curve flow that was 20 of the
40 columns -- and inferring from such a sample would type a column from the
one delivery that happened to fill it.

The rule has three cases:

``TIME_PERIOD``
    a ``date``, see the note on the header line below.
``OBS_VALUE``
    a ``double``, see the note on numeric types in ``tools/README.md``.
everything else
    a ``string``, which is what keeps the Bronze layer the answer as it was
    published. Several of these columns hold numbers -- ``DECIMALS``,
    ``UNIT_MULT`` -- but they are SDMX attributes rather than observations,
    and reading them as text cannot null a record.

The header line
---------------

An answer carries its column names as its first line. A reader that parses it
with a declared schema and no ``header`` option -- which is how the package
reads these files -- takes that line as an ordinary record. Typing
``TIME_PERIOD`` as a date is what makes such a record recognisable: Spark
parses permissively, so the literal text ``TIME_PERIOD`` does not parse as a
date and the record is null in that one column, where every real observation
carries a date. Filtering on it is then enough to drop the header, and a
reader that does not filter keeps a row of column names in the Bronze layer.
Typing the column as a string would take that away.

Usage, from the root of the repository::

    python tools/ecb_csv_to_spark_schema.py --flow FM \\
        --out src/fincommons/data/marketpricedata_ecb_fm.json

It needs only the standard library, so it runs on the workstation and not in
the Docker image.
"""

import argparse
import csv
import io
import json
import sys
import urllib.parse
import urllib.request

#: Where a data flow is requested from. The path carries the flow and, after
#: it, an optional series key that narrows the dimensions.
BASE_URL = "https://data-api.ecb.europa.eu/service/data"

#: The columns that are not read as text, see the module docstring.
TIME_COLUMN = "TIME_PERIOD"
VALUE_COLUMN = "OBS_VALUE"

#: The API is slow to first byte for a request it has not cached. Four minutes
#: is comfortably more than the answers measured here needed.
TIMEOUT = 240


def build_url(flow, key=None, start=None, end=None):
    """The address of a ``csvdata`` request.

    Parameters
    ----------
    flow: str
        The data flow, e.g. ``YC`` for the yield curves, ``FM`` for the
        financial market series or ``EST`` for the euro short-term rate.
    key: str, optional
        A series key narrowing the dimensions of the flow, e.g.
        ``B.U2.EUR.4F.G_N_A.SV_C_YM.``. A trailing dot leaves the last
        dimension open. Where it is left out the whole flow is asked for,
        which is the larger answer but is also what a schema is best derived
        from, because a narrow key can leave an attribute empty that the flow
        does fill.
    start, end: str, optional
        The period to restrict the answer to, as ``YYYY-MM-DD``. It has no
        bearing on the schema and is worth giving only to keep the answer
        small.
    """
    path = f"{BASE_URL}/{flow}"
    if key:
        path += f"/{key}"

    query = [("format", "csvdata")]
    if start:
        query.append(("startPeriod", start))
    if end:
        query.append(("endPeriod", end))
    return path + "?" + urllib.parse.urlencode(query)


def fetch(url):
    """Fetch an answer of the data API.

    The request carries a user agent naming the package. The API answers a
    request without one, but a public service is entitled to know who is
    asking, and it is the one thing that makes a run of this script
    attributable in the ECB's logs.
    """
    request = urllib.request.Request(
        url, headers={"User-Agent": "fincommons (tools/ecb_csv_to_spark_schema.py)",
                      "Accept": "text/csv"}
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as answer:
        charset = answer.headers.get_content_charset() or "utf-8"
        return answer.read().decode(charset)


def columns_of(text):
    """The column names of an answer, in the order it carries them.

    Parameters
    ----------
    text: str
        The body of the answer.

    Returns
    -------
    tuple
        The names of the header line, and the number of data rows behind it,
        which is reported so that a run against an empty answer is noticed
        rather than written out as a schema.
    """
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise SystemExit("The answer is empty.")

    header = rows[0]
    body = rows[1:]

    widths = {len(row) for row in body}
    if widths - {len(header)}:
        raise SystemExit(
            f"The answer has rows of {sorted(widths)} fields against a header "
            f"of {len(header)}. It is not the flat csvdata format this script reads."
        )
    for required in (TIME_COLUMN, VALUE_COLUMN):
        if required not in header:
            raise SystemExit(f"The answer carries no {required} column.")

    return tuple(header), len(body)


def spark_type(column, value_type):
    """The Spark type one column is read as, see the module docstring."""
    if column == TIME_COLUMN:
        return "date"
    if column == VALUE_COLUMN:
        return value_type
    return "string"


def schema_of(columns, value_type):
    """The Spark schema of an answer with these columns."""
    return {
        "type": "struct",
        "fields": [
            {
                "name": column,
                "type": spark_type(column, value_type),
                "nullable": True,
                "metadata": {},
            }
            for column in columns
        ],
    }


def report(columns, rows, text, stream):
    """Print what the answer the schema was derived from contained.

    Which columns the flow leaves empty is the thing a reader of a generated
    schema wants to know, because it is what the typing rule turns on.
    """
    parsed = list(csv.reader(io.StringIO(text)))[1:]
    filled = {
        column: sum(1 for row in parsed if row[index] != "")
        for index, column in enumerate(columns)
    }
    empty = [column for column in columns if filled[column] == 0]

    print(f"{len(columns)} columns over {rows} data rows", file=stream)
    print(f"{len(empty)} of them empty in this answer: {', '.join(empty) or '-'}",
          file=stream)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--flow", help="the data flow to derive the schema from, "
                                       "e.g. YC, FM or EST")
    source.add_argument("--url", help="a request spelled out in full, instead of "
                                      "building one from --flow")
    source.add_argument("--csv", help="an answer saved earlier, instead of fetching one")
    parser.add_argument("--key", help="a series key narrowing --flow")
    parser.add_argument("--start", help="first day of the period asked for")
    parser.add_argument("--end", help="last day of the period asked for")
    parser.add_argument("--value-type", default="double", choices=("double", "string"),
                        help="the type of OBS_VALUE; 'string' leaves the parsing of "
                             "the observation to the Silver stage")
    parser.add_argument("--out", help="where to write the json; standard output "
                                      "if it is not given")
    args = parser.parse_args(argv)

    if args.csv:
        with open(args.csv, encoding="utf-8") as f:
            text = f.read()
    else:
        url = args.url or build_url(args.flow, args.key, args.start, args.end)
        print(f"fetching {url}", file=sys.stderr)
        text = fetch(url)

    columns, rows = columns_of(text)
    if rows == 0:
        raise SystemExit("The answer carries a header and no data. Ask for a "
                         "period the flow was published for.")

    report(columns, rows, text, sys.stderr)

    payload = json.dumps(schema_of(columns, args.value_type), indent=4) + "\n"
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="\n") as out:
            out.write(payload)
        print(f"written to {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(payload)


if __name__ == "__main__":
    main()
