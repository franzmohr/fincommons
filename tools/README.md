# Tools for deriving Spark schemas

Two scripts write the JSON schema files that
`fincommons.schemas.load_pyspark_schema` reads. Spark uses such a file to read
a source file with fixed column names and types instead of guessing them. The
files sit in `src/fincommons/data`, or in the data directory of any package
built on fincommons.

- `xsd_to_spark_schema.py` derives a schema from the XSD (XML Schema
  Definition) of an ISO 20022 message. That covers every regulatory XML
  source, and is most of this document.
- `ecb_csv_to_spark_schema.py` derives one from an answer of the ECB data
  API, which has no schema definition to derive from.

At a glance:

- **Needs:** only the Python standard library, so both run on the
  workstation without Spark.
- **Network:** `xsd_to_spark_schema.py` none; `ecb_csv_to_spark_schema.py`
  fetches from `https://data-api.ecb.europa.eu` unless given a saved answer
  with `--csv`.
- **Writes:** one JSON file with `--out`, or standard output without it.
  Progress, warnings and reports go to standard error.
- **Run from:** the root of the repository, as in every example below.

The folder also holds `rebuild_silver.py`, a one-off for lakes written before
the column rename of 0.5.0; it has a section of its own at the end.

## Quick start

### From an ISO 20022 XSD

First list the elements that could be a row of the table, then derive the
schema for the one you choose:

```bash
python tools/xsd_to_spark_schema.py path/to/auth.036.001.03.xsd --show-rows

python tools/xsd_to_spark_schema.py path/to/auth.036.001.03.xsd \
    --row-tag FinInstrm \
    --out src/fincommons/data/firds_schema_036_001_03.json
```

The XSD is not shipped with fincommons; download it from the publisher of
the message. The second command writes
`src/fincommons/data/firds_schema_036_001_03.json` and reports on standard
error which element it used:

```
FinInstrm: Document/.../FinInstrm (<type>) -> src/fincommons/data/firds_schema_036_001_03.json
```

Read a file with the schema and the same row tag:

```python
from fincommons.schemas import load_pyspark_schema

df = spark.read.format("xml") \
    .options(rowTag="FinInstrm") \
    .schema(load_pyspark_schema("firds_schema_036_001_03")) \
    .load("path/to/file.xml")
```

### From an ECB data API answer

```bash
python tools/ecb_csv_to_spark_schema.py --flow EST --out src/fincommons/data/marketpricedata_ecb_estr.json
python tools/ecb_csv_to_spark_schema.py --flow FM  --out src/fincommons/data/marketpricedata_ecb_fm.json
```

Each command fetches the whole flow, reports on standard error how many
columns and data rows the answer had and which columns it left empty, and
writes the schema to the file given with `--out`. These are the schemas the
ECB sources of `fincommons.marketpricedata` read their downloads with
(`load_pyspark_schema("marketpricedata_ecb_estr")` and
`load_pyspark_schema("marketpricedata_ecb_fm")`).

The API is slow to first byte for a request it has not cached; the script
waits up to four minutes.

## Common tasks

### Choose the row tag of an XSD

The row tag is the XML element that becomes one row of the table. The script
does not guess it; run `--show-rows` first:

```bash
python tools/xsd_to_spark_schema.py path/to/message.xsd --show-rows
```

It prints the elements down to depth 3 with their path, type and whether
they repeat. **The repeating element under the message root is usually the
one.** Without `--show-rows`, `--row-tag` is required.

### Check a warning about a reused element name

If the row tag is declared in more than one place, the script says so on
standard error:

- `note: ... inside the row itself, which the reader handles` -- harmless,
  nothing to do.
- `warning: ... which is not inside the row; check that one record of the
  table really ends where it should` -- look at the XSD before using the
  schema.

See [Choosing the row tag](#choosing-the-row-tag) for why.

### Decimal or double

Leave the default, `--decimal-as decimal`, for anything you load: regulatory
amounts need the precision. Use `--decimal-as double` only to compare with
a schema inferred from sample files:

```bash
python tools/xsd_to_spark_schema.py path/to/message.xsd --row-tag Tx \
    --decimal-as double > message_as_double.json
```

### Fields in alphabetical order

By default the fields of every struct follow the order of the XSD. `--sort`
orders them alphabetically instead, which makes two schemas easier to diff.

### Narrow an ECB request, or reuse a saved answer

```bash
# a series key and a period
python tools/ecb_csv_to_spark_schema.py --flow YC --key B.U2.EUR.4F.G_N_A.SV_C_YM. \
    --start 2024-01-01 --end 2024-01-31 --out yc.json

# a request spelled out in full
python tools/ecb_csv_to_spark_schema.py \
    --url "https://data-api.ecb.europa.eu/service/data/EST?format=csvdata" --out estr.json

# an answer saved earlier, without fetching
python tools/ecb_csv_to_spark_schema.py --csv saved_answer.csv --out estr.json
```

A trailing dot in a key leaves the last dimension open. The period has no
bearing on the schema and only keeps the answer small.

**Derive from the whole flow rather than from a narrow key** where the
answer is small enough to fetch. The columns are the same either way, but a
narrow key can leave an attribute empty that the flow does fill -- and what
is empty is what the report on standard error is for.

### Keep the ECB observation as text

`--value-type string` types `OBS_VALUE` as a string and leaves its parsing
to the Silver stage, where that is wanted.

## How it works

### Deriving from an ISO 20022 XSD

#### Why derive rather than infer

**A schema derived from the XSD depends on the message, not on a sample.**

A schema inferred by spark-xml from sample files has two defects:

1. Numeric types depend on what the sample happened to contain, so the same
   field is a `double` in one file and a `long` in another.
2. A message cannot be onboarded before a file of it has been received.

Deriving from the schema definition avoids both.

The inferred typing is not merely untidy. Reading an XML file against a
schema that types a field too narrowly does not raise: spark-xml reads
permissively, so the record that does not fit comes back as nulls in *every*
field, the identifier included. A MiFIR transaction with a fractional price
read against an inferred schema that types every numeric field of the
transaction as `long` is returned as an all-null row.

#### Choosing the row tag

**Which element is a row is a decision about the shape of the table rather
than a fact about the message**, so the script leaves it to you.

A name declared in more than one place is reported:

- A second declaration *inside* the row is harmless. The reader matches the
  outer element and consumes the file to its closing tag. Both
  auth.016.001.01 (`Tx` inside `New`) and auth.106.001.01 (`Rpt` inside
  `MssngValtn`) were measured to read correctly.
- A name reused on a branch the row does not contain would not be harmless,
  and is what the warning asks to be checked.

The schemas of the supervisory reporting regimes -- EMIR, MiFIR, SFTR and
SECR -- are not shipped with fincommons; a package that ships them derives
them with this script.

#### Numeric types

**An `xs:decimal` becomes a Spark `decimal` by default**, because these are
regulatory amounts and a `double` cannot hold the 25 significant digits the
messages declare.

The precision is `totalDigits + fractionDigits`, capped at Spark's maximum
of 38, and the scale is `fractionDigits`:

- The two XSD facets are independent maxima -- at most `totalDigits` digits
  altogether and at most `fractionDigits` of them after the point -- so
  `totalDigits` integer digits are legal when there is no fractional part.
- Spark's precision counts the integer and the fractional digits together.
- So translating `totalDigits="25" fractionDigits="19"` into
  `decimal(25,19)` would leave room for only six integer digits, and an
  ordinary amount such as `-1234567.89` would overflow it and null the whole
  record.

A decimal with `fractionDigits="0"`, which is how ISO 20022 spells a counter
such as `Number` or `Max20PositiveNumber`, becomes a `long`.

The full mapping is in [Reference](#xsd-types-and-spark-types).

#### How the XSD is turned into a tree

An ISO 20022 XSD does not read as a tree. It declares one global element --
the root of the instance document -- and then a flat, alphabetically sorted
list of named types that refer to one another through `type=`. The script
recovers the tree by starting at that global element and following the type
references down. ISO 20022 value types restrict one another rather than an
`xs:` type directly, so the script follows each chain of restrictions to its
`xs:` base and keeps the narrowest facets on the way.

### Deriving from an ECB data API answer

#### Why it takes an answer

**The columns of an ECB answer are a property of the flow, and are only
learned from an answer.** A `format=csvdata` request is answered with an
SDMX (Statistical Data and Metadata eXchange) flat file whose columns are the
dimensions and attributes of the flow that was asked for. There is no schema
definition to derive the set of columns and their order from, so the script
takes an answer -- fetched, or saved earlier.

#### Why the types come from a rule and not from the answer

**The script reads the shape off the answer -- the column names and their
order -- and takes the types from a rule.** An SDMX answer leaves the
attributes it has nothing to say about empty -- in the yield curve flow, 20
of the 40 columns -- and inferring from such a sample would type a column
from the one delivery that happened to fill it.

The rule:

| Column | Spark type | Why |
|---|---|---|
| `TIME_PERIOD` | `date` | Makes a header line recognisable, see [The header line](#the-header-line). |
| `OBS_VALUE` | `double` (or `string` with `--value-type string`) | See below. |
| everything else | `string` | Keeps the Bronze layer (the raw data as downloaded) the answer as it was published. |

Several of the string columns hold numbers -- `DECIMALS`, `UNIT_MULT` -- but
they are SDMX attributes rather than observations, and reading them as text
cannot null a record.

**`OBS_VALUE` is a `double` rather than a `decimal`**, the opposite of the
choice made for the regulatory amounts above, because there is nothing to
size a decimal from:

- SDMX declares no precision for an observation.
- The `DECIMALS` attribute is a display hint that the values contradict: it
  says 6 for the yield curve parameters, which carry 10 fractional digits.
- An over-narrow decimal nulls the whole record.

A published market rate is a measurement rather than an amount someone is
owed, and the ten significant digits the ECB publishes fit a double with
room to spare.

#### The header line

**Typing `TIME_PERIOD` as a date lets a reader tell the header line from the
data.** An answer carries its column names as its first line. A reader that
parses it with a declared schema and no `header` option takes that line as
an ordinary record. Spark parses permissively, the literal text
`TIME_PERIOD` does not parse as a date, and the record is null in that one
column where every real observation carries a date.

Filtering on that column is then enough to drop the header. A reader that
does not filter keeps a row of column names in the Bronze layer, and where
the import partitions by the reference date it gets a
`TIME_PERIOD=TIME_PERIOD` partition beside the real ones. Typing the column
as a string would take the means of telling them apart away altogether.

The Bronze import in `fincommons.marketpricedata` (`bronze_ecb.py`) reads
with `header` set to `true`, so it skips the line; the date typing is the
safety net for any reader that does not.

## Reference

### `xsd_to_spark_schema.py`

```
python tools/xsd_to_spark_schema.py XSD [--row-tag TAG | --show-rows]
    [--decimal-as {decimal,double}] [--sort] [--out FILE]
```

| Option | What it does | Default |
|---|---|---|
| `XSD` (positional) | The XSD of the message. | required |
| `--row-tag TAG` | The element one row of the table is read from. Use the same value as the `rowTag` option when reading. An error if the name is not declared in the XSD, or if the element has no fields of its own. | required unless `--show-rows` |
| `--show-rows` | Print the candidate row elements (depth up to 3, path, type, repeating) and exit. | off |
| `--decimal-as {decimal,double}` | How to read an `xs:decimal`. `decimal` keeps the precision the XSD declares. | `decimal` |
| `--sort` | Order the fields of every struct alphabetically instead of in the order of the XSD. | off |
| `--out FILE` | Where to write the JSON. | standard output |

#### Shape of the output

The output follows the conventions of the files already in the data
directory:

- The schema describes the *content* of the row element, so it is used
  together with the matching `rowTag`.
- Attributes are prefixed with `_`; the text of an element that also carries
  attributes is `_VALUE`. A monetary amount, for example, becomes a struct of
  `_Ccy` and `_VALUE`.
- Every field is nullable and carries an empty metadata dict.
- An element with `maxOccurs` greater than 1 becomes an array.
- The branches of an `xs:choice` are flattened into their parent struct as
  sibling nullable fields, because a file holds one branch per record but the
  schema has to admit them all.
- Of two siblings with the same name, the first is kept, as spark-xml does.
- A recursive type, and an element with no content the schema describes
  (such as one whose only child is `xs:any`), becomes a `string`.

#### XSD types and Spark types

| XSD base type | Spark type |
|---|---|
| `xs:decimal` with `fractionDigits="0"` | `long` |
| `xs:decimal`, `--decimal-as decimal` | `decimal(min(totalDigits + fractionDigits, 38), fractionDigits)`; a missing facet counts as `totalDigits` 20, `fractionDigits` 18 |
| `xs:decimal` with `--decimal-as double`, `xs:float`, `xs:double` | `double` |
| integer types (`xs:integer`, `xs:int`, `xs:long`, `xs:positiveInteger`, ...) | `long` |
| `xs:boolean` | `boolean` |
| `xs:date` | `date` |
| `xs:dateTime` | `timestamp` |
| string-like types (`xs:string`, `xs:token`, `xs:anyURI`, `xs:time`, `xs:gYear`, ...), unions, lists, anything else | `string` |

### `ecb_csv_to_spark_schema.py`

```
python tools/ecb_csv_to_spark_schema.py (--flow FLOW | --url URL | --csv FILE)
    [--key KEY] [--start DATE] [--end DATE] [--value-type {double,string}] [--out FILE]
```

Exactly one of `--flow`, `--url` and `--csv` is required.

| Option | What it does | Default |
|---|---|---|
| `--flow FLOW` | The data flow to derive the schema from, e.g. `YC` (yield curves), `FM` (financial market series) or `EST` (euro short-term rate). The request goes to `https://data-api.ecb.europa.eu/service/data/<FLOW>[/<KEY>]?format=csvdata`. | -- |
| `--url URL` | A request spelled out in full, instead of building one from `--flow`. | -- |
| `--csv FILE` | An answer saved earlier, read instead of fetching one. | -- |
| `--key KEY` | A series key narrowing `--flow`, e.g. `B.U2.EUR.4F.G_N_A.SV_C_YM.`. Used with `--flow` only. | whole flow |
| `--start DATE` | First day of the period asked for, `YYYY-MM-DD` (`startPeriod`). Used with `--flow` only. | -- |
| `--end DATE` | Last day of the period asked for, `YYYY-MM-DD` (`endPeriod`). Used with `--flow` only. | -- |
| `--value-type {double,string}` | The type of `OBS_VALUE`; `string` leaves the parsing of the observation to the Silver stage. | `double` |
| `--out FILE` | Where to write the JSON. | standard output |

The script stops without writing a schema if the answer is empty, carries a
header and no data ("Ask for a period the flow was published for"), has rows
of a different width from its header (not the flat `csvdata` format), or
lacks a `TIME_PERIOD` or `OBS_VALUE` column. It sends a user agent naming
fincommons and the script, and waits up to 240 seconds for an answer.

### Related code

- `fincommons.schemas.load_pyspark_schema(schema, package=None)` -- loads
  `<schema>.json` from the `data` directory of fincommons, or of `package`.
- The schema files: `src/fincommons/data/firds_schema_*.json`,
  `src/fincommons/data/marketpricedata_ecb_estr.json`,
  `src/fincommons/data/marketpricedata_ecb_fm.json`.

# Rebuilding the Silver layer after the column rename

`rebuild_silver.py` writes the Silver tables (the cleaned tables analyses
read) again from Bronze. A lake written before the rename of 0.5.0 carries
the old column names, so the code after it cannot read the lake at all: the
Quarto reports of the financial report modules fail on their first table.
The coverage report was worse than a failure -- its header set
`error: true`, so it rendered a PDF whose pages were
`object 'period_end' not found` and exited 0 -- which is why every template
now names `error: false` and says in the header why.

Bronze is the source and the rename did not touch it, so nothing is lost by
writing Silver again.

## Quick start

```bash
python tools/rebuild_silver.py exchangerate
python tools/rebuild_silver.py esef --clean
python tools/rebuild_silver.py sec
python tools/rebuild_silver.py combined
```

`combined` reads what the others write and goes last. Run one stage at a
time: the workstation has under 6 GB of RAM and the session is sized for it
(single-threaded, small driver heap). A machine with more can raise both
with `--master` and `--driver-memory`.

The script reads the configuration from `~/data/fincommons/paths.yaml`, or
from the file named in the environment variable `FINCOMMONS_CONFIG`.

## Why the first esef rebuild has to remove something

**Without `--clean`, a rebuilt facts table still reads as though it had not
been rebuilt.**

The facts table is partitioned by country and rebuilt a partition at a time,
and the partition column was renamed along with the rest. So the rebuild
writes `country=AT` into a table that already holds `COUNTRY=AT`, and
`replace_silver_partitions` replaces partitions of the column it is given
and knows nothing of the other spelling. Both directories survive.

The table then holds each rebuilt country twice under two schemas, and arrow
resolves the dataset to whichever file it reads first. Measured on a lake
with ten countries rebuilt, `open_dataset` on the facts table returned the
pre-rename names plus both partition columns:

    FILING LEI PERIODEND FACT CONCEPT ... COUNTRY country

`--clean` removes the `COUNTRY=` directories and leaves everything else,
which is why it is not the same as emptying the table. The script refuses to
run the `esef` stage while such directories remain, unless `--clean` or
`--countries` is given.

`addressing.rename_folders_to_snake_case` does not help here: it renames
hyphenated table folders and skips any name containing `=`, so partition
directories are outside it. Renaming them by hand would not help either,
since the columns inside the files of a country not yet rebuilt are still
upper case.

## Resuming

`--countries` rebuilds the countries named and cleans nothing, for
continuing a run that was interrupted:

```bash
python tools/rebuild_silver.py esef --countries GR,HR,HU
```

Which countries are already done can be read off the partition directories:
a `country=XX` is rebuilt, a `COUNTRY=XX` is not.

## Reference

| Option | What it does | Default |
|---|---|---|
| `stage` (positional) | `exchangerate`, `esef`, `sec` or `combined`. | required |
| `--countries LIST` | `esef` only: comma-separated countries to rebuild; resumes an interrupted rebuild and does not clean. | all |
| `--clean` | `esef` only: remove the pre-rename `COUNTRY=` partitions of the facts table before writing it, which the first rebuild of a pre-rename lake needs. | off |
| `--master` | Spark master. | `local[1]` |
| `--driver-memory` | Spark driver heap. | `1500m` |
| `--local-dir` | Spark scratch directory, kept off `/tmp` (a tmpfs on the workstation, where a large shuffle ends in "No space left on device"). | `~/data/fincommons/spark-tmp` |
