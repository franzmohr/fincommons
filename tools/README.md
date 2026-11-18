# Deriving Spark schemas

Two scripts write the schema files that `fincommons.schemas.load_pyspark_schema`
reads, in `src/fincommons/data` and in the data directory of any package
built on fincommons. `xsd_to_spark_schema.py` derives a schema from the XSD of
an ISO 20022 message, which is every regulatory XML source and most of this
document.
`ecb_csv_to_spark_schema.py` derives one from an answer of the ECB data API,
which has no schema definition to derive from; it has a section of its own at
the end.

## Deriving from an ISO 20022 XSD

`xsd_to_spark_schema.py` needs only the standard library, so it runs on the
workstation without Spark.

### Why derive rather than infer

A schema inferred by spark-xml from sample files has two defects. Numeric
types depend on what the sample happened to contain, so the same field is a
`double` in one file and a `long` in another; and a message cannot be
onboarded before a file of it has been received. Deriving from the schema
definition avoids both.

The inferred typing is not merely untidy. Reading an XML file against a
schema that types a field too narrowly does not raise: spark-xml reads
permissively, so the record that does not fit comes back as nulls in *every*
field, the identifier included. A MiFIR transaction with a fractional price
read against an inferred schema that types every numeric field of the
transaction as `long` is returned as an all-null row.

### Choosing the row tag

The script does not guess the row tag, because which element is a row is a
decision about the shape of the table rather than a fact about the message.
`--show-rows` prints the candidates with their depth and whether they repeat;
the repeating element under the message root is usually the one.

A name declared in more than one place is reported. A second declaration
*inside* the row is harmless -- the reader matches the outer element and
consumes the file to its closing tag -- and both auth.016.001.01 (`Tx` inside
`New`) and auth.106.001.01 (`Rpt` inside `MssngValtn`) were measured to read
correctly. A name reused on a branch the row does not contain would not be
harmless, and is what the warning asks to be checked.

The schemas of the supervisory reporting regimes -- EMIR, MiFIR, SFTR and
SECR -- are not shipped with fincommons; a package that ships them derives
them with the script described here.

### Numeric types

An `xs:decimal` becomes a Spark `decimal` by default, because these are
regulatory amounts and a `double` cannot hold the 25 significant digits the
messages declare. `--decimal-as double` is available for comparison with a
schema inferred from sample files.

The precision is `totalDigits + fractionDigits`, capped at Spark's maximum of
38, and the scale is `fractionDigits`. The two XSD facets are independent
maxima -- at most `totalDigits` digits altogether and at most
`fractionDigits` of them after the point -- so `totalDigits` integer digits
are legal when there is no fractional part. Spark's precision counts the
integer and the fractional digits together, so translating
`totalDigits="25" fractionDigits="19"` into `decimal(25,19)` leaves room for
only six integer digits, and an ordinary amount such as `-1234567.89`
overflows it and nulls the whole record.

A decimal with `fractionDigits="0"`, which is how ISO 20022 spells a counter
such as `Number` or `Max20PositiveNumber`, becomes a `long`.

## Deriving from an ECB data API answer

`ecb_csv_to_spark_schema.py` writes the schemas that the ECB sources of
`fincommons.marketpricedata` read their downloads with. It needs only the
standard library.

The ECB data API has no schema definition to derive from. A `format=csvdata`
request is answered with an SDMX flat file whose columns are the dimensions
and attributes of the flow that was asked for, so the set of columns and the
order they come in are a property of the flow and are only learned from an
answer. The script therefore takes one:

```
python tools/ecb_csv_to_spark_schema.py --flow EST --out src/fincommons/data/marketpricedata_ecb_estr.json
python tools/ecb_csv_to_spark_schema.py --flow FM  --out src/fincommons/data/marketpricedata_ecb_fm.json
```

`--key` narrows the flow to a series key, `--start` and `--end` restrict the
period, `--url` takes a request spelled out in full, and `--csv` reads an
answer saved earlier instead of fetching one. The API is slow to first byte
for a request it has not cached; the script waits four minutes.

Derive from the whole flow rather than from a narrow key where the answer is
small enough to fetch. The columns are the same either way, but a narrow key
can leave an attribute empty that the flow does fill, and what is empty is
what the report on standard error is for.

### Why the types come from a rule and not from the answer

What the script reads off the answer is the *shape*: the names of the columns
and the order they appear in. The types come from a rule, because an SDMX
answer leaves the attributes it has nothing to say about empty -- in the yield
curve flow, 20 of the 40 columns -- and inferring from such a sample
would type a column from the one delivery that happened to fill it.

`TIME_PERIOD` becomes a `date`, `OBS_VALUE` a `double`, and everything else a
`string`, which keeps the Bronze layer the answer as it was published. Several
of the remaining columns hold numbers -- `DECIMALS`, `UNIT_MULT` -- but they
are SDMX attributes rather than observations, and reading them as text cannot
null a record.

`OBS_VALUE` is a `double` rather than a `decimal`, which is the opposite of
the choice made for the regulatory amounts above. The reason is that there is
nothing to size a decimal from: SDMX declares no precision for an observation,
the `DECIMALS` attribute is a display hint that the values contradict -- it
says 6 for the yield curve parameters, which carry 10 fractional digits -- and
an over-narrow decimal nulls the whole record. A published market rate is a
measurement rather than an amount someone is owed, and the ten significant
digits the ECB publishes fit a double with room to spare. `--value-type
string` leaves the parsing to the Silver stage where that is wanted.

### The header line

An answer carries its column names as its first line, and the package reads
these files with a declared schema and no `header` option, so the header
arrives as an ordinary record. Typing `TIME_PERIOD` as a date is what makes
such a record recognisable: Spark parses permissively, the literal text
`TIME_PERIOD` does not parse as a date, and the record is null in that one
column where every real observation carries a date.

Filtering on that column is then enough to drop the header. A reader that does
not filter keeps a row of column names in the Bronze layer, and where the
import partitions by the reference date it gets a `TIME_PERIOD=TIME_PERIOD`
partition beside the real ones. Typing the column as a string would take the
means of telling them apart away altogether.

# Rebuilding the Silver layer after the column rename

`rebuild_silver.py` writes the Silver tables again from Bronze. A lake written
before the rename of 0.5.0 carries the old column names, so the code after it
cannot read the lake at all: the three Quarto reports of `financialreport`
fail on their first table, and the coverage report, whose header sets
`error: true`, renders a PDF of error messages and exits 0.

Bronze is the source and the rename did not touch it, so nothing is lost by
writing Silver again.

    python tools/rebuild_silver.py exchangerate
    python tools/rebuild_silver.py esef --clean
    python tools/rebuild_silver.py sec
    python tools/rebuild_silver.py combined

`combined` reads what the others write and goes last. One stage at a time: the
workstation has under 6 GB of RAM and the session is sized for it.

## Why the first esef rebuild has to remove something

The facts table is partitioned by country and rebuilt a partition at a time,
and the partition column was renamed along with the rest. So the rebuild
writes `country=AT` into a table that already holds `COUNTRY=AT`, and
`replace_silver_partitions` replaces partitions of the column it is given and
knows nothing of the other spelling. Both directories survive.

The table then holds each rebuilt country twice under two schemas, and arrow
resolves the dataset to whichever file it reads first. Measured on a lake with
ten countries rebuilt, `open_dataset` on the facts table returned the
pre-rename names plus both partition columns:

    FILING LEI PERIODEND FACT CONCEPT ... COUNTRY country

which is a table that has just been rebuilt and still reads as though it had
not. `--clean` removes the `COUNTRY=` directories and leaves everything else,
which is why it is not the same as emptying the table.

`addressing.rename_folders_to_snake_case` does not help here: it renames
hyphenated table folders and skips any name containing `=`, so partition
directories are outside it. Renaming them by hand would not help either, since
the columns inside the files of a country not yet rebuilt are still upper case.

## Resuming

`--countries` rebuilds the countries named and cleans nothing, for continuing a
run that was interrupted. Which countries are already done can be read off the
partition directories: a `country=XX` is rebuilt, a `COUNTRY=XX` is not.
