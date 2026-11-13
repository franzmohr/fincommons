"""Financial reports: what a figure of one regime is taken to mean in the other.

This is the only module of the package that knows IFRS from US GAAP, and it is
the only one that may. :mod:`fincommons.esef` and
:mod:`fincommons.secfiling` each keep what their publisher gave out, concept
by concept, and the combined table puts the two populations side by side
without reconciling them. The question this module answers -- whether
``us-gaap:Revenues`` and ``ifrs-full:Revenue`` are the same figure -- is a
judgement about accounting rather than about storage, and it is made here so
that it is made once, in one place, where it can be read and disagreed with.

It is made here and applied anywhere. The resolution joins on the concept as
both source tables spell it, so an ESEF-only lake can use the catalogue
against :mod:`fincommons.esef` directly, without building a combined table
and without that module depending on this one.

The judgement is therefore data and not code. It is a catalogue shipped with
the package, in the data directory beside the column catalogue and the FIRDS
schemas, and every row carries who decided, on what basis, and what the
caveat is. A row whose caveat is empty is a row whose author thought the two
frameworks agree; a row with one is a row where somebody should look before
using the figure. Expressing the mapping as branches in Python would make the
same statements unreadable to the people most able to find them wrong, who are
accountants rather than programmers.

Nothing in this catalogue is authoritative, and the module says so rather than
implying otherwise. There is no official correspondence between IFRS and
US GAAP: the convergence programme of the two boards did not produce one, and
the data quality rules published for filings with the Commission do not cross
frameworks. What is shipped here is a starting point that a deployment should
review and extend, and the ``source`` column exists so that a row somebody has
reviewed can be told from a row nobody has.

The module deliberately stops at resolution. It says which concepts a measure
is taken to be, and it labels facts accordingly; it does not define ratios,
aggregate across entities or build a metrics layer. Those belong to whoever is
asking the question, and the package they ship, rather than to the
infrastructure that holds the data.
"""

import logging
from dataclasses import dataclass
from importlib import resources

import pandas as pd

from ..log import context

logger = logging.getLogger(__name__)

#: The file the catalogue is shipped as, in the data directory of a package.
CATALOGUE_FILE = "financialreport_measures.csv"

#: The columns of the catalogue.
#:
#: ``measure``
#:     the name being defined, such as ``revenue``,
#: ``framework``
#:     the reporting framework the row applies to, as the Silver report table
#:     records it,
#: ``taxonomy`` and ``element``
#:     the concept the measure resolves to, as the prefix the filings bind to
#:     the taxonomy and the name of the element -- ``ifrs-full`` and
#:     ``Revenue``. That is how both tables spell a concept, the ESEF reports
#:     because xBRL-JSON names a concept by its prefix and the company files
#:     because the Commission keys its facts by the taxonomy. The namespace is
#:     deliberately not used: a published taxonomy takes a new one every year,
#:     and neither table carries it,
#: ``priority``
#:     which concept wins where a filing states several, lowest first,
#: ``basis``, ``caveat`` and ``source``
#:     why this concept, where the two frameworks do not actually agree, and
#:     who decided.
CATALOGUE_COLUMNS = ("measure", "framework", "taxonomy", "element", "priority",
                     "basis", "caveat", "source")


@dataclass(frozen=True)
class MeasureDefinition:
    """How one measure resolves under one framework, for one concept."""

    measure: str
    framework: str
    taxonomy: str
    element: str
    priority: int
    basis: str
    caveat: str
    source: str

    def __str__(self) -> str:
        lines = [
            f"{self.measure} under {self.framework} -- {self.taxonomy}:{self.element}"
            f" (priority {self.priority})",
            f"  basis    {self.basis}",
        ]
        if self.caveat:
            lines.append(f"  CAVEAT   {self.caveat}")
        lines.append(f"  source   {self.source}")
        return "\n".join(lines)


class MeasureDefinitions(list):
    """The definitions of one measure, one per framework and concept."""

    def __str__(self) -> str:
        return "\n\n".join(str(definition) for definition in self)


def _root(package):
    """The importable name of ``package``; this one as it was imported."""
    return (__package__.rsplit(".", 1)[0] if package == "fincommons" else package)


def measure_catalogue(measure=None, framework=None, packages=("fincommons",)):
    """The catalogue as a data frame, or the part of it that is asked for.

    Parameters
    ----------
    measure: str, optional
        Only the rows defining this measure.
    framework: str, optional
        Only the rows applying to this framework, e.g. ``'IFRS'``.
    packages: iterable of str
        The packages whose catalogues are read. A deployment that keeps its
        own judgements in a package of its own ships a catalogue of the same
        shape and names it here, and its rows are read beside these. That is
        how a mapping somebody has reviewed is kept without editing the one
        this package ships.

    Returns
    -------
    pandas.DataFrame
        The rows, in the order measure, framework, priority.
    """
    parts = []
    for package in packages:
        path = resources.files(_root(package)).joinpath("data", CATALOGUE_FILE)
        if not path.is_file():
            raise FileNotFoundError(
                f"{package} ships no {CATALOGUE_FILE}. A package that keeps "
                "its own measures puts one in its data directory, with the "
                f"columns {', '.join(CATALOGUE_COLUMNS)}."
            )
        with path.open(encoding="utf-8") as handle:
            parts.append(pd.read_csv(handle, dtype=str, keep_default_na=False))

    catalogue = pd.concat(parts, ignore_index=True)
    missing = [name for name in CATALOGUE_COLUMNS if name not in catalogue.columns]
    if missing:
        raise ValueError(
            f"The measure catalogue is missing the columns {', '.join(missing)}"
        )
    catalogue["priority"] = catalogue["priority"].astype(int)

    if measure is not None:
        catalogue = catalogue[catalogue["measure"] == str(measure).strip().lower()]
    if framework is not None:
        catalogue = catalogue[catalogue["framework"] == str(framework).strip().upper()]

    return (catalogue.sort_values(["measure", "framework", "priority"], kind="stable")
            .reset_index(drop=True))


def measures(packages=("fincommons",)):
    """The names of the measures the catalogues define."""
    return sorted(measure_catalogue(packages=packages)["measure"].unique())


def describe_measure(name, packages=("fincommons",)):
    """Describe one measure: what it resolves to, and what the caveats are.

    Parameters
    ----------
    name: str
        The measure, e.g. ``"revenue"``. Case does not matter.
    packages: iterable of str
        The catalogues to look in; see :func:`measure_catalogue`.

    Returns
    -------
    MeasureDefinitions
        A list of :class:`MeasureDefinition`, one per framework and concept;
        printable.

    Raises
    ------
    KeyError
        Where no catalogue defines a measure of that name.

    Examples
    --------
    >>> print(describe_measure("revenue"))
    >>> describe_measure("equity")[1].caveat
    """
    wanted = str(name).strip().lower()
    rows = measure_catalogue(measure=wanted, packages=packages)
    if rows.empty:
        known = measures(packages)
        raise KeyError(
            f"{wanted!r} is no measure of the catalogue. Defined: "
            + ", ".join(known)
        )
    return MeasureDefinitions(
        MeasureDefinition(row.measure, row.framework, row.taxonomy, row.element,
                          int(row.priority), row.basis, row.caveat, row.source)
        for row in rows.itertuples()
    )


def concepts_for(measure, framework, packages=("fincommons",)):
    """The concepts a measure resolves to under a framework, best first.

    Returns a list of ``(taxonomy, element)``. Empty where the catalogue
    defines the measure but says nothing about that framework, which is a
    statement in itself: the measure has no counterpart there, or nobody has
    decided what it is.
    """
    rows = measure_catalogue(measure=measure, framework=framework,
                             packages=packages)
    return [(row.taxonomy, row.element) for row in rows.itertuples()]


def caveats(measure=None, packages=("fincommons",)):
    """The rows of the catalogue that carry a caveat.

    A deployment publishing a comparison across the two regimes should be able
    to print what it is relying on, and this is that list.
    """
    catalogue = measure_catalogue(measure=measure, packages=packages)
    return catalogue[catalogue["caveat"] != ""].reset_index(drop=True)


def with_measures(facts, framework=None, packages=("fincommons",)):
    """Label facts with the measures they are taken to be.

    Adds ``MEASURE`` and ``MEASURE_PRIORITY``. A fact that no row of the
    catalogue names keeps a null in both, which is the great majority of them:
    a filing states thousands of facts and the catalogue defines a handful of
    measures.

    The join is on ``CONCEPT`` as the tables carry it -- the prefix and the
    element name -- which is what both the ESEF table and the SEC table hold.
    It therefore works on the combined table and, just as well, on either of
    the two source tables on its own: an ESEF-only lake can resolve a measure
    over :mod:`fincommons.esef` without building anything here and without
    that module depending on this one.

    Parameters
    ----------
    facts: pyspark.sql.DataFrame
        Any table carrying a ``CONCEPT`` column spelled that way: the
        combined table, the ESEF table, or the SEC table.
    framework: str, optional
        Resolve as though every fact came from a filing under this framework.
        Left out, every framework the catalogue defines is matched, which is
        what the prefix of a concept already decides.
    packages: iterable of str
        The catalogues to use; see :func:`measure_catalogue`.
    """
    import pyspark.sql.functions as func
    import pyspark.sql.types as types

    spark = facts.sparkSession
    catalogue = measure_catalogue(framework=framework, packages=packages)
    rows = [(row.measure, f"{row.taxonomy}:{row.element}", int(row.priority))
            for row in catalogue.itertuples()]
    if not rows:
        known = sorted(measure_catalogue(packages=packages)["framework"].unique())
        raise ValueError(
            f"The catalogue defines no measure for {framework!r}; measures "
            "are defined for " + ", ".join(known)
        )

    lookup = spark.createDataFrame(rows, schema=types.StructType([
        types.StructField("MEASURE", types.StringType(), nullable=False),
        types.StructField("CONCEPT", types.StringType(), nullable=False),
        types.StructField("MEASURE_PRIORITY", types.IntegerType(), nullable=False),
    ]))

    return facts.join(func.broadcast(lookup), ["CONCEPT"], "left")


def measure_values(facts, measure, packages=("fincommons",)):
    """One value per filing and period for one measure, by the best concept.

    Where a filing states several of the concepts a measure resolves to, the
    one with the lowest priority wins and the others are dropped. Which that
    is, and why, is in the catalogue; :func:`describe_measure` prints it.

    Facts reported under a dimension are excluded. A measure is the figure for
    the entity as a whole, and a figure broken down by segment, by class of
    instrument or by any other axis is a part of it; summing the parts beside
    the whole is the arithmetic this exclusion exists to prevent. It also
    makes the two sources comparable on this one point, since the company
    files of the Commission carry no dimensional facts at all.

    Parameters
    ----------
    facts: pyspark.sql.DataFrame
        The combined table, or either source table; see :func:`with_measures`.
    measure: str
        The measure to resolve, e.g. ``"revenue"``.
    packages: iterable of str
        The catalogues to use; see :func:`measure_catalogue`.
    """
    import pyspark.sql.functions as func
    from pyspark.sql.window import Window

    wanted = str(measure).strip().lower()
    definitions = describe_measure(wanted, packages)
    caveated = [d for d in definitions if d.caveat]
    if caveated:
        logger.warning(
            "The measure is resolved through concepts that carry caveats; "
            "see describe_measure(%r)", wanted,
            extra=context(module="financialreport", table="fact",
                          rows=len(caveated)),
        )

    labelled = with_measures(facts, packages=packages) \
        .where(func.col("MEASURE") == wanted)
    if "DIMENSIONS" in facts.columns:
        labelled = labelled.where(func.col("DIMENSIONS").isNull())

    best = Window.partitionBy("FILING", "PRDSTART", "PRDEND") \
        .orderBy("MEASURE_PRIORITY")
    return (
        labelled
        .withColumn("_rank", func.row_number().over(best))
        .where(func.col("_rank") == 1)
        .drop("_rank")
    )


#: What this module offers. The judgement about what a figure of one regime is
#: taken to mean in the other, and nothing above it.
__all__ = [
    "CATALOGUE_COLUMNS",
    "CATALOGUE_FILE",
    "MeasureDefinition",
    "MeasureDefinitions",
    "caveats",
    "concepts_for",
    "describe_measure",
    "measure_catalogue",
    "measure_values",
    "measures",
    "with_measures",
]
