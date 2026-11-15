"""Financial reports: what a figure of one regime is taken to mean in the other.

This is the only module of the package that knows IFRS from US GAAP, and it is
the only one that may. :mod:`fincommons.financialreport.esef` and
:mod:`fincommons.financialreport.sec` each keep what their publisher gave out, concept
by concept, and the combined table puts the two populations side by side
without reconciling them. The question this module answers -- whether
``us-gaap:Revenues`` and ``ifrs-full:Revenue`` are the same figure -- is a
judgement about accounting rather than about storage, and it is made here so
that it is made once, in one place, where it can be read and disagreed with.

It is made here and applied anywhere. The resolution joins on the concept as
both source tables spell it, so an ESEF-only lake can use the catalogue
against :mod:`fincommons.financialreport.esef` directly, without building a combined table
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

from ...log import context

logger = logging.getLogger(__name__)

#: The file the catalogue is shipped as, in the data directory of a package.
CATALOGUE_FILE = "financialreport_measures.csv"

#: The priority of the concept a framework states a measure as, as against
#: the approximations the catalogue ranks below it. Only these are written
#: into a table; see :func:`canonical_concepts`.
FIRST_CONCEPT = 1

#: The measures written into the combined table of facts as a column, so that
#: a fact carries the identifier of what it is rather than only the concept
#: its filer tagged it with.
#:
#: It is deliberately short and grows one measure at a time. A measure may be
#: added here only once its concepts are agreed to mean the same figure in
#: every framework the catalogue defines it for, because a materialised
#: identifier is read by people who will not go back to the caveat: a query
#: filtering on ``MEASURE = 'assets'`` is making the claim that the rows it
#: gets back are comparable, and this tuple is where that claim is made.
#:
#: Total assets is first because it is the cleanest of the eight. One element
#: per taxonomy, no fallbacks at any priority, and every framework states the
#: same line of the same statement: ifrs-full:Assets, us-gaap:Assets,
#: jppfs_cor:Assets and jpigp_cor:AssetsIFRS. What differs between them is
#: recognition rather than definition -- development costs are capitalised
#: under IAS 38 and generally expensed under ASC 730, and LIFO is permitted
#: under US GAAP and prohibited by IAS 2 -- and that is on the catalogue row
#: where a reader can find it.
#:
#: Revenue is second, and it is the measure that made the rule explicit. The
#: catalogue gives it seven rows and six concepts, three of them ranked below
#: the first as approximations, and each of those says in its own caveat that
#: it is not the same figure. Only the first concept of each framework is
#: written down -- ifrs-full:Revenue, jpigp_cor:RevenueIFRS,
#: jppfs_cor:NetSales and the ASC 606 element -- so the identifier means one
#: thing wherever it appears. The cost is coverage rather than correctness: a
#: registrant tagging only the pre-ASC-606 ``us-gaap:Revenues`` carries no
#: identifier, which is the honest answer, that element being tagged
#: inconsistently and not an equivalent of the IFRS total.
MATERIALISED_MEASURES = ("assets", "revenue")

#: Dimension members that name a basis of preparation rather than a part of
#: an entity, and which a measure is therefore resolved over.
#:
#: There is one, and it belongs to the Japanese regime. A report that
#: prepares consolidated statements states every figure twice, the group's
#: with no dimension and the parent company's own under this member, and a
#: filer that prepares no consolidated statements states only the second.
#: Treating it as a breakdown -- which is what every other member of every
#: regime is -- would return nothing for every filer that does not
#: consolidate.
BASIS_MEMBERS = ("NonConsolidatedMember",)

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
    """The importable name of ``package``; this one as it was imported.

    The first segment of ``__package__`` and not the whole of it, nor its
    parent: the data directory belongs to the distribution, while the module
    asking for it may sit at any depth below the top -- ``mapping`` is three
    levels down -- and counting levels is how a file that moves stops finding
    its own data.
    """
    return __package__.split(".")[0] if package == "fincommons" else package


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


def canonical_concepts(measures=MATERIALISED_MEASURES,
                       packages=("fincommons",)):
    """The concept-to-measure pairs that may be written into a table.

    Only the first concept of a measure under each framework -- the rows of
    priority 1 -- and that is the whole of the rule. The priorities exist to
    rank approximations for a caller resolving a measure at query time, who
    wants the best answer the filing affords; a materialised identifier is
    held to a stricter bar, that the figure is the same one. Every row of
    priority 2 or 3 in the catalogue says in its own caveat that it is not:
    ``us-gaap:Revenues`` "is the last resort and not an equivalent of the
    IFRS total", ``RevenueFromContractWithCustomerIncludingAssessedTax`` is
    "larger than the IFRS measure", ``jppfs_cor:CashAndDeposits`` is "NOT the
    same figure". A fact tagged with one of those keeps a null rather than an
    identifier that overstates what is known about it.

    Dropping them is also what keeps the column answerable. A filing may tag
    several concepts of one measure -- that is why the priorities are there
    -- and labelling all of them would put two different figures of one
    filing and one period under one name, which a query filtering on the
    name could not tell apart. At priority 1 the catalogue has one element
    per taxonomy per measure, so that cannot happen.

    One row per concept besides: a concept that two measures claim cannot be
    written down as either, and is refused here rather than resolved by a
    rule. No such claim exists at priority 1 today. The two that exist in the
    catalogue are each between a first concept of one measure and a second of
    another -- ``us-gaap:StockholdersEquity`` is the equity attributable to
    the parent and the stand-in for equity where a registrant states no wider
    total, ``us-gaap:NetIncomeLoss`` likewise -- so the priority rule settles
    them rather than hiding them. The check stays for the row that collides
    tomorrow.

    Returns
    -------
    list of (str, str)
        ``(concept, measure)`` pairs, the concept spelled as the tables
        spell it.

    Raises
    ------
    ValueError
        Where a measure is not in the catalogue, where one measure has two
        first concepts under one taxonomy, or where one concept is claimed
        by two of the measures asked for.
    """
    catalogue = measure_catalogue(packages=packages)
    wanted = [str(m).strip().lower() for m in measures]
    unknown = [m for m in wanted if m not in set(catalogue["measure"])]
    if unknown:
        raise ValueError(
            f"{unknown[0]!r} is no measure of the catalogue; it defines "
            + ", ".join(sorted(set(catalogue["measure"])))
        )

    rows = catalogue[catalogue["measure"].isin(wanted)
                     & (catalogue["priority"] == FIRST_CONCEPT)]
    claimed = {}
    per_taxonomy = {}
    for row in rows.itertuples():
        concept = f"{row.taxonomy}:{row.element}"
        here = (row.measure, row.taxonomy)
        if here in per_taxonomy and per_taxonomy[here] != concept:
            raise ValueError(
                f"{row.measure!r} has two first concepts under "
                f"{row.taxonomy}, {per_taxonomy[here]} and {concept}, so a "
                "filing tagging both would state the measure twice under one "
                "name. One of them belongs at a lower priority."
            )
        per_taxonomy[here] = concept
        if concept in claimed and claimed[concept] != row.measure:
            raise ValueError(
                f"{concept} is claimed by both {claimed[concept]!r} and "
                f"{row.measure!r}, so it cannot be written as either. A "
                "concept that two measures claim is resolved at query time, "
                "where the caller has asked for one of them; see "
                "canonical_concepts."
            )
        claimed[concept] = row.measure
    return sorted(claimed.items())


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

    A table that already carries ``MEASURE`` has it replaced rather than
    doubled. The combined table of facts stores one, for the measures
    :data:`MATERIALISED_MEASURES` covers; resolving here answers the wider
    question -- every measure the catalogue defines, under a framework the
    caller may name -- so what it works out supersedes what was stored, for
    that query and no further. The stored column is untouched on disk.

    The join is on ``CONCEPT`` as the tables carry it -- the prefix and the
    element name -- which is what both the ESEF table and the SEC table hold.
    It therefore works on the combined table and, just as well, on either of
    the two source tables on its own: an ESEF-only lake can resolve a measure
    over :mod:`fincommons.financialreport.esef` without building anything here and without
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

    stored = [name for name in ("MEASURE", "MEASURE_PRIORITY")
              if name in facts.columns]
    if stored:
        facts = facts.drop(*stored)
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
    makes the sources comparable on this one point, since the company files
    of the Commission carry no dimensional facts at all.

    :data:`BASIS_MEMBERS` is the exception, and a Japanese filing is why. A
    report that prepares consolidated statements states the group's figures
    with no dimension and the parent company's own under
    ``NonConsolidatedMember``, and a filer that prepares none states only the
    second. That member does not name a part of an entity -- it names which
    of two sets of accounts the figure belongs to -- so excluding it would
    return nothing at all for every filer that does not consolidate, which is
    a large share of them. A fact carrying it is therefore kept, and where a
    filing states both, the consolidated figure wins: that is the basis the
    filing itself reports on, and it is the one comparable with a group
    reporting under either of the other frameworks.

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
    consolidated = func.lit(0)
    if "DIMENSIONS" in facts.columns:
        labelled = labelled.where(
            func.col("DIMENSIONS").isNull()
            | func.col("DIMENSIONS").isin(list(BASIS_MEMBERS)))
        # The group's figure before the parent company's, where a filing
        # states both; see BASIS_MEMBERS.
        consolidated = func.when(func.col("DIMENSIONS").isNull(),
                                 func.lit(0)).otherwise(func.lit(1))

    best = Window.partitionBy("FILING", "PRDSTART", "PRDEND") \
        .orderBy(consolidated, "MEASURE_PRIORITY")
    return (
        labelled
        .withColumn("_rank", func.row_number().over(best))
        .where(func.col("_rank") == 1)
        .drop("_rank")
    )


#: What this module offers. The judgement about what a figure of one regime is
#: taken to mean in the other, and nothing above it.
__all__ = [
    "BASIS_MEMBERS",
    "CATALOGUE_COLUMNS",
    "CATALOGUE_FILE",
    "FIRST_CONCEPT",
    "MATERIALISED_MEASURES",
    "MeasureDefinition",
    "MeasureDefinitions",
    "canonical_concepts",
    "caveats",
    "concepts_for",
    "describe_measure",
    "measure_catalogue",
    "measure_values",
    "measures",
    "with_measures",
]
