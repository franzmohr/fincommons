# Contributing to fincommons

fincommons is maintained together by the institutions that use it, and a
contribution is welcome whether it is a fix, a new data set, a test, or a
sentence of documentation that was missing when you needed it. This file says
how to get from a clone to a pull request that can be merged without anyone
having to ask you for more.

The rules a contribution has to follow are set out in the handbook, in the
section *Code access and how to contribute* of
[`docs/handbook/fincommons.tex`](docs/handbook/fincommons.tex). They are few
and follow from the purpose of the package; the checklist of every pull request
repeats them, so that neither the author nor the reviewer has to remember
them.

## Setting up

You need Python 3.10 or later and, for the tests that run Spark, a Java
runtime. Java 17 is the version to install, the one continuous integration
runs on; Spark 4 also runs on Java 21. A Java that Spark does not support need
not fail with a clear message; the run may just stall.

Install the package in editable mode with the `dev` extra, which brings
PySpark, Delta and pytest, into a virtual environment of its own:

```
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

[uv](https://docs.astral.sh/uv/) does the same faster, and also works where
the system Python ships without `venv`:

```
uv venv
uv pip install -e ".[dev]"
```

Spark cannot start natively on Windows. Work in WSL there.

## Running the tests

The suite has two tiers. Every test that needs the local Spark session is
marked `spark` as it is collected, by [`tests/conftest.py`](tests/conftest.py),
so a new test lands in the right tier without being marked by hand.

```
pytest -m "not spark"
```

runs the tests that need no Spark session. It takes seconds and is what to run
while working.

```
pytest
```

runs everything. It takes the better part of an hour on a laptop, so it is
usually enough to run the module you changed, for instance
`pytest tests/test_exchangerate.py`, and leave the whole suite to the
continuous integration, which runs it on every pull request.

Nothing in the suite reaches the network, and a new test must not either: where
a module downloads something, the test supplies the answer. Tests run on
synthetic data or on fixtures written into the test, never on data that is not
public.

## Working on the documentation

The documentation is in three places, and a change to the code that alters
what one of them states comes with a change to it:

* the **docstring** of every public function, which states what a caller may
  rely on;
* the **README** of every module, under `src/fincommons/<module>/README.md`,
  which explains how the module is used and why it works the way it does;
* the **handbook** in [`docs/handbook`](docs/handbook), which explains the
  architecture and documents every data set.

The READMEs are Markdown and can be edited in the browser on GitHub, which is
the easiest way to propose a correction: open the file, choose the pencil, and
GitHub turns the edit into a pull request.

The handbook is LaTeX. Build it with

```
cd docs/handbook
latexmk -pdf fincommons.tex
```

which needs a TeX distribution with `biber`. The continuous integration builds
it on every pull request that changes it and attaches the PDF to the run, so a
change can be reviewed as it will read without building it locally. A figure
is committed with the source it is drawn from -- TikZ in the document, or the
`.odg` file beside the PDF -- so that the next person can change it.

The documentation explains rather than lists: it says why something is done
the way it is, what a caller may rely on, and what was tried and rejected where
that is not obvious. Follow the tone of the chapter or README you are editing.

## Proposing a change

1. Open an issue first for anything larger than a fix, so that the approach
   can be agreed before the work is done. The issue templates ask for what a
   maintainer would otherwise have to ask for.
2. Work on a branch, and keep a pull request to one subject.
3. Fill in the checklist of the pull request template.
4. The continuous integration runs the whole suite, the tests against the
   oldest versions of the dependencies the package declares, the licence
   check, the build of the wheel and, where the handbook changed, the build of
   the handbook. A pull request is merged when they pass and a maintainer has
   reviewed it.

A change that removes or renames a name listed in
[`tests/test_public_api.py`](tests/test_public_api.py) breaks the packages that
build on fincommons and calls for a new major version; say so in the pull
request.

## Licensing

fincommons is licensed under the
[European Union Public Licence 1.2](LICENSES/EUPL-1.2.txt), and a contribution
is made under the same licence. Every file is covered by
[`REUSE.toml`](REUSE.toml); `reuse lint` checks it, and the continuous
integration runs it. fincommons depends only on open source packages, and
[`tests/test_licensing.py`](tests/test_licensing.py) fails on an import of
anything else; a new dependency is declared in `pyproject.toml` and added to
the list in that test.

## Reporting a vulnerability

Never in a public issue. [`SECURITY.md`](SECURITY.md) says how.
