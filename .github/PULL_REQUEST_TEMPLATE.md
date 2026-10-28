<!--
Say what the pull request changes and why. Link the issue it settles, if
there is one ("Closes #123").
-->

## What and why



## Checklist

The rules behind these points are in the handbook, section *Code access and
how to contribute*, and in [CONTRIBUTING.md](https://github.com/franzmohr/fincommons/blob/main/CONTRIBUTING.md).

- [ ] Nothing in the change is confidential, proprietary or specific to a
      counterparty; the tests run on synthetic data or on fixtures written into
      the test.
- [ ] A test covers the change, and it fails without the change.
- [ ] Every public function that was added or changed has a docstring that
      states what a caller may rely on.
- [ ] The README of the module says what changed for someone using it.
- [ ] Where the change alters what the handbook describes, or adds a module,
      the handbook is changed with it.
- [ ] No new dependency on code that is not open source. A new open source
      dependency is declared in `pyproject.toml` and added to
      `tests/test_licensing.py`.
- [ ] No name listed in `tests/test_public_api.py` is removed or renamed, or,
      if one is, the pull request says so: it calls for a new major version.
