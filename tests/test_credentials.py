"""Where a key comes from, and that it never reaches a log.

Two sources of the package refuse a request without a registered key. The
tests here are mostly about the ways a key must *not* be handled: not read
from the configuration file, not returned with the newline an editor left on
the end of it, and not left in an address that a pipeline is about to write
to its log.
"""

from __future__ import annotations

import pytest

from fincommons.credentials import (KEY_ENVIRONMENT, VAULT_ENVIRONMENT,
                                    api_key, redacted, vault_path)


# Where a key is found
# --------------------------------------------------------------------------


def test_the_argument_wins_over_everything(monkeypatch, tmp_path):
    monkeypatch.setenv("FINCOMMONS_EDINET_KEY", "from-env")
    monkeypatch.setenv(VAULT_ENVIRONMENT, str(tmp_path))
    (tmp_path / "edinet").write_text("from-vault", encoding="utf-8")

    assert api_key("edinet", key="given") == "given"


def test_the_environment_is_read_where_no_argument_is_given(monkeypatch):
    monkeypatch.setenv("FINCOMMONS_EDINET_KEY", "from-env")

    assert api_key("edinet") == "from-env"


def test_the_environment_variable_follows_the_name_of_the_source(monkeypatch):
    monkeypatch.setenv("FINCOMMONS_DART_KEY", "korea")

    assert api_key("dart") == "korea"
    assert KEY_ENVIRONMENT.format(name="DART") == "FINCOMMONS_DART_KEY"


def test_a_named_variable_is_read_instead_of_the_default(monkeypatch):
    monkeypatch.setenv("SOMETHING_ELSE", "elsewhere")

    assert api_key("edinet", env="SOMETHING_ELSE") == "elsewhere"


def test_the_vault_is_read_where_the_environment_holds_nothing(monkeypatch, tmp_path):
    monkeypatch.delenv("FINCOMMONS_EDINET_KEY", raising=False)
    monkeypatch.setenv(VAULT_ENVIRONMENT, str(tmp_path))
    (tmp_path / "edinet").write_text("from-vault", encoding="utf-8")

    assert api_key("edinet") == "from-vault"


def test_a_key_file_is_stripped_of_the_newline_an_editor_leaves(monkeypatch, tmp_path):
    """A newline in an HTTP header is worse than a wrong key."""
    monkeypatch.delenv("FINCOMMONS_EDINET_KEY", raising=False)
    monkeypatch.setenv(VAULT_ENVIRONMENT, str(tmp_path))
    (tmp_path / "edinet").write_text("  from-vault\n", encoding="utf-8")

    assert api_key("edinet") == "from-vault"


def test_an_empty_value_counts_as_no_value(monkeypatch, tmp_path):
    monkeypatch.setenv("FINCOMMONS_EDINET_KEY", "   ")
    monkeypatch.setenv(VAULT_ENVIRONMENT, str(tmp_path))
    (tmp_path / "edinet").write_text("from-vault", encoding="utf-8")

    assert api_key("edinet") == "from-vault"
    assert api_key("edinet", key="  ") == "from-vault"


# Where it is not found
# --------------------------------------------------------------------------


def test_no_key_anywhere_is_refused_with_all_three_places_named(monkeypatch):
    monkeypatch.delenv("FINCOMMONS_EDINET_KEY", raising=False)
    monkeypatch.delenv(VAULT_ENVIRONMENT, raising=False)

    with pytest.raises(ValueError) as refusal:
        api_key("edinet")

    said = str(refusal.value)
    assert "key=" in said
    assert "FINCOMMONS_EDINET_KEY" in said
    assert VAULT_ENVIRONMENT in said
    # And the one place it must never be put.
    assert "paths.yaml" in said


def test_a_vault_without_the_file_is_no_key(monkeypatch, tmp_path):
    monkeypatch.delenv("FINCOMMONS_EDINET_KEY", raising=False)
    monkeypatch.setenv(VAULT_ENVIRONMENT, str(tmp_path))

    with pytest.raises(ValueError, match="No API key for edinet"):
        api_key("edinet")


def test_no_vault_is_set_and_none_is_invented(monkeypatch):
    monkeypatch.delenv(VAULT_ENVIRONMENT, raising=False)

    assert vault_path("edinet") is None


def test_a_source_name_cannot_walk_out_of_the_vault():
    """The name becomes a file name, so it is held to one."""
    for bad in ("../../etc/passwd", "edinet/../../x", "EDINET", "a b", ""):
        with pytest.raises(ValueError, match="is not a source name"):
            vault_path(bad)


# What reaches a log
# --------------------------------------------------------------------------


def test_the_key_of_a_query_parameter_is_replaced():
    """EDINET takes its key in the address, which is what a pipeline logs."""
    url = ("https://api.edinet-fsa.go.jp/api/v2/documents.json"
           "?date=2026-01-01&type=2&Subscription-Key=abcdef123456")

    said = redacted(url)

    assert "abcdef123456" not in said
    assert "Subscription-Key=***" in said
    # Everything else about the request survives, or the log says nothing.
    assert "date=2026-01-01" in said
    assert "type=2" in said


def test_the_korean_parameter_is_replaced_too():
    url = ("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
           "?crtfc_key=deadbeef&corp_code=00126380")

    said = redacted(url)

    assert "deadbeef" not in said
    assert "crtfc_key=***" in said
    assert "corp_code=00126380" in said


def test_a_key_at_the_end_of_an_address_is_replaced_whole():
    assert redacted("https://x.test/a?key=zzz") == "https://x.test/a?key=***"


def test_an_address_with_no_key_in_it_is_unchanged():
    url = "https://filings.xbrl.org/index.json?year=2026"

    assert redacted(url) == url
