"""Validaciones de uso del CLI (no necesitan Redis ni Postgres)."""

from __future__ import annotations

import pytest

from rpa_worker.cli import EXIT_CONFIG, main, parse_ids, parse_when


@pytest.mark.parametrize(
    "argv",
    [
        ["drain-dlq", "--execute", "--operator", "ana", "--reason", "r"],  # falta --limit/--all
        ["drain-dlq", "--execute", "--limit", "5"],  # faltan --operator/--reason
        ["stop"],  # falta --ttl
    ],
)
def test_unsafe_invocations_are_rejected(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(argv) == EXIT_CONFIG
    assert "ERROR" in capsys.readouterr().err


def test_limit_and_all_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        main(["drain-dlq", "--execute", "--limit", "5", "--all"])


def test_parse_ids() -> None:
    assert parse_ids("1-3,101,abc") == ["1", "2", "3", "101", "abc"]


def test_parse_when_assumes_utc() -> None:
    assert parse_when("2023-11-14T22:13:20") == 1_700_000_000.0
    assert parse_when("2023-11-14T17:13:20-05:00") == 1_700_000_000.0
