"""Repositorio Postgres contra un Postgres real (esquema aplicado con Alembic)."""

from __future__ import annotations

import psycopg
import pytest

from rpa_worker.adapters.postgres_repository import PostgresRepository, create_pool
from rpa_worker.application.ports import DlqEntry, NotReopenableError, TransactionRecord
from rpa_worker.domain.exceptions import AlertKind, SystemException
from rpa_worker.domain.transaction import TransactionStatus as S
from tests.fakes import make_tx


def rec(status: S, *, retry: int = 0, result: dict[str, object] | None = None) -> TransactionRecord:
    return TransactionRecord(
        reference="REF-1",
        status=status,
        priority="normal",
        retry_count=retry,
        correlation_id="corr-1",
        result=result,
        error_reason=None if result else "server_error",
        finished_at=1_700_000_000.0 if status.is_terminal else None,
    )


def row(conninfo: str) -> dict[str, object]:
    with psycopg.connect(conninfo) as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        found = cur.execute("SELECT * FROM transaction_results WHERE reference='REF-1'").fetchone()
    assert found is not None
    return found


def test_retried_then_successful_writes_result(
    repo: PostgresRepository, postgres_conninfo: str
) -> None:
    assert repo.record(rec(S.RETRIED, retry=1)) is True
    assert repo.find_terminal_status("REF-1") is None
    assert repo.record(rec(S.SUCCESSFUL, retry=1, result={"id": 1, "title": "t"})) is True

    r = row(postgres_conninfo)
    assert r["status"] == "Successful"
    assert r["result"] == {"id": 1, "title": "t"}
    assert r["retry_count"] == 1
    assert r["finished_at"] is not None
    assert repo.find_terminal_status("REF-1") is S.SUCCESSFUL


def test_terminal_row_is_never_overwritten(
    repo: PostgresRepository, postgres_conninfo: str
) -> None:
    assert repo.record(rec(S.SUCCESSFUL, result={"id": 1})) is True
    updated_at = row(postgres_conninfo)["updated_at"]
    assert repo.record(rec(S.SUCCESSFUL, result={"id": 999})) is False  # doble entrega
    assert repo.record(rec(S.RETRIED, retry=2)) is False
    r = row(postgres_conninfo)
    assert r["result"] == {"id": 1}
    assert r["updated_at"] == updated_at


def test_check_constraint_rejection_is_non_retryable_contract(repo: PostgresRepository) -> None:
    with pytest.raises(SystemException) as info:
        repo.record(rec(S.SUCCESSFUL, result=None))  # Successful sin resultado viola el CHECK
    assert (info.value.reason, info.value.retryable) == ("database_rejected", False)
    assert info.value.alert is AlertKind.CONTRACT


def test_unreachable_database_is_retryable() -> None:
    pool = create_pool(
        "host=127.0.0.1 port=1 dbname=x user=x password=x",
        min_size=1,
        max_size=1,
        connect_timeout=1,
        statement_timeout_ms=1_000,
    )
    pool.open(wait=False)
    try:
        with pytest.raises(SystemException) as info:
            PostgresRepository(pool).find_terminal_status("REF-1")
        assert (info.value.reason, info.value.retryable) == ("database_unavailable", True)
        assert PostgresRepository(pool).ping() is False
    finally:
        pool.close()


def _entry(entry_id: str = "1-0") -> DlqEntry:
    return DlqEntry(entry_id, make_tx("REF-1", 1), "server_error", "HTTP 503")


def test_reopen_abandoned_moves_to_retried_and_audits(
    repo: PostgresRepository, postgres_conninfo: str
) -> None:
    repo.record(rec(S.ABANDONED, retry=2))
    assert repo.reopen(entry=_entry(), requeue_id=_uuid(1), operator="ana", reason="r") == 1
    r = row(postgres_conninfo)
    assert (r["status"], r["retry_count"], r["requeue_count"], r["finished_at"]) == (
        "Retried",
        0,
        1,
        None,
    )
    # Re-ejecución con el mismo requeue_id: no cuenta dos veces ni duplica auditoría.
    assert repo.reopen(entry=_entry(), requeue_id=_uuid(1), operator="ana", reason="r") == 1
    with psycopg.connect(postgres_conninfo) as conn:
        audits = conn.execute("SELECT operator, reason, dlq_reason FROM requeue_audit").fetchall()
    assert audits == [("ana", "r", "server_error")]


def test_reopen_without_row_creates_retried(
    repo: PostgresRepository, postgres_conninfo: str
) -> None:
    assert repo.reopen(entry=_entry(), requeue_id=_uuid(2), operator="ana", reason="r") == 1
    assert row(postgres_conninfo)["status"] == "Retried"


@pytest.mark.parametrize("status", [S.SUCCESSFUL, S.FAILED])
def test_reopen_refuses_successful_and_failed(
    repo: PostgresRepository, postgres_conninfo: str, status: S
) -> None:
    repo.record(rec(status, result={"id": 1} if status is S.SUCCESSFUL else None))
    with pytest.raises(NotReopenableError):
        repo.reopen(entry=_entry(), requeue_id=_uuid(3), operator="ana", reason="r")
    with psycopg.connect(postgres_conninfo) as conn:
        assert conn.execute("SELECT count(*) FROM requeue_audit").fetchone() == (0,)  # rollback


def _uuid(n: int) -> str:
    return f"00000000-0000-0000-0000-{n:012d}"
