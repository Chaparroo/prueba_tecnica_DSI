from __future__ import annotations

from dataclasses import replace

import pytest

from rpa_worker.application.ports import NotReopenableError
from rpa_worker.application.requeue import (
    NotInDlqError,
    RequeueFromDlq,
    RequeueStatus,
    requeue_id_for,
)
from rpa_worker.domain import policies
from rpa_worker.domain.exceptions import SystemException
from rpa_worker.domain.transaction import TransactionStatus as S
from tests.conftest import Harness
from tests.fakes import make_tx


def _requeue(h: Harness) -> RequeueFromDlq:
    return RequeueFromDlq(dlq=h.queue, repository=h.repo, clock=h.clock)


def _abandon(h: Harness, reference: str = "REF-AB", post_id: int = 7) -> None:
    err = policies.classify_http_status(503)
    assert err is not None
    h.api.scripted[post_id] = [err] * 9
    h.queue.push(make_tx(reference, post_id))
    h.drain_with_retries()
    h.run()
    assert h.repo.rows[reference].record.status is S.ABANDONED


def test_abandoned_requeue_then_successful_writes_result_once(h: Harness) -> None:
    _abandon(h)
    result = _requeue(h)("REF-AB", operator="ana", reason="API restaurada")

    assert result.status is RequeueStatus.REQUEUED
    assert result.requeue_count == 1
    row = h.repo.rows["REF-AB"].record
    assert (row.status, row.retry_count, row.requeue_count) == (S.RETRIED, 0, 1)
    assert h.queue.dlq == []
    (delivery,) = h.queue.streams["normal"]
    tx = delivery.transaction
    assert (tx.retry_count, tx.requeue_count, tx.requeue_id) == (0, 1, result.requeue_id)
    assert tx.correlation_id == "corr-REF-AB"  # se conserva para seguirlo en Loki

    h.restart()
    h.stop_when_idle()
    h.run()
    row_after = h.repo.rows["REF-AB"]
    assert row_after.record.status is S.SUCCESSFUL  # Retried no es terminal: el upsert escribe
    assert row_after.record.requeue_count == 1
    assert row_after.result_writes == 1
    assert [(a.operator, a.reason) for a in h.repo.audit] == [("ana", "API restaurada")]


def test_second_requeue_does_not_duplicate(h: Harness) -> None:
    _abandon(h)
    first = _requeue(h)("REF-AB", operator="ana", reason="r1")
    second = _requeue(h)("REF-AB", operator="luis", reason="r2")
    assert first.status is RequeueStatus.REQUEUED
    assert second.status is RequeueStatus.ALREADY_REQUEUED
    assert len(h.queue.streams["normal"]) == 1


def test_crash_between_postgres_and_redis_then_rerun_completes(h: Harness) -> None:
    _abandon(h)
    h.queue.fail_next_redis_op = True  # muere tras el paso 1
    with pytest.raises(SystemException):
        _requeue(h)("REF-AB", operator="ana", reason="r")
    assert h.repo.rows["REF-AB"].record.status is S.RETRIED
    assert len(h.queue.dlq) == 1  # la entrada sigue: es la fuente de verdad

    result = _requeue(h)("REF-AB", operator="ana", reason="r")
    assert result.status is RequeueStatus.REQUEUED
    assert result.requeue_count == 1  # no se cuenta dos veces
    assert len(h.repo.audit) == 1
    assert len(h.queue.streams["normal"]) == 1
    assert h.queue.dlq == []


@pytest.mark.parametrize("status", [S.SUCCESSFUL, S.FAILED])
def test_successful_and_failed_are_not_reopened(h: Harness, status: S) -> None:
    _abandon(h)
    row = h.repo.rows["REF-AB"]
    row.record = replace(row.record, status=status)
    with pytest.raises(NotReopenableError):
        _requeue(h)("REF-AB", operator="ana", reason="r")
    assert len(h.queue.dlq) == 1
    assert h.repo.audit == []


def test_poison_entry_without_row_creates_row_in_retried(h: Harness) -> None:
    d = h.queue.push(make_tx("REF-PZ", 1))
    h.queue.streams["normal"][0] = replace(d, delivery_count=99)
    h.repo.down = True
    h.stop_when_idle()
    h.run()
    h.repo.down = False
    assert "REF-PZ" not in h.repo.rows

    result = _requeue(h)("REF-PZ", operator="ana", reason="fix desplegado")
    assert result.status is RequeueStatus.REQUEUED
    row = h.repo.rows["REF-PZ"].record
    assert (row.status, row.requeue_count) == (S.RETRIED, 1)


def test_not_in_dlq(h: Harness) -> None:
    with pytest.raises(NotInDlqError):
        _requeue(h)("REF-NONE", operator="ana", reason="r")


@pytest.mark.parametrize(("operator", "reason"), [("", "r"), ("ana", "  ")])
def test_operator_and_reason_are_required(h: Harness, operator: str, reason: str) -> None:
    with pytest.raises(ValueError, match="obligatorios"):
        _requeue(h)("REF-X", operator=operator, reason=reason)


def test_requeue_id_is_deterministic_per_dlq_entry() -> None:
    assert requeue_id_for("1-0") == requeue_id_for("1-0")
    assert requeue_id_for("1-0") != requeue_id_for("2-0")
