"""Reapertura manual de un item del DLQ (``rpa-worker requeue``).

La entrada del DLQ es la fuente de verdad. Dos pasos, cada uno idempotente:
  1. Postgres (una transacción): auditoría con requeue_id determinista y
     Abandoned -> Retried (o crear la fila en Retried; si ya está en Retried,
     continuar).
  2. Redis (un script Lua): XDEL de la entrada del DLQ y, solo si existía,
     XADD al stream de su prioridad. El XDEL es la puerta contra duplicados.
Si el proceso muere entre 1 y 2, volver a ejecutar el comando lo completa:
la entrada sigue en el DLQ, el requeue_id es el mismo y la auditoría no se
duplica.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from enum import StrEnum

import structlog
from opentelemetry import propagate, trace

from rpa_worker.application.ports import (
    Clock,
    DeadLetterStore,
    NotReopenableError,
    RequeueRepository,
)
from rpa_worker.domain.transaction import TransactionStatus

log = structlog.get_logger(__name__)
tracer = trace.get_tracer("rpa_worker.requeue")

_REQUEUE_NAMESPACE = uuid.UUID("3b8f1f8e-6f0e-4d4a-9c55-6a0d3f6c2a10")


class RequeueStatus(StrEnum):
    REQUEUED = "requeued"
    # Otro operador (o una ejecución anterior) ya lo encoló: no se hizo nada.
    ALREADY_REQUEUED = "already_requeued"


@dataclass(frozen=True, slots=True)
class RequeueResult:
    status: RequeueStatus
    reference: str
    requeue_id: str | None = None
    requeue_count: int | None = None


class NotInDlqError(Exception):
    pass


def requeue_id_for(dlq_entry_id: str) -> str:
    """Determinista por entrada del DLQ: re-ejecutar no crea otra auditoría."""
    return str(uuid.uuid5(_REQUEUE_NAMESPACE, dlq_entry_id))


class RequeueFromDlq:
    def __init__(self, *, dlq: DeadLetterStore, repository: RequeueRepository, clock: Clock):
        self._dlq = dlq
        self._repo = repository
        self._clock = clock

    def __call__(self, reference: str, *, operator: str, reason: str) -> RequeueResult:
        if not operator.strip() or not reason.strip():
            raise ValueError("--operator y --reason son obligatorios")

        entry = self._dlq.find(reference)
        if entry is None:
            status = self._repo.current_status(reference)
            if status in (TransactionStatus.SUCCESSFUL, TransactionStatus.FAILED):
                raise NotReopenableError(reference, status)
            if status is TransactionStatus.RETRIED:
                return RequeueResult(RequeueStatus.ALREADY_REQUEUED, reference)
            raise NotInDlqError(f"{reference} no está en el DLQ (estado en BD: {status})")

        original = entry.transaction
        requeue_id = requeue_id_for(entry.entry_id)
        with structlog.contextvars.bound_contextvars(
            reference=reference, correlation_id=original.correlation_id, requeue_id=requeue_id
        ):
            # Paso 1: Postgres (lanza NotReopenableError si es Successful/Failed).
            requeue_count = self._repo.reopen(
                entry=entry, requeue_id=requeue_id, operator=operator, reason=reason
            )
            with tracer.start_as_current_span(
                "requeue", attributes={"rpa.requeue_count": requeue_count}
            ):
                carrier: dict[str, str] = {}
                propagate.inject(carrier)
                tx = replace(
                    original,
                    status=TransactionStatus.NEW,
                    retry_count=0,
                    requeue_count=requeue_count,
                    requeue_id=requeue_id,
                    enqueued_at=self._clock.now(),
                    traceparent=carrier.get("traceparent", original.traceparent),
                )
                # Paso 2: Redis (XDEL como puerta + XADD).
                restored = self._dlq.restore(entry, tx)

            log.info(
                "transaction.requeued" if restored else "transaction.requeue_skipped",
                operator=operator,
                requeue_reason=reason,
                dlq_reason=entry.reason,
                requeue_count=requeue_count,
                from_status=str(TransactionStatus.ABANDONED),
                to_status=str(TransactionStatus.RETRIED),
            )
        outcome = RequeueStatus.REQUEUED if restored else RequeueStatus.ALREADY_REQUEUED
        return RequeueResult(outcome, reference, requeue_id, requeue_count)
