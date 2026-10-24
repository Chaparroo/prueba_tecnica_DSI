"""Estado Process Transaction: ejecuta un item y decide su estado final.

Orden de efectos (garantiza que reprocesar el mismo mensaje sea seguro):
  1. Postgres: registrar el estado (Successful/Failed/Retried/Abandoned).
  2. Redis, en un solo script Lua: XACK, o ZADD+XACK, o XADD DLQ+XACK.
Si el proceso muere entre 1 y 2, el mensaje sigue pendiente; al reclamarse
lleva el mismo retry_count, así que produce la misma fila y la misma acción.

Fallo transitorio de infraestructura (Postgres caído): el item se *libera*
(ZADD+XACK con el mismo retry_count y ready_at corto) en vez de quedar
pendiente. Así una caída larga no infla delivery_count ni manda items sanos
al DLQ por max_deliveries; esa guarda queda solo para mensajes venenosos.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from functools import partial

import structlog

from rpa_worker.application.circuit_breaker import BreakerSignal
from rpa_worker.application.ports import (
    Clock,
    Delivery,
    ExternalApi,
    Metrics,
    ResultRepository,
    Sleeper,
    TransactionQueue,
    TransactionRecord,
)
from rpa_worker.application.settings import ProcessingSettings
from rpa_worker.domain import policies
from rpa_worker.domain.exceptions import AlertKind, BusinessException, SystemException
from rpa_worker.domain.transaction import Transaction, TransactionStatus

log = structlog.get_logger(__name__)

S = TransactionStatus


class _PostponedByShutdownError(Exception):
    """ShouldStop interrumpió un backoff dentro del item."""


@dataclass(frozen=True, slots=True)
class ItemResult:
    """Lo que el runner necesita saber tras procesar un item."""

    signal: BreakerSignal
    # > 0 tras un 429 final: el runner no toma items durante este tiempo.
    pause_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class Outcome:
    status: TransactionStatus
    retry_count: int
    signal: BreakerSignal
    result: Mapping[str, object] | None = None
    reason: str | None = None
    error: str | None = None
    ready_at: float | None = None
    alert: AlertKind | None = None
    pause_seconds: float = 0.0


def log_transition(
    transaction: Transaction,
    to: TransactionStatus,
    *,
    src: TransactionStatus | None = None,
    **kw: object,
) -> None:
    """Registro de SetTransactionStatus. reference/correlation_id vienen de
    contextvars, pero se repiten aquí para que la línea sea autocontenida."""
    log.info(
        "transaction.status_changed",
        reference=transaction.reference,
        correlation_id=transaction.correlation_id,
        from_status=str(src or transaction.status),
        to_status=str(to),
        **kw,
    )


class ProcessTransaction:
    def __init__(
        self,
        *,
        api: ExternalApi,
        repository: ResultRepository,
        queue: TransactionQueue,
        clock: Clock,
        sleeper: Sleeper,
        metrics: Metrics,
        settings: ProcessingSettings,
        heartbeat: Callable[[], None] = lambda: None,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._api = api
        self._repo = repository
        self._queue = queue
        self._clock = clock
        self._sleeper = sleeper
        self._metrics = metrics
        self._s = settings
        self._beat = heartbeat
        self._rand = rand

    # ------------------------------------------------------------------ API

    def __call__(self, delivery: Delivery) -> ItemResult:
        tx = delivery.transaction
        started = self._clock.monotonic()
        log_transition(tx, S.IN_PROGRESS, delivery_count=delivery.delivery_count)

        # Antes de tocar Postgres: un mensaje venenoso debe salir aunque la BD falle.
        if delivery.delivery_count > self._s.max_deliveries:
            return self._abandon_poison(delivery, started)

        try:
            existing = self._repo.find_terminal_status(tx.reference)
        except SystemException as exc:
            return self._release_after_infra_failure(delivery, exc, started)
        if existing is not None:
            # Doble entrega de un item ya cerrado: no se reprocesa, se acusa.
            log.info("transaction.duplicate_delivery", existing_status=str(existing))
            signal = self._redis_step(
                delivery, partial(self._queue.ack, delivery), BreakerSignal.NEUTRAL
            )
            return ItemResult(signal)

        return self._finish(delivery, self._execute(tx), started)

    # ------------------------------------------------------------ ejecución

    def _execute(self, tx: Transaction) -> Outcome:
        try:
            post_id = policies.parse_post_id(tx.payload)
            body = self._call_with_retries(post_id)
            result = policies.validate_api_result(body)
        except BusinessException as exc:
            # El sistema funcionó: para el breaker es un éxito.
            return Outcome(
                S.FAILED, tx.retry_count, BreakerSignal.SUCCESS, reason=exc.reason, error=str(exc)
            )
        except _PostponedByShutdownError:
            return Outcome(
                S.RETRIED,
                tx.retry_count,  # no consume MaxRetryNumber
                BreakerSignal.NEUTRAL,
                reason="shutdown_during_backoff",
                ready_at=self._clock.now() + self._s.release_delay_seconds,
            )
        except SystemException as exc:
            return self._system_outcome(tx, exc)
        return Outcome(S.SUCCESSFUL, tx.retry_count, BreakerSignal.SUCCESS, result=result)

    def _call_with_retries(self, post_id: int) -> object:
        """Capa 1: hasta max_attempts con backoff exponencial + jitter."""
        attempt = 1
        while True:
            self._beat()
            try:
                return self._api.fetch_post(post_id)
            except SystemException as exc:
                if not exc.retryable or attempt >= self._s.max_attempts:
                    raise
                if exc.retry_after is not None:
                    if exc.retry_after > self._s.max_retry_after_seconds:
                        raise  # Retry-After largo: se reencola en vez de esperar en caliente
                    delay = exc.retry_after
                else:
                    delay = policies.backoff_delay(
                        attempt,
                        self._s.backoff_base_seconds,
                        self._s.backoff_max_seconds,
                        self._rand,
                    )
                self._metrics.retry("in_item", exc.reason)
                log.warning(
                    "transaction.attempt_failed",
                    attempt=attempt,
                    reason=exc.reason,
                    retry_in_seconds=round(delay, 3),
                )
                if self._sleeper.sleep(delay):
                    raise _PostponedByShutdownError from exc
                attempt += 1

    def _system_outcome(self, tx: Transaction, exc: SystemException) -> Outcome:
        # Neutro = ni suma ni reinicia el contador de excepciones consecutivas.
        breaker = BreakerSignal.NEUTRAL if exc.rate_limited else BreakerSignal.FAILURE
        pause = 0.0
        if exc.rate_limited:
            pause = exc.retry_after if exc.retry_after is not None else self._s.backoff_max_seconds

        status = S.ABANDONED
        retry_count = tx.retry_count
        ready_at: float | None = None
        exhausted = (
            policies.status_after_system_failure(tx.retry_count, self._s.max_retry_number)
            is S.ABANDONED
        )
        if exc.retryable and not exhausted:
            status = S.RETRIED
            retry_count = tx.retry_count + 1
            if exc.retry_after is not None:
                delay = exc.retry_after
            else:
                delay = policies.item_retry_delay(
                    retry_count, self._s.item_retry_base_seconds, self._s.max_retry_delay_seconds
                )
            # Tope absoluto de ready_at.
            ready_at = self._clock.now() + min(delay, self._s.max_retry_delay_seconds)
        return Outcome(
            status,
            retry_count,
            breaker,
            reason=exc.reason,
            error=str(exc),
            ready_at=ready_at,
            alert=exc.alert,
            pause_seconds=pause,
        )

    # ------------------------------------------------------- persistir + ACK

    def _record_for(self, tx: Transaction, outcome: Outcome) -> TransactionRecord:
        return TransactionRecord(
            reference=tx.reference,
            status=outcome.status,
            priority=tx.priority,
            retry_count=outcome.retry_count,
            correlation_id=tx.correlation_id,
            requeue_count=tx.requeue_count,
            result=outcome.result,
            error_reason=outcome.reason,
            finished_at=self._clock.now() if outcome.status.is_terminal else None,
        )

    def _finish(self, delivery: Delivery, outcome: Outcome, started: float) -> ItemResult:
        tx = delivery.transaction
        self._beat()
        try:
            if not self._repo.record(self._record_for(tx, outcome)):  # 1. Postgres primero
                log.info("transaction.idempotent_noop", attempted_status=str(outcome.status))
        except SystemException as exc:
            if exc.retryable:
                return self._release_after_infra_failure(delivery, exc, started)
            return self._abandon_rejected_row(delivery, exc, started)

        log_transition(
            tx,
            outcome.status,
            src=S.IN_PROGRESS,
            retry_count=outcome.retry_count,
            reason=outcome.reason,
        )
        if outcome.alert is not None:
            self._raise_alert(outcome.alert, outcome.reason or "unknown", outcome.error)

        updated = tx.with_retry_count(outcome.retry_count).with_status(outcome.status)
        reason = outcome.reason or "unknown"
        action: Callable[[], None]
        if outcome.status is S.RETRIED:
            if outcome.ready_at is None:
                raise RuntimeError("Outcome Retried sin ready_at")  # invariante interna
            action = partial(self._queue.schedule_retry, delivery, updated, outcome.ready_at)
            self._metrics.retry("item", reason)
        elif outcome.status is S.ABANDONED:
            action = partial(self._queue.dead_letter, delivery, updated, reason, outcome.error)
        else:
            action = partial(self._queue.ack, delivery)

        signal = self._redis_step(delivery, action, outcome.signal)  # 2. Redis después
        self._metrics.transaction_finished(
            outcome.status, tx.priority, self._clock.monotonic() - started
        )
        return ItemResult(signal, outcome.pause_seconds)

    # ------------------------------------------------------- caminos de fallo

    def _release_after_infra_failure(
        self, delivery: Delivery, exc: SystemException, started: float
    ) -> ItemResult:
        """Postgres (u otra dependencia) no respondió: devolver el item sin procesar.

        No consume retry_count ni max_deliveries (cada liberación es una entrada
        nueva en el stream, al final de su prioridad: puede alterar el orden).
        El ready_at crece con release_count y el tope es por antigüedad: así una
        caída larga de Postgres no manda items sanos al DLQ, pero un item que
        falla "transitoriamente" para siempre no se libera para siempre.
        """
        log.error("transaction.infra_failure", reason=exc.reason, error=str(exc))
        if exc.alert is not None:
            self._raise_alert(exc.alert, exc.reason, str(exc))
        tx = delivery.transaction
        now = self._clock.now()
        first_enqueued_at = tx.first_enqueued_at or now
        age = now - first_enqueued_at
        if age > self._s.max_item_age_seconds:
            outcome = Outcome(
                S.ABANDONED,
                tx.retry_count,
                BreakerSignal.FAILURE,
                reason="max_age",
                error=(
                    f"{exc.reason}: {exc} (antigüedad {age:.0f}s, {tx.release_count} liberaciones)"
                ),
                alert=AlertKind.MAX_AGE,
            )
            return self._dead_letter_best_effort(delivery, outcome, started)

        delay = min(
            self._s.release_delay_seconds * (2.0**tx.release_count),
            self._s.max_release_delay_seconds,
        )
        released = replace(
            tx, release_count=tx.release_count + 1, first_enqueued_at=first_enqueued_at
        )
        release = partial(self._queue.schedule_retry, delivery, released, now + delay)
        if self._redis_step(delivery, release, BreakerSignal.NEUTRAL) is BreakerSignal.NEUTRAL:
            self._metrics.retry("release", exc.reason)
            log.info(
                "transaction.released",
                reason=exc.reason,
                release_count=released.release_count,
                retry_in_seconds=delay,
            )
        # Sea cual sea el resultado en Redis, es una excepción de sistema para el breaker.
        return ItemResult(BreakerSignal.FAILURE)

    def _abandon_rejected_row(
        self, delivery: Delivery, exc: SystemException, started: float
    ) -> ItemResult:
        """La BD rechaza la fila (dato inválido para el esquema): contrato roto."""
        tx = delivery.transaction
        outcome = Outcome(
            S.ABANDONED,
            tx.retry_count,
            BreakerSignal.FAILURE,
            reason=exc.reason,
            error=str(exc),
            alert=exc.alert or AlertKind.CONTRACT,
        )
        return self._dead_letter_best_effort(delivery, outcome, started)

    def _abandon_poison(self, delivery: Delivery, started: float) -> ItemResult:
        tx = delivery.transaction
        outcome = Outcome(
            S.ABANDONED,
            tx.retry_count,
            BreakerSignal.NEUTRAL,  # el problema es el item, no el sistema
            reason="max_deliveries",
            alert=AlertKind.POISON,
        )
        return self._dead_letter_best_effort(delivery, outcome, started)

    def _dead_letter_best_effort(
        self, delivery: Delivery, outcome: Outcome, started: float
    ) -> ItemResult:
        """Abandoned sin depender de Postgres: si no se puede registrar, igual va al DLQ."""
        tx = delivery.transaction
        try:
            self._repo.record(self._record_for(tx, outcome))
        except SystemException as exc:
            log.error("transaction.persist_failed", reason=exc.reason, error=str(exc))
        log_transition(
            tx, S.ABANDONED, src=S.IN_PROGRESS, retry_count=tx.retry_count, reason=outcome.reason
        )
        reason = outcome.reason or "unknown"
        if outcome.alert is not None:
            self._raise_alert(outcome.alert, reason, outcome.error)
        updated = tx.with_status(S.ABANDONED)
        action = partial(self._queue.dead_letter, delivery, updated, reason, outcome.error)
        signal = self._redis_step(delivery, action, outcome.signal)
        self._metrics.transaction_finished(
            S.ABANDONED, tx.priority, self._clock.monotonic() - started
        )
        return ItemResult(signal)

    def _raise_alert(self, kind: AlertKind, reason: str, error: str | None) -> None:
        log.error("transaction.requires_attention", alert=str(kind), reason=reason, error=error)
        self._metrics.alert(kind, reason)

    def _redis_step(
        self, delivery: Delivery, action: Callable[[], None], signal: BreakerSignal
    ) -> BreakerSignal:
        try:
            action()
        except SystemException as exc:
            # Postgres ya tiene el estado; el mensaje sigue pendiente y al
            # reclamarse producirá la misma fila (idempotente) y la misma acción.
            log.error(
                "transaction.queue_update_failed",
                message_id=delivery.message_id,
                reason=exc.reason,
                error=str(exc),
            )
            return BreakerSignal.FAILURE
        return signal
