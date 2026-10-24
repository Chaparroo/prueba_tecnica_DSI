"""Máquina de estados REFramework.

    Init -> Get Transaction Data -> Process Transaction -> (Get Transaction Data ...)
                       |
                       +-- ShouldStop activo --> End Process (exit 0)

ShouldStop solo se consulta en Get Transaction Data: el item en vuelo siempre
completa su ciclo (persistir y luego ACK).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import structlog
from opentelemetry import propagate, trace

from rpa_worker.application.circuit_breaker import BreakerSignal, CircuitBreaker, CircuitState
from rpa_worker.application.health import HealthState, WorkerState
from rpa_worker.application.ports import (
    Clock,
    Delivery,
    Metrics,
    RemoteStopFlag,
    Sleeper,
    TransactionQueue,
)
from rpa_worker.application.process_transaction import ItemResult
from rpa_worker.application.settings import RunnerSettings
from rpa_worker.application.should_stop import ShouldStop
from rpa_worker.domain.exceptions import SystemException

log = structlog.get_logger(__name__)
tracer = trace.get_tracer("rpa_worker.runner")

ProcessFn = Callable[[Delivery], ItemResult]


class Runner:
    def __init__(
        self,
        *,
        queue: TransactionQueue,
        remote_stop: RemoteStopFlag,
        process: ProcessFn,
        should_stop: ShouldStop,
        health: HealthState,
        clock: Clock,
        sleeper: Sleeper,
        metrics: Metrics,
        settings: RunnerSettings,
        closers: Sequence[Callable[[], None]] = (),
    ) -> None:
        self._queue = queue
        self._remote_stop = remote_stop
        self._process = process
        self._stop = should_stop
        self._health = health
        self._clock = clock
        self._sleeper = sleeper
        self._metrics = metrics
        self._s = settings
        self._closers = closers
        self.breaker = CircuitBreaker(
            settings.max_consecutive_system_exceptions, settings.circuit_cooldown_seconds, clock
        )
        self._last_circuit_state = CircuitState.CLOSED

    def run(self) -> int:
        try:
            if self._init():
                while self._get_transaction_data_and_process():
                    pass
        finally:
            self._end_process()
        return 0

    # ----------------------------------------------------------------- Init

    def _init(self) -> bool:
        self._health.set_state(WorkerState.STARTING)
        log.info("runner.state", state="Init")
        while not self._stop.is_set():
            try:
                self._queue.prepare()
                self._metrics.circuit_state(CircuitState.CLOSED)
                return True
            except SystemException as exc:
                log.error("runner.init_failed", reason=exc.reason, error=str(exc))
                self._wait(self._s.fetch_error_backoff_seconds)
        return False

    # ------------------------------------------------- Get Transaction Data

    def _stop_requested(self) -> bool:
        if not self._stop.is_set():
            try:
                if self._remote_stop.is_requested():
                    self._stop.request("redis_key")
            except SystemException as exc:
                log.warning("runner.remote_stop_check_failed", reason=exc.reason)
        return self._stop.is_set()

    def _get_transaction_data_and_process(self) -> bool:
        """Una vuelta del loop. Devuelve False para pasar a End Process."""
        self._health.beat()
        if self._stop_requested():
            return False

        if not self.breaker.allows_request():
            self._sync_circuit()
            self._health.set_state(WorkerState.DEGRADED)
            self._wait(self.breaker.seconds_until_probe())
            return True
        self._sync_circuit()

        try:
            self._queue.promote_due(self._clock.now())
            delivery = self._queue.fetch_next()
        except SystemException as exc:
            log.error("runner.fetch_failed", reason=exc.reason, error=str(exc))
            self._record(BreakerSignal.FAILURE)
            self._wait(self._s.fetch_error_backoff_seconds)
            return True

        if delivery is None:
            self._health.set_state(
                WorkerState.IDLE
                if self.breaker.state is CircuitState.CLOSED
                else WorkerState.DEGRADED
            )
            return True

        if self._stop.is_set():
            # ShouldStop llegó durante el XREADGROUP bloqueante: no se toma el
            # item. Se devuelve a la cola (mismo retry_count) para no esperar el lease.
            self._release(delivery)
            return False

        self._health.set_state(WorkerState.PROCESSING)
        result = self._process_one(delivery)
        self._record(result.signal)
        if result.pause_seconds > 0:
            self._pause_for_rate_limit(result.pause_seconds)
        return True

    # --------------------------------------------------- Process Transaction

    def _process_one(self, delivery: Delivery) -> ItemResult:
        tx = delivery.transaction
        parent = propagate.extract({"traceparent": tx.traceparent} if tx.traceparent else {})
        with (
            structlog.contextvars.bound_contextvars(
                reference=tx.reference, correlation_id=tx.correlation_id
            ),
            tracer.start_as_current_span(
                "process_transaction",
                context=parent,
                attributes={"rpa.priority": tx.priority, "rpa.retry_count": tx.retry_count},
            ),
        ):
            try:
                return self._process(delivery)
            except Exception as exc:  # frontera: un bug no debe tumbar el loop
                log.exception("transaction.unexpected_error")
                self._note_failure(delivery, exc)
                # Sin ACK: se reclama tras el lease; si se repite, max_deliveries lo saca.
                return ItemResult(BreakerSignal.FAILURE)

    def _note_failure(self, delivery: Delivery, exc: Exception) -> None:
        try:
            self._queue.note_failure(delivery, f"{type(exc).__name__}: {exc}")
        except SystemException as err:
            log.warning("runner.note_failure_failed", reason=err.reason)

    def _pause_for_rate_limit(self, seconds: float) -> None:
        """429 final: no tomar items hasta el Retry-After (con tope). Interrumpible."""
        pause = min(seconds, self._s.max_rate_limit_pause_seconds)
        log.warning("runner.rate_limit_pause", pause_seconds=pause)
        self._health.set_state(WorkerState.DEGRADED)
        self._wait(pause)

    def _release(self, delivery: Delivery) -> None:
        try:
            self._queue.schedule_retry(delivery, delivery.transaction, self._clock.now())
            log.info("runner.delivery_released", reference=delivery.transaction.reference)
        except SystemException as exc:
            log.warning("runner.release_failed", reason=exc.reason)

    # ---------------------------------------------------------- End Process

    def _end_process(self) -> None:
        self._health.set_state(WorkerState.STOPPING)
        log.info("runner.state", state="EndProcess", stop_origin=self._stop.origin)
        for close in self._closers:
            try:
                close()
            except Exception:  # cerrar todo lo posible aunque uno falle
                log.exception("runner.close_failed")

    # -------------------------------------------------------------- apoyo

    def _record(self, signal: BreakerSignal) -> None:
        self.breaker.record(signal)
        self._sync_circuit()

    def _sync_circuit(self) -> None:
        state = self.breaker.state
        if state is not self._last_circuit_state:
            log.warning(
                "runner.circuit_changed",
                from_state=str(self._last_circuit_state),
                to_state=str(state),
                consecutive_system_exceptions=self.breaker.consecutive_failures,
            )
            self._metrics.circuit_state(state)
            self._last_circuit_state = state

    def _wait(self, seconds: float) -> None:
        """Espera interrumpible por ShouldStop que sigue latiendo para /livez."""
        deadline = self._clock.monotonic() + seconds
        while not self._stop.is_set():
            remaining = deadline - self._clock.monotonic()
            if remaining <= 0:
                return
            self._health.beat()
            if self._sleeper.sleep(min(remaining, self._s.heartbeat_interval_seconds)):
                return
