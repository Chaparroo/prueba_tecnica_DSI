"""MaxConsecutiveSystemExceptions como circuit breaker.

CLOSED --(N fallos de sistema seguidos)--> OPEN --(cooldown)--> HALF_OPEN
HALF_OPEN --(éxito)--> CLOSED ; HALF_OPEN --(fallo)--> OPEN

Un resultado de negocio (Failed) cuenta como éxito: el sistema respondió.
Un 429 es neutro: ni suma ni reinicia el contador.
"""

from __future__ import annotations

from enum import StrEnum

from rpa_worker.application.clock import MonotonicSource


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BreakerSignal(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    NEUTRAL = "neutral"


class CircuitBreaker:
    def __init__(self, threshold: int, cooldown_seconds: float, clock: MonotonicSource) -> None:
        self._threshold = threshold
        self._cooldown = cooldown_seconds
        self._clock = clock
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._probing = False

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def state(self) -> CircuitState:
        if self._opened_at is None:
            return CircuitState.HALF_OPEN if self._probing else CircuitState.CLOSED
        if self._clock.monotonic() - self._opened_at >= self._cooldown:
            return CircuitState.HALF_OPEN
        return CircuitState.OPEN

    def seconds_until_probe(self) -> float:
        if self._opened_at is None:
            return 0.0
        return max(0.0, self._opened_at + self._cooldown - self._clock.monotonic())

    def allows_request(self) -> bool:
        if self.state is CircuitState.HALF_OPEN and self._opened_at is not None:
            # Fin del cooldown: se permite exactamente un item de prueba.
            self._opened_at = None
            self._probing = True
        return self.state is not CircuitState.OPEN

    def record(self, signal: BreakerSignal) -> None:
        if signal is BreakerSignal.SUCCESS:
            self._consecutive_failures = 0
            self._opened_at = None
            self._probing = False
        elif signal is BreakerSignal.FAILURE:
            self._consecutive_failures += 1
            if self._probing or self._consecutive_failures >= self._threshold:
                self._opened_at = self._clock.monotonic()
                self._probing = False
