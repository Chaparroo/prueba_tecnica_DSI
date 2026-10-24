from __future__ import annotations

from rpa_worker.application.circuit_breaker import BreakerSignal, CircuitBreaker, CircuitState
from tests.fakes import FakeClock

F, S, N = BreakerSignal.FAILURE, BreakerSignal.SUCCESS, BreakerSignal.NEUTRAL


def _breaker(clock: FakeClock) -> CircuitBreaker:
    return CircuitBreaker(threshold=3, cooldown_seconds=60, clock=clock)


def state(b: CircuitBreaker) -> CircuitState:
    # Lectura vía función: el estado cambia entre llamadas y mypy no debe estrecharlo.
    return b.state


def test_opens_after_threshold_consecutive_failures() -> None:
    b = _breaker(FakeClock())
    for _ in range(2):
        b.record(F)
    assert state(b) is CircuitState.CLOSED
    b.record(F)
    assert state(b) is CircuitState.OPEN
    assert not b.allows_request()


def test_success_resets_counter_and_neutral_does_not() -> None:
    b = _breaker(FakeClock())
    b.record(F)
    b.record(F)
    b.record(N)  # 429: ni suma ni reinicia
    assert b.consecutive_failures == 2
    b.record(S)
    assert b.consecutive_failures == 0


def test_half_open_probe_success_closes() -> None:
    clock = FakeClock()
    b = _breaker(clock)
    for _ in range(3):
        b.record(F)
    clock.advance(59)
    assert not b.allows_request()
    clock.advance(1)
    assert b.allows_request()
    assert state(b) is CircuitState.HALF_OPEN
    b.record(S)
    assert state(b) is CircuitState.CLOSED


def test_half_open_probe_failure_reopens_immediately() -> None:
    clock = FakeClock()
    b = _breaker(clock)
    for _ in range(3):
        b.record(F)
    clock.advance(60)
    assert b.allows_request()
    b.record(F)
    assert state(b) is CircuitState.OPEN
    assert b.seconds_until_probe() == 60
