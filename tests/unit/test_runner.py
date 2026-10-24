"""Comportamiento del runner REFramework con fakes en memoria."""

from __future__ import annotations

import signal
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace
from typing import Any

import pytest
from structlog.testing import capture_logs

from rpa_worker.application.circuit_breaker import CircuitState
from rpa_worker.application.health import WorkerState
from rpa_worker.application.should_stop import install_signal_handlers
from rpa_worker.domain import policies
from rpa_worker.domain.exceptions import AlertKind
from rpa_worker.domain.transaction import TransactionStatus as S
from tests.conftest import Harness
from tests.fakes import make_tx

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def http(status: int, retry_after: float | None = None) -> Exception:
    err = policies.classify_http_status(status, retry_after)
    assert err is not None
    return err


def transitions(logs: Sequence[Mapping[str, Any]], reference: str) -> list[tuple[object, object]]:
    return [
        (e["from_status"], e["to_status"])
        for e in logs
        if e["event"] == "transaction.status_changed" and e["reference"] == reference
    ]


# ------------------------------------------------------------- flujo feliz


def test_happy_path_persists_then_acks_and_logs_every_transition(h: Harness) -> None:
    a = h.queue.push(make_tx("REF-A", 1))
    h.queue.push(make_tx("REF-B", 2, priority="high"))
    h.stop_when_idle()

    with capture_logs() as logs:
        assert h.run() == 0

    assert h.queue.fetched == ["REF-B", "REF-A"]  # prioridad respetada
    for ref in ("REF-A", "REF-B"):
        row = h.repo.rows[ref]
        assert row.record.status is S.SUCCESSFUL
        assert row.record.result is not None
        assert row.result_writes == 1
        assert row.record.finished_at is not None
    assert a.message_id in h.queue.acked
    assert not h.queue.pending
    assert transitions(logs, "REF-A") == [("New", "InProgress"), ("InProgress", "Successful")]
    assert h.closed == ["closed"]
    assert h.health.snapshot().state is WorkerState.STOPPING


def _scenario_success(h: Harness) -> None:
    h.queue.push(make_tx("REF-OK", 1))


def _scenario_business(h: Harness) -> None:
    h.api.scripted[101] = [http(404)]
    h.queue.push(make_tx("REF-404", 101))


def _scenario_retry_then_abandon(h: Harness) -> None:
    h.api.scripted[7] = [http(503)] * 9
    h.queue.push(make_tx("REF-503", 7))


def _scenario_non_retryable(h: Harness) -> None:
    h.api.scripted[3] = [http(401)]
    h.queue.push(make_tx("REF-401", 3))


def _scenario_shutdown_in_backoff(h: Harness) -> None:
    h.api.scripted[6] = [http(503)]
    h.queue.push(make_tx("REF-BO", 6))
    h.sleeper.on_sleep = lambda: h.should_stop.request("signal:15")


def _scenario_poison(h: Harness) -> None:
    d = h.queue.push(make_tx("REF-P", 1))
    h.queue.streams["normal"][0] = replace(d, delivery_count=99)


@pytest.mark.parametrize(
    "scenario",
    [
        _scenario_success,
        _scenario_business,
        _scenario_retry_then_abandon,
        _scenario_non_retryable,
        _scenario_shutdown_in_backoff,
        _scenario_poison,
    ],
)
def test_every_status_change_carries_reference_and_correlation_id(
    h: Harness, scenario: Any
) -> None:
    scenario(h)
    h.drain_with_retries()
    with capture_logs() as logs:
        h.run()
    changes = [e for e in logs if e["event"] == "transaction.status_changed"]
    assert len(changes) >= 2  # al menos New->InProgress y la salida de InProgress
    for e in changes:
        assert e["reference"].startswith("REF-")
        assert e["correlation_id"] == f"corr-{e['reference']}"
        assert e["from_status"]
        assert e["to_status"]


# --------------------------------------------------------------- negocio


def test_404_is_business_failed_without_retry(h: Harness) -> None:
    h.api.scripted[101] = [http(404)]
    h.queue.push(make_tx("REF-404", 101))
    h.stop_when_idle()
    h.run()

    row = h.repo.rows["REF-404"].record
    assert row.status is S.FAILED
    assert row.error_reason == "not_found"
    assert h.api.calls == [101]
    assert not h.queue.retry_zset
    assert not h.queue.dlq
    assert h.runner.breaker.consecutive_failures == 0


@pytest.mark.parametrize(
    ("payload", "reason"), [({"id": "abc"}, "non_numeric_id"), ({}, "invalid_payload")]
)
def test_invalid_payload_fails_without_calling_api(
    h: Harness, payload: dict[str, object], reason: str
) -> None:
    h.queue.push(make_tx("REF-BAD", payload=payload))
    h.stop_when_idle()
    h.run()
    assert h.repo.rows["REF-BAD"].record.status is S.FAILED
    assert h.repo.rows["REF-BAD"].record.error_reason == reason
    assert h.api.calls == []


# ---------------------------------------------------------------- sistema


def test_system_retries_exhausted_go_to_dlq_as_abandoned(h: Harness) -> None:
    h.api.scripted[7] = [http(503)] * 9  # 3 intentos x (1 + MaxRetryNumber=2) entregas
    h.queue.push(make_tx("REF-503", 7))
    h.drain_with_retries()

    with capture_logs() as logs:
        h.run()

    assert h.api.calls == [7] * 9
    assert [(r.status, r.retry_count) for r in h.repo.history] == [
        (S.RETRIED, 1),
        (S.RETRIED, 2),
        (S.ABANDONED, 2),
    ]
    (entry,) = h.queue.dlq
    assert (entry.transaction.reference, entry.transaction.status, entry.reason) == (
        "REF-503",
        S.ABANDONED,
        "server_error",
    )
    assert entry.last_error == "HTTP 503"
    assert not h.queue.pending
    assert h.sleeper.sleeps[:2] == [0.5, 1.0]  # backoff con jitter determinista (techo)
    assert h.metrics.retries.count(("in_item", "server_error")) == 6
    assert h.metrics.retries.count(("item", "server_error")) == 2
    assert transitions(logs, "REF-503")[-1] == ("InProgress", "Abandoned")


def test_item_retry_ready_at_uses_exponential_delay(h: Harness) -> None:
    h.api.scripted[7] = [http(500)] * 3
    h.queue.push(make_tx("REF-R", 7))
    h.stop_when_idle()
    start = h.clock.now()
    h.run()
    ((tx, ready_at),) = h.queue.retry_zset.values()
    assert tx.retry_count == 1
    in_item_waits = 0.5 + 1.0
    assert ready_at == pytest.approx(start + in_item_waits + 30)


@pytest.mark.parametrize(
    ("status", "alert"),
    [(401, AlertKind.AUTH), (403, AlertKind.AUTH), (400, AlertKind.CONTRACT)],
)
def test_non_retryable_is_abandoned_with_alert_and_counts_for_breaker(
    h: Harness, status: int, alert: AlertKind
) -> None:
    h.api.scripted[3] = [http(status)]
    h.queue.push(make_tx("REF-NR", 3))
    h.stop_when_idle()
    with capture_logs() as logs:
        h.run()
    assert h.api.calls == [3]
    assert h.repo.rows["REF-NR"].record.status is S.ABANDONED  # nunca Failed
    assert len(h.queue.dlq) == 1
    assert h.metrics.alerts == [(alert, policies.classify_http_status(status).reason)]  # type: ignore[union-attr]
    assert any(
        e["event"] == "transaction.requires_attention" and e["alert"] == str(alert) for e in logs
    )
    assert h.runner.breaker.consecutive_failures == 1


@pytest.mark.parametrize("status", [409, 422])
def test_other_4xx_are_contract_alerts(h: Harness, status: int) -> None:
    h.api.scripted[3] = [http(status)]
    h.queue.push(make_tx("REF-4XX", 3))
    h.stop_when_idle()
    h.run()
    assert h.repo.rows["REF-4XX"].record.status is S.ABANDONED
    assert h.metrics.alerts == [(AlertKind.CONTRACT, "unexpected_status")]
    assert h.runner.breaker.consecutive_failures == 1


def test_invalid_2xx_body_is_contract_alert(h: Harness) -> None:
    h.api.scripted[3] = [["not", "a", "post"]]
    h.queue.push(make_tx("REF-SHAPE", 3))
    h.stop_when_idle()
    h.run()
    assert h.repo.rows["REF-SHAPE"].record.status is S.ABANDONED
    assert h.metrics.alerts == [(AlertKind.CONTRACT, "invalid_response")]
    assert h.runner.breaker.consecutive_failures == 1


def test_rejected_row_is_abandoned_with_contract_alert(h: Harness) -> None:
    h.repo.reject_next = True
    h.queue.push(make_tx("REF-REJ", 1))
    h.stop_when_idle()
    h.run()
    (entry,) = h.queue.dlq
    assert entry.reason == "database_rejected"
    assert entry.last_error == "value too long"
    assert h.repo.rows["REF-REJ"].record.status is S.ABANDONED
    assert h.metrics.alerts == [(AlertKind.CONTRACT, "database_rejected")]
    assert h.runner.breaker.consecutive_failures == 1


# ------------------------------------------------------------ Retry-After


def test_short_retry_after_is_honored_inside_the_item(h: Harness) -> None:
    h.api.scripted[4] = [http(429, retry_after=3)]
    h.queue.push(make_tx("REF-429", 4))
    h.stop_when_idle()
    h.run()
    assert h.sleeper.sleeps == [3]
    assert h.repo.rows["REF-429"].record.status is S.SUCCESSFUL


def test_long_retry_after_requeues_consumes_retry_but_not_breaker(h: Harness) -> None:
    h.api.scripted[4] = [http(429, retry_after=120)]
    h.queue.push(make_tx("REF-429L", 4))
    h.stop_when_idle()
    start = h.clock.now()
    h.run()

    (tx, ready_at), *_ = h.queue.scheduled
    assert tx.retry_count == 1  # SÍ consume MaxRetryNumber
    assert ready_at == start + 120
    assert h.sleeper.sleeps
    assert max(h.sleeper.sleeps) <= 5
    assert h.runner.breaker.consecutive_failures == 0  # NO cuenta para el breaker
    # Tras la pausa vence el reintento y el item termina bien.
    assert h.repo.rows["REF-429L"].record.status is S.SUCCESSFUL


def test_final_429_pauses_taking_items_until_retry_after(h: Harness) -> None:
    h.api.scripted[4] = [http(429, retry_after=120)]
    h.queue.push(make_tx("REF-429P", 4))
    h.queue.push(make_tx("REF-NEXT", 5))
    taken_at: dict[str, float] = {}

    def on_call(post_id: int) -> None:
        taken_at.setdefault(str(post_id), h.clock.now())  # primera llamada de cada id

    h.api.on_call = on_call
    states: list[WorkerState] = []
    h.sleeper.on_sleep = lambda: states.append(h.health.snapshot().state)
    h.stop_when_idle()
    h.run()

    assert taken_at["5"] - taken_at["4"] >= 120  # no tomó el siguiente durante la pausa
    assert WorkerState.DEGRADED in states
    assert h.runner.breaker.consecutive_failures == 0


def test_rate_limit_pause_is_interruptible_by_should_stop(h: Harness) -> None:
    h.api.scripted[4] = [http(429, retry_after=3_000)]
    h.queue.push(make_tx("REF-429S", 4))
    h.queue.push(make_tx("REF-NEXT", 5))
    h.sleeper.on_sleep = lambda: h.should_stop.request("signal:15")
    start = h.clock.monotonic()
    assert h.run() == 0
    assert h.clock.monotonic() - start < 10
    assert h.queue.fetched == ["REF-429S"]


def test_retry_after_is_capped_by_absolute_max_delay(h: Harness) -> None:
    h.api.scripted[4] = [http(429, retry_after=999_999)]
    h.queue.push(make_tx("REF-CAP", 4))
    h.stop_when_idle()
    start = h.clock.now()
    h.run()
    (_, ready_at), *_ = h.queue.scheduled
    assert ready_at == start + h.processing.max_retry_delay_seconds
    # La pausa de toma también queda topada (3600 s), no 999 999 s.
    assert h.clock.monotonic() <= h.runner_settings.max_rate_limit_pause_seconds + 60


def test_rate_limit_exhausting_max_retry_is_abandoned_but_neutral(h: Harness) -> None:
    h.api.scripted[4] = [http(429, retry_after=120)]
    h.queue.push(make_tx("REF-429X", 4, retry_count=2))
    h.stop_when_idle()
    h.run()
    assert h.repo.rows["REF-429X"].record.status is S.ABANDONED
    assert h.queue.dlq[0].reason == "rate_limited"
    assert h.runner.breaker.consecutive_failures == 0


# ----------------------------------------------------------- idempotencia


def test_double_delivery_does_not_write_twice_and_is_acked(h: Harness) -> None:
    h.queue.push(make_tx("REF-DUP", 9))
    second = h.queue.push(make_tx("REF-DUP", 9))
    h.stop_when_idle()
    h.run()

    assert h.api.calls == [9]  # la segunda entrega no llama a la API
    assert h.repo.rows["REF-DUP"].result_writes == 1
    assert len(h.repo.history) == 1
    assert second.message_id in h.queue.acked


def test_retried_then_successful_writes_result_once_and_redelivery_is_noop(h: Harness) -> None:
    h.api.scripted[5] = [http(503)] * 3  # primera entrega: Retried
    h.queue.push(make_tx("REF-RS", 5))
    h.drain_with_retries()
    h.run()

    row = h.repo.rows["REF-RS"]
    assert [r.status for r in h.repo.history] == [S.RETRIED, S.SUCCESSFUL]
    assert row.record.status is S.SUCCESSFUL
    assert row.record.retry_count == 1
    assert row.result_writes == 1

    h.restart()  # doble entrega posterior del mismo item
    late = h.queue.push(make_tx("REF-RS", 5, retry_count=1))
    h.stop_when_idle()
    calls_before = len(h.api.calls)
    h.run()
    assert len(h.api.calls) == calls_before
    assert h.repo.rows["REF-RS"].result_writes == 1
    assert len(h.repo.history) == 2
    assert late.message_id in h.queue.acked


def test_crash_between_postgres_and_ack_redelivers_without_duplicate(h: Harness) -> None:
    h.queue.push(make_tx("REF-CRASH", 11))
    h.queue.fail_next_redis_op = True  # Postgres OK, XACK falla
    h.reclaim_until_drained()
    h.run()

    assert h.api.calls == [11]  # el reproceso detecta el terminal y solo acusa
    assert h.repo.rows["REF-CRASH"].result_writes == 1
    assert not h.queue.pending


def test_crash_between_postgres_and_retry_reproduces_same_decision(h: Harness) -> None:
    h.api.scripted[12] = [http(503)] * 6
    h.queue.push(make_tx("REF-RC", 12))
    h.queue.fail_next_redis_op = True  # Postgres Retried OK, ZADD+XACK falla
    h.reclaim_until_drained()
    h.run()

    # Ambas pasadas producen exactamente la misma fila (retry_count del mensaje + 1).
    assert [(r.status, r.retry_count) for r in h.repo.history] == [
        (S.RETRIED, 1),
        (S.RETRIED, 1),
    ]
    ((tx, _),) = h.queue.retry_zset.values()
    assert tx.retry_count == 1


def test_transient_postgres_failure_releases_item_untouched(h: Harness) -> None:
    h.repo.fail_record = 1
    h.queue.push(make_tx("REF-PG", 13))
    h.drain_with_retries(step_seconds=10)
    h.run()
    assert h.repo.rows["REF-PG"].record.status is S.SUCCESSFUL
    assert h.repo.rows["REF-PG"].record.retry_count == 0  # liberar no consume reintentos
    assert h.api.calls == [13, 13]
    assert ("release", "database_unavailable") in h.metrics.retries


def test_long_postgres_outage_sends_nothing_to_dlq(h: Harness) -> None:
    """Postgres caído durante muchos leases (> max_deliveries x 120 s) con varios items."""
    for i in range(1, 6):
        h.queue.push(make_tx(f"REF-{i}", i))
    h.repo.down = True
    outage_end = h.clock.monotonic() + 30 * 60  # 30 min >> 5 x 120 s

    fetches_while_open: list[int] = []

    def on_sleep() -> None:
        if h.runner.breaker.state is CircuitState.OPEN:
            fetches_while_open.append(h.queue.fetch_calls)
        if h.clock.monotonic() >= outage_end:
            h.repo.down = False

    h.sleeper.on_sleep = on_sleep
    h.drain_with_retries(step_seconds=10)
    h.run()

    assert h.queue.dlq == []
    assert all(h.repo.rows[f"REF-{i}"].record.status is S.SUCCESSFUL for i in range(1, 6))
    assert all(r.record.retry_count == 0 for r in h.repo.rows.values())
    # Con el breaker abierto no se lee ni se reclama: solo la sonda tras cada cooldown.
    cooldowns = h.clock.monotonic() // h.runner_settings.circuit_cooldown_seconds + 1
    assert h.queue.fetch_calls <= 3 + cooldowns + 10
    assert len(set(fetches_while_open)) <= cooldowns
    assert h.runner.breaker.state is CircuitState.CLOSED


def test_poison_message_goes_to_dlq_even_if_postgres_is_down(h: Harness) -> None:
    d = h.queue.push(make_tx("REF-POISON", 14))
    h.queue.note_failure(d, "RuntimeError: boom")
    h.queue.streams["normal"][0] = replace(d, delivery_count=h.processing.max_deliveries + 1)
    h.repo.down = True
    h.stop_when_idle()
    h.run()
    (entry,) = h.queue.dlq
    assert entry.reason == "max_deliveries"
    assert entry.last_error == "RuntimeError: boom"
    assert h.metrics.alerts == [(AlertKind.POISON, "max_deliveries")]
    assert h.api.calls == []
    assert h.runner.breaker.consecutive_failures == 0  # el problema es el item


def test_unexpected_bug_is_noted_and_left_pending(h: Harness) -> None:
    h.api.scripted[1] = [RuntimeError("bug")]
    h.queue.push(make_tx("REF-BUG", 1))
    h.queue.push(make_tx("REF-FINE", 2))
    h.stop_when_idle()
    assert h.run() == 0
    assert "REF-BUG" not in h.repo.rows
    assert h.repo.rows["REF-FINE"].record.status is S.SUCCESSFUL
    assert len(h.queue.pending) == 1  # queda pendiente para reclamo
    assert h.queue.errors["REF-BUG"] == "RuntimeError: bug"


def test_repeated_bug_ends_in_dlq_with_last_error(h: Harness) -> None:
    h.api.scripted[1] = [RuntimeError("bug")] * 10
    h.queue.push(make_tx("REF-LOOP", 1))
    h.reclaim_until_drained()
    h.run()
    (entry,) = h.queue.dlq
    assert (entry.reason, entry.last_error) == ("max_deliveries", "RuntimeError: bug")
    assert len(h.api.calls) == h.processing.max_deliveries


# ------------------------------------------------------------- ShouldStop


@pytest.fixture
def restore_signals() -> Iterator[None]:
    saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    yield
    for s, handler in saved.items():
        signal.signal(s, handler)


@pytest.mark.usefixtures("restore_signals")
def test_signal_mid_item_finishes_item_and_takes_no_other(h: Harness) -> None:
    install_signal_handlers(h.should_stop)
    first = h.queue.push(make_tx("REF-1", 1))
    h.queue.push(make_tx("REF-2", 2))
    h.api.on_call = lambda _id: signal.raise_signal(signal.SIGINT)  # llega a mitad del item

    assert h.run() == 0

    assert h.repo.rows["REF-1"].record.status is S.SUCCESSFUL  # terminó y se persistió
    assert first.message_id in h.queue.acked  # ...y luego se acusó
    assert h.queue.fetched == ["REF-1"]  # no tomó otro
    assert [d.transaction.reference for d in h.queue.streams["normal"]] == ["REF-2"]
    assert h.should_stop.origin == f"signal:{int(signal.SIGINT)}"
    assert h.closed == ["closed"]


def test_redis_key_mid_item_finishes_item_and_takes_no_other(h: Harness) -> None:
    h.queue.push(make_tx("REF-1", 1))
    h.queue.push(make_tx("REF-2", 2))

    def operator_sets_key(_id: int) -> None:
        h.remote.requested = True

    h.api.on_call = operator_sets_key
    assert h.run() == 0
    assert h.repo.rows["REF-1"].record.status is S.SUCCESSFUL
    assert h.queue.fetched == ["REF-1"]
    assert h.should_stop.origin == "redis_key"


def test_should_stop_during_backoff_requeues_short_without_consuming_retry(h: Harness) -> None:
    h.api.scripted[6] = [http(503)]
    first = h.queue.push(make_tx("REF-BO", 6, retry_count=1))
    h.queue.push(make_tx("REF-NEXT", 7))
    h.sleeper.on_sleep = lambda: h.should_stop.request("signal:15")
    start = h.clock.now()

    assert h.run() == 0

    row = h.repo.rows["REF-BO"].record
    assert row.status is S.RETRIED  # ni Abandoned ni perdido
    assert row.retry_count == 1  # no consume MaxRetryNumber
    assert row.error_reason == "shutdown_during_backoff"
    ((tx, ready_at),) = h.queue.retry_zset.values()
    assert tx.retry_count == 1
    assert ready_at == start + h.processing.release_delay_seconds
    assert first.message_id in h.queue.acked
    assert not h.queue.dlq
    assert h.queue.fetched == ["REF-BO"]
    assert h.runner.breaker.consecutive_failures == 0


def test_stop_during_blocking_read_returns_item_intact_to_sorted_set(h: Harness) -> None:
    original = make_tx("REF-LATE", 1, priority="high", retry_count=1, traceparent=TRACEPARENT)
    d = h.queue.push(original)
    h.queue.on_fetch = lambda: h.should_stop.request("signal:15")
    h.run()
    assert h.api.calls == []
    assert d.message_id in h.queue.acked  # XACK + ZADD
    ((tx, ready_at),) = h.queue.retry_zset.values()
    assert tx == original  # retry_count, correlation_id, traceparent y priority intactos
    assert ready_at == h.clock.now()
    assert "REF-LATE" not in h.repo.rows


# -------------------------------------------------------- circuit breaker


def test_breaker_opens_degrades_cools_down_and_recovers_without_exiting(h: Harness) -> None:
    for i in (1, 2, 3):
        h.api.scripted[i] = [http(503)] * 3
        h.queue.push(make_tx(f"REF-F{i}", i))
    h.queue.push(make_tx("REF-OK", 4))
    states_while_waiting: list[WorkerState] = []
    h.sleeper.on_sleep = lambda: states_while_waiting.append(h.health.snapshot().state)
    h.stop_when_idle()

    assert h.run() == 0

    assert WorkerState.DEGRADED in states_while_waiting
    assert h.metrics.circuit == [
        CircuitState.CLOSED,
        CircuitState.OPEN,
        CircuitState.HALF_OPEN,
        CircuitState.CLOSED,
    ]
    assert h.repo.rows["REF-OK"].record.status is S.SUCCESSFUL  # el item de prueba
    assert h.runner.breaker.state is CircuitState.CLOSED


def test_queue_outage_opens_breaker_and_worker_keeps_running(h: Harness) -> None:
    h.queue.fail_fetch = 3
    h.queue.push(make_tx("REF-AFTER", 1))
    h.stop_when_idle()
    assert h.run() == 0
    assert CircuitState.OPEN in h.metrics.circuit
    assert h.repo.rows["REF-AFTER"].record.status is S.SUCCESSFUL
