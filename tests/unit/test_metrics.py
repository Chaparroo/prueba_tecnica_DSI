"""OtelMetrics con un MeterProvider en memoria: nombres, labels y series en 0."""

from __future__ import annotations

from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from rpa_worker.adapters.observability import ALERT_CATEGORIES, OtelMetrics
from rpa_worker.application.circuit_breaker import CircuitState
from rpa_worker.domain.exceptions import AlertKind
from rpa_worker.domain.transaction import TransactionStatus

Point = tuple[dict[str, Any], Any]


def collect(reader: InMemoryMetricReader) -> dict[str, list[Point]]:
    data = reader.get_metrics_data()
    assert data is not None
    out: dict[str, list[Point]] = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                out[m.name] = [
                    (dict(p.attributes or {}), getattr(p, "value", getattr(p, "count", None)))
                    for p in m.data.data_points
                ]
    return out


def test_alert_series_start_at_zero_and_use_fixed_categories() -> None:
    reader = InMemoryMetricReader()
    m = OtelMetrics(MeterProvider(metric_readers=[reader]).get_meter("t"))
    initial = {attrs["category"]: v for attrs, v in collect(reader)["rpa.alerts"]}
    assert initial == dict.fromkeys(ALERT_CATEGORIES, 0)

    m.alert(AlertKind.POISON, "max_deliveries")
    m.alert(AlertKind.POISON, "unparseable")
    m.alert(AlertKind.AUTH, "unauthorized")
    after = {attrs["category"]: v for attrs, v in collect(reader)["rpa.alerts"]}
    assert after == {"auth": 1, "contract": 0, "poison": 1, "max_deliveries": 1, "max_age": 0}


def test_labels_never_include_reference_or_correlation_id() -> None:
    reader = InMemoryMetricReader()
    m = OtelMetrics(MeterProvider(metric_readers=[reader]).get_meter("t"))
    m.transaction_finished(TransactionStatus.SUCCESSFUL, "normal", 0.2)
    m.retry("in_item", "server_error")
    m.circuit_state(CircuitState.OPEN)
    for name, points in collect(reader).items():
        for attrs, _v in points:
            assert not {"reference", "correlation_id"} & set(attrs), name
    assert collect(reader)["rpa.circuit.state"] == [({}, 2)]
