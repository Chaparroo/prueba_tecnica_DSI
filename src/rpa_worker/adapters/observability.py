"""Configuración de structlog y del SDK de OpenTelemetry.

Solo este módulo y el composition root importan el SDK; la aplicación usa
únicamente la API (``opentelemetry.trace``/``propagate``) y el puerto Metrics.

Logs: JSON a stdout (Alloy los recoge del runtime de Docker y los envía a
Loki). Cada línea lleva timestamp, level, trace_id/span_id del span activo y
el contexto ligado (reference, correlation_id) mediante contextvars. La PII
se redacta antes de renderizar.

Trazas y métricas: OTLP/HTTP hacia el collector (Alloy). Sin endpoint no se
exporta nada, pero las trazas siguen generando IDs para los logs.
"""

from __future__ import annotations

import logging
import re
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TextIO

import structlog
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from structlog.typing import EventDict, Processor, WrappedLogger

from rpa_worker.application.circuit_breaker import CircuitState
from rpa_worker.domain.exceptions import AlertKind
from rpa_worker.domain.transaction import TransactionStatus

REDACTED = "[REDACTED]"
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s:@]*:[^/\s@]*@")
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_LIBPQ_PASSWORD = re.compile(r"password\s*=\s*('(?:[^'\\]|\\.)*'|\S+)", re.IGNORECASE)


# ------------------------------------------------------------------ logs


class PiiRedactor:
    """Procesador structlog: redacta por nombre de llave (recursivo) y por patrón."""

    def __init__(self, fields: Iterable[str]) -> None:
        self._fields = {f.lower() for f in fields}

    def __call__(self, _logger: WrappedLogger, _name: str, event_dict: EventDict) -> EventDict:
        return {k: self._value(k, v) for k, v in event_dict.items()}

    def _value(self, key: str, value: Any) -> Any:
        if key.lower() in self._fields:
            return REDACTED
        if isinstance(value, Mapping):
            return {k: self._value(str(k), v) for k, v in value.items()}
        if isinstance(value, list | tuple):
            return [self._value(key, v) for v in value]
        if isinstance(value, str):
            return scrub(value)
        return value


def scrub(text: str) -> str:
    """Quita credenciales en URLs/cadenas libpq y correos de texto libre."""
    text = _URL_CREDENTIALS.sub(r"\g<scheme>***@", text)
    text = _LIBPQ_PASSWORD.sub("password=***", text)
    return _EMAIL.sub(REDACTED, text)


def add_trace_context(_logger: WrappedLogger, _name: str, event_dict: EventDict) -> EventDict:
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def configure_logging(
    *,
    level: str,
    fmt: str,
    pii_fields: Sequence[str],
    service_name: str,
    stream: TextIO | None = None,
) -> None:
    def add_service(_l: WrappedLogger, _n: str, event_dict: EventDict) -> EventDict:
        event_dict.setdefault("service", service_name)
        return event_dict

    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_service,
        add_trace_context,
    ]
    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,  # logs de librerías (httpx, psycopg...) con el mismo formato
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.dict_tracebacks,
            PiiRedactor(pii_fields),  # siempre el último antes de renderizar
            renderer,
        ],
    )
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for noisy in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))


def flush_logs() -> None:
    for handler in logging.getLogger().handlers:
        handler.flush()


# ---------------------------------------------------------- trazas/métricas


@dataclass
class Telemetry:
    tracer_provider: TracerProvider | None = None
    meter_provider: MeterProvider | None = None

    def shutdown(self) -> None:
        """Flush + cierre. Llamar en End Process."""
        if self.tracer_provider is not None:
            self.tracer_provider.force_flush(timeout_millis=5_000)
            self.tracer_provider.shutdown()
        if self.meter_provider is not None:
            self.meter_provider.force_flush(timeout_millis=5_000)
            self.meter_provider.shutdown()


def configure_telemetry(
    *,
    service_name: str,
    otlp_endpoint: str | None,
    metrics_interval_seconds: float,
    instance_id: str | None = None,
) -> Telemetry:
    # service.instance.id estable (nombre del consumer): si no, el SDK genera uno
    # aleatorio por arranque y cada reinicio crea series nuevas en Prometheus.
    attrs = {"service.name": service_name}
    if instance_id:
        attrs["service.instance.id"] = instance_id
    resource = Resource.create(attrs)
    # El TracerProvider del SDK se instala siempre: genera trace_id/span_id
    # reales para los logs y el traceparent aunque no haya collector.
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    if not otlp_endpoint:
        return Telemetry(tracer_provider)
    base = otlp_endpoint.rstrip("/")
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces", timeout=5))
    )
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{base}/v1/metrics", timeout=5),
        export_interval_millis=int(metrics_interval_seconds * 1000),
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)
    return Telemetry(tracer_provider, meter_provider)


_CIRCUIT_VALUE = {CircuitState.CLOSED: 0, CircuitState.HALF_OPEN: 1, CircuitState.OPEN: 2}

# Categorías de alerta = motivos de DLQ que alertan (ver observability/prometheus).
ALERT_CATEGORIES = ("auth", "contract", "poison", "max_deliveries", "max_age")


def alert_category(kind: AlertKind, reason: str) -> str:
    if kind is AlertKind.POISON and reason == "max_deliveries":
        return "max_deliveries"
    return str(kind)  # poison restante = mensajes ilegibles


class OtelMetrics:
    """Implementa el puerto Metrics. Labels de baja cardinalidad solamente."""

    def __init__(self, meter: metrics.Meter | None = None) -> None:
        m = meter or metrics.get_meter("rpa_worker")
        self._transactions = m.create_counter(
            "rpa.transactions", unit="{transaction}", description="Items procesados por resultado"
        )
        self._duration = m.create_histogram(
            "rpa.transaction.duration", unit="s", description="Duración de un item"
        )
        self._retries = m.create_counter(
            "rpa.retries", unit="{retry}", description="Reintentos por capa y motivo"
        )
        self._circuit = m.create_gauge(
            "rpa.circuit.state", description="0=closed 1=half_open 2=open"
        )
        self._alerts = m.create_counter(
            "rpa.alerts", unit="{alert}", description="Eventos que requieren atención humana"
        )
        # Series creadas en 0 al arrancar: increase() necesita una muestra previa,
        # si no, el PRIMER evento de una categoría nueva no dispararía la alerta.
        for category in ALERT_CATEGORIES:
            self._alerts.add(0, {"category": category})

    def transaction_finished(
        self, status: TransactionStatus, priority: str, duration_seconds: float
    ) -> None:
        attrs = {"status": str(status), "priority": priority}
        self._transactions.add(1, attrs)
        self._duration.record(duration_seconds, {"status": str(status)})

    def retry(self, layer: str, reason: str) -> None:
        self._retries.add(1, {"layer": layer, "reason": reason})

    def circuit_state(self, state: CircuitState) -> None:
        self._circuit.set(_CIRCUIT_VALUE[state])

    def alert(self, kind: AlertKind, reason: str) -> None:
        # El reason detallado va en los logs; aquí solo la categoría (cardinalidad fija).
        self._alerts.add(1, {"category": alert_category(kind, reason)})


@dataclass
class QueueGauges:
    """Gauges observables leídos de Redis en el hilo del exportador de métricas.

    ``read_stats`` debe usar un cliente con timeout corto: si Redis tarda, el
    callback no reporta (hueco en la serie) en vez de bloquear. Nunca corre en
    el loop del worker.
    """

    read_stats: Callable[[], Any]
    log: Callable[..., None] = field(default=lambda *a, **k: None)
    monotonic: Callable[[], float] = time.monotonic
    # Una recolección invoca los 4 callbacks seguidos: una sola lectura de Redis.
    cache_seconds: float = 1.0
    _cached: tuple[float, Any] | None = None

    def register(self, meter: metrics.Meter | None = None) -> None:
        m = meter or metrics.get_meter("rpa_worker")
        m.create_observable_gauge(
            "rpa.queue.oldest_item_age",
            [self._ages],
            unit="s",
            description="Edad del item más viejo sin terminar, por prioridad",
        )
        m.create_observable_gauge(
            "rpa.queue.depth",
            [self._depth],
            unit="{item}",
            description="Items en stream (sin entregar + pendientes)",
        )
        m.create_observable_gauge(
            "rpa.retry.scheduled",
            [self._retry],
            unit="{item}",
            description="Items esperando en el sorted set de reintentos",
        )
        m.create_observable_gauge(
            "rpa.dlq.length", [self._dlq], unit="{item}", description="Items en el DLQ"
        )
        # Constante 1 mientras el pipeline exporta: base de la alerta absent().
        m.create_observable_gauge("rpa.worker.up", [lambda _o: [Observation(1)]])

    def _stats(self) -> Any | None:
        now = self.monotonic()
        if self._cached is not None and now - self._cached[0] < self.cache_seconds:
            return self._cached[1]
        try:
            stats = self.read_stats()
        except Exception as exc:  # noqa: BLE001 - timeout o Redis caído: sin observación
            self.log("metrics.queue_stats_failed", error=type(exc).__name__)
            stats = None
        self._cached = (now, stats)
        return stats

    def _ages(self, _o: CallbackOptions) -> Iterable[Observation]:
        s = self._stats()
        return (
            []
            if s is None
            else [Observation(v, {"priority": p}) for p, v in s.oldest_age_seconds.items()]
        )

    def _depth(self, _o: CallbackOptions) -> Iterable[Observation]:
        s = self._stats()
        return [] if s is None else [Observation(v, {"priority": p}) for p, v in s.depth.items()]

    def _retry(self, _o: CallbackOptions) -> Iterable[Observation]:
        s = self._stats()
        return [] if s is None else [Observation(s.retry_scheduled)]

    def _dlq(self, _o: CallbackOptions) -> Iterable[Observation]:
        s = self._stats()
        return [] if s is None else [Observation(s.dlq_length)]
