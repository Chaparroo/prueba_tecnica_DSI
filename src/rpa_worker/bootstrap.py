"""Composition root: el único lugar que conoce adaptadores, SDK y aplicación."""

from __future__ import annotations

import os
import socket
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import redis
import structlog
from opentelemetry import propagate, trace
from psycopg_pool import ConnectionPool

from rpa_worker.adapters.health_server import HealthServer
from rpa_worker.adapters.http_api import JsonPlaceholderApi
from rpa_worker.adapters.observability import (
    OtelMetrics,
    QueueGauges,
    Telemetry,
    configure_logging,
    configure_telemetry,
    flush_logs,
)
from rpa_worker.adapters.postgres_repository import PostgresRepository, create_pool
from rpa_worker.adapters.redis_queue import (
    QueueKeys,
    RedisQueue,
    read_queue_stats,
    redis_cluster_enabled,
)
from rpa_worker.application.clock import SystemClock
from rpa_worker.application.drain_dlq import DrainDlq, DrainFilters, DrainReport
from rpa_worker.application.health import HealthState
from rpa_worker.application.process_transaction import ProcessTransaction
from rpa_worker.application.requeue import RequeueFromDlq, RequeueResult
from rpa_worker.application.runner import Runner
from rpa_worker.application.settings import ProcessingSettings, RunnerSettings
from rpa_worker.application.should_stop import (
    InterruptibleSleeper,
    ShouldStop,
    install_signal_handlers,
)
from rpa_worker.config import Settings
from rpa_worker.domain.exceptions import AlertKind
from rpa_worker.domain.transaction import Transaction

log = structlog.get_logger("rpa_worker.bootstrap")


# ------------------------------------------------------------------ piezas


def setup_observability(settings: Settings) -> Telemetry:
    obs = settings.config.observability
    configure_logging(
        level=obs.log_level,
        fmt=obs.log_format,
        pii_fields=obs.pii_fields,
        service_name=obs.service_name,
    )
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or (
        str(obs.otlp_endpoint) if obs.otlp_endpoint else None
    )
    return configure_telemetry(
        service_name=obs.service_name,
        otlp_endpoint=endpoint,
        metrics_interval_seconds=obs.metrics_export_interval_seconds,
        instance_id=consumer_name(settings),
    )


def queue_keys(settings: Settings) -> QueueKeys:
    q = settings.config.queue
    return QueueKeys.build(
        stream_prefix=q.stream_prefix,
        priorities=q.priorities,
        retry_zset=q.retry_zset_key,
        dlq=q.dlq_stream,
        should_stop=q.should_stop_key,
    )


def redis_client(settings: Settings, *, socket_timeout: float) -> redis.Redis:
    return redis.Redis.from_url(
        settings.secrets.redis_url.get_secret_value(),
        decode_responses=True,
        socket_timeout=socket_timeout,
        socket_connect_timeout=min(socket_timeout, 3.0),
        health_check_interval=30,
    )


def build_queue(
    settings: Settings,
    client: redis.Redis,
    consumer: str,
    on_unparseable: Callable[[], None] = lambda: None,
) -> RedisQueue:
    q = settings.config.queue
    return RedisQueue(
        client,
        queue_keys(settings),
        group=q.consumer_group,
        consumer=consumer,
        block_ms=q.block_ms,
        lease_ms=q.item_lease_ms,
        claim_interval_ms=q.claim_interval_ms,
        stale_consumer_ms=int(q.stale_consumer_hours * 3_600_000),
        on_unparseable=on_unparseable,
    )


def check_redis_mode(settings: Settings) -> bool | None:
    """True si Redis está en modo cluster (no compatible); None si no se pudo verificar."""
    client = redis_client(settings, socket_timeout=2.0)
    try:
        return redis_cluster_enabled(client)
    except redis.RedisError:
        return None
    finally:
        client.close()


def build_repository(settings: Settings) -> tuple[PostgresRepository, ConnectionPool]:
    pg = settings.config.postgres
    pool = create_pool(
        settings.secrets.postgres.conninfo(),
        min_size=pg.pool_min_size,
        max_size=pg.pool_max_size,
        connect_timeout=pg.connect_timeout_seconds,
        statement_timeout_ms=pg.statement_timeout_ms,
    )
    pool.open(wait=False)
    return PostgresRepository(pool), pool


def consumer_name(settings: Settings) -> str:
    """CONSUMER_NAME > worker.consumer_name > HOSTNAME. Estable y único por proceso:
    dos procesos con el mismo nombre se robarían los items en vuelo al arrancar."""
    return (
        os.environ.get("CONSUMER_NAME")
        or settings.config.worker.consumer_name
        or os.environ.get("HOSTNAME")
        or socket.gethostname()
    )


@dataclass(frozen=True, slots=True)
class _Deps:
    repo: PostgresRepository
    queue: RedisQueue


def _wait_for_dependencies(
    deps: _Deps, should_stop: ShouldStop, health: HealthState, sleeper: InterruptibleSleeper
) -> bool:
    """El worker espera a que Redis y Postgres estén sanos (interrumpible)."""
    attempt = 0
    while not should_stop.is_set():
        health.beat()
        redis_ok, pg_ok = deps.queue.ping(), deps.repo.ping()
        if redis_ok and pg_ok:
            return True
        if attempt % 5 == 0:
            log.warning("worker.waiting_for_dependencies", redis=redis_ok, postgres=pg_ok)
        attempt += 1
        sleeper.sleep(2.0)
    return False


# --------------------------------------------------------------- comandos


def run_worker(settings: Settings) -> int:
    cfg = settings.config
    telemetry = setup_observability(settings)
    should_stop = ShouldStop()
    install_signal_handlers(should_stop)  # SIGTERM/SIGINT solo activan la bandera
    clock = SystemClock()
    health = HealthState(clock)
    sleeper = InterruptibleSleeper(should_stop)
    consumer = consumer_name(settings)
    log.info(
        "worker.starting",
        consumer=consumer,
        redis=settings.secrets.redis_url_for_logs(),
        postgres_host=settings.secrets.postgres.host,
        worst_case_item_seconds=round(cfg.worst_case_item_seconds(), 1),
        stop_grace_seconds=cfg.worker.stop_grace_seconds,
    )

    # Cliente principal: el timeout debe superar el XREADGROUP BLOCK.
    client = redis_client(settings, socket_timeout=cfg.queue.block_ms / 1000 + 5)
    # Cliente para sondas y gauges: timeout corto, nunca bloquea nada importante.
    probe_client = redis_client(settings, socket_timeout=0.5)
    metrics = OtelMetrics()
    queue = build_queue(
        settings,
        client,
        consumer,
        on_unparseable=lambda: metrics.alert(AlertKind.POISON, "unparseable"),
    )
    repo, pool = build_repository(settings)
    api = JsonPlaceholderApi(
        base_url=str(cfg.http.base_url),
        connect_timeout=cfg.http.connect_timeout_seconds,
        read_timeout=cfg.http.read_timeout_seconds,
        user_agent=cfg.http.user_agent,
    )
    health_server = HealthServer(
        host=cfg.health.host,
        port=cfg.health.port,
        health=health,
        checks={"redis": lambda: bool(probe_client.ping()), "postgres": repo.ping},
        liveness_max_age_seconds=cfg.health.liveness_max_age_seconds,
    )
    health_server.start()

    keys = queue_keys(settings)
    QueueGauges(
        read_stats=lambda: read_queue_stats(
            probe_client, keys, cfg.queue.consumer_group, time.time()
        ),
        log=log.debug,
    ).register()

    closers = [
        api.close,
        pool.close,
        client.close,
        probe_client.close,
        health_server.stop,
        telemetry.shutdown,  # flush de trazas y métricas
        flush_logs,
    ]
    if not _wait_for_dependencies(_Deps(repo, queue), should_stop, health, sleeper):
        for close in closers:
            close()
        return 0
    if redis_cluster_enabled(probe_client):
        log.error("worker.redis_cluster_unsupported", hint="usa Redis standalone con AOF")
        for close in closers:
            close()
        return 2

    b = cfg.http.backoff
    process = ProcessTransaction(
        api=api,
        repository=repo,
        queue=queue,
        clock=clock,
        sleeper=sleeper,
        metrics=metrics,
        settings=ProcessingSettings(
            max_retry_number=cfg.worker.max_retry_number,
            max_attempts=b.max_attempts,
            backoff_base_seconds=b.base_seconds,
            backoff_max_seconds=b.max_seconds,
            max_retry_after_seconds=cfg.http.max_retry_after_seconds,
            item_retry_base_seconds=cfg.worker.item_retry_base_seconds,
            max_retry_delay_seconds=cfg.worker.max_retry_delay_seconds,
            release_delay_seconds=cfg.worker.release_delay_seconds,
            max_release_delay_seconds=cfg.worker.max_release_delay_seconds,
            max_item_age_seconds=cfg.worker.max_item_age_seconds,
            max_deliveries=cfg.worker.max_deliveries,
        ),
        heartbeat=health.beat,
    )
    runner = Runner(
        queue=queue,
        remote_stop=queue,
        process=process,
        should_stop=should_stop,
        health=health,
        clock=clock,
        sleeper=sleeper,
        metrics=metrics,
        settings=RunnerSettings(
            max_consecutive_system_exceptions=cfg.worker.max_consecutive_system_exceptions,
            circuit_cooldown_seconds=cfg.worker.circuit_cooldown_seconds,
            heartbeat_interval_seconds=min(5.0, cfg.health.liveness_max_age_seconds / 3),
            max_rate_limit_pause_seconds=cfg.worker.max_retry_delay_seconds,
        ),
        closers=closers,
    )
    return runner.run()


CLI_CONSUMER = "cli"  # los comandos de operación no leen del grupo


def requeue(settings: Settings, reference: str, *, operator: str, reason: str) -> RequeueResult:
    telemetry = setup_observability(settings)
    client = redis_client(settings, socket_timeout=5.0)
    repo, pool = build_repository(settings)
    try:
        use_case = RequeueFromDlq(
            dlq=build_queue(settings, client, CLI_CONSUMER),
            repository=repo,
            clock=SystemClock(),
        )
        return use_case(reference, operator=operator, reason=reason)
    finally:
        pool.close()
        client.close()
        telemetry.shutdown()
        flush_logs()


def drain_dlq(
    settings: Settings,
    filters: DrainFilters,
    *,
    execute: bool,
    operator: str,
    reason: str,
    limit: int | None,
    batch_size: int,
    pause_seconds: float,
    sample_size: int,
) -> DrainReport:
    telemetry = setup_observability(settings)
    client = redis_client(settings, socket_timeout=5.0)
    repo, pool = build_repository(settings)
    should_stop = ShouldStop()
    install_signal_handlers(should_stop)  # Ctrl+C termina el lote en curso y sale limpio
    try:
        queue = build_queue(settings, client, CLI_CONSUMER)
        use_case = DrainDlq(
            dlq=queue,
            requeue=RequeueFromDlq(dlq=queue, repository=repo, clock=SystemClock()),
            sleep=InterruptibleSleeper(should_stop).sleep,
        )
        return use_case.run(
            filters,
            execute=execute,
            operator=operator,
            reason=reason,
            limit=limit,
            batch_size=batch_size,
            pause_seconds=pause_seconds,
            sample_size=sample_size,
        )
    finally:
        pool.close()
        client.close()
        telemetry.shutdown()
        flush_logs()


def request_stop(settings: Settings, *, ttl_seconds: int | None, clear: bool) -> float | bool:
    """``rpa-worker stop``: crea la llave (valor = hora de Redis, TTL obligatorio) o la borra."""
    client = redis_client(settings, socket_timeout=5.0)
    try:
        queue = build_queue(settings, client, CLI_CONSUMER)
        if clear:
            return queue.clear_stop()
        if ttl_seconds is None:
            raise ValueError("--ttl es obligatorio")
        return queue.request_stop(ttl_seconds)
    finally:
        client.close()


def seed(settings: Settings, post_ids: Sequence[str], *, priority: str) -> list[Transaction]:
    """Encola items de prueba. Quien encola genera correlation_id y traceparent."""
    telemetry = setup_observability(settings)
    client = redis_client(settings, socket_timeout=5.0)
    queue = build_queue(settings, client, CLI_CONSUMER)
    tracer = trace.get_tracer("rpa_worker.seed")
    published: list[Transaction] = []
    try:
        queue.ensure_groups()
        for raw_id in post_ids:
            reference = f"post-{raw_id}-{uuid.uuid4().hex[:8]}"
            correlation_id = str(uuid.uuid4())
            with tracer.start_as_current_span(
                "enqueue",
                attributes={"rpa.priority": priority, "rpa.correlation_id": correlation_id},
            ):
                carrier: dict[str, str] = {}
                propagate.inject(carrier)
                payload: dict[str, object] = {"id": int(raw_id) if raw_id.isdigit() else raw_id}
                tx = Transaction(
                    reference=reference,
                    priority=priority,
                    payload=payload,
                    correlation_id=correlation_id,
                    traceparent=carrier.get("traceparent"),
                    first_enqueued_at=time.time(),
                )
                message_id = queue.publish(tx)
            log.info(
                "transaction.enqueued",
                reference=reference,
                correlation_id=correlation_id,
                priority=priority,
                message_id=message_id,
            )
            published.append(tx)
        return published
    finally:
        client.close()
        telemetry.shutdown()
        flush_logs()
