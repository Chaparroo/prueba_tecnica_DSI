"""Contenedores reales de Redis y Postgres (testcontainers).

Se omiten si Docker no está disponible. Las migraciones se aplican con
Alembic igual que en un despliegue, así que también quedan probadas.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import cast

import psycopg
import pytest
import redis
from psycopg_pool import ConnectionPool

from rpa_worker.adapters.postgres_repository import PostgresRepository, create_pool
from rpa_worker.adapters.redis_queue import QueueKeys, RedisQueue

ROOT = Path(__file__).resolve().parents[2]
REDIS_IMAGE = "redis:7.4-alpine"
POSTGRES_IMAGE = "postgres:17-alpine"
PRIORITIES = ("high", "normal", "low")


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:  # noqa: BLE001 - cualquier fallo = sin Docker
        return False


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in str(item.path):
            item.add_marker(pytest.mark.integration)
    if not _docker_available():
        skip = pytest.mark.skip(reason="Docker no disponible")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    from testcontainers.community.redis import RedisContainer

    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture(scope="session")
def postgres_conninfo() -> Iterator[str]:
    from alembic import command
    from alembic.config import Config
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver=None) as container:
        url = container.get_connection_url()  # postgresql://user:pw@host:port/db
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.attributes["url"] = url.replace("postgresql://", "postgresql+psycopg://", 1)
        cfg.attributes["configure_logger"] = False
        command.upgrade(cfg, "head")
        yield url


@pytest.fixture
def redis_client(redis_url: str) -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=10)
    client.flushdb()  # solo en pruebas: base aislada en un contenedor efímero
    yield client
    client.close()


@pytest.fixture
def keys() -> QueueKeys:
    return QueueKeys.build(
        stream_prefix="rpa:queue",
        priorities=PRIORITIES,
        retry_zset="rpa:retry",
        dlq="rpa:dlq",
        should_stop="control:should_stop",
    )


def make_queue(
    client: redis.Redis,
    keys: QueueKeys,
    consumer: str = "worker-a",
    *,
    lease_ms: int = 120_000,
    block_ms: int = 100,
) -> RedisQueue:
    return RedisQueue(
        client,
        keys,
        group="rpa-workers",
        consumer=consumer,
        block_ms=block_ms,
        lease_ms=lease_ms,
        claim_interval_ms=100,
    )


def xrange(client: redis.Redis, key: str) -> list[tuple[str, dict[str, str]]]:
    """XRANGE tipado (el cliente usa decode_responses=True)."""
    return cast(list[tuple[str, dict[str, str]]], client.xrange(key) or [])


def zrange_with_scores(client: redis.Redis, key: str) -> list[tuple[str, float]]:
    return cast(list[tuple[str, float]], client.zrange(key, 0, -1, withscores=True))


@pytest.fixture
def queue(redis_client: redis.Redis, keys: QueueKeys) -> RedisQueue:
    q = make_queue(redis_client, keys)
    q.prepare()
    return q


@pytest.fixture
def pool(postgres_conninfo: str) -> Iterator[ConnectionPool]:
    with psycopg.connect(postgres_conninfo, autocommit=True) as conn:
        conn.execute("TRUNCATE transaction_results, requeue_audit")
    p = create_pool(
        postgres_conninfo, min_size=1, max_size=4, connect_timeout=5, statement_timeout_ms=5_000
    )
    p.open(wait=True)
    yield p
    p.close()


@pytest.fixture
def repo(pool: ConnectionPool) -> PostgresRepository:
    return PostgresRepository(pool)
