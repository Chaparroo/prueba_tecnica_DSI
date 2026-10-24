"""Requeue y runner de punta a punta con Redis y Postgres reales (API fake)."""

from __future__ import annotations

import threading

import psycopg
import pytest
import redis

from rpa_worker.adapters import codec
from rpa_worker.adapters.postgres_repository import PostgresRepository
from rpa_worker.adapters.redis_queue import QueueKeys, RedisQueue
from rpa_worker.application.clock import SystemClock
from rpa_worker.application.health import HealthState
from rpa_worker.application.process_transaction import ProcessTransaction
from rpa_worker.application.requeue import RequeueFromDlq, RequeueStatus
from rpa_worker.application.runner import Runner
from rpa_worker.application.settings import ProcessingSettings, RunnerSettings
from rpa_worker.application.should_stop import InterruptibleSleeper, ShouldStop
from rpa_worker.domain import policies
from rpa_worker.domain.exceptions import SystemException
from tests.fakes import FakeApi, FakeMetrics, make_tx
from tests.integration.conftest import make_queue, xrange


def _dead_letter(queue: RedisQueue, reference: str) -> None:
    queue.publish(make_tx(reference, 7))
    d = queue.fetch_next()
    assert d is not None
    queue.dead_letter(d, d.transaction, "server_error", "HTTP 503")


def _requeue(
    redis_url: str, keys: QueueKeys, repo: PostgresRepository, dlq: RedisQueue | None = None
) -> RequeueFromDlq:
    if dlq is None:
        dlq = make_queue(redis.Redis.from_url(redis_url, decode_responses=True), keys, "cli")
    return RequeueFromDlq(dlq=dlq, repository=repo, clock=SystemClock())


def test_concurrent_requeue_enqueues_exactly_one_item(
    redis_url: str,
    keys: QueueKeys,
    queue: RedisQueue,
    repo: PostgresRepository,
    redis_client: redis.Redis,
) -> None:
    _dead_letter(queue, "REF-CC")
    results: list[RequeueStatus] = []
    barrier = threading.Barrier(2)

    def operator(name: str) -> None:
        use_case = _requeue(redis_url, keys, repo)
        barrier.wait()
        results.append(use_case("REF-CC", operator=name, reason="dos a la vez").status)

    threads = [threading.Thread(target=operator, args=(n,)) for n in ("ana", "luis")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == [RequeueStatus.ALREADY_REQUEUED, RequeueStatus.REQUEUED]
    assert redis_client.xlen("rpa:queue:normal") == 2  # el original (acusado) + 1 reencolado
    assert redis_client.xlen("rpa:dlq") == 0
    requeued = [
        tx
        for _id, f in xrange(redis_client, "rpa:queue:normal")
        if (tx := codec.decode(f["data"])).requeue_id is not None
    ]
    assert len(requeued) == 1
    assert (requeued[0].retry_count, requeued[0].requeue_count) == (0, 1)
    assert requeued[0].correlation_id == "corr-REF-CC"


def test_crash_between_postgres_and_redis_then_rerun(
    redis_url: str,
    keys: QueueKeys,
    queue: RedisQueue,
    repo: PostgresRepository,
    redis_client: redis.Redis,
    postgres_conninfo: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _dead_letter(queue, "REF-CR")
    dlq = make_queue(redis.Redis.from_url(redis_url, decode_responses=True), keys, "cli")
    use_case = _requeue(redis_url, keys, repo, dlq)

    class Crash(Exception):
        pass

    def crash(*_a: object) -> bool:
        raise Crash  # el proceso muere tras el paso 1 (Postgres)

    with monkeypatch.context() as m:
        m.setattr(dlq, "restore", crash)
        with pytest.raises(Crash):
            use_case("REF-CR", operator="ana", reason="r")
    assert redis_client.xlen("rpa:dlq") == 1  # la entrada sigue

    result = use_case("REF-CR", operator="ana", reason="r")  # re-ejecución
    assert result.status is RequeueStatus.REQUEUED
    assert result.requeue_count == 1
    assert redis_client.xlen("rpa:dlq") == 0
    with psycopg.connect(postgres_conninfo) as conn:
        assert conn.execute("SELECT count(*) FROM requeue_audit").fetchone() == (1,)
        assert conn.execute(
            "SELECT status, requeue_count FROM transaction_results WHERE reference='REF-CR'"
        ).fetchone() == ("Retried", 1)


def _runner(
    client: redis.Redis, keys: QueueKeys, repo: PostgresRepository, api: FakeApi, stop: ShouldStop
) -> Runner:
    queue = make_queue(client, keys, "worker-e2e")
    clock = SystemClock()
    health = HealthState(clock)
    sleeper = InterruptibleSleeper(stop)
    metrics = FakeMetrics()
    process = ProcessTransaction(
        api=api,
        repository=repo,
        queue=queue,
        clock=clock,
        sleeper=sleeper,
        metrics=metrics,
        settings=ProcessingSettings(backoff_base_seconds=0.01),
        heartbeat=health.beat,
    )
    return Runner(
        queue=queue,
        remote_stop=queue,
        process=process,
        should_stop=stop,
        health=health,
        clock=clock,
        sleeper=sleeper,
        metrics=metrics,
        settings=RunnerSettings(),
    )


def test_redis_stop_key_mid_item_persists_it_and_takes_no_other(
    redis_client: redis.Redis,
    keys: QueueKeys,
    queue: RedisQueue,
    repo: PostgresRepository,
    postgres_conninfo: str,
) -> None:
    queue.publish(make_tx("REF-E1", 1))
    queue.publish(make_tx("REF-E2", 2))
    api = FakeApi()

    operator = make_queue(redis_client, keys, "cli")

    def operator_sets_stop_key(_id: int) -> None:
        operator.request_stop(ttl_seconds=60)  # `rpa-worker stop`, a mitad del item

    api.on_call = operator_sets_stop_key

    assert _runner(redis_client, keys, repo, api, ShouldStop()).run() == 0

    assert api.calls == [1]
    with psycopg.connect(postgres_conninfo) as conn:
        rows = conn.execute("SELECT reference, status FROM transaction_results").fetchall()
    assert rows == [("REF-E1", "Successful")]
    assert redis_client.xpending("rpa:queue:normal", "rpa-workers")["pending"] == 0
    groups = redis_client.xinfo_groups("rpa:queue:normal")
    assert groups[0]["lag"] == 1  # REF-E2 sigue sin entregar


def test_abandoned_requeue_then_successful_end_to_end(
    redis_url: str,
    redis_client: redis.Redis,
    keys: QueueKeys,
    queue: RedisQueue,
    repo: PostgresRepository,
    postgres_conninfo: str,
) -> None:
    err = policies.classify_http_status(401)
    assert isinstance(err, SystemException)
    api = FakeApi(scripted={9: [err]})
    queue.publish(make_tx("REF-AB", 9))

    stop = ShouldStop()
    threading.Timer(1.5, stop.request, args=("test",)).start()  # cola ya vacía para entonces
    _runner(redis_client, keys, repo, api, stop).run()

    with psycopg.connect(postgres_conninfo) as conn:
        assert conn.execute(
            "SELECT status FROM transaction_results WHERE reference='REF-AB'"
        ).fetchone() == ("Abandoned",)

    result = _requeue(redis_url, keys, repo)("REF-AB", operator="ana", reason="token rotado")
    assert result.status is RequeueStatus.REQUEUED

    stop2 = ShouldStop()
    threading.Timer(1.5, stop2.request, args=("test",)).start()
    _runner(redis_client, keys, repo, api, stop2).run()

    with psycopg.connect(postgres_conninfo) as conn:
        status, requeue_count, result_json = conn.execute(
            "SELECT status, requeue_count, result FROM transaction_results WHERE reference='REF-AB'"
        ).fetchone()  # type: ignore[misc]
    assert (status, requeue_count) == ("Successful", 1)
    assert result_json["id"] == 9
