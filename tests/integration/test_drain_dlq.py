"""drain-dlq contra Redis y Postgres reales: concurrencia y crash."""

from __future__ import annotations

import threading

import pytest
import redis

from rpa_worker.adapters import codec
from rpa_worker.adapters.postgres_repository import PostgresRepository
from rpa_worker.adapters.redis_queue import QueueKeys, RedisQueue
from rpa_worker.application.clock import SystemClock
from rpa_worker.application.drain_dlq import DrainDlq, DrainFilters, DrainOutcome
from rpa_worker.application.ports import DlqEntry
from rpa_worker.application.requeue import RequeueFromDlq
from rpa_worker.domain.transaction import Transaction
from tests.fakes import make_tx
from tests.integration.conftest import make_queue, xrange

N = 40


def fill_dlq(queue: RedisQueue) -> None:
    for i in range(N):
        queue.publish(make_tx(f"REF-{i:03d}", i + 1))
        d = queue.fetch_next()
        assert d is not None
        queue.dead_letter(d, d.transaction, "server_error", "HTTP 503")


def drainer(
    redis_url: str, keys: QueueKeys, repo: PostgresRepository
) -> tuple[DrainDlq, RedisQueue]:
    q = make_queue(redis.Redis.from_url(redis_url, decode_responses=True), keys, "cli")
    requeue = RequeueFromDlq(dlq=q, repository=repo, clock=SystemClock())
    return DrainDlq(dlq=q, requeue=requeue, sleep=lambda _s: False, page_size=7), q


def requeued_refs(client: redis.Redis) -> list[str]:
    return [
        tx.reference
        for _id, f in xrange(client, "rpa:queue:normal")
        if (tx := codec.decode(f["data"])).requeue_id is not None
    ]


def test_two_concurrent_drains_do_not_duplicate(
    redis_url: str,
    keys: QueueKeys,
    queue: RedisQueue,
    repo: PostgresRepository,
    redis_client: redis.Redis,
) -> None:
    fill_dlq(queue)
    reports = []
    barrier = threading.Barrier(2)

    def run(name: str) -> None:
        d, _q = drainer(redis_url, keys, repo)
        barrier.wait()
        reports.append(
            d.run(DrainFilters(), execute=True, operator=name, reason="concurrente", batch_size=5)
        )

    threads = [threading.Thread(target=run, args=(n,)) for n in ("ana", "luis")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    refs = requeued_refs(redis_client)
    assert sorted(refs) == [f"REF-{i:03d}" for i in range(N)]  # cada item exactamente una vez
    assert sum(r.requeued for r in reports) == N
    # El drain más lento no ve lo que el otro ya restauró (XRANGE ya no lo
    # devuelve) o lo cuenta como "ya desaparecido" si la puerta XDEL se lo gana.
    assert all(r.failed == {} for r in reports)
    assert redis_client.xlen("rpa:dlq") == 0


def test_crash_mid_drain_then_rerun_completes(
    redis_url: str,
    keys: QueueKeys,
    queue: RedisQueue,
    repo: PostgresRepository,
    redis_client: redis.Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fill_dlq(queue)
    d, q = drainer(redis_url, keys, repo)
    real_restore = q.restore
    calls = {"n": 0}

    class Crash(Exception):
        pass

    def crashing_restore(entry: DlqEntry, tx: Transaction) -> bool:
        calls["n"] += 1
        if calls["n"] == 17:
            raise Crash  # el proceso muere entre Postgres y Redis
        return real_restore(entry, tx)

    with monkeypatch.context() as m:
        m.setattr(q, "restore", crashing_restore)
        with pytest.raises(Crash):
            d.run(DrainFilters(), execute=True, operator="ana", reason="r")
    assert len(requeued_refs(redis_client)) == 16

    second, _ = drainer(redis_url, keys, repo)
    report = second.run(DrainFilters(), execute=True, operator="ana", reason="r")
    assert report.outcome is DrainOutcome.OK
    refs = requeued_refs(redis_client)
    assert sorted(refs) == [f"REF-{i:03d}" for i in range(N)]  # sin duplicar ni saltar
    assert redis_client.xlen("rpa:dlq") == 0
