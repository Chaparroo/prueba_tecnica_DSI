"""Adaptador Redis contra un Redis real."""

from __future__ import annotations

import threading
import time

import redis

from rpa_worker.adapters import codec
from rpa_worker.adapters.redis_queue import (
    QueueKeys,
    RedisQueue,
    read_queue_stats,
    redis_cluster_enabled,
)
from tests.fakes import make_tx
from tests.integration.conftest import make_queue, xrange, zrange_with_scores

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_priority_order_and_ack_empties_pel(queue: RedisQueue, redis_client: redis.Redis) -> None:
    queue.publish(make_tx("LOW", priority="low"))
    queue.publish(make_tx("HIGH", priority="high"))
    queue.publish(make_tx("NORMAL", priority="normal"))

    got = []
    while (d := queue.fetch_next()) is not None:
        got.append(d.transaction.reference)
        queue.ack(d)
    assert got == ["HIGH", "NORMAL", "LOW"]
    for stream in ("rpa:queue:high", "rpa:queue:normal", "rpa:queue:low"):
        assert redis_client.xpending(stream, "rpa-workers")["pending"] == 0


def test_schedule_retry_is_atomic_and_preserves_every_field(
    queue: RedisQueue, redis_client: redis.Redis
) -> None:
    original = make_tx("REF-R", 5, priority="high", retry_count=1, traceparent=TRACEPARENT)
    queue.publish(original)
    d = queue.fetch_next()
    assert d is not None
    queue.schedule_retry(d, d.transaction, ready_at=123.0)

    assert redis_client.xpending("rpa:queue:high", "rpa-workers")["pending"] == 0
    ((member, score),) = zrange_with_scores(redis_client, "rpa:retry")
    assert score == 123.0
    assert codec.decode(member) == original  # correlation_id, traceparent, priority intactos


def test_same_retry_twice_does_not_duplicate_member(
    queue: RedisQueue, redis_client: redis.Redis
) -> None:
    queue.publish(make_tx("REF-D", 1))
    d = queue.fetch_next()
    assert d is not None
    queue.schedule_retry(d, d.transaction, ready_at=10)
    queue.schedule_retry(d, d.transaction, ready_at=20)  # reproceso tras crash
    assert redis_client.zcard("rpa:retry") == 1


def test_promote_due_moves_to_its_priority_stream(
    queue: RedisQueue, redis_client: redis.Redis
) -> None:
    for ref, prio, ready in [("A", "low", 5), ("B", "high", 5), ("C", "normal", 999)]:
        redis_client.zadd("rpa:retry", {codec.encode(make_tx(ref, priority=prio)): ready})
    assert queue.promote_due(now=10) == 2
    assert redis_client.xlen("rpa:queue:low") == 1
    assert redis_client.xlen("rpa:queue:high") == 1
    assert redis_client.zcard("rpa:retry") == 1  # C aún no vence


def test_two_concurrent_promoters_lose_nothing_and_duplicate_nothing(
    redis_url: str, redis_client: redis.Redis, keys: QueueKeys
) -> None:
    total = 500
    redis_client.zadd(
        "rpa:retry",
        {
            codec.encode(make_tx(f"REF-{i}", i, priority=("high", "normal", "low")[i % 3])): i
            for i in range(total)
        },
    )
    start = threading.Barrier(2)
    moved: list[int] = []

    def promoter(name: str) -> None:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        q = RedisQueue(
            client,
            keys,
            group="rpa-workers",
            consumer=name,
            block_ms=100,
            lease_ms=120_000,
            claim_interval_ms=100,
            promote_batch=7,
        )
        start.wait()
        while (n := q.promote_due(now=10_000)) > 0:
            moved.append(n)
        client.close()

    threads = [threading.Thread(target=promoter, args=(n,)) for n in ("p1", "p2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    refs = [
        codec.decode(fields["data"]).reference
        for stream in keys.streams.values()
        for _id, fields in xrange(redis_client, stream)
    ]
    assert sum(moved) == total
    assert len(refs) == total  # sin pérdida
    assert len(set(refs)) == total  # sin duplicados
    assert redis_client.zcard("rpa:retry") == 0


def test_xautoclaim_recovers_item_after_crash(redis_client: redis.Redis, keys: QueueKeys) -> None:
    a = make_queue(redis_client, keys, "worker-a", lease_ms=200)
    a.prepare()
    a.publish(make_tx("REF-CRASH", 1))
    taken = a.fetch_next()
    assert taken is not None  # A muere sin ACK (SIGKILL)

    b = make_queue(redis_client, keys, "worker-b", lease_ms=200)
    assert b.fetch_next() is None  # dentro del lease: nadie lo roba
    time.sleep(0.4)
    reclaimed = b.fetch_next()
    assert reclaimed is not None
    assert reclaimed.message_id == taken.message_id
    assert reclaimed.delivery_count == 2
    b.ack(reclaimed)


def test_restart_with_same_consumer_recovers_own_pending_immediately(
    redis_client: redis.Redis, keys: QueueKeys
) -> None:
    first = make_queue(redis_client, keys, "worker-a")
    first.prepare()
    first.publish(make_tx("REF-OWN", 1))
    assert first.fetch_next() is not None  # SIGKILL

    restarted = make_queue(redis_client, keys, "worker-a")  # mismo HOSTNAME
    d = restarted.fetch_next()
    assert d is not None
    assert d.transaction.reference == "REF-OWN"
    assert d.delivery_count == 2


def test_dead_letter_keeps_reason_and_last_noted_error(
    queue: RedisQueue, redis_client: redis.Redis
) -> None:
    queue.publish(make_tx("REF-P", 1))
    d = queue.fetch_next()
    assert d is not None
    queue.note_failure(d, "RuntimeError: boom")
    queue.dead_letter(d, d.transaction, "max_deliveries", None)

    ((_id, fields),) = xrange(redis_client, "rpa:dlq")
    assert fields["reason"] == "max_deliveries"
    assert fields["last_error"] == "RuntimeError: boom"
    assert redis_client.hlen("rpa:queue:last_error") == 0
    assert redis_client.xpending("rpa:queue:normal", "rpa-workers")["pending"] == 0
    entry = queue.find("REF-P")
    assert entry is not None
    assert (entry.reason, entry.last_error) == ("max_deliveries", "RuntimeError: boom")


def test_unparseable_message_goes_to_dlq(queue: RedisQueue, redis_client: redis.Redis) -> None:
    redis_client.xadd("rpa:queue:normal", {"data": "{not json"})
    queue.publish(make_tx("REF-OK", 1))
    d = queue.fetch_next()
    assert d is not None
    assert d.transaction.reference == "REF-OK"
    ((_id, fields),) = xrange(redis_client, "rpa:dlq")
    assert fields["reason"] == "unparseable"


def test_stop_key_created_after_start_is_honored(
    queue: RedisQueue, redis_client: redis.Redis, keys: QueueKeys
) -> None:
    assert queue.is_requested() is False
    cli = make_queue(redis_client, keys, "cli")
    cli.request_stop(ttl_seconds=60)
    assert queue.is_requested() is True
    assert 0 < int(redis_client.ttl("control:should_stop")) <= 60  # TTL obligatorio
    assert cli.clear_stop() is True
    assert queue.is_requested() is False


def test_stop_key_older_than_worker_start_is_ignored(
    redis_client: redis.Redis, keys: QueueKeys
) -> None:
    """Una llave olvidada no produce un bucle de reinicios."""
    make_queue(redis_client, keys, "cli").request_stop(ttl_seconds=60)
    time.sleep(0.01)
    restarted = make_queue(redis_client, keys, "worker-a")
    restarted.prepare()  # arranque posterior a la llave
    assert restarted.is_requested() is False


def test_stop_key_written_by_hand_is_ignored(queue: RedisQueue, redis_client: redis.Redis) -> None:
    redis_client.set("control:should_stop", "1", ex=60)  # redis-cli sin el comando
    assert queue.is_requested() is False


def test_stale_idle_consumers_without_pending_are_deleted(
    redis_client: redis.Redis, keys: QueueKeys
) -> None:
    first = make_queue(redis_client, keys, "old-idle")
    first.prepare()
    assert first.fetch_next() is None  # registra el consumer sin pendientes
    busy = make_queue(redis_client, keys, "old-busy")
    busy.publish(make_tx("REF-BUSY", 1))
    assert busy.fetch_next() is not None  # queda con 1 pendiente
    time.sleep(0.3)

    worker = RedisQueue(
        redis_client,
        keys,
        group="rpa-workers",
        consumer="worker-new",
        block_ms=100,
        lease_ms=120_000,
        claim_interval_ms=100,
        stale_consumer_ms=200,
    )
    worker.prepare()
    names = {c["name"] for c in redis_client.xinfo_consumers("rpa:queue:normal", "rpa-workers")}
    assert "old-idle" not in names  # 0 pendientes e inactivo: borrado
    assert "old-busy" in names  # con pendientes: NUNCA se borra


def test_standalone_redis_is_not_cluster(redis_client: redis.Redis) -> None:
    assert redis_cluster_enabled(redis_client) is False


def test_queue_stats_report_oldest_age_and_dlq(
    queue: RedisQueue, redis_client: redis.Redis, keys: QueueKeys
) -> None:
    queue.publish(make_tx("REF-OLD", 1))
    queue.publish(make_tx("REF-NEW", 2))
    assert queue.fetch_next() is not None  # uno pendiente, otro sin entregar
    redis_client.xadd("rpa:dlq", {"data": "x", "reason": "r"})

    stats = read_queue_stats(redis_client, keys, "rpa-workers", now=time.time() + 30)
    assert stats.oldest_age_seconds["normal"] >= 29
    assert stats.oldest_age_seconds["high"] == 0
    assert stats.depth["normal"] == 2
    assert stats.dlq_length == 1
