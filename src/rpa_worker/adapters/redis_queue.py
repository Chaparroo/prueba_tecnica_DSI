"""Adaptador Redis Streams: cola por prioridad, reintentos diferidos y DLQ.

Estructuras (prefijos configurables):
  rpa:queue:<prioridad>   stream por prioridad, consumer group compartido
  rpa:retry               sorted set; score = ready_at (epoch s), miembro = JSON
  rpa:dlq                 stream de items abandonados (data, reason, last_error...)
  rpa:last_error          hash reference -> último error de un item pendiente
  control:should_stop     llave de parada suave del operador

Toda operación que mueve un item entre estructuras es un único script Lua:
Redis lo ejecuta de forma atómica, así que un crash nunca deja el item en
dos sitios ni en ninguno.

Requisito de despliegue: Redis con AOF (appendonly yes, appendfsync everysec).
Sin AOF, un reinicio de Redis pierde streams, PEL y reintentos. No soporta
Redis Cluster: los scripts tocan llaves que no comparten hash slot.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

import redis
import structlog
from redis.exceptions import RedisError, ResponseError

from rpa_worker.adapters import codec
from rpa_worker.application.ports import Delivery, DlqEntry
from rpa_worker.domain.exceptions import SystemException
from rpa_worker.domain.transaction import Transaction

log = structlog.get_logger(__name__)

# KEYS[1]=zset, KEYS[2..n+1]=streams por prioridad; ARGV[1]=now, ARGV[2]=límite,
# ARGV[3..n+2]=nombres de prioridad en el mismo orden que los streams.
PROMOTE_DUE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for _, member in ipairs(due) do
  local target = KEYS[#KEYS]
  local ok, msg = pcall(cjson.decode, member)
  if ok and type(msg) == 'table' then
    for i = 3, #ARGV do
      if msg['priority'] == ARGV[i] then target = KEYS[i - 1] end
    end
  end
  if redis.call('ZREM', KEYS[1], member) == 1 then
    redis.call('XADD', target, '*', 'data', member)
    moved = moved + 1
  end
end
return moved
"""

# KEYS[1]=stream, KEYS[2]=zset; ARGV: group, id, ready_at, member
SCHEDULE_AND_ACK = """
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
return 1
"""

# KEYS[1]=stream, KEYS[2]=errors hash; ARGV: group, id, reference
ACK = """
local n = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[3])
return n
"""

# KEYS[1]=stream, KEYS[2]=dlq, KEYS[3]=errors hash
# ARGV: group, id, member, reason, last_error ('' = usar el hash), reference, failed_at
DEAD_LETTER = """
local err = ARGV[5]
if err == '' then err = redis.call('HGET', KEYS[3], ARGV[6]) or '' end
redis.call('XADD', KEYS[2], '*', 'data', ARGV[3], 'reason', ARGV[4], 'last_error', err,
           'failed_at', ARGV[7], 'source_stream', KEYS[1], 'source_id', ARGV[2])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[3], ARGV[6])
return 1
"""

# KEYS[1]=dlq, KEYS[2]=stream destino; ARGV: entry_id, member
# El XDEL es la puerta: si otro operador ya restauró la entrada, no se encola.
RESTORE_FROM_DLQ = """
if redis.call('XDEL', KEYS[1], ARGV[1]) == 1 then
  redis.call('XADD', KEYS[2], '*', 'data', ARGV[2])
  return 1
end
return 0
"""


@contextmanager
def redis_errors(operation: str) -> Iterator[None]:
    """Traduce errores de redis-py a SystemException (frontera del adaptador)."""
    try:
        yield
    except RedisError as exc:
        raise SystemException(
            "redis_unavailable", f"{operation}: {type(exc).__name__}: {exc}", retryable=True
        ) from exc


def _s(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _data(fields: Mapping[Any, Any] | None) -> str | None:
    """Campo ``data`` de una entrada de stream (bytes o str según el cliente)."""
    if not fields:
        return None
    for key, value in fields.items():
        if _s(key) == codec.FIELD:
            return _s(value)
    return None


@dataclass(frozen=True, slots=True)
class QueueKeys:
    streams: Mapping[str, str]  # prioridad -> stream, en orden de prioridad
    retry_zset: str
    dlq: str
    errors: str
    should_stop: str

    @classmethod
    def build(
        cls,
        *,
        stream_prefix: str,
        priorities: Sequence[str],
        retry_zset: str,
        dlq: str,
        should_stop: str,
    ) -> QueueKeys:
        return cls(
            streams={p: f"{stream_prefix}:{p}" for p in priorities},
            retry_zset=retry_zset,
            dlq=dlq,
            errors=f"{stream_prefix}:last_error",
            should_stop=should_stop,
        )


class RedisQueue:
    """Implementa TransactionQueue, DeadLetterStore y RemoteStopFlag."""

    def __init__(
        self,
        client: redis.Redis,
        keys: QueueKeys,
        *,
        group: str,
        consumer: str,
        block_ms: int,
        lease_ms: int,
        claim_interval_ms: int,
        promote_batch: int = 100,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._r = client
        self._k = keys
        self._group = group
        self._consumer = consumer
        self._block_ms = block_ms
        self._lease_ms = lease_ms
        self._claim_interval = claim_interval_ms / 1000
        self._promote_batch = promote_batch
        self._now = clock
        self._mono = monotonic
        self._next_claim_check = 0.0
        self._own_pending_recovered = False
        self._promote = client.register_script(PROMOTE_DUE)
        self._schedule = client.register_script(SCHEDULE_AND_ACK)
        self._ack = client.register_script(ACK)
        self._dead = client.register_script(DEAD_LETTER)
        self._restore = client.register_script(RESTORE_FROM_DLQ)

    # ---------------------------------------------------------- preparación

    def prepare(self) -> None:
        with redis_errors("prepare"):
            for stream in self._k.streams.values():
                try:
                    # id 0: también consume lo encolado antes de crear el grupo.
                    self._r.xgroup_create(stream, self._group, id="0", mkstream=True)
                except ResponseError as exc:
                    if "BUSYGROUP" not in str(exc):
                        raise
            if self._r.exists(self._k.should_stop):
                log.warning(
                    "queue.should_stop_key_present",
                    key=self._k.should_stop,
                    hint="el worker se detendrá enseguida; bórrala con DEL si no es intencional",
                )

    # ------------------------------------------------------------ productor

    def publish(self, tx: Transaction) -> str:
        """XADD al stream de su prioridad (CLI ``seed``)."""
        stream = self._stream_for(tx.priority)
        with redis_errors("publish"):
            return _s(self._r.xadd(stream, {codec.FIELD: codec.encode(tx)}))

    # ------------------------------------------------------------- consumo

    def promote_due(self, now: float) -> int:
        keys = [self._k.retry_zset, *self._k.streams.values()]
        args: list[Any] = [now, self._promote_batch, *self._k.streams.keys()]
        with redis_errors("promote_due"):
            return int(cast(int, self._promote(keys=keys, args=args)))

    def fetch_next(self) -> Delivery | None:
        with redis_errors("fetch_next"):
            for _ in range(10):  # salta mensajes ilegibles sin bloquear el loop
                delivery, skipped = self._fetch_once()
                if not skipped:
                    return delivery
            return None

    def _fetch_once(self) -> tuple[Delivery | None, bool]:
        """Devuelve (delivery, se_saltó_un_mensaje_ilegible)."""
        if not self._own_pending_recovered:
            entry = self._own_pending()
            if entry is not None:
                return self._to_delivery(*entry)
            self._own_pending_recovered = True

        if self._mono() >= self._next_claim_check:
            entry = self._claim_one()
            if entry is not None:
                return self._to_delivery(*entry)
            self._next_claim_check = self._mono() + self._claim_interval

        for stream in self._k.streams.values():
            resp = self._read({stream: ">"}, block=None)
            if resp:
                return self._take_first(resp)
        resp = self._read({s: ">" for s in self._k.streams.values()}, block=self._block_ms)
        if not resp:
            return None, False
        return self._take_first(resp)

    def _read(self, streams: dict[str, str], block: int | None) -> list[Any]:
        resp = self._r.xreadgroup(self._group, self._consumer, streams, count=1, block=block)  # type: ignore[arg-type]
        return cast(list[Any], resp or [])

    def _take_first(self, resp: list[Any]) -> tuple[Delivery | None, bool]:
        """Con BLOCK sobre varios streams pueden llegar varios items: se queda con
        el de mayor prioridad y devuelve el resto intactos (XACK+ZADD ready_at=ahora)."""
        order = {s: i for i, s in enumerate(self._k.streams.values())}
        entries = [
            (_s(stream), _s(mid), fields) for stream, messages in resp for mid, fields in messages
        ]
        entries.sort(key=lambda e: order.get(e[0], len(order)))
        first, *extra = entries
        for stream, mid, fields in extra:
            self._schedule(
                keys=[stream, self._k.retry_zset],
                args=[self._group, mid, self._now(), _data(fields) or ""],
            )
            log.info("queue.extra_entry_released", stream=stream, message_id=mid)
        return self._to_delivery(first[0], first[1], first[2], 1)

    def _own_pending(self) -> tuple[str, str, Any, int] | None:
        """Al arrancar: mensajes que ESTE consumer tenía sin ACK (SIGKILL previo)."""
        for stream in self._k.streams.values():
            while True:
                resp = self._read({stream: "0"}, block=None)
                messages = [m for _stream, ms in resp for m in ms]
                if not messages:
                    break
                mid, fields = messages[0]
                mid_s = _s(mid)
                if fields:
                    log.warning("queue.recovered_own_pending", stream=stream, message_id=mid_s)
                    return stream, mid_s, fields, self._times_delivered(stream, mid_s)
                # La entrada fue borrada del stream (XDEL/XTRIM): solo queda en el PEL.
                self._r.xack(stream, self._group, mid_s)
        return None

    def _claim_one(self) -> tuple[str, str, Any, int] | None:
        """XAUTOCLAIM COUNT 1: reclama un huérfano de otro consumer (lease vencido)."""
        for stream in self._k.streams.values():
            resp = self._r.xautoclaim(
                stream, self._group, self._consumer, self._lease_ms, start_id="0-0", count=1
            )
            for mid, fields in resp[1]:
                if fields:
                    mid_s = _s(mid)
                    log.warning("queue.claimed_stale", stream=stream, message_id=mid_s)
                    return stream, mid_s, fields, self._times_delivered(stream, mid_s)
        return None

    def _times_delivered(self, stream: str, message_id: str) -> int:
        info = cast(
            list[dict[str, Any]],
            self._r.xpending_range(stream, self._group, min=message_id, max=message_id, count=1),
        )
        return int(info[0]["times_delivered"]) if info else 1

    def _to_delivery(
        self, stream: str, message_id: str, fields: Any, delivery_count: int
    ) -> tuple[Delivery | None, bool]:
        raw = _data(fields) or ""
        try:
            tx = codec.decode(raw)
        except codec.UnparseableMessageError as exc:
            log.error("queue.unparseable_message", stream=stream, message_id=message_id)
            self._dead(
                keys=[stream, self._k.dlq, self._k.errors],
                args=[self._group, message_id, raw, "unparseable", str(exc), "", self._now()],
            )
            return None, True
        return Delivery(message_id, stream, tx, delivery_count), False

    # ------------------------------------------------------- confirmaciones

    def ack(self, delivery: Delivery) -> None:
        with redis_errors("ack"):
            self._ack(
                keys=[delivery.source, self._k.errors],
                args=[self._group, delivery.message_id, delivery.transaction.reference],
            )

    def schedule_retry(self, delivery: Delivery, transaction: Transaction, ready_at: float) -> None:
        with redis_errors("schedule_retry"):
            self._schedule(
                keys=[delivery.source, self._k.retry_zset],
                args=[self._group, delivery.message_id, ready_at, codec.encode(transaction)],
            )

    def dead_letter(
        self, delivery: Delivery, transaction: Transaction, reason: str, last_error: str | None
    ) -> None:
        with redis_errors("dead_letter"):
            self._dead(
                keys=[delivery.source, self._k.dlq, self._k.errors],
                args=[
                    self._group,
                    delivery.message_id,
                    codec.encode(transaction),
                    reason,
                    (last_error or "")[:2_000],
                    transaction.reference,
                    self._now(),
                ],
            )

    def note_failure(self, delivery: Delivery, error: str) -> None:
        with redis_errors("note_failure"):
            self._r.hset(self._k.errors, delivery.transaction.reference, error[:2_000])

    # ----------------------------------------------------------------- DLQ

    def find(self, reference: str) -> DlqEntry | None:
        with redis_errors("dlq_find"):
            entries = cast(list[Any], self._r.xrevrange(self._k.dlq) or [])
        for entry_id, fields in entries:
            f = {_s(k): _s(v) for k, v in (fields or {}).items()}
            try:
                tx = codec.decode(f.get(codec.FIELD, ""))
            except codec.UnparseableMessageError:
                continue
            if tx.reference == reference:
                return DlqEntry(_s(entry_id), tx, f.get("reason", ""), f.get("last_error") or None)
        return None

    def restore(self, entry: DlqEntry, transaction: Transaction) -> bool:
        with redis_errors("dlq_restore"):
            restored = self._restore(
                keys=[self._k.dlq, self._stream_for(transaction.priority)],
                args=[entry.entry_id, codec.encode(transaction)],
            )
        return int(cast(int, restored)) == 1

    # ------------------------------------------------------------ ShouldStop

    def is_requested(self) -> bool:
        with redis_errors("should_stop"):
            return bool(self._r.exists(self._k.should_stop))

    # ---------------------------------------------------------------- apoyo

    def _stream_for(self, priority: str) -> str:
        streams = self._k.streams
        return streams.get(priority) or list(streams.values())[-1]

    def ping(self) -> bool:
        try:
            return bool(self._r.ping())
        except RedisError:
            return False


@dataclass(frozen=True, slots=True)
class QueueStats:
    oldest_age_seconds: Mapping[str, float]  # por prioridad
    depth: Mapping[str, int]  # mensajes sin entregar + pendientes, por prioridad
    retry_scheduled: int
    dlq_length: int


def read_queue_stats(client: redis.Redis, keys: QueueKeys, group: str, now: float) -> QueueStats:
    """Para el gauge de edad: usar un cliente con socket_timeout corto (~0.5 s)."""
    ages: dict[str, float] = {}
    depth: dict[str, int] = {}
    for priority, stream in keys.streams.items():
        oldest_ms: int | None = None
        groups = client.xinfo_groups(stream)
        info = next((g for g in groups if _s(g["name"]) == group), None)
        if info is not None:
            pending = int(info.get("pending") or 0)
            lag = info.get("lag")
            depth[priority] = pending + (int(lag) if lag is not None else 0)
            if pending:
                summary = client.xpending(stream, group)
                oldest_ms = codec.stream_id_ms(_s(summary["min"]))
            last = _s(info["last-delivered-id"])
            nxt = cast(list[Any], client.xrange(stream, min=f"({last}", count=1) or [])
            if nxt:
                ts = codec.stream_id_ms(_s(nxt[0][0]))
                oldest_ms = ts if oldest_ms is None else min(oldest_ms, ts)
        ages[priority] = max(0.0, now - oldest_ms / 1000) if oldest_ms is not None else 0.0
    return QueueStats(
        oldest_age_seconds=ages,
        depth=depth,
        retry_scheduled=int(client.zcard(keys.retry_zset)),
        dlq_length=int(client.xlen(keys.dlq)),
    )
