"""Fakes en memoria de los puertos. Replican la semántica que los adaptadores
reales deben cumplir (atomicidad Redis, upsert condicional de Postgres)."""

from __future__ import annotations

import itertools
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace

from rpa_worker.application.circuit_breaker import CircuitState
from rpa_worker.application.ports import (
    Delivery,
    DlqEntry,
    NotReopenableError,
    TransactionRecord,
    UnparseableDlqEntry,
)
from rpa_worker.application.should_stop import ShouldStop
from rpa_worker.domain.exceptions import AlertKind, SystemException
from rpa_worker.domain.transaction import Transaction, TransactionStatus

Hook = Callable[[], None]
S = TransactionStatus


class FakeClock:
    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self._now = now
        self._mono = 0.0

    def now(self) -> float:
        return self._now

    def monotonic(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        self._now += seconds
        self._mono += seconds


class FakeSleeper:
    """No duerme: avanza el reloj. Respeta ShouldStop como event.wait()."""

    def __init__(self, clock: FakeClock, should_stop: ShouldStop) -> None:
        self.clock = clock
        self.should_stop = should_stop
        self.sleeps: list[float] = []
        self.on_sleep: Hook | None = None

    def sleep(self, seconds: float) -> bool:
        self.sleeps.append(seconds)
        if self.on_sleep is not None:
            self.on_sleep()
        if self.should_stop.is_set():
            return True
        self.clock.advance(seconds)
        return False


class FakeQueue:
    """Streams por prioridad + PEL + sorted set de reintentos + DLQ."""

    def __init__(self, priorities: tuple[str, ...] = ("high", "normal", "low")) -> None:
        self.priorities = priorities
        self.streams: dict[str, deque[Delivery]] = {p: deque() for p in priorities}
        self.pending: dict[str, Delivery] = {}
        self.acked: list[str] = []
        # Miembro determinista (reference, retry_count): reprogramar el mismo
        # reintento actualiza el score en vez de duplicar el miembro.
        self.retry_zset: dict[tuple[str, int], tuple[Transaction, float]] = {}
        self.scheduled: list[tuple[Transaction, float]] = []  # historial de ZADD
        self.dlq_all: list[DlqEntry | UnparseableDlqEntry] = []
        self.errors: dict[str, str] = {}
        self.fetched: list[str] = []
        self.fetch_calls = 0
        self._ids = itertools.count(1)
        self.on_empty: Hook | None = None
        self.on_fetch: Hook | None = None
        self.on_restore: Callable[[DlqEntry], None] | None = None
        self.fail_next_redis_op = False
        self.fail_fetch = 0

    # helpers de prueba
    def push(self, tx: Transaction) -> Delivery:
        d = Delivery(f"{next(self._ids)}-0", f"q:{tx.priority}", tx)
        self.streams[tx.priority].append(d)
        return d

    def redeliver_pending(self) -> None:
        """Simula XAUTOCLAIM tras un crash: los pendientes vuelven a entregarse."""
        for mid, d in list(self.pending.items()):
            del self.pending[mid]
            self.streams[d.transaction.priority].appendleft(
                replace(d, delivery_count=d.delivery_count + 1)
            )

    def queued(self) -> int:
        return sum(len(s) for s in self.streams.values()) + len(self.retry_zset)

    def _maybe_fail(self) -> None:
        if self.fail_next_redis_op:
            self.fail_next_redis_op = False
            raise SystemException("redis_unavailable", "fake", retryable=True)

    # ------------------------------------------------------------ puerto cola
    def prepare(self) -> None:
        pass

    def promote_due(self, now: float) -> int:
        due = [key for key, (_, ready) in self.retry_zset.items() if ready <= now]
        for key in due:
            tx, _ = self.retry_zset.pop(key)
            self.push(tx)
        return len(due)

    def fetch_next(self) -> Delivery | None:
        self.fetch_calls += 1
        if self.fail_fetch:
            self.fail_fetch -= 1
            raise SystemException("redis_unavailable", "fake", retryable=True)
        for p in self.priorities:
            if self.streams[p]:
                d = self.streams[p].popleft()
                self.pending[d.message_id] = d
                self.fetched.append(d.transaction.reference)
                if self.on_fetch is not None:
                    self.on_fetch()
                return d
        if self.on_empty is not None:
            self.on_empty()
        return None

    def ack(self, delivery: Delivery) -> None:
        self._maybe_fail()
        self.pending.pop(delivery.message_id, None)
        self.acked.append(delivery.message_id)

    def schedule_retry(self, delivery: Delivery, transaction: Transaction, ready_at: float) -> None:
        self._maybe_fail()
        key = (transaction.reference, transaction.retry_count)
        self.retry_zset[key] = (transaction, ready_at)
        self.scheduled.append((transaction, ready_at))
        self.pending.pop(delivery.message_id, None)
        self.acked.append(delivery.message_id)

    def dead_letter(
        self, delivery: Delivery, transaction: Transaction, reason: str, last_error: str | None
    ) -> None:
        self._maybe_fail()
        error = last_error or self.errors.pop(transaction.reference, None)
        self.add_dlq(DlqEntry(self._next_dlq_id(), transaction, reason, error))
        self.pending.pop(delivery.message_id, None)
        self.acked.append(delivery.message_id)

    def _next_dlq_id(self) -> str:
        return f"{next(self._ids)}-0"  # ordenable como los ids de stream

    def add_dlq(self, entry: DlqEntry | UnparseableDlqEntry) -> None:
        self.dlq_all.append(entry)

    @property
    def dlq(self) -> list[DlqEntry]:
        return [e for e in self.dlq_all if isinstance(e, DlqEntry)]

    def note_failure(self, delivery: Delivery, error: str) -> None:
        self.errors[delivery.transaction.reference] = error

    # ------------------------------------------------------------- puerto DLQ
    def find(self, reference: str) -> DlqEntry | None:
        matches = [e for e in self.dlq if e.transaction.reference == reference]
        return matches[-1] if matches else None

    def restore(self, entry: DlqEntry, transaction: Transaction) -> bool:
        if self.on_restore is not None:
            self.on_restore(entry)
        self._maybe_fail()
        if entry not in self.dlq_all:  # XDEL como puerta
            return False
        self.dlq_all.remove(entry)
        self.push(transaction)
        return True

    def last_entry_id(self) -> str | None:
        return self.dlq_all[-1].entry_id if self.dlq_all else None

    def scan(
        self, *, after: str | None, until: str, count: int
    ) -> list[DlqEntry | UnparseableDlqEntry]:
        def key(entry_id: str) -> int:
            return int(entry_id.split("-")[0])

        lo = key(after) if after else -1
        return [e for e in self.dlq_all if lo < key(e.entry_id) <= key(until)][:count]


@dataclass
class Row:
    record: TransactionRecord
    result_writes: int = 0


@dataclass
class Audit:
    requeue_id: str
    reference: str
    operator: str
    reason: str


class FakeRepository:
    """Upsert condicional: no-op solo si la fila existente es terminal (opción a)."""

    def __init__(self) -> None:
        self.rows: dict[str, Row] = {}
        self.history: list[TransactionRecord] = []
        self.audit: list[Audit] = []
        self.fail_record = 0
        self.down = False  # Postgres caído: toda operación falla (transitorio)
        self.reject_next = False  # la BD rechaza la fila (no reintentable)
        # Referencias cuyo record() falla SIEMPRE como transitorio (bug disfrazado).
        self.fail_refs: set[str] = set()

    def _check(self) -> None:
        if self.down:
            raise SystemException("database_unavailable", "fake down", retryable=True)

    def find_terminal_status(self, reference: str) -> TransactionStatus | None:
        self._check()
        row = self.rows.get(reference)
        if row is not None and row.record.status.is_terminal:
            return row.record.status
        return None

    def record(self, record: TransactionRecord) -> bool:
        self._check()
        if self.fail_record or record.reference in self.fail_refs:
            self.fail_record = max(0, self.fail_record - 1)
            raise SystemException("database_unavailable", "fake", retryable=True)
        if self.reject_next:
            self.reject_next = False
            raise SystemException(
                "database_rejected", "value too long", retryable=False, alert=AlertKind.CONTRACT
            )
        row = self.rows.get(record.reference)
        if row is not None and row.record.status.is_terminal:
            return False
        writes = (row.result_writes if row else 0) + (1 if record.result is not None else 0)
        self.rows[record.reference] = Row(record, writes)
        self.history.append(record)
        return True

    # ----------------------------------------------------- puerto requeue
    def current_status(self, reference: str) -> TransactionStatus | None:
        self._check()
        row = self.rows.get(reference)
        return row.record.status if row else None

    def reopen(self, *, entry: DlqEntry, requeue_id: str, operator: str, reason: str) -> int:
        self._check()
        tx = entry.transaction
        row = self.rows.get(tx.reference)
        if row is not None and row.record.status in (S.SUCCESSFUL, S.FAILED):
            raise NotReopenableError(tx.reference, row.record.status)
        if all(a.requeue_id != requeue_id for a in self.audit):  # ON CONFLICT DO NOTHING
            self.audit.append(Audit(requeue_id, tx.reference, operator, reason))
        if row is None:  # entró al DLQ por max_deliveries sin fila
            rec = TransactionRecord(
                tx.reference, S.RETRIED, tx.priority, 0, tx.correlation_id, requeue_count=1
            )
            self.rows[tx.reference] = Row(rec)
        elif row.record.status is S.ABANDONED:
            row.record = replace(
                row.record,
                status=S.RETRIED,
                retry_count=0,
                requeue_count=row.record.requeue_count + 1,
                finished_at=None,
            )
        # Retried: re-ejecución tras crash, continuar sin cambios.
        return self.rows[tx.reference].record.requeue_count


@dataclass
class FakeApi:
    """Respuestas programadas por id; por defecto devuelve un post válido."""

    scripted: dict[int, list[object]] = field(default_factory=dict)
    calls: list[int] = field(default_factory=list)
    on_call: Callable[[int], None] | None = None

    def fetch_post(self, post_id: int) -> object:
        self.calls.append(post_id)
        if self.on_call is not None:
            self.on_call(post_id)
        queue = self.scripted.get(post_id)
        if queue:
            item = queue.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        return {"userId": 1, "id": post_id, "title": f"post {post_id}", "body": "..."}


class FakeRemoteStop:
    def __init__(self) -> None:
        self.requested = False

    def is_requested(self) -> bool:
        return self.requested


@dataclass
class FakeMetrics:
    finished: list[tuple[TransactionStatus, str]] = field(default_factory=list)
    retries: list[tuple[str, str]] = field(default_factory=list)
    circuit: list[CircuitState] = field(default_factory=list)
    alerts: list[tuple[AlertKind, str]] = field(default_factory=list)

    def transaction_finished(
        self, status: TransactionStatus, priority: str, duration_seconds: float
    ) -> None:
        self.finished.append((status, priority))

    def retry(self, layer: str, reason: str) -> None:
        self.retries.append((layer, reason))

    def circuit_state(self, state: CircuitState) -> None:
        self.circuit.append(state)

    def alert(self, kind: AlertKind, reason: str) -> None:
        self.alerts.append((kind, reason))


def make_tx(
    reference: str,
    post_id: object = 1,
    *,
    priority: str = "normal",
    retry_count: int = 0,
    payload: Mapping[str, object] | None = None,
    traceparent: str | None = None,
) -> Transaction:
    return Transaction(
        reference=reference,
        priority=priority,
        payload=payload if payload is not None else {"id": post_id},
        retry_count=retry_count,
        correlation_id=f"corr-{reference}",
        traceparent=traceparent,
    )
