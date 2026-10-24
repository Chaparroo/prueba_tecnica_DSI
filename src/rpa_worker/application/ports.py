"""Puertos (interfaces) que la aplicación necesita del mundo exterior.

Convención de errores para todos los adaptadores: cualquier fallo de
infraestructura se traduce a ``SystemException`` (o ``BusinessException`` si
el dato es el problema). La aplicación nunca ve excepciones de redis, httpx o
psycopg.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from rpa_worker.application.circuit_breaker import CircuitState
from rpa_worker.domain.exceptions import AlertKind
from rpa_worker.domain.transaction import Transaction, TransactionStatus


@dataclass(frozen=True, slots=True)
class Delivery:
    """Una entrega concreta de un item. El mismo item puede entregarse varias veces."""

    message_id: str
    source: str  # stream de origen (necesario para el ACK)
    transaction: Transaction
    # Veces que se ha entregado este mensaje (XREADGROUP + XAUTOCLAIM).
    delivery_count: int = 1


class TransactionQueue(Protocol):
    def prepare(self) -> None:
        """Crea streams y consumer groups si no existen (idempotente)."""

    def promote_due(self, now: float) -> int:
        """Mueve al stream de su prioridad los reintentos con ready_at <= now.

        Debe ser atómico (selección + ZREM + XADD juntos): nada se pierde ni se
        duplica aunque varios workers lo invoquen a la vez.
        """

    def fetch_next(self) -> Delivery | None:
        """Como máximo UN item: primero un pendiente reclamable (XAUTOCLAIM
        COUNT 1), luego por prioridad. Bloquea hasta ``block_ms``."""

    def ack(self, delivery: Delivery) -> None: ...

    def schedule_retry(self, delivery: Delivery, transaction: Transaction, ready_at: float) -> None:
        """ZADD al sorted set de reintentos + XACK, en un solo script Lua.

        También es la operación de *liberar* un item sin procesarlo: misma
        transacción (mismo retry_count) y ready_at cercano.
        """

    def dead_letter(
        self, delivery: Delivery, transaction: Transaction, reason: str, last_error: str | None
    ) -> None:
        """XADD al DLQ (motivo + último error) + XACK, en un solo script Lua.

        Si ``last_error`` es None se usa el registrado con ``note_failure``.
        """

    def note_failure(self, delivery: Delivery, error: str) -> None:
        """Recuerda el último error de un item que queda pendiente (best effort)."""


class RemoteStopFlag(Protocol):
    """Origen remoto de ShouldStop (llave ``control:should_stop`` en Redis)."""

    def is_requested(self) -> bool: ...


class ExternalApi(Protocol):
    def fetch_post(self, post_id: int) -> object:
        """Un único intento con timeout acotado. Devuelve el JSON decodificado.

        Lanza BusinessException/SystemException según domain.policies.
        """


@dataclass(frozen=True, slots=True)
class TransactionRecord:
    reference: str
    status: TransactionStatus
    priority: str
    retry_count: int
    correlation_id: str
    requeue_count: int = 0
    result: Mapping[str, object] | None = None
    error_reason: str | None = None
    # Epoch (s); solo para estados terminales.
    finished_at: float | None = None


class ResultRepository(Protocol):
    def find_terminal_status(self, reference: str) -> TransactionStatus | None: ...

    def record(self, record: TransactionRecord) -> bool:
        """Upsert por ``reference``.

        Escribe si no hay fila o si la existente NO es terminal; si ya es
        terminal (Successful/Failed/Abandoned) es un no-op y devuelve False.
        """


# ------------------------------------------------------------- requeue


@dataclass(frozen=True, slots=True)
class DlqEntry:
    entry_id: str
    transaction: Transaction
    reason: str
    last_error: str | None
    failed_at: float | None = None  # epoch (s) en que entró al DLQ


@dataclass(frozen=True, slots=True)
class UnparseableDlqEntry:
    """Entrada del DLQ sin reference legible: se reporta, nunca se reencola."""

    entry_id: str
    reason: str


class DeadLetterStore(Protocol):
    def find(self, reference: str) -> DlqEntry | None:
        """Entrada más reciente del DLQ para ``reference``."""

    def restore(self, entry: DlqEntry, transaction: Transaction) -> bool:
        """Lua: XDEL de la entrada y, solo si existía, XADD al stream.

        El XDEL es la puerta: con dos operadores a la vez solo uno encola.
        """

    def last_entry_id(self) -> str | None:
        """Id de la entrada más reciente (tope fijo para un recorrido)."""

    def scan(
        self, *, after: str | None, until: str, count: int
    ) -> list[DlqEntry | UnparseableDlqEntry]:
        """Entradas con id en (after, until], en orden, como máximo ``count``."""


class RequeueRepository(Protocol):
    def current_status(self, reference: str) -> TransactionStatus | None: ...

    def reopen(self, *, entry: DlqEntry, requeue_id: str, operator: str, reason: str) -> int:
        """En UNA transacción: auditoría (idempotente por requeue_id) y
        Abandoned -> Retried (o crea la fila en Retried si no existe; si ya está
        en Retried continúa). Devuelve el requeue_count resultante.

        Lanza NotReopenableError si la fila es Successful o Failed.
        """


class NotReopenableError(Exception):
    def __init__(self, reference: str, status: TransactionStatus) -> None:
        super().__init__(f"{reference} está en {status}: solo se reabren items Abandoned")
        self.reference = reference
        self.status = status


# ---------------------------------------------------------- transversales


class Clock(Protocol):
    def now(self) -> float:
        """Epoch en segundos (para ready_at)."""

    def monotonic(self) -> float:
        """Reloj monotónico (para duraciones y cooldowns)."""


class Sleeper(Protocol):
    def sleep(self, seconds: float) -> bool:
        """Espera interrumpible. Devuelve True si ShouldStop la interrumpió."""


class Metrics(Protocol):
    """Labels de baja cardinalidad solamente: nunca reference ni correlation_id."""

    def transaction_finished(
        self, status: TransactionStatus, priority: str, duration_seconds: float
    ) -> None: ...

    def retry(self, layer: str, reason: str) -> None:
        """layer: 'in_item' (backoff), 'item' (reencolado) o 'release' (liberado)."""

    def circuit_state(self, state: CircuitState) -> None: ...

    def alert(self, kind: AlertKind, reason: str) -> None: ...
