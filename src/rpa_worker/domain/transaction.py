"""Entidad Transaction y su ciclo de estados (SetTransactionStatus de REFramework)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum


class TransactionStatus(StrEnum):
    """New -> InProgress -> Successful | Failed | Retried | Abandoned."""

    NEW = "New"
    IN_PROGRESS = "InProgress"
    SUCCESSFUL = "Successful"  # resultado obtenido y persistido
    FAILED = "Failed"  # Business Exception: no se reintenta
    RETRIED = "Retried"  # System Exception: reencolado (no terminal)
    ABANDONED = "Abandoned"  # agotó MaxRetryNumber o no es reintentable: DLQ

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset(
    {TransactionStatus.SUCCESSFUL, TransactionStatus.FAILED, TransactionStatus.ABANDONED}
)


@dataclass(frozen=True, slots=True)
class Transaction:
    """Un item de la cola.

    ``retry_count`` viaja en el mensaje: es la fuente de verdad para decidir
    Retried vs Abandoned. Nunca se lee de la base de datos, de modo que
    reprocesar el mismo mensaje produce exactamente la misma decisión.
    """

    reference: str
    priority: str
    payload: Mapping[str, object] = field(default_factory=dict)
    retry_count: int = 0
    status: TransactionStatus = TransactionStatus.NEW
    correlation_id: str = ""
    traceparent: str | None = None
    # Epoch (s) del primer encolado. Se conserva entre reintentos para medir
    # la edad real del item.
    enqueued_at: float = 0.0
    # Reaperturas manuales desde el DLQ (``requeue``). retry_count vuelve a 0
    # en cada una; requeue_count conserva la historia.
    requeue_count: int = 0
    requeue_id: str | None = None

    def __post_init__(self) -> None:
        if not self.reference:
            raise ValueError("Transaction.reference no puede estar vacío")
        if self.retry_count < 0 or self.requeue_count < 0:
            raise ValueError("Transaction.retry_count/requeue_count no pueden ser negativos")

    def with_status(self, status: TransactionStatus) -> Transaction:
        return replace(self, status=status)

    def with_retry_count(self, retry_count: int) -> Transaction:
        return replace(self, retry_count=retry_count)
