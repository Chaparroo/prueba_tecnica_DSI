"""Repositorio Postgres (psycopg 3 + pool).

Idempotencia, opción (a): el upsert por ``reference`` solo es no-op si la
fila existente ya es terminal (Successful, Failed, Abandoned). Un item
Retried que luego termina bien escribe su resultado; una doble entrega
posterior no lo vuelve a escribir. Es una sola sentencia, así que el
resultado y el estado se escriben en la misma transacción.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from rpa_worker.application.ports import DlqEntry, NotReopenableError, TransactionRecord
from rpa_worker.domain.exceptions import AlertKind, SystemException
from rpa_worker.domain.transaction import TransactionStatus

S = TransactionStatus

UPSERT_RESULT = """
INSERT INTO transaction_results AS t
    (reference, status, priority, retry_count, requeue_count, result, error_reason,
     correlation_id, finished_at)
VALUES (%(reference)s, %(status)s, %(priority)s, %(retry_count)s, %(requeue_count)s,
        %(result)s, %(error_reason)s, %(correlation_id)s, to_timestamp(%(finished_at)s))
ON CONFLICT (reference) DO UPDATE SET
    status         = EXCLUDED.status,
    priority       = EXCLUDED.priority,
    retry_count    = EXCLUDED.retry_count,
    requeue_count  = EXCLUDED.requeue_count,
    result         = EXCLUDED.result,
    error_reason   = EXCLUDED.error_reason,
    correlation_id = EXCLUDED.correlation_id,
    finished_at    = EXCLUDED.finished_at,
    updated_at     = now()
WHERE t.status NOT IN ('Successful', 'Failed', 'Abandoned')
RETURNING reference
"""

TERMINAL_STATUS = """
SELECT status FROM transaction_results
WHERE reference = %s AND status IN ('Successful', 'Failed', 'Abandoned')
"""

INSERT_AUDIT = """
INSERT INTO requeue_audit
    (requeue_id, reference, operator, reason, dlq_entry_id, dlq_reason, dlq_last_error)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (requeue_id) DO NOTHING
"""


@contextmanager
def postgres_errors(operation: str) -> Iterator[None]:
    """Traduce psycopg a la frontera del dominio.

    * Conexión/pool/timeout (OperationalError, incluye PoolTimeout y
      statement_timeout): transitorio, reintentable.
    * La BD rechaza el dato (DataError, IntegrityError): no reintentable,
      contrato roto.
    * Esquema (ProgrammingError: tabla/columna inexistente, migración sin
      aplicar): no reintentable, contrato roto.
    """
    try:
        yield
    except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
        raise SystemException(
            "database_unavailable", f"{operation}: {type(exc).__name__}", retryable=True
        ) from exc
    except (psycopg.DataError, psycopg.IntegrityError) as exc:
        raise SystemException(
            "database_rejected",
            f"{operation}: {type(exc).__name__}: {exc}",
            retryable=False,
            alert=AlertKind.CONTRACT,
        ) from exc
    except psycopg.ProgrammingError as exc:
        raise SystemException(
            "database_schema",
            f"{operation}: {type(exc).__name__}: {exc}",
            retryable=False,
            alert=AlertKind.CONTRACT,
        ) from exc
    except psycopg.Error as exc:
        raise SystemException(
            "database_error", f"{operation}: {type(exc).__name__}", retryable=True
        ) from exc


def create_pool(
    conninfo: str,
    *,
    min_size: int,
    max_size: int,
    connect_timeout: float,
    statement_timeout_ms: int,
) -> ConnectionPool:
    return ConnectionPool(
        conninfo,
        min_size=min_size,
        max_size=max_size,
        open=False,
        timeout=connect_timeout,  # espera máxima por una conexión del pool
        kwargs={
            "connect_timeout": max(1, int(connect_timeout)),
            "options": f"-c statement_timeout={statement_timeout_ms}",
            "application_name": "rpa-worker",
        },
    )


class PostgresRepository:
    """Implementa ResultRepository y RequeueRepository."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def find_terminal_status(self, reference: str) -> TransactionStatus | None:
        with postgres_errors("find_terminal_status"), self._pool.connection() as conn:
            row = conn.execute(TERMINAL_STATUS, (reference,)).fetchone()
        return S(row[0]) if row else None

    def record(self, record: TransactionRecord) -> bool:
        params: dict[str, Any] = {
            "reference": record.reference,
            "status": str(record.status),
            "priority": record.priority,
            "retry_count": record.retry_count,
            "requeue_count": record.requeue_count,
            "result": Jsonb(dict(record.result)) if record.result is not None else None,
            "error_reason": record.error_reason,
            "correlation_id": record.correlation_id,
            "finished_at": record.finished_at,
        }
        with postgres_errors("record"), self._pool.connection() as conn:
            row = conn.execute(UPSERT_RESULT, params).fetchone()
        return row is not None

    # ---------------------------------------------------------- requeue

    def current_status(self, reference: str) -> TransactionStatus | None:
        with postgres_errors("current_status"), self._pool.connection() as conn:
            row = conn.execute(
                "SELECT status FROM transaction_results WHERE reference = %s", (reference,)
            ).fetchone()
        return S(row[0]) if row else None

    def reopen(self, *, entry: DlqEntry, requeue_id: str, operator: str, reason: str) -> int:
        tx = entry.transaction
        with (
            postgres_errors("reopen"),
            self._pool.connection() as conn,
            conn.transaction(),
            conn.cursor(row_factory=dict_row) as cur,
        ):
            row = cur.execute(
                "SELECT status, requeue_count FROM transaction_results "
                "WHERE reference = %s FOR UPDATE",
                (tx.reference,),
            ).fetchone()
            if row is not None and row["status"] in (S.SUCCESSFUL, S.FAILED):
                # Sale del bloque de transacción por excepción: rollback.
                raise NotReopenableError(tx.reference, S(row["status"]))
            cur.execute(
                INSERT_AUDIT,
                (
                    requeue_id,
                    tx.reference,
                    operator,
                    reason,
                    entry.entry_id,
                    entry.reason,
                    entry.last_error,
                ),
            )
            if row is None:  # entró al DLQ por max_deliveries sin fila
                cur.execute(
                    "INSERT INTO transaction_results "
                    "(reference, status, priority, retry_count, requeue_count, correlation_id) "
                    "VALUES (%s, 'Retried', %s, 0, 1, %s)",
                    (tx.reference, tx.priority, tx.correlation_id),
                )
                return 1
            if row["status"] == S.ABANDONED:
                cur.execute(
                    "UPDATE transaction_results SET status = 'Retried', retry_count = 0, "
                    "requeue_count = requeue_count + 1, finished_at = NULL, updated_at = now() "
                    "WHERE reference = %s RETURNING requeue_count",
                    (tx.reference,),
                )
                updated = cur.fetchone()
                return int(updated["requeue_count"]) if updated else 1
            # Ya en Retried: re-ejecución tras un crash entre Postgres y Redis.
            return int(row["requeue_count"])

    def ping(self) -> bool:
        try:
            with self._pool.connection(timeout=1.0) as conn:
                conn.execute("SELECT 1")
            return True
        except psycopg.Error:
            return False
