"""Serialización del mensaje en la cola.

El mensaje es un único campo ``data`` con JSON canónico (llaves ordenadas,
sin espacios). Ser determinista importa: el mismo item con el mismo
retry_count produce el mismo miembro del sorted set, así que reprogramar un
reintento tras un crash actualiza el score en vez de duplicar el item.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from rpa_worker.domain.transaction import Transaction

FIELD = "data"


class UnparseableMessageError(ValueError):
    pass


def encode(tx: Transaction) -> str:
    body: dict[str, Any] = {
        "reference": tx.reference,
        "priority": tx.priority,
        "payload": dict(tx.payload),
        "retry_count": tx.retry_count,
        "correlation_id": tx.correlation_id,
        "traceparent": tx.traceparent,
        "enqueued_at": tx.enqueued_at,
        "requeue_count": tx.requeue_count,
        "requeue_id": tx.requeue_id,
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def decode(raw: str | bytes) -> Transaction:
    try:
        body = json.loads(raw)
        if not isinstance(body, Mapping):
            raise TypeError("el mensaje no es un objeto JSON")
        payload = body.get("payload") or {}
        if not isinstance(payload, Mapping):
            raise TypeError("payload no es un objeto")
        return Transaction(
            reference=str(body["reference"]),
            priority=str(body["priority"]),
            payload=dict(payload),
            retry_count=int(body.get("retry_count", 0)),
            correlation_id=str(body.get("correlation_id") or ""),
            traceparent=body.get("traceparent"),
            enqueued_at=float(body.get("enqueued_at") or 0.0),
            requeue_count=int(body.get("requeue_count", 0)),
            requeue_id=body.get("requeue_id"),
        )
    except (ValueError, TypeError, KeyError) as exc:
        raise UnparseableMessageError(str(exc)) from exc


def stream_id_ms(message_id: str) -> int:
    """Timestamp (ms) embebido en un id de stream ``<ms>-<seq>``."""
    return int(message_id.split("-", 1)[0])
