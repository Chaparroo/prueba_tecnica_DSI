"""Políticas puras: clasificación de errores, backoff y decisión de reintento.

Esta es la tabla de errores que documenta el README; cualquier cambio aquí
debe reflejarse allá y en tests/unit/test_policies.py.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from email.utils import parsedate_to_datetime

from rpa_worker.domain.exceptions import (
    AlertKind,
    BusinessException,
    SystemException,
    TransactionError,
)
from rpa_worker.domain.transaction import TransactionStatus

NON_RETRYABLE_CLIENT_ERRORS = {400: "bad_request", 401: "unauthorized", 403: "forbidden"}


def parse_post_id(payload: Mapping[str, object]) -> int:
    """Extrae el id del payload. Datos inválidos son de negocio."""
    if "id" not in payload:
        raise BusinessException("invalid_payload", "el payload no contiene 'id'")
    raw = payload["id"]
    if isinstance(raw, bool):  # bool es subclase de int: True no es un id
        raise BusinessException("non_numeric_id", "id booleano")
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str) and raw.isascii() and raw.isdigit():
        value = int(raw)
    else:
        raise BusinessException("non_numeric_id", "el id no es numérico")
    if value <= 0:
        raise BusinessException("invalid_payload", "el id debe ser positivo")
    return value


def parse_retry_after(value: str | None, now: float) -> float | None:
    """Retry-After admite segundos o fecha HTTP (RFC 9110). Devuelve segundos >= 0."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - now)


def classify_http_status(status: int, retry_after: float | None = None) -> TransactionError | None:
    """None si la respuesta es exitosa; si no, la excepción que corresponde."""
    if 200 <= status < 300:
        return None
    if status == 404:
        return BusinessException("not_found", "el recurso no existe (404)")
    if status == 408:
        return SystemException("request_timeout", "HTTP 408", retryable=True)
    if status == 429:
        return SystemException(
            "rate_limited", "HTTP 429", retryable=True, retry_after=retry_after, rate_limited=True
        )
    if status in NON_RETRYABLE_CLIENT_ERRORS:
        alert = AlertKind.AUTH if status in (401, 403) else AlertKind.CONTRACT
        return SystemException(
            NON_RETRYABLE_CLIENT_ERRORS[status], f"HTTP {status}", retryable=False, alert=alert
        )
    if 500 <= status < 600:
        return SystemException("server_error", f"HTTP {status}", retryable=True)
    # Otros 4xx (409, 410, 422...) y códigos inesperados: no sabemos si
    # reintentar sirve, así que se trata como no reintentable con alerta.
    return SystemException("unexpected_status", f"HTTP {status}", retryable=False)


def validate_api_result(body: object) -> Mapping[str, object]:
    """Una respuesta 2xx con forma inesperada es un fallo del sistema externo."""
    if not isinstance(body, Mapping) or "id" not in body:
        raise SystemException(
            "invalid_response", "la API devolvió un cuerpo inesperado", retryable=False
        )
    return body


def backoff_delay(attempt: int, base: float, cap: float, rand: Callable[[], float]) -> float:
    """Backoff exponencial con full jitter: U(0, min(cap, base * 2^(attempt-1)))."""
    ceiling = min(cap, base * (2.0 ** (attempt - 1)))
    return rand() * ceiling


def item_retry_delay(retry_count: int, base: float, cap: float) -> float:
    """Retraso del reencolado de item (capa 2), sin jitter: lo dispersa el sorted set."""
    return min(cap, base * (2.0 ** max(0, retry_count - 1)))


def status_after_system_failure(retry_count: int, max_retry_number: int) -> TransactionStatus:
    """REFramework: se reencola mientras retry_count + 1 <= MaxRetryNumber."""
    if retry_count + 1 <= max_retry_number:
        return TransactionStatus.RETRIED
    return TransactionStatus.ABANDONED
