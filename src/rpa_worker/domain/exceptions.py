"""Frontera explícita entre errores de negocio y de sistema.

* BusinessException: el dato es el problema. Reintentar no cambia nada.
  -> status Failed, sin reintento. Único origen de Failed.
* SystemException(retryable=True): el entorno falló de forma transitoria.
  -> backoff dentro del item y, si persiste, reencolado (MaxRetryNumber).
* SystemException(retryable=False): configuración o contrato roto (400, 401,
  403, 409, 422, respuesta 2xx con forma inválida, BD que rechaza la fila).
  -> Abandoned + DLQ (reabrible con ``requeue``) + alerta. Cuenta para el
  circuit breaker: no es un item malo, es el sistema mal configurado.
"""

from __future__ import annotations

from enum import StrEnum


class AlertKind(StrEnum):
    AUTH = "auth"  # credenciales/permisos: 401, 403
    CONTRACT = "contract"  # la API o la BD no aceptan lo que enviamos o devuelven otra cosa
    POISON = "poison"  # max_deliveries (tumba al worker o dispara un bug) o mensaje ilegible
    MAX_AGE = "max_age"  # superó max_item_age liberándose por fallos transitorios


class TransactionError(Exception):
    """Base. ``reason`` es un código estable apto para logs y labels."""

    def __init__(self, reason: str, message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason


class BusinessException(TransactionError):
    pass


class SystemException(TransactionError):
    def __init__(
        self,
        reason: str,
        message: str = "",
        *,
        retryable: bool,
        retry_after: float | None = None,
        rate_limited: bool = False,
        alert: AlertKind | None = None,
    ) -> None:
        super().__init__(reason, message)
        self.retryable = retryable
        # Segundos pedidos por el servidor (Retry-After), ya normalizados.
        self.retry_after = retry_after
        # Un 429 no indica que el sistema esté roto: es neutro para el breaker.
        self.rate_limited = rate_limited
        # Toda excepción no reintentable alerta; por defecto como contrato roto.
        self.alert = alert if alert is not None or retryable else AlertKind.CONTRACT

    @property
    def requires_alert(self) -> bool:
        return self.alert is not None
