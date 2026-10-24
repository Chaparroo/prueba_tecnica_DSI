"""Cliente HTTP de la API externa (jsonplaceholder ``GET /posts/{id}``).

Un único intento por llamada, con timeout acotado; los reintentos los decide
la aplicación. Toda falla se traduce con ``domain.policies``.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import httpx
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from rpa_worker.domain import policies
from rpa_worker.domain.exceptions import AlertKind, SystemException

tracer = trace.get_tracer("rpa_worker.http")


class JsonPlaceholderApi:
    def __init__(
        self,
        *,
        base_url: str,
        connect_timeout: float,
        read_timeout: float,
        user_agent: str,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._clock = clock
        self._client = httpx.Client(
            base_url=base_url,
            timeout=httpx.Timeout(read_timeout, connect=connect_timeout, pool=connect_timeout),
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=False,  # un 3xx es inesperado: se clasifica, no se sigue
            transport=transport,
        )

    def fetch_post(self, post_id: int) -> object:
        with tracer.start_as_current_span(
            "GET /posts/{id}",
            kind=SpanKind.CLIENT,
            attributes={"http.request.method": "GET", "url.template": "/posts/{id}"},
        ) as span:
            headers: dict[str, str] = {}
            propagate.inject(headers)  # traceparent W3C hacia la API
            try:
                response = self._client.get(f"/posts/{post_id}", headers=headers)
            except httpx.TimeoutException as exc:
                span.set_status(Status(StatusCode.ERROR, "timeout"))
                raise SystemException("timeout", f"timeout: {exc}", retryable=True) from exc
            except httpx.TransportError as exc:
                span.set_status(Status(StatusCode.ERROR, "connection_error"))
                raise SystemException(
                    "connection_error", f"{type(exc).__name__}: {exc}", retryable=True
                ) from exc

            span.set_attribute("http.response.status_code", response.status_code)
            retry_after = policies.parse_retry_after(
                response.headers.get("Retry-After"), self._clock()
            )
            error = policies.classify_http_status(response.status_code, retry_after)
            if error is not None:
                span.set_status(Status(StatusCode.ERROR, error.reason))
                raise error
            try:
                return response.json()
            except ValueError as exc:
                raise SystemException(
                    "invalid_response",
                    "la API devolvió un cuerpo que no es JSON",
                    retryable=False,
                    alert=AlertKind.CONTRACT,
                ) from exc

    def close(self) -> None:
        self._client.close()
