from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider

from rpa_worker.adapters.http_api import JsonPlaceholderApi
from rpa_worker.domain.exceptions import AlertKind, BusinessException, SystemException

NOW = 1_700_000_000.0


def api(handler: Callable[[httpx.Request], httpx.Response]) -> JsonPlaceholderApi:
    return JsonPlaceholderApi(
        base_url="https://jsonplaceholder.test",
        connect_timeout=1,
        read_timeout=1,
        user_agent="test",
        transport=httpx.MockTransport(handler),
        clock=lambda: NOW,
    )


def test_success_returns_json() -> None:
    client = api(lambda r: httpx.Response(200, json={"id": 1, "title": "t"}))
    assert client.fetch_post(1) == {"id": 1, "title": "t"}


def test_404_is_business() -> None:
    with pytest.raises(BusinessException) as info:
        api(lambda r: httpx.Response(404, json={})).fetch_post(101)
    assert info.value.reason == "not_found"


@pytest.mark.parametrize("status", [500, 502, 503, 504, 408])
def test_retryable_statuses(status: int) -> None:
    with pytest.raises(SystemException) as info:
        api(lambda r: httpx.Response(status)).fetch_post(1)
    assert info.value.retryable


@pytest.mark.parametrize(
    ("status", "alert"), [(400, AlertKind.CONTRACT), (401, AlertKind.AUTH), (403, AlertKind.AUTH)]
)
def test_non_retryable_statuses(status: int, alert: AlertKind) -> None:
    with pytest.raises(SystemException) as info:
        api(lambda r: httpx.Response(status)).fetch_post(1)
    assert not info.value.retryable
    assert info.value.alert is alert


def test_429_retry_after_seconds_and_http_date() -> None:
    with pytest.raises(SystemException) as info:
        api(lambda r: httpx.Response(429, headers={"Retry-After": "7"})).fetch_post(1)
    assert (info.value.rate_limited, info.value.retry_after) == (True, 7.0)

    date = "Tue, 14 Nov 2023 22:14:20 GMT"  # NOW + 60 s
    with pytest.raises(SystemException) as info:
        api(lambda r: httpx.Response(429, headers={"Retry-After": date})).fetch_post(1)
    assert info.value.retry_after == pytest.approx(60.0)


def _raise(exc: Exception) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    return handler


@pytest.mark.parametrize(
    ("exc", "reason"),
    [
        (httpx.ReadTimeout("slow"), "timeout"),
        (httpx.ConnectTimeout("slow"), "timeout"),
        (httpx.ConnectError("refused"), "connection_error"),
        (httpx.RemoteProtocolError("reset"), "connection_error"),
    ],
)
def test_transport_errors_are_retryable(exc: Exception, reason: str) -> None:
    with pytest.raises(SystemException) as info:
        api(_raise(exc)).fetch_post(1)
    assert (info.value.reason, info.value.retryable) == (reason, True)


def test_non_json_body_is_contract_alert() -> None:
    with pytest.raises(SystemException) as info:
        api(lambda r: httpx.Response(200, text="<html>")).fetch_post(1)
    assert (info.value.reason, info.value.retryable) == ("invalid_response", False)
    assert info.value.alert is AlertKind.CONTRACT


def test_traceparent_is_propagated() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, json={"id": 1})

    tracer = TracerProvider().get_tracer("test")
    with tracer.start_as_current_span("parent") as parent:
        api(handler).fetch_post(1)
        trace_id = format(parent.get_span_context().trace_id, "032x")
    assert seen["traceparent"].split("-")[1] == trace_id


def test_redirects_are_not_followed() -> None:
    client = api(lambda r: httpx.Response(302, headers={"Location": "https://evil.test"}))
    with pytest.raises(SystemException) as info:
        client.fetch_post(1)
    assert info.value.reason == "unexpected_status"
