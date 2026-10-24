"""Tabla de errores (debe coincidir con el README)."""

from __future__ import annotations

import pytest

from rpa_worker.domain import policies
from rpa_worker.domain.exceptions import BusinessException, SystemException
from rpa_worker.domain.transaction import TransactionStatus


@pytest.mark.parametrize(
    ("status", "kind", "retryable", "reason"),
    [
        (404, "business", None, "not_found"),
        (408, "system", True, "request_timeout"),
        (429, "system", True, "rate_limited"),
        (500, "system", True, "server_error"),
        (502, "system", True, "server_error"),
        (503, "system", True, "server_error"),
        (504, "system", True, "server_error"),
        (400, "system", False, "bad_request"),
        (401, "system", False, "unauthorized"),
        (403, "system", False, "forbidden"),
        (409, "system", False, "unexpected_status"),
        (302, "system", False, "unexpected_status"),
    ],
)
def test_http_status_classification(
    status: int, kind: str, retryable: bool | None, reason: str
) -> None:
    err = policies.classify_http_status(status)
    assert err is not None
    assert err.reason == reason
    if kind == "business":
        assert isinstance(err, BusinessException)
    else:
        assert isinstance(err, SystemException)
        assert err.retryable is retryable
        assert err.requires_alert is (not retryable)


@pytest.mark.parametrize("status", [200, 201, 204])
def test_success_statuses_are_not_errors(status: int) -> None:
    assert policies.classify_http_status(status) is None


def test_429_carries_retry_after_and_is_rate_limited() -> None:
    err = policies.classify_http_status(429, retry_after=7.0)
    assert isinstance(err, SystemException)
    assert err.retry_after == 7.0
    assert err.rate_limited


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({}, "invalid_payload"),
        ({"id": "abc"}, "non_numeric_id"),
        ({"id": "1.5"}, "non_numeric_id"),
        ({"id": 3.0}, "non_numeric_id"),
        ({"id": True}, "non_numeric_id"),
        ({"id": None}, "non_numeric_id"),
        ({"id": "١٢"}, "non_numeric_id"),  # dígitos no ASCII
        ({"id": 0}, "invalid_payload"),
        ({"id": -4}, "invalid_payload"),
    ],
)
def test_invalid_payload_is_business(payload: dict[str, object], reason: str) -> None:
    with pytest.raises(BusinessException) as info:
        policies.parse_post_id(payload)
    assert info.value.reason == reason


@pytest.mark.parametrize(("payload", "expected"), [({"id": 7}, 7), ({"id": "101"}, 101)])
def test_valid_ids(payload: dict[str, object], expected: int) -> None:
    assert policies.parse_post_id(payload) == expected


def test_invalid_api_body_is_non_retryable_system() -> None:
    with pytest.raises(SystemException) as info:
        policies.validate_api_result(["not", "an", "object"])
    assert info.value.retryable is False


def test_retry_after_parsing() -> None:
    now = 1_700_000_000.0
    assert policies.parse_retry_after("12", now) == 12.0
    assert policies.parse_retry_after(None, now) is None
    assert policies.parse_retry_after("garbage", now) is None
    http_date = "Tue, 14 Nov 2023 22:13:50 GMT"  # now + 30 s
    assert policies.parse_retry_after(http_date, now) == pytest.approx(30.0)


def test_backoff_is_exponential_capped_and_jittered() -> None:
    assert [policies.backoff_delay(a, 0.5, 8, lambda: 1.0) for a in range(1, 7)] == [
        0.5,
        1.0,
        2.0,
        4.0,
        8.0,
        8.0,
    ]
    assert policies.backoff_delay(3, 0.5, 8, lambda: 0.25) == 0.5
    assert policies.backoff_delay(3, 0.5, 8, lambda: 0.0) == 0.0


def test_item_retry_delay() -> None:
    assert [policies.item_retry_delay(n, 30, 3600) for n in (1, 2, 3, 10)] == [30, 60, 120, 3600]


@pytest.mark.parametrize(
    ("retry_count", "expected"),
    [
        (0, TransactionStatus.RETRIED),
        (1, TransactionStatus.RETRIED),
        (2, TransactionStatus.ABANDONED),
        (5, TransactionStatus.ABANDONED),
    ],
)
def test_max_retry_number(retry_count: int, expected: TransactionStatus) -> None:
    assert policies.status_after_system_failure(retry_count, max_retry_number=2) is expected
