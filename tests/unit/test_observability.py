from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator

import pytest
import structlog
from opentelemetry.sdk.trace import TracerProvider

from rpa_worker.adapters.observability import REDACTED, configure_logging, scrub
from rpa_worker.config import load_settings
from tests.unit.test_config import ENV, EXAMPLE

PII = ["email", "name", "username", "phone", "address", "password", "token"]


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    buf = io.StringIO()
    configure_logging(level="INFO", fmt="json", pii_fields=PII, service_name="rpa-test", stream=buf)
    yield buf
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    logging.getLogger().handlers = []


def lines(buf: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buf.getvalue().splitlines() if line.strip()]


def test_every_line_has_correlation_trace_and_span_ids(stream: io.StringIO) -> None:
    tracer = TracerProvider().get_tracer("test")
    log = structlog.get_logger("t")
    with (
        structlog.contextvars.bound_contextvars(reference="REF-1", correlation_id="corr-1"),
        tracer.start_as_current_span("process_transaction") as span,
    ):
        log.info("transaction.status_changed", to_status="Successful")
        ctx = span.get_span_context()
    (line,) = lines(stream)
    assert line["reference"] == "REF-1"
    assert line["correlation_id"] == "corr-1"
    assert line["trace_id"] == format(ctx.trace_id, "032x")
    assert line["span_id"] == format(ctx.span_id, "016x")
    assert line["service"] == "rpa-test"
    assert line["level"] == "info"


def test_pii_fields_are_redacted_recursively(stream: io.StringIO) -> None:
    structlog.get_logger("t").info(
        "user.seen",
        email="ana@example.com",
        nested={"Phone": "555-1234", "ok": 1},
        items=[{"token": "abc"}],
        note="escribir a ana@example.com",
    )
    (line,) = lines(stream)
    assert line["email"] == REDACTED
    assert line["nested"] == {"Phone": REDACTED, "ok": 1}
    assert line["items"] == [{"token": REDACTED}]
    assert line["note"] == f"escribir a {REDACTED}"


def test_secrets_never_reach_the_logs(stream: io.StringIO) -> None:
    env = {**ENV, "REDIS_URL": "redis://default:r3dis-pw@redis:6379/0"}
    settings = load_settings(EXAMPLE, env)
    log = structlog.get_logger("t")
    log.info("worker.starting", redis=settings.secrets.redis_url_for_logs(), settings=settings)
    log.error("boom", error=f"cannot connect {settings.secrets.redis_url.get_secret_value()}")
    log.error("pg", error=settings.secrets.postgres.conninfo())
    logging.getLogger("psycopg").warning("conn redis://u:r3dis-pw@h/0 failed")
    text = stream.getvalue()
    assert "r3dis-pw" not in text
    assert "s3cr" not in text
    assert "redis://***@redis:6379/0" in text


def test_scrub_patterns() -> None:
    assert scrub("postgresql://rpa:pw@db:5432/rpa") == "postgresql://***@db:5432/rpa"
    assert scrub("host=db password='a b' user=x") == "host=db password=*** user=x"
    assert scrub("no secrets here") == "no secrets here"
