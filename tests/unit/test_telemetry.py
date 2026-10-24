"""configure_telemetry sin collector: los logs igual deben llevar trace_id/span_id.

Corre en un subproceso porque el TracerProvider global solo se puede fijar una vez.
"""

from __future__ import annotations

import json
import subprocess
import sys

SCRIPT = """
import io, json, structlog
from opentelemetry import propagate, trace
from rpa_worker.adapters.observability import configure_logging, configure_telemetry

buf = io.StringIO()
configure_logging(level="INFO", fmt="json", pii_fields=[], service_name="t", stream=buf)
configure_telemetry(service_name="t", otlp_endpoint=None, metrics_interval_seconds=60)

# El productor inyecta traceparent; el worker continúa la misma traza.
with trace.get_tracer("seed").start_as_current_span("enqueue"):
    carrier = {}
    propagate.inject(carrier)
parent = propagate.extract(carrier)
with trace.get_tracer("w").start_as_current_span("process_transaction", context=parent):
    structlog.get_logger("w").info("transaction.status_changed")
print(json.dumps({"carrier": carrier, "line": json.loads(buf.getvalue())}))
"""


def test_logs_carry_real_trace_ids_without_collector() -> None:
    out = subprocess.run(  # noqa: S603 - intérprete actual, script fijo
        [sys.executable, "-c", SCRIPT], capture_output=True, text=True, check=True, timeout=60
    )
    data = json.loads(out.stdout.strip().splitlines()[-1])
    traceparent = data["carrier"]["traceparent"]
    line = data["line"]
    assert line["trace_id"] == traceparent.split("-")[1]  # misma traza que el productor
    assert len(line["span_id"]) == 16
    assert line["trace_id"] != "0" * 32
