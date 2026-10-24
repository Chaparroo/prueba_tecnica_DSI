"""Servidor HTTP mínimo de salud (stdlib, hilo daemon).

/livez   200 si el loop latió hace menos de ``liveness_max_age``; 503 si no.
         Late también en idle, cooldown y pausas. No mira dependencias.
/readyz  200 si Redis y Postgres responden y el worker no está arrancando ni
         deteniéndose. Reporta el estado: starting|idle|processing|degraded|stopping.
         ``degraded`` (circuito abierto o pausa por 429) responde 200 con el
         estado en el cuerpo: el proceso está sano, solo no toma items.
La API externa no cuenta para ninguno de los dos.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from rpa_worker.application.health import HealthState, WorkerState

Check = Callable[[], bool]

NOT_READY_STATES = {WorkerState.STARTING, WorkerState.STOPPING}


class HealthServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        health: HealthState,
        checks: Mapping[str, Check],
        liveness_max_age_seconds: float,
        check_timeout_seconds: float = 2.0,
    ) -> None:
        self._health = health
        self._checks = checks
        self._max_age = liveness_max_age_seconds
        self._check_timeout = check_timeout_seconds
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="health-check")
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/livez":
                    code, body = server.livez()
                elif self.path == "/readyz":
                    code, body = server.readyz()
                else:
                    code, body = 404, {"error": "not found"}
                payload = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: object) -> None:
                pass  # sin ruido: los sondeos llegan cada pocos segundos

        self._httpd = ThreadingHTTPServer((host, port), Handler)
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="health-server", daemon=True
        )

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def livez(self) -> tuple[int, dict[str, object]]:
        snap = self._health.snapshot()
        alive = snap.seconds_since_heartbeat <= self._max_age
        return (200 if alive else 503), {
            "status": "ok" if alive else "fail",
            "state": str(snap.state),
            "seconds_since_heartbeat": round(snap.seconds_since_heartbeat, 3),
        }

    def readyz(self) -> tuple[int, dict[str, object]]:
        snap = self._health.snapshot()
        results = {name: self._run_check(check) for name, check in self._checks.items()}
        ready = all(results.values()) and snap.state not in NOT_READY_STATES
        return (200 if ready else 503), {
            "status": "ok" if ready else "fail",
            "state": str(snap.state),
            "checks": {name: "ok" if ok else "fail" for name, ok in results.items()},
        }

    def _run_check(self, check: Check) -> bool:
        # Timeout duro: un Redis/Postgres colgado no debe colgar el sondeo.
        future = self._executor.submit(check)
        try:
            return bool(future.result(timeout=self._check_timeout))
        except FutureTimeout:
            return False
        except Exception:  # noqa: BLE001 - un check que lanza es un check fallido
            return False
