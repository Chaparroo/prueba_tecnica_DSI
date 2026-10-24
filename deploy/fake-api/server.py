"""API falsa compatible con jsonplaceholder para la prueba de humo y CI.

Sin internet y con latencia controlada, para dejar un item en vuelo el tiempo
necesario y enviarle SIGTERM a mitad.

    GET /posts/{id}   1..100 -> 200 JSON ; >100 -> 404 (igual que jsonplaceholder)
    GET /healthz      200

Variables:
    FAKE_API_LATENCY_SECONDS   latencia base de cada respuesta (default 0)
    FAKE_API_SLOW_IDS          ids con latencia extra, p. ej. "7,10"
    FAKE_API_SLOW_SECONDS      latencia de esos ids (default 8)
"""

from __future__ import annotations

import json
import os
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE_LATENCY = float(os.environ.get("FAKE_API_LATENCY_SECONDS", "0"))
SLOW_IDS = {int(i) for i in os.environ.get("FAKE_API_SLOW_IDS", "").split(",") if i.strip()}
SLOW_SECONDS = float(os.environ.get("FAKE_API_SLOW_SECONDS", "8"))
POST_PATH = re.compile(r"^/posts/(\d+)$")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send(200, {"status": "ok"})
            return
        match = POST_PATH.match(self.path)
        if not match:
            self._send(404, {})
            return
        post_id = int(match.group(1))
        delay = SLOW_SECONDS if post_id in SLOW_IDS else BASE_LATENCY
        print(f"request post_id={post_id} delay={delay}", flush=True)
        time.sleep(delay)
        if post_id > 100:
            self._send(404, {})
            return
        self._send(200, {"userId": 1, "id": post_id, "title": f"post {post_id}", "body": "..."})

    def _send(self, status: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
