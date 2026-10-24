from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from rpa_worker.adapters.health_server import HealthServer
from rpa_worker.application.health import HealthState, WorkerState
from tests.fakes import FakeClock


@dataclass
class Env:
    server: HealthServer
    health: HealthState
    clock: FakeClock
    redis_ok: bool = True
    postgres_ok: bool = True


@pytest.fixture
def env() -> Iterator[Env]:
    clock = FakeClock()
    health = HealthState(clock)
    holder: list[Env] = []
    server = HealthServer(
        host="127.0.0.1",
        port=0,
        health=health,
        checks={"redis": lambda: holder[0].redis_ok, "postgres": lambda: holder[0].postgres_ok},
        liveness_max_age_seconds=30,
        check_timeout_seconds=0.3,
    )
    holder.append(Env(server, health, clock))
    server.start()
    yield holder[0]
    server.stop()


def get(server: HealthServer, path: str) -> tuple[int, dict[str, object]]:
    url = f"http://127.0.0.1:{server.port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def test_ready_when_idle_and_dependencies_up(env: Env) -> None:
    env.health.set_state(WorkerState.IDLE)
    code, body = get(env.server, "/readyz")
    assert code == 200
    assert body == {"status": "ok", "state": "idle", "checks": {"redis": "ok", "postgres": "ok"}}


@pytest.mark.parametrize("state", [WorkerState.STARTING, WorkerState.STOPPING])
def test_not_ready_while_starting_or_stopping(env: Env, state: WorkerState) -> None:
    env.health.set_state(state)
    assert get(env.server, "/readyz")[0] == 503


def test_degraded_is_reported_but_ready(env: Env) -> None:
    env.health.set_state(WorkerState.DEGRADED)
    code, body = get(env.server, "/readyz")
    assert (code, body["state"]) == (200, "degraded")


def test_dependency_down_is_not_ready(env: Env) -> None:
    env.health.set_state(WorkerState.IDLE)
    env.postgres_ok = False
    code, body = get(env.server, "/readyz")
    assert code == 503
    assert body["checks"] == {"redis": "ok", "postgres": "fail"}


def test_livez_depends_only_on_heartbeat(env: Env) -> None:
    env.redis_ok = env.postgres_ok = False  # dependencias caídas no afectan liveness
    env.health.beat()
    assert get(env.server, "/livez")[0] == 200
    env.clock.advance(31)
    code, body = get(env.server, "/livez")
    assert code == 503
    assert body["status"] == "fail"


def test_hanging_check_times_out() -> None:
    health = HealthState(FakeClock())
    health.set_state(WorkerState.IDLE)

    def hangs() -> bool:
        time.sleep(3)
        return True

    server = HealthServer(
        host="127.0.0.1",
        port=0,
        health=health,
        checks={"redis": hangs},
        liveness_max_age_seconds=30,
        check_timeout_seconds=0.2,
    )
    server.start()
    try:
        started = time.monotonic()
        code, _ = get(server, "/readyz")
        assert code == 503
        assert time.monotonic() - started < 2
    finally:
        server.stop()
