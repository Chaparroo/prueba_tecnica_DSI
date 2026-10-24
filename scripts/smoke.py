"""Prueba de humo de punta a punta sobre docker compose (local y CI).

    uv run python scripts/smoke.py           # o: make smoke
    uv run python scripts/smoke.py --keep    # no destruye el stack al terminar

Escenarios:
  1. Arranque: migrate termina, worker sano, /readyz ok, PID 1 = init y el
     proceso del worker es Python del venv (no `uv run`).
  2. Flujo: 1-3 Successful, 101 Failed (not_found), abc Failed (non_numeric_id).
  3. SIGTERM a mitad de item (API falsa lenta): el item termina y se persiste,
     el worker sale con 0 y no toma los siguientes.
  4. `rpa-worker stop` a mitad de item: mismo resultado con origen redis_key; al
     volver a arrancar, la llave anterior se ignora (sin bucle de reinicios).

Usa un proyecto compose propio (rpa-smoke) y contraseñas aleatorias.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "rpa-smoke"
HEALTH_PORT = "18080"
ENV = {
    **os.environ,
    "POSTGRES_DB": "rpa",
    "POSTGRES_USER": "rpa",
    "POSTGRES_PASSWORD": secrets.token_urlsafe(16),
    "REDIS_PASSWORD": secrets.token_urlsafe(16),
    "WORKER_HEALTH_PORT": HEALTH_PORT,
    "FAKE_API_SLOW_IDS": "7,10",
    "FAKE_API_SLOW_SECONDS": "8",
}
COMPOSE = [
    "docker",
    "compose",
    "-p",
    PROJECT,
    "-f",
    "docker-compose.yml",
    "-f",
    "docker-compose.smoke.yml",
]


class SmokeError(AssertionError):
    pass


def sh(*args: str, check: bool = True, timeout: float = 600) -> str:
    proc = subprocess.run(
        [*COMPOSE, *args],
        cwd=ROOT,
        env=ENV,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=timeout,
        check=False,
    )
    if check and proc.returncode != 0:
        raise SmokeError(f"falló: compose {' '.join(args)}\n{proc.stdout}\n{proc.stderr}")
    return proc.stdout


def step(msg: str) -> None:
    print(f"\n==> {msg}", flush=True)


def ok(msg: str) -> None:
    print(f"    OK  {msg}", flush=True)


def wait_until(desc: str, cond: Callable[[], bool], timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(1)
    raise SmokeError(f"timeout esperando: {desc}")


def psql(sql: str) -> list[list[str]]:
    out = sh(
        "exec", "-T", "postgres", "psql", "-U", "rpa", "-d", "rpa", "-tA", "-F", "|", "-c", sql
    )
    return [line.split("|") for line in out.splitlines() if line.strip()]


def statuses() -> dict[str, tuple[str, str]]:
    rows = psql("SELECT reference, status, coalesce(error_reason,'') FROM transaction_results")
    return {r[0]: (r[1], r[2]) for r in rows}


def seed(ids: str) -> dict[str, str]:
    """Devuelve {id: reference}."""
    out = sh("exec", "-T", "worker", "rpa-worker", "seed", "--ids", ids)
    refs = [line.split("\t")[0] for line in out.splitlines() if line.startswith("post-")]
    return {ref.split("-")[1]: ref for ref in refs}


def worker_logs() -> list[dict[str, object]]:
    out = sh("logs", "--no-log-prefix", "worker", check=False)
    lines = []
    for line in out.splitlines():
        try:
            lines.append(json.loads(line))
        except ValueError:
            continue
    return lines


def in_progress(reference: str) -> bool:
    return any(
        e.get("reference") == reference and e.get("to_status") == "InProgress"
        for e in worker_logs()
    )


def worker_container() -> str:
    return sh("ps", "-aq", "worker").strip()


def worker_state() -> tuple[str, int]:
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", worker_container()],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return out[0], int(out[1])


def http_json(path: str) -> tuple[int, dict[str, object]]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HEALTH_PORT}{path}", timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def end_process_origin() -> str | None:
    for e in reversed(worker_logs()):
        if e.get("event") == "runner.state" and e.get("state") == "EndProcess":
            return str(e.get("stop_origin"))
    return None


# ------------------------------------------------------------------ escenarios


def scenario_startup() -> None:
    step("1. Arranque (build, migrate, worker sano)")
    sh("up", "-d", "--build", "--wait", timeout=900)
    ok("compose up --wait: servicios sanos")
    code, body = http_json("/readyz")
    if code != 200 or body.get("status") != "ok":
        raise SmokeError(f"/readyz no está ok: {code} {body}")
    ok(f"/readyz -> {body}")

    top = subprocess.run(
        ["docker", "top", worker_container(), "-o", "pid,ppid,args"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    if "docker-init" not in top or "/app/.venv/bin/rpa-worker run" not in top or " uv " in top:
        raise SmokeError(f"árbol de procesos inesperado:\n{top}")
    ok("PID 1 = docker-init; worker = /app/.venv/bin/rpa-worker run (sin uv)")


def scenario_flow() -> None:
    step("2. Flujo: éxito, 404 de negocio, id no numérico")
    refs = seed("1-3,101,abc")
    wait_until("5 items terminados", lambda: len(statuses()) >= 5)
    st = statuses()
    for i in ("1", "2", "3"):
        if st[refs[i]][0] != "Successful":
            raise SmokeError(f"{i} -> {st[refs[i]]}")
    if st[refs["101"]] != ("Failed", "not_found"):
        raise SmokeError(f"101 -> {st[refs['101']]}")
    if st[refs["abc"]] != ("Failed", "non_numeric_id"):
        raise SmokeError(f"abc -> {st[refs['abc']]}")
    ok("1-3 Successful, 101 Failed/not_found, abc Failed/non_numeric_id")


def scenario_sigterm() -> None:
    step("3. SIGTERM real a mitad de item (API falsa: id 7 tarda 8 s)")
    refs = seed("7,8,9")
    wait_until("item 7 en vuelo", lambda: in_progress(refs["7"]), timeout=30)
    sh("kill", "-s", "SIGTERM", "worker")
    ok("SIGTERM enviado con el item 7 en vuelo")
    wait_until("worker detenido", lambda: worker_state()[0] == "exited", timeout=120)
    _status, code = worker_state()
    if code != 0:
        raise SmokeError(f"exit code {code}, se esperaba 0")
    ok("el worker salió con código 0")
    st = statuses()
    if st.get(refs["7"], ("",))[0] != "Successful":
        raise SmokeError(f"el item en vuelo no se persistió: {st.get(refs['7'])}")
    ok("el item 7 terminó y se persistió (Successful)")
    if refs["8"] in st or refs["9"] in st:
        raise SmokeError("el worker tomó otro item después de SIGTERM")
    ok("8 y 9 siguen en la cola (no se tomaron)")
    origin = end_process_origin()
    if origin != "signal:15":
        raise SmokeError(f"stop_origin={origin}, se esperaba signal:15")
    ok("End Process con stop_origin=signal:15 (la señal llegó a Python)")

    step("   reanudar: 8 y 9 se procesan")
    sh("up", "-d", "--wait", "worker", timeout=300)
    wait_until("8 y 9 terminados", lambda: {refs["8"], refs["9"]} <= set(statuses()), timeout=60)
    ok("8 y 9 Successful tras reiniciar")


def scenario_stop_key() -> None:
    step("4. `rpa-worker stop` a mitad de item (id 10 tarda 8 s)")
    refs = seed("10,11")
    wait_until("item 10 en vuelo", lambda: in_progress(refs["10"]), timeout=30)
    sh("exec", "-T", "worker", "rpa-worker", "stop", "--ttl", "300")
    wait_until("worker detenido", lambda: worker_state()[0] == "exited", timeout=120)
    if worker_state()[1] != 0:
        raise SmokeError(f"exit code {worker_state()[1]}")
    st = statuses()
    if st.get(refs["10"], ("",))[0] != "Successful" or refs["11"] in st:
        raise SmokeError(f"estado inesperado: {st.get(refs['10'])}, 11 tomado={refs['11'] in st}")
    if end_process_origin() != "redis_key":
        raise SmokeError(f"stop_origin={end_process_origin()}")
    ok("item 10 persistido, 11 no tomado, salida 0 con stop_origin=redis_key")

    step("   reinicio con la llave aún vigente: debe ignorarla")
    sh("up", "-d", "--wait", "worker", timeout=300)
    wait_until("item 11 terminado", lambda: refs["11"] in statuses(), timeout=60)
    time.sleep(5)
    if worker_state()[0] != "running":
        raise SmokeError("el worker se detuvo de nuevo: bucle de reinicios")
    if not any(e.get("event") == "queue.stale_should_stop_ignored" for e in worker_logs()):
        raise SmokeError("no se registró el aviso de llave ignorada")
    ok("la llave anterior al arranque se ignora con warning; el worker sigue corriendo")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="No destruir el stack al terminar")
    args = parser.parse_args()
    try:
        scenario_startup()
        scenario_flow()
        scenario_sigterm()
        scenario_stop_key()
        print("\nSMOKE OK", flush=True)
        return 0
    except (SmokeError, subprocess.SubprocessError) as exc:
        print(f"\nSMOKE FALLÓ: {exc}", file=sys.stderr)
        print(sh("logs", "--tail", "80", check=False), file=sys.stderr)
        return 1
    finally:
        if not args.keep:
            sh("down", "-v", "--remove-orphans", check=False)


if __name__ == "__main__":
    raise SystemExit(main())
