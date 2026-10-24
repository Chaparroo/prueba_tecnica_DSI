"""Punto de entrada de línea de comandos.

    rpa-worker run                      worker (Init -> ... -> End Process)
    rpa-worker check-config             valida YAML + entorno y sale
    rpa-worker seed --ids 1-10,101,abc  encola items de prueba
    rpa-worker requeue --reference R --operator O --reason "..."

Códigos de salida: 0 ok, 2 configuración inválida, 3 item no reabrible
(Successful/Failed), 4 no está en el DLQ, 5 infraestructura no disponible.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from rpa_worker.config import ConfigError, load_settings

EXIT_CONFIG = 2
EXIT_NOT_REOPENABLE = 3
EXIT_NOT_IN_DLQ = 4
EXIT_INFRA = 5


def parse_ids(spec: str) -> list[str]:
    """'1-3,101,abc' -> ['1','2','3','101','abc'] (los no numéricos son casos de negocio)."""
    ids: list[str] = []
    for part in (p.strip() for p in spec.split(",") if p.strip()):
        lo, sep, hi = part.partition("-")
        if sep and lo.isdigit() and hi.isdigit():
            ids.extend(str(i) for i in range(int(lo), int(hi) + 1))
        else:
            ids.append(part)
    return ids


def _check_config(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    cfg = settings.config
    print(
        "config OK | "
        f"peor caso por item={cfg.worst_case_item_seconds():.1f}s "
        f"stop_grace={cfg.worker.stop_grace_seconds}s "
        f"lease={cfg.queue.item_lease_ms / 1000:.0f}s "
        f"redis={settings.secrets.redis_url_for_logs()}"
    )
    return 0


def _run(args: argparse.Namespace) -> int:
    from rpa_worker.bootstrap import run_worker

    return run_worker(load_settings(args.config))


def _seed(args: argparse.Namespace) -> int:
    from rpa_worker.bootstrap import seed
    from rpa_worker.domain.exceptions import SystemException

    settings = load_settings(args.config)
    if args.priority not in settings.config.queue.priorities:
        print(f"prioridad desconocida: {args.priority}", file=sys.stderr)
        return EXIT_CONFIG
    try:
        published = seed(settings, parse_ids(args.ids), priority=args.priority)
    except SystemException as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_INFRA
    for tx in published:
        print(f"{tx.reference}\tcorrelation_id={tx.correlation_id}")
    return 0


def _requeue(args: argparse.Namespace) -> int:
    from rpa_worker.application.ports import NotReopenableError
    from rpa_worker.application.requeue import NotInDlqError
    from rpa_worker.bootstrap import requeue
    from rpa_worker.domain.exceptions import SystemException

    settings = load_settings(args.config)
    try:
        result = requeue(settings, args.reference, operator=args.operator, reason=args.reason)
    except NotReopenableError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_NOT_REOPENABLE
    except NotInDlqError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_NOT_IN_DLQ
    except SystemException as exc:
        print(f"ERROR de infraestructura (re-ejecuta el comando): {exc}", file=sys.stderr)
        return EXIT_INFRA
    print(
        f"{result.status}: reference={result.reference} "
        f"requeue_id={result.requeue_id} requeue_count={result.requeue_count}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rpa-worker")
    parser.add_argument(
        "--config", type=Path, default=None, help="Ruta al YAML (por defecto $RPA_CONFIG_PATH)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config", help="Valida config + variables de entorno y sale")
    sub.add_parser("run", help="Ejecuta el worker")

    seed = sub.add_parser("seed", help="Encola items de prueba")
    seed.add_argument("--ids", default="1-10", help="Lista/rangos: 1-10,101,abc")
    seed.add_argument("--priority", default="normal")

    rq = sub.add_parser("requeue", help="Reabre un item Abandoned desde el DLQ")
    rq.add_argument("--reference", required=True)
    rq.add_argument("--operator", required=True, help="Quién ejecuta (queda en la auditoría)")
    rq.add_argument("--reason", required=True, help="Por qué (queda en la auditoría)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {
        "check-config": _check_config,
        "run": _run,
        "seed": _seed,
        "requeue": _requeue,
    }
    try:
        return handlers[args.command](args)
    except ConfigError as exc:
        print(f"ERROR de configuración: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    except ValueError as exc:  # p. ej. --operator vacío
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
