"""Punto de entrada de línea de comandos.

    rpa-worker run                      worker (Init -> ... -> End Process)
    rpa-worker check-config             valida YAML + entorno (y que Redis no sea Cluster)
    rpa-worker seed --ids 1-10,101,abc  encola items de prueba
    rpa-worker requeue --reference R --operator O --reason "..."
    rpa-worker drain-dlq [...]          dry-run por defecto; --execute para reencolar
    rpa-worker stop --ttl 600           parada suave de los workers en ejecución
    rpa-worker stop --clear             cancela la parada suave

Códigos de salida: 0 ok, 2 configuración/uso inválido, 3 item no reabrible
(Successful/Failed), 4 no está en el DLQ, 5 infraestructura no disponible (o
drain sin progreso), 6 éxito parcial (drain con fallos tras reencolar algo).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from rpa_worker.config import ConfigError, load_settings

EXIT_CONFIG = 2
EXIT_NOT_REOPENABLE = 3
EXIT_NOT_IN_DLQ = 4
EXIT_INFRA = 5
EXIT_PARTIAL = 6


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


def parse_when(value: str) -> float:
    """ISO 8601 (sin zona = UTC) -> epoch."""
    try:
        when = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"fecha inválida (ISO 8601): {value}") from exc
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return when.timestamp()


def _check_config(args: argparse.Namespace) -> int:
    from rpa_worker.bootstrap import check_redis_mode

    settings = load_settings(args.config)
    cfg = settings.config
    cluster = check_redis_mode(settings)
    if cluster:
        print(
            "ERROR: Redis está en modo Cluster. No es compatible: los scripts Lua mueven "
            "items entre llaves de distintos hash slots. Usa Redis standalone con AOF.",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    redis_note = "standalone" if cluster is False else "NO VERIFICADO (Redis inalcanzable)"
    print(
        "config OK | "
        f"peor caso por item={cfg.worst_case_item_seconds():.1f}s "
        f"stop_grace={cfg.worker.stop_grace_seconds}s "
        f"lease={cfg.queue.item_lease_ms / 1000:.0f}s "
        f"redis={settings.secrets.redis_url_for_logs()} ({redis_note})"
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


def _drain_dlq(args: argparse.Namespace) -> int:
    from rpa_worker.application.drain_dlq import DrainFilters, DrainOutcome, DrainReport
    from rpa_worker.bootstrap import drain_dlq
    from rpa_worker.domain.exceptions import SystemException

    if args.execute:
        if not (args.operator and args.reason):
            print("ERROR: --execute exige --operator y --reason", file=sys.stderr)
            return EXIT_CONFIG
        if args.limit is None and not args.all:
            print("ERROR: --execute exige --limit N o --all explícito", file=sys.stderr)
            return EXIT_CONFIG
    settings = load_settings(args.config)
    filters = DrainFilters(
        dlq_reasons=frozenset(args.dlq_reason or ()),
        since=args.since,
        until=args.until,
        include_poison=args.include_poison,
        force=args.force,
    )
    try:
        report: DrainReport = drain_dlq(
            settings,
            filters,
            execute=args.execute,
            operator=args.operator or "",
            reason=args.reason or "",
            limit=args.limit,
            batch_size=args.batch_size,
            pause_seconds=args.pause,
            sample_size=args.sample,
        )
    except SystemException as exc:
        print(f"ERROR de infraestructura: {exc}", file=sys.stderr)
        return EXIT_INFRA

    mode = "EJECUCIÓN" if report.executed else "DRY-RUN (nada cambió; usa --execute)"
    print(f"drain-dlq {mode} | tope={report.max_entry_id} revisadas={report.scanned}")
    print("\nPor motivo del DLQ:")
    for name, count in sorted(report.by_dlq_reason.items()):
        print(f"  {name:<24}{count:>6}")
    print("\nPor tipo:")
    print(f"  {'elegibles':<24}{report.eligible:>6}")
    for name, count in sorted(report.skipped.items()):
        print(f"  {'saltados/' + name:<24}{count:>6}")
    if report.executed:
        print("\nResultado:")
        print(f"  {'reencolados':<24}{report.requeued:>6}")
        print(f"  {'ya desaparecidos':<24}{report.already_gone:>6}")
        for name, count in sorted(report.failed.items()):
            print(f"  {'fallidos/' + name:<24}{count:>6}")
        if report.aborted:
            print(f"\nABORTADO: {report.aborted} (re-ejecuta para continuar)")
    if report.sample:
        print("\nMuestra de references:", ", ".join(report.sample))

    return {
        DrainOutcome.OK: 0,
        DrainOutcome.PARTIAL: EXIT_PARTIAL,
        DrainOutcome.ERROR: EXIT_INFRA,
    }[report.outcome]


def _stop(args: argparse.Namespace) -> int:
    from rpa_worker.bootstrap import request_stop
    from rpa_worker.domain.exceptions import SystemException

    if not args.clear and args.ttl is None:
        print("ERROR: --ttl es obligatorio (o --clear para cancelar)", file=sys.stderr)
        return EXIT_CONFIG
    settings = load_settings(args.config)
    try:
        result = request_stop(settings, ttl_seconds=args.ttl, clear=args.clear)
    except SystemException as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_INFRA
    if args.clear:
        print("parada suave cancelada" if result else "no había parada suave activa")
    else:
        print(
            f"parada suave solicitada (ttl={args.ttl}s). Los workers en ejecución terminan su "
            "item y salen con código 0; los que arranquen después la ignoran."
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rpa-worker")
    parser.add_argument(
        "--config", type=Path, default=None, help="Ruta al YAML (por defecto $RPA_CONFIG_PATH)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config", help="Valida config + entorno + modo de Redis y sale")
    sub.add_parser("run", help="Ejecuta el worker")

    seed = sub.add_parser("seed", help="Encola items de prueba")
    seed.add_argument("--ids", default="1-10", help="Lista/rangos: 1-10,101,abc")
    seed.add_argument("--priority", default="normal")

    rq = sub.add_parser("requeue", help="Reabre un item Abandoned desde el DLQ")
    rq.add_argument("--reference", required=True)
    rq.add_argument("--operator", required=True, help="Quién ejecuta (queda en la auditoría)")
    rq.add_argument("--reason", required=True, help="Por qué (queda en la auditoría)")

    dr = sub.add_parser(
        "drain-dlq", help="Reencola en masa desde el DLQ (dry-run por defecto; lista el DLQ)"
    )
    dr.add_argument("--execute", action="store_true", help="Reencola de verdad")
    dr.add_argument("--operator", help="Quién ejecuta (obligatorio con --execute)")
    dr.add_argument("--reason", help="Justificación de auditoría (obligatoria con --execute)")
    dr.add_argument("--dlq-reason", action="append", help="Filtra por motivo del DLQ (repetible)")
    dr.add_argument("--since", type=parse_when, help="Entradas que llegaron desde (ISO 8601)")
    dr.add_argument("--until", type=parse_when, help="Entradas que llegaron hasta (ISO 8601)")
    dr.add_argument(
        "--include-poison", action="store_true", help="Incluye max_deliveries (peligroso)"
    )
    dr.add_argument("--force", action="store_true", help="Incluye items con requeue_count >= 3")
    limit = dr.add_mutually_exclusive_group()
    limit.add_argument("--limit", type=int, help="Máximo de items a reencolar")
    limit.add_argument("--all", action="store_true", help="Sin límite (explícito)")
    dr.add_argument("--batch-size", type=int, default=50, help="Items por lote")
    dr.add_argument("--pause", type=float, default=1.0, help="Segundos de pausa entre lotes")
    dr.add_argument("--sample", type=int, default=10, help="Tamaño de la muestra de references")

    st = sub.add_parser("stop", help="Parada suave de los workers en ejecución (llave en Redis)")
    st.add_argument("--ttl", type=int, help="Segundos de vida de la llave (obligatorio)")
    st.add_argument("--clear", action="store_true", help="Borra la llave")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {
        "check-config": _check_config,
        "run": _run,
        "seed": _seed,
        "requeue": _requeue,
        "drain-dlq": _drain_dlq,
        "stop": _stop,
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
