"""Reencolado masivo del DLQ (``rpa-worker drain-dlq``).

Reutiliza ``RequeueFromDlq.requeue_entry`` por cada entrada: auditoría por
item (--operator/--reason) y el XDEL del Lua como puerta contra duplicados.

Garantías:
* Sin bucle infinito: el id máximo del DLQ se fija al empezar y el recorrido
  (XRANGE) se detiene ahí. Un item reencolado que vuelve a fallar durante la
  corrida entra con un id mayor y no se procesa otra vez.
* Seguro ante concurrencia y crash: dos drains a la vez no duplican (puerta
  XDEL); re-ejecutar tras un crash retoma lo que quedó (lo ya restaurado ya no
  está en el DLQ y el paso de Postgres es idempotente por requeue_id).
* Exclusiones por defecto: venenos (max_deliveries) salvo --include-poison;
  ilegibles siempre; items con requeue_count >= 3 salvo --force.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

import structlog

from rpa_worker.application.ports import (
    DeadLetterStore,
    DlqEntry,
    NotReopenableError,
    UnparseableDlqEntry,
)
from rpa_worker.application.requeue import RequeueFromDlq, RequeueStatus
from rpa_worker.domain.exceptions import SystemException

log = structlog.get_logger(__name__)

POISON_REASONS = frozenset({"max_deliveries"})
MAX_REQUEUES_WITHOUT_FORCE = 3


class Skip(StrEnum):
    FILTERED = "filtered"  # no coincide con --dlq-reason/--since/--until
    UNPARSEABLE = "unparseable"  # sin reference: se reporta, nunca se reencola
    POISON = "poison"  # max_deliveries sin --include-poison
    REQUEUE_LIMIT = "requeue_limit"  # requeue_count >= 3 sin --force
    OVER_LIMIT = "over_limit"  # elegible, pero ya se alcanzó --limit


class DrainOutcome(StrEnum):
    OK = "ok"
    PARTIAL = "partial"  # hubo fallos o se abortó, pero algo se reencoló
    ERROR = "error"  # hubo fallos o se abortó y no se reencoló nada


@dataclass(frozen=True, slots=True)
class DrainFilters:
    dlq_reasons: frozenset[str] = frozenset()
    since: float | None = None  # epoch (s) de entrada al DLQ, inclusivo
    until: float | None = None
    include_poison: bool = False
    force: bool = False


@dataclass
class DrainReport:
    executed: bool
    max_entry_id: str | None = None
    scanned: int = 0
    eligible: int = 0
    requeued: int = 0
    already_gone: int = 0  # otro operador u otra corrida lo restauró antes
    by_dlq_reason: Counter[str] = field(default_factory=Counter)
    skipped: Counter[str] = field(default_factory=Counter)
    failed: Counter[str] = field(default_factory=Counter)
    sample: list[str] = field(default_factory=list)
    aborted: str | None = None

    @property
    def outcome(self) -> DrainOutcome:
        if not self.failed and self.aborted is None:
            return DrainOutcome.OK
        return DrainOutcome.PARTIAL if self.requeued else DrainOutcome.ERROR


class DrainDlq:
    def __init__(
        self,
        *,
        dlq: DeadLetterStore,
        requeue: RequeueFromDlq,
        sleep: Callable[[float], bool],
        page_size: int = 100,
    ) -> None:
        self._dlq = dlq
        self._requeue = requeue
        self._sleep = sleep  # devuelve True si se interrumpió (Ctrl+C)
        self._page_size = page_size

    def run(
        self,
        filters: DrainFilters,
        *,
        execute: bool,
        operator: str = "",
        reason: str = "",
        limit: int | None = None,
        batch_size: int = 50,
        pause_seconds: float = 1.0,
        sample_size: int = 10,
    ) -> DrainReport:
        report = DrainReport(executed=execute)
        report.max_entry_id = self._dlq.last_entry_id()
        if report.max_entry_id is None:
            return report
        log.info(
            "dlq.drain_started",
            executed=execute,
            max_entry_id=report.max_entry_id,
            operator=operator or None,
            limit=limit,
        )

        cursor: str | None = None
        in_batch = 0
        while True:
            page = self._dlq.scan(after=cursor, until=report.max_entry_id, count=self._page_size)
            if not page:
                break
            for entry in page:
                cursor = entry.entry_id
                report.scanned += 1
                report.by_dlq_reason[entry.reason] += 1
                skip = self._classify(entry, filters)
                if skip is not None or not isinstance(entry, DlqEntry):
                    report.skipped[skip or Skip.UNPARSEABLE] += 1
                    continue
                report.eligible += 1
                if not execute:
                    if len(report.sample) < sample_size:
                        report.sample.append(entry.transaction.reference)
                    continue
                if limit is not None and report.requeued >= limit:
                    report.skipped[Skip.OVER_LIMIT] += 1
                    continue
                try:
                    result = self._requeue.requeue_entry(entry, operator=operator, reason=reason)
                except NotReopenableError as exc:
                    report.failed["not_reopenable"] += 1
                    log.warning(
                        "dlq.drain_not_reopenable", reference=exc.reference, status=str(exc.status)
                    )
                    continue
                except SystemException as exc:
                    # Infraestructura caída: abortar. Re-ejecutar retoma desde aquí.
                    report.failed["infra"] += 1
                    report.aborted = exc.reason
                    log.error(
                        "dlq.drain_aborted", reason=exc.reason, error=str(exc), at_entry=cursor
                    )
                    return self._finish(report)
                if result.status is RequeueStatus.REQUEUED:
                    report.requeued += 1
                    in_batch += 1
                    if len(report.sample) < sample_size:
                        report.sample.append(entry.transaction.reference)
                else:
                    report.already_gone += 1
                if in_batch >= batch_size:
                    in_batch = 0
                    if self._sleep(pause_seconds):  # ritmo: no inundar la cola
                        report.aborted = "interrupted"
                        return self._finish(report)
        return self._finish(report)

    @staticmethod
    def _classify(entry: DlqEntry | UnparseableDlqEntry, f: DrainFilters) -> Skip | None:
        if isinstance(entry, UnparseableDlqEntry):
            return Skip.UNPARSEABLE
        if f.dlq_reasons and entry.reason not in f.dlq_reasons:
            return Skip.FILTERED
        if entry.failed_at is not None and (
            (f.since is not None and entry.failed_at < f.since)
            or (f.until is not None and entry.failed_at > f.until)
        ):
            return Skip.FILTERED
        if entry.reason in POISON_REASONS and not f.include_poison:
            return Skip.POISON
        if entry.transaction.requeue_count >= MAX_REQUEUES_WITHOUT_FORCE and not f.force:
            return Skip.REQUEUE_LIMIT
        return None

    @staticmethod
    def _finish(report: DrainReport) -> DrainReport:
        log.info(
            "dlq.drain_finished",
            executed=report.executed,
            outcome=str(report.outcome),
            scanned=report.scanned,
            requeued=report.requeued,
            already_gone=report.already_gone,
            skipped=dict(report.skipped),
            failed=dict(report.failed),
        )
        return report
