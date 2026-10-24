from __future__ import annotations

from dataclasses import replace

from rpa_worker.application.drain_dlq import DrainDlq, DrainFilters, DrainOutcome, Skip
from rpa_worker.application.ports import DlqEntry, TransactionRecord, UnparseableDlqEntry
from rpa_worker.application.requeue import RequeueFromDlq
from rpa_worker.domain.transaction import TransactionStatus as S
from tests.conftest import Harness
from tests.fakes import Row, make_tx

T0 = 1_700_000_000.0


def add(
    h: Harness, ref: str, reason: str = "server_error", *, requeues: int = 0, at: float = T0
) -> None:
    tx = replace(make_tx(ref, 1), requeue_count=requeues)
    h.queue.add_dlq(DlqEntry(h.queue._next_dlq_id(), tx, reason, "err", at))


def drain(h: Harness, pauses: list[float] | None = None) -> DrainDlq:
    def sleep(seconds: float) -> bool:
        if pauses is not None:
            pauses.append(seconds)
        return False

    requeue = RequeueFromDlq(dlq=h.queue, repository=h.repo, clock=h.clock)
    return DrainDlq(dlq=h.queue, requeue=requeue, sleep=sleep, page_size=3)


def queued_refs(h: Harness) -> list[str]:
    return [d.transaction.reference for s in h.queue.streams.values() for d in s]


def mixed_dlq(h: Harness) -> None:
    add(h, "A", "unauthorized")
    add(h, "B", "server_error")
    add(h, "C", "max_deliveries")
    add(h, "D", "server_error", requeues=3)
    h.queue.add_dlq(UnparseableDlqEntry(h.queue._next_dlq_id(), "unparseable"))
    add(h, "E", "unauthorized")


def test_dry_run_reports_counts_and_changes_nothing(h: Harness) -> None:
    mixed_dlq(h)
    report = drain(h).run(DrainFilters(), execute=False)

    assert report.scanned == 6
    assert report.eligible == 3
    assert report.by_dlq_reason == {
        "unauthorized": 2,
        "server_error": 2,
        "max_deliveries": 1,
        "unparseable": 1,
    }
    assert report.skipped == {Skip.POISON: 1, Skip.REQUEUE_LIMIT: 1, Skip.UNPARSEABLE: 1}
    assert report.sample == ["A", "B", "E"]
    assert report.requeued == 0
    assert len(h.queue.dlq_all) == 6
    assert queued_refs(h) == []
    assert h.repo.audit == []


def test_execute_requeues_eligible_with_audit_per_item(h: Harness) -> None:
    mixed_dlq(h)
    report = drain(h).run(DrainFilters(), execute=True, operator="ana", reason="token rotado")

    assert report.outcome is DrainOutcome.OK
    assert report.requeued == 3
    assert sorted(queued_refs(h)) == ["A", "B", "E"]
    assert [a.reference for a in h.repo.audit] == ["A", "B", "E"]
    assert all(a.operator == "ana" and a.reason == "token rotado" for a in h.repo.audit)
    # Lo excluido sigue en el DLQ.
    left = {e.entry_id for e in h.queue.dlq_all}
    assert len(left) == 3


def test_include_poison_and_force_lift_exclusions_but_never_unparseable(h: Harness) -> None:
    mixed_dlq(h)
    report = drain(h).run(
        DrainFilters(include_poison=True, force=True), execute=True, operator="a", reason="r"
    )
    assert report.requeued == 5
    assert report.skipped == {Skip.UNPARSEABLE: 1}
    assert len(h.queue.dlq_all) == 1  # solo el ilegible


def test_filters_by_dlq_reason_and_time_window(h: Harness) -> None:
    add(h, "OLD", "unauthorized", at=T0 - 7200)
    add(h, "NEW", "unauthorized", at=T0)
    add(h, "OTHER", "server_error", at=T0)
    report = drain(h).run(
        DrainFilters(dlq_reasons=frozenset({"unauthorized"}), since=T0 - 3600),
        execute=True,
        operator="a",
        reason="r",
    )
    assert queued_refs(h) == ["NEW"]
    assert report.skipped == {Skip.FILTERED: 2}


def test_limit_and_batch_pause(h: Harness) -> None:
    for i in range(7):
        add(h, f"R{i}")
    pauses: list[float] = []
    report = drain(h, pauses).run(
        DrainFilters(),
        execute=True,
        operator="a",
        reason="r",
        limit=5,
        batch_size=2,
        pause_seconds=0.5,
    )
    assert report.requeued == 5
    assert report.skipped == {Skip.OVER_LIMIT: 2}
    assert pauses == [0.5, 0.5]  # tras cada lote de 2
    assert len(h.queue.dlq_all) == 2


def test_items_that_fail_again_during_the_run_are_not_reprocessed(h: Harness) -> None:
    for i in range(4):
        add(h, f"R{i}")

    def fails_again_immediately(entry: DlqEntry) -> None:
        # El item reencolado vuelve al DLQ durante la corrida: id mayor que el tope.
        add(h, entry.transaction.reference, "server_error")

    h.queue.on_restore = fails_again_immediately
    report = drain(h).run(DrainFilters(), execute=True, operator="a", reason="r")
    assert report.scanned == 4
    assert report.requeued == 4
    assert len(h.queue.dlq_all) == 4  # las 4 entradas nuevas quedan para otra corrida


def test_crash_mid_drain_then_rerun_finishes_without_duplicates_or_gaps(h: Harness) -> None:
    for i in range(6):
        add(h, f"R{i}")
    calls = {"n": 0}

    def crash_on_third(_entry: DlqEntry) -> None:
        calls["n"] += 1
        if calls["n"] == 3:
            h.queue.fail_next_redis_op = True  # muere entre Postgres y Redis

    h.queue.on_restore = crash_on_third
    first = drain(h).run(DrainFilters(), execute=True, operator="a", reason="r")
    assert first.outcome is DrainOutcome.PARTIAL
    assert first.requeued == 2
    assert first.aborted == "redis_unavailable"

    h.queue.on_restore = None
    second = drain(h).run(DrainFilters(), execute=True, operator="a", reason="r")
    assert second.outcome is DrainOutcome.OK
    assert second.requeued == 4
    refs = queued_refs(h)
    assert sorted(refs) == [f"R{i}" for i in range(6)]  # ni duplicados ni huecos
    assert len(h.repo.audit) == 6  # el R2 a medias no duplica auditoría
    assert h.queue.dlq_all == []


def test_not_reopenable_items_make_the_run_partial(h: Harness) -> None:
    add(h, "DONE")
    add(h, "OPEN")
    h.repo.rows["DONE"] = Row(
        TransactionRecord("DONE", S.SUCCESSFUL, "normal", 0, "c", result={"id": 1})
    )
    report = drain(h).run(DrainFilters(), execute=True, operator="a", reason="r")
    assert report.outcome is DrainOutcome.PARTIAL
    assert report.failed == {"not_reopenable": 1}
    assert queued_refs(h) == ["OPEN"]


def test_empty_dlq(h: Harness) -> None:
    report = drain(h).run(DrainFilters(), execute=True, operator="a", reason="r")
    assert (report.outcome, report.scanned, report.max_entry_id) == (DrainOutcome.OK, 0, None)
