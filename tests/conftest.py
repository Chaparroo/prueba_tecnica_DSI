from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from rpa_worker.application.health import HealthState
from rpa_worker.application.process_transaction import ProcessTransaction
from rpa_worker.application.runner import Runner
from rpa_worker.application.settings import ProcessingSettings, RunnerSettings
from rpa_worker.application.should_stop import ShouldStop
from tests.fakes import (
    FakeApi,
    FakeClock,
    FakeMetrics,
    FakeQueue,
    FakeRemoteStop,
    FakeRepository,
    FakeSleeper,
)


@dataclass
class Harness:
    clock: FakeClock = field(default_factory=FakeClock)
    should_stop: ShouldStop = field(default_factory=ShouldStop)
    queue: FakeQueue = field(default_factory=FakeQueue)
    repo: FakeRepository = field(default_factory=FakeRepository)
    api: FakeApi = field(default_factory=FakeApi)
    remote: FakeRemoteStop = field(default_factory=FakeRemoteStop)
    metrics: FakeMetrics = field(default_factory=FakeMetrics)
    processing: ProcessingSettings = field(default_factory=ProcessingSettings)
    runner_settings: RunnerSettings = field(default_factory=RunnerSettings)
    closed: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.sleeper = FakeSleeper(self.clock, self.should_stop)
        self.health = HealthState(self.clock)

    def build(self) -> Runner:
        process = ProcessTransaction(
            api=self.api,
            repository=self.repo,
            queue=self.queue,
            clock=self.clock,
            sleeper=self.sleeper,
            metrics=self.metrics,
            settings=self.processing,
            heartbeat=self.health.beat,
            rand=lambda: 1.0,  # jitter determinista: siempre el techo
        )
        self.runner = Runner(
            queue=self.queue,
            remote_stop=self.remote,
            process=process,
            should_stop=self.should_stop,
            health=self.health,
            clock=self.clock,
            sleeper=self.sleeper,
            metrics=self.metrics,
            settings=self.runner_settings,
            closers=[lambda: self.closed.append("closed")],
        )
        return self.runner

    def restart(self) -> None:
        """Nuevo proceso del worker sobre la misma cola/BD (bandera limpia)."""
        self.should_stop = ShouldStop()
        self.__post_init__()

    def reclaim_until_drained(self) -> None:
        """Al quedar vacía la cola, simula XAUTOCLAIM de pendientes; para al final."""

        def on_empty() -> None:
            if self.queue.pending:
                self.queue.redeliver_pending()
            else:
                self.should_stop.request("test:idle")

        self.queue.on_empty = on_empty

    def stop_when_idle(self) -> None:
        self.queue.on_empty = lambda: self.should_stop.request("test:idle")

    def drain_with_retries(self, step_seconds: float = 4_000) -> None:
        """Al quedar vacía la cola, adelanta el reloj para vencer reintentos; para al final."""

        def on_empty() -> None:
            if self.queue.retry_zset:
                self.clock.advance(step_seconds)
            else:
                self.should_stop.request("test:idle")

        self.queue.on_empty = on_empty

    def run(self, setup: Callable[[Harness], None] | None = None) -> int:
        if setup is not None:
            setup(self)
        return self.build().run()


@pytest.fixture
def h() -> Harness:
    return Harness()
