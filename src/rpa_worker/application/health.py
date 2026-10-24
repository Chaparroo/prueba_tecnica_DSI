"""Estado de salud compartido entre el runner (escribe) y el servidor HTTP (lee)."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import StrEnum

from rpa_worker.application.clock import MonotonicSource


class WorkerState(StrEnum):
    STARTING = "starting"
    IDLE = "idle"
    PROCESSING = "processing"
    DEGRADED = "degraded"
    STOPPING = "stopping"


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    state: WorkerState
    seconds_since_heartbeat: float


class HealthState:
    def __init__(self, clock: MonotonicSource) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._state = WorkerState.STARTING
        self._last_beat = clock.monotonic()

    def beat(self) -> None:
        with self._lock:
            self._last_beat = self._clock.monotonic()

    def set_state(self, state: WorkerState) -> None:
        with self._lock:
            self._state = state
            self._last_beat = self._clock.monotonic()

    def snapshot(self) -> HealthSnapshot:
        with self._lock:
            return HealthSnapshot(self._state, self._clock.monotonic() - self._last_beat)
