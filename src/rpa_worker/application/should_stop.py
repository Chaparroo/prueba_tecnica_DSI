"""ShouldStop: una única bandera con dos orígenes (señal del SO y llave en Redis)."""

from __future__ import annotations

import signal
import threading
from types import FrameType


class ShouldStop:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._origin: str | None = None

    def request(self, origin: str) -> None:
        """Activa la bandera. Seguro de llamar desde un signal handler: no loguea
        ni toma locks propios; solo asigna y hace ``Event.set``."""
        if self._origin is None:
            self._origin = origin
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    @property
    def origin(self) -> str | None:
        return self._origin

    def wait(self, timeout: float) -> bool:
        """True si la bandera se activó antes de agotar el timeout."""
        return self._event.wait(timeout)


class InterruptibleSleeper:
    """Sleeper real: ``event.wait(timeout)``, se despierta en cuanto llega ShouldStop."""

    def __init__(self, should_stop: ShouldStop) -> None:
        self._should_stop = should_stop

    def sleep(self, seconds: float) -> bool:
        if seconds <= 0:
            return self._should_stop.is_set()
        return self._should_stop.wait(seconds)


def install_signal_handlers(should_stop: ShouldStop) -> None:
    """SIGTERM/SIGINT solo activan la bandera. Debe llamarse desde el hilo principal."""

    def _handler(signum: int, _frame: FrameType | None) -> None:
        should_stop.request(f"signal:{signum}")

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
