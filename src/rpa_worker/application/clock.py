"""Reloj del sistema (implementación real del puerto Clock)."""

from __future__ import annotations

import time
from typing import Protocol


class MonotonicSource(Protocol):
    def monotonic(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()
