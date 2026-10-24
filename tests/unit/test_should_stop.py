from __future__ import annotations

import signal
import threading
import time
from collections.abc import Iterator

import pytest

from rpa_worker.application.should_stop import (
    InterruptibleSleeper,
    ShouldStop,
    install_signal_handlers,
)


@pytest.fixture
def restore_signals() -> Iterator[None]:
    saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    yield
    for s, handler in saved.items():
        signal.signal(s, handler)


@pytest.mark.usefixtures("restore_signals")
def test_signal_handler_only_sets_the_flag() -> None:
    stop = ShouldStop()
    install_signal_handlers(stop)
    signal.raise_signal(signal.SIGINT)
    assert stop.is_set()
    assert stop.origin == f"signal:{int(signal.SIGINT)}"


def test_first_origin_wins() -> None:
    stop = ShouldStop()
    stop.request("redis_key")
    stop.request("signal:15")
    assert stop.origin == "redis_key"


def test_sleeper_wakes_up_as_soon_as_flag_is_set() -> None:
    stop = ShouldStop()
    sleeper = InterruptibleSleeper(stop)
    threading.Timer(0.05, stop.request, args=("test",)).start()
    started = time.monotonic()
    assert sleeper.sleep(30) is True
    assert time.monotonic() - started < 5


def test_sleeper_returns_false_when_not_interrupted() -> None:
    assert InterruptibleSleeper(ShouldStop()).sleep(0.01) is False
