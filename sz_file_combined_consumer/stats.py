"""Process-global counters, the ``Combined stats:`` line, the redo-backlog EWMA
slope and the redo-floor guard.

Counters are process-global (one process = one Senzing environment), shared by
both run paths (file loader and pure redoer). :func:`reset_all` exists for
tests that run several drivers in one interpreter.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from enum import Enum


class Counter:
    """Lock-protected integer counter (``+=`` on an attribute is not atomic)."""

    __slots__ = ("_lock", "_v")

    def __init__(self) -> None:
        self._v = 0
        self._lock = threading.Lock()

    def add(self, n: int = 1) -> int:
        with self._lock:
            self._v += n
            return self._v

    def sub(self, n: int = 1) -> int:
        return self.add(-n)

    @property
    def value(self) -> int:
        return self._v

    def reset(self) -> None:
        with self._lock:
            self._v = 0


class Flag:
    """Boolean flag with the initial value remembered for :meth:`reset`."""

    __slots__ = ("_ev", "_initial")

    def __init__(self, initial: bool) -> None:
        self._initial = initial
        self._ev = threading.Event()
        if initial:
            self._ev.set()

    def set(self) -> None:
        self._ev.set()

    def clear(self) -> None:
        self._ev.clear()

    def is_set(self) -> bool:
        return self._ev.is_set()

    def reset(self) -> None:
        self._ev.set() if self._initial else self._ev.clear()


THROUGHPUT_INTERVAL = 10_000
"""Emit ``Processed N adds, R records per second`` every this many adds."""

RUNNING = Flag(True)
"""Global run flag: cleared on shutdown (signal or fatal error)."""

WORKER_FATAL = Flag(False)
"""Set when a worker/fetcher hit a fatal condition -> exit non-zero after teardown."""

SAMPLE_REDO_RECORDS = Flag(False)
"""Set by the redo-floor guard: the fetcher then logs each raw redo record."""

ADDS_PROCESSED = Counter()
ADDS_REJECTED = Counter()
REDOS_PROCESSED = Counter()
REDOS_DROPPED = Counter()
ERRORS = Counter()

REDO_OUTSTANDING = Counter()
"""Redo records fetched but not yet finished processing (queued or in a worker).

Drives the fetcher's drain-tail short re-probe and, in file mode, the
"redo drained" exit condition: an empty ``get_redo_record()`` probe with zero
outstanding redo and the file fully loaded means nothing can enqueue more.
"""

LOAD_BUSY_NS = Counter()
REDO_BUSY_NS = Counter()
"""Cumulative wall-clock ns inside load / redo engine calls (-> redo_share_effective)."""

_START = time.monotonic()
_start_lock = threading.Lock()


def start_time() -> float:
    """Process (or last reset) start, ``time.monotonic()`` based."""
    return _START


def mark_fatal() -> None:
    """Count an error, flag the process fatal and request shutdown."""
    ERRORS.add()
    WORKER_FATAL.set()
    RUNNING.clear()


def reset_all() -> None:
    """Reset every counter/flag (tests run several drivers per interpreter)."""
    global _START
    for c in (
        ADDS_PROCESSED,
        ADDS_REJECTED,
        REDOS_PROCESSED,
        REDOS_DROPPED,
        ERRORS,
        REDO_OUTSTANDING,
        LOAD_BUSY_NS,
        REDO_BUSY_NS,
    ):
        c.reset()
    for f in (RUNNING, WORKER_FATAL, SAMPLE_REDO_RECORDS):
        f.reset()
    with _start_lock:
        _START = time.monotonic()


class ThroughputTicker:
    """Emits the sibling drivers' ``Processed N adds, R records per second`` line."""

    def __init__(self) -> None:
        self._last_at = time.monotonic()

    def observe(self, before: int, processed: int) -> int | None:
        """Records/sec when ``processed`` just crossed an interval boundary, else None.

        ``-1`` mirrors the Python siblings when the window is zero-length.
        """
        if processed > before and processed % THROUGHPUT_INTERVAL == 0:
            elapsed = time.monotonic() - self._last_at
            speed = int(THROUGHPUT_INTERVAL / elapsed) if elapsed > 0.0 else -1
            self._last_at = time.monotonic()
            return speed
        return None

    def report(self, before: int, processed: int) -> None:
        speed = self.observe(before, processed)
        if speed is not None:
            print(f"Processed {processed} adds, {speed} records per second", flush=True)


class Ewma:
    """Exponentially weighted moving average (redo-backlog slope)."""

    def __init__(self, alpha: float) -> None:
        self.alpha = alpha
        self._value: float | None = None

    def update(self, sample: float) -> float:
        prev = self._value
        v = sample if prev is None else self.alpha * sample + (1.0 - self.alpha) * prev
        self._value = v
        return v

    @property
    def value(self) -> float | None:
        return self._value


@dataclass
class StatusLine:
    """Inputs for one ``Combined stats:`` line."""

    redo_percent: int
    load_pref: int
    redo_pref: int
    adds: int
    adds_rate: float
    redos: int
    redos_rate: float
    load_active: bool | None
    """File-mode analog of MQ depth: True while lines remain to load; None at redo% = 100."""
    redo_backlog: int | None
    redo_backlog_slope: float | None


def _round1(v: float) -> float:
    return round(v, 1)


def mode(s: StatusLine) -> str:
    """Derived (not tracked) mode string, for log readability only."""
    if s.redo_percent == 0:
        return "load_only"
    if s.redo_percent == 100:
        return "redo_only"
    return "redo_drain" if s.load_active is False else "mixed"


def status_object(s: StatusLine) -> dict[str, object]:
    """Build the ``Combined stats`` JSON object (redo fields omitted at 0%, load at 100%)."""
    obj: dict[str, object] = {}
    if s.redo_percent < 100:
        obj["adds"] = s.adds
        obj["adds_rate"] = _round1(s.adds_rate)
        obj["adds_rejected"] = ADDS_REJECTED.value
        if s.load_active is not None:
            obj["load_active"] = s.load_active
    if s.redo_percent > 0:
        obj["redos"] = s.redos
        obj["redos_rate"] = _round1(s.redos_rate)
        obj["redos_dropped"] = REDOS_DROPPED.value
        if s.redo_backlog is not None:
            obj["redo_backlog"] = s.redo_backlog
        if s.redo_backlog_slope is not None:
            obj["redo_backlog_slope"] = _round1(s.redo_backlog_slope)
    obj["errors"] = ERRORS.value
    load_ns, redo_ns = LOAD_BUSY_NS.value, REDO_BUSY_NS.value
    if load_ns + redo_ns > 0:
        obj["redo_share_effective"] = round(redo_ns / (load_ns + redo_ns), 3)
    obj["mode"] = mode(s)
    obj["threads"] = {"load_pref": s.load_pref, "redo_pref": s.redo_pref}
    return obj


def emit_status_line(s: StatusLine) -> None:
    """Print the machine-parseable ``Combined stats: {...}`` line."""
    print("Combined stats: " + json.dumps(status_object(s), separators=(",", ":")), flush=True)


FLOOR_GUARD_INTERVALS = 5
"""Consecutive suspicious stats intervals before the redo-floor guard trips."""
_FLOOR_BACKLOG_SMALL = 1000
_FLOOR_BACKLOG_FLAT_DELTA = 5


class GuardTransition(Enum):
    TRIPPED = "tripped"
    CLEARED = "cleared"
    UNCHANGED = "unchanged"


class FloorGuard:
    """Redo-floor / ``__REPAIR__``-loop guard.

    Redo throughput > 0 while the backlog stays flat at a small value and no
    load remains, for ``FLOOR_GUARD_INTERVALS`` consecutive intervals, suggests
    a redo loop that will never drain. The guard only WARNS (and turns on raw
    redo-record sampling); stopping remains the operator's call. With
    ``load_active=None`` (pure redoer) the "no load" conjunct is vacuously true.
    """

    def __init__(self) -> None:
        self._consecutive = 0
        self._prev_backlog: int | None = None
        self._prev_redos = 0
        self._active = False

    def observe(
        self, redos_total: int, backlog: int | None, load_active: bool | None
    ) -> GuardTransition:
        redo_progress = redos_total > self._prev_redos
        load_idle = not load_active
        prev = self._prev_backlog
        backlog_small_flat = (
            backlog is not None
            and prev is not None
            and 0 < backlog <= _FLOOR_BACKLOG_SMALL
            and abs(backlog - prev) <= _FLOOR_BACKLOG_FLAT_DELTA
        )
        suspicious = redo_progress and load_idle and backlog_small_flat

        self._prev_backlog = backlog
        self._prev_redos = redos_total

        if suspicious:
            self._consecutive += 1
            if self._consecutive >= FLOOR_GUARD_INTERVALS and not self._active:
                self._active = True
                return GuardTransition.TRIPPED
            return GuardTransition.UNCHANGED
        self._consecutive = 0
        if self._active:
            self._active = False
            return GuardTransition.CLEARED
        return GuardTransition.UNCHANGED
