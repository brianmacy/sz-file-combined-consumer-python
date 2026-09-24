"""The periodic stats/monitor tick shared by the file loader and the pure redoer:
``Engine stats:`` (mandatory prefix; harness-scraped), the ``Combined stats:``
JSON line, the redo-floor guard, and the long-record monitors.
"""

from __future__ import annotations

import logging
import threading
import time

from senzing import SzEngine

from . import stats
from .config import Config
from .worker import LoadSide, RedoSide, monitor_load_in_progress, monitor_redo_in_flight

log = logging.getLogger(__name__)

SLOPE_EWMA_ALPHA = 0.3


class Monitor:
    """Owns the per-interval state (rates, EWMA slope, floor guard)."""

    def __init__(self, config: Config, engine: SzEngine) -> None:
        self.config = config
        self.engine = engine
        self.interval = max(config.long_record_secs / 2.0, 1.0)
        self.redo_pref = config.redo_pref_workers()
        self.load_pref = config.threads - self.redo_pref
        self._slope = stats.Ewma(SLOPE_EWMA_ALPHA)
        self._floor = stats.FloorGuard()
        self._last_at = time.monotonic()
        self._prev_adds = 0
        self._prev_redos = 0
        self._prev_backlog: int | None = None

    def print_engine_stats(self) -> None:
        try:
            print(f"Engine stats: {self.engine.get_stats()}", flush=True)
        except Exception as err:
            log.warning("Could not retrieve engine stats: %s", err)

    def tick(self, load: LoadSide | None, redo: RedoSide | None, load_active: bool | None) -> None:
        """One stats interval."""
        self.print_engine_stats()
        # Backlog gauge deliberately absent: count_redo_records() is a full table
        # scan of SYS_EVAL_QUEUE and dominated DB CPU at scale. Emptiness is
        # detected by the fetcher's get_redo_record() coming back empty.
        backlog: int | None = None
        now = time.monotonic()
        dt = max(now - self._last_at, 0.001)
        adds, redos = stats.ADDS_PROCESSED.value, stats.REDOS_PROCESSED.value
        slope = (
            self._slope.update(float(backlog - self._prev_backlog))
            if backlog is not None and self._prev_backlog is not None
            else self._slope.value
        )
        stats.emit_status_line(
            stats.StatusLine(
                redo_percent=self.config.redo_percent,
                load_pref=self.load_pref,
                redo_pref=self.redo_pref,
                adds=adds,
                adds_rate=(adds - self._prev_adds) / dt,
                redos=redos,
                redos_rate=(redos - self._prev_redos) / dt,
                load_active=load_active,
                redo_backlog=backlog,
                redo_backlog_slope=slope,
            )
        )
        self._guard(redos, backlog, load_active)
        if redo is not None:
            monitor_redo_in_flight(redo, self.config.long_record_secs, self.redo_pref)
        if load is not None:
            monitor_load_in_progress(load, self.config.long_record_secs, self.config.threads)
        self._prev_adds, self._prev_redos = adds, redos
        self._prev_backlog = backlog
        self._last_at = now

    def _guard(self, redos: int, backlog: int | None, load_active: bool | None) -> None:
        match self._floor.observe(redos, backlog, load_active):
            case stats.GuardTransition.TRIPPED:
                log.warning(
                    "redo-floor suspected (possible __REPAIR__ loop): redo progressing while "
                    "backlog stays flat at a small value; sampling raw redo records to the log"
                )
                stats.SAMPLE_REDO_RECORDS.set()
            case stats.GuardTransition.CLEARED:
                log.info("redo-floor condition cleared; stopping redo-record sampling")
                stats.SAMPLE_REDO_RECORDS.clear()
            case stats.GuardTransition.UNCHANGED:
                pass


def wait_interval(seconds: float, threads: list[threading.Thread]) -> None:
    """Sleep up to ``seconds`` in short steps.

    Returns early once every thread in ``threads`` has finished or shutdown was
    requested. Signals are delivered to the main thread between these steps,
    so the main loop stays responsive to SIGINT/SIGTERM.
    """
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not any(t.is_alive() for t in threads) or not stats.RUNNING.is_set():
            return
        time.sleep(min(0.25, max(deadline - time.monotonic(), 0.0)))
