"""The single redo-fetcher thread: one serial ``get_redo_record()`` loop per
process feeding a SMALL bounded channel.

* ONE fetcher, never per-worker fetching: the single-producer pattern is the
  documented-safe redo dequeue shape, and fetch is far cheaper than
  ``process_redo_record`` so it is not the bottleneck.
* The channel is deliberately tiny (|B| + 2): redo records are already durably
  queued in the DB; hoarding them in memory buys nothing and loses work on
  crash. Backpressure against the full channel is the redo-fetch gating valve.
* Drain-tail short re-probe: an empty probe while redo is still outstanding
  re-probes after 2 s (cascades), and only sleeps the full quantum when quiet.
* File mode exit: with ``load_complete`` given, an empty probe with zero
  outstanding redo once the file is fully loaded means nothing can enqueue
  more redo, so the fetcher closes the channel and the workers exit.
"""

from __future__ import annotations

import itertools
import logging
import time
from collections.abc import Callable

from senzing import SzEngine

from . import stats
from .channels import ClosableQueue
from .record import RecordInfo
from .worker import Action, ActionKind, Outcome, RedoJob

log = logging.getLogger(__name__)

DRAIN_TAIL_REPROBE = 2.0
_next_redo_id = itertools.count()


def interruptible_sleep(seconds: float, step: float = 0.25) -> None:
    """Sleep up to ``seconds``, waking early if shutdown is requested."""
    deadline = time.monotonic() + seconds
    while stats.RUNNING.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(step, remaining))


def _signal_fatal(results: ClosableQueue[Outcome] | None, msg: str) -> None:
    stats.mark_fatal()
    if results is not None:
        results.put(
            Outcome(0, RecordInfo.empty(), Action(ActionKind.FATAL, msg)), keep_going=lambda: True
        )


def fetcher_loop(
    engine: SzEngine,
    jobs: ClosableQueue[RedoJob],
    redo_sleep_secs: float,
    results: ClosableQueue[Outcome] | None = None,
    load_complete: Callable[[], bool] | None = None,
) -> None:
    """Run until shutdown, a fatal engine error, or (file mode) redo is drained.

    Always closes ``jobs`` on exit, which is how redo-capable workers learn no
    more redo is coming.
    """
    try:
        _fetch(engine, jobs, redo_sleep_secs, results, load_complete)
    except Exception as err:
        log.error("Error retrieving redo record: %s", err)
        _signal_fatal(results, f"redo fetcher: {err}")
    finally:
        jobs.close()


def _fetch(
    engine: SzEngine,
    jobs: ClosableQueue[RedoJob],
    redo_sleep_secs: float,
    results: ClosableQueue[Outcome] | None,
    load_complete: Callable[[], bool] | None,
) -> None:
    while stats.RUNNING.is_set():
        record = engine.get_redo_record()
        if not record.strip():
            # Emptiness is detected HERE, by get_redo_record() coming back empty —
            # never by count_redo_records() (a table scan; monitoring only).
            if stats.REDO_OUTSTANDING.value > 0:
                log.debug("redo queue empty but redo still outstanding; re-probing for cascades")
                interruptible_sleep(min(DRAIN_TAIL_REPROBE, redo_sleep_secs))
                continue
            if load_complete is not None and load_complete():
                log.info("Redo queue drained after file load; stopping redo fetcher")
                return
            log.info("No redo records available. Pausing for %s seconds.", redo_sleep_secs)
            interruptible_sleep(redo_sleep_secs)
            continue
        if stats.SAMPLE_REDO_RECORDS.is_set():
            log.warning("redo-floor sample: %s", record)
        stats.REDO_OUTSTANDING.add()
        if not jobs.put((next(_next_redo_id), record), keep_going=stats.RUNNING.is_set):
            stats.REDO_OUTSTANDING.sub()
            log.warning("Redo fetcher stopping: shutdown requested or worker channel closed")
            return
