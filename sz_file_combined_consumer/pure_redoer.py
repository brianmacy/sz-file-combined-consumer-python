"""The redo% = 100 endpoint: pure redoer, no file input.

One fetcher thread + N redo-preferring workers + the monitor loop on the main
thread; runs until SIGINT/SIGTERM or a fatal error (sibling-redoer parity).
"""

from __future__ import annotations

import logging
import threading
import time

from senzing import SzAbstractFactory, SzEngine

from . import config_reload, stats
from .channels import ClosableQueue
from .config import Config
from .monitor import Monitor, wait_interval
from .redo import fetcher_loop
from .worker import RedoJob, RedoSide, WorkerClass, WorkerCtx, join_bounded, redo_flags, worker_loop

log = logging.getLogger(__name__)


def run(config: Config, factory: SzAbstractFactory, engine: SzEngine) -> tuple[bool, str | None]:
    """Run the pure-redoer topology until shutdown. Returns ``(workers_clean, error)``."""
    n = config.threads
    log.info("Pure redoer (redo%% = 100): %d workers, no file input", n)
    config_reload.log_startup_config(factory, engine)

    # Channel capacity N + 2: small on purpose — fetched-but-unprocessed redo
    # records are lost on crash, so the fetcher must not run far ahead.
    redo = RedoSide(jobs=ClosableQueue[RedoJob](n + 2))
    workers = [
        threading.Thread(
            target=worker_loop,
            args=(
                WorkerCtx(
                    worker_id=i,
                    worker_class=WorkerClass.REDO_PREFERRING,
                    factory=factory,
                    engine=engine,
                    load=None,
                    redo=redo,
                    add_flags=0,
                    redo_flags=redo_flags(config.info),
                    want_info=config.info,
                ),
            ),
            name=f"sz-worker-{i}",
            daemon=True,
        )
        for i in range(n)
    ]
    fetcher = threading.Thread(
        target=fetcher_loop,
        args=(engine, redo.jobs, config.redo_sleep_secs),
        name="sz-redo-fetcher",
        daemon=True,
    )
    for t in [*workers, fetcher]:
        t.start()

    monitor = Monitor(config, engine)
    while stats.RUNNING.is_set() and any(w.is_alive() for w in workers):
        wait_interval(monitor.interval, workers)
        if not stats.RUNNING.is_set() or not any(w.is_alive() for w in workers):
            break
        try:
            monitor.tick(None, redo, load_active=None)
        except Exception:
            log.exception("stats tick failed (diagnostics only; redo continues)")

    stats.RUNNING.clear()
    workers_clean = join_bounded([*workers, fetcher])

    redos, dropped, errors = (
        stats.REDOS_PROCESSED.value,
        stats.REDOS_DROPPED.value,
        stats.ERRORS.value,
    )
    log.info("Completed processing %d redo records (%d dropped, %d errors)", redos, dropped, errors)
    elapsed = time.monotonic() - stats.start_time()
    rate = redos / elapsed if elapsed > 0 else 0.0
    print(
        f"Stats: {redos} redo records processed, {rate:.1f}/sec, runtime: {elapsed:.0f}s",
        flush=True,
    )
    print(
        f"Processed total of 0 adds, {redos} redo records "
        f"({dropped} redo dropped, {errors} errors)",
        flush=True,
    )
    monitor.print_engine_stats()

    if stats.WORKER_FATAL.is_set():
        return workers_clean, (
            "a worker or the redo fetcher failed fatally (unrecoverable engine/DB error) — see log"
        )
    return workers_clean, None
