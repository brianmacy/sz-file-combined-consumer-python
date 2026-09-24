"""Worker-pool logic that needs no engine call: long-record monitors, bounded
join, and the worker's generic-failure path."""

from __future__ import annotations

import logging
import threading
import time

from sz_file_combined_consumer import stats
from sz_file_combined_consumer.channels import ClosableQueue, TryState
from sz_file_combined_consumer.worker import (
    ActionKind,
    LoadSide,
    RedoSide,
    WorkerClass,
    WorkerCtx,
    add_record_flags,
    join_bounded,
    monitor_load_in_progress,
    monitor_redo_in_flight,
    redo_flags,
    worker_loop,
)


def test_flags() -> None:
    assert add_record_flags(True) == 1 << 62
    assert add_record_flags(False) == 0
    assert redo_flags(True) == 1 << 62
    assert redo_flags(False) == 0


def test_monitor_redo_in_flight_logs_long_and_stuck(caplog) -> None:  # type: ignore[no-untyped-def]
    redo = RedoSide(jobs=ClosableQueue(2))
    now = time.monotonic()
    redo.in_flight[1] = (now - 3.0, '{"DATA_SOURCE":"TEST","RECORD_ID":"slow"}')
    redo.in_flight[2] = (now, '{"DATA_SOURCE":"TEST","RECORD_ID":"fresh"}')
    with caplog.at_level(logging.INFO):
        monitor_redo_in_flight(redo, long_record_secs=1, redo_capable=1)
    assert "Long redo record" in caplog.text and "TEST : slow" in caplog.text
    assert "TEST : fresh" not in caplog.text
    assert "All 1 redo-preferring threads are stuck" in caplog.text
    caplog.clear()
    monitor_redo_in_flight(redo, long_record_secs=1, redo_capable=2)
    assert "are stuck" not in caplog.text


def test_monitor_load_in_progress_logs_long_and_stuck(caplog) -> None:  # type: ignore[no-untyped-def]
    load = LoadSide(work=ClosableQueue(2), results=ClosableQueue(2))
    now = time.monotonic()
    load.in_progress[7] = now - 3.0
    load.in_progress[8] = now
    with caplog.at_level(logging.INFO):
        monitor_load_in_progress(load, long_record_secs=1, load_capable=1)
    assert "Still processing" in caplog.text and "line 7" in caplog.text
    assert "line 8" not in caplog.text
    assert "All 1 threads are stuck on long running records" in caplog.text


def test_join_bounded_reports_stuck_threads(caplog) -> None:  # type: ignore[no-untyped-def]
    release = threading.Event()
    t = threading.Thread(target=release.wait, daemon=True)
    t.start()
    try:
        with caplog.at_level(logging.WARNING):
            assert join_bounded([t], grace=0.2) is False
        assert "1 thread(s) still running" in caplog.text
    finally:
        release.set()
        t.join(timeout=5)
    assert join_bounded([t], grace=0.2) is True


def test_worker_without_work_sources_is_fatal_and_loud(caplog) -> None:  # type: ignore[no-untyped-def]
    ctx = WorkerCtx(
        worker_id=3,
        worker_class=WorkerClass.LOAD_PREFERRING,
        factory=None,  # type: ignore[arg-type]  # never touched: fails before any engine call
        engine=None,  # type: ignore[arg-type]
        load=None,
        redo=None,
        add_flags=0,
        redo_flags=0,
        want_info=False,
    )
    with caplog.at_level(logging.ERROR):
        worker_loop(ctx)
    assert "worker 3 died" in caplog.text
    assert stats.WORKER_FATAL.is_set() and not stats.RUNNING.is_set()


def test_worker_death_with_load_side_signals_fatal_outcome() -> None:
    load = LoadSide(work=ClosableQueue(1), results=ClosableQueue(4))
    ctx = WorkerCtx(
        worker_id=1,
        worker_class=WorkerClass.LOAD_PREFERRING,
        factory=None,  # type: ignore[arg-type]
        engine=None,  # type: ignore[arg-type]
        load=load,
        redo=RedoSide(jobs=ClosableQueue(1)),
        add_flags=0,
        redo_flags=0,
        want_info=False,
    )
    # A mixed worker with a None engine: the first redo job reaches
    # engine.process_redo_record on None -> AttributeError -> fatal path.
    stats.REDO_OUTSTANDING.add()
    ctx.redo.jobs.put((1, "{}"), keep_going=lambda: True)  # type: ignore[union-attr]
    worker_loop(ctx)
    state, outcome = load.results.try_get()
    assert state is TryState.ITEM and outcome is not None
    assert outcome.action.kind is ActionKind.FATAL and outcome.line_no == 0
    assert stats.WORKER_FATAL.is_set()
