"""Engine worker pool: homogeneous workers processing load AND redo items, split
into load-preferring / redo-preferring classes with cross-over fallback.

Dispatch discipline (do not "simplify" this): in mixed mode BOTH dequeues are
NON-BLOCKING. A blocking get on the preferred channel would park the worker
there and the cross-over fallback would never fire (a load-preferring worker
would sit on the drained load channel through the whole redo tail). A worker
polls its preferred channel, then the other, then sleeps a SHORT interval
(1 ms); only after ~25 consecutive empty passes does it fall back to the long
50 ms backoff. At the endpoints only one channel exists, so the sibling
drivers' exact dispatch is used.

Senzing engine calls release the GIL (ctypes), so N Python threads drive N
concurrent engine calls; DB connections are owned per-OS-thread inside libSz.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto

from senzing import SzAbstractFactory, SzEngine, SzEngineFlags

from . import config_reload, stats
from .channels import ClosableQueue, TryState
from .record import ErrorClass, RecordInfo, classify_error, logging_id

log = logging.getLogger(__name__)

SHORT_POLL = 0.001
SHORT_POLL_PASSES = 25
IDLE_BACKOFF = 0.05
SHUTDOWN_GRACE = 10.0
"""Grace window shared by the in-flight drain and the bounded worker join."""

REDO_STATS_INTERVAL = 1000
"""Throughput line cadence for redo records (redoer parity)."""


def add_record_flags(info: bool) -> int:
    return int(SzEngineFlags.SZ_WITH_INFO if info else SzEngineFlags.SZ_ADD_RECORD_DEFAULT_FLAGS)


def redo_flags(info: bool) -> int:
    return int(SzEngineFlags.SZ_WITH_INFO if info else SzEngineFlags.SZ_REDO_DEFAULT_FLAGS)


@dataclass(frozen=True)
class LoadItem:
    line_no: int
    body: bytes
    info: RecordInfo


RedoJob = tuple[int, str]
"""(monotonic id, raw redo record JSON)."""


class ActionKind(Enum):
    ACK = auto()
    REJECT = auto()
    FATAL = auto()


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    payload: str | None = None
    """WithInfo response for ACK; error text for FATAL."""


@dataclass(frozen=True)
class Outcome:
    line_no: int
    info: RecordInfo
    action: Action


class WorkerClass(Enum):
    LOAD_PREFERRING = auto()
    REDO_PREFERRING = auto()


@dataclass
class LoadSide:
    work: ClosableQueue[LoadItem]
    results: ClosableQueue[Outcome]
    in_progress: dict[int, float] = field(default_factory=dict)
    """line_no -> pickup time for lines inside a worker (long-record monitor)."""
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class RedoSide:
    jobs: ClosableQueue[RedoJob]
    in_flight: dict[int, tuple[float, str]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class WorkerCtx:
    worker_id: int
    worker_class: WorkerClass
    factory: SzAbstractFactory
    engine: SzEngine
    load: LoadSide | None
    redo: RedoSide | None
    add_flags: int
    redo_flags: int
    want_info: bool


def worker_loop(ctx: WorkerCtx) -> None:
    """Worker thread entry point; a raised exception is a fatal worker failure."""
    try:
        match (ctx.load, ctx.redo):
            case (LoadSide(), None):
                _pure_load_loop(ctx)
            case (None, RedoSide()):
                _pure_redo_loop(ctx)
            case (LoadSide(), RedoSide()):
                _mixed_loop(ctx)
            case _:
                raise AssertionError("worker requires at least one work source")
    except Exception:
        log.exception("worker %d died", ctx.worker_id)
        stats.mark_fatal()
        if ctx.load is not None:
            _send_fatal(ctx.load, f"worker {ctx.worker_id} died with an unexpected exception")
    log.debug("worker %d finished", ctx.worker_id)


def _send_fatal(load: LoadSide, msg: str) -> None:
    load.results.put(
        Outcome(0, RecordInfo.empty(), Action(ActionKind.FATAL, msg)), keep_going=lambda: True
    )


def _pure_load_loop(ctx: WorkerCtx) -> None:
    load = ctx.load
    assert load is not None
    while stats.RUNNING.is_set():
        state, item = load.work.get(timeout=0.25)
        match state:
            case TryState.CLOSED:
                return
            case TryState.EMPTY:
                continue
            case TryState.ITEM:
                assert item is not None
                if not _process_load(ctx, load, item):
                    return


def _pure_redo_loop(ctx: WorkerCtx) -> None:
    redo = ctx.redo
    assert redo is not None
    while True:
        state, job = redo.jobs.try_get()
        match state:
            case TryState.CLOSED:
                return
            case TryState.EMPTY:
                if stats.RUNNING.is_set():
                    time.sleep(IDLE_BACKOFF)
                    continue
                # Shutdown: drain one record queued since the probe, then exit.
                state, job = redo.jobs.try_get()
                if state is not TryState.ITEM:
                    return
        assert job is not None
        if not _process_redo(ctx, redo, job):
            return


def _mixed_loop(ctx: WorkerCtx) -> None:
    load, redo = ctx.load, ctx.redo
    assert load is not None and redo is not None
    prefer_redo = ctx.worker_class is WorkerClass.REDO_PREFERRING
    load_open = redo_open = True
    idle_passes = 0
    while stats.RUNNING.is_set():
        got: LoadItem | RedoJob | None = None
        for pick_redo in (prefer_redo, not prefer_redo):
            if pick_redo and redo_open:
                state, job = redo.jobs.try_get()
                if state is TryState.ITEM:
                    got = job
                    break
                redo_open = state is not TryState.CLOSED
            elif not pick_redo and load_open:
                state, item = load.work.try_get()
                if state is TryState.ITEM:
                    got = item
                    break
                load_open = state is not TryState.CLOSED
        if got is None:
            if not load_open and not redo_open:
                return
            idle_passes += 1
            time.sleep(SHORT_POLL if idle_passes <= SHORT_POLL_PASSES else IDLE_BACKOFF)
            continue
        idle_passes = 0
        keep_going = (
            _process_load(ctx, load, got)
            if isinstance(got, LoadItem)
            else _process_redo(ctx, redo, got)
        )
        if not keep_going:
            return


def _add_record(ctx: WorkerCtx, item: LoadItem, body: str) -> Action:
    info = item.info
    try:
        try:
            resp = ctx.engine.add_record(info.data_source, info.record_id, body, ctx.add_flags)
        except Exception:
            if not config_reload.reinit_if_stale(ctx.factory):
                raise
            # Registered default drifted; engine reinitialized — retry once.
            resp = ctx.engine.add_record(info.data_source, info.record_id, body, ctx.add_flags)
    except Exception as err:
        if classify_error(err) is ErrorClass.BAD_INPUT_OR_TIMEOUT:
            # The loader logs WHERE the record went; only here is WHY known.
            log.warning(
                "REJECTING due to bad data or timeout [worker %d]: %s : %s -> %s",
                ctx.worker_id,
                info.data_source,
                info.record_id,
                err,
            )
            return Action(ActionKind.REJECT)
        return Action(ActionKind.FATAL, str(err))
    return Action(ActionKind.ACK, resp if ctx.want_info else None)


def _process_load(ctx: WorkerCtx, load: LoadSide, item: LoadItem) -> bool:
    """One ``add_record``; reports the outcome. Returns False when the worker should exit."""
    config_reload.poll(ctx.factory)
    with load.lock:
        load.in_progress[item.line_no] = time.monotonic()
    t0 = time.monotonic_ns()
    try:
        body = item.body.decode("utf-8")
    except UnicodeDecodeError:
        log.warning("worker %d: non-UTF-8 record body at line %d", ctx.worker_id, item.line_no)
        action = Action(ActionKind.REJECT)
    else:
        action = _add_record(ctx, item, body)
    stats.LOAD_BUSY_NS.add(time.monotonic_ns() - t0)
    with load.lock:
        load.in_progress.pop(item.line_no, None)
    if action.kind is ActionKind.FATAL:
        stats.mark_fatal()
    load.results.put(Outcome(item.line_no, item.info, action), keep_going=lambda: True)
    return action.kind is not ActionKind.FATAL


def _process_redo_record(ctx: WorkerCtx, record: str) -> str:
    try:
        return ctx.engine.process_redo_record(record, ctx.redo_flags)
    except Exception:
        if not config_reload.reinit_if_stale(ctx.factory):
            raise
        return ctx.engine.process_redo_record(record, ctx.redo_flags)


def _process_redo(ctx: WorkerCtx, redo: RedoSide, job: RedoJob) -> bool:
    """One ``process_redo_record``. Outcomes are terminal here (no delivery to ack)."""
    job_id, record = job
    config_reload.poll(ctx.factory)
    with redo.lock:
        redo.in_flight[job_id] = (time.monotonic(), record)
    t0 = time.monotonic_ns()
    keep_going = True
    try:
        result = _process_redo_record(ctx, record)
    except Exception as err:
        if classify_error(err) is ErrorClass.BAD_INPUT_OR_TIMEOUT:
            # Engine-internal record, no queue to reject to: log WITH the engine
            # error text so the cause is recoverable from the log, and drop.
            log.warning(
                "REDO FAILED due to bad data or timeout [worker %d]: %s -> %s",
                ctx.worker_id,
                logging_id(record),
                err,
            )
            stats.REDOS_DROPPED.add()
        else:
            log.error(
                "FATAL error processing redo record [worker %d]: %s [%s]",
                ctx.worker_id,
                err,
                logging_id(record),
            )
            stats.mark_fatal()
            if ctx.load is not None:
                _send_fatal(ctx.load, str(err))
            keep_going = False
    else:
        count = stats.REDOS_PROCESSED.add()
        if ctx.want_info and result:
            print(result, flush=True)
        if count % REDO_STATS_INTERVAL == 0:
            elapsed = time.monotonic() - stats.start_time()
            rate = count / elapsed if elapsed > 0 else 0.0
            log.info("Stats: %d redo records processed, %.1f/sec", count, rate)
    stats.REDO_BUSY_NS.add(time.monotonic_ns() - t0)
    with redo.lock:
        redo.in_flight.pop(job_id, None)
    stats.REDO_OUTSTANDING.sub()
    return keep_going


def monitor_redo_in_flight(redo: RedoSide, long_record_secs: int, redo_capable: int) -> None:
    """Log long-running redo records; warn when every redo-capable worker is stuck."""
    now = time.monotonic()
    num_stuck = 0
    with redo.lock:
        snapshot = list(redo.in_flight.values())
    for started, record in snapshot:
        duration = now - started
        if duration > long_record_secs:
            log.info("Long redo record (%.1f min): %s", duration / 60.0, logging_id(record))
        if duration > 2 * long_record_secs:
            num_stuck += 1
    if redo_capable > 0 and num_stuck >= redo_capable:
        log.warning("All %d redo-preferring threads are stuck on long redo records", redo_capable)


def monitor_load_in_progress(load: LoadSide, long_record_secs: int, load_capable: int) -> None:
    """Log long-running adds (by line number); warn when every worker is stuck."""
    now = time.monotonic()
    num_stuck = 0
    with load.lock:
        snapshot = list(load.in_progress.items())
    for line_no, started in snapshot:
        duration = now - started
        if duration > long_record_secs:
            log.info("Still processing (%.1f min): line %d", duration / 60.0, line_no)
        if duration > 2 * long_record_secs:
            num_stuck += 1
    if load_capable > 0 and num_stuck >= load_capable:
        log.warning("All %d threads are stuck on long running records", load_capable)


def join_bounded(threads: list[threading.Thread], grace: float = SHUTDOWN_GRACE) -> bool:
    """Join ``threads`` within ``grace`` seconds.

    Returns True iff ALL finished. False means a thread is still inside an
    uninterruptible engine call; the caller must then SKIP the native
    environment destroy (leak-on-exit over use-after-free).
    """
    deadline = time.monotonic() + grace
    for t in threads:
        remaining = deadline - time.monotonic()
        if remaining > 0:
            t.join(timeout=remaining)
    pending = [t for t in threads if t.is_alive()]
    if pending:
        log.warning(
            "%d thread(s) still running after %.0fs grace; detaching and skipping native "
            "teardown to avoid use-after-free",
            len(pending),
            grace,
        )
        return False
    return True
