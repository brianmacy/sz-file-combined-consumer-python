"""File-input load mode: read newline-delimited JSON (JSONL) records from a
single file and feed them through the worker pool, optionally alongside
concurrent redo processing (redo% in (0, 100)).

There is no message broker and therefore no ack/redelivery: a file cannot be
"requeued". Instead ``--skip-lines N`` skips the first N physical lines so an
interrupted load can resume. ``add_record`` is idempotent (re-adding the same
record is an update, not a duplicate), so resuming is at-least-once and safe.

Safe resume offset (contiguous-completion watermark)
    Records complete out of order across N workers, so "lines read" is NOT a
    safe resume point. We track the highest line L such that EVERY line up to
    L has completed (added, rejected, or skipped as blank) and report
    ``skip + watermark`` at shutdown; at most a few in-flight lines past the
    watermark are reprocessed (idempotent).

Reject file
    A file has no dead-letter queue, so every rejected line — unparseable
    JSON, engine bad input, retry timeout, SENZ0082 — is appended VERBATIM to a
    JSONL side file (``--reject-file``, default ``<input>.rejected.jsonl``) for
    reprocessing by pointing ``--file`` at it. Created lazily on the first
    reject, opened in append mode, one unbuffered write per line so a SIGTERM
    loses no rejects. WHY each record was rejected is in the application log.

Redo
    With redo% > 0 the redo fetcher and |B| redo-preferring workers run
    concurrently with the load. Once the file is fully loaded every worker
    falls into redo; when ``get_redo_record()`` comes back empty with nothing
    outstanding, the fetcher closes the redo channel and the process exits 0.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from senzing import SzAbstractFactory, SzEngine

from . import config_reload, stats
from .channels import ClosableQueue, TryState
from .config import Config
from .monitor import Monitor, wait_interval
from .record import ParseError, parse_record
from .redo import fetcher_loop
from .worker import (
    SHUTDOWN_GRACE,
    ActionKind,
    LoadItem,
    LoadSide,
    Outcome,
    RedoJob,
    RedoSide,
    WorkerClass,
    WorkerCtx,
    add_record_flags,
    join_bounded,
    redo_flags,
    worker_loop,
)

log = logging.getLogger(__name__)

PROGRESS_EVERY = 50_000
"""Progress log cadence, in physical lines read."""

UTF8_BOM = b"\xef\xbb\xbf"
"""Tolerated (stripped) at the start of the first line only."""


class ResumeTracker:
    """Contiguous-completion watermark for safe ``--skip-lines`` resume."""

    def __init__(self, first_line: int) -> None:
        """``first_line`` is the first line number this run will read (skip + 1)."""
        self._next = first_line
        self._out_of_order: set[int] = set()

    def complete(self, line: int) -> None:
        """Mark physical ``line`` complete; advance across now-contiguous lines."""
        if line < self._next:
            return
        self._out_of_order.add(line)
        while self._next in self._out_of_order:
            self._out_of_order.remove(self._next)
            self._next += 1

    @property
    def watermark(self) -> int:
        """Highest line with every preceding line complete (= safe --skip-lines)."""
        return self._next - 1


class RejectSink:
    """Append-only JSONL sink for rejected lines; opens lazily on first write."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._fh: BinaryIO | None = None
        self.written = 0

    def write_line(self, line: bytes) -> None:
        """Append ``line`` + newline in one unbuffered write (raises OSError)."""
        if self._fh is None:
            self._fh = open(self.path, "ab", buffering=0)  # noqa: SIM115 - lifetime spans the run
        self._fh.write(line + b"\n")
        self.written += 1

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


@dataclass
class LoadState:
    """Shared reader/consumer bookkeeping (watermark, reject sink, in-flight bodies)."""

    resume: ResumeTracker
    sink: RejectSink
    in_flight: dict[int, bytes] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    reader_done: threading.Event = field(default_factory=threading.Event)
    lines_read: int = 0
    read_error: str | None = None

    def complete(self, line_no: int) -> None:
        with self.lock:
            self.resume.complete(line_no)

    def reject(self, line_no: int, body: bytes) -> None:
        """Write one rejected line and count it. A sink I/O failure is logged
        loudly WITH the body (so it is not lost) but does not abort the load."""
        stats.ADDS_REJECTED.add()
        with self.lock:
            try:
                self.sink.write_line(body)
            except OSError as err:
                log.warning(
                    "cannot write rejected line %d to %s: %s; record follows: %s",
                    line_no,
                    self.sink.path,
                    err,
                    body.decode("utf-8", errors="replace"),
                )

    def load_complete(self) -> bool:
        """True once the reader finished AND every dispatched line reported back."""
        with self.lock:
            return self.reader_done.is_set() and not self.in_flight

    @property
    def watermark(self) -> int:
        with self.lock:
            return self.resume.watermark


def read_and_feed(path: str, skip: int, work: ClosableQueue[LoadItem], state: LoadState) -> None:
    """Reader thread: skip, then feed each non-blank line to the pool keyed by line number.

    Blank and unparseable lines complete immediately (blank = skipped,
    unparseable = rejected) so the watermark can advance past them. Always
    closes ``work`` and sets ``reader_done`` on exit.
    """
    line_no = 0
    try:
        with open(path, "rb") as fh:
            for raw in fh:
                if not stats.RUNNING.is_set():
                    log.info("shutdown requested; stopping file read at line %d", line_no)
                    break
                line_no += 1
                if line_no <= skip:
                    continue
                if line_no % PROGRESS_EVERY == 0:
                    log.info("file load progress: %d lines read", line_no)
                if line_no == 1:
                    raw = raw.removeprefix(UTF8_BOM)
                if not _feed_line(line_no, raw.strip(), work, state):
                    break
    except OSError as err:
        state.read_error = f"cannot read input file {path!r}: {err}"
    except Exception as err:
        # Never let the reader die silently: a partial load must not exit 0.
        log.exception("unexpected reader failure at line %d", line_no)
        state.read_error = f"unexpected reader failure at line {line_no}: {err!r}"
    finally:
        state.lines_read = line_no
        work.close()
        state.reader_done.set()


def _feed_line(line_no: int, body: bytes, work: ClosableQueue[LoadItem], state: LoadState) -> bool:
    """Dispatch one stripped line; False when the pool is gone (stop reading)."""
    if not body:
        state.complete(line_no)
        return True
    try:
        info = parse_record(body)
    except ParseError as err:
        log.warning("REJECTING unparseable record at line %d: %s", line_no, err)
        state.reject(line_no, body)
        state.complete(line_no)
        return True
    with state.lock:
        state.in_flight[line_no] = body
    # Backpressure: blocks while the work queue is full; False means every
    # worker has exited (e.g. fatal) or shutdown was requested.
    if work.put(LoadItem(line_no, body, info), keep_going=stats.RUNNING.is_set):
        return True
    log.warning("worker pool closed; stopping file read at line %d", line_no)
    with state.lock:
        state.in_flight.pop(line_no, None)
    return False


def result_consumer(results: ClosableQueue[Outcome], want_info: bool, state: LoadState) -> None:
    """Drain worker outcomes: count, print WithInfo, write rejects, advance the watermark."""
    ticker = stats.ThroughputTicker()
    while True:
        st, outcome = results.get(timeout=0.25)
        if st is TryState.CLOSED:
            return
        if outcome is None:
            continue
        try:
            _settle(outcome, want_info, state, ticker)
        except Exception:
            # A consumer that dies silently would lose outcomes and let a
            # partial load exit 0; flag the run fatal instead.
            log.exception("result consumer failed on line %d", outcome.line_no)
            stats.mark_fatal()


def _settle(
    outcome: Outcome, want_info: bool, state: LoadState, ticker: stats.ThroughputTicker
) -> None:
    with state.lock:
        body = state.in_flight.pop(outcome.line_no, None)
    match outcome.action.kind:
        case ActionKind.ACK:
            if want_info and outcome.action.payload:
                print(outcome.action.payload, flush=True)
            after = stats.ADDS_PROCESSED.add()
            ticker.report(after - 1, after)
            state.complete(outcome.line_no)
        case ActionKind.REJECT:
            # The worker logged WHY (engine error text); log WHERE.
            log.warning(
                "REJECTING line %d (%s : %s) -> %s",
                outcome.line_no,
                outcome.info.data_source,
                outcome.info.record_id,
                state.sink.path,
            )
            if body is None:
                stats.ADDS_REJECTED.add()
                log.warning("no in-flight body for rejected line %d; not written", outcome.line_no)
            else:
                state.reject(outcome.line_no, body)
            state.complete(outcome.line_no)
        case ActionKind.FATAL:
            # Worker already flagged fatal + stopped the run; do NOT advance
            # the watermark past this line (it did not load).
            log.warning("fatal engine error during file load: %s", outcome.action.payload)


def _spawn_workers(
    config: Config,
    factory: SzAbstractFactory,
    engine: SzEngine,
    load: LoadSide,
    redo: RedoSide | None,
) -> list[threading.Thread]:
    redo_pref = config.redo_pref_workers()
    threads = []
    for worker_id in range(config.threads):
        ctx = WorkerCtx(
            worker_id=worker_id,
            worker_class=(
                WorkerClass.REDO_PREFERRING
                if worker_id < redo_pref
                else WorkerClass.LOAD_PREFERRING
            ),
            factory=factory,
            engine=engine,
            load=load,
            redo=redo,
            add_flags=add_record_flags(config.info),
            redo_flags=redo_flags(config.info),
            want_info=config.info,
        )
        threads.append(
            threading.Thread(
                target=worker_loop, args=(ctx,), name=f"sz-worker-{worker_id}", daemon=True
            )
        )
    return threads


def run(config: Config, factory: SzAbstractFactory, engine: SzEngine) -> tuple[bool, str | None]:
    """Run the file loader (+ concurrent redo) to EOF/drain or SIGTERM.

    Returns ``(workers_clean, error)``: ``workers_clean`` is True iff every
    worker finished within the shutdown grace (safe to destroy the
    environment); ``error`` is None on success.
    """
    path, reject_path = config.input_file, config.reject_file
    if path is None or reject_path is None:
        raise ValueError(
            "file mode requires an input file and a reject file (validated at startup)"
        )
    n, redo_pref = config.threads, config.redo_pref_workers()
    log.info(
        "File loader: reading %r with %d workers (%d redo-preferring, redo%% = %d; "
        "skip-lines: %d); rejected lines are appended to %r",
        path,
        n,
        redo_pref,
        config.redo_percent,
        config.skip_lines,
        reject_path,
    )
    config_reload.log_startup_config(factory, engine)

    load = LoadSide(work=ClosableQueue[LoadItem](n), results=ClosableQueue[Outcome](n * 2))
    redo = RedoSide(jobs=ClosableQueue[RedoJob](redo_pref + 2)) if redo_pref > 0 else None
    state = LoadState(resume=ResumeTracker(config.skip_lines + 1), sink=RejectSink(reject_path))

    workers = _spawn_workers(config, factory, engine, load, redo)
    consumer = threading.Thread(
        target=result_consumer,
        args=(load.results, config.info, state),
        name="sz-file-result",
        daemon=True,
    )
    reader = threading.Thread(
        target=read_and_feed,
        args=(path, config.skip_lines, load.work, state),
        name="sz-file-reader",
        daemon=True,
    )
    fetcher = (
        threading.Thread(
            target=fetcher_loop,
            args=(engine, redo.jobs, config.redo_sleep_secs, load.results, state.load_complete),
            name="sz-redo-fetcher",
            daemon=True,
        )
        if redo is not None
        else None
    )
    for t in [*workers, consumer, reader, *([fetcher] if fetcher else [])]:
        t.start()

    _monitor_until_done(config, engine, workers, load, redo, state)

    # Release a reader wedged on a full queue (every worker has exited), then
    # join within the grace window; a worker still inside an engine call after
    # the grace means the caller must skip the native teardown.
    load.work.close()
    workers_clean = join_bounded(workers)
    if redo is not None:
        redo.jobs.close()
    aux = [reader, *([fetcher] if fetcher else [])]
    join_bounded(aux, grace=SHUTDOWN_GRACE)
    load.results.close()
    consumer.join(timeout=SHUTDOWN_GRACE)
    state.sink.close()
    return workers_clean, _finish(config, state, reject_path)


def _monitor_until_done(
    config: Config,
    engine: SzEngine,
    workers: list[threading.Thread],
    load: LoadSide,
    redo: RedoSide | None,
    state: LoadState,
) -> None:
    monitor = Monitor(config, engine)
    while True:
        wait_interval(monitor.interval, workers)
        if not any(w.is_alive() for w in workers) or not stats.RUNNING.is_set():
            return
        try:
            monitor.tick(load, redo, load_active=not state.load_complete())
        except Exception:
            log.exception("stats tick failed (diagnostics only; the load continues)")


def _finish(config: Config, state: LoadState, reject_path: str) -> str | None:
    adds, rejected = stats.ADDS_PROCESSED.value, stats.ADDS_REJECTED.value
    redos, dropped = stats.REDOS_PROCESSED.value, stats.REDOS_DROPPED.value
    errors, written = stats.ERRORS.value, state.sink.written
    watermark = state.watermark
    # "Processed total of ..." keeps the prefix the e2e tests / tooling scrape.
    print(
        f"Processed total of {adds} adds, {redos} redo records "
        f"({dropped} redo dropped, {errors} errors)",
        flush=True,
    )
    print(
        f"File load: {rejected} record(s) rejected ({written} written to {reject_path}); "
        f"safe resume with --skip-lines {watermark}",
        flush=True,
    )
    if state.read_error is not None:
        return f"file read failed: {state.read_error}"
    log.info(
        "File loader finished: %d physical line(s) read from %r",
        state.lines_read,
        config.input_file,
    )
    if stats.WORKER_FATAL.is_set():
        return "a worker or the redo fetcher reported a fatal engine error during file load"
    unwritten = rejected - written
    if unwritten > 0:
        # The load itself completed, but rejects exist only in the log (e.g. a
        # mistyped --reject-file directory). Exit non-zero so the operator
        # notices before the log rotates away.
        return (
            f"{unwritten} rejected record(s) could NOT be written to {reject_path}; "
            "their bodies are in the log"
        )
    if not stats.RUNNING.is_set():
        log.info("stopped before EOF (shutdown requested); resume with --skip-lines %d", watermark)
    return None
