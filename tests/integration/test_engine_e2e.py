"""End-to-end tests against a REAL Senzing engine (no mocks).

Skipped unless ``SENZING_ENGINE_CONFIGURATION_JSON`` points at an initialized
repository. Most runs are in-process (``runtime.main(..., factory=...)``
sharing the session factory) so coverage sees them; the signal/teardown paths
spawn the CLI as a subprocess.

Local run (macOS, homebrew cask; Linux: LD_LIBRARY_PATH=/opt/senzing/er/lib):

    SZ=/opt/homebrew/Caskroom/senzingsdk/<ver>/senzing
    cp $SZ/er/resources/templates/G2C.db.template /tmp/G2C.db
    export DYLD_LIBRARY_PATH=$SZ/er/lib
    export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{"CONFIGPATH":"'$SZ'/er/etc",
      "RESOURCEPATH":"'$SZ'/er/resources","SUPPORTPATH":"'$SZ'/data"},
      "SQL":{"CONNECTION":"sqlite3://na:na@/tmp/G2C.db"}}'
    pytest tests/integration -v
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest
from senzing import SzNotFoundError

from sz_file_combined_consumer import config_reload, runtime, stats

pytestmark = pytest.mark.engine

DS = "TEST"
TOTAL_RE = re.compile(
    r"Processed total of (\d+) adds, (\d+) redo records \((\d+) redo dropped, (\d+) errors\)"
)
RESUME_RE = re.compile(r"safe resume with --skip-lines (\d+)")

NAMES = [
    "Alice Johnson", "Bob Smith", "Carol White", "Dan Brown", "Eve Black", "Frank Green",
    "Grace Hall", "Hank Young", "Ivy King", "Jack Lee", "Kim Moore", "Leo Clark", "Mia Lewis",
    "Ned Walker", "Ola Allen", "Pat Wright", "Quin Scott", "Rex Adams", "Sue Baker", "Tom Hill",
]  # fmt: skip


def _prefix() -> str:
    return uuid.uuid4().hex[:8]


def simple_records(prefix: str, n: int) -> list[dict[str, str]]:
    """Distinct people: never resolve together, never generate redo."""
    return [
        {"DATA_SOURCE": DS, "RECORD_ID": f"{prefix}-S{i}", "NAME_FULL": f"Person {prefix} {i}",
         "DATE_OF_BIRTH": f"19{50 + i % 40:02d}-01-{1 + i % 28:02d}"}
        for i in range(n)
    ]  # fmt: skip


def redo_records(prefix: str) -> list[dict[str, str]]:
    """Distinct names sharing ONE phone/address/SSN: the shared features go
    generic once enough entities carry them, which enqueues REPAIR_ENTITY redo
    records (verified empirically against engine 4.4: ~10 redo after ~12
    records). The shared values are derived from ``prefix`` because a feature
    value only generates redo the FIRST time it turns generic in a repository.
    """
    digits = f"{int(prefix, 16) % 10_000_000:07d}"
    return [
        {
            "DATA_SOURCE": DS,
            "RECORD_ID": f"{prefix}-R{i}",
            "NAME_FULL": name,
            "PHONE_NUMBER": f"702-{digits[:3]}-{digits[3:]}",
            "ADDR_FULL": f"{digits[:4]} Main St Las Vegas NV 89101",
            "SSN_NUMBER": f"{digits[:3]}-{digits[3:5]}-{digits[5:]}0",
        }
        for i, name in enumerate(NAMES)
    ]


def write_jsonl(path: Path, lines: list[object]) -> None:
    text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines) + "\n"
    path.write_text(text)


def assert_loaded(engine, record_ids: list[str]) -> None:  # type: ignore[no-untyped-def]
    for rid in record_ids:
        assert json.loads(engine.get_record(DS, rid))["RECORD_ID"] == rid


def assert_not_loaded(engine, record_ids: list[str]) -> None:  # type: ignore[no-untyped-def]
    for rid in record_ids:
        with pytest.raises(SzNotFoundError):
            engine.get_record(DS, rid)


def run_main(args: list[str], factory) -> int:  # type: ignore[no-untyped-def]
    return runtime.main(args, factory=factory)


def drain_redo(factory) -> None:  # type: ignore[no-untyped-def]
    """Leave the redo queue empty so a later test's redo assertions are its own."""
    engine = factory.create_engine()
    while record := engine.get_redo_record():
        engine.process_redo_record(record)


# --------------------------------------------------------------------------
# File load (pure loader, redo% = 0)
# --------------------------------------------------------------------------


def test_file_load_rejects_and_reports_resume_offset(
    tmp_path: Path, factory, engine, capsys
) -> None:  # type: ignore[no-untyped-def]
    p = _prefix()
    good = simple_records(p, 5)
    unknown_ds = {"DATA_SOURCE": "NOPE", "RECORD_ID": f"{p}-X1", "NAME_FULL": "Nobody"}
    missing_ds = {"RECORD_ID": f"{p}-X2"}
    lines: list[object] = [
        good[0],
        good[1],
        "",
        "not json at all",
        unknown_ds,
        good[2],
        missing_ds,
        good[3],
        good[4],
    ]
    src = tmp_path / "in.jsonl"
    write_jsonl(src, lines)

    code = run_main(
        [
            "--file",
            str(src),
            "--redo-percent",
            "0",
            "--threads-per-process",
            "3",
            "--long-record",
            "2",
        ],
        factory,
    )
    out = capsys.readouterr()
    assert code == 0, out.err

    assert_loaded(engine, [r["RECORD_ID"] for r in good])
    m = TOTAL_RE.search(out.out)
    assert m and m.groups() == ("5", "0", "0", "0"), out.out
    assert RESUME_RE.search(out.out).group(1) == str(len(lines))  # type: ignore[union-attr]
    assert "File load: 3 record(s) rejected (3 written to" in out.out

    reject = tmp_path / "in.jsonl.rejected.jsonl"
    assert reject.exists()
    rejected = reject.read_text().splitlines()
    assert sorted(rejected) == sorted(
        ["not json at all", json.dumps(unknown_ds), json.dumps(missing_ds)]
    )
    assert stats.ADDS_REJECTED.value == 3 and stats.ADDS_PROCESSED.value == 5


def test_clean_load_creates_no_reject_file_and_prints_engine_stats(
    tmp_path: Path, factory, engine, capsys
) -> None:  # type: ignore[no-untyped-def]
    p = _prefix()
    src = tmp_path / "clean.jsonl"
    write_jsonl(src, simple_records(p, 3))
    src.write_bytes(b"\xef\xbb\xbf" + src.read_bytes())  # UTF-8 BOM must not reject line 1
    code = run_main(
        [
            "--file",
            str(src),
            "--redo-percent",
            "0",
            "--threads-per-process",
            "1",
            "--long-record",
            "2",
        ],
        factory,
    )
    out = capsys.readouterr().out
    assert code == 0
    assert not (tmp_path / "clean.jsonl.rejected.jsonl").exists()
    assert "File load: 0 record(s) rejected (0 written to" in out
    assert_loaded(engine, [f"{p}-S{i}" for i in range(3)])


def test_skip_lines_resumes_past_already_loaded_lines(
    tmp_path: Path, factory, engine, capsys
) -> None:  # type: ignore[no-untyped-def]
    p = _prefix()
    recs = simple_records(p, 4)
    src = tmp_path / "resume.jsonl"
    write_jsonl(src, recs)
    code = run_main(
        [
            "--file",
            str(src),
            "--skip-lines",
            "2",
            "--redo-percent",
            "0",
            "--threads-per-process",
            "2",
        ],
        factory,
    )
    out = capsys.readouterr().out
    assert code == 0
    assert_not_loaded(engine, [recs[0]["RECORD_ID"], recs[1]["RECORD_ID"]])
    assert_loaded(engine, [recs[2]["RECORD_ID"], recs[3]["RECORD_ID"]])
    assert RESUME_RE.search(out).group(1) == "4"  # type: ignore[union-attr]


def test_with_info_prints_affected_entities(tmp_path: Path, factory, capsys) -> None:  # type: ignore[no-untyped-def]
    src = tmp_path / "info.jsonl"
    write_jsonl(src, simple_records(_prefix(), 2))
    code = run_main(
        ["--file", str(src), "--redo-percent", "0", "--threads-per-process", "1", "-i"], factory
    )
    out = capsys.readouterr().out
    assert code == 0
    assert out.count('"AFFECTED_ENTITIES"') == 2


def test_explicit_reject_file_and_second_pass_reprocessing(
    tmp_path: Path, factory, engine, capsys
) -> None:  # type: ignore[no-untyped-def]
    p = _prefix()
    bad = {"DATA_SOURCE": "NOPE", "RECORD_ID": f"{p}-B", "NAME_FULL": "Bad DS"}
    src, rej, rej2 = tmp_path / "a.jsonl", tmp_path / "bad.jsonl", tmp_path / "still-bad.jsonl"
    write_jsonl(src, [simple_records(p, 1)[0], bad])
    assert (
        run_main(
            [
                "--file",
                str(src),
                "--reject-file",
                str(rej),
                "--redo-percent",
                "0",
                "--threads-per-process",
                "1",
            ],
            factory,
        )
        == 0
    )
    assert rej.read_text() == json.dumps(bad) + "\n"
    capsys.readouterr()
    # Reprocess the reject file with its own reject file: still rejected, verbatim, once.
    assert (
        run_main(
            [
                "--file",
                str(rej),
                "--reject-file",
                str(rej2),
                "--redo-percent",
                "0",
                "--threads-per-process",
                "1",
            ],
            factory,
        )
        == 0
    )
    assert rej2.read_text() == json.dumps(bad) + "\n"
    assert rej.read_text() == json.dumps(bad) + "\n", "input reject file must not be appended to"


def test_unwritable_reject_file_loads_but_exits_fatal(
    tmp_path: Path, factory, engine, capsys
) -> None:  # type: ignore[no-untyped-def]
    p = _prefix()
    good = simple_records(p, 1)[0]
    src = tmp_path / "u.jsonl"
    write_jsonl(src, [good, "garbage"])
    code = run_main(
        [
            "--file",
            str(src),
            "--reject-file",
            "/nonexistent-dir-for-sz-test/r.jsonl",
            "--redo-percent",
            "0",
            "--threads-per-process",
            "1",
        ],
        factory,
    )
    out = capsys.readouterr()
    assert code == 255
    assert "1 rejected record(s) could NOT be written" in out.err
    assert_loaded(engine, [good["RECORD_ID"]])


def test_missing_input_file_exits_fatal(tmp_path: Path, factory, capsys) -> None:  # type: ignore[no-untyped-def]
    code = run_main(
        [
            "--file",
            str(tmp_path / "nope.jsonl"),
            "--redo-percent",
            "0",
            "--threads-per-process",
            "1",
        ],
        factory,
    )
    out = capsys.readouterr()
    assert code == 255
    assert "file read failed: cannot read input file" in out.err
    assert "Processed total of 0 adds" in out.out


# --------------------------------------------------------------------------
# Combined: file load + concurrent redo, exits when redo is drained
# --------------------------------------------------------------------------


def test_combined_load_drains_redo_and_exits(tmp_path: Path, factory, engine, capsys) -> None:  # type: ignore[no-untyped-def]
    drain_redo(factory)
    p = _prefix()
    recs = redo_records(p)
    src = tmp_path / "redo.jsonl"
    write_jsonl(src, recs)
    code = run_main(
        [
            "--file",
            str(src),
            "--redo-percent",
            "50",
            "--threads-per-process",
            "4",
            "--redo-sleep-secs",
            "1",
            "--long-record",
            "2",
        ],
        factory,
    )
    out = capsys.readouterr()
    assert code == 0, out.err
    assert engine.count_redo_records() == 0, "redo queue must be drained before exit"
    assert_loaded(engine, [r["RECORD_ID"] for r in recs])
    m = TOTAL_RE.search(out.out)
    assert m is not None, out.out
    adds, redos, dropped, errors = (int(x) for x in m.groups())
    assert (adds, dropped, errors) == (len(recs), 0, 0)
    assert redos > 0, "the shared-feature dataset must have generated redo that this run processed"
    assert stats.REDOS_PROCESSED.value == redos and stats.REDO_OUTSTANDING.value == 0
    combined = [line for line in out.out.splitlines() if line.startswith("Combined stats: ")]
    assert combined, "stats interval (long-record/2 = 1s) must have produced a Combined stats line"
    obj = json.loads(combined[-1][len("Combined stats: ") :])
    assert obj["threads"] == {"load_pref": 2, "redo_pref": 2}
    assert obj["mode"] in ("mixed", "redo_drain") and "redo_share_effective" in obj
    assert "Engine stats: {" in out.out


def test_pure_loader_leaves_redo_untouched(tmp_path: Path, factory, engine, capsys) -> None:  # type: ignore[no-untyped-def]
    drain_redo(factory)
    src = tmp_path / "r0.jsonl"
    write_jsonl(src, redo_records(_prefix()))
    code = run_main(
        ["--file", str(src), "--redo-percent", "0", "--threads-per-process", "2"], factory
    )
    out = capsys.readouterr().out
    assert code == 0
    assert engine.count_redo_records() > 0, "redo% = 0 must not process redo"
    assert TOTAL_RE.search(out).group(2) == "0"  # type: ignore[union-attr]
    obj_lines = [line for line in out.splitlines() if line.startswith("Combined stats: ")]
    for line in obj_lines:
        assert "redos" not in json.loads(line[len("Combined stats: ") :])
    drain_redo(factory)


# --------------------------------------------------------------------------
# Live config reload (real config manager)
# --------------------------------------------------------------------------


def test_config_reload_adopts_new_registered_default(factory, engine, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    mgr = factory.create_configmanager()
    config_reload.log_startup_config(factory, engine)
    assert config_reload.reinit_if_stale(factory) is False, "on the registered default: no reinit"

    cfg = mgr.create_config_from_config_id(mgr.get_default_config_id())
    cfg.register_data_source(f"DS_{_prefix().upper()}")
    new_default = mgr.set_default_config(cfg.export(), "config-reload test")
    assert new_default != engine.get_active_config_id()

    assert config_reload.reinit_if_stale(factory) is True
    assert engine.get_active_config_id() == new_default
    assert config_reload.reinit_if_stale(factory) is False

    # Periodic trigger: disabled at 0, active otherwise (forced interval expiry).
    monkeypatch.setenv("SENZING_CONFIG_RELOAD_SECS", "0")
    config_reload.reset()
    config_reload.poll(factory)
    monkeypatch.setenv("SENZING_CONFIG_RELOAD_SECS", "1")
    config_reload.reset()
    config_reload.poll(factory)  # first observation adopts silently
    config_reload.poll(factory)  # throttled
    monkeypatch.setenv("SENZING_CONFIG_RELOAD_SECS", "not-a-number")
    config_reload.reset()
    config_reload.poll(factory)


# --------------------------------------------------------------------------
# Subprocess: real signals, teardown, environment bring-up failure
# --------------------------------------------------------------------------


def _cli(*args: str) -> list[str]:
    return [sys.executable, "-m", "sz_file_combined_consumer", *args]


def _pump(stream, sink: list[str], event_text: str, event: threading.Event) -> None:  # type: ignore[no-untyped-def]
    for raw in stream:
        line = raw.decode(errors="replace")
        sink.append(line)
        if event_text in line:
            event.set()


def _start(
    args: list[str], watch_for: str
) -> tuple[subprocess.Popen[bytes], list[str], threading.Event]:
    proc = subprocess.Popen(
        _cli(*args), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=os.environ.copy()
    )
    lines: list[str] = []
    seen = threading.Event()
    threading.Thread(target=_pump, args=(proc.stdout, lines, watch_for, seen), daemon=True).start()
    return proc, lines, seen


def test_pure_redoer_drains_then_stops_on_sigterm(factory, engine) -> None:  # type: ignore[no-untyped-def]
    drain_redo(factory)
    for rec in redo_records(_prefix()):
        engine.add_record(DS, rec["RECORD_ID"], json.dumps(rec))
    before = engine.count_redo_records()
    assert before > 0, "precondition: the shared-feature dataset must enqueue redo"

    proc, lines, idle = _start(
        [
            "--redo-percent",
            "100",
            "--threads-per-process",
            "2",
            "--redo-sleep-secs",
            "1",
            "--long-record",
            "2",
        ],
        watch_for="No redo records available",
    )
    try:
        assert idle.wait(timeout=120), "".join(lines)
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
    text = "".join(lines)
    assert code == 0, text
    assert engine.count_redo_records() == 0
    m = TOTAL_RE.search(text)
    assert m is not None and m.group(1) == "0" and int(m.group(2)) >= before, text
    assert "Graceful shutdown requested" in text
    assert "Senzing environment destroyed" in text


def _wait_until(lines: list[str], predicate, timeout: float) -> bool:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate(lines):
            return True
        time.sleep(0.05)
    return False


def test_file_load_sigterm_reports_safe_resume(tmp_path: Path, factory, engine) -> None:  # type: ignore[no-untyped-def]
    """SIGTERM mid-load: the reader stops at the next line, the watermark is exact.

    The input is a FIFO so the interruption is deterministic: 30 records are
    written and acknowledged (``-i`` prints one WithInfo line per add), SIGTERM
    is delivered while the reader is blocked waiting for more input, and only
    then are 20 more records written. The reader must refuse them.
    """
    p = _prefix()
    first, second = simple_records(p, 30), simple_records(f"{p}b", 20)
    fifo = tmp_path / "records.fifo"
    os.mkfifo(fifo)
    proc, lines, _ = _start(
        ["--file", str(fifo), "--redo-percent", "0", "--threads-per-process", "2", "-i"],
        watch_for="File loader:",
    )
    try:
        with open(fifo, "w") as writer:
            for rec in first:
                writer.write(json.dumps(rec) + "\n")
            writer.flush()
            acked = lambda ls: sum('"AFFECTED_ENTITIES"' in line for line in ls) >= len(first)  # noqa: E731
            assert _wait_until(lines, acked, timeout=60), "".join(lines)
            proc.send_signal(signal.SIGTERM)
            assert _wait_until(
                lines,
                lambda ls: any("Graceful shutdown requested" in line for line in ls),
                timeout=30,
            ), "".join(lines)
            for rec in second:
                writer.write(json.dumps(rec) + "\n")
        code = proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
    text = "".join(lines)
    assert code == 0, text
    m = RESUME_RE.search(text)
    assert m is not None and int(m.group(1)) == len(first), text
    assert "stopped before EOF" in text and "stopping file read at line 30" in text
    assert_loaded(engine, [r["RECORD_ID"] for r in first])
    assert_not_loaded(engine, [r["RECORD_ID"] for r in second])
    assert TOTAL_RE.search(text).group(1) == str(len(first))  # type: ignore[union-attr]


def test_environment_init_failure_exits_255(tmp_path: Path) -> None:
    src = tmp_path / "x.jsonl"
    write_jsonl(src, [])
    env = os.environ.copy()
    env["SENZING_ENGINE_CONFIGURATION_JSON"] = json.dumps(
        {
            "PIPELINE": {
                "CONFIGPATH": "/nonexistent",
                "RESOURCEPATH": "/nonexistent",
                "SUPPORTPATH": "/nonexistent",
            },
            "SQL": {"CONNECTION": "sqlite3://na:na@/nonexistent-dir-for-sz-test/G2C.db"},
        }
    )
    proc = subprocess.run(
        _cli("--file", str(src), "--redo-percent", "0"),
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    assert proc.returncode == 255, proc.stdout + proc.stderr
    assert "Failed to initialize Senzing environment" in proc.stderr


# --------------------------------------------------------------------------
# Redo worker paths driven with real engine errors
# --------------------------------------------------------------------------


def test_redo_worker_drops_bad_redo_record(factory, engine, caplog) -> None:  # type: ignore[no-untyped-def]
    """A malformed redo record is bad input to process_redo_record: the worker
    logs the engine error, counts a drop, and keeps going (no queue to reject to)."""
    from sz_file_combined_consumer import worker
    from sz_file_combined_consumer.channels import ClosableQueue

    redo = worker.RedoSide(jobs=ClosableQueue(2))
    ctx = worker.WorkerCtx(
        worker_id=0,
        worker_class=worker.WorkerClass.REDO_PREFERRING,
        factory=factory,
        engine=engine,
        load=None,
        redo=redo,
        add_flags=0,
        redo_flags=0,
        want_info=False,
    )
    stats.REDO_OUTSTANDING.add()
    with caplog.at_level(logging.WARNING):
        keep_going = worker._process_redo(ctx, redo, (1, "this is not a redo record"))
    assert keep_going is True
    assert stats.REDOS_DROPPED.value == 1 and stats.REDOS_PROCESSED.value == 0
    assert stats.REDO_OUTSTANDING.value == 0 and not redo.in_flight
    assert "REDO FAILED due to bad data or timeout" in caplog.text
    assert not stats.WORKER_FATAL.is_set()
