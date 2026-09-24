from __future__ import annotations

import json
import threading

from sz_file_combined_consumer import stats


def test_counter_is_thread_safe() -> None:
    c = stats.Counter()

    def bump() -> None:
        for _ in range(10_000):
            c.add()

    threads = [threading.Thread(target=bump) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert c.value == 80_000
    assert c.sub(80_000) == 0
    c.add(5)
    c.reset()
    assert c.value == 0


def test_flag_reset_restores_initial() -> None:
    f = stats.Flag(True)
    f.clear()
    assert not f.is_set()
    f.reset()
    assert f.is_set()
    g = stats.Flag(False)
    g.set()
    g.reset()
    assert not g.is_set()


def test_mark_fatal_and_reset_all() -> None:
    stats.mark_fatal()
    assert stats.ERRORS.value == 1
    assert stats.WORKER_FATAL.is_set()
    assert not stats.RUNNING.is_set()
    stats.reset_all()
    assert stats.ERRORS.value == 0
    assert not stats.WORKER_FATAL.is_set()
    assert stats.RUNNING.is_set()


def test_throughput_ticker_fires_only_on_interval_boundary_crossings(capsys) -> None:  # type: ignore[no-untyped-def]
    t = stats.ThroughputTicker()
    assert t.observe(0, 1) is None
    assert t.observe(9_999, 9_999) is None
    assert t.observe(9_999, 10_000) is not None
    assert t.observe(10_000, 10_001) is None
    t.report(19_999, 20_000)
    assert capsys.readouterr().out.startswith("Processed 20000 adds, ")


def test_ewma() -> None:
    e = stats.Ewma(0.3)
    assert e.value is None
    assert e.update(10.0) == 10.0
    e2 = stats.Ewma(0.5)
    e2.update(0.0)
    assert e2.update(10.0) == 5.0


def _line(**over: object) -> stats.StatusLine:
    base: dict[str, object] = {
        "redo_percent": 20,
        "load_pref": 10,
        "redo_pref": 2,
        "adds": 0,
        "adds_rate": 0.0,
        "redos": 0,
        "redos_rate": 0.0,
        "load_active": True,
        "redo_backlog": None,
        "redo_backlog_slope": None,
    }
    base.update(over)
    return stats.StatusLine(**base)  # type: ignore[arg-type]


def test_mode_derivation() -> None:
    assert stats.mode(_line()) == "mixed"
    assert stats.mode(_line(load_active=False)) == "redo_drain"
    assert stats.mode(_line(redo_percent=0)) == "load_only"
    assert stats.mode(_line(redo_percent=100, load_active=None)) == "redo_only"


def test_status_object_endpoint_field_omission() -> None:
    at0 = stats.status_object(_line(redo_percent=0))
    assert "redos" not in at0 and "adds" in at0 and at0["load_active"] is True
    at100 = stats.status_object(_line(redo_percent=100, load_active=None))
    assert "adds" not in at100 and "redos" in at100 and "load_active" not in at100
    mixed = stats.status_object(
        _line(
            adds=5,
            adds_rate=1.26,
            redos=3,
            redos_rate=0.44,
            redo_backlog=7,
            redo_backlog_slope=-0.55,
        )
    )
    assert mixed["adds_rate"] == 1.3 and mixed["redos_rate"] == 0.4
    assert mixed["redo_backlog"] == 7 and mixed["redo_backlog_slope"] == -0.6
    assert mixed["threads"] == {"load_pref": 10, "redo_pref": 2}
    assert "redo_share_effective" not in mixed


def test_status_line_share_and_print(capsys) -> None:  # type: ignore[no-untyped-def]
    stats.LOAD_BUSY_NS.add(3_000)
    stats.REDO_BUSY_NS.add(1_000)
    stats.emit_status_line(_line())
    out = capsys.readouterr().out
    assert out.startswith("Combined stats: ")
    obj = json.loads(out[len("Combined stats: ") :])
    assert obj["redo_share_effective"] == 0.25
    assert obj["mode"] == "mixed"


def test_floor_guard_trips_after_k_flat_intervals_and_clears() -> None:
    g = stats.FloorGuard()
    assert g.observe(100, 50, None) is stats.GuardTransition.UNCHANGED
    tripped_at = None
    for i in range(1, stats.FLOOR_GUARD_INTERVALS + 1):
        if g.observe(100 + i, 50, None) is stats.GuardTransition.TRIPPED:
            tripped_at = i
    assert tripped_at == stats.FLOOR_GUARD_INTERVALS
    # Still suspicious afterwards -> unchanged (already active), then clears on a jump.
    assert g.observe(200, 50, None) is stats.GuardTransition.UNCHANGED
    assert g.observe(300, 500, None) is stats.GuardTransition.CLEARED


def test_floor_guard_not_suspicious_while_load_active_or_without_progress() -> None:
    g = stats.FloorGuard()
    g.observe(100, 50, True)
    for i in range(1, stats.FLOOR_GUARD_INTERVALS * 2):
        assert g.observe(100 + i, 50, True) is stats.GuardTransition.UNCHANGED
    h = stats.FloorGuard()
    h.observe(100, 50, None)
    for _ in range(stats.FLOOR_GUARD_INTERVALS * 2):
        assert h.observe(100, 50, None) is stats.GuardTransition.UNCHANGED
    # load_active=False (file fully loaded) counts as idle, like None.
    k = stats.FloorGuard()
    k.observe(1, 50, False)
    for i in range(1, stats.FLOOR_GUARD_INTERVALS):
        assert k.observe(1 + i, 50, False) is stats.GuardTransition.UNCHANGED
    assert k.observe(99, 50, False) is stats.GuardTransition.TRIPPED
