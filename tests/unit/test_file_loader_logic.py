from __future__ import annotations

from pathlib import Path

from sz_file_combined_consumer import stats
from sz_file_combined_consumer.file_loader import LoadState, RejectSink, ResumeTracker


def test_watermark_advances_contiguously_and_handles_out_of_order() -> None:
    t = ResumeTracker(6)  # skip = 5
    assert t.watermark == 5
    t.complete(8)
    assert t.watermark == 5
    t.complete(6)
    assert t.watermark == 6
    t.complete(7)
    assert t.watermark == 8


def test_watermark_ignores_lines_at_or_below_start() -> None:
    t = ResumeTracker(1)
    for n in (1, 2, 3):
        t.complete(n)
    assert t.watermark == 3
    t.complete(2)
    assert t.watermark == 3


def test_reject_sink_is_lazy_appends_and_counts(tmp_path: Path) -> None:
    path = tmp_path / "in.jsonl.rejected.jsonl"
    sink = RejectSink(str(path))
    assert not path.exists(), "sink must not create the file until the first write"
    assert sink.written == 0
    sink.write_line(b'{"A":1}')
    sink.write_line(b'{"B":2}')
    assert sink.written == 2
    sink.close()
    sink.close()  # idempotent
    sink2 = RejectSink(str(path))
    sink2.write_line(b'{"C":3}')
    sink2.close()
    assert path.read_text() == '{"A":1}\n{"B":2}\n{"C":3}\n'


def test_reject_sink_reports_unwritable_path() -> None:
    sink = RejectSink("/nonexistent-dir-for-sz-test/x.jsonl")
    try:
        sink.write_line(b"{}")
    except OSError:
        pass
    else:
        raise AssertionError("expected OSError")
    assert sink.written == 0


def test_load_state_reject_counts_even_when_sink_fails(caplog, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    ok = LoadState(ResumeTracker(1), RejectSink(str(tmp_path / "r.jsonl")))
    ok.reject(1, b'{"X":1}')
    assert stats.ADDS_REJECTED.value == 1 and ok.sink.written == 1
    bad = LoadState(ResumeTracker(1), RejectSink("/nonexistent-dir-for-sz-test/x.jsonl"))
    bad.reject(2, b'{"Y":2}')
    assert stats.ADDS_REJECTED.value == 2 and bad.sink.written == 0
    assert 'record follows: {"Y":2}' in caplog.text


def test_load_state_load_complete_requires_reader_done_and_empty_in_flight(tmp_path: Path) -> None:
    st = LoadState(ResumeTracker(1), RejectSink(str(tmp_path / "r.jsonl")))
    assert not st.load_complete()
    st.reader_done.set()
    assert st.load_complete()
    st.in_flight[3] = b"{}"
    assert not st.load_complete()
    st.in_flight.pop(3)
    st.complete(1)
    assert st.watermark == 1
    assert st.load_complete()
