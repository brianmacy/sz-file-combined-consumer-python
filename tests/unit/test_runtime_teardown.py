from __future__ import annotations

import logging

from sz_file_combined_consumer import runtime


def test_teardown_skips_native_destroy_when_workers_not_clean(caplog) -> None:  # type: ignore[no-untyped-def]
    with caplog.at_level(logging.WARNING):
        runtime.teardown(None, workers_clean=False)  # type: ignore[arg-type]  # never touched
    assert "skipping native teardown" in caplog.text
