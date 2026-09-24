"""CLI paths that need no engine: configuration failures exit 1 before any
Senzing call."""

from __future__ import annotations

import pytest

from sz_file_combined_consumer import runtime

ENGINE = '{"PIPELINE":{},"SQL":{"CONNECTION":"sqlite3://na:na@/tmp/x.db"}}'


def test_missing_engine_config_exits_1(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("SENZING_ENGINE_CONFIGURATION_JSON", raising=False)
    monkeypatch.delenv("SENZING_INPUT_FILE", raising=False)
    assert runtime.main(["--file", "x.jsonl"]) == runtime.EXIT_CONFIG
    assert "SENZING_ENGINE_CONFIGURATION_JSON must be set" in capsys.readouterr().err


def test_missing_file_below_100_exits_1(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("SENZING_ENGINE_CONFIGURATION_JSON", ENGINE)
    monkeypatch.delenv("SENZING_INPUT_FILE", raising=False)
    assert runtime.main([]) == runtime.EXIT_CONFIG
    assert "No input file provided" in capsys.readouterr().err


def test_out_of_range_percent_exits_1(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("SENZING_ENGINE_CONFIGURATION_JSON", ENGINE)
    assert runtime.main(["--file", "x.jsonl", "--redo-percent", "101"]) == runtime.EXIT_CONFIG
    assert "[0, 100]" in capsys.readouterr().err


def test_argparse_rejects_long_record_zero(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("SENZING_ENGINE_CONFIGURATION_JSON", ENGINE)
    with pytest.raises(SystemExit) as exc:
        runtime.main(["--file", "x.jsonl", "--long-record", "0"])
    assert exc.value.code == 2


def test_init_logging_levels(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import logging

    runtime.init_logging("debug")
    assert logging.getLogger().level == logging.DEBUG
    runtime.init_logging("nonsense")
    assert logging.getLogger().level == logging.INFO
    monkeypatch.setenv("SENZING_LOG_LEVEL", "warning")
    runtime.init_logging()
    assert logging.getLogger().level == logging.WARNING


def test_install_shutdown_handler_is_noop_off_main_thread() -> None:
    import threading

    done = threading.Event()
    t = threading.Thread(target=lambda: (runtime.install_shutdown_handler(), done.set()))
    t.start()
    t.join(timeout=5)
    assert done.is_set()
