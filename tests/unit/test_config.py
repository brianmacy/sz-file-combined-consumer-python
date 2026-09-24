from __future__ import annotations

import pytest

from sz_file_combined_consumer import config as cfg

ENGINE = '{"PIPELINE":{},"SQL":{"CONNECTION":"sqlite3://na:na@/tmp/x.db"}}'


def _args(**over: object) -> cfg.Args:
    base: dict[str, object] = {
        "input_file": "/data/in.jsonl",
        "skip_lines": 0,
        "reject_file": None,
        "redo_percent": 20,
        "threads_per_process": 12,
        "redo_sleep_secs": 60,
        "long_record": 300,
        "info": False,
        "debug_trace": False,
    }
    base.update(over)
    return cfg.Args(**base)  # type: ignore[arg-type]


def test_redo_pref_count_endpoints() -> None:
    assert cfg.redo_preferring_count(12, 0) == 0
    assert cfg.redo_preferring_count(12, 100) == 12
    assert cfg.redo_preferring_count(1, 0) == 0
    assert cfg.redo_preferring_count(1, 100) == 1


def test_redo_pref_count_rounds_and_clamps() -> None:
    assert cfg.redo_preferring_count(12, 20) == 2
    assert cfg.redo_preferring_count(12, 17) == 2
    assert cfg.redo_preferring_count(12, 10) == 1
    assert cfg.redo_preferring_count(12, 1) == 1
    assert cfg.redo_preferring_count(12, 99) == 11
    assert cfg.redo_preferring_count(2, 50) == 1


def test_topology_requires_file_below_100() -> None:
    with pytest.raises(cfg.ConfigError, match="No input file"):
        cfg.validate_topology(12, 0, None)
    with pytest.raises(cfg.ConfigError, match="No input file"):
        cfg.validate_topology(12, 50, "")
    cfg.validate_topology(12, 50, "in.jsonl")
    cfg.validate_topology(12, 100, None)


def test_topology_rejects_file_at_100() -> None:
    with pytest.raises(cfg.ConfigError, match="pure redoer reads no file"):
        cfg.validate_topology(12, 100, "in.jsonl")


def test_topology_requires_two_threads_for_interior_percent() -> None:
    with pytest.raises(cfg.ConfigError, match="at least 2"):
        cfg.validate_topology(1, 50, "in.jsonl")
    cfg.validate_topology(2, 50, "in.jsonl")
    cfg.validate_topology(1, 0, "in.jsonl")
    cfg.validate_topology(1, 100, None)


def test_topology_rejects_out_of_range_percent_and_zero_threads() -> None:
    with pytest.raises(cfg.ConfigError, match=r"\[0, 100\]"):
        cfg.validate_topology(12, 101, "in.jsonl")
    with pytest.raises(cfg.ConfigError, match=r"\[0, 100\]"):
        cfg.validate_topology(12, -1, "in.jsonl")
    with pytest.raises(cfg.ConfigError, match="at least 1"):
        cfg.validate_topology(0, 0, "in.jsonl")


def test_reject_file_resolution() -> None:
    assert cfg.resolve_reject_file(None, "x.jsonl") is None
    assert cfg.resolve_reject_file(None, None) is None
    assert cfg.resolve_reject_file("/data/in.jsonl", None) == "/data/in.jsonl.rejected.jsonl"
    assert cfg.resolve_reject_file("/data/in.jsonl", "") == "/data/in.jsonl.rejected.jsonl"
    assert cfg.resolve_reject_file("/data/in.jsonl", "/out/bad.jsonl") == "/out/bad.jsonl"


def test_engine_config_required_and_validated() -> None:
    with pytest.raises(cfg.ConfigError, match="must be set"):
        cfg.engine_config_from_env({})
    with pytest.raises(cfg.ConfigError, match="not valid JSON"):
        cfg.engine_config_from_env({"SENZING_ENGINE_CONFIGURATION_JSON": "{nope"})
    assert cfg.engine_config_from_env({"SENZING_ENGINE_CONFIGURATION_JSON": ENGINE}) == ENGINE


def test_parse_args_cli_over_env_over_default() -> None:
    env = {
        "SENZING_INPUT_FILE": "/env/in.jsonl",
        "SENZING_REDO_PERCENT": "35",
        "SENZING_THREADS_PER_PROCESS": "4",
        "SENZING_SKIP_LINES": "7",
        "SENZING_REJECT_FILE": "/env/rej.jsonl",
        "SENZING_REDO_SLEEP_TIME_IN_SECONDS": "5",
        "LONG_RECORD": "20",
    }
    a = cfg.parse_args([], environ=env)
    assert a == cfg.Args("/env/in.jsonl", 7, "/env/rej.jsonl", 35, 4, 5, 20, False, False)
    b = cfg.parse_args(["-f", "/cli/in.jsonl", "--redo-percent", "0", "-i", "-t"], environ=env)
    assert (b.input_file, b.redo_percent, b.info, b.debug_trace) == ("/cli/in.jsonl", 0, True, True)
    c = cfg.parse_args([], environ={})
    assert (c.redo_percent, c.threads_per_process, c.long_record, c.redo_sleep_secs) == (
        20,
        12,
        300,
        60,
    )


def test_parse_args_rejects_bad_values_at_parse() -> None:
    with pytest.raises(SystemExit):
        cfg.parse_args(["--long-record", "0"], environ={})
    with pytest.raises(SystemExit):
        cfg.parse_args(["--skip-lines", "-1"], environ={})
    with pytest.raises(cfg.ConfigError, match="must be an integer"):
        cfg.parse_args([], environ={"SENZING_REDO_PERCENT": "twenty"})
    assert cfg.parse_args(["--long-record", "1"], environ={}).long_record == 1


def test_resolve_full_config() -> None:
    env = {"SENZING_ENGINE_CONFIGURATION_JSON": ENGINE}
    c = cfg.resolve(_args(), environ=env)
    assert c.input_file == "/data/in.jsonl"
    assert c.reject_file == "/data/in.jsonl.rejected.jsonl"
    assert c.threads == 12
    assert c.redo_pref_workers() == 2
    assert c.long_record_secs == 300


def test_resolve_zero_threads_uses_cpu_count() -> None:
    env = {"SENZING_ENGINE_CONFIGURATION_JSON": ENGINE}
    c = cfg.resolve(_args(threads_per_process=0, redo_percent=0), environ=env)
    assert c.threads >= 1


def test_resolve_pure_redoer_has_no_file_fields() -> None:
    env = {"SENZING_ENGINE_CONFIGURATION_JSON": ENGINE}
    c = cfg.resolve(_args(input_file=None, redo_percent=100, threads_per_process=1), environ=env)
    assert c.input_file is None and c.reject_file is None
    assert c.redo_pref_workers() == 1


def test_resolve_propagates_validation_errors() -> None:
    env = {"SENZING_ENGINE_CONFIGURATION_JSON": ENGINE}
    with pytest.raises(cfg.ConfigError):
        cfg.resolve(_args(redo_percent=101), environ=env)
    with pytest.raises(cfg.ConfigError):
        cfg.resolve(_args(input_file=""), environ=env)
    with pytest.raises(cfg.ConfigError):
        cfg.resolve(_args(), environ={})
