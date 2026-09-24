"""Configuration: CLI arguments with environment-variable fallbacks.

Priority: CLI argument > environment variable > default. Environment variable
names are verbatim-compatible with the sibling drivers
(``sz_queue_combined_consumer``, ``sz_rabbit_consumer-v4``,
``sz_simple_redoer-v4``) so existing compose files need minimal changes.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

DEFAULT_LONG_RECORD_SECS = 300
"""Long-record threshold, seconds (stats cadence = LONG_RECORD / 2)."""

DEFAULT_REDO_PERCENT = 20
"""Default redo share of worker capacity, in percent."""

DEFAULT_THREADS = 12
"""Default worker-pool size; scale by processes, not threads (0 -> CPU count)."""

DEFAULT_REDO_SLEEP_SECS = 60
"""Fetcher pause when ``get_redo_record()`` comes back empty, seconds."""

DEFAULT_REJECT_SUFFIX = ".rejected.jsonl"
"""Suffix appended to the input path when ``--reject-file`` is not given."""

ENGINE_CONFIG_HELP = (
    "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set "
    "with a proper JSON configuration.\n"
    "Please see https://senzing.zendesk.com/hc/en-us/articles/"
    "360038774134-G2Module-Configuration-and-the-Senzing-API"
)


class ConfigError(Exception):
    """A loud, user-facing configuration/validation failure (exit code 1)."""


@dataclass(frozen=True)
class Args:
    """Parsed command line (before validation / env resolution)."""

    input_file: str | None
    skip_lines: int
    reject_file: str | None
    redo_percent: int
    threads_per_process: int
    redo_sleep_secs: int
    long_record: int
    info: bool
    debug_trace: bool


@dataclass(frozen=True)
class Config:
    """Fully resolved, validated runtime configuration."""

    engine_config: str
    input_file: str | None
    """``None`` iff redo% = 100 (pure redoer)."""
    skip_lines: int
    reject_file: str | None
    """Always set in file mode (explicit or derived); ``None`` at redo% = 100."""
    redo_percent: int
    threads: int
    redo_sleep_secs: int
    long_record_secs: int
    info: bool
    debug_trace: bool

    def redo_pref_workers(self) -> int:
        """|B|: number of redo-preferring workers."""
        return redo_preferring_count(self.threads, self.redo_percent)


def _env_int(environ: Mapping[str, str], key: str, default: int) -> int:
    raw = environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as err:
        raise ConfigError(f"{key} must be an integer, got {raw!r}") from err


def _positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {value}")
    return value


def _non_negative_int(text: str) -> int:
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0, got {value}")
    return value


def build_parser(environ: Mapping[str, str] | None = None) -> argparse.ArgumentParser:
    """argparse parser whose defaults come from the environment."""
    env = os.environ if environ is None else environ
    p = argparse.ArgumentParser(
        prog="sz_file_combined_consumer",
        description=(
            "Combined Senzing driver: add_record from a JSONL file and "
            "process_redo_record, split by one redo%% knob "
            "(0 = pure loader, 100 = pure redoer)"
        ),
    )
    p.add_argument(
        "-f",
        "--file",
        dest="input_file",
        default=env.get("SENZING_INPUT_FILE"),
        help="JSONL input file (one JSON record per line) [env: SENZING_INPUT_FILE]",
    )
    p.add_argument(
        "--skip-lines",
        type=_non_negative_int,
        default=_env_int(env, "SENZING_SKIP_LINES", 0),
        help="skip the first N physical lines (resume) [env: SENZING_SKIP_LINES]",
    )
    p.add_argument(
        "--reject-file",
        default=env.get("SENZING_REJECT_FILE"),
        help=(
            "JSONL file receiving every rejected input line verbatim "
            f"(default: <input>{DEFAULT_REJECT_SUFFIX}) [env: SENZING_REJECT_FILE]"
        ),
    )
    p.add_argument(
        "--redo-percent",
        type=int,
        default=_env_int(env, "SENZING_REDO_PERCENT", DEFAULT_REDO_PERCENT),
        help="share (%%) of workers preferring redo, in [0, 100] [env: SENZING_REDO_PERCENT]",
    )
    p.add_argument(
        "--threads-per-process",
        type=_non_negative_int,
        default=_env_int(env, "SENZING_THREADS_PER_PROCESS", DEFAULT_THREADS),
        help="worker thread count; 0 = CPU count [env: SENZING_THREADS_PER_PROCESS]",
    )
    p.add_argument(
        "--redo-sleep-secs",
        type=_non_negative_int,
        default=_env_int(env, "SENZING_REDO_SLEEP_TIME_IN_SECONDS", DEFAULT_REDO_SLEEP_SECS),
        help="fetcher pause when no redo is available [env: SENZING_REDO_SLEEP_TIME_IN_SECONDS]",
    )
    p.add_argument(
        "--long-record",
        type=_positive_int,
        default=_env_int(env, "LONG_RECORD", DEFAULT_LONG_RECORD_SECS),
        help="long-record threshold, seconds (>= 1); stats cadence is half [env: LONG_RECORD]",
    )
    p.add_argument(
        "-i", "--info", action="store_true", help="print the WithInfo response per record"
    )
    p.add_argument(
        "-t", "--debugTrace", dest="debug_trace", action="store_true", help="engine debug trace"
    )
    return p


def parse_args(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> Args:
    """Parse ``argv`` (default ``sys.argv[1:]``) with env-derived defaults."""
    ns = build_parser(environ).parse_args(argv)
    return Args(
        input_file=ns.input_file,
        skip_lines=ns.skip_lines,
        reject_file=ns.reject_file,
        redo_percent=ns.redo_percent,
        threads_per_process=ns.threads_per_process,
        redo_sleep_secs=ns.redo_sleep_secs,
        long_record=ns.long_record,
        info=ns.info,
        debug_trace=ns.debug_trace,
    )


def engine_config_from_env(environ: Mapping[str, str] | None = None) -> str:
    """Read and JSON-validate ``SENZING_ENGINE_CONFIGURATION_JSON``."""
    env = os.environ if environ is None else environ
    engine_config = env.get("SENZING_ENGINE_CONFIGURATION_JSON", "")
    if not engine_config:
        raise ConfigError(ENGINE_CONFIG_HELP)
    try:
        json.loads(engine_config)
    except ValueError as err:
        raise ConfigError("SENZING_ENGINE_CONFIGURATION_JSON is not valid JSON") from err
    return engine_config


def resolve_reject_file(input_file: str | None, explicit: str | None) -> str | None:
    """Explicit ``--reject-file`` if non-empty, else ``<input>.rejected.jsonl``; None w/o file."""
    if input_file is None:
        return None
    return explicit if explicit else f"{input_file}{DEFAULT_REJECT_SUFFIX}"


def redo_preferring_count(threads: int, redo_percent: int) -> int:
    """|B| = clamp(round(N * redo% / 100), 1, N-1) for interior redo%; 0 at 0; N at 100."""
    match redo_percent:
        case 0:
            return 0
        case 100:
            return threads
        case pct:
            raw = round(threads * pct / 100.0)
            return max(1, min(raw, max(threads - 1, 1)))


def validate_topology(threads: int, redo_percent: int, input_file: str | None) -> None:
    """Loud startup gate for the (threads, redo%, file) topology."""
    if not 0 <= redo_percent <= 100:
        raise ConfigError(f"SENZING_REDO_PERCENT must be within [0, 100], got {redo_percent}")
    if redo_percent < 100 and not input_file:
        raise ConfigError(
            "No input file provided (use --file or SENZING_INPUT_FILE); required when redo% < 100"
        )
    if redo_percent == 100 and input_file:
        raise ConfigError(
            "--file cannot be combined with redo% = 100: the pure redoer reads no file "
            "(use redo% < 100 to load the file and drain redo in one run)"
        )
    if 0 < redo_percent < 100 and threads < 2:
        raise ConfigError(
            f"0 < redo% < 100 requires at least 2 worker threads (got {threads}): "
            "one worker cannot host both preference classes"
        )
    if threads < 1:
        raise ConfigError("at least 1 worker thread is required")


def resolve(args: Args, environ: Mapping[str, str] | None = None) -> Config:
    """Resolve + validate :class:`Args` against the environment (raises ConfigError)."""
    engine_config = engine_config_from_env(environ)
    threads = args.threads_per_process or (os.cpu_count() or 1)
    input_file = args.input_file or None
    validate_topology(threads, args.redo_percent, input_file)
    return Config(
        engine_config=engine_config,
        input_file=input_file,
        skip_lines=args.skip_lines,
        reject_file=resolve_reject_file(input_file, args.reject_file),
        redo_percent=args.redo_percent,
        threads=threads,
        redo_sleep_secs=args.redo_sleep_secs,
        long_record_secs=args.long_record,
        info=args.info,
        debug_trace=args.debug_trace,
    )
