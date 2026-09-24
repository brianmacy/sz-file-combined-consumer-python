"""Process runtime: logging, environment bring-up, signal handling, dispatch to
the run mode, bounded native teardown, exit codes.

Exit codes: **0** clean shutdown / file EOF + redo drained, **1** configuration
or validation failure at startup, **255** fatal runtime error (engine / DB)
after an orderly teardown.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from collections.abc import Sequence
from types import FrameType

from senzing import SzAbstractFactory, SzEngine

from . import INSTANCE_NAME, file_loader, pure_redoer, stats
from .config import Config, ConfigError, parse_args, resolve

log = logging.getLogger(__name__)

TEARDOWN_GRACE = 5.0
"""Upper bound on the native environment destroy at shutdown (it can block)."""

EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_FATAL = 255

_LOG_LEVELS = {
    "notset": logging.DEBUG,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.ERROR,
    "critical": logging.ERROR,
}


def init_logging(level_name: str | None = None) -> None:
    """Configure logging from ``SENZING_LOG_LEVEL`` (default info)."""
    name = (level_name or os.environ.get("SENZING_LOG_LEVEL", "info")).lower()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=_LOG_LEVELS.get(name, logging.INFO),
        force=True,
    )


def create_environment(config: Config) -> tuple[SzAbstractFactory, SzEngine]:
    """Exactly one Senzing environment per process; the engine handle is shared
    by every thread (engine calls release the GIL; connections are per-OS-thread
    inside libSz)."""
    from senzing_core import SzAbstractFactoryCore  # lazy: needs libSz on the path

    factory = SzAbstractFactoryCore(
        INSTANCE_NAME, config.engine_config, verbose_logging=int(config.debug_trace)
    )
    return factory, factory.create_engine()


def install_shutdown_handler() -> None:
    """SIGINT/SIGTERM/SIGHUP -> request graceful shutdown (main thread only)."""

    def handler(signum: int, _frame: FrameType | None) -> None:
        log.warning("Graceful shutdown requested (signal %d)", signum)
        stats.RUNNING.clear()

    if threading.current_thread() is not threading.main_thread():
        return
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, handler)


def run(config: Config, factory: SzAbstractFactory, engine: SzEngine) -> tuple[bool, str | None]:
    """Library entry: run the configured mode with a caller-owned environment."""
    stats.reset_all()
    if config.redo_percent == 100:
        return pure_redoer.run(config, factory, engine)
    return file_loader.run(config, factory, engine)


def teardown(factory: SzAbstractFactory, workers_clean: bool) -> None:
    """Destroy the native environment, time-bounded; skipped when a worker is
    still inside an engine call (leak-on-exit over use-after-free)."""
    if not workers_clean:
        log.warning(
            "worker still in an uninterruptible engine call at shutdown; skipping native "
            "teardown and forcing process exit (restart-on-failure restarts clean)"
        )
        return
    t = threading.Thread(target=factory.destroy, name="sz-teardown", daemon=True)
    t.start()
    t.join(timeout=TEARDOWN_GRACE)
    if t.is_alive():
        log.warning(
            "native teardown did not complete within %.0fs; forcing process exit", TEARDOWN_GRACE
        )
    else:
        log.info("Senzing environment destroyed; exiting cleanly")


def main(argv: Sequence[str] | None = None, factory: SzAbstractFactory | None = None) -> int:
    """CLI main. Returns the process exit code.

    ``factory`` lets an embedding caller (tests) supply its own environment;
    the caller then owns its lifetime and no teardown is performed here.
    """
    init_logging()
    try:
        config = resolve(parse_args(argv))
    except ConfigError as err:
        print(err, file=sys.stderr)
        return EXIT_CONFIG

    owns_env = factory is None
    try:
        if factory is None:
            factory, engine = create_environment(config)
        else:
            engine = factory.create_engine()
    except Exception as err:
        print(f"Failed to initialize Senzing environment: {err}", file=sys.stderr)
        return EXIT_FATAL

    install_shutdown_handler()
    workers_clean, error = run(config, factory, engine)
    code = EXIT_OK
    if error is not None:
        print(error, file=sys.stderr)
        code = EXIT_FATAL
    if owns_env:
        teardown(factory, workers_clean)
    if not workers_clean:
        _hard_exit(code)
    return code


def _hard_exit(code: int) -> None:
    """Exit WITHOUT interpreter finalization: a thread wedged in a native call
    must not be able to hold the process past the SIGTERM grace."""
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


def entry() -> None:
    """Console-script entry point."""
    sys.exit(main())
