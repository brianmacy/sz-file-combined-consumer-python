"""Live engine-config reload: adopt a new registered DEFAULT config without a restart.

Senzing does not auto-update a running engine when the registered default
config changes; the engine keeps the config it was initialized with until
``reinitialize`` is called. This module lets an operator bump the registered
default and have every worker converge onto it within one poll interval.

The reload is keyed on ``get_default_config_id()`` (reliable) differing from
the default this process has ADOPTED — never on ``get_active_config_id()``,
which can report 0 on the settings-JSON init path (GDEV-4313) and would cause
a perpetual reinit storm.

Two triggers feed one double-checked reconcile:

1. PERIODIC — every looping engine thread calls :func:`poll`; a process-global
   throttle collapses that to one ``get_default_config_id`` round-trip per
   ``SENZING_CONFIG_RELOAD_SECS`` (default 60; 0 disables) per process.
2. ERROR-DRIVEN — on an ``add_record`` / ``process_redo_record`` error the
   caller asks :func:`reinit_if_stale`; if the default changed the engine is
   reinitialized and the caller RETRIES once.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from senzing import SzAbstractFactory, SzEngine

log = logging.getLogger(__name__)

_NO_DEFAULT: int | None = None


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reinit_lock = threading.Lock()
        self.adopted_default: int | None = _NO_DEFAULT
        self.last_poll: float = 0.0
        self.interval: float | None = None
        self.interval_loaded = False


_state = _State()


def reset() -> None:
    """Forget adopted default / throttle state (tests)."""
    global _state
    _state = _State()


def _poll_interval() -> float | None:
    if not _state.interval_loaded:
        try:
            secs = int(os.environ.get("SENZING_CONFIG_RELOAD_SECS", "60"))
        except ValueError:
            secs = 60
        _state.interval = float(secs) if secs > 0 else None
        _state.interval_loaded = True
    return _state.interval


def poll(factory: SzAbstractFactory) -> None:
    """PERIODIC trigger: cheap no-op unless this thread claims the interval slot."""
    interval = _poll_interval()
    if interval is None:
        return
    now = time.monotonic()
    with _state.lock:
        if now - _state.last_poll < interval:
            return
        _state.last_poll = now
    try:
        _reconcile(factory)
    except Exception as err:
        log.warning("periodic config check failed: %s", err)


def reinit_if_stale(factory: SzAbstractFactory) -> bool:
    """ERROR-DRIVEN trigger: True iff the default changed (engine now reinitialized -> retry)."""
    try:
        return _reconcile(factory)
    except Exception as err:
        log.warning("config staleness check failed: %s", err)
        return False


def _reconcile(factory: SzAbstractFactory) -> bool:
    default = factory.create_configmanager().get_default_config_id()
    adopted = _state.adopted_default
    if adopted is _NO_DEFAULT:
        # First observation before startup seeding: the engine already runs it.
        _state.adopted_default = default
        return False
    if default == adopted:
        return False
    with _state.reinit_lock:
        default = factory.create_configmanager().get_default_config_id()
        if default == _state.adopted_default:
            return True  # a peer adopted it while we waited
        factory.reinitialize(default)
        _state.adopted_default = default
        log.info(
            "CONFIG REFRESHED: registered default %s -> %s; reinitialize(%s) done",
            adopted,
            default,
            default,
        )
        try:
            log.info("LICENSE AFTER REINIT: %s", factory.create_product().get_license())
        except Exception as err:
            log.warning("get_license after reinit failed: %s", err)
        return True


def log_startup_config(factory: SzAbstractFactory, engine: SzEngine) -> None:
    """Seed the adopted default with what the engine loaded at init and log both ids."""
    active: int | str
    default: int | str
    try:
        active = engine.get_active_config_id()
    except Exception as err:
        active = f"error: {err}"
    try:
        default = factory.create_configmanager().get_default_config_id()
        _state.adopted_default = default
    except Exception as err:
        default = f"error: {err}"
    log.info(
        "CONFIG AT INIT: active=%s default=%s (config-reload keys off registered-default change)",
        active,
        default,
    )
