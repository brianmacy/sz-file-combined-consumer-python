"""Shared fixtures.

Real-engine fixtures activate only when ``SENZING_ENGINE_CONFIGURATION_JSON``
points at an initialized Senzing repository (schema applied + default config
registered). No mocks anywhere: tests that need the engine skip without it and
fail loudly when the engine is present but a step that should succeed does not.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator

import pytest

from sz_file_combined_consumer import INSTANCE_NAME, config_reload, stats

TEST_DATA_SOURCE = "TEST"


@pytest.fixture(autouse=True)
def _reset_globals() -> Iterator[None]:
    stats.reset_all()
    config_reload.reset()
    yield
    stats.reset_all()
    config_reload.reset()


def engine_settings() -> str | None:
    value = os.environ.get("SENZING_ENGINE_CONFIGURATION_JSON", "")
    return value or None


@pytest.fixture(scope="session")
def factory():  # type: ignore[no-untyped-def]
    """Session-wide Senzing environment (process singleton) with the TEST data
    source registered; same instance name/settings as the driver so an
    in-process ``main(..., factory=...)`` shares it."""
    settings = engine_settings()
    if settings is None:
        pytest.skip("SENZING_ENGINE_CONFIGURATION_JSON not set (needs a real Senzing engine)")
    from senzing_core import SzAbstractFactoryCore

    fac = SzAbstractFactoryCore(INSTANCE_NAME, settings, verbose_logging=0)
    mgr = fac.create_configmanager()
    if not mgr.get_default_config_id():
        cfg = mgr.create_config_from_template()
        mgr.set_default_config(cfg.export(), "test initial configuration")
    cfg = mgr.create_config_from_config_id(mgr.get_default_config_id())
    if TEST_DATA_SOURCE not in json.loads(cfg.get_data_source_registry()).get("DATA_SOURCES", []):
        cfg.register_data_source(TEST_DATA_SOURCE)
        mgr.set_default_config(cfg.export(), "test data source")
        fac.reinitialize(mgr.get_default_config_id())
    yield fac
    fac.destroy()


@pytest.fixture
def engine(factory):  # type: ignore[no-untyped-def]
    return factory.create_engine()
