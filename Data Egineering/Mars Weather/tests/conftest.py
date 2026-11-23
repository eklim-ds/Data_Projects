"""Shared fixtures.

Design rules for this test suite
- No test touches the network or needs a real API key: the NASA API is faked, and fixtures are built
  from a real saved response (sample_data/insight_weather_sample.json).
- Heavy dependencies (dbt, duckdb, streamlit) are imported lazily inside fixtures / via importorskip, so
  that if this folder is collected by the monorepo's root pytest run (where they are not installed) the
  affected tests are skipped instead of erroring.
"""
from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"
SAMPLE_PATH = PROJECT_ROOT / "sample_data" / "insight_weather_sample.json"

sys.path.insert(0, str(PROJECT_ROOT / "src"))

T1 = datetime(2026, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
T2 = T1 + timedelta(days=1)


@pytest.fixture(scope="session")
def sample_bytes() -> bytes:
    return SAMPLE_PATH.read_bytes()


@pytest.fixture(scope="session")
def sample_payload(sample_bytes) -> dict:
    return json.loads(sample_bytes)


def build_window(sample: dict, sols: List[str], mutate: Optional[Callable[[dict], None]] = None) -> bytes:
    """A payload shaped like the API's, holding only ``sols`` (a smaller rolling window)."""
    payload: Dict[str, object] = {"sol_keys": list(sols), "validity_checks": copy.deepcopy(sample["validity_checks"])}
    for sol in sols:
        payload[sol] = copy.deepcopy(sample[sol])
    if mutate:
        mutate(payload)
    return json.dumps(payload).encode()


@pytest.fixture(scope="session")
def window_factory(sample_payload):
    return lambda sols, mutate=None: build_window(sample_payload, sols, mutate)


def _run_dbt_build(lake: Path, warehouse: Path, workdir: Path) -> None:
    """Run `dbt build` exactly as a user would, in its own process.

    A subprocess (not dbt's in-process runner) because dbt-duckdb keeps the database file open for the
    life of the process, which would block the read-only connections the tests open afterwards.
    """
    env = {**os.environ, "LAKE_PATH": str(lake), "WAREHOUSE_PATH": str(warehouse), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    command = [
        sys.executable, "-c", "from dbt.cli.main import cli; cli()",  # the `dbt` console script's entry point
        "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
        "--target-path", str(workdir / "target"), "--log-path", str(workdir / "logs"), "--no-use-colors",
    ]
    proc = subprocess.run(command, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"dbt build failed (exit {proc.returncode}):\n{proc.stdout[-6000:]}\n{proc.stderr[-2000:]}")


def _build_warehouse(root: Path, landings: list) -> Path:
    """Land (payload_bytes, fetched_at) pairs into a fresh lake, then run the real dbt project on it."""
    from mars_weather.ingest import land_raw_payload
    from mars_weather.storage import LocalObjectStore

    lake, warehouse = root / "lake", root / "warehouse.duckdb"
    store = LocalObjectStore(lake)
    for payload, fetched_at in landings:
        land_raw_payload(payload, store, fetched_at=fetched_at)
    _run_dbt_build(lake, warehouse, root)
    return warehouse


@pytest.fixture(scope="session")
def rolling_window_warehouse(tmp_path_factory, window_factory) -> Path:
    """Two overlapping daily pulls, the way a live feed behaves.

    Day 1 returns sols 675-679. Day 2 returns 677-681 and revises sol 679's average temperature;
    it also reports sol 678 with almost no wind-sensor coverage (a data-quality problem).
    """
    pytest.importorskip("dbt.cli.main")
    pytest.importorskip("duckdb")

    def day2_mutations(payload: dict) -> None:
        payload["679"]["AT"]["av"] = -50.0
        payload["validity_checks"]["678"]["HWS"]["sol_hours_with_data"] = [0, 1, 2]

    day1 = window_factory(["675", "676", "677", "678", "679"])
    day2 = window_factory(["677", "678", "679", "680", "681"], day2_mutations)
    return _build_warehouse(tmp_path_factory.mktemp("rolling"), [(day1, T1), (day2, T2)])


@pytest.fixture(scope="session")
def frozen_feed_warehouse(tmp_path_factory, sample_bytes) -> Path:
    """The real-world situation today: NASA's feed is frozen, so every pull returns the same 7 sols."""
    pytest.importorskip("dbt.cli.main")
    pytest.importorskip("duckdb")
    return _build_warehouse(tmp_path_factory.mktemp("frozen"), [(sample_bytes, T1), (sample_bytes, T2)])
