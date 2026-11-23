"""Runtime configuration, read from environment variables with safe local defaults.

Nothing secret has a default: the NASA API key must come from the environment
(``NASA_API_KEY``), typically via an untracked ``.env`` file loaded by the Makefile.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_API_URL = "https://api.nasa.gov/insight_weather/"

# Object-store key prefix for raw API payloads. The dbt source reads this exact layout:
#   raw/insight_weather/dt=YYYYMMDD/insight_weather_YYYYMMDDTHHMMSSZ.json
RAW_PREFIX = "raw/insight_weather"


@dataclass(frozen=True)
class Settings:
    # repr=False so the key can never leak through logging a Settings object.
    nasa_api_key: Optional[str] = field(default=None, repr=False)
    api_url: str = DEFAULT_API_URL
    lake_path: Path = PROJECT_ROOT / "data" / "lake"
    warehouse_path: Path = PROJECT_ROOT / "data" / "warehouse" / "mars_weather.duckdb"

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "Settings":
        env = os.environ if env is None else env
        defaults = cls()
        return cls(
            nasa_api_key=(env.get("NASA_API_KEY") or "").strip() or None,
            api_url=env.get("NASA_API_URL", defaults.api_url),
            lake_path=Path(env.get("LAKE_PATH", defaults.lake_path)),
            warehouse_path=Path(env.get("WAREHOUSE_PATH", defaults.warehouse_path)),
        )
