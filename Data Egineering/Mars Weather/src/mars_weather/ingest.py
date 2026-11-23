"""Batch ingestion: NASA InSight API -> raw JSON in the object store.

Usage
    python -m mars_weather.ingest                      # live pull (needs NASA_API_KEY)
    python -m mars_weather.ingest --from-file FILE     # land a saved payload (no key, no network)

Each run appends ONE immutable object:
    raw/insight_weather/dt=YYYYMMDD/insight_weather_YYYYMMDDTHHMMSSZ.json

Runs never overwrite history. The API returns an overlapping rolling window of sols, so the same
sol can appear in many files; de-duplication is the warehouse's job (see
``dbt/models/staging/stg_insight__sol_payloads.sql``), which keeps the raw layer replayable.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Mapping, Optional

from mars_weather.config import RAW_PREFIX, Settings
from mars_weather.nasa_client import InsightApiError, InsightPayloadError, InsightWeatherClient, validate_payload
from mars_weather.storage import LocalObjectStore, ObjectStore

logger = logging.getLogger("mars_weather.ingest")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 2


@dataclass(frozen=True)
class IngestResult:
    key: str
    uri: str
    sol_keys: List[str]
    size_bytes: int


def raw_object_key(fetched_at: datetime) -> str:
    """Object key for a payload fetched at ``fetched_at`` (always expressed in UTC).

    The dbt source parses the timestamp back out of this name, so changing the layout means
    changing ``stg_insight__sol_payloads.sql`` too (a test pins this contract).
    """
    utc = fetched_at.astimezone(timezone.utc)
    return f"{RAW_PREFIX}/dt={utc:%Y%m%d}/insight_weather_{utc:%Y%m%dT%H%M%SZ}.json"


def land_raw_payload(raw: bytes, store: ObjectStore, fetched_at: Optional[datetime] = None) -> IngestResult:
    """Validate ``raw`` and write it, byte for byte, to the object store.

    Validation happens *before* the write, so a malformed response never pollutes the raw layer.
    """
    sol_keys = validate_payload(raw)
    key = raw_object_key(fetched_at or datetime.now(timezone.utc))
    uri = store.put(key, raw)
    logger.info("Landed %d bytes (%d sols: %s) at %s", len(raw), len(sol_keys), ", ".join(sol_keys) or "none", uri)
    if not sol_keys:
        logger.warning("The API returned no valid sols; the file was landed for audit but adds no data.")
    return IngestResult(key=key, uri=uri, sol_keys=sol_keys, size_bytes=len(raw))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Land NASA InSight weather data in the object store.")
    parser.add_argument("--from-file", type=Path, help="Land a previously saved API response instead of calling the API.")
    return parser


def main(
    argv: Optional[List[str]] = None,
    env: Optional[Mapping[str, str]] = None,
    client: Optional[InsightWeatherClient] = None,
) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _build_parser().parse_args(argv)
    settings = Settings.from_env(env)
    store = LocalObjectStore(settings.lake_path)

    try:
        if args.from_file:
            raw = args.from_file.read_bytes()
        else:
            if client is None:
                if not settings.nasa_api_key:
                    logger.error("NASA_API_KEY is not set. Copy .env.example to .env and add your key (or use --from-file).")
                    return EXIT_CONFIG
                client = InsightWeatherClient(settings.nasa_api_key, base_url=settings.api_url)
            raw = client.fetch_raw()
        land_raw_payload(raw, store)
    except (InsightApiError, InsightPayloadError, OSError) as exc:
        logger.error("Ingestion failed: %s", exc)
        return EXIT_FAILURE
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
