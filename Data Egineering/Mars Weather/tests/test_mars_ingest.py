from datetime import datetime, timezone

import pytest

pytest.importorskip("requests")

from mars_weather import ingest  # noqa: E402
from mars_weather.storage import LocalObjectStore  # noqa: E402

FETCHED_AT = datetime(2026, 3, 9, 14, 5, 7, tzinfo=timezone.utc)


def test_valid_payload_lands_byte_for_byte_under_the_utc_key(tmp_path, sample_bytes):
    store = LocalObjectStore(tmp_path)
    result = ingest.land_raw_payload(sample_bytes, store, fetched_at=FETCHED_AT)

    # This key layout is the contract the dbt source parses, so it is pinned here.
    assert result.key == "raw/insight_weather/dt=20260309/insight_weather_20260309T140507Z.json"
    assert store.get(result.key) == sample_bytes
    assert result.sol_keys == ["675", "676", "677", "678", "679", "680", "681"]


def test_invalid_payload_is_rejected_before_anything_is_written(tmp_path):
    store = LocalObjectStore(tmp_path)
    with pytest.raises(ValueError):
        ingest.land_raw_payload(b'{"oops": true}', store, fetched_at=FETCHED_AT)
    assert store.list() == []
