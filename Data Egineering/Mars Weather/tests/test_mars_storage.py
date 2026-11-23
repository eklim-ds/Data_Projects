import pytest

from mars_weather.storage import InvalidKeyError, LocalObjectStore


def test_round_trip_and_keys_cannot_escape_the_store(tmp_path):
    store = LocalObjectStore(tmp_path / "lake")
    store.put("raw/a.json", b"{}")
    assert store.get("raw/a.json") == b"{}"
    assert store.list("raw/") == ["raw/a.json"]

    with pytest.raises(InvalidKeyError):
        store.put("../escape.json", b"x")
    assert not (tmp_path / "escape.json").exists()
