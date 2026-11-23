import pytest

pytest.importorskip("requests")

from mars_weather.nasa_client import InsightApiError, InsightWeatherClient  # noqa: E402

SECRET = "super-secret-key-123"
OK_BODY = b'{"sol_keys": [], "validity_checks": {}}'


class FakeResponse:
    def __init__(self, status=200, content=OK_BODY, headers=None):
        self.status_code, self.content, self.headers = status, content, headers or {}


class FakeSession:
    """Plays back scripted responses and records every request."""

    def __init__(self, *script):
        self.script, self.calls = list(script), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers})
        return self.script.pop(0)


def test_retries_a_transient_error_and_never_leaks_the_key():
    sleeps = []
    session = FakeSession(FakeResponse(503), FakeResponse(200))
    client = InsightWeatherClient(SECRET, session=session, sleep=sleeps.append, backoff_seconds=1)

    assert client.fetch_raw() == OK_BODY
    assert sleeps == [1]
    for call in session.calls:
        assert call["headers"]["X-Api-Key"] == SECRET  # sent as a header...
        assert SECRET not in call["url"]  # ...never in the URL


def test_a_rejected_key_fails_fast_with_a_hint_and_without_the_key():
    session = FakeSession(FakeResponse(403))
    client = InsightWeatherClient(SECRET, session=session, sleep=lambda _: None)

    with pytest.raises(InsightApiError) as err:
        client.fetch_raw()
    assert len(session.calls) == 1  # not retried
    assert "NASA_API_KEY" in str(err.value)
    assert SECRET not in str(err.value)
