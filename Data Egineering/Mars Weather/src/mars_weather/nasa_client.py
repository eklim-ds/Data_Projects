"""Client for NASA's InSight: Mars Weather Service API.

Endpoint: https://api.nasa.gov/insight_weather/?feedtype=json&ver=1.0

Design notes
- The API key is sent in the ``X-Api-Key`` header (supported by api.nasa.gov) instead of the
  ``api_key`` query parameter, so it never ends up in URLs, exception messages or logs.
- Only transient failures are retried (timeouts, connection errors, HTTP 429 and 5xx).
  Client errors such as 403 (bad key) fail fast with an actionable message.
- ``fetch_raw`` returns the response *bytes* untouched. The raw layer stores exactly what the
  API said; all reshaping happens later in dbt.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Callable, List, Optional

import requests

from mars_weather.config import DEFAULT_API_URL

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_RETRY_AFTER_SECONDS = 60.0


class InsightApiError(RuntimeError):
    """The API call failed (after retries, or with a non-retryable status)."""


class InsightPayloadError(ValueError):
    """The API answered, but the body is not the InSight weather payload we expect."""


def validate_payload(raw: bytes) -> List[str]:
    """Check the response has the structure the dbt models rely on; return its ``sol_keys``.

    The feed is a JSON object with a ``sol_keys`` list, one object per listed sol (keyed by the
    sol number), and a ``validity_checks`` object. An empty ``sol_keys`` list is legal: it means
    no sol currently has enough data.
    """
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InsightPayloadError(f"Response is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise InsightPayloadError("Expected a JSON object at the top level")

    sol_keys = payload.get("sol_keys")
    if not isinstance(sol_keys, list) or not all(isinstance(key, str) for key in sol_keys):
        raise InsightPayloadError("Missing or malformed 'sol_keys' list")

    if not isinstance(payload.get("validity_checks"), dict):
        raise InsightPayloadError("Missing 'validity_checks' object")

    for key in sol_keys:
        if not key.isdigit():
            raise InsightPayloadError(f"Sol key {key!r} is not a number")
        if not isinstance(payload.get(key), dict):
            raise InsightPayloadError(f"Sol {key} is listed in 'sol_keys' but has no data object")
    return sol_keys


class InsightWeatherClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_API_URL,
        session: Optional[requests.Session] = None,
        timeout: float = 30.0,
        max_attempts: int = 4,
        backoff_seconds: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key:
            raise ValueError("A NASA API key is required (set NASA_API_KEY)")
        self._api_key = api_key
        self.base_url = base_url
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self._sleep = sleep

    def __repr__(self) -> str:  # never include the key
        return f"InsightWeatherClient(base_url={self.base_url!r})"

    def _delay_for(self, attempt: int, response: Optional[requests.Response]) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                return min(float(retry_after), MAX_RETRY_AFTER_SECONDS)
        return min(self.backoff_seconds * (2 ** (attempt - 1)), MAX_RETRY_AFTER_SECONDS)

    def fetch_raw(self) -> bytes:
        """GET the weather feed and return the unmodified response body."""
        params = {"feedtype": "json", "ver": "1.0"}
        headers = {"X-Api-Key": self._api_key, "Accept": "application/json"}
        last_problem = "no attempts made"

        for attempt in range(1, self.max_attempts + 1):
            response: Optional[requests.Response] = None
            try:
                response = self.session.get(self.base_url, params=params, headers=headers, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                # str(exc) can embed the URL; the key is only in headers, so this is safe to log.
                last_problem = f"{type(exc).__name__}"
            else:
                remaining = response.headers.get("X-RateLimit-Remaining")
                if remaining is not None and remaining.isdigit() and int(remaining) < 5:
                    logger.warning("NASA API rate limit nearly exhausted: %s requests left this hour", remaining)

                if response.status_code == 200:
                    return response.content
                if response.status_code not in RETRYABLE_STATUS:
                    hint = " (is NASA_API_KEY valid?)" if response.status_code in (401, 403) else ""
                    raise InsightApiError(f"NASA API returned HTTP {response.status_code}{hint}")
                last_problem = f"HTTP {response.status_code}"

            if attempt < self.max_attempts:
                delay = self._delay_for(attempt, response)
                logger.warning(
                    "Attempt %d/%d failed (%s); retrying in %.1fs", attempt, self.max_attempts, last_problem, delay
                )
                self._sleep(delay)

        raise InsightApiError(f"NASA API request failed after {self.max_attempts} attempts (last problem: {last_problem})")
