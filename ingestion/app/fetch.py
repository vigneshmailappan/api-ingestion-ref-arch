from __future__ import annotations

import gzip
import io
from typing import Any, Dict, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

DEFAULT_TIMEOUT = 20.0  # seconds


class ApiError(Exception):
    pass


@retry(wait=wait_exponential_jitter(initial=1, max=20), stop=stop_after_attempt(5))
def fetch_open_meteo(
    latitude: float,
    longitude: float,
    hourly: str = "temperature_2m,relative_humidity_2m",
    start_date: Optional[str] = None,  # YYYY-MM-DD
    end_date: Optional[str] = None,    # YYYY-MM-DD
    timeout: float = DEFAULT_TIMEOUT,
    user_agent: str = "ingest-api/0.1 (+https://example.local)",
) -> Dict[str, Any]:
    """
    Fetches data from Open-Meteo API with retries.
    Returns parsed JSON as dict. Raises ApiError for non-2xx or malformed JSON.
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": hourly,
        "timezone": "UTC",
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    with httpx.Client(timeout=timeout, headers=headers) as client:
        resp = client.get(base_url, params=params)
        if resp.status_code // 100 != 2:
            raise ApiError(f"Open-Meteo error: {resp.status_code} {resp.text}")
        try:
            return resp.json()
        except Exception as e:
            raise ApiError(f"Failed to parse JSON: {e}") from e


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(data)
    return buf.getvalue()


def fetch_raw_bytes(
    latitude: float,
    longitude: float,
    hourly: str = "temperature_2m,relative_humidity_2m",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    timeout: float = DEFAULT_TIMEOUT,
    user_agent: str = "ingest-api/0.1 (+https://example.local)",
) -> bytes:
    """Fetch raw response bytes (for optional _raw_bytes retention)."""
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": hourly,
        "timezone": "UTC",
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    with httpx.Client(timeout=timeout, headers=headers) as client:
        resp = client.get(base_url, params=params)
        if resp.status_code // 100 != 2:
            raise ApiError(f"Open-Meteo error: {resp.status_code} {resp.text}")
        return resp.content
#``