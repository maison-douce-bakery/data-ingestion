"""dlt resource for Open-Meteo hourly weather: API calls and array-to-row reshaping."""

import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import dlt
import requests

from .config import (
    API_URL,
    DEFAULT_LOOKBACK_DAYS,
    HOURLY_VARIABLES,
    MAX_CHUNK_DAYS,
    SOURCE,
    TABLE,
    TIMEZONE,
    Location,
)

# Explicit types so all-null columns still exist and observed_at is a timestamptz.
COLUMNS = {
    "location_code": {"data_type": "text", "nullable": False},
    "latitude": {"data_type": "double"},
    "longitude": {"data_type": "double"},
    "observed_at_local": {"data_type": "text", "nullable": False},
    "utc_offset_seconds": {"data_type": "bigint"},
    "observed_at": {"data_type": "timestamp", "nullable": False},
    "temperature_2m": {"data_type": "double"},
    "apparent_temperature": {"data_type": "double"},
    "relative_humidity_2m": {"data_type": "double"},
    "precipitation": {"data_type": "double"},
    "rain": {"data_type": "double"},
    "weather_code": {"data_type": "bigint"},
    "cloud_cover": {"data_type": "double"},
    "shortwave_radiation": {"data_type": "double"},
    "wind_speed_10m": {"data_type": "double"},
    "source": {"data_type": "text"},
    "api_fetched_at": {"data_type": "timestamp"},
    "window_start": {"data_type": "date"},
    "window_end": {"data_type": "date"},
}

RETRY_STATUS = {429, 500, 502, 503, 504}


class ApiError(RuntimeError):
    pass


def resolve_window(event: dict, today: date) -> tuple[date, date, date]:
    """Return (run_date, window_start, window_end) from a Lambda event."""
    run_date = date.fromisoformat(event["run_date"]) if event.get("run_date") else today
    end = date.fromisoformat(event["end_date"]) if event.get("end_date") else run_date - timedelta(days=1)
    if event.get("start_date"):
        start = date.fromisoformat(event["start_date"])
    else:
        lookback = int(event.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
        if lookback < 1:
            raise ValueError("lookback_days must be >= 1")
        start = end - timedelta(days=lookback - 1)
    if start > end:
        raise ValueError(f"start_date {start} is after end_date {end}")
    return run_date, start, end


def chunk_window(start: date, end: date, max_days: int = MAX_CHUNK_DAYS) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive chunks of at most max_days days."""
    chunks, cur = [], start
    while cur <= end:
        stop = min(cur + timedelta(days=max_days - 1), end)
        chunks.append((cur, stop))
        cur = stop + timedelta(days=1)
    return chunks


def fetch_archive(
    session: requests.Session,
    loc: Location,
    start: date,
    end: date,
    retries: int = 5,
    backoff: float = 2.0,
    timeout: float = 30.0,
) -> dict:
    """GET one archive chunk; retry connection errors, 429 and 5xx with backoff."""
    params = {
        "latitude": loc.latitude,
        "longitude": loc.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": TIMEZONE,
    }
    for attempt in range(retries):
        last = attempt == retries - 1
        try:
            resp = session.get(API_URL, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if last:
                raise ApiError(f"Open-Meteo unreachable after {retries} attempts: {exc}") from exc
            time.sleep(backoff**attempt)
            continue
        if resp.status_code in RETRY_STATUS and not last:
            retry_after = resp.headers.get("Retry-After", "")
            time.sleep(float(retry_after) if retry_after.isdigit() else backoff**attempt)
            continue
        if resp.status_code != 200:
            raise ApiError(f"Open-Meteo HTTP {resp.status_code}: {resp.text[:300]}")
        payload = resp.json()
        if payload.get("error"):
            raise ApiError(f"Open-Meteo error: {payload.get('reason')}")
        return payload
    raise ApiError("unreachable")


def rows_from_response(
    loc: Location,
    payload: dict,
    fetched_at: datetime,
    window_start: date,
    window_end: date,
) -> Iterator[dict]:
    """Turn the parallel hourly arrays into one dict per hour."""
    hourly = payload["hourly"]
    times = hourly["time"]
    offset = int(payload["utc_offset_seconds"])
    for var in HOURLY_VARIABLES:
        if var in hourly and len(hourly[var]) != len(times):
            raise ApiError(f"{var} has {len(hourly[var])} values for {len(times)} hours")
    for i, local in enumerate(times):
        local_dt = datetime.fromisoformat(local)
        row = {
            "location_code": loc.code,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "observed_at_local": local,
            "utc_offset_seconds": offset,
            "observed_at": (local_dt - timedelta(seconds=offset)).replace(tzinfo=timezone.utc),
        }
        for var in HOURLY_VARIABLES:
            row[var] = hourly[var][i] if var in hourly else None
        row.update(
            source=SOURCE,
            api_fetched_at=fetched_at,
            window_start=window_start,
            window_end=window_end,
        )
        yield row


@dlt.resource(name=TABLE, write_disposition="append", columns=COLUMNS)
def hourly_weather(
    locations: list[Location],
    window_start: date,
    window_end: date,
    session: requests.Session | None = None,
) -> Iterator[dict]:
    session = session or requests.Session()
    for loc in locations:
        for start, end in chunk_window(window_start, window_end):
            payload = fetch_archive(session, loc, start, end)
            fetched_at = datetime.now(timezone.utc)
            yield from rows_from_response(loc, payload, fetched_at, window_start, window_end)
