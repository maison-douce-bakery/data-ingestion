import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from open_meteo.config import HOURLY_VARIABLES, load_locations
from open_meteo.source import (
    ApiError,
    chunk_window,
    fetch_archive,
    resolve_window,
    rows_from_response,
)

PAYLOAD = json.loads((Path(__file__).parent / "fixtures_archive.json").read_text())
LOC = load_locations()["HCM_MAIN"]
FETCHED = datetime(2026, 11, 26, 1, 0, tzinfo=timezone.utc)


def rows():
    return list(rows_from_response(LOC, PAYLOAD, FETCHED, date(2026, 11, 23), date(2026, 11, 25)))


def test_one_row_per_hour_with_contract_columns():
    out = rows()
    assert len(out) == 3
    assert set(out[0]) == {
        "location_code", "latitude", "longitude", "observed_at_local",
        "utc_offset_seconds", "observed_at", *HOURLY_VARIABLES,
        "source", "api_fetched_at", "window_start", "window_end",
    }
    assert out[0]["location_code"] == "HCM_MAIN"
    assert out[0]["source"] == "open_meteo_archive"
    assert out[0]["window_start"] == date(2026, 11, 23)
    assert out[2]["precipitation"] == 0.2


def test_nulls_are_kept():
    out = rows()
    assert out[1]["temperature_2m"] is None
    assert out[2]["weather_code"] is None


def test_utc_conversion_across_offset():
    out = rows()
    # 00:00 local (+07:00) is 17:00 UTC the previous day
    assert out[0]["observed_at"] == datetime(2026, 11, 22, 17, 0, tzinfo=timezone.utc)
    assert out[1]["observed_at"] == datetime(2026, 11, 22, 18, 0, tzinfo=timezone.utc)
    assert out[2]["observed_at"] == datetime(2026, 11, 23, 0, 0, tzinfo=timezone.utc)
    assert out[0]["observed_at_local"] == "2026-11-23T00:00"


def test_length_mismatch_raises():
    bad = json.loads(json.dumps(PAYLOAD))
    bad["hourly"]["rain"] = [0.0]
    with pytest.raises(ApiError):
        list(rows_from_response(LOC, bad, FETCHED, date(2026, 11, 23), date(2026, 11, 25)))


def test_default_window():
    run, start, end = resolve_window({"run_date": "2026-11-26"}, today=date(2026, 1, 1))
    assert (run, start, end) == (date(2026, 11, 26), date(2026, 11, 19), date(2026, 11, 25))


def test_default_run_date_is_today_and_lookback():
    run, start, end = resolve_window({"lookback_days": 3}, today=date(2026, 11, 26))
    assert (run, start, end) == (date(2026, 11, 26), date(2026, 11, 23), date(2026, 11, 25))


def test_explicit_dates_override():
    ev = {"run_date": "2026-11-26", "start_date": "2026-01-01", "end_date": "2026-03-31"}
    assert resolve_window(ev, today=date(2026, 11, 26)) == (
        date(2026, 11, 26), date(2026, 1, 1), date(2026, 3, 31),
    )


def test_bad_windows_raise():
    with pytest.raises(ValueError):
        resolve_window({"start_date": "2026-02-01", "end_date": "2026-01-01"}, today=date(2026, 3, 1))
    with pytest.raises(ValueError):
        resolve_window({"lookback_days": 0}, today=date(2026, 3, 1))


def test_chunks_cover_window_without_overlap():
    chunks = chunk_window(date(2026, 1, 1), date(2026, 4, 10), max_days=90)
    assert chunks == [
        (date(2026, 1, 1), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 4, 10)),
    ]
    assert chunk_window(date(2026, 1, 1), date(2026, 1, 1)) == [(date(2026, 1, 1), date(2026, 1, 1))]


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers, self.text = status, body or {}, headers or {}, str(body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return self.responses.pop(0)


def test_retries_429_then_succeeds(monkeypatch):
    monkeypatch.setattr("open_meteo.source.time.sleep", lambda s: None)
    session = FakeSession([FakeResponse(429), FakeResponse(503), FakeResponse(200, PAYLOAD)])
    out = fetch_archive(session, LOC, date(2026, 11, 23), date(2026, 11, 25))
    assert out["utc_offset_seconds"] == 25200 and session.calls == 3


def test_gives_up_and_rejects_api_errors(monkeypatch):
    monkeypatch.setattr("open_meteo.source.time.sleep", lambda s: None)
    with pytest.raises(ApiError):
        fetch_archive(FakeSession([FakeResponse(429)] * 3), LOC, date(2026, 1, 1), date(2026, 1, 2), retries=3)
    with pytest.raises(ApiError):
        fetch_archive(
            FakeSession([FakeResponse(400, {"error": True, "reason": "bad"})]),
            LOC, date(2026, 1, 1), date(2026, 1, 2),
        )
    with pytest.raises(ApiError):
        fetch_archive(
            FakeSession([FakeResponse(200, {"error": True, "reason": "bad"})]),
            LOC, date(2026, 1, 1), date(2026, 1, 2),
        )
