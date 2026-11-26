"""Constants and settings shared by the extract and load pipelines."""

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

API_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE = "open_meteo_archive"
TIMEZONE = "Asia/Ho_Chi_Minh"
# Hourly variables requested from the API; the API names are the column names.
HOURLY_VARIABLES = (
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "weather_code",
    "cloud_cover",
    "shortwave_radiation",
    "wind_speed_10m",
)
MAX_CHUNK_DAYS = 90
DEFAULT_LOOKBACK_DAYS = 7
# Layout of the raw files in the bucket; run_date is the ingestion date.
TABLE = "hourly_weather"
# One prefix per source: data files and dlt's own metadata (_dlt_*, init) all live under it.
DATASET = "raw/open_meteo"
LAYOUT = "{table_name}/dt={run_date}/{load_id}.{file_id}.{ext}"
FILE_GLOB = "raw/open_meteo/hourly_weather/dt=*/*.jsonl"
LOCATIONS_FILE = Path(__file__).with_name("locations.yaml")


@dataclass(frozen=True)
class Location:
    code: str
    latitude: float
    longitude: float
    timezone: str


def load_locations(path: Path = LOCATIONS_FILE) -> dict[str, Location]:
    raw = yaml.safe_load(path.read_text())
    return {
        code: Location(code, float(v["latitude"]), float(v["longitude"]), v["timezone"])
        for code, v in raw.items()
    }


def bucket_url() -> str:
    """DATALAKE_URL (tests, file:// or s3://) wins over DATALAKE_BUCKET."""
    url = os.environ.get("DATALAKE_URL")
    if url:
        return url.rstrip("/")
    bucket = os.environ.get("DATALAKE_BUCKET")
    if not bucket:
        raise RuntimeError("DATALAKE_BUCKET (or DATALAKE_URL) is not set")
    return f"s3://{bucket}"
