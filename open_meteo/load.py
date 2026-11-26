"""Load pipeline: new JSONL files in the data lake -> Postgres raw.open_meteo_hourly_weather.

Usage: python -m open_meteo.load
"""

import json
import os
from typing import Iterator
from urllib.parse import quote

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.sources.filesystem import filesystem

from .config import FILE_GLOB, bucket_url
from .source import COLUMNS

SCHEMA = "raw"
TABLE = "open_meteo_hourly_weather"


def connection_string() -> str:
    """Build the Postgres URL from DP_LOADER_* (no dlt env-var credential convention)."""
    env = os.environ
    for name in ("DP_LOADER_HOST", "DP_LOADER_PASSWORD"):
        if not env.get(name):
            raise RuntimeError(f"{name} is not set")
    user = quote(env.get("DP_LOADER_USER", "dp_loader"), safe="")
    password = quote(env["DP_LOADER_PASSWORD"], safe="")
    port = env.get("DP_LOADER_PORT", "5432")
    db = quote(env.get("DP_LOADER_DBNAME", "bakery"), safe="")
    ssl = env.get("DP_LOADER_SSLMODE", "require")
    return f"postgresql://{user}:{password}@{env['DP_LOADER_HOST']}:{port}/{db}?sslmode={ssl}"


def raw_files():
    """New files only: modification_date is the incremental cursor kept in dlt state."""
    files = filesystem(bucket_url=bucket_url(), file_glob=FILE_GLOB)
    files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    return files


def make_rows(counter: dict):
    @dlt.transformer(name=TABLE, write_disposition="append", columns=COLUMNS)
    def rows(items: Iterator[FileItemDict]) -> Iterator[dict]:
        for item in items:
            counter["files"] += 1
            with item.open() as f:
                for line in f:
                    row = json.loads(line)
                    # The extract run's own dlt ids are replaced by this load's.
                    row.pop("_dlt_load_id", None)
                    row.pop("_dlt_id", None)
                    yield row

    return rows


def run() -> dict:
    counter = {"files": 0}
    rows = raw_files() | make_rows(counter)

    destination = dlt.destinations.postgres(credentials=connection_string())
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_load",
        destination=destination,
        dataset_name=SCHEMA,
    )
    info = pipeline.run(rows)
    info.raise_on_failed_jobs()
    loaded = pipeline.last_trace.last_normalize_info.row_counts.get(TABLE, 0)
    return {
        "files_read": counter["files"],
        "rows_loaded": loaded,
        "load_id": info.loads_ids[0] if info.loads_ids else None,
    }


if __name__ == "__main__":
    print(json.dumps(run()))
