"""Extract pipeline: Open-Meteo archive API -> JSONL files in the data lake (dlt filesystem)."""

import shutil
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

import dlt
from dlt.common.storages import fsspec_from_config
from dlt.destinations import filesystem

from .config import DATASET, LAYOUT, TABLE, TIMEZONE, bucket_url, load_locations
from .source import hourly_weather, resolve_window


def run(event: dict) -> dict:
    """Run one extract for a Lambda-style event and return the handler response."""
    all_locations = load_locations()
    codes = event.get("locations") or list(all_locations)
    unknown = [c for c in codes if c not in all_locations]
    if unknown:
        raise ValueError(f"unknown locations: {unknown}")
    today = datetime.now(ZoneInfo(TIMEZONE)).date()
    run_date, start, end = resolve_window(event, today)

    dlt.config["data_writer.disable_compression"] = True  # plain .jsonl
    destination = filesystem(
        bucket_url=bucket_url(),
        layout=LAYOUT,
        # Keep the "/" in dataset_name: it is the per-source prefix, not a schema name.
        enable_dataset_name_normalization=False,
        extra_placeholders={"run_date": run_date.isoformat()},
    )
    # A fresh working dir per run: no state is carried over, nothing outside /tmp is written.
    workdir = tempfile.mkdtemp(prefix="dlt-")
    try:
        pipeline = dlt.pipeline(
            pipeline_name="open_meteo_extract",
            pipelines_dir=workdir,
            destination=destination,
            dataset_name=DATASET,
        )
        info = pipeline.run(
            hourly_weather([all_locations[c] for c in codes], start, end),
            loader_file_format="jsonl",
        )
        rows = pipeline.last_trace.last_normalize_info.row_counts.get(TABLE, 0)
        fs, base = fsspec_from_config(pipeline.destination_client().config)
        prefix = f"{base}/{DATASET}/{TABLE}/dt={run_date.isoformat()}/{info.loads_ids[0]}."
        files = sorted(
            p.removeprefix(base).lstrip("/") for p in fs.glob(prefix + "*")
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    info.raise_on_failed_jobs()
    return {
        "status": "ok",
        "run_date": run_date.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "locations": codes,
        "rows": rows,
        "files": files,
    }
