# data-ingestion

dlt pipelines that bring external sources into the data platform. First source: Open-Meteo hourly weather.

| Step | Where it runs | What it does |
|---|---|---|
| Extract | AWS Lambda (container, arm64) | Calls the Open-Meteo archive API and writes JSONL to `s3://$DATALAKE_BUCKET/raw/open_meteo/hourly_weather/dt=<run_date>/` |
| Load | Dagu container | Appends files not loaded yet into Postgres `raw.open_meteo_hourly_weather` |

## Extract

Handler: `open_meteo.handler.handler`. Event (all keys optional):

```json
{"run_date": "2026-11-26", "lookback_days": 7, "start_date": "2026-01-01", "end_date": "2026-03-31", "locations": ["HCM_MAIN"]}
```

The window is the `lookback_days` days ending the day before `run_date`; `start_date` / `end_date` override it for backfills.
Locations live in `open_meteo/locations.yaml`. Env: `DATALAKE_BUCKET`.

Everything a source writes, data and dlt metadata (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`, `init`), stays under
`raw/<source>/`, so the Lambda role needs access to that prefix only. The role needs `s3:PutObject` and `s3:GetObject` on
`raw/open_meteo/*` and `s3:ListBucket` on the bucket (dlt's `makedirs` calls `HeadBucket`, which a `s3:prefix` condition denies).

## Load

```
pip install -r requirements-load.txt
python -m open_meteo.load
```

Env: `DATALAKE_BUCKET`, `DP_LOADER_HOST`, `DP_LOADER_PASSWORD`; optional `DP_LOADER_PORT` (5432), `DP_LOADER_USER` (dp_loader),
`DP_LOADER_DBNAME` (bakery), `DP_LOADER_SSLMODE` (require).

### Invoke

Dagu triggers the extract with `python -m open_meteo.invoke --run-date 2026-11-26 --lookback-days 7`. It calls the Lambda
synchronously (no retries, so an extract is never duplicated), prints the response as one JSON line and exits non-zero if the
function fails or `status` is not `ok`. Options: `--start-date`, `--end-date`, `--locations A,B`, `--function`
(default env `INGEST_FUNCTION_NAME`, else `mdb-prod-ingest-open-meteo`). It needs `lambda:InvokeFunction`.

Files are append-only, so overlapping windows load as revisions. Latest value per hour:

```sql
select distinct on (location_code, observed_at) *
from raw.open_meteo_hourly_weather
order by location_code, observed_at, api_fetched_at desc;
```

## Develop

```
pip install -r requirements-dev.txt && pytest
docker build --platform linux/arm64 -t ingest-open-meteo:dev .
```
