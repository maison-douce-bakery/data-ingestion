import json
from pathlib import Path

from open_meteo import source
from open_meteo.config import FILE_GLOB

PAYLOAD = json.loads((Path(__file__).parent / "fixtures_archive.json").read_text())


def test_everything_lands_under_the_source_prefix(tmp_path, monkeypatch):
    from open_meteo.extract import run

    monkeypatch.setattr(source, "fetch_archive", lambda *a, **k: PAYLOAD)
    monkeypatch.setenv("DATALAKE_URL", tmp_path.as_uri())
    out = run({"run_date": "2026-11-26", "lookback_days": 3, "locations": ["HCM_MAIN"]})

    keys = {p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()}
    assert len(out["files"]) == 1 and out["files"][0] in keys
    assert out["files"][0].startswith("raw/open_meteo/hourly_weather/dt=2026-11-26/")
    assert all(k.startswith("raw/open_meteo/") for k in keys)
    assert {"raw/open_meteo/init"} <= keys
    assert any(k.startswith("raw/open_meteo/_dlt_loads/") for k in keys)
    # The loader's glob matches exactly the data file.
    assert [k for k in keys if Path(k).match(FILE_GLOB)] == out["files"]
