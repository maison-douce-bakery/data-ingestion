"""Invoke the deployed extract Lambda (used by Dagu).

Usage: python -m open_meteo.invoke [--run-date D] [--lookback-days N] [--start-date D] [--end-date D] [--locations A,B]
Env: INGEST_FUNCTION_NAME (default mdb-prod-ingest-open-meteo), AWS_REGION / AWS_DEFAULT_REGION (default ap-southeast-2).
"""

import argparse
import json
import os
import sys

import botocore.session
from botocore.config import Config

DEFAULT_FUNCTION = "mdb-prod-ingest-open-meteo"
DEFAULT_REGION = "ap-southeast-2"


def make_client():
    """Lambda client without retries: the function runs up to 300 s and a retry would duplicate the extract."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or DEFAULT_REGION
    config = Config(read_timeout=330, connect_timeout=10, retries={"max_attempts": 0})
    return botocore.session.get_session().create_client("lambda", region_name=region, config=config)


def build_event(args: argparse.Namespace) -> dict:
    """Only the options that were given; the handler applies its own defaults."""
    event = {}
    for key in ("run_date", "lookback_days", "start_date", "end_date"):
        value = getattr(args, key)
        if value is not None:
            event[key] = value
    if args.locations:
        event["locations"] = [c.strip() for c in args.locations.split(",") if c.strip()]
    return event


def invoke(function: str, event: dict, client=None) -> dict:
    """Call the Lambda synchronously and return its payload; raise SystemExit on any failure."""
    client = client or make_client()
    response = client.invoke(
        FunctionName=function,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode(),
    )
    raw = response["Payload"].read()
    if response.get("FunctionError"):
        raise SystemExit(f"{function} failed ({response['FunctionError']}): {raw.decode(errors='replace')}")
    try:
        payload = json.loads(raw)
    except ValueError:
        raise SystemExit(f"{function} returned invalid JSON: {raw.decode(errors='replace')[:500]}")
    if not isinstance(payload, dict) or payload.get("status") != "ok":
        raise SystemExit(f"{function} returned status != ok: {json.dumps(payload)}")
    return payload


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Invoke the Open-Meteo extract Lambda.")
    p.add_argument("--function", default=os.environ.get("INGEST_FUNCTION_NAME", DEFAULT_FUNCTION))
    p.add_argument("--run-date")
    p.add_argument("--lookback-days", type=int)
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--locations", help="comma separated location codes")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    payload = invoke(args.function, build_event(args))
    print(json.dumps(payload))


if __name__ == "__main__":
    main(sys.argv[1:])
