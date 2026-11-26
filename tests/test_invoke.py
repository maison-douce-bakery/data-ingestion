import io
import json

import pytest

from open_meteo.invoke import DEFAULT_FUNCTION, build_event, invoke, main, parse_args

OK = {"status": "ok", "rows": 24}


class FakeClient:
    def __init__(self, payload, function_error=None):
        self.payload = payload
        self.function_error = function_error
        self.calls = []

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        response = {"Payload": io.BytesIO(self.payload if isinstance(self.payload, bytes) else json.dumps(self.payload).encode())}
        if self.function_error:
            response["FunctionError"] = self.function_error
        return response


def test_success_returns_payload_and_sends_event():
    client = FakeClient(OK)
    assert invoke("fn", {"run_date": "2026-11-26"}, client) == OK
    call = client.calls[0]
    assert call["FunctionName"] == "fn"
    assert call["InvocationType"] == "RequestResponse"
    assert json.loads(call["Payload"]) == {"run_date": "2026-11-26"}


def test_function_error_raises():
    with pytest.raises(SystemExit, match="Unhandled"):
        invoke("fn", {}, FakeClient({"errorMessage": "boom"}, function_error="Unhandled"))


def test_status_not_ok_raises():
    with pytest.raises(SystemExit, match="status"):
        invoke("fn", {}, FakeClient({"status": "error"}))


def test_invalid_json_raises():
    with pytest.raises(SystemExit, match="invalid JSON"):
        invoke("fn", {}, FakeClient(b"not json"))


def test_event_only_has_given_options():
    assert build_event(parse_args([])) == {}
    args = parse_args(["--run-date", "2026-11-26", "--lookback-days", "7", "--locations", "A, B"])
    assert build_event(args) == {"run_date": "2026-11-26", "lookback_days": 7, "locations": ["A", "B"]}


def test_parse_args_function_default_and_env(monkeypatch):
    monkeypatch.delenv("INGEST_FUNCTION_NAME", raising=False)
    assert parse_args([]).function == DEFAULT_FUNCTION
    monkeypatch.setenv("INGEST_FUNCTION_NAME", "from-env")
    assert parse_args([]).function == "from-env"
    assert parse_args(["--function", "x"]).function == "x"


def test_main_prints_one_line(monkeypatch, capsys):
    monkeypatch.setattr("open_meteo.invoke.make_client", lambda: FakeClient(OK))
    main(["--start-date", "2026-01-01", "--end-date", "2026-01-31"])
    assert json.loads(capsys.readouterr().out) == OK
