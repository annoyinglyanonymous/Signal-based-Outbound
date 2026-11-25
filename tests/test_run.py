"""run.py: failures alert, successes ping, and the alert says what broke."""
import pytest

import run


@pytest.fixture
def calls(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(run, "notify", lambda url, text: sent.append(("alert", url, text)))
    monkeypatch.setattr(run, "ping", lambda url, suffix="", body=None: sent.append(("ping", url, suffix)))
    monkeypatch.setattr(run, "REPORT_DIR", tmp_path)
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/alert")
    monkeypatch.setenv("HEALTHCHECK_URL_DAILY", "https://hc.example/daily")
    return sent


def fake_daily(pull=None, load_error=None, signals_ok=True):
    import load
    import pull_daily
    import signals

    def load_run(mode, day):
        if load_error:
            raise RuntimeError(load_error)
        return {"ok": True, "tables": {}}
    return {
        (pull_daily, "run"): lambda: pull or {"ok": True, "source_dates": ["2026-11-24"], "datasets": {}},
        (load, "run"): load_run,
        (signals, "run"): lambda rules: {"ok": signals_ok, "created": {"renewal": 3 if signals_ok else "failed: boom"}},
    }


def run_job(monkeypatch, fakes, job="daily"):
    for (mod, name), fn in fakes.items():
        monkeypatch.setattr(mod, name, fn)
    monkeypatch.setattr("sys.argv", ["run.py", job])
    with pytest.raises(SystemExit) as exit_:
        run.main()
    return exit_.value.code


def test_success_pings_and_does_not_alert(monkeypatch, calls, tmp_path):
    assert run_job(monkeypatch, fake_daily()) == 0
    assert ("ping", "https://hc.example/daily", "/start") in calls
    assert ("ping", "https://hc.example/daily", "") in calls
    assert not [c for c in calls if c[0] == "alert"]
    assert list((tmp_path / "daily").glob("*.json"))


def test_failure_alerts_with_the_errors(monkeypatch, calls):
    pull = {"ok": False, "source_dates": ["2026-11-24"],
            "datasets": {"motus_insur": {"status": "invalid", "errors": ["row count 5 does not match the API's 6"]}}}
    code = run_job(monkeypatch, fake_daily(pull=pull, load_error="connection refused", signals_ok=False))
    assert code == 1
    alert = next(c for c in calls if c[0] == "alert")[2]
    assert "daily run FAILED" in alert
    assert "motus_insur: row count 5 does not match the API's 6" in alert
    assert "connection refused" in alert
    assert "renewal: failed: boom" in alert
    assert ("ping", "https://hc.example/daily", "/fail") in calls
