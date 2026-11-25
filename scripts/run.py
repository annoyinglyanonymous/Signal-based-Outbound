"""Scheduled entry point: one call per systemd timer.

  python scripts/run.py daily    pull Motus daily files -> load -> daily signals
  python scripts/run.py weekly   full pull + reconcile -> all signals -> outreach queue

Every run writes a JSON report (REPORT_DIR, R2 reports/, pipeline_runs table).
On failure it posts to ALERT_WEBHOOK_URL; it pings the dead man's switch
(HEALTHCHECK_URL_DAILY / HEALTHCHECK_URL_WEEKLY) at start, on success and on failure,
so a run that never finishes or never starts also raises an alert.
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import shutil
import sys
import traceback

import requests

import common
import storage

REPORT_DIR = pathlib.Path(os.getenv("REPORT_DIR", "data/reports"))
RAW_DIR = pathlib.Path(os.getenv("RAW_DIR", "data/raw"))
PARQUET_DIR = pathlib.Path(os.getenv("PARQUET_DIR", "data/parquet"))
KEEP_DAILY_DAYS = int(os.getenv("KEEP_LOCAL_DAILY_DAYS", "14"))  # R2 keeps everything; the server keeps two weeks


def step(report, name, fn, *args, **kwargs):
    """Run one stage; a failure is recorded and later stages still run."""
    common.log(f"== {name}")
    try:
        result = fn(*args, **kwargs)
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc(limit=5)}
    report["steps"][name] = result
    if isinstance(result, dict) and result.get("ok") is False:
        report["ok"] = False
    return result


def daily(report):
    import load
    import pull_daily
    import signals
    pulled = step(report, "pull", pull_daily.run)
    for day in pulled.get("source_dates", []):
        step(report, f"load {day}", load.run, "daily", day)
    try:
        report["gap_warning"] = gap_warning(pulled.get("source_dates", []))
    except Exception as e:
        report["gap_warning"] = f"gap check failed: {e}"
    step(report, "signals", signals.run, "daily")


def weekly(report):
    import outreach
    import pull_full
    import signals
    step(report, "pull and reconcile", pull_full.run, "reconcile")
    step(report, "signals", signals.run, "weekly")
    step(report, "outreach queue", outreach.build)
    step(report, "queue export", outreach.export)
    step(report, "local cleanup", cleanup)


def gap_warning(new_dates):
    """Daily files hold one day each; a skipped day can only be recovered by the weekly reconcile."""
    if not new_dates or not os.getenv("SUPABASE_DB_URL"):
        return None
    import psycopg
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
        last = pg.execute("""SELECT max(d::date) FROM pipeline_runs r,
                             jsonb_array_elements_text(r.report->'steps'->'pull'->'source_dates') d
                             WHERE r.job = 'daily'""").fetchone()[0]
    first = min(dt.date.fromisoformat(d) for d in new_dates)
    if last and (first - last).days > 1:
        return f"no daily files loaded between {last} and {first}; Sunday's reconcile will fill the gap"
    return None


def cleanup():
    """Delete local daily files older than KEEP_DAILY_DAYS (they are in R2)."""
    cutoff = (dt.date.today() - dt.timedelta(days=KEEP_DAILY_DAYS)).isoformat()
    removed = []
    for folder in (RAW_DIR / "daily", PARQUET_DIR / "daily"):
        for p in sorted(folder.glob("????-??-??")) if folder.exists() else []:
            if p.name < cutoff:
                shutil.rmtree(p, ignore_errors=True)
                removed.append(str(p))
    return {"ok": True, "removed": len(removed)}


def summary(report):
    """Short text for the alert: which steps failed and their first errors."""
    lines = [f"TruckShield {report['job']} run FAILED ({report['started_at'][:16]})"]
    for name, r in report["steps"].items():
        if not isinstance(r, dict) or r.get("ok") is not False:
            continue
        errs = [r.get("error")] if r.get("error") else []
        for part in ("datasets", "tables"):
            for k, v in (r.get(part) or {}).items():
                errs += [f"{k}: {e}" for e in (v.get("errors") or [v.get("error")]) if e]
        errs += [f"{k}: {v}" for k, v in (r.get("created") or {}).items() if str(v).startswith("failed")]
        if r.get("r2", {}).get("error"):
            errs.append(r["r2"]["error"])
        if r.get("reconcile", {}).get("ok") is False:
            errs += [f"reconcile {k}: {v.get('error')}" for k, v in r["reconcile"]["tables"].items() if v.get("error")]
        lines.append(f"- {name}: " + ("; ".join(str(e) for e in errs[:3]) or "failed"))
    return "\n".join(lines)


def notify(url, text):
    if not url:
        return
    try:  # Slack reads "text", Discord reads "content"
        requests.post(url, json={"text": text, "content": text[:1900]}, timeout=30)
    except requests.RequestException as e:
        common.log(f"alert webhook failed: {e}")


def ping(url, suffix="", body=None):
    if not url:
        return
    try:
        requests.post(url.rstrip("/") + suffix, data=(body or "").encode()[:10000], timeout=30)
    except requests.RequestException as e:
        common.log(f"healthcheck ping failed: {e}")


def save(report):
    """Keep the report locally, in R2 and in the pipeline_runs table; never let saving hide the run's result."""
    stamp = report["started_at"].replace(":", "").replace("-", "")[:15]
    path = REPORT_DIR / report["job"] / f"{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str))
    report["report_file"] = str(path)
    try:
        storage.upload(path, f"reports/{report['job']}/{path.name}")
    except Exception as e:
        common.log(f"report upload failed: {e}")
    if os.getenv("SUPABASE_DB_URL"):
        try:
            import psycopg
            with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
                pg.execute("INSERT INTO pipeline_runs (job, started_at, ok, report) VALUES (%s, %s, %s, %s)",
                           (report["job"], report["started_at"], report["ok"], json.dumps(report, default=str)))
        except Exception as e:
            common.log(f"run log insert failed: {e}")
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("job", choices=["daily", "weekly"])
    args = ap.parse_args()

    hc = os.getenv(f"HEALTHCHECK_URL_{args.job.upper()}")
    ping(hc, "/start")
    report = {"job": args.job, "started_at": dt.datetime.now(dt.timezone.utc).isoformat(), "steps": {}, "ok": True}
    try:
        (daily if args.job == "daily" else weekly)(report)
    except Exception as e:  # a bug outside any step: still alert
        report["ok"] = False
        report["steps"]["crash"] = {"ok": False, "error": f"{type(e).__name__}: {e}",
                                    "trace": traceback.format_exc(limit=5)}
    report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    path = save(report)

    if report["ok"]:
        ping(hc, "", f"ok; report {path}")
    else:
        text = summary(report) + f"\nReport: {path}"
        notify(os.getenv("ALERT_WEBHOOK_URL"), text)
        ping(hc, "/fail", text)
    common.print_report({k: report[k] for k in ("job", "ok", "started_at", "finished_at", "report_file")})
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
