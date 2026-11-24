"""Daily pull of the four FMCSA Motus daily-difference datasets."""
import argparse
import datetime as dt
import os
import pathlib
import sys

import duckdb

import common

DATASETS = {
    "motus_carrier": {"id": "nakq-58th", "dates": [], "order_by": "usdot_number, docket_number"},
    "motus_authhist": {"id": "dm5j-zc6c", "dates": ["status_change_date"],
                       "order_by": "usdot_number, docket_number, status_change_date"},
    "motus_insur": {"id": "x96h-evps", "dates": ["effective_date", "trans_date"],
                    "order_by": "usdot_number, docket_number, policy_no"},
    "motus_inshist": {"id": "xe5s-wca7", "dates": ["effective_date", "cancl_effective_date"],
                      "order_by": "usdot_number, docket_number, policy_no"},
}
MAX_AGE_HOURS = 48
RAW_DIR = pathlib.Path(os.getenv("RAW_DIR", "data/raw"))
PARQUET_DIR = pathlib.Path(os.getenv("PARQUET_DIR", "data/parquet"))
WORK_DIR = pathlib.Path(os.getenv("WORK_DIR", "data/work"))


def run_dataset(con, name, cfg, log):
    entry = {"id": cfg["id"], "status": "ok", "errors": [], "warnings": []}
    meta = common.metadata(cfg["id"])
    updated, age_h = common.freshness(meta)
    entry["source_updated_utc"] = updated.isoformat()
    if age_h > MAX_AGE_HOURS:
        entry["status"] = "stale"
        entry["errors"].append(f"source not updated for {age_h:.0f}h; nothing pulled")
        return entry

    # Folders are named by the source's publish date, not the run date, so a rerun
    # reuses pages only when FMCSA has not published a newer file since.
    src_day = updated.date().isoformat()
    entry["source_date"] = src_day
    expected = common.count_rows(cfg["id"])
    raw = RAW_DIR / "daily" / src_day / name
    log(f"{name}: pulling {expected:,} rows")
    pages = common.pull_pages(cfg["id"], raw, log=log)
    entry["raw_dir"] = str(raw)

    if not pages:
        entry["rows"] = 0
        entry["warnings"].append("no rows today")
        return entry

    rows = common.load_pages(con, name, pages, WORK_DIR / f"daily_{src_day}")
    entry["rows"] = rows
    if rows != expected:
        entry["errors"].append(f"row count {rows:,} does not match the API's {expected:,}")
    errs, warns = common.validate(con, name, meta, strict_dates=cfg["dates"])
    entry["errors"] += errs
    entry["warnings"] += warns

    if entry["errors"]:
        entry["status"] = "invalid"
        return entry

    out = PARQUET_DIR / "daily" / src_day / f"{name}.parquet"
    common.write_parquet(con, name, out, cfg["order_by"])
    entry["parquet_file"] = str(out)
    return entry


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=list(DATASETS), action="append", help="limit to these datasets")
    args = ap.parse_args()

    day = dt.date.today().isoformat()
    log = lambda msg: print(msg, file=sys.stderr, flush=True)
    con = duckdb.connect()
    common.configure(con, WORK_DIR)
    report = {"mode": "daily", "run_date": day, "datasets": {}, "ok": True}

    for name, cfg in DATASETS.items():
        if args.only and name not in args.only:
            continue
        try:
            entry = run_dataset(con, name, cfg, log)
        except Exception as e:
            entry = {"id": cfg["id"], "status": "failed", "errors": [str(e)]}
        if entry["status"] != "ok":
            report["ok"] = False
        report["datasets"][name] = entry

    common.print_report(report)
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()