""" Pulls data once only"""
import argparse
import datetime as dt
import os
import pathlib
import shutil
import sys

import duckdb

import common

DATASETS = {
    "census": {
        "id": "az4n-8mr2",
        "strict_dates": ["add_date"],
        "soft_dates": ["mcs150_date", "mcsipdate", "review_date", "safety_rating_date"],
        "order_by": "status_code, phy_state, carrier_operation, dot_number",
        "keep_raw": False,
    },
    "motus_carrier": {
        "id": "inys-ebih",
        "strict_dates": [],
        "soft_dates": [],
        "order_by": "usdot_number, docket_number",
        "keep_raw": True,
    },
    "motus_authhist": {
        "id": "yu5v-wbh6",
        "strict_dates": ["status_change_date"],
        "soft_dates": [],
        "order_by": "usdot_number, docket_number, status_change_date",
        "keep_raw": True,
    },
    "motus_insur": {
        "id": "c5y8-a4uz",
        "strict_dates": ["effective_date", "trans_date"],
        "soft_dates": [],
        "order_by": "usdot_number, docket_number, policy_no",
        "keep_raw": True,
    },
    "motus_inshist": {
        "id": "3uet-3z4i",
        "strict_dates": ["effective_date", "cancl_effective_date"],
        "soft_dates": [],
        "order_by": "usdot_number, docket_number, policy_no",
        "keep_raw": True,
    },
}

MAX_AGE_HOURS = 72
RAW_DIR = pathlib.Path(os.getenv("RAW_DIR", "data/raw"))
WORK_DIR = pathlib.Path(os.getenv("WORK_DIR", "data/work"))
PARQUET_DIR = pathlib.Path(os.getenv("PARQUET_DIR", "data/parquet"))


def profile_census(con):
    """Target-carrier counts and contact coverage."""
    row = con.execute(f"""
        SELECT count(*) AS targets,
               count(NULLIF(email_address, '')) AS with_email,
               count(NULLIF(phone, '')) AS with_phone,
               count(NULLIF(cell_phone, '')) AS with_cell,
               count(*) - count(DISTINCT dot_number) AS duplicate_dot_numbers
        FROM census WHERE {common.TARGET_FILTER}""").fetchone()
    keys = ["targets", "with_email", "with_phone", "with_cell", "duplicate_dot_numbers"]
    return dict(zip(keys, row))


def run_dataset(con, name, cfg, day, sample, log):
    entry = {"id": cfg["id"], "status": "ok", "errors": [], "warnings": []}
    meta = common.metadata(cfg["id"])
    updated, age_h = common.freshness(meta)
    entry["source_updated_utc"] = updated.isoformat()
    if age_h > MAX_AGE_HOURS:
        entry["warnings"].append(f"source not updated for {age_h:.0f}h")

    expected = common.count_rows(cfg["id"])
    entry["expected_rows"] = expected if sample is None else min(sample, expected)

    kind = "sample" if sample else "full"  # sample runs never share pages with full runs
    raw = RAW_DIR / kind / day / name
    log(f"{name}: pulling {entry['expected_rows']:,} rows")
    pages = common.pull_pages(cfg["id"], raw, max_rows=sample, log=log)
    rows = common.load_pages(con, name, pages, WORK_DIR / day) if pages else 0
    entry["rows"] = rows

    if rows != entry["expected_rows"]:
        entry["errors"].append(f"row count {rows:,} does not match the API's {entry['expected_rows']:,}")
    errs, warns = common.validate(con, name, meta, cfg["strict_dates"], cfg["soft_dates"])
    entry["errors"] += errs
    entry["warnings"] += warns

    if entry["errors"]:
        entry["status"] = "invalid"
        entry["raw_dir"] = str(raw)
        return entry

    out = PARQUET_DIR / kind / day / f"{name}.parquet"
    size = common.write_parquet(con, name, out, cfg["order_by"])
    entry.update(parquet_file=str(out), parquet_mb=round(size / 1e6, 1))
    if not cfg["keep_raw"]:
        shutil.rmtree(raw, ignore_errors=True)
    else:
        entry["raw_dir"] = str(raw)
    return entry


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["initial", "reconcile"], required=True)
    ap.add_argument("--sample", type=int, help="pull only the first N rows per dataset (testing)")
    ap.add_argument("--only", choices=list(DATASETS), action="append", help="limit to these datasets")
    ap.add_argument("--date", help="run folder to use (default today); reuse a failed run's date to resume its pages")
    args = ap.parse_args()

    if args.mode == "reconcile":
        sys.exit("reconcile mode is built in Phase 3; use --mode initial for now")

    day = args.date or dt.date.today().isoformat()
    log = lambda msg: print(msg, file=sys.stderr, flush=True)
    con = duckdb.connect()
    common.configure(con, WORK_DIR)
    report = {"mode": args.mode, "run_date": day, "sample": args.sample, "datasets": {}, "ok": True}

    for name, cfg in DATASETS.items():
        if args.only and name not in args.only:
            continue
        try:
            entry = run_dataset(con, name, cfg, day, args.sample, log)
            if name == "census" and entry["status"] == "ok":
                entry["profile"] = profile_census(con)
        except Exception as e:
            entry = {"id": cfg["id"], "status": "failed", "errors": [str(e)]}
        if entry["status"] != "ok":
            report["ok"] = False
        report["datasets"][name] = entry
        con.execute(f"DROP VIEW IF EXISTS {name}")

    con.close()
    shutil.rmtree(WORK_DIR / day, ignore_errors=True)
    common.print_report(report)
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()