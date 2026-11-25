"""Full pull of the census and the four Motus full-history datasets.

--mode initial    pull and write Parquet (first load; then run load.py --mode initial)
--mode reconcile  the weekly job: pull, then load.run("reconcile") to fix anything the
                  daily runs missed, refresh target carriers, and report mismatch counts
"""
import argparse
import datetime as dt
import os
import pathlib
import shutil
import sys

import duckdb

import common
import storage

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
KEEP_LOCAL = int(os.getenv("KEEP_LOCAL_FULL", "2"))  # weekly folders kept on the server's own disk
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


def write_fleet_snapshot(con, day, kind):
    """Fleet size of every target carrier this week. Small (a few MB), kept forever,
    and what the fleet-growth signal compares across weeks."""
    out = (PARQUET_DIR / "sample" / "fleet" if kind == "sample" else PARQUET_DIR / "fleet") / f"{day}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT TRY_CAST(dot_number AS BIGINT) AS usdot_number,
                     TRY_CAST(power_units AS INTEGER) AS power_units,
                     TRY_CAST(truck_units AS INTEGER) AS truck_units,
                     TRY_CAST(total_drivers AS INTEGER) AS total_drivers,
                     DATE '{day}' AS census_date
              FROM census WHERE {common.TARGET_FILTER} AND TRY_CAST(dot_number AS BIGINT) IS NOT NULL
              ORDER BY usdot_number)
        TO '{out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
    return out


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
    log(f"{name}: writing Parquet")
    size = common.write_parquet(con, name, out, cfg["order_by"])
    entry.update(parquet_file=str(out), parquet_mb=round(size / 1e6, 1))
    if not cfg["keep_raw"]:
        shutil.rmtree(raw, ignore_errors=True)
    else:
        entry["raw_dir"] = str(raw)
    return entry


def prune_local(folder, keep):
    """Delete all but the newest `keep` dated subfolders (YYYY-MM-DD) of folder."""
    if not folder.exists():
        return []
    dated = sorted((p for p in folder.iterdir() if p.is_dir() and len(p.name) == 10 and p.name[4] == "-"),
                   reverse=True)
    for p in dated[keep:]:
        shutil.rmtree(p, ignore_errors=True)
    return [p.name for p in dated[keep:]]


def archive(report, day):
    """Upload this run's Parquet files to R2 and keep the last storage.KEEP_FULL weeks there."""
    r2 = {}
    try:
        for name, entry in report["datasets"].items():
            if entry.get("parquet_file"):
                storage.upload(entry["parquet_file"], f"parquet/full/{day}/{name}.parquet")
        if report.get("fleet_snapshot"):
            storage.upload(report["fleet_snapshot"], f"parquet/fleet/{day}.parquet")
        r2["pruned"] = storage.prune("parquet/full", storage.KEEP_FULL)
        r2.update(storage.status())
    except Exception as e:
        r2["error"] = f"R2 upload failed: {e}"
        report["ok"] = False
    report["r2"] = r2


def run(mode, sample=None, only=None, date=None, log=common.log):
    day = date or dt.date.today().isoformat()
    con = duckdb.connect()
    common.configure(con, WORK_DIR)
    report = {"mode": mode, "run_date": day, "sample": sample, "datasets": {}, "ok": True}
    kind = "sample" if sample else "full"

    for name, cfg in DATASETS.items():
        if only and name not in only:
            continue
        try:
            entry = run_dataset(con, name, cfg, day, sample, log)
            if name == "census" and entry["status"] == "ok":
                entry["profile"] = profile_census(con)
                report["fleet_snapshot"] = str(write_fleet_snapshot(con, day, kind))
        except Exception as e:
            entry = {"id": cfg["id"], "status": "failed", "errors": [str(e)]}
        if entry["status"] != "ok":
            report["ok"] = False
        report["datasets"][name] = entry
        con.execute(f"DROP VIEW IF EXISTS {name}")

    con.close()
    shutil.rmtree(WORK_DIR / day, ignore_errors=True)

    if not sample:
        archive(report, day)
        report["local_pruned"] = prune_local(PARQUET_DIR / "full", KEEP_LOCAL) + prune_local(RAW_DIR / "full", KEEP_LOCAL)

    if mode == "reconcile":
        import load  # needs SUPABASE_DB_URL; initial mode does not
        good = {n for n, e in report["datasets"].items() if e.get("parquet_file")}
        tables = [t for t, spec in load.TABLES.items() if spec["source"] in good]
        if tables:
            rec = load.run("reconcile", day, sample=bool(sample), only=tables, log=log)
            report["reconcile"] = rec
            report["mismatches"] = mismatches(rec)
            report["ok"] = report["ok"] and rec["ok"]
    return report


def mismatches(rec):
    """Rows the weekly reconcile had to fix in the Motus tables, i.e. what the daily runs missed.
    Carriers are left out: the census only arrives weekly, so its changes are expected."""
    out = {}
    for table, e in rec["tables"].items():
        if table == "carriers" or e.get("status") != "ok":
            continue
        missing = next((v for k, v in e.items() if k.startswith("missing_from_source")), 0) or 0
        out[table] = {"inserted": e["inserted"], "updated": e["updated"], "missing_from_source": missing,
                      "rate_pct": round(100 * (e["inserted"] + e["updated"] + missing) / max(e["rows_in"], 1), 3)}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["initial", "reconcile"], required=True)
    ap.add_argument("--sample", type=int, help="pull only the first N rows per dataset (testing)")
    ap.add_argument("--only", choices=list(DATASETS), action="append", help="limit to these datasets")
    ap.add_argument("--date", help="run folder to use (default today); reuse a failed run's date to resume its pages")
    args = ap.parse_args()
    report = run(args.mode, args.sample, args.only, args.date)
    common.print_report(report)
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
