"""Build outreach signals from the loaded FMCSA data.

  python scripts/signals.py run --rules daily     cancellation, renewal, new_entrant (+ expire)
  python scripts/signals.py run --rules weekly    all of the above + fleet_growth, specialty
  python scripts/signals.py check-export --n 50   sample to hand-check against SAFER
  python scripts/signals.py check-import FILE     record hand-check results, report accuracy

Rules live in sql/signals.sql; their settings are below and can be overridden by
environment variables of the same name.
"""
import argparse
import csv
import datetime as dt
import os
import pathlib
import re
import sys

import duckdb
import psycopg

import common
import storage

RULES_SQL = pathlib.Path(__file__).resolve().parent.parent / "sql" / "signals.sql"
PARQUET_DIR = pathlib.Path(os.getenv("PARQUET_DIR", "data/parquet"))


def setting(name, default):
    return type(default)(os.getenv(name, default))


CANCEL_DAYS = setting("CANCEL_DAYS", 30)
RENEWAL_DAYS = setting("RENEWAL_DAYS", 60)
ENTRANT_DAYS = setting("ENTRANT_DAYS", 60)
COOLDOWN_DAYS = setting("COOLDOWN_DAYS", 90)
SPECIALTY_PER_WEEK = setting("SPECIALTY_PER_WEEK", 200)
FLEET_COMPARE_DAYS = setting("FLEET_COMPARE_DAYS", 28)   # compare with the snapshot about 4 weeks back
FLEET_MIN_ADD = setting("FLEET_MIN_ADD", 2)              # at least this many more power units
FLEET_MIN_PCT = setting("FLEET_MIN_PCT", 20)             # and at least this much growth
# Specialty cargo (census crgo_* names). A working list: confirm with the business.
SPECIALTY_CARGO = os.getenv("SPECIALTY_CARGO", "chem,liqgas,oilfield,livestock,coldfood,meat,intermodal,"
                            "passengers,mobilehome,drivetow,motoveh,logpole,machlrg,garbage").split(",")

DAILY_RULES = ["cancellation", "renewal", "new_entrant"]
WEEKLY_RULES = DAILY_RULES + ["fleet_growth", "specialty"]
SAFER_URL = ("https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot"
             "&query_param=USDOT&query_string={dot}")


def rules():
    """Named SQL blocks from sql/signals.sql ('-- name: x' starts block x)."""
    blocks, name = {}, None
    for line in RULES_SQL.read_text().splitlines():
        if m := re.match(r"--\s*name:\s*(\w+)", line):
            name = m.group(1)
            blocks[name] = []
        elif name:
            blocks[name].append(line)
    return {k: "\n".join(v).strip() for k, v in blocks.items()}


def params(today):
    return {"today": today, "cancel_days": CANCEL_DAYS, "renewal_days": RENEWAL_DAYS,
            "entrant_days": ENTRANT_DAYS, "cooldown_days": COOLDOWN_DAYS, "specialty": SPECIALTY_CARGO,
            "specialty_limit": SPECIALTY_PER_WEEK, "week": today - dt.timedelta(days=today.weekday())}


def fleet_snapshots():
    """Local weekly fleet snapshots, fetching any that only R2 has (e.g. on a new server)."""
    folder = PARQUET_DIR / "fleet"
    for key in storage.list_keys("parquet/fleet/"):
        local = folder / pathlib.Path(key).name
        if not local.exists():
            storage.download(key, local)
    return sorted(folder.glob("*.parquet")) if folder.exists() else []


def fleet_growth(snapshots, today):
    """Carriers whose power units grew between the latest snapshot and one about
    FLEET_COMPARE_DAYS earlier. Returns (rows, note)."""
    dated = {dt.date.fromisoformat(p.stem): p for p in snapshots if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.stem)}
    dated = {d: p for d, p in dated.items() if d <= today}
    if not dated:
        return [], "no fleet snapshots yet"
    latest = max(dated)
    older = [d for d in dated if d <= latest - dt.timedelta(days=FLEET_COMPARE_DAYS - 7)]
    if not older:
        return [], f"needs a snapshot at least {FLEET_COMPARE_DAYS - 7} days older than {latest}"
    target = latest - dt.timedelta(days=FLEET_COMPARE_DAYS)
    base = min(older, key=lambda d: abs((d - target).days))
    rows = duckdb.sql(f"""
        SELECT n.usdot_number, n.census_date, b.census_date AS baseline_date,
               b.power_units AS before_units, n.power_units AS now_units,
               b.total_drivers AS before_drivers, n.total_drivers AS now_drivers
        FROM read_parquet('{dated[latest].as_posix()}') n
        JOIN read_parquet('{dated[base].as_posix()}') b USING (usdot_number)
        WHERE b.power_units > 0
          AND n.power_units - b.power_units >= {FLEET_MIN_ADD}
          AND (n.power_units - b.power_units) * 100 >= {FLEET_MIN_PCT} * b.power_units""").fetchall()
    return rows, f"{latest} compared with {base}"


def run(rule_set="daily", today=None, log=common.log):
    today = today or dt.date.today()
    names = DAILY_RULES if rule_set == "daily" else WEEKLY_RULES
    sql, p = rules(), params(today)
    report = {"rules": rule_set, "today": today.isoformat(), "created": {}, "ok": True}
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
        for name in names:
            try:
                with pg.cursor() as cur:
                    if name == "fleet_growth":
                        rows, note = fleet_growth(fleet_snapshots(), today)
                        report["fleet_growth_note"] = note
                        cur.execute("""CREATE TEMP TABLE fleet_growth (usdot_number bigint, census_date date,
                            baseline_date date, before_units int, now_units int, before_drivers int,
                            now_drivers int) ON COMMIT DROP""")
                        cur.executemany("INSERT INTO fleet_growth VALUES (%s, %s, %s, %s, %s, %s, %s)", rows)
                    cur.execute(sql[name], p)
                    report["created"][name] = cur.rowcount
                pg.commit()
                log(f"{name}: {report['created'][name]:,} new signals")
            except Exception as e:
                pg.rollback()
                report["created"][name] = f"failed: {e}"
                report["ok"] = False
        with pg.cursor() as cur:
            cur.execute(sql["expire"], p)
            report["closed"] = cur.rowcount
            pg.commit()
            report["open"] = dict(cur.execute(
                "SELECT signal_type, count(*) FROM signals WHERE status = 'open' GROUP BY 1 ORDER BY 1").fetchall())
    return report


def check_export(n, out):
    """A random sample of open signals, spread across types, with a SAFER link to check each."""
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
        rows = pg.execute("""
            SELECT id, usdot_number, legal_name, signal_type, signal_date, details FROM (
                SELECT s.*, c.legal_name,
                       row_number() OVER (PARTITION BY s.signal_type ORDER BY random()) AS rn,
                       count(*) OVER (PARTITION BY s.signal_type) AS per_type
                FROM signals s JOIN carriers c USING (usdot_number)
                WHERE s.status = 'open'
                  AND NOT EXISTS (SELECT 1 FROM signal_checks k WHERE k.signal_id = s.id)) x
            ORDER BY rn, signal_type LIMIT %s""", (n,)).fetchall()
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["signal_id", "usdot_number", "legal_name", "signal_type", "signal_date", "details",
                    "safer_url", "correct (y/n)", "note"])
        for r in rows:
            w.writerow([*r[:5], r[5], SAFER_URL.format(dot=r[1]), "", ""])
    return {"exported": len(rows), "file": str(out)}


def check_import(path, checked_by=None):
    """Store hand-check results; incorrect signals are marked invalid. Reports accuracy per type."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        done = [r for r in csv.DictReader(f) if r.get("correct (y/n)", "").strip().lower()[:1] in ("y", "n")]
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
        for r in done:
            ok = r["correct (y/n)"].strip().lower().startswith("y")
            pg.execute("""INSERT INTO signal_checks (signal_id, correct, note, checked_by) VALUES (%s, %s, %s, %s)
                          ON CONFLICT (signal_id) DO UPDATE SET correct = EXCLUDED.correct, note = EXCLUDED.note,
                          checked_by = EXCLUDED.checked_by, checked_at = now()""",
                       (int(r["signal_id"]), ok, r.get("note") or None, checked_by))
            if not ok:
                pg.execute("UPDATE signals SET status = 'invalid' WHERE id = %s", (int(r["signal_id"]),))
        pg.commit()
        acc = pg.execute("""SELECT s.signal_type, count(*), count(*) FILTER (WHERE k.correct)
                            FROM signal_checks k JOIN signals s ON s.id = k.signal_id GROUP BY 1 ORDER BY 1""").fetchall()
    total, right = sum(a[1] for a in acc), sum(a[2] for a in acc)
    return {"recorded": len(done),
            "accuracy_by_type": {t: f"{c}/{n} ({100 * c / n:.0f}%)" for t, n, c in acc},
            "accuracy": f"{right}/{total} ({100 * right / max(total, 1):.0f}%)",
            "passes_90pct": total > 0 and right / total >= 0.9}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--rules", choices=["daily", "weekly"], default="daily")
    r.add_argument("--today", type=dt.date.fromisoformat, help="run as of this date (testing)")
    e = sub.add_parser("check-export")
    e.add_argument("--n", type=int, default=50)
    e.add_argument("--out", default="signal_check.csv")
    i = sub.add_parser("check-import")
    i.add_argument("file")
    i.add_argument("--by", help="who checked them")
    args = ap.parse_args()

    if args.cmd == "run":
        report = run(args.rules, args.today)
    elif args.cmd == "check-export":
        report = check_export(args.n, args.out)
    else:
        report = check_import(args.file, args.by)
    common.print_report(report)
    sys.exit(0 if report.get("ok", True) else 1)


if __name__ == "__main__":
    main()
