"""Weekly outreach queue, human review, sending hand-off and outcome tracking.

  python scripts/outreach.py build                    this week's queue (best signal per carrier)
  python scripts/outreach.py export --out q.csv       queue for review (also copied to R2)
  python scripts/outreach.py import-review q.csv --by NAME   decisions back in (approve / reject)
  python scripts/outreach.py export-send --out s.csv  approved, non-shadow rows for the sending tool
  python scripts/outreach.py import-outcomes o.csv    replied / bounced / unsubscribed / won / lost
  python scripts/outreach.py report                   outcomes per signal type

Shadow mode (OUTREACH_SHADOW=true, the default) queues and reviews as normal but
export-send never releases shadow rows. Set OUTREACH_SHADOW=false to go live.
"""
import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import sys

import psycopg

import common
import contacts
import storage

QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "300"))
COOLDOWN_DAYS = int(os.getenv("OUTREACH_COOLDOWN_DAYS", "30"))
SHADOW = os.getenv("OUTREACH_SHADOW", "true").lower() != "false"
QUEUE_DIR = pathlib.Path(os.getenv("QUEUE_DIR", "data/queue"))
RANK = {"cancellation": 1, "new_entrant": 2, "renewal": 3, "fleet_growth": 4, "specialty": 5}
# Share of each week's queue per signal type (percent), so every type gets outcomes.
# A share a type cannot fill goes to the best remaining candidates of any type.
QUEUE_MIX = {k: int(v) for k, v in (x.split(":") for x in os.getenv(
    "QUEUE_MIX", "cancellation:50,renewal:20,new_entrant:15,fleet_growth:10,specialty:5").split(","))}
OUTCOMES = {"replied", "bounced", "unsubscribed", "won", "lost"}

POOL_SQL = """
CREATE TEMP TABLE pool AS
WITH best AS (
    SELECT DISTINCT ON (s.usdot_number) s.id AS signal_id, s.usdot_number, s.signal_type, s.signal_date,
           CASE s.signal_type WHEN 'cancellation' THEN 1 WHEN 'new_entrant' THEN 2 WHEN 'renewal' THEN 3
                              WHEN 'fleet_growth' THEN 4 ELSE 5 END AS rank
    FROM signals s JOIN carriers c ON c.usdot_number = s.usdot_number AND c.is_target
    WHERE s.status = 'open'
    ORDER BY s.usdot_number, rank, s.signal_date
)
SELECT b.*, row_number() OVER (ORDER BY b.rank, COALESCE(c.hazmat OR c.cargo_types && %(specialty)s::text[], false) DESC,
                                        b.signal_date, c.power_units DESC NULLS LAST, b.usdot_number) AS priority
FROM best b JOIN carriers c USING (usdot_number)
WHERE NOT EXISTS (SELECT 1 FROM suppressions x WHERE x.kind = 'usdot' AND x.value = b.usdot_number::text)
  AND NOT EXISTS (SELECT 1 FROM outreach o WHERE o.usdot_number = b.usdot_number
                  AND (o.queue_week = %(week)s OR o.created_at > %(week)s::date - %(cooldown)s))
"""


def pick(pool, size, mix=None):
    """pool: (usdot_number, signal_type) in priority order. Fill each type's share of
    `size` first, then the rest in priority order. Returns the picked USDOT numbers in order."""
    mix = mix or QUEUE_MIX
    quota = {t: size * pct // 100 for t, pct in mix.items()}
    picked, used = [], {}
    for dot, kind in pool:
        if used.get(kind, 0) < quota.get(kind, 0):
            picked.append(dot)
            used[kind] = used.get(kind, 0) + 1
    chosen = set(picked)
    picked += [dot for dot, _ in pool if dot not in chosen][:size - len(picked)]
    order = {dot: i for i, (dot, _) in enumerate(pool)}
    return sorted(picked, key=order.get)

INSERT_SQL = """
INSERT INTO outreach (signal_id, usdot_number, contact_id, queue_week, priority, channel, contact_value, shadow)
SELECT q.signal_id, q.usdot_number, k.id, %(week)s, q.queue_rank,
       CASE k.kind WHEN 'email' THEN 'email' WHEN 'phone' THEN 'phone' WHEN 'cell' THEN 'phone' END,
       k.value, %(shadow)s
FROM cand q
LEFT JOIN LATERAL (
    SELECT k.id, k.kind, k.value FROM contacts k
    WHERE k.usdot_number = q.usdot_number AND k.verified_status IN ('valid', 'unverified')
      AND NOT EXISTS (SELECT 1 FROM suppressions x WHERE x.value = k.value
                      AND x.kind = CASE k.kind WHEN 'email' THEN 'email' ELSE 'phone' END)
    ORDER BY k.kind = 'email' DESC, k.verified_status = 'valid' DESC, k.source = 'fmcsa' DESC, k.kind = 'cell', k.id
    LIMIT 1) k ON true
ON CONFLICT (usdot_number, queue_week) DO NOTHING"""


def monday(day=None):
    day = day or dt.date.today()
    return day - dt.timedelta(days=day.weekday())


def connect():
    return psycopg.connect(os.environ["SUPABASE_DB_URL"])


def why(signal_type, signal_date, d):
    """One line a reviewer (and an email template) can use."""
    d = d or {}
    if signal_type == "cancellation":
        return f"{d.get('coverage') or 'Insurance'} policy with {d.get('insurer') or 'current insurer'} ends {signal_date}"
    if signal_type == "renewal":
        return f"{d.get('coverage') or 'Policy'} with {d.get('insurer') or 'current insurer'} renews {signal_date}"
    if signal_type == "new_entrant":
        return f"New authority granted {signal_date}"
    if signal_type == "fleet_growth":
        return (f"Fleet grew from {d.get('power_units_before')} to {d.get('power_units_now')} power units "
                f"since {d.get('compared_with')}")
    if signal_type == "specialty":
        cargo = ", ".join(d.get("cargo") or []) + (" + hazmat" if d.get("hazmat") else "")
        return f"Specialty hauler ({cargo.strip(' +') or 'hazmat'}), {d.get('power_units')} power units"
    return signal_type


def build(week=None, size=QUEUE_SIZE, shadow=SHADOW, log=common.log):
    from signals import SPECIALTY_CARGO
    week = week or monday()
    with connect() as pg:
        report = {"week": week.isoformat(), "shadow": shadow, "contacts": contacts.sync(pg)}
        pg.execute("DROP TABLE IF EXISTS pool, cand")
        pg.execute(POOL_SQL, {"week": week, "cooldown": COOLDOWN_DAYS, "specialty": SPECIALTY_CARGO})
        pool = pg.execute("SELECT usdot_number, signal_type FROM pool ORDER BY priority").fetchall()
        report["candidates"] = len(pool)
        dots = pick(pool, size)
        pg.execute("CREATE TEMP TABLE cand AS SELECT p.*, x.ord AS queue_rank FROM pool p "
                   "JOIN unnest(%s::bigint[]) WITH ORDINALITY x(usdot_number, ord) USING (usdot_number)", (dots,))
        log(f"queue: {len(dots)} carriers; verifying their emails")
        report["verify"] = contacts.verify(dots, pg)
        report["queued"] = pg.execute(INSERT_SQL, {"week": week, "shadow": shadow}).rowcount
        pg.execute("UPDATE signals SET status = 'queued' WHERE id IN (SELECT signal_id FROM outreach "
                   "WHERE queue_week = %s) AND status = 'open'", (week,))
        pg.commit()
        report["by_signal"] = dict(pg.execute("""SELECT s.signal_type, count(*) FROM outreach o
            JOIN signals s ON s.id = o.signal_id WHERE o.queue_week = %s GROUP BY 1 ORDER BY 1""", (week,)).fetchall())
        report["by_channel"] = {k or "no contact": v for k, v in pg.execute(
            "SELECT channel, count(*) FROM outreach WHERE queue_week = %s GROUP BY 1 ORDER BY 1", (week,)).fetchall()}
    return report


REVIEW_COLS = ["outreach_id", "priority", "usdot_number", "legal_name", "signal_type", "signal_date", "why",
               "channel", "contact", "contact_status", "contact_name", "phone", "power_units", "state",
               "decision (approve/reject)", "notes"]


def export(week=None, out=None):
    week = week or monday()
    out = pathlib.Path(out or QUEUE_DIR / f"{week}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with connect() as pg:
        rows = pg.execute("""
            SELECT o.id, o.priority, o.usdot_number, c.legal_name, s.signal_type, s.signal_date, s.details,
                   o.channel, o.contact_value, k.verified_status, k.contact_name, c.phone, c.power_units, c.phy_state
            FROM outreach o JOIN signals s ON s.id = o.signal_id JOIN carriers c ON c.usdot_number = o.usdot_number
            LEFT JOIN contacts k ON k.id = o.contact_id
            WHERE o.queue_week = %s AND o.status = 'queued' ORDER BY o.priority""", (week,)).fetchall()
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(REVIEW_COLS)
        for r in rows:
            w.writerow([*r[:6], why(r[4], r[5], r[6]), *r[7:], "", ""])
    key = storage.upload(out, f"queue/{week}.csv")
    return {"exported": len(rows), "file": str(out), "r2": key}


def import_review(path, by):
    counts = {"approved": 0, "rejected": 0, "skipped": 0}
    with open(path, newline="", encoding="utf-8-sig") as f, connect() as pg:
        for r in csv.DictReader(f):
            d = (r.get("decision (approve/reject)") or "").strip().lower()
            status = "approved" if d.startswith("a") else "rejected" if d.startswith("r") else None
            if not status:
                counts["skipped"] += 1
                continue
            n = pg.execute("""UPDATE outreach SET status = %s, reviewed_by = %s, reviewed_at = now(),
                              notes = COALESCE(NULLIF(%s, ''), notes) WHERE id = %s AND status = 'queued'""",
                           (status, by, r.get("notes"), int(r["outreach_id"]))).rowcount
            counts[status if n else "skipped"] += 1
        pg.commit()
    return counts


def export_send(week=None, out=None):
    """Approved rows for the sending tool; marks them sent. Shadow rows are never released."""
    week = week or monday()
    out = pathlib.Path(out or QUEUE_DIR / f"{week}_send.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with connect() as pg:
        held = pg.execute("SELECT count(*) FROM outreach WHERE queue_week = %s AND status = 'approved' AND shadow",
                          (week,)).fetchone()[0]
        rows = pg.execute("""
            SELECT o.id, o.channel, o.contact_value, k.contact_name, c.legal_name, c.phy_state, s.signal_type,
                   s.signal_date, s.details
            FROM outreach o JOIN signals s ON s.id = o.signal_id JOIN carriers c ON c.usdot_number = o.usdot_number
            LEFT JOIN contacts k ON k.id = o.contact_id
            WHERE o.queue_week = %s AND o.status = 'approved' AND NOT o.shadow AND o.contact_value IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM suppressions x WHERE x.value IN (o.contact_value, o.usdot_number::text))
            ORDER BY o.priority""", (week,)).fetchall()
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["outreach_id", "channel", "email_or_phone", "contact_name", "company", "state",
                        "signal_type", "why"])
            for r in rows:
                w.writerow([*r[:7], why(r[6], r[7], r[8])])
        pg.execute("UPDATE outreach SET status = 'sent', sent_at = now() WHERE id = ANY(%s)", ([r[0] for r in rows],))
        pg.commit()
    return {"released": len(rows), "held_by_shadow_mode": held, "file": str(out)}


def import_outcomes(path):
    """CSV with outreach_id, outcome, notes. Bounces invalidate the contact; bounces and
    unsubscribes go on the suppression list."""
    counts = {}
    with open(path, newline="", encoding="utf-8-sig") as f, connect() as pg:
        for r in csv.DictReader(f):
            outcome = (r.get("outcome") or "").strip().lower()
            if outcome not in OUTCOMES:
                counts["skipped"] = counts.get("skipped", 0) + 1
                continue
            row = pg.execute("""UPDATE outreach SET status = %s, outcome_at = now(),
                                notes = COALESCE(NULLIF(%s, ''), notes)
                                WHERE id = %s RETURNING contact_id, contact_value, channel""",
                             (outcome, r.get("notes"), int(r["outreach_id"]))).fetchone()
            if row and outcome in ("bounced", "unsubscribed") and row[1]:
                kind = "email" if row[2] == "email" else "phone"
                pg.execute("INSERT INTO suppressions (kind, value, reason) VALUES (%s, %s, %s) "
                           "ON CONFLICT DO NOTHING", (kind, row[1], outcome))
                if outcome == "bounced" and row[0]:
                    pg.execute("UPDATE contacts SET verified_status = 'invalid', detail = 'bounced' WHERE id = %s",
                               (row[0],))
            counts[outcome] = counts.get(outcome, 0) + (1 if row else 0)
        pg.commit()
    return counts


def report():
    with connect() as pg:
        cur = pg.execute("SELECT * FROM outreach_outcomes ORDER BY signal_type")
        cols = [c.name for c in cur.description]
        return {"outcomes": [dict(zip(cols, r)) for r in cur.fetchall()]}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--week", type=dt.date.fromisoformat)
    b.add_argument("--size", type=int, default=QUEUE_SIZE)
    for name in ("export", "export-send"):
        x = sub.add_parser(name)
        x.add_argument("--week", type=dt.date.fromisoformat)
        x.add_argument("--out")
    r = sub.add_parser("import-review")
    r.add_argument("file")
    r.add_argument("--by", required=True, help="reviewer's name")
    o = sub.add_parser("import-outcomes")
    o.add_argument("file")
    sub.add_parser("report")
    args = ap.parse_args()

    if args.cmd == "build":
        result = build(args.week, args.size)
    elif args.cmd == "export":
        result = export(args.week, args.out)
    elif args.cmd == "export-send":
        result = export_send(args.week, args.out)
    elif args.cmd == "import-review":
        result = import_review(args.file, args.by)
    elif args.cmd == "import-outcomes":
        result = import_outcomes(args.file)
    else:
        result = report()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    sys.exit(main())
