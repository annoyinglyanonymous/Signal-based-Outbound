"""Contact points for outreach: FMCSA first, verified before use, vendor enrichment only for gaps.

  python scripts/contacts.py sync                      census emails/phones -> contacts
  python scripts/contacts.py verify --dots 123,456     verify those carriers' emails (outreach.py
                                                       build does this for each week's queue)
  python scripts/contacts.py enrich-export --out f.csv carriers in this week's queue with no usable
                                                       email, up to the monthly credit cap
  python scripts/contacts.py enrich-import f.csv       vendor results back in (as unverified)

Verification providers (VERIFY_PROVIDER):
  mx          free: marks emails whose domain cannot receive mail as invalid. It cannot
              check the mailbox itself, so other emails stay 'unverified'.
  zerobounce  paid mailbox check (ZEROBOUNCE_API_KEY), capped by VERIFY_MONTHLY_CAP credits.
Enrichment goes through a CSV round trip so any vendor (Apollo, Clay, ...) works;
exports are capped by ENRICH_MONTHLY_CAP and logged in vendor_calls.
"""
import argparse
import csv
import datetime as dt
import os
import re
import sys

import psycopg
import requests

import common

VERIFY_PROVIDER = os.getenv("VERIFY_PROVIDER", "mx")
VERIFY_MONTHLY_CAP = int(os.getenv("VERIFY_MONTHLY_CAP", "1000"))
VERIFY_MAX_AGE_DAYS = int(os.getenv("VERIFY_MAX_AGE_DAYS", "90"))
ENRICH_MONTHLY_CAP = int(os.getenv("ENRICH_MONTHLY_CAP", "500"))
SHARED_EMAIL_MIN = int(os.getenv("SHARED_EMAIL_MIN", "3"))  # an email on this many carriers is an agent's, not the carrier's
EMAIL_RE = r"^[A-Za-z0-9._%+'-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$"

SYNC_SQL = """
WITH src AS (
    SELECT usdot_number, company_officer_1 AS contact_name, 'email' AS kind, lower(trim(email_address)) AS value
    FROM carriers WHERE is_target AND email_address ~ %(email_re)s
    UNION ALL
    SELECT usdot_number, company_officer_1, k, CASE WHEN length(d) = 11 AND d LIKE '1%%' THEN substr(d, 2) ELSE d END
    FROM (SELECT usdot_number, company_officer_1, 'phone' AS k, regexp_replace(phone, '\\D', '', 'g') AS d
          FROM carriers WHERE is_target AND phone IS NOT NULL
          UNION ALL
          SELECT usdot_number, company_officer_1, 'cell', regexp_replace(cell_phone, '\\D', '', 'g')
          FROM carriers WHERE is_target AND cell_phone IS NOT NULL) p
)
INSERT INTO contacts (usdot_number, kind, value, contact_name, source)
SELECT usdot_number, kind, value, contact_name, 'fmcsa' FROM src
WHERE kind = 'email' OR length(value) = 10
ON CONFLICT (usdot_number, kind, value) DO NOTHING"""

SHARED_SQL = """
UPDATE contacts c SET verified_status = 'risky', detail = 'shared by ' || s.n || ' carriers'
FROM (SELECT value, count(DISTINCT usdot_number) AS n FROM contacts WHERE kind = 'email'
      GROUP BY value HAVING count(DISTINCT usdot_number) >= %(min)s) s
WHERE c.kind = 'email' AND c.value = s.value AND c.verified_status = 'unverified'"""


def connect():
    return psycopg.connect(os.environ["SUPABASE_DB_URL"])


def sync(pg=None):
    """Copy target carriers' census emails and phones into contacts; flag shared emails."""
    own = pg is None
    pg = pg or connect()
    try:
        added = pg.execute(SYNC_SQL, {"email_re": EMAIL_RE}).rowcount
        shared = pg.execute(SHARED_SQL, {"min": SHARED_EMAIL_MIN}).rowcount
        pg.commit()
        return {"added": added, "flagged_shared": shared}
    finally:
        if own:
            pg.close()


def credits_used(pg, purpose):
    month = dt.date.today().replace(day=1)
    return pg.execute("SELECT COALESCE(sum(credits), 0) FROM vendor_calls WHERE purpose = %s AND created_at >= %s",
                      (purpose, month)).fetchone()[0]


_mx_cache = {}


def check_mx(email):
    """'invalid' if the domain has no mail server, else None (mailbox unknown)."""
    import dns.exception
    import dns.resolver
    domain = email.rsplit("@", 1)[1]
    if domain in _mx_cache:
        return _mx_cache[domain]
    for attempt in range(3):
        try:
            dns.resolver.resolve(domain, "MX", lifetime=10)
            _mx_cache[domain] = None
            return None
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            _mx_cache[domain] = "invalid"
            return "invalid"
        except dns.exception.DNSException:
            continue   # timeout or no reachable nameserver: retry, and never cache it
    return "error"


ZB_STATUS = {"valid": "valid", "invalid": "invalid", "spamtrap": "invalid", "abuse": "invalid",
             "do_not_mail": "invalid", "catch-all": "risky", "unknown": "risky"}


def check_zerobounce(email):
    r = requests.get("https://api.zerobounce.net/v2/validate", timeout=60,
                     params={"api_key": os.environ["ZEROBOUNCE_API_KEY"], "email": email, "ip_address": ""})
    r.raise_for_status()
    body = r.json()
    if "error" in body:
        raise RuntimeError(body["error"])
    return ZB_STATUS.get(body.get("status"), "risky"), body.get("sub_status") or body.get("status")


def verify(dots, pg=None, log=common.log):
    """Verify the unverified (or stale) emails of these carriers. Returns counts by result."""
    own = pg is None
    pg = pg or connect()
    out = {"provider": VERIFY_PROVIDER, "checked": 0}
    try:
        rows = pg.execute("""
            SELECT id, usdot_number, value FROM contacts
            WHERE kind = 'email' AND usdot_number = ANY(%s) AND verified_status IN ('unverified', 'valid')
              AND (verified_at IS NULL OR verified_at < now() - make_interval(days => %s))
            ORDER BY id""", (list(dots), VERIFY_MAX_AGE_DAYS)).fetchall()
        if VERIFY_PROVIDER == "zerobounce":
            left = VERIFY_MONTHLY_CAP - credits_used(pg, "verify")
            if len(rows) > left:
                out["capped"] = f"{len(rows) - max(left, 0)} not checked: monthly cap of {VERIFY_MONTHLY_CAP} reached"
                rows = rows[:max(left, 0)]
        for cid, dot, email in rows:
            if VERIFY_PROVIDER == "zerobounce":
                status, detail = check_zerobounce(email)
                pg.execute("INSERT INTO vendor_calls (purpose, provider, usdot_number, contact_id, result) "
                           "VALUES ('verify', 'zerobounce', %s, %s, %s)", (dot, cid, detail))
            else:
                status = check_mx(email)
                if status == "error":
                    out["dns_error"] = out.get("dns_error", 0) + 1
                    continue
                detail = "domain has no mail server" if status == "invalid" else "domain ok; mailbox not checked"
            # status None (mx: domain fine, mailbox unknown) keeps the current status
            pg.execute("""UPDATE contacts SET verified_status = COALESCE(%s, verified_status), detail = %s,
                          verified_at = now() WHERE id = %s""", (status, detail, cid))
            out["checked"] += 1
            key = status or "domain_ok"
            out[key] = out.get(key, 0) + 1
        pg.commit()
    finally:
        if own:
            pg.close()
    return out


GAPS_SQL = """
SELECT c.usdot_number, c.legal_name, c.dba_name, c.phy_street, c.phy_city, c.phy_state, c.phy_zip,
       c.phone, c.company_officer_1
FROM outreach o JOIN carriers c USING (usdot_number)
WHERE o.queue_week = %(week)s AND o.status IN ('queued', 'approved')
  AND NOT EXISTS (SELECT 1 FROM contacts k WHERE k.usdot_number = c.usdot_number AND k.kind = 'email'
                  AND k.verified_status IN ('valid', 'unverified'))
  AND NOT EXISTS (SELECT 1 FROM vendor_calls v WHERE v.usdot_number = c.usdot_number AND v.purpose = 'enrich'
                  AND v.created_at > now() - interval '180 days')
ORDER BY o.priority
LIMIT %(limit)s"""


def enrich_export(week, out, provider="csv"):
    """Carriers in the week's queue without a usable email, for a vendor to fill in."""
    with connect() as pg:
        left = max(ENRICH_MONTHLY_CAP - credits_used(pg, "enrich"), 0)
        rows = pg.execute(GAPS_SQL, {"week": week, "limit": left}).fetchall()
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["usdot_number", "legal_name", "dba_name", "street", "city", "state", "zip", "phone",
                        "officer", "email", "contact_name", "contact_phone"])
            for r in rows:
                w.writerow([*r, "", "", ""])
        for r in rows:   # count the credit at export so the cap holds even if results never come back
            pg.execute("INSERT INTO vendor_calls (purpose, provider, usdot_number, result) "
                       "VALUES ('enrich', %s, %s, 'exported')", (provider, r[0]))
        pg.commit()
    return {"exported": len(rows), "credits_left_this_month": left - len(rows), "file": str(out)}


def enrich_import(path, vendor):
    """Vendor results: rows with an email/contact_phone become unverified vendor contacts."""
    added = 0
    with open(path, newline="", encoding="utf-8-sig") as f, connect() as pg:
        for r in csv.DictReader(f):
            dot = int(r["usdot_number"])
            email = (r.get("email") or "").strip().lower()
            phone = re.sub(r"\D", "", r.get("contact_phone") or "")
            phone = phone[1:] if len(phone) == 11 and phone.startswith("1") else phone
            for kind, value in (("email", email if re.match(EMAIL_RE, email) else ""),
                                ("phone", phone if len(phone) == 10 else "")):
                if value:
                    added += pg.execute("""INSERT INTO contacts (usdot_number, kind, value, contact_name, source, vendor)
                        VALUES (%s, %s, %s, %s, 'vendor', %s) ON CONFLICT (usdot_number, kind, value) DO NOTHING""",
                                        (dot, kind, value, r.get("contact_name") or None, vendor)).rowcount
        pg.commit()
    return {"added": added}


def monday(day=None):
    day = day or dt.date.today()
    return day - dt.timedelta(days=day.weekday())


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync")
    v = sub.add_parser("verify")
    v.add_argument("--dots", required=True, help="comma-separated USDOT numbers")
    e = sub.add_parser("enrich-export")
    e.add_argument("--week", type=dt.date.fromisoformat, default=monday())
    e.add_argument("--out", default="enrich_gaps.csv")
    e.add_argument("--provider", default="csv", help="vendor name, for the credit log")
    i = sub.add_parser("enrich-import")
    i.add_argument("file")
    i.add_argument("--vendor", required=True)
    args = ap.parse_args()

    if args.cmd == "sync":
        report = sync()
    elif args.cmd == "verify":
        report = verify([int(x) for x in args.dots.split(",")])
    elif args.cmd == "enrich-export":
        report = enrich_export(args.week, args.out, args.provider)
    else:
        report = enrich_import(args.file, args.vendor)
    common.print_report(report)


if __name__ == "__main__":
    sys.exit(main())
