"""Profiling report on the loaded database: contact coverage, duplicate keys,
carriers per ICP and database size (settles Supabase Free vs Pro)."""
import os
import sys

import psycopg

import common
from signals import SPECIALTY_CARGO

FREE_TIER_MB = 500

QUERIES = {
    "carriers": "SELECT count(*) FROM carriers WHERE is_target",
    "with_email": "SELECT count(*) FROM carriers WHERE is_target AND email_address IS NOT NULL",
    "with_phone": "SELECT count(*) FROM carriers WHERE is_target AND phone IS NOT NULL",
    "with_cell": "SELECT count(*) FROM carriers WHERE is_target AND cell_phone IS NOT NULL",
    "with_any_contact": """SELECT count(*) FROM carriers WHERE is_target
                           AND COALESCE(email_address, phone, cell_phone) IS NOT NULL""",
    "shared_emails": """SELECT count(*) FROM (SELECT email_address FROM carriers WHERE is_target
                        AND email_address IS NOT NULL GROUP BY 1 HAVING count(*) > 1) s""",
    "carriers_on_shared_emails": """SELECT COALESCE(sum(n), 0)::bigint FROM (SELECT count(*) AS n FROM carriers
                        WHERE is_target AND email_address IS NOT NULL GROUP BY email_address HAVING count(*) > 1) s""",
}
DUPLICATES = {
    "carriers": "usdot_number",
    "authorities": "usdot_number, docket_number, op_auth_type",
    "authority_history": "usdot_number, docket_number, op_auth_type, op_auth_status, status_change_date, reason",
    "policies": "docket_number, policy_no, ins_form_code, effective_date",
    "policy_history": "docket_number, policy_no, ins_form_code, effective_date, cancl_effective_date",
}
ICP = {
    "specialty": """SELECT count(*) FROM carriers WHERE is_target AND (hazmat OR cargo_types && %(cargo)s)""",
    # A current policy whose next anniversary of its effective date falls in the next 60 days.
    "renewals_next_60d": """SELECT count(DISTINCT c.usdot_number) FROM carriers c JOIN policies p USING (usdot_number)
        WHERE c.is_target AND p.is_current AND p.effective_date < current_date
          AND (p.effective_date + (date_part('year', age(current_date, p.effective_date))::int + 1)
               * interval '1 year')::date <= current_date + 60""",
    "cancellations_next_30d": """SELECT count(DISTINCT c.usdot_number) FROM carriers c
        JOIN policy_history h USING (usdot_number)
        WHERE c.is_target AND h.cancl_effective_date BETWEEN current_date AND current_date + 30""",
    "new_authority_last_90d": """SELECT count(DISTINCT c.usdot_number) FROM carriers c
        JOIN authority_history a USING (usdot_number)
        WHERE c.is_target AND a.reason ILIKE 'granted'
          AND a.status_change_date >= current_date - 90""",
}


def main():
    report = {"contacts": {}, "duplicate_keys": {}, "icp": {}, "size": {}}
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
        one = lambda sql, params=None: pg.execute(sql, params).fetchone()[0]
        for name, sql in QUERIES.items():
            report["contacts"][name] = one(sql)
        total = report["contacts"]["carriers"] or 1
        for name in ("with_email", "with_phone", "with_cell", "with_any_contact"):
            report["contacts"][name + "_pct"] = round(100 * report["contacts"][name] / total, 1)
        for table, key in DUPLICATES.items():
            report["duplicate_keys"][table] = one(
                f"SELECT count(*) FROM (SELECT 1 FROM {table} GROUP BY {key} HAVING count(*) > 1) d")
        for name, sql in ICP.items():
            report["icp"][name] = one(sql, {"cargo": SPECIALTY_CARGO})
        report["icp"]["fleet_growth"] = "needs two census snapshots (Phase 3)"
        tables = pg.execute("""SELECT relname, pg_total_relation_size(relid) FROM pg_stat_user_tables
                               ORDER BY 2 DESC""").fetchall()
        report["size"]["tables_mb"] = {t: round(b / 1e6, 1) for t, b in tables}
        db_mb = round(one("SELECT pg_database_size(current_database())") / 1e6, 1)
        report["size"]["database_mb"] = db_mb
        report["size"]["free_tier_mb"] = FREE_TIER_MB
        report["size"]["fits_free_tier"] = db_mb < FREE_TIER_MB * 0.7   # leave room for signals and growth
    common.print_report(report)


if __name__ == "__main__":
    sys.exit(main())
