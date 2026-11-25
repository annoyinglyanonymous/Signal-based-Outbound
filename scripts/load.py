import argparse
import os
import pathlib
import sys

import duckdb
import psycopg

import common

PARQUET_DIR = pathlib.Path(os.getenv("PARQUET_DIR", "data/parquet"))
SCHEMA = pathlib.Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
BATCH = 10_000

CARGO = ["genfreight", "household", "metalsheet", "motoveh", "drivetow", "logpole", "bldgmat",
         "mobilehome", "machlrg", "produce", "liqgas", "intermodal", "passengers", "oilfield",
         "livestock", "grainfeed", "coalcoke", "meat", "garbage", "usmail", "chem", "drybulk",
         "coldfood", "beverages", "paperprod", "utility", "farmsupp", "construct", "waterwell",
         "cargoothr"]


def t(c):  # trimmed text, empty -> NULL
    return f"NULLIF(TRIM({c}), '')"


def d(c):  # YYYYMMDD text -> DATE
    return f"TRY_STRPTIME(NULLIF(TRIM({c}), ''), '%Y%m%d')::DATE"


def i(c):
    return f"TRY_CAST({c} AS INTEGER)"


def n(c):
    return f"TRY_CAST({c} AS DECIMAL(18, 2))"


def big(c):
    return f"TRY_CAST({c} AS BIGINT)"


CARGO_LIST = "[x FOR x IN [" + ", ".join(
    f"CASE WHEN crgo_{c} = 'X' THEN '{c}' END" for c in CARGO) + "] IF x IS NOT NULL]"
DOCKET_LIST = "[x FOR x IN [" + ", ".join(
    f"CASE WHEN {t(f'docket{k}')} IS NOT NULL THEN COALESCE({t(f'docket{k}prefix')}, '') || TRIM(docket{k}) END"
    for k in (1, 2, 3)) + "] IF x IS NOT NULL]"

# Each spec: source Parquet, DuckDB select (column aliases = Postgres columns),
# conflict key, whether existing rows update, and which duplicate wins.
TABLES = {
    "carriers": {
        "source": "census",
        "modes": ["initial", "reconcile"],
        "select": f"""
            {big('dot_number')} AS usdot_number, {t('legal_name')} AS legal_name, {t('dba_name')} AS dba_name,
            {t('status_code')} AS status_code, {t('carrier_operation')} AS carrier_operation,
            {t('classdef')} AS classdef, {t('business_org_desc')} AS business_org_desc,
            {t('phy_street')} AS phy_street, {t('phy_city')} AS phy_city, {t('phy_state')} AS phy_state,
            {t('phy_zip')} AS phy_zip, {t('phy_cnty')} AS phy_county,
            {t('carrier_mailing_street')} AS mail_street, {t('carrier_mailing_city')} AS mail_city,
            {t('carrier_mailing_state')} AS mail_state, {t('carrier_mailing_zip')} AS mail_zip,
            {t('phone')} AS phone, {t('cell_phone')} AS cell_phone, {t('fax')} AS fax,
            lower({t('email_address')}) AS email_address,
            {t('company_officer_1')} AS company_officer_1, {t('company_officer_2')} AS company_officer_2,
            {i('power_units')} AS power_units, {i('truck_units')} AS truck_units,
            {i('total_drivers')} AS total_drivers, {i('total_cdl')} AS total_cdl, {t('fleetsize')} AS fleetsize,
            CASE hm_ind WHEN 'Y' THEN true WHEN 'N' THEN false END AS hazmat,
            {CARGO_LIST} AS cargo_types, {DOCKET_LIST} AS dockets,
            {t('safety_rating')} AS safety_rating, {d('safety_rating_date')} AS safety_rating_date,
            {d('mcs150_date')} AS mcs150_date, {d('add_date')} AS add_date,
            true AS is_target, $source_date AS census_date""",
        "where": f"{common.TARGET_FILTER} AND {big('dot_number')} IS NOT NULL AND {t('legal_name')} IS NOT NULL",
        "key": ["usdot_number"],
        "update": True,
        "winner": "mcs150_date DESC NULLS LAST",
        "missing": "untarget",   # left the filter: keep the row, stop targeting it
    },
    "authorities": {
        "source": "motus_carrier",
        "modes": ["initial", "daily", "reconcile"],
        "select": f"""
            {big('usdot_number')} AS usdot_number, COALESCE({t('docket_number')}, '') AS docket_number,
            COALESCE({t('op_auth_type')}, '') AS op_auth_type, {t('op_auth_status')} AS op_auth_status,
            {n('min_cov_amount')} AS min_cov_amount, {t('cargo_req')} AS cargo_req, {t('bond_req')} AS bond_req,
            {t('bipd_file')} AS bipd_file, {t('cargo_file')} AS cargo_file, {t('bond_file')} AS bond_file,
            {t('legal_name')} AS legal_name, {t('dba_name')} AS dba_name,
            {t('bus_street_po')} AS bus_street, {t('bus_city')} AS bus_city,
            {t('bus_state_code')} AS bus_state, {t('bus_zip_code')} AS bus_zip, {t('bus_telno')} AS bus_telno,
            $source_date AS source_date""",
        "where": f"{big('usdot_number')} IS NOT NULL",
        "key": ["usdot_number", "docket_number", "op_auth_type"],
        "update": True,
        "winner": "op_auth_status",
        "missing": "delete",     # authority no longer listed
    },
    "authority_history": {
        "source": "motus_authhist",
        "modes": ["initial", "daily", "reconcile"],
        "select": f"""
            {big('usdot_number')} AS usdot_number, COALESCE({t('docket_number')}, '') AS docket_number,
            {t('op_auth_type')} AS op_auth_type, {t('op_auth_status')} AS op_auth_status,
            {t('reason')} AS reason, {d('status_change_date')} AS status_change_date""",
        "where": f"{big('usdot_number')} IS NOT NULL AND {d('status_change_date')} IS NOT NULL",
        "key": ["usdot_number", "docket_number", "op_auth_type", "op_auth_status", "status_change_date", "reason"],
        "update": False,
        "winner": "reason",
        "missing": "count",      # history is append-only; just report
    },
    "policies": {
        "source": "motus_insur",
        "modes": ["initial", "daily", "reconcile"],
        "select": f"""
            {big('usdot_number')} AS usdot_number, COALESCE({t('docket_number')}, '') AS docket_number,
            {t('policy_no')} AS policy_no, {t('ins_form_code')} AS ins_form_code,
            {t('ins_type_code')} AS ins_type_code, {t('ins_class_code')} AS ins_class_code,
            {t('insurance_company_name')} AS insurance_company_name,
            {n('max_cov_amount')} AS max_cov_amount, {n('underl_lim_amount')} AS underl_lim_amount,
            {d('effective_date')} AS effective_date, {d('trans_date')} AS trans_date,
            $source_date AS source_date""",   # is_current is set by the database, not the load
        "where": f"{big('usdot_number')} IS NOT NULL",
        "key": ["docket_number", "policy_no", "ins_form_code", "effective_date"],
        "update": True,
        "winner": "trans_date DESC NULLS LAST",   # refiled policies: keep the latest filing
        "missing": "close",      # policy no longer on file
    },
    "policy_history": {
        "source": "motus_inshist",
        "modes": ["initial", "daily", "reconcile"],
        "select": f"""
            {big('usdot_number')} AS usdot_number, COALESCE({t('docket_number')}, '') AS docket_number,
            {t('policy_no')} AS policy_no, {t('ins_form_code')} AS ins_form_code,
            {t('ins_type_code')} AS ins_type_code, {t('ins_type_ind')} AS ins_type_ind,
            {t('ins_type_desc')} AS ins_type_desc, {t('ins_class_code')} AS ins_class_code,
            {t('filing_status_reason')} AS filing_status_reason,
            {t('insurance_company_name')} AS insurance_company_name,
            {n('min_cov_amount')} AS min_cov_amount, {n('max_cov_amount')} AS max_cov_amount,
            {n('underl_lim_amount')} AS underl_lim_amount,
            {d('effective_date')} AS effective_date, {d('cancl_effective_date')} AS cancl_effective_date,
            $source_date AS source_date""",
        "where": f"{big('usdot_number')} IS NOT NULL",
        "key": ["docket_number", "policy_no", "ins_form_code", "effective_date", "cancl_effective_date"],
        "update": False,
        "winner": "filing_status_reason",
        "missing": "count",
    },
}

# Policies stop being current once a cancellation for them has taken effect.
CLOSE_CANCELLED = """
    UPDATE policies p SET is_current = false
    FROM policy_history h
    WHERE p.is_current
      AND h.docket_number = p.docket_number
      AND h.policy_no IS NOT DISTINCT FROM p.policy_no
      AND h.ins_form_code IS NOT DISTINCT FROM p.ins_form_code
      AND h.effective_date IS NOT DISTINCT FROM p.effective_date
      AND h.cancl_effective_date <= current_date"""


def build(duck, spec, parquet, source_date):
    """Transform and dedupe into the DuckDB table `staged`; returns its columns."""
    select = spec["select"].replace("$source_date", f"DATE '{source_date}'")
    keys = ", ".join(spec["key"])
    duck.execute(f"""
        CREATE OR REPLACE TABLE staged AS
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY {spec['winner']}) AS _rn
            FROM (SELECT {select} FROM read_parquet('{parquet.as_posix()}') WHERE {spec['where']})
        ) WHERE _rn = 1""")
    cols = [r[0] for r in duck.execute("DESCRIBE staged").fetchall()]
    return cols


def key_text(alias, keys):
    """One hashable text value per key, so anti-joins on keys with NULLs stay fast."""
    return "concat_ws('|', " + ", ".join(f"COALESCE({alias}.{k}::text, '<null>')" for k in keys) + ")"


MISSING_ACTIONS = {
    "untarget": "UPDATE {table} t SET is_target = false WHERE t.is_target AND {absent}",
    "delete": "DELETE FROM {table} t WHERE {absent}",
    "close": "UPDATE {table} t SET is_current = false WHERE t.is_current AND {absent}",
    "count": "SELECT count(*) FROM {table} t WHERE {absent}",
}


def upsert(pg, duck, table, spec, cols, reconcile=False):
    """Copy `staged` into Postgres and upsert it. Returns (inserted, updated, missing):
    missing is the number of database rows absent from the source (reconcile only)."""
    keys = spec["key"]
    col_list = ", ".join(cols)
    with pg.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE stage ON COMMIT DROP AS SELECT {col_list} FROM {table} WITH NO DATA")
        with cur.copy(f"COPY stage ({col_list}) FROM STDIN") as copy:
            res = duck.execute(f"SELECT {col_list} FROM staged")
            while rows := res.fetchmany(BATCH):
                for row in rows:
                    copy.write_row(row)

        if spec["update"]:
            others = [c for c in cols if c not in keys]
            sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in others)
            changed = " OR ".join(f"{table}.{c} IS DISTINCT FROM EXCLUDED.{c}"
                                  for c in others if c not in ("source_date", "census_date"))
            action = f"DO UPDATE SET {sets} WHERE {changed}"
        else:
            action = "DO NOTHING"
        cur.execute(f"""
            INSERT INTO {table} ({col_list}) SELECT {col_list} FROM stage
            ON CONFLICT ({', '.join(keys)}) {action}
            RETURNING (xmax = 0) AS inserted""")
        flags = [r[0] for r in cur.fetchall()]

        missing = None
        if reconcile:
            absent = (f"NOT EXISTS (SELECT 1 FROM (SELECT {key_text('s', keys)} AS k FROM stage s) s "
                      f"WHERE s.k = {key_text('t', keys)})")
            cur.execute(MISSING_ACTIONS[spec["missing"]].format(table=table, absent=absent))
            missing = cur.fetchone()[0] if spec["missing"] == "count" else cur.rowcount
    return sum(flags), len(flags) - sum(flags), missing


def run(mode, date, sample=False, only=None, log=common.log):
    kind = "sample" if sample else ("daily" if mode == "daily" else "full")
    folder = PARQUET_DIR / kind / date
    report = {"mode": mode, "date": date, "folder": str(folder), "tables": {}, "ok": True}

    duck = duckdb.connect()
    common.configure(duck, os.getenv("WORK_DIR", "data/work"))
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as pg:
        pg.execute(SCHEMA.read_text())  # idempotent: creates only what is missing
        pg.commit()
        for table, spec in TABLES.items():
            if mode not in spec["modes"] or (only and table not in only):
                continue
            parquet = folder / f"{spec['source']}.parquet"
            entry = {"source": str(parquet)}
            try:
                if not parquet.exists():
                    entry.update(status="skipped", note="no Parquet file for this date")
                else:
                    log(f"{table}: preparing")
                    cols = build(duck, spec, parquet, date)
                    entry["rows_in"] = duck.execute("SELECT count(*) FROM staged").fetchone()[0]
                    log(f"{table}: loading {entry['rows_in']:,} rows")
                    inserted, updated, missing = upsert(pg, duck, table, spec, cols, mode == "reconcile")
                    pg.commit()
                    entry.update(status="ok", inserted=inserted, updated=updated,
                                 unchanged=entry["rows_in"] - inserted - updated)
                    if mode == "reconcile":
                        entry[f"missing_from_source_{spec['missing']}"] = missing
            except Exception as e:
                pg.rollback()
                entry.update(status="failed", error=str(e))
                report["ok"] = False
            report["tables"][table] = entry

        try:
            with pg.cursor() as cur:
                cur.execute(CLOSE_CANCELLED)
                report["policies_closed"] = cur.rowcount
            pg.commit()
        except Exception as e:
            pg.rollback()
            report["policies_closed_error"] = str(e)
            report["ok"] = False
    duck.close()
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["initial", "daily", "reconcile"], required=True)
    ap.add_argument("--date", required=True, help="the Parquet folder date to load")
    ap.add_argument("--sample", action="store_true", help="load from data/parquet/sample/ (testing)")
    ap.add_argument("--only", choices=list(TABLES), action="append", help="limit to these tables")
    args = ap.parse_args()
    report = run(args.mode, args.date, args.sample, args.only)
    common.print_report(report)
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
