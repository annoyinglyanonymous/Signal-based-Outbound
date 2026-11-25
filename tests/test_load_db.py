"""load.py against a real Postgres (needs TEST_DB_URL)."""
import datetime as dt

import load
from helpers import carrier, write

DAY = "2026-11-19"


def fill(parquet, folder="full", day=DAY, census=None, insur=None, inshist=None, authhist=None, motus=None):
    base = parquet / folder / day
    if census is not None:
        write(base / "census.parquet", "census", census)
    if motus is not None:
        write(base / "motus_carrier.parquet", "motus_carrier", motus)
    if authhist is not None:
        write(base / "motus_authhist.parquet", "motus_authhist", authhist)
    if insur is not None:
        write(base / "motus_insur.parquet", "motus_insur", insur)
    if inshist is not None:
        write(base / "motus_inshist.parquet", "motus_inshist", inshist)


def test_initial_load_is_idempotent(pg, data_dirs):
    fill(data_dirs["parquet"],
         census=[carrier(1, email_address="A@B.COM"), carrier(2), carrier(3, status_code="I")],
         motus=[{"usdot_number": "1", "docket_number": "MC1", "op_auth_type": "COMMON", "op_auth_status": "A"}],
         authhist=[{"usdot_number": "1", "docket_number": "MC1", "op_auth_status": "Active",
                    "reason": "GRANTED", "status_change_date": "20260101"}] * 2)
    report = load.run("initial", DAY)
    assert report["ok"], report
    assert report["tables"]["carriers"]["inserted"] == 2           # 3 is inactive
    assert report["tables"]["authority_history"]["inserted"] == 1  # duplicate row deduped
    assert pg.execute("SELECT email_address FROM carriers WHERE usdot_number = 1").fetchone()[0] == "a@b.com"

    again = load.run("initial", DAY)
    for table, entry in again["tables"].items():
        assert entry["status"] == "skipped" or (entry["inserted"], entry["updated"]) == (0, 0), table


def test_changed_rows_update_and_null_keys_dedupe(pg, data_dirs):
    policy = {"usdot_number": "1", "docket_number": "MC1", "policy_no": None, "ins_form_code": "91X",
              "effective_date": "20250101", "insurance_company_name": "ACME"}
    fill(data_dirs["parquet"], census=[carrier(1)], insur=[policy])
    load.run("initial", DAY)
    fill(data_dirs["parquet"], folder="daily", day="2026-11-20",
         insur=[{**policy, "insurance_company_name": "ACME MUTUAL"}])
    report = load.run("daily", "2026-11-20")
    assert report["tables"]["policies"]["updated"] == 1
    assert report["tables"]["policies"]["inserted"] == 0           # NULL policy_no still matched
    assert pg.execute("SELECT count(*), max(insurance_company_name) FROM policies").fetchone() == (1, "ACME MUTUAL")


def test_cancelled_policies_close(pg, data_dirs):
    past = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y%m%d")
    future = (dt.date.today() + dt.timedelta(days=20)).strftime("%Y%m%d")
    pol = lambda no: {"usdot_number": "1", "docket_number": "MC1", "policy_no": no,
                      "ins_form_code": "91X", "effective_date": "20250101"}
    fill(data_dirs["parquet"], census=[carrier(1)], insur=[pol("P1"), pol("P2")],
         inshist=[{**pol("P1"), "cancl_effective_date": past}, {**pol("P2"), "cancl_effective_date": future}])
    report = load.run("initial", DAY)
    assert report["policies_closed"] == 1
    rows = dict(pg.execute("SELECT policy_no, is_current FROM policies").fetchall())
    assert rows == {"P1": False, "P2": True}


def test_reconcile_fixes_drift(pg, data_dirs):
    auth = lambda dot: {"usdot_number": str(dot), "docket_number": f"MC{dot}", "op_auth_type": "COMMON",
                        "op_auth_status": "A"}
    pol = lambda no: {"usdot_number": "1", "docket_number": "MC1", "policy_no": no, "ins_form_code": "91X",
                      "effective_date": "20250101"}
    fill(data_dirs["parquet"], census=[carrier(1), carrier(2)], motus=[auth(1), auth(2)],
         insur=[pol("P1"), pol("P2")])
    load.run("initial", DAY)

    # A week later: carrier 2 is inactive, its authority is gone, P2 left the file, P3 was missed daily.
    week = "2026-11-26"
    fill(data_dirs["parquet"], day=week, census=[carrier(1), carrier(2, status_code="I")],
         motus=[auth(1)], insur=[pol("P1"), pol("P3")])
    rec = load.run("reconcile", week)
    assert rec["ok"], rec
    assert rec["tables"]["carriers"]["missing_from_source_untarget"] == 1
    assert rec["tables"]["authorities"]["missing_from_source_delete"] == 1
    assert rec["tables"]["policies"]["missing_from_source_close"] == 1
    assert rec["tables"]["policies"]["inserted"] == 1
    assert pg.execute("SELECT usdot_number FROM carriers WHERE is_target").fetchall() == [(1,)]
    assert pg.execute("SELECT count(*) FROM authorities").fetchone()[0] == 1
    assert dict(pg.execute("SELECT policy_no, is_current FROM policies").fetchall()) == \
        {"P1": True, "P2": False, "P3": True}

    # Carrier 2 back in the filter: targeted again. A clean second reconcile changes nothing else.
    fill(data_dirs["parquet"], day=week, census=[carrier(1), carrier(2)], motus=[auth(1)],
         insur=[pol("P1"), pol("P3")])
    rec = load.run("reconcile", week)
    assert rec["tables"]["carriers"]["updated"] == 1
    assert rec["tables"]["policies"]["missing_from_source_close"] == 0

    import pull_full
    mm = pull_full.mismatches(rec)
    assert "carriers" not in mm and mm["policies"]["rate_pct"] == 0
