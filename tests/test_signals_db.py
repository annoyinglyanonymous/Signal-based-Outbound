"""Signal rules against a real Postgres (needs TEST_DB_URL)."""
import datetime as dt

import duckdb
import pytest

import signals

TODAY = dt.date(2026, 11, 25)
D = lambda days: TODAY + dt.timedelta(days=days)


def add_carrier(pg, dot, target=True, cargo=(), hazmat=False, units=5, email="x@y.com"):
    pg.execute("""INSERT INTO carriers (usdot_number, legal_name, is_target, census_date, cargo_types, hazmat,
                  power_units, email_address) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
               (dot, f"C{dot}", target, TODAY, list(cargo), hazmat, units, email))


def add_policy(pg, dot, no, eff, type_code="1", current=True):
    pg.execute("""INSERT INTO policies (usdot_number, docket_number, policy_no, ins_form_code, ins_type_code,
                  effective_date, is_current, source_date) VALUES (%s, %s, %s, 'BMC-91X', %s, %s, %s, %s)""",
               (dot, f"MC{dot}", no, type_code, eff, current, TODAY))


def add_history(pg, dot, no, cancel, reason="CANCEL", form="BMC-91X", type_code="1", eff=dt.date(2025, 1, 1)):
    pg.execute("""INSERT INTO policy_history (usdot_number, docket_number, policy_no, ins_form_code, ins_type_code,
                  filing_status_reason, effective_date, cancl_effective_date, source_date)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
               (dot, f"MC{dot}", no, form, type_code, reason, eff, cancel, TODAY))


def created(pg, kind):
    return sorted(pg.execute("SELECT usdot_number, signal_date FROM signals WHERE signal_type = %s",
                             (kind,)).fetchall())


def test_cancellation_rule(pg):
    for dot in range(1, 11):
        add_carrier(pg, dot, target=dot != 6)
    add_history(pg, 1, "P1", D(10))                                   # yes
    add_history(pg, 2, "P2", D(45))                                   # too far ahead
    add_history(pg, 3, "P3", D(10), reason="TERM/REPL")
    add_policy(pg, 3, "P3-NEW", dt.date(2026, 11, 1))                 # replacement on file: no
    add_history(pg, 7, "P7", D(12), reason="TERM/REPL")               # no replacement on file: lapse risk, yes
    add_history(pg, 8, "P8", D(12), reason="NAMECHG", form="BMC-35")  # name change: no
    add_history(pg, 9, "P9", D(12), reason="CANCEL", type_code="2")
    add_policy(pg, 9, "P9-NEW", dt.date(2026, 11, 1), type_code="1")  # newer policy is another type: still yes
    add_history(pg, 10, "P10", D(8), reason="TERM/REPL")
    add_policy(pg, 10, "P10", dt.date(2026, 11, 8))                   # same number refiled (renewal): no
    add_history(pg, 4, "P4", D(10))
    add_policy(pg, 4, "P4-NEW", dt.date(2026, 6, 1))                  # already replaced
    add_history(pg, 5, None, D(5), reason=None, form="BMC-35", type_code=None)  # BMC-35 notice: yes
    add_history(pg, 6, "P6", D(10))                                   # not a target carrier
    signals.run("daily", TODAY)
    assert created(pg, "cancellation") == [(1, D(10)), (5, D(5)), (7, D(12)), (9, D(12))]
    signals.run("daily", TODAY)                                       # idempotent
    assert len(created(pg, "cancellation")) == 4


def test_renewal_rule(pg):
    for dot in (1, 2, 3, 4):
        add_carrier(pg, dot)
    add_policy(pg, 1, "P1", dt.date(2024, 12, 10))                    # anniversary 2026-12-10: yes
    add_policy(pg, 2, "P2", dt.date(2025, 3, 1))                      # anniversary in 96 days: no
    add_policy(pg, 3, "P3", dt.date(2025, 12, 1))
    add_history(pg, 3, "P3", D(20), eff=dt.date(2025, 12, 1))         # being cancelled: no renewal
    add_policy(pg, 4, "P4", dt.date(2023, 1, 5))                      # anniversary 2027-01-05 (41 days): yes
    signals.run("daily", TODAY)
    assert created(pg, "renewal") == [(1, dt.date(2026, 12, 10)), (4, dt.date(2027, 1, 5))]


def test_new_entrant_rule(pg):
    for dot in (1, 2):
        add_carrier(pg, dot)
    q = """INSERT INTO authority_history (usdot_number, docket_number, op_auth_type, op_auth_status, reason,
           status_change_date) VALUES (%s, 'MC', 'COMMON', 'Active', %s, %s)"""
    pg.execute(q, (1, "GRANTED", D(-10)))                             # first grant: yes
    pg.execute(q, (2, "GRANTED", D(-5)))
    pg.execute(q, (2, "GRANTED", dt.date(2019, 1, 1)))                # granted before: not new
    signals.run("daily", TODAY)
    assert created(pg, "new_entrant") == [(1, D(-10))]


def test_fleet_growth_and_specialty(pg, tmp_path, monkeypatch):
    monkeypatch.setattr(signals, "PARQUET_DIR", tmp_path)
    (tmp_path / "fleet").mkdir()
    snap = lambda day, rows: duckdb.sql(
        "COPY (SELECT * FROM (VALUES " + ", ".join(f"({d}, {u}, {u}, {u}, DATE '{day}')" for d, u in rows)
        + ") t(usdot_number, power_units, truck_units, total_drivers, census_date)) "
        f"TO '{(tmp_path / 'fleet' / f'{day}.parquet').as_posix()}' (FORMAT parquet)")
    snap(D(-28), [(1, 5), (2, 5), (3, 10)])
    snap(D(-7), [(1, 6), (2, 5), (3, 10)])
    snap(TODAY, [(1, 8), (2, 6), (3, 11)])   # 1: +3 (+60%) yes; 2: +1 no; 3: +1 (+10%) no
    add_carrier(pg, 1)
    add_carrier(pg, 2)
    add_carrier(pg, 3, cargo=["chem"], units=10)
    add_carrier(pg, 4, cargo=["genfreight"], hazmat=True, units=3)
    add_carrier(pg, 5, cargo=["genfreight"])                          # not specialty
    add_carrier(pg, 6, cargo=["livestock"], email=None)               # no contact
    report = signals.run("weekly", TODAY)
    assert report["ok"], report
    assert created(pg, "fleet_growth") == [(1, TODAY)]
    monday = TODAY - dt.timedelta(days=TODAY.weekday())
    # carrier 1 already has a signal (cooldown); 3 and 4 qualify
    assert created(pg, "specialty") == [(3, monday), (4, monday)]
    detail = pg.execute("SELECT details FROM signals WHERE signal_type = 'specialty' AND usdot_number = 3").fetchone()[0]
    assert detail["cargo"] == ["chem"]


def test_fleet_growth_needs_an_old_enough_snapshot(tmp_path):
    (tmp_path / f"{TODAY}.parquet").touch()
    (tmp_path / f"{D(-7)}.parquet").touch()
    rows, note = signals.fleet_growth(sorted(tmp_path.glob("*.parquet")), TODAY)
    assert rows == [] and "needs a snapshot" in note


def test_expire_closes_past_and_replaced(pg):
    add_carrier(pg, 1)
    add_history(pg, 1, "P1", D(3))
    signals.run("daily", TODAY)
    report = signals.run("daily", D(5))                               # cancellation date has passed
    assert report["closed"] == 1
    assert pg.execute("SELECT status FROM signals").fetchone()[0] == "closed"


def test_check_round_trip(pg, tmp_path):
    add_carrier(pg, 1)
    add_carrier(pg, 2)
    add_history(pg, 1, "P1", D(3))
    add_history(pg, 2, "P2", D(4))
    signals.run("daily", TODAY)
    out = tmp_path / "check.csv"
    assert signals.check_export(50, out)["exported"] == 2
    text = out.read_text(encoding="utf-8").splitlines()
    text[1] = text[1].rstrip(",") + ",y,"
    text[2] = text[2].rstrip(",") + ",n,wrong date"
    out.write_text("\n".join(text) + "\n", encoding="utf-8")
    result = signals.check_import(out, "tester")
    assert result["recorded"] == 2 and result["accuracy"] == "1/2 (50%)" and not result["passes_90pct"]
    assert pg.execute("SELECT count(*) FROM signals WHERE status = 'invalid'").fetchone()[0] == 1
