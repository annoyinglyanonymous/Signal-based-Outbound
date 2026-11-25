"""Contacts and the outreach flow against a real Postgres (needs TEST_DB_URL)."""
import csv
import datetime as dt

import pytest

import contacts
import outreach

WEEK = dt.date(2026, 11, 23)  # a Monday


@pytest.fixture(autouse=True)
def no_dns(monkeypatch):
    monkeypatch.setattr(contacts, "check_mx", lambda email: "invalid" if email.endswith("@dead.test") else None)
    monkeypatch.setattr(contacts, "VERIFY_PROVIDER", "mx")


def carrier(pg, dot, email=None, phone=None, cell=None, cargo=(), units=5):
    pg.execute("""INSERT INTO carriers (usdot_number, legal_name, is_target, census_date, email_address, phone,
                  cell_phone, cargo_types, power_units, company_officer_1)
                  VALUES (%s, %s, true, %s, %s, %s, %s, %s, %s, 'PAT LEE')""",
               (dot, f"C{dot}", WEEK, email, phone, cell, list(cargo), units))


def signal(pg, dot, kind, day, details="{}"):
    return pg.execute("INSERT INTO signals (usdot_number, signal_type, signal_date, details) "
                      "VALUES (%s, %s, %s, %s) RETURNING id", (dot, kind, day, details)).fetchone()[0]


def read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def test_sync_normalizes_and_flags_shared(pg):
    carrier(pg, 1, email="Ops@Acme.com", phone="(555) 123-4567", cell="1-555-987-6543")
    carrier(pg, 2, email="not an email", phone="12345")
    for dot in (3, 4, 5):
        carrier(pg, dot, email="filings@agent.com")
    report = contacts.sync()
    assert report == {"added": 6, "flagged_shared": 3}
    rows = set(pg.execute("SELECT usdot_number, kind, value, verified_status FROM contacts").fetchall())
    assert (1, "email", "ops@acme.com", "unverified") in rows
    assert (1, "phone", "5551234567", "unverified") in rows
    assert (1, "cell", "5559876543", "unverified") in rows
    assert (3, "email", "filings@agent.com", "risky") in rows
    assert not any(r[0] == 2 for r in rows)                      # bad email and short phone skipped
    assert contacts.sync()["added"] == 0                          # idempotent


def test_queue_review_send_outcomes(pg, tmp_path, monkeypatch):
    carrier(pg, 1, email="a@one.com", phone="5550000001")         # cancellation + renewal: cancellation wins
    carrier(pg, 2, email="b@dead.test", phone="5550000002")       # dead email domain: falls back to phone
    carrier(pg, 3)                                                # no contact at all: queued as a gap
    carrier(pg, 4, email="d@four.com")                            # suppressed carrier: never queued
    carrier(pg, 5, email="e@five.com")                            # contacted last week: cooling down
    s1 = signal(pg, 1, "cancellation", WEEK + dt.timedelta(days=9),
                '{"coverage": "BIPD", "insurer": "ACME MUTUAL"}')
    signal(pg, 1, "renewal", WEEK + dt.timedelta(days=30))
    signal(pg, 2, "renewal", WEEK + dt.timedelta(days=20))
    signal(pg, 3, "new_entrant", WEEK - dt.timedelta(days=3))
    signal(pg, 4, "cancellation", WEEK + dt.timedelta(days=5))
    s5 = signal(pg, 5, "renewal", WEEK + dt.timedelta(days=10))
    pg.execute("INSERT INTO suppressions (kind, value, reason) VALUES ('usdot', '4', 'do not contact')")
    pg.execute("INSERT INTO outreach (signal_id, usdot_number, queue_week, priority, created_at) "
               "VALUES (%s, 5, %s, 1, %s)", (s5, WEEK - dt.timedelta(days=7), WEEK - dt.timedelta(days=7)))

    built = outreach.build(WEEK, size=10, shadow=True)
    assert built["queued"] == 3
    q = {r[0]: r[1:] for r in pg.execute(
        "SELECT usdot_number, priority, channel, contact_value, signal_id FROM outreach WHERE queue_week = %s",
        (WEEK,)).fetchall()}
    assert q[1] == (1, "email", "a@one.com", s1)                  # cancellation ranks first
    assert q[2][1:3] == ("phone", "5550000002")
    assert q[3][1] is None                                        # enrichment gap
    assert pg.execute("SELECT verified_status FROM contacts WHERE value = 'b@dead.test'").fetchone()[0] == "invalid"
    assert outreach.build(WEEK, size=10)["queued"] == 0           # rebuilding the same week adds nothing

    # Review: approve 1 and 2, reject 3.
    monkeypatch.setattr(outreach, "QUEUE_DIR", tmp_path)
    exported = outreach.export(WEEK)
    rows = read(exported["file"])
    assert [r["usdot_number"] for r in rows] == ["1", "3", "2"]   # cancellation, new entrant, renewal
    assert rows[0]["why"] == "BIPD policy with ACME MUTUAL ends 2026-12-02"
    decisions = {"1": "approve", "2": "approve", "3": "reject"}
    for r in rows:
        r["decision (approve/reject)"] = decisions[r["usdot_number"]]
    write(tmp_path / "reviewed.csv", rows)
    assert outreach.import_review(tmp_path / "reviewed.csv", "sam") == {"approved": 2, "rejected": 1, "skipped": 0}

    # Shadow mode holds everything back.
    sent = outreach.export_send(WEEK)
    assert sent["released"] == 0 and sent["held_by_shadow_mode"] == 2

    # Live: release, then record outcomes.
    pg.execute("UPDATE outreach SET shadow = false")
    sent = outreach.export_send(WEEK)
    assert sent["released"] == 2
    ids = {r["email_or_phone"]: r["outreach_id"] for r in read(sent["file"])}
    write(tmp_path / "outcomes.csv", [{"outreach_id": ids["a@one.com"], "outcome": "bounced", "notes": ""},
                                      {"outreach_id": ids["5550000002"], "outcome": "won", "notes": "bound BIPD"}])
    assert outreach.import_outcomes(tmp_path / "outcomes.csv") == {"bounced": 1, "won": 1}
    assert pg.execute("SELECT verified_status FROM contacts WHERE value = 'a@one.com'").fetchone()[0] == "invalid"
    assert pg.execute("SELECT count(*) FROM suppressions WHERE value = 'a@one.com'").fetchone()[0] == 1

    outcomes = {o["signal_type"]: o for o in outreach.report()["outcomes"]}
    assert outcomes["renewal"]["won"] == 1 and outcomes["cancellation"]["bounced"] == 1
    assert outcomes["new_entrant"]["rejected"] == 1


def test_enrichment_round_trip_respects_cap(pg, tmp_path, monkeypatch):
    monkeypatch.setattr(contacts, "ENRICH_MONTHLY_CAP", 1)
    for dot in (1, 2):
        carrier(pg, dot)
        signal(pg, dot, "new_entrant", WEEK)
    outreach.build(WEEK, size=10)
    out = tmp_path / "gaps.csv"
    first = contacts.enrich_export(WEEK, out, "apollo")
    assert first["exported"] == 1 and first["credits_left_this_month"] == 0
    assert contacts.enrich_export(WEEK, tmp_path / "again.csv")["exported"] == 0    # cap reached

    rows = read(out)
    rows[0].update(email="Owner@Carrier.com", contact_name="Jo", contact_phone="+1 (555) 222-3333")
    write(out, rows)
    assert contacts.enrich_import(out, "apollo") == {"added": 2}
    got = pg.execute("SELECT kind, value, source, vendor FROM contacts ORDER BY kind").fetchall()
    assert got == [("email", "owner@carrier.com", "vendor", "apollo"), ("phone", "5552223333", "vendor", "apollo")]
