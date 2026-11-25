-- TruckShield FMCSA pipeline schema (Postgres 15+, Supabase).
-- Safe to re-run: every object is created only if it does not exist.
--
-- Column names and conflict keys must match the TABLES specs in scripts/load.py.
-- Unique keys use NULLS NOT DISTINCT so rows with a NULL key part (for example a
-- policy with no number) are still deduplicated on every daily load.

-- Target carriers: active, interstate, for-hire, power units > 0 (from the census).
CREATE TABLE IF NOT EXISTS carriers (
    usdot_number       bigint PRIMARY KEY,
    legal_name         text NOT NULL,
    dba_name           text,
    status_code        text,
    carrier_operation  text,
    classdef           text,
    business_org_desc  text,
    phy_street         text,
    phy_city           text,
    phy_state          text,
    phy_zip            text,
    phy_county         text,
    mail_street        text,
    mail_city          text,
    mail_state         text,
    mail_zip           text,
    phone              text,
    cell_phone         text,
    fax                text,
    email_address      text,
    company_officer_1  text,
    company_officer_2  text,
    power_units        integer,
    truck_units        integer,
    total_drivers      integer,
    total_cdl          integer,
    fleetsize          text,
    hazmat             boolean,
    cargo_types        text[] NOT NULL DEFAULT '{}',
    dockets            text[] NOT NULL DEFAULT '{}',
    safety_rating      text,
    safety_rating_date date,
    mcs150_date        date,
    add_date           date,
    is_target          boolean NOT NULL DEFAULT true,   -- false once a carrier drops out of the filter
    census_date        date NOT NULL,                   -- census file the row last changed in
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS carriers_phy_state_idx ON carriers (phy_state);
CREATE INDEX IF NOT EXISTS carriers_cargo_types_idx ON carriers USING gin (cargo_types);

-- Motus Carrier: one row per authority (docket and authority type) per carrier.
CREATE TABLE IF NOT EXISTS authorities (
    usdot_number    bigint NOT NULL,
    docket_number   text NOT NULL DEFAULT '',
    op_auth_type    text NOT NULL DEFAULT '',
    op_auth_status  text,
    min_cov_amount  numeric(18, 2),
    cargo_req       text,
    bond_req        text,
    bipd_file       text,
    cargo_file      text,
    bond_file       text,
    legal_name      text,
    dba_name        text,
    bus_street      text,
    bus_city        text,
    bus_state       text,
    bus_zip         text,
    bus_telno       text,
    source_date     date NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (usdot_number, docket_number, op_auth_type)
);

-- Motus AuthHist: authority status changes (grants, revocations, reinstatements). Append-only.
CREATE TABLE IF NOT EXISTS authority_history (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usdot_number        bigint NOT NULL,
    docket_number       text NOT NULL DEFAULT '',
    op_auth_type        text,
    op_auth_status      text,
    reason              text,
    status_change_date  date NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT authority_history_key UNIQUE NULLS NOT DISTINCT
        (usdot_number, docket_number, op_auth_type, op_auth_status, status_change_date, reason)
);
CREATE INDEX IF NOT EXISTS authority_history_dot_idx ON authority_history (usdot_number);
CREATE INDEX IF NOT EXISTS authority_history_date_idx ON authority_history (status_change_date);

-- Motus Insur: insurance policies on file. is_current turns false once a cancellation takes effect.
CREATE TABLE IF NOT EXISTS policies (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usdot_number            bigint NOT NULL,
    docket_number           text NOT NULL DEFAULT '',
    policy_no               text,
    ins_form_code           text,
    ins_type_code           text,
    ins_class_code          text,
    insurance_company_name  text,
    max_cov_amount          numeric(18, 2),
    underl_lim_amount       numeric(18, 2),
    effective_date          date,
    trans_date              date,
    is_current              boolean NOT NULL DEFAULT true,
    source_date             date NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT policies_key UNIQUE NULLS NOT DISTINCT
        (docket_number, policy_no, ins_form_code, effective_date)
);
CREATE INDEX IF NOT EXISTS policies_dot_idx ON policies (usdot_number);
CREATE INDEX IF NOT EXISTS policies_effective_idx ON policies (effective_date) WHERE is_current;

-- Motus InsHist: cancelled and replaced policies. Append-only.
CREATE TABLE IF NOT EXISTS policy_history (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usdot_number            bigint NOT NULL,
    docket_number           text NOT NULL DEFAULT '',
    policy_no               text,
    ins_form_code           text,
    ins_type_code           text,
    ins_type_ind            text,
    ins_type_desc           text,
    ins_class_code          text,
    filing_status_reason    text,
    insurance_company_name  text,
    min_cov_amount          numeric(18, 2),
    max_cov_amount          numeric(18, 2),
    underl_lim_amount       numeric(18, 2),
    effective_date          date,
    cancl_effective_date    date,
    source_date             date NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT policy_history_key UNIQUE NULLS NOT DISTINCT
        (docket_number, policy_no, ins_form_code, effective_date, cancl_effective_date)
);
CREATE INDEX IF NOT EXISTS policy_history_dot_idx ON policy_history (usdot_number);
CREATE INDEX IF NOT EXISTS policy_history_cancl_idx ON policy_history (cancl_effective_date);

-- Outreach signals, one per carrier, type and trigger date. Built by scripts/signals.py (Phase 5).
CREATE TABLE IF NOT EXISTS signals (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usdot_number  bigint NOT NULL REFERENCES carriers (usdot_number),
    signal_type   text NOT NULL CHECK (signal_type IN
                    ('cancellation', 'renewal', 'fleet_growth', 'new_entrant', 'specialty')),
    signal_date   date NOT NULL,          -- the date the event happens (cancellation, renewal, ...)
    details       jsonb NOT NULL DEFAULT '{}',
    status        text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'queued', 'closed', 'invalid')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (usdot_number, signal_type, signal_date)
);
CREATE INDEX IF NOT EXISTS signals_open_idx ON signals (signal_type, signal_date) WHERE status = 'open';

-- Hand checks of signals against SAFER / L&I (Phase 5 accuracy check).
CREATE TABLE IF NOT EXISTS signal_checks (
    signal_id   bigint PRIMARY KEY REFERENCES signals (id),
    correct     boolean NOT NULL,
    note        text,
    checked_by  text,
    checked_at  timestamptz NOT NULL DEFAULT now()
);

-- Contact points: FMCSA first, vendor enrichment only for gaps (Phase 6).
CREATE TABLE IF NOT EXISTS contacts (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usdot_number     bigint NOT NULL REFERENCES carriers (usdot_number),
    kind             text NOT NULL CHECK (kind IN ('email', 'phone', 'cell')),
    value            text NOT NULL,
    contact_name     text,
    source           text NOT NULL CHECK (source IN ('fmcsa', 'vendor', 'manual')),
    vendor           text,
    verified_status  text NOT NULL DEFAULT 'unverified'
                       CHECK (verified_status IN ('unverified', 'valid', 'invalid', 'risky')),
    verified_at      timestamptz,
    detail           text,                 -- why risky/invalid, e.g. 'shared by 12 carriers'
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (usdot_number, kind, value)
);

-- Weekly outreach queue, review and outcomes (Phase 6).
CREATE TABLE IF NOT EXISTS outreach (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    signal_id      bigint NOT NULL REFERENCES signals (id),
    usdot_number   bigint NOT NULL REFERENCES carriers (usdot_number),
    contact_id     bigint REFERENCES contacts (id),   -- NULL: no usable contact yet (enrichment gap)
    queue_week     date NOT NULL,          -- Monday of the queue week
    priority       integer NOT NULL,       -- rank within the week, 1 = first
    channel        text CHECK (channel IN ('email', 'phone', 'sms')),
    contact_value  text,                   -- the email or phone used, as it was when queued
    status         text NOT NULL DEFAULT 'queued' CHECK (status IN
                     ('queued', 'approved', 'rejected', 'sent', 'replied', 'bounced', 'unsubscribed',
                      'won', 'lost')),
    shadow         boolean NOT NULL DEFAULT true,   -- shadow mode: reviewed but never sent
    reviewed_by    text,
    reviewed_at    timestamptz,
    sent_at        timestamptz,
    outcome_at     timestamptz,
    notes          text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (usdot_number, queue_week)
);
CREATE INDEX IF NOT EXISTS outreach_week_idx ON outreach (queue_week, status);

-- Never contact these: unsubscribes, hard bounces, do-not-contact requests.
CREATE TABLE IF NOT EXISTS suppressions (
    kind        text NOT NULL CHECK (kind IN ('email', 'phone', 'usdot')),
    value       text NOT NULL,
    reason      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, value)
);

-- Every paid vendor call (verification or enrichment), for the monthly credit caps.
CREATE TABLE IF NOT EXISTS vendor_calls (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    purpose       text NOT NULL CHECK (purpose IN ('verify', 'enrich')),
    provider      text NOT NULL,
    usdot_number  bigint,
    contact_id    bigint REFERENCES contacts (id),
    credits       integer NOT NULL DEFAULT 1,
    result        text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS vendor_calls_month_idx ON vendor_calls (purpose, created_at);

-- Pipeline run log: one row per daily/weekly run, with its full JSON report.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job          text NOT NULL,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now(),
    ok           boolean NOT NULL,
    report       jsonb NOT NULL
);

-- Outcomes per signal type: what the "done when" of Phase 6 looks at.
CREATE OR REPLACE VIEW outreach_outcomes WITH (security_invoker = true) AS
SELECT s.signal_type,
       count(*)                                                        AS queued,
       count(*) FILTER (WHERE o.status = 'rejected')                   AS rejected,
       count(*) FILTER (WHERE o.sent_at IS NOT NULL)                   AS sent,
       count(*) FILTER (WHERE o.status = 'replied')                    AS replied,
       count(*) FILTER (WHERE o.status = 'bounced')                    AS bounced,
       count(*) FILTER (WHERE o.status = 'won')                        AS won,
       count(*) FILTER (WHERE o.status = 'lost')                       AS lost,
       round(100.0 * count(*) FILTER (WHERE o.status IN ('replied', 'won', 'lost'))
             / NULLIF(count(*) FILTER (WHERE o.sent_at IS NOT NULL), 0), 1) AS reply_rate_pct,
       round(100.0 * count(*) FILTER (WHERE o.status = 'won')
             / NULLIF(count(*) FILTER (WHERE o.sent_at IS NOT NULL), 0), 1) AS win_rate_pct
FROM outreach o JOIN signals s ON s.id = o.signal_id
GROUP BY s.signal_type;

-- Keep updated_at honest on every table that has it.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['carriers', 'authorities', 'policies', 'signals', 'contacts', 'outreach'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = t || '_updated_at') THEN
            EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION set_updated_at()',
                           t || '_updated_at', t);
        END IF;
    END LOOP;
END $$;

-- Supabase exposes the public schema through its REST API. These tables hold
-- contact data, so turn on row level security with no policies: only the
-- service role and direct database connections (the pipeline) can read them.
ALTER TABLE carriers ENABLE ROW LEVEL SECURITY;
ALTER TABLE authorities ENABLE ROW LEVEL SECURITY;
ALTER TABLE authority_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE policy_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_checks ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE outreach ENABLE ROW LEVEL SECURITY;
ALTER TABLE suppressions ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_runs ENABLE ROW LEVEL SECURITY;
