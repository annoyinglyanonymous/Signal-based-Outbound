-- Signal rules. scripts/signals.py runs each named block with these parameters:
--   %(today)s          the run date
--   %(cancel_days)s    cancellations this many days ahead
--   %(renewal_days)s   renewals (policy anniversaries) this many days ahead
--   %(entrant_days)s   first authority granted within this many days
--   %(cooldown_days)s  don't repeat fleet_growth / specialty for a carrier within this many days
--   %(specialty)s      cargo types that count as specialty (text[])
--   %(specialty_limit)s specialty carriers to add per weekly run
--   %(week)s           Monday of the run week
-- Every rule only targets carriers with is_target, and inserts are idempotent
-- (unique on carrier, type and date), so re-running a day adds nothing.

-- name: cancellation
-- A policy ending soon with no newer policy of the same type on file. Covers InsHist
-- CANCEL rows, BMC-35/36 cancellation notices, and TERM/REPL (policy terminated for a
-- replacement): most TERM/REPL rows already have their replacement on file and drop out
-- here; the rest are carriers about to lapse. Name-change filings are not cancellations.
INSERT INTO signals (usdot_number, signal_type, signal_date, details)
SELECT DISTINCT ON (h.usdot_number, h.cancl_effective_date)
       h.usdot_number, 'cancellation', h.cancl_effective_date,
       jsonb_build_object('policy_no', h.policy_no, 'insurer', h.insurance_company_name,
                          'coverage', COALESCE(h.ins_type_desc, h.ins_form_code), 'form', h.ins_form_code,
                          'docket', h.docket_number, 'reason', h.filing_status_reason)
FROM policy_history h
JOIN carriers c ON c.usdot_number = h.usdot_number AND c.is_target
WHERE h.cancl_effective_date BETWEEN %(today)s AND %(today)s::date + %(cancel_days)s
  AND (h.filing_status_reason IN ('CANCEL', 'TERM/CANCL', 'TERM/REPL')
       OR (h.ins_form_code IN ('BMC-35', 'BMC-36') AND h.filing_status_reason IS DISTINCT FROM 'NAMECHG'))
  AND NOT EXISTS (
      SELECT 1 FROM policies p
      WHERE p.usdot_number = h.usdot_number AND p.is_current
        -- a newer filing replaces it, even under the same policy number (a renewal)
        AND p.ins_type_code IS NOT DISTINCT FROM COALESCE(h.ins_type_code,
              CASE h.ins_form_code WHEN 'BMC-35' THEN '1' WHEN 'BMC-36' THEN '3' END)
        AND p.effective_date > COALESCE(h.effective_date, '-infinity'::date))
-- liability (type 1) first when a carrier has several cancellations on one date
ORDER BY h.usdot_number, h.cancl_effective_date, h.ins_type_code NULLS LAST
ON CONFLICT (usdot_number, signal_type, signal_date) DO NOTHING;

-- name: renewal
-- The next anniversary of a current liability or cargo policy falls soon, and no
-- cancellation is pending for that policy. One signal per carrier: the earliest.
INSERT INTO signals (usdot_number, signal_type, signal_date, details)
SELECT DISTINCT ON (r.usdot_number)
       r.usdot_number, 'renewal', r.anniversary,
       jsonb_build_object('policy_no', r.policy_no, 'insurer', r.insurance_company_name,
                          'coverage', CASE r.ins_type_code WHEN '1' THEN 'BIPD' WHEN '2' THEN 'CARGO' END,
                          'form', r.ins_form_code, 'effective_date', r.effective_date,
                          'max_cov_amount', r.max_cov_amount, 'docket', r.docket_number)
FROM (
    SELECT p.*, (p.effective_date + (date_part('year', age(%(today)s::date, p.effective_date))::int + 1)
                 * interval '1 year')::date AS anniversary
    FROM policies p
    JOIN carriers c ON c.usdot_number = p.usdot_number AND c.is_target
    WHERE p.is_current AND p.ins_type_code IN ('1', '2') AND p.effective_date < %(today)s
) r
WHERE r.anniversary <= %(today)s::date + %(renewal_days)s
  AND NOT EXISTS (
      SELECT 1 FROM policy_history h
      WHERE h.docket_number = r.docket_number AND h.policy_no IS NOT DISTINCT FROM r.policy_no
        AND h.cancl_effective_date >= %(today)s)
ORDER BY r.usdot_number, r.anniversary, r.ins_type_code
ON CONFLICT (usdot_number, signal_type, signal_date) DO NOTHING;

-- name: new_entrant
-- A carrier's first-ever authority grant, recently. Needs the carrier in the target
-- list (census, weekly), which is why the window is wider than a week.
INSERT INTO signals (usdot_number, signal_type, signal_date, details)
SELECT DISTINCT ON (a.usdot_number)
       a.usdot_number, 'new_entrant', a.status_change_date,
       jsonb_build_object('docket', a.docket_number, 'authority', a.op_auth_type)
FROM authority_history a
JOIN carriers c ON c.usdot_number = a.usdot_number AND c.is_target
WHERE a.reason ILIKE 'granted'
  AND a.status_change_date BETWEEN %(today)s::date - %(entrant_days)s AND %(today)s
  AND NOT EXISTS (
      SELECT 1 FROM authority_history e
      WHERE e.usdot_number = a.usdot_number AND e.reason ILIKE 'granted'
        AND e.status_change_date < a.status_change_date)
ORDER BY a.usdot_number, a.status_change_date
ON CONFLICT (usdot_number, signal_type, signal_date) DO NOTHING;

-- name: fleet_growth
-- Rows come from signals.py (which compares weekly fleet snapshots) via temp table fleet_growth.
INSERT INTO signals (usdot_number, signal_type, signal_date, details)
SELECT g.usdot_number, 'fleet_growth', g.census_date,
       jsonb_build_object('power_units_before', g.before_units, 'power_units_now', g.now_units,
                          'drivers_before', g.before_drivers, 'drivers_now', g.now_drivers,
                          'compared_with', g.baseline_date)
FROM fleet_growth g
JOIN carriers c ON c.usdot_number = g.usdot_number AND c.is_target
WHERE NOT EXISTS (
    SELECT 1 FROM signals s
    WHERE s.usdot_number = g.usdot_number AND s.signal_type = 'fleet_growth'
      AND s.signal_date > g.census_date - %(cooldown_days)s)
ON CONFLICT (usdot_number, signal_type, signal_date) DO NOTHING;

-- name: specialty
-- Specialty is a segment, not an event: each week add a batch of specialty carriers
-- (largest fleets first) that have a contact and no signal or outreach in the cooldown.
INSERT INTO signals (usdot_number, signal_type, signal_date, details)
SELECT c.usdot_number, 'specialty', %(week)s,
       jsonb_build_object('cargo', ARRAY(SELECT unnest(c.cargo_types) INTERSECT SELECT unnest(%(specialty)s::text[])),
                          'hazmat', c.hazmat, 'power_units', c.power_units)
FROM carriers c
WHERE c.is_target
  AND (c.hazmat OR c.cargo_types && %(specialty)s::text[])
  AND COALESCE(c.email_address, c.phone, c.cell_phone) IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM signals s WHERE s.usdot_number = c.usdot_number
                  AND s.created_at > %(today)s::date - %(cooldown_days)s)
  AND NOT EXISTS (SELECT 1 FROM outreach o WHERE o.usdot_number = c.usdot_number
                  AND o.created_at > %(today)s::date - %(cooldown_days)s)
ORDER BY c.power_units DESC NULLS LAST, c.usdot_number
LIMIT %(specialty_limit)s
ON CONFLICT (usdot_number, signal_type, signal_date) DO NOTHING;

-- name: expire
-- Close open signals that are past their date (cancellations and renewals) or older
-- than a month (the others), and cancellations a replacement policy has since covered.
UPDATE signals s SET status = 'closed'
WHERE s.status = 'open'
  AND ((s.signal_type IN ('cancellation', 'renewal') AND s.signal_date < %(today)s)
       OR (s.signal_type IN ('new_entrant', 'fleet_growth', 'specialty')
           AND s.created_at < %(today)s::date - 30)
       OR (s.signal_type = 'cancellation' AND EXISTS (
              SELECT 1 FROM policies p
              WHERE p.usdot_number = s.usdot_number AND p.is_current
                AND p.created_at > s.created_at)));
