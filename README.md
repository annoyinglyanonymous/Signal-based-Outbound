# truckshield-fmcsa

Keeps FMCSA carrier data current in Supabase, turns changes into outreach signals for
TruckShield (cancellations, renewals, new entrants, fleet growth, specialty cargo), and
builds a reviewed weekly outreach queue.

## How it fits together

| When | Job | What it does |
|---|---|---|
| Daily 06:00 Nepal time | `run.py daily` | Pull the four Motus daily files, load them, create daily signals |
| Sunday 00:30 Nepal time | `run.py weekly` | Pull the census and Motus full history, reconcile Supabase, refresh target carriers, create all signals, build and export the outreach queue |
| Weekly, by people | review | Approve or reject the queue, send, record outcomes (see "Weekly outreach routine") |

Sources (data.transportation.gov):

| Dataset | Full | Daily difference | Used for |
|---|---|---|---|
| Company Census File | `az4n-8mr2` | none (weekly full pull) | Target carriers, contacts, fleet size, cargo |
| Motus Carrier | `inys-ebih` | `nakq-58th` | Authorities |
| Motus AuthHist | `yu5v-wbh6` | `dm5j-zc6c` | New entrants |
| Motus Insur | `c5y8-a4uz` | `x96h-evps` | Renewals |
| Motus InsHist | `3uet-3z4i` | `xe5s-wca7` | Cancellations |

Target carriers: active, interstate, for-hire, at least one power unit (`common.TARGET_FILTER`).

## Layout

```
scripts/  run.py         scheduled entry point (daily / weekly), reports, alerts, dead man's switch
          common.py      API calls with retries, resumable page pulls, DuckDB limits, validation
          pull_full.py   census + Motus full history -> Parquet; --mode reconcile also fixes Supabase
          pull_daily.py  Motus daily files -> Parquet -> R2
          load.py        Parquet -> Supabase: dedupe, upsert, reconcile, close cancelled policies
          storage.py     Cloudflare R2 uploads, downloads, keep-last-N
          signals.py     signal rules (sql/signals.sql), SAFER hand-check export/import
          contacts.py    FMCSA contacts, verification, enrichment CSV round trip with credit caps
          outreach.py    weekly queue, review, send export, outcomes
          profile_db.py  coverage, duplicate keys, carriers per ICP, database size
sql/      schema.sql     all tables and views; applied automatically, safe to re-run
          signals.sql    the signal rules
deploy/   setup.sh       one-time server setup;  update.sh  deploy / roll back
          systemd/       daily and weekly services and timers
tests/    unit tests on fixture files, database tests (need TEST_DB_URL)
```

R2 bucket layout: `raw/daily/` (daily CSVs), `parquet/daily/`, `parquet/full/<date>/` (last 4
weeks), `parquet/fleet/` (weekly fleet snapshots, kept), `reports/`, `queue/`.

## Local setup

```
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt      # Linux: .venv/bin/pip
copy .env.example .env                             # fill in SOCRATA_TOKEN and SUPABASE_DB_URL at least
```

## Running by hand

Every script prints a JSON report to stdout, logs progress to stderr, and exits 1 on failure.

```
python scripts/run.py daily                         # the whole daily job
python scripts/run.py weekly                        # the whole weekly job (2 to 3 hours)

python scripts/pull_full.py --mode initial          # first full pull (--sample 1000 to test)
python scripts/load.py --mode initial --date 2026-11-23
python scripts/pull_full.py --mode reconcile        # weekly pull + reconcile on its own
python scripts/pull_daily.py
python scripts/load.py --mode daily --date <source date from the daily report>
python scripts/signals.py run --rules weekly
python scripts/profile_db.py
```

A first load can also be done by running the weekly job once: on an empty database the
reconcile loads everything.

## Tests

```
python -m pytest -q                                              # unit tests only
docker run -d --name ts-pg -e POSTGRES_PASSWORD=dev -p 55432:5432 postgres:17
docker exec ts-pg psql -U postgres -c "create database test"
set TEST_DB_URL=postgresql://postgres:dev@localhost:55432/test   # Linux: export ...
python -m pytest -q                                              # unit + database tests
```

GitHub Actions runs both on every pull request.

## Server (Ubuntu 24.04 VPS, 4 GB RAM, 50 GB disk)

1. Copy `deploy/setup.sh` to the server and run `sudo bash setup.sh`.
2. The first run prints a deploy key. Add it to the GitHub repo (Settings > Deploy keys,
   read-only) and run `sudo bash setup.sh` again.
3. Fill in `/etc/truckshield.env` (same variables as `.env.example`), then run
   `sudo bash setup.sh` a third time. It enables the timers.
4. First load: `sudo systemctl start truckshield-weekly.service`, and follow it with
   `journalctl -u truckshield-weekly -f`.

| Task | Command |
|---|---|
| Deploy latest main | `sudo bash /opt/truckshield/app/deploy/update.sh` |
| Roll back | `sudo bash /opt/truckshield/app/deploy/update.sh <previous commit>` (the deploy prints it) |
| Run now | `sudo systemctl start truckshield-daily.service` (or `-weekly`) |
| Next scheduled runs | `systemctl list-timers 'truckshield*'` |
| Logs | `journalctl -u truckshield-daily --since today` |
| Run reports | `/opt/truckshield/data/reports/`, R2 `reports/`, table `pipeline_runs` |
| Change settings | `sudo nano /etc/truckshield.env` (read at the next run) |

The daily and weekly jobs share a lock, so a daily run waits while Sunday's weekly run finishes.

Alerts: a failed run posts to `ALERT_WEBHOOK_URL` with the failing steps and their errors,
and pings `<healthcheck>/fail`. A run that crashes or never starts misses its success ping,
so the dead man's switch alerts after its grace period.

## Weekly outreach routine

Each Sunday the weekly job builds the queue for the week (`QUEUE_SIZE`, split by `QUEUE_MIX`),
verifies the queued emails, and exports `data/queue/<monday>.csv` (also in R2 `queue/`).

1. Fill enrichment gaps (optional): `python scripts/contacts.py enrich-export --out gaps.csv`,
   have the vendor fill `email`, `contact_name`, `contact_phone`, then
   `python scripts/contacts.py enrich-import gaps.csv --vendor apollo`.
   Exports stop at `ENRICH_MONTHLY_CAP` credits a month.
2. Review: `python scripts/outreach.py export --out queue.csv`, fill the decision column
   with approve or reject, then `python scripts/outreach.py import-review queue.csv --by <name>`.
3. Send: `python scripts/outreach.py export-send --out send.csv` and load the file into the
   sending tool. In shadow mode (`OUTREACH_SHADOW=true`) nothing is released.
4. Outcomes: a CSV with `outreach_id`, `outcome` (replied, bounced, unsubscribed, won, lost)
   and `notes`, then `python scripts/outreach.py import-outcomes outcomes.csv`.
   Bounces and unsubscribes go on the suppression list.
5. Results per signal type: `python scripts/outreach.py report` (or the `outreach_outcomes` view).

Going live: after two shadow weeks, set `OUTREACH_SHADOW=false` in `/etc/truckshield.env`.
Rows queued while in shadow mode stay shadow.

## Phase checks

| Phase | Check | How |
|---|---|---|
| 3 | Reconcile finds close to zero mismatches | `mismatches` in the weekly report: rate per Motus table |
| 5 | 9 in 10 signals correct | `python scripts/signals.py check-export --n 50`, check each against SAFER/L&I, fill `correct (y/n)`, then `check-import <file> --by <name>`; it reports accuracy and `passes_90pct` |
| 6 | Outcomes per signal | `python scripts/outreach.py report` |

## When something fails

| Symptom | What to do |
|---|---|
| A step shows a network error | Rerun the job (`systemctl start ...`). Pages already on disk are reused. For a full pull, `pull_full.py --mode reconcile --date <failed run's date>` resumes it. |
| `row count X does not match the API's Y` | The source changed during the pull. Delete `data/raw/<kind>/<date>/<dataset>` and rerun. |
| `missing columns` | FMCSA changed the dataset layout. Nothing was written. Update the mapping in `load.py`, then rerun. |
| `new columns` warning | Harmless; the column is kept in Parquet but not loaded. |
| `source not updated for Nh` | FMCSA has not published. A warning under 96h; an error after. Nothing to do unless it persists. |
| `gap_warning` in the daily report | Daily files were missed (server down, FMCSA skipped). Sunday's reconcile fills the gap. |
| `R2 upload failed` | Data still loaded. Check the R2 keys; the next run re-uploads. |
| A `load` table `failed` | That table rolled back; others are unaffected. Fix and rerun; loads are idempotent. |
| `Out of Memory Error` | Set `DUCKDB_THREADS=1`, check free disk for spilling (`df -h`). |
| No alert, but the dead man's switch fired | The run crashed or never started: `systemctl status truckshield-daily`, `journalctl -u truckshield-daily`. |
