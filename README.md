# truckshield-fmcsa

Keeps FMCSA carrier data current in Supabase and turns changes into outreach signals for TruckShield.

Sources (data.transportation.gov, Socrata API):

| Name | Full dataset | Daily difference |
|---|---|---|
| Company Census File | `az4n-8mr2` | (none; weekly full pull) |
| Motus Carrier | `inys-ebih` | `nakq-58th` |
| Motus AuthHist | `yu5v-wbh6` | `dm5j-zc6c` |
| Motus Insur | `c5y8-a4uz` | `x96h-evps` |
| Motus InsHist | `3uet-3z4i` | `xe5s-wca7` |

Target carriers are active, interstate, for-hire, with at least one power unit (`common.TARGET_FILTER`).

## Layout

```
scripts/   common.py      API calls, resumable page pulls, DuckDB settings, validation
           pull_full.py   full census + Motus full history -> Parquet  (initial load, weekly reconcile)
           pull_daily.py  Motus daily differences -> Parquet
           load.py        Parquet -> Supabase (dedupe, upsert, close cancelled policies)
           profile_db.py  coverage, duplicate keys, carriers per ICP, database size
sql/       schema.sql     all tables; safe to re-run
tests/     unit tests on small fixture files
data/      raw/ (CSV pages), parquet/ (output), work/ (DuckDB staging and spill); not in git
```

## Setup

```
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt      # Linux: .venv/bin/pip
copy .env.example .env                             # then fill in the values
```

## Run

Every script prints a JSON report to stdout, logs progress to stderr, and exits 1 if anything failed.

```
# Full pull (all five datasets). --sample N pulls only N rows per dataset for testing.
python scripts/pull_full.py --mode initial > full_report.json
python scripts/pull_full.py --mode initial --only census

# Daily Motus differences
python scripts/pull_daily.py > daily_report.json

# Load into Supabase. initial also creates the schema if it is missing.
python scripts/load.py --mode initial --date 2026-11-23
python scripts/load.py --mode daily   --date <source date from the daily report>

# Profiling report on whatever database SUPABASE_DB_URL points at
python scripts/profile_db.py > profile_report.json

# Tests
python -m pytest -q
```

Disk and memory: the census is about 2 GB of CSV. It is streamed into a staging Parquet file and then
sorted, and DuckDB stays within `DUCKDB_MEMORY_LIMIT` (default 1 GB) by spilling to `data/work/tmp`.
The sort can spill around 10 GB, so keep 15 GB free. The census download takes about 2.5 hours (91 pages; the API is slow at deep offsets), then about 17 minutes to convert.

## When something fails

| Symptom | What to do |
|---|---|
| `status: failed` with a network error | Rerun the same command with `--date <the failed run's date>`. Pages already on disk are reused; only missing pages are fetched. |
| `row count X does not match the API's Y` | The source changed during the pull. Delete `data/raw/<kind>/<date>/<dataset>` and rerun. |
| `missing columns` | FMCSA changed the dataset layout. Nothing was written. Update the column mapping in `load.py` before rerunning. |
| `new columns` warning | Harmless; the new column is kept in Parquet but not loaded. |
| `Out of Memory Error` | Lower `DUCKDB_THREADS` to 1, or check that `WORK_DIR` has free disk for spilling. |
| `source not updated for Nh` | FMCSA has not published. The daily run pulls nothing; rerun the next day. |
| `load.py` table `failed` | That table's transaction rolled back; other tables are unaffected. Fix and rerun the same `--date`; loads are idempotent. |
