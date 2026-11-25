import csv
import datetime as dt
import io
import json
import os
import pathlib
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "https://data.transportation.gov"
TOKEN = os.getenv("SOCRATA_TOKEN")
HEADERS = {"X-App-Token": TOKEN} if TOKEN else {}
PAGE = 50_000

# Target carriers: active, interstate, for-hire, at least one power unit.
TARGET_FILTER = (
    "status_code = 'A' AND carrier_operation = 'A' "
    "AND classdef LIKE '%AUTHORIZED FOR HIRE%' "
    "AND TRY_CAST(power_units AS INTEGER) > 0"
)


def log(msg):
    """Progress goes to stderr so stdout stays clean for the JSON report."""
    print(msg, file=sys.stderr, flush=True)


def csv_rows(text):
    return max(sum(1 for _ in csv.reader(io.StringIO(text))) - 1, 0)


def retryable(exc):
    """Network errors, rate limits and server errors can pass on retry; a bad query cannot."""
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and (exc.response.status_code == 429 or exc.response.status_code >= 500)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def get(url, params=None, retries=4, wait=15):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=300)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == retries or not retryable(e):
                raise
            time.sleep(wait * attempt)


def metadata(ds_id):
    return get(f"{BASE}/api/views/{ds_id}.json").json()


def freshness(meta):
    """Returns (last updated UTC, age in hours) from dataset metadata."""
    updated = dt.datetime.fromtimestamp(meta["rowsUpdatedAt"], dt.timezone.utc)
    age_h = (dt.datetime.now(dt.timezone.utc) - updated).total_seconds() / 3600
    return updated, age_h


def count_rows(ds_id, where=None):
    params = {"$select": "count(*) AS n"}
    if where:
        params["$where"] = where
    return int(get(f"{BASE}/resource/{ds_id}.json", params).json()[0]["n"])


def pull_pages(ds_id, out_dir, where=None, max_rows=None, log=print):
    """Pull a dataset page by page into out_dir/page_00001.csv, ...

    Resumable: pages already on disk are skipped. Each page is written to a
    .part file and renamed only when complete, so a crash never leaves a
    half-written page that looks finished.
    """
    out_dir = pathlib.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    page_no, offset = 1, 0
    while True:
        limit = PAGE if max_rows is None else min(PAGE, max_rows - offset)
        if limit <= 0:
            break
        path = out_dir / f"page_{page_no:05d}.csv"
        if path.exists():
            log(f"  page {page_no}: already on disk, skipping")
            rows = csv_rows(path.read_text(encoding="utf-8"))
        else:
            params = {"$limit": limit, "$offset": offset, "$order": ":id"}
            if where:
                params["$where"] = where
            text = get(f"{BASE}/resource/{ds_id}.csv", params).text
            rows = csv_rows(text)
            if rows == 0:
                break
            tmp = path.with_suffix(".part")
            tmp.write_text(text, encoding="utf-8")
            tmp.rename(path)
            log(f"  page {page_no}: {rows:,} rows")
        if rows < limit:
            break
        page_no, offset = page_no + 1, offset + limit
    return sorted(out_dir.glob("page_*.csv"))


def configure(con, work_dir, memory_limit=None, threads=None):
    """Keep DuckDB inside a small box (the Droplet has 2 GB) and let it spill to disk."""
    work_dir = pathlib.Path(work_dir)
    (work_dir / "tmp").mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = '{memory_limit or os.getenv('DUCKDB_MEMORY_LIMIT', '1GB')}'")
    con.execute(f"SET threads = {int(threads or os.getenv('DUCKDB_THREADS', '2'))}")
    con.execute(f"SET temp_directory = '{(work_dir / 'tmp').as_posix()}'")
    con.execute("SET preserve_insertion_order = false")


def load_pages(con, table, pages, stage_dir):
    """Expose CSV pages as a DuckDB view named `table`, all columns as text.

    The pages are first streamed into an unsorted staging Parquet file. Streaming
    never holds the whole dataset in memory (a 4.5M-row census table did, and ran
    out of memory), and later scans of Parquet are much faster than of CSV.
    """
    stage = pathlib.Path(stage_dir) / f"{table}.stage.parquet"
    stage.parent.mkdir(parents=True, exist_ok=True)
    files = "[" + ", ".join(f"'{p.as_posix()}'" for p in pages) + "]"
    con.execute(f"COPY (SELECT * FROM read_csv({files}, header=true, all_varchar=true, union_by_name=true)) "
                f"TO '{stage.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{stage.as_posix()}')")
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def validate(con, table, meta, strict_dates=(), soft_dates=()):
    errors, warnings = [], []
    got = {c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()}
    expected = {c["fieldName"] for c in meta["columns"] if not c["fieldName"].startswith(":")}
    if missing := expected - got:
        errors.append(f"missing columns: {sorted(missing)}")
    if extra := got - expected:
        warnings.append(f"new columns: {sorted(extra)}")
    for col, bucket in [(c, errors) for c in strict_dates] + [(c, warnings) for c in soft_dates]:
        if col not in got:
            continue
        bad = con.execute(
            f"SELECT count(*) FROM {table} WHERE {col} IS NOT NULL AND {col} <> '' "
            f"AND NOT regexp_matches({col}, '^[0-9]{{8}}$')").fetchone()[0]
        if bad:
            bucket.append(f"{col}: {bad:,} values not YYYYMMDD")
    return errors, warnings


def write_parquet(con, table, path, order_by=None):
    """Write a table to Parquet with zstd compression, optionally sorted."""
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    order = f" ORDER BY {order_by}" if order_by else ""
    con.execute(f"COPY (SELECT * FROM {table}{order}) TO '{path.as_posix()}' "
                f"(FORMAT parquet, COMPRESSION zstd)")
    return path.stat().st_size


def print_report(report):
    print(json.dumps(report, indent=2, default=str))