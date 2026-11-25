"""Shared fixtures. Database tests need TEST_DB_URL (a throwaway Postgres 15+);
without it they are skipped. Each test gets a fresh public schema."""
import os
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "sql" / "schema.sql"


@pytest.fixture
def db_url(monkeypatch):
    url = os.getenv("TEST_DB_URL")
    if not url:
        pytest.skip("TEST_DB_URL not set")
    import psycopg
    with psycopg.connect(url, autocommit=True) as pg:
        pg.execute("DROP SCHEMA public CASCADE")
        pg.execute("CREATE SCHEMA public")
        pg.execute(SCHEMA.read_text())
    monkeypatch.setenv("SUPABASE_DB_URL", url)
    return url


@pytest.fixture
def pg(db_url):
    import psycopg
    with psycopg.connect(db_url, autocommit=True) as conn:
        yield conn


@pytest.fixture
def data_dirs(tmp_path, monkeypatch):
    """Point every data folder at a temporary directory."""
    import common
    import load
    dirs = {k: tmp_path / k for k in ("raw", "parquet", "work")}
    for d in dirs.values():
        d.mkdir()
    monkeypatch.setattr(load, "PARQUET_DIR", dirs["parquet"])
    monkeypatch.setenv("WORK_DIR", str(dirs["work"]))
    return dirs
