"""Unit tests on small fixture files. No network or database needed."""
import pathlib

import duckdb
import pytest

import common
import load

FIX = pathlib.Path(__file__).parent / "fixtures"

# Census columns load.py reads that the small fixture leaves out (filled with NULL).
CENSUS_EXTRA = ["dba_name", "business_org_desc", "phy_street", "phy_city", "phy_state", "phy_zip",
                "phy_cnty", "carrier_mailing_street", "carrier_mailing_city", "carrier_mailing_state",
                "carrier_mailing_zip", "phone", "cell_phone", "fax", "company_officer_1",
                "company_officer_2", "truck_units", "total_drivers", "total_cdl", "fleetsize",
                "safety_rating", "safety_rating_date"]
CENSUS_EXTRA += [f"crgo_{c}" for c in load.CARGO if c not in ("genfreight", "household")]
CENSUS_EXTRA += [f"docket{k}{s}" for k in (1, 2, 3) for s in ("", "prefix")]


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture
def census_parquet(con, tmp_path):
    con.execute(f"CREATE TABLE census AS SELECT * FROM read_csv('{(FIX / 'census.csv').as_posix()}', "
                "header=true, all_varchar=true)")
    for col in CENSUS_EXTRA:
        con.execute(f"ALTER TABLE census ADD COLUMN {col} VARCHAR")
    con.execute("UPDATE census SET docket1prefix = 'MC', docket1 = ' 123 ' WHERE dot_number = '100'")
    path = tmp_path / "census.parquet"
    con.execute(f"COPY census TO '{path.as_posix()}' (FORMAT parquet)")
    return path


def test_csv_rows_handles_quoted_fields():
    assert common.csv_rows((FIX / "page_00001.csv").read_text()) == 2
    assert common.csv_rows("") == 0
    assert common.csv_rows("a,b\n") == 0


def test_target_filter(con, census_parquet):
    dots = [r[0] for r in con.execute(
        f"SELECT DISTINCT dot_number FROM read_parquet('{census_parquet.as_posix()}') "
        f"WHERE {common.TARGET_FILTER} ORDER BY 1").fetchall()]
    # 101 private, 102 intrastate, 103 inactive, 104 no power units are all excluded
    assert dots == ["100", "105"]


def test_date_parsing(con):
    rows = con.execute(f"SELECT {load.d('x')} FROM (VALUES ('20250131'), (''), ('  '), "
                       "('bad-date'), ('20251399'), (NULL)) v(x)").fetchall()
    assert [r[0] for r in rows] == [__import__("datetime").date(2025, 1, 31), None, None, None, None, None]


def test_carriers_build_dedupes_and_maps(con, census_parquet):
    spec = load.TABLES["carriers"]
    cols = load.build(con, spec, census_parquet, "2026-11-19")
    assert cols[0] == "usdot_number"
    rows = {r[0]: r for r in con.execute(
        "SELECT usdot_number, legal_name, mcs150_date, email_address, cargo_types, dockets, hazmat, add_date "
        "FROM staged").fetchall()}
    assert sorted(rows) == [100, 105]
    # duplicate DOT 100: the newest MCS-150 wins
    assert rows[100][1] == "ACTIVE FOR HIRE LLC"
    assert rows[100][3] == "ops@example.com"
    assert rows[100][4] == ["genfreight"]
    assert rows[100][5] == ["MC123"]
    assert rows[105][4] == ["household"]
    assert rows[105][6] is True
    assert rows[105][7] is None  # unparseable date becomes NULL, not an error


def test_load_pages_unions_columns_and_counts(con, tmp_path):
    pages = sorted(FIX.glob("page_*.csv"))
    n = common.load_pages(con, "t", pages, tmp_path)
    assert n == 3
    cols = {r[0] for r in con.execute("DESCRIBE t").fetchall()}
    assert cols == {"usdot_number", "effective_date", "name", "new_col"}
    assert con.execute("SELECT name FROM t WHERE usdot_number = '1'").fetchone()[0] == 'Smith, "Bob"'


def test_validate_flags_columns_and_dates(con, tmp_path):
    common.load_pages(con, "t", sorted(FIX.glob("page_*.csv")), tmp_path)
    meta = {"columns": [{"fieldName": f} for f in ("usdot_number", "effective_date", "name", "gone", ":id")]}
    errors, warnings = common.validate(con, "t", meta, strict_dates=["effective_date"])
    assert "missing columns: ['gone']" in errors
    assert "effective_date: 1 values not YYYYMMDD" in errors
    assert "new columns: ['new_col']" in warnings


def test_every_table_spec_has_a_unique_key_in_schema():
    schema = (pathlib.Path(load.__file__).resolve().parent.parent / "sql" / "schema.sql").read_text()
    for table, spec in load.TABLES.items():
        assert f"CREATE TABLE IF NOT EXISTS {table} (" in schema
        key = ", ".join(spec["key"])
        assert key in schema.replace("\n", " ").replace("        ", ""), table
