"""Build small Parquet files shaped like the FMCSA datasets, for tests."""
import pathlib

import duckdb

import load

FIX = pathlib.Path(__file__).parent / "fixtures"

COLUMNS = {
    "census": ["dot_number", "legal_name", "dba_name", "status_code", "carrier_operation", "classdef",
               "business_org_desc", "phy_street", "phy_city", "phy_state", "phy_zip", "phy_cnty",
               "carrier_mailing_street", "carrier_mailing_city", "carrier_mailing_state", "carrier_mailing_zip",
               "phone", "cell_phone", "fax", "email_address", "company_officer_1", "company_officer_2",
               "power_units", "truck_units", "total_drivers", "total_cdl", "fleetsize", "hm_ind",
               "safety_rating", "safety_rating_date", "mcs150_date", "add_date"]
              + [f"crgo_{c}" for c in load.CARGO]
              + [f"docket{k}{s}" for k in (1, 2, 3) for s in ("", "prefix")],
    "motus_carrier": ["usdot_number", "docket_number", "op_auth_type", "op_auth_status", "min_cov_amount",
                      "cargo_req", "bond_req", "bipd_file", "cargo_file", "bond_file", "legal_name", "dba_name",
                      "bus_street_po", "bus_city", "bus_state_code", "bus_zip_code", "bus_telno"],
    "motus_authhist": ["usdot_number", "docket_number", "op_auth_type", "op_auth_status", "reason",
                       "status_change_date"],
    "motus_insur": ["usdot_number", "docket_number", "policy_no", "ins_form_code", "ins_type_code",
                    "ins_class_code", "insurance_company_name", "max_cov_amount", "underl_lim_amount",
                    "effective_date", "trans_date"],
    "motus_inshist": ["usdot_number", "docket_number", "policy_no", "ins_form_code", "ins_type_code",
                      "ins_type_ind", "ins_type_desc", "ins_class_code", "filing_status_reason",
                      "insurance_company_name", "min_cov_amount", "max_cov_amount", "underl_lim_amount",
                      "effective_date", "cancl_effective_date"],
}

# A census row that passes the target filter; tests override what they need.
CARRIER = {"status_code": "A", "carrier_operation": "A", "classdef": "AUTHORIZED FOR HIRE",
           "power_units": "3", "mcs150_date": "20250101", "add_date": "20200101", "hm_ind": "N",
           "phone": "(555) 123-4567", "company_officer_1": "PAT LEE"}


def write(path, dataset, rows):
    """Write rows (dicts of text values) to a Parquet file with all of the dataset's columns."""
    cols = COLUMNS[dataset]
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (" + ", ".join(f'"{c}" VARCHAR' for c in cols) + ")")
    for row in rows:
        unknown = set(row) - set(cols)
        assert not unknown, f"unknown columns {unknown}"
        con.execute(f"INSERT INTO t VALUES ({', '.join('?' for _ in cols)})", [row.get(c) for c in cols])
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT parquet)")
    con.close()
    return path


def carrier(dot, **kw):
    return {**CARRIER, "dot_number": str(dot), "legal_name": f"CARRIER {dot}", **kw}
