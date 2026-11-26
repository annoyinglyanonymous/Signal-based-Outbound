"""Cloudflare R2 (S3-compatible) storage for raw files, Parquet snapshots and reports.

One bucket (R2_BUCKET), laid out as:
  raw/daily/<date>/<dataset>/page_*.csv     daily Motus CSVs, kept
  parquet/daily/<date>/<dataset>.parquet    daily Motus Parquet, kept
  parquet/full/<date>/<dataset>.parquet     weekly census + Motus full history, last KEEP_FULL kept
  parquet/fleet/<date>.parquet              weekly fleet-size snapshot of target carriers, kept
  reports/<job>/<timestamp>.json            run reports
  queue/<week>.csv                          weekly outreach queue exports

When R2 is not configured (local development) every call is a no-op that says so.
"""
import os
import pathlib

KEEP_FULL = int(os.getenv("R2_KEEP_FULL", "4"))
_client = None


def configured():
    return all(os.getenv(k) for k in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"))


def bucket():
    return os.environ["R2_BUCKET"]


def client():
    global _client
    if _client is None:
        import boto3
        endpoint = os.getenv("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        _client = boto3.client("s3", endpoint_url=endpoint, region_name="auto",
                               aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                               aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    return _client


def upload(local, key):
    if not configured():
        return None
    client().upload_file(str(local), bucket(), key)
    return key


def upload_dir(local_dir, prefix, pattern="*"):
    """Upload every file under local_dir matching pattern to prefix/<relative path>."""
    if not configured():
        return []
    local_dir = pathlib.Path(local_dir)
    return [upload(p, f"{prefix}/{p.relative_to(local_dir).as_posix()}")
            for p in sorted(local_dir.rglob(pattern)) if p.is_file()]


def list_keys(prefix):
    if not configured():
        return []
    keys = []
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=bucket(), Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def download(key, local):
    local = pathlib.Path(local)
    local.parent.mkdir(parents=True, exist_ok=True)
    client().download_file(bucket(), key, str(local))
    return local


def delete(keys):
    keys = list(keys)
    for i in range(0, len(keys), 1000):  # the S3 API takes at most 1000 keys per call
        client().delete_objects(Bucket=bucket(), Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
    return len(keys)


def prune(prefix, keep):
    """Keep only the newest `keep` dated folders directly under prefix (e.g. parquet/full/<date>/)."""
    if not configured():
        return []
    keys = list_keys(prefix.rstrip("/") + "/")
    folders = sorted({k[len(prefix):].strip("/").split("/")[0] for k in keys}, reverse=True)
    old = set(folders[keep:])
    delete(k for k in keys if k[len(prefix):].strip("/").split("/")[0] in old)
    return sorted(old)


def status():
    return {"r2": "configured" if configured() else "not configured; nothing uploaded"}
