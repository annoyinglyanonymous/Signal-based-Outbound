"""storage.py against an in-memory stand-in for the S3 API."""
import pytest

import storage


class FakeS3:
    def __init__(self):
        self.objects = {}

    def upload_file(self, filename, bucket, key):
        self.objects[key] = open(filename, "rb").read()

    def download_file(self, bucket, key, filename):
        open(filename, "wb").write(self.objects[key])

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)

    def get_paginator(self, name):
        fake = self

        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in sorted(fake.objects) if k.startswith(Prefix)]}
        return P()


@pytest.fixture
def s3(monkeypatch):
    for k in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.setenv(k, "x")
    fake = FakeS3()
    monkeypatch.setattr(storage, "_client", fake)
    return fake


def test_noop_when_not_configured(monkeypatch, tmp_path):
    monkeypatch.delenv("R2_BUCKET", raising=False)
    assert storage.upload(tmp_path, "k") is None
    assert storage.list_keys("") == [] and storage.prune("parquet/full", 4) == []


def test_upload_dir_and_download(s3, tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "page_00001.csv").write_text("x,y\n1,2\n")
    keys = storage.upload_dir(tmp_path, "raw/daily/2026-11-22")
    assert keys == ["raw/daily/2026-11-22/a/page_00001.csv"]
    out = storage.download(keys[0], tmp_path / "back" / "p.csv")
    assert out.read_text() == "x,y\n1,2\n"


def test_prune_keeps_newest_folders(s3, tmp_path):
    f = tmp_path / "f"
    f.write_text("x")
    for day in ("2026-10-25", "2026-11-01", "2026-11-08", "2026-11-15", "2026-11-22"):
        storage.upload(f, f"parquet/full/{day}/census.parquet")
        storage.upload(f, f"parquet/full/{day}/motus_insur.parquet")
    storage.upload(f, "parquet/fleet/2026-10-25.parquet")
    removed = storage.prune("parquet/full", keep=4)
    assert removed == ["2026-10-25"]
    assert not any(k.startswith("parquet/full/2026-10-25") for k in s3.objects)
    assert "parquet/fleet/2026-10-25.parquet" in s3.objects      # other prefixes untouched
    assert len([k for k in s3.objects if k.startswith("parquet/full/")]) == 8
