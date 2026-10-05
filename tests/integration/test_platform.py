"""The running platform, from the host (`.\\make.ps1 test-integration`): S3, Postgres, Airflow.

Needs `.\\make.ps1 up`. Host-side clients use 127.0.0.1, never localhost (gotcha #20).
"""

from __future__ import annotations

import json
import urllib.request
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
BUCKETS = {"raw", "bronze", "stream", "marts"}


def env() -> dict[str, str]:
    """The secrets `.\\make.ps1 env` wrote; read here so no test ever prints them."""
    values = {}
    for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def test_an_object_round_trips_through_the_s3_api() -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import fs

    secrets = env()
    s3 = fs.S3FileSystem(
        access_key=secrets["S3_ACCESS_KEY"],
        secret_key=secrets["S3_SECRET_KEY"],
        endpoint_override="127.0.0.1:39000",
        scheme="http",
        region="us-east-1",
        force_virtual_addressing=False,
    )
    buckets = {info.path for info in s3.get_file_info(fs.FileSelector("", recursive=False))}
    assert buckets >= BUCKETS, f"missing buckets: {BUCKETS - buckets}"

    key = f"raw/_integration/{uuid.uuid4().hex}.parquet"
    table = pa.table({"id": [1, 2, 3], "type": ["PushEvent", "WatchEvent", "PushEvent"]})
    pq.write_table(table, key, filesystem=s3, compression="zstd")
    try:
        assert pq.read_table(key, filesystem=s3).equals(table)
    finally:
        s3.delete_file(key)
    assert s3.get_file_info(key).type == fs.FileType.NotFound


def test_a_row_round_trips_through_ghflow_ops() -> None:
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=35432,
        user="ghflow",
        password=env()["GHFLOW_OPS_PASSWORD"],
        dbname="ghflow_ops",
        connect_timeout=5,
    ) as conn:
        conn.execute("create temporary table probe (id int, note text)")
        conn.execute("insert into probe values (%s, %s)", (42, "m0-s4"))
        assert conn.execute("select id, note from probe").fetchall() == [(42, "m0-s4")]


def test_airflow_reports_itself_healthy() -> None:
    with urllib.request.urlopen("http://127.0.0.1:38082/api/v2/monitor/health", timeout=10) as r:
        health = json.load(r)
    assert health["metadatabase"]["status"] == "healthy", health
    assert health["scheduler"]["status"] == "healthy", health
