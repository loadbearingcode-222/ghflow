"""Reaching the running stack from the host: endpoints on 127.0.0.1 and secrets from `.env`.

Inside compose the same values arrive as environment variables (S3_ENDPOINT, GHFLOW_OPS_DSN);
an environment variable always wins, so a command runs unchanged in either place. Secrets are
read, never printed.
"""

from __future__ import annotations

import os
from pathlib import Path

from ghflow.storage import S3Config

REPO_ROOT = Path(__file__).resolve().parents[3]
AIRFLOW_URL = "http://127.0.0.1:38082"


def dotenv() -> dict[str, str]:
    path = REPO_ROOT / ".env"
    if not path.exists():
        return {}
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def secret(name: str) -> str:
    value = os.environ.get(name) or dotenv().get(name)
    if not value:
        raise KeyError(f"{name} is not set: run .\\make.ps1 env (or set it in the environment)")
    return value


def ops_dsn() -> str:
    return os.environ.get("GHFLOW_OPS_DSN") or (
        f"postgresql://ghflow:{secret('GHFLOW_OPS_PASSWORD')}@127.0.0.1:35432/ghflow_ops"
    )


def s3_config() -> S3Config:
    endpoint = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:39000")
    return S3Config(endpoint, secret("S3_ACCESS_KEY"), secret("S3_SECRET_KEY"))
