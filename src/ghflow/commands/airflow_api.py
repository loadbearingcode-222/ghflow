"""A minimal client for Airflow 3's REST API (`/api/v2`), authenticated as the admin user.

Used where the CLI cannot do the job: Airflow 3.3.2's `airflow tasks clear` silently matches
nothing (CLAUDE.md), and backfills are paused and resumed only through the API.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

from ghflow.commands.local import AIRFLOW_URL, secret


class Airflow:
    def __init__(self, base: str = AIRFLOW_URL) -> None:
        self.base = base
        auth = {"username": "admin", "password": secret("AIRFLOW_ADMIN_PASSWORD")}
        self.token = self._send("POST", "/auth/token", auth, token=None)["access_token"]

    def _send(self, method: str, path: str, body: Any, token: str | None) -> Any:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data, headers, method=method)
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read()
        return json.loads(text) if text else None

    def call(self, method: str, path: str, body: Any = None, **query: Any) -> Any:
        if query:
            path = f"{path}?{urllib.parse.urlencode(query, doseq=True)}"
        return self._send(method, path, body, self.token)
