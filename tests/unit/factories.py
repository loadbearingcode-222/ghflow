"""The one way tests build a synthetic GH Archive event (no real user data, ever).

A change to the event envelope is one edit here, not one per test module. The declared hazard
rows stay in tests/fixtures/hazard_rows.json: they are evidence, not a factory.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from ghflow.hours import Hour

HOUR = Hour.parse("2026-10-02-15")


def event(
    type_: str = "PushEvent",
    payload: dict[str, Any] | None = None,
    *,
    hour: Hour = HOUR,
    minute: int = 7,
    id_: int = 40_000_000_001,
) -> dict[str, Any]:
    """A contract-valid event: full envelope, synthetic actor, repo and org."""
    created = (hour.start + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "id": str(id_),
        "type": type_,
        "actor": {"id": 1, "login": "synthetic-dev"},
        "repo": {"id": 2, "name": "synthetic-org/repo"},
        "payload": {} if payload is None else payload,
        "public": True,
        "created_at": created,
        "org": {"id": 9, "login": "synthetic-org"},
    }


def line(body: dict[str, Any]) -> bytes:
    """One archive line, as the archive serves it: compact JSON and a newline."""
    return (json.dumps(body, separators=(",", ":")) + "\n").encode()


def push(id_: int, **payload: Any) -> dict[str, Any]:
    """A PushEvent with the post-trim payload, overridable per key."""
    body = {
        "repository_id": 2,
        "push_id": 3,
        "ref": "refs/heads/main",
        "head": "aaa",
        "before": "bbb",
    }
    return event("PushEvent", body | payload, id_=id_)
