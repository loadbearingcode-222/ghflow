"""Cross-milestone cohesion (M5-S3): no unused public code, no helper or constant list written
twice across modules. The size budget lives in test_laws.py."""

from __future__ import annotations

import ast
import hashlib
from collections import defaultdict
from pathlib import Path

import vulture

SRC = Path(__file__).resolve().parents[2] / "src" / "ghflow"

#: Reviewed: what vulture cannot see being used, each with its reason (M5-S3).
ALLOWLIST = {
    # dataclass fields, read through asdict() / the audit row
    **dict.fromkeys(["hour_start", "http_verdict", "content_length", "refusal_code", "rows_in",
                     "rows_accepted", "rows_rejected", "raw_sha256", "bronze_sha256",
                     "sidecar_sha256", "duration_ms", "raw_bytes", "payload_bytes", "outside_hour",
                     "reference_hours"], "dataclass field, serialized whole"),
    # framework callbacks
    **dict.fromkeys(["handle_starttag", "handle_endtag", "handle_data"], "HTMLParser callback"),
    # read by the law tests and docs, not by src
    "ROW_RULES": "first-match rule order, asserted by test_contract",
    "SOURCE_KEYS": "the one-cast law test reads it",
    "FIGURES": "the dashboard template test reads it",
    "STREAM_RETENTION_DAYS": "topics.conf mirrors it (test_repo_hygiene)",
    # used outside src
    "download_hour": "scripts/bench.py caches reference hours with it",
}  # fmt: skip


def test_no_unused_public_code() -> None:
    v = vulture.Vulture()
    v.scavenge([str(SRC)])
    unused = [f"{Path(i.filename).relative_to(SRC)}:{i.first_lineno} {i.name}"
              for i in v.get_unused_code(min_confidence=60) if i.name not in ALLOWLIST]  # fmt: skip
    assert unused == [], f"unused (delete it, or allowlist it with a reason): {unused}"


def test_the_allowlist_has_no_stale_entries() -> None:
    v = vulture.Vulture()
    v.scavenge([str(SRC)])
    flagged = {i.name for i in v.get_unused_code(min_confidence=60)}
    assert set(ALLOWLIST) <= flagged, (
        f"no longer unused, drop from the allowlist: {set(ALLOWLIST) - flagged}"
    )


def _bodies() -> dict[str, list[str]]:
    seen: dict[str, list[str]] = defaultdict(list)
    for path in SRC.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text("utf-8"))):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                body = node.body
                if (
                    body
                    and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                ):
                    body = body[1:]  # the docstring does not make a body different
                if len(body) >= 2:
                    key = hashlib.sha1("".join(ast.dump(b) for b in body).encode()).hexdigest()
                    seen[key].append(f"{path.relative_to(SRC)}:{node.name}")
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.List | ast.Tuple):
                items = node.value.elts
                if len(items) >= 3 and all(
                    isinstance(e, ast.Constant) and isinstance(e.value, str) for e in items
                ):
                    seen["const:" + ast.dump(node.value)].append(f"{path.relative_to(SRC)}:list")
    return seen


def test_no_helper_or_constant_list_is_written_twice() -> None:
    dupes = [where for where in _bodies().values() if len({w.split(":")[0] for w in where}) > 1]
    assert dupes == [], f"one home each: import it instead: {dupes}"
