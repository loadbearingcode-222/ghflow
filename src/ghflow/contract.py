"""The event contract: the ONE place a GH Archive field is cast (a test enforces it by AST).

Structure refuses; rows get counted (doctrine §8):

- **Structure** is the set of top-level keys, plus every projected payload field that is in its
  era. An unknown or missing top-level key, or an in-era field that vanished from every row of
  its type, refuses the whole hour: nothing is written, and the caller exits 1. A refused hour
  produces no metric at all — which is exactly why it must be loud.
- **A bad row** is counted under the FIRST named rule it breaks, kept in the rejected-row
  sidecar, and dropped. Rows in = accepted + rejected, always.

Eras are part of the shape, not branches in code: each projected field says from when and until
when it exists (`from_`/`until`). GitHub trimmed payloads at 2025-10-08 17:00 UTC (M0-S2,
`docs/data/gharchive.md`). A field out of its era is never projected; if it shows up anyway, it
is counted as an era violation.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ghflow.archive import open_hour
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour

#: When GitHub trimmed event payloads; the first hour of the new shape (M0-S2 bisection).
TRIM = datetime(2025, 10, 8, 17, tzinfo=UTC)

REQUIRED_KEYS = frozenset({"id", "type", "actor", "repo", "payload", "public", "created_at"})
OPTIONAL_KEYS = frozenset({"org"})

KNOWN_TYPES = frozenset(
    {
        "CommitCommentEvent", "CreateEvent", "DeleteEvent", "DiscussionEvent", "ForkEvent",
        "GollumEvent", "IssueCommentEvent", "IssuesEvent", "MemberEvent", "PublicEvent",
        "PullRequestEvent", "PullRequestReviewCommentEvent", "PullRequestReviewEvent",
        "PushEvent", "ReleaseEvent", "WatchEvent",
    }
)  # fmt: skip

#: An in-era field missing from every row of its type refuses the hour only if the type has at
#: least this many rows — one row without an optional key is not a schema change.
VANISHED_MIN_ROWS = 20


@dataclass(frozen=True)
class Field:
    """A projected payload field: where it lives, what it is, and when it exists."""

    name: str
    types: frozenset[str]
    path: tuple[str, ...]
    kind: str  # "int" | "str" | "bool" | "count" (len of a list)
    from_: datetime | None = None
    until: datetime | None = None

    def in_era(self, at: datetime) -> bool:
        return (self.from_ is None or at >= self.from_) and (self.until is None or at < self.until)


def _types(*names: str) -> frozenset[str]:
    return frozenset(names)


ACTION_TYPES = _types(
    "IssuesEvent", "IssueCommentEvent", "MemberEvent", "PullRequestEvent",
    "PullRequestReviewCommentEvent", "PullRequestReviewEvent", "ReleaseEvent", "WatchEvent",
)  # fmt: skip
PR_TYPES = _types("PullRequestEvent", "PullRequestReviewCommentEvent", "PullRequestReviewEvent")

FIELDS: tuple[Field, ...] = (
    Field("action", ACTION_TYPES, ("action",), "str"),
    Field("action", _types("ForkEvent", "CommitCommentEvent"), ("action",), "str", from_=TRIM),
    Field("push_id", _types("PushEvent"), ("push_id",), "int"),
    Field("ref", _types("PushEvent", "CreateEvent", "DeleteEvent"), ("ref",), "str"),
    Field("head_sha", _types("PushEvent"), ("head",), "str"),
    Field("before_sha", _types("PushEvent"), ("before",), "str"),
    Field("push_size", _types("PushEvent"), ("size",), "int", until=TRIM),
    Field("push_distinct_size", _types("PushEvent"), ("distinct_size",), "int", until=TRIM),
    Field("ref_type", _types("CreateEvent", "DeleteEvent"), ("ref_type",), "str"),
    Field("full_ref", _types("CreateEvent", "DeleteEvent"), ("full_ref",), "str", from_=TRIM),
    Field("pr_number", _types("PullRequestEvent"), ("number",), "int"),
    Field("pr_id", PR_TYPES, ("pull_request", "id"), "int"),
    Field("pr_merged", _types("PullRequestEvent"), ("pull_request", "merged"), "bool", until=TRIM),
    Field("pr_additions", _types("PullRequestEvent"), ("pull_request", "additions"), "int", until=TRIM),
    Field("pr_deletions", _types("PullRequestEvent"), ("pull_request", "deletions"), "int", until=TRIM),
    Field("pr_changed_files", _types("PullRequestEvent"), ("pull_request", "changed_files"), "int", until=TRIM),
    Field("issue_number", _types("IssuesEvent", "IssueCommentEvent"), ("issue", "number"), "int"),
    Field("comment_id", _types("IssueCommentEvent", "PullRequestReviewCommentEvent", "CommitCommentEvent"), ("comment", "id"), "int"),
    Field("review_state", _types("PullRequestReviewEvent"), ("review", "state"), "str"),
    Field("release_tag", _types("ReleaseEvent"), ("release", "tag_name"), "str"),
    Field("forkee_id", _types("ForkEvent"), ("forkee", "id"), "int"),
    Field("pages", _types("GollumEvent"), ("pages",), "count"),
)  # fmt: skip

PAYLOAD_COLUMNS: tuple[str, ...] = tuple(dict.fromkeys(f.name for f in FIELDS))
ENVELOPE_COLUMNS: tuple[str, ...] = (
    "id", "type", "created_at", "actor_id", "actor_login", "repo_id", "repo_name",
    "org_id", "org_login", "public",
)  # fmt: skip
COLUMNS = ENVELOPE_COLUMNS + PAYLOAD_COLUMNS

#: Every key a caller could read out of a raw event: what the AST test treats as a source field.
SOURCE_KEYS = frozenset(
    REQUIRED_KEYS | OPTIONAL_KEYS | {"login", "name"} | {p for f in FIELDS for p in f.path}
)

#: Row rules, in first-match order. The order is law: the sidecar files each row under one rule,
#: so its per-rule counts reconcile with the report exactly.
ROW_RULES = (
    "unparseable_json",
    "not_an_object",
    "id_not_numeric",
    "created_at_unparseable",
    "created_at_outside_hour",
    "actor_malformed",
    "repo_malformed",
    "duplicate_id",
)


class Refusal(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass
class HourResult:
    hour: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    lines_in: int = 0
    rules: Counter[str] = field(default_factory=Counter)
    unknown_types: Counter[str] = field(default_factory=Counter)
    era_violations: Counter[str] = field(default_factory=Counter)
    missing_in_era: Counter[str] = field(default_factory=Counter)

    def report(self) -> dict[str, Any]:
        return {
            "hour": self.hour,
            "lines_in": self.lines_in,
            "accepted": len(self.rows),
            "rejected": len(self.rejected),
            "rules": dict(sorted(self.rules.items())),
            "unknown_types": dict(sorted(self.unknown_types.items())),
            "era_violations": dict(sorted(self.era_violations.items())),
            "missing_in_era": dict(sorted(self.missing_in_era.items())),
        }


# ------------------------------------------------------------------ the casts (the only ones)


class _RowRule(Exception):
    def __init__(self, rule: str) -> None:
        super().__init__(rule)
        self.rule = rule


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _as_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _as_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _as_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC)


def _cast(kind: str, value: Any) -> Any:
    if kind == "int":
        return _as_int(value)
    if kind == "str":
        return _as_str(value)
    if kind == "bool":
        return _as_bool(value)
    if kind == "count":
        return len(value) if isinstance(value, list) else None
    raise ValueError(f"unknown field kind {kind!r}")


_MISSING = object()


def _dig(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    node: Any = payload
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return _MISSING
        node = node[key]
    return node


# ------------------------------------------------------------------ one row


def _envelope(event: dict[str, Any], hour: Hour) -> dict[str, Any]:
    event_id = _as_int(event["id"])
    if event_id is None:
        raise _RowRule("id_not_numeric")
    created = _as_timestamp(event["created_at"])
    if created is None:
        raise _RowRule("created_at_unparseable")
    if not hour.start <= created < hour.plus(1).start:
        raise _RowRule("created_at_outside_hour")
    actor, repo, org = event["actor"], event["repo"], event.get("org")
    if (
        not isinstance(actor, dict)
        or _as_int(actor.get("id")) is None
        or _as_str(actor.get("login")) is None
    ):
        raise _RowRule("actor_malformed")
    if (
        not isinstance(repo, dict)
        or _as_int(repo.get("id")) is None
        or _as_str(repo.get("name")) is None
    ):
        raise _RowRule("repo_malformed")
    org = org if isinstance(org, dict) else {}
    return {
        "id": event_id,
        "type": _as_str(event["type"]),
        "created_at": created,
        "actor_id": _as_int(actor["id"]),
        "actor_login": _as_str(actor["login"]),
        "repo_id": _as_int(repo["id"]),
        "repo_name": _as_str(repo["name"]),
        "org_id": _as_int(org.get("id")),
        "org_login": _as_str(org.get("login")),
        "public": _as_bool(event["public"]),
    }


def _project(row: dict[str, Any], payload: Any, result: HourResult, hour: Hour) -> None:
    payload = payload if isinstance(payload, dict) else {}
    for column in PAYLOAD_COLUMNS:
        row.setdefault(column, None)
    for f in FIELDS:
        if row["type"] not in f.types:
            continue
        value = _dig(payload, f.path)
        if f.in_era(hour.start):
            if value is _MISSING:
                result.missing_in_era[f"{row['type']}.{f.name}"] += 1
            else:
                row[f.name] = _cast(f.kind, value)
        elif value is not _MISSING:
            result.era_violations[f"{row['type']}.{f.name}"] += 1


# ------------------------------------------------------------------ one hour


def apply(lines: Iterable[bytes | str], hour: Hour) -> HourResult:
    """Apply the contract to one hour. Raises Refusal on a structure change."""
    result = HourResult(hour.key)
    seen_ids: set[int] = set()
    type_rows: Counter[str] = Counter()
    for number, line in enumerate(lines, start=1):
        result.lines_in += 1
        raw = line.decode("utf-8", errors="replace") if isinstance(line, bytes) else line
        try:
            try:
                event = json.loads(raw)
            except ValueError:
                raise _RowRule("unparseable_json") from None
            if not isinstance(event, dict):
                raise _RowRule("not_an_object")
            keys = set(event)
            if unknown := keys - REQUIRED_KEYS - OPTIONAL_KEYS:
                raise Refusal("STRUCTURE-UNKNOWN-KEY", f"line {number}: {sorted(unknown)}")
            if missing := REQUIRED_KEYS - keys:
                raise Refusal("STRUCTURE-MISSING-KEY", f"line {number}: {sorted(missing)}")
            row = _envelope(event, hour)
            if row["id"] in seen_ids:
                raise _RowRule("duplicate_id")
            seen_ids.add(row["id"])
        except _RowRule as bad:
            result.rules[bad.rule] += 1
            result.rejected.append({"line": number, "rule": bad.rule, "raw": raw.rstrip("\n")})
            continue
        if row["type"] not in KNOWN_TYPES:
            result.unknown_types[str(row["type"])] += 1
        type_rows[str(row["type"])] += 1
        _project(row, event["payload"], result, hour)
        result.rows.append(row)

    for f in FIELDS:
        if not f.in_era(hour.start):
            continue
        for type_ in sorted(f.types):
            rows = type_rows[type_]
            if rows >= VANISHED_MIN_ROWS and result.missing_in_era[f"{type_}.{f.name}"] == rows:
                raise Refusal(
                    "STRUCTURE-FIELD-VANISHED",
                    f"{type_}.{'.'.join(f.path)} is in its era but absent from all {rows} rows",
                )
    return result


# ------------------------------------------------------------------ CLI


def main(argv: list[str] | None = None) -> int:
    """Check hours (streamed, or one local .json file) against the contract; write no data.

    With --record, the per-hour verdicts are written to that JSON file as evidence. A refused
    hour still produces no output data: the record is about the run, not the hour's rows.
    """
    parser = argparse.ArgumentParser(description=(main.__doc__ or "").strip().splitlines()[0])
    parser.add_argument("hours", nargs="+", help="GH Archive hour keys, e.g. 2026-10-02-15")
    parser.add_argument("--file", type=Path, help="read lines from this file (one hour only)")
    parser.add_argument("--record", type=Path, help="write per-hour verdicts here")
    args = parser.parse_args(argv)

    def check(hour: Hour) -> dict[str, Any]:
        try:
            if args.file:
                with args.file.open("rb") as lines:
                    result = apply(lines, hour)
            else:
                with open_hour(hour) as lines:
                    result = apply(lines, hour)
        except Refusal as refusal:
            print(f"REFUSED {hour.key}: {refusal}", file=sys.stderr)
            return {
                "hour": hour.key,
                "verdict": "refused",
                "code": refusal.code,
                "detail": refusal.detail,
            }
        return {"verdict": "passed", **result.report()}

    def run() -> int:
        if args.file and len(args.hours) != 1:
            print("--file takes exactly one hour", file=sys.stderr)
            return ExitCode.COULD_NOT_BUILD
        verdicts = [check(Hour.parse(key)) for key in args.hours]
        if args.record:
            record = {"story": "M0-S3", "contract_trim": TRIM.isoformat(), "hours": verdicts}
            args.record.parent.mkdir(parents=True, exist_ok=True)
            text = json.dumps(record, indent=2) + "\n"
            args.record.write_text(text, encoding="utf-8", newline="\n")
        elif not any(v["verdict"] == "refused" for v in verdicts):
            print(json.dumps(verdicts, indent=2))
        refused = any(v["verdict"] == "refused" for v in verdicts)
        return ExitCode.REFUSED if refused else ExitCode.PASSED

    return guarded(run)


if __name__ == "__main__":
    sys.exit(main())
