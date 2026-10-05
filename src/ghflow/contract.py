"""The event contract: the ONE place a GH Archive field is cast (a test enforces it by AST).

Structure refuses; rows get counted (doctrine §8):

- **Structure** is the set of top-level keys, plus every projected payload field that is in its
  era. An unknown or missing top-level key, or an in-era field that vanished from every row of
  its type, refuses the whole hour: nothing is written, and the caller exits 1. A refused hour
  produces no metric at all — which is exactly why it must be loud.
- **A bad row** is counted under the FIRST named rule it breaks, kept in the rejected-row
  sidecar, and dropped. Rows in = accepted + rejected, always.

What is projected, and in which era, is declared in `ghflow.fields`; lines become values only
through `ghflow.events`. Pure: the caller hands in the lines (`ghflow.commands.contract` is the
CLI). A field out of its era is never projected; if it shows up anyway, it is counted as an era
violation.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ghflow import events, settings
from ghflow.fields import FIELDS, KNOWN_TYPES, OPTIONAL_KEYS, PAYLOAD_COLUMNS, REQUIRED_KEYS
from ghflow.hours import Hour

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

    def reject(self, number: int, rule: str, line: bytes | str) -> None:
        self.rules[rule] += 1
        self.rejected.append({"line": number, "rule": rule, "raw": events.text(line)})


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


def _event(line: bytes | str, number: int) -> dict[str, Any]:
    """The line as an event object; a row rule if it is not one, a Refusal if its keys moved."""
    try:
        event = events.parse(line)
    except ValueError:
        raise _RowRule("unparseable_json") from None
    if not isinstance(event, dict):
        raise _RowRule("not_an_object")
    keys = set(event)
    if unknown := keys - REQUIRED_KEYS - OPTIONAL_KEYS:
        raise Refusal("STRUCTURE-UNKNOWN-KEY", f"line {number}: {sorted(unknown)}")
    if missing := REQUIRED_KEYS - keys:
        raise Refusal("STRUCTURE-MISSING-KEY", f"line {number}: {sorted(missing)}")
    return event


def _refuse_vanished(result: HourResult, type_rows: Counter[str], hour: Hour) -> None:
    """Refuse the hour if an in-era field is absent from every row of a well-populated type."""
    for f in FIELDS:
        if not f.in_era(hour.start):
            continue
        for type_ in sorted(f.types):
            rows = type_rows[type_]
            vanished = result.missing_in_era[f"{type_}.{f.name}"] == rows
            if rows >= settings.VANISHED_MIN_ROWS and vanished:
                raise Refusal(
                    "STRUCTURE-FIELD-VANISHED",
                    f"{type_}.{'.'.join(f.path)} is in its era but absent from all {rows} rows",
                )


def apply(lines: Iterable[bytes | str], hour: Hour) -> HourResult:
    """Apply the contract to one hour. Raises Refusal on a structure change."""
    result = HourResult(hour.key)
    seen_ids: set[int] = set()
    type_rows: Counter[str] = Counter()
    for number, line in enumerate(lines, start=1):
        result.lines_in += 1
        try:
            event = _event(line, number)
            row = _envelope(event, hour)
            if row["id"] in seen_ids:
                raise _RowRule("duplicate_id")
        except _RowRule as bad:
            result.reject(number, bad.rule, line)
            continue
        seen_ids.add(row["id"])
        if row["type"] not in KNOWN_TYPES:
            result.unknown_types[str(row["type"])] += 1
        type_rows[str(row["type"])] += 1
        _project(row, event["payload"], result, hour)
        result.rows.append(row)
    _refuse_vanished(result, type_rows, hour)
    return result
