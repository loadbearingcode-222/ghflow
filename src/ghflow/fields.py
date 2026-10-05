"""What the contract projects: top-level keys, event types, payload fields and their eras.

Declarations only — no casting and no I/O. Eras are part of the shape, not branches in code:
each projected field says from when and until when it exists (`from_`/`until`). GitHub trimmed
payloads at 2025-10-08 17:00 UTC (M0-S2, `docs/data/gharchive.md`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

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
