"""The one JSON-line reader: every GH Archive line becomes a Python value here, and only here.

An AST test (`tests/unit/test_events.py`) refuses a JSON parse of raw lines anywhere else, so
the parser, and how undecodable bytes are treated, can change in exactly one place. Pure: it
reads iterables it is given and never opens a file or a socket.

Undecodable bytes are replaced (U+FFFD), never fatal: whether such a line is acceptable is the
contract's decision, not the reader's.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any


def text(raw: bytes | str) -> str:
    """The line as text, for humans and the rejected-row sidecar (trailing newline removed)."""
    decoded = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    return decoded.rstrip("\n")


def parse(raw: bytes | str) -> Any:
    """One line as a JSON value. Raises ValueError when the line is not JSON."""
    return json.loads(raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw)


def events(lines: Iterable[bytes | str]) -> Iterator[Any]:
    """Every line's JSON value, in order. Raises ValueError on the first line that is not JSON."""
    for line in lines:
        yield parse(line)


def sized(lines: Iterable[bytes | str]) -> Iterator[tuple[int, Any]]:
    """(raw length, JSON value) per line: for measurements that weigh lines as well as count them."""
    for line in lines:
        yield len(line), parse(line)
