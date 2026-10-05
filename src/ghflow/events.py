"""The one JSON-line reader: every GH Archive line becomes a Python value here, and only here.

An AST test (`tests/unit/test_events.py`) refuses a JSON parse of raw lines anywhere else, so
the parser, and how undecodable bytes are treated, can change in exactly one place. Pure: it
reads iterables it is given and never opens a file or a socket.

Undecodable bytes are replaced (U+FFFD), never fatal: whether such a line is acceptable is the
contract's decision, not the reader's.

Speed without a second behaviour: orjson parses first, and the standard library decides every
line orjson would treat differently — anything orjson refuses (invalid UTF-8, lone surrogates,
NaN, overflowing floats) and any line with a 19-digit run, since orjson turns integers outside
the 64-bit range into floats where `json` keeps them exact (M0-S5, prediction P2).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator
from typing import Any

import orjson

#: The wide-integer guard runs on every line, so it must cost far less than the parse it guards:
#: a regex scan cost more than json.loads itself (M0-S5 bench). Instead, map every digit to "0"
#: and everything else to "." in C, then look for nineteen zeros in a row.
_DIGITS_AS_ZERO = bytes(0x30 if 0x30 <= b <= 0x39 else 0x2E for b in range(256))
_NINETEEN_DIGITS = b"0" * 19
_WIDE_INT_TEXT = re.compile(r"[0-9]{19}")  # str lines are rare (tests, callers with text)


def _has_wide_int(raw: bytes | str) -> bool:
    """A run of 19+ digits: possibly an integer orjson would read as a float."""
    if isinstance(raw, bytes):
        return _NINETEEN_DIGITS in raw.translate(_DIGITS_AS_ZERO)
    return _WIDE_INT_TEXT.search(raw) is not None


def text(raw: bytes | str) -> str:
    """The line as text, for humans and the rejected-row sidecar (trailing newline removed)."""
    decoded = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    return decoded.rstrip("\n")


def _stdlib(raw: bytes | str) -> Any:
    return json.loads(raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw)


def parse(raw: bytes | str) -> Any:
    """One line as a JSON value, exactly as `json.loads` reads it. Raises ValueError if not JSON."""
    if _has_wide_int(raw):
        return _stdlib(raw)
    try:
        return orjson.loads(raw)
    except orjson.JSONDecodeError:
        return _stdlib(raw)


def events(lines: Iterable[bytes | str]) -> Iterator[Any]:
    """Every line's JSON value, in order. Raises ValueError on the first line that is not JSON."""
    for line in lines:
        yield parse(line)


def sized(lines: Iterable[bytes | str]) -> Iterator[tuple[int, Any]]:
    """(raw length, JSON value) per line: for measurements that weigh lines as well as count them."""
    for line in lines:
        yield len(line), parse(line)
