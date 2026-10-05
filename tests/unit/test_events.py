"""The one reader reads every line exactly as `json.loads` would.

The tricky lines below are where a faster parser (orjson, tried and reverted in M0-S5) disagrees
with the standard library; any future parser swap must keep every one of them equal.
"""

from __future__ import annotations

import json
import math
from typing import Any

import pytest

from ghflow.events import events, parse, sized, text

#: Lines where orjson, left alone, disagrees with the standard library or refuses.
TRICKY: list[bytes] = [
    b'{"id": 18446744073709551616}',  # above u64: orjson gives a float
    b'{"id": -9223372036854775809}',  # below i64: same
    b'{"n": 123456789012345678901234567890}',
    b'{"x": 1e400}',  # float overflow: orjson refuses, json gives inf
    b'{"x": NaN}',
    rb'{"s": "\ud800"}',  # lone surrogate escape
    b'{"s": "\xff\xfe"}',  # invalid UTF-8: replaced, never fatal
    b'{"a": 1, "a": 2}',  # duplicate key: last wins in both
    rb'{"id": 18446744073709551615, "f": 0.1, "z": "a\u0000b"}',  # in range: orjson's fast path
    b"[1, 2]",
    b'"just a string"\n',
]


def same(a: Any, b: Any) -> bool:
    """Equal, with NaN equal to itself and float never equal to an int of the same value."""
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a):
        return math.isnan(b)
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b, strict=True))
    return bool(a == b)


@pytest.mark.parametrize("line", TRICKY, ids=range(len(TRICKY)))
def test_parse_reads_exactly_what_json_reads(line: bytes) -> None:
    expected = json.loads(line.decode("utf-8", errors="replace"))
    assert same(parse(line), expected)
    assert same(parse(line.decode("utf-8", errors="replace")), expected)


@pytest.mark.parametrize("line", [b"", b"{", b"not json", b'{"a": }'])
def test_a_line_that_is_not_json_raises_value_error(line: bytes) -> None:
    with pytest.raises(ValueError):
        parse(line)


def test_events_and_sized_read_in_order() -> None:
    lines = [b'{"a": 1}\n', b'{"a": 2}\n']
    assert list(events(lines)) == [{"a": 1}, {"a": 2}]
    assert list(sized(lines)) == [(9, {"a": 1}), (9, {"a": 2})]


def test_text_replaces_undecodable_bytes_and_drops_the_newline() -> None:
    assert text(b'{"s": "\xff"}\n') == '{"s": "�"}'
