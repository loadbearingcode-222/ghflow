"""docs/data/gharchive.md quotes only numbers the evidence record holds.

Two halves: the GENERATED block must equal what the record renders to right now, and the prose
outside it may not carry a number of its own. A hand-typed number is a number nobody re-derives.
"""

from __future__ import annotations

import json
import re

import pytest

from ghflow.profile import BEGIN, DOC, END, RECORD, render, splice

#: Things in prose that contain digits but are not measurements: inline code, story ids,
#: gotcha references, HTTP status codes, and ISO dates.
NOT_A_MEASUREMENT = re.compile(r"`[^`]*`|\bM\d+-S\d+\b|#\d+|HTTP \d{3}|\b\d{4}-\d{2}-\d{2}\b")


def doc() -> str:
    return DOC.read_text(encoding="utf-8")


def record() -> dict[str, object]:
    return dict(json.loads(RECORD.read_text(encoding="utf-8")))


def test_generated_block_equals_the_record_rendered_now() -> None:
    text = doc()
    assert splice(text, render(record())) == text, (
        "docs/data/gharchive.md is stale: run `.\\make.ps1 profile-render`"
    )


def prose(text: str) -> str:
    before, _, rest = text.partition(BEGIN)
    _, _, after = rest.partition(END)
    return NOT_A_MEASUREMENT.sub("", before + after)


def test_prose_outside_the_block_carries_no_numbers() -> None:
    numbers = re.findall(r"[^\n]*\d[^\n]*", prose(doc()))
    assert numbers == [], f"numbers outside the GENERATED block: {numbers}"


@pytest.mark.parametrize("planted", ["It weighs about 0.5 GB a day.", "We saw 111 broken hours."])
def test_a_planted_prose_number_is_caught(planted: str) -> None:
    """Red team for the rule above, on a copy: never edits the real doc."""
    tampered = doc().replace(
        "## What the pipeline must assume", f"{planted}\n\n## What the pipeline must assume"
    )
    assert re.findall(r"\d", prose(tampered))


def test_a_planted_record_change_makes_the_block_stale() -> None:
    """Red team for the block: one number moved in a copy of the record."""
    tampered = record()
    total = tampered["near_empty_total"]
    assert isinstance(total, int)
    tampered["near_empty_total"] = total + 1
    assert splice(doc(), render(tampered)) != doc()
