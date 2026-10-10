"""The stakeholder dashboard's pure parts (M5-S2): formats, slots, figures, the page reader."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from ghflow import dashboard, figures
from ghflow.commands.dashboard import Page, Truth, fig_problems, span_problems

TEMPLATES = Path(__file__).resolve().parents[2] / "dashboard"


def test_formats_are_the_ones_the_checker_reads_back() -> None:
    assert dashboard.fmt("millions", 2228544.5) == "2.23 million"
    assert dashboard.fmt("int", 141292.0) == "141,292"
    assert dashboard.fmt("thousands", 268_512) == "269,000"
    assert dashboard.fmt("pct0", 0.8899) == "89%"
    assert dashboard.fmt("pct1", 0.99939) == "99.9%"
    assert dashboard.fmt("plus_pct", 0.42) == "42% more"
    assert dashboard.fmt("plus_pct", -0.17) == "17% less", "the direction is data, never prose"
    assert dashboard.fmt("date", date(2026, 8, 3)) == "3 August 2026"
    with pytest.raises(ValueError):
        dashboard.fmt("int", None)


def test_every_template_slot_is_known_and_no_digit_sits_outside_a_slot() -> None:
    known = {
        "n": set(dashboard.NUMBERS),
        "e": set(dashboard.ENTITIES),
        "fig": set(figures.build.__code__.co_consts)
        | {"gist", "trends", "when", "rising", "bot_share", "top_bots", "trust"},
    }
    for page in dashboard.PAGES:
        text = (TEMPLATES / f"{page}.qmd").read_text("utf-8")
        for kind, qid in dashboard.SLOT.findall(text):
            assert qid in known[kind], f"{page}: unknown slot {kind}:{qid}"
        prose = dashboard.SLOT.sub("", text.split("---", 2)[2])
        assert not re.search(r"\d", prose), f"{page}: a number in the prose must be a slot"


def test_an_unknown_slot_is_an_error_never_left_on_the_page() -> None:
    with pytest.raises(KeyError):
        dashboard.fill("{{n:no_such_number}}", {}, {}, {})


def test_only_repos_and_bots_can_be_named() -> None:
    assert {kind for kind, _ in dashboard.ENTITIES.values()} == {"repo", "bot"}
    for spec in dashboard.NUMBERS.values():
        assert not re.search(r"actor_(login|id)", spec.sql), "no dashboard number reads a person"


TRUTH = Truth({"typical_day": "2.23 million"}, {"o/rising"}, {"ci[bot]"}, {"flagged:2026-08-05"})


def test_the_page_reader_finds_unanchored_digits_and_checks_spans() -> None:
    page = Page()
    page.feed('<main><p>About <span class="num" data-q="typical_day">2.23 million</span> a day, '
              'and 7 more.</p><span class="entity" data-kind="person" data-q="x">someone</span>'
              '<script>var x = 42;</script></main>')  # fmt: skip
    assert re.findall(r"\d[\d,.]*", " ".join(page.free)) == ["7"]
    assert span_problems("p", page.spans[0], TRUTH) == []
    assert "only ranked repos and bots" in span_problems("p", page.spans[1], TRUTH)[0]


def test_figures_shade_exactly_the_flagged_days_and_empty_charts_fail() -> None:
    trust = [{"event_date": date(2026, 8, 5), "flagged_hours": 2, "shifted_hours": 0, "clean_hours": 22,
              "reconciled_green": 24},
             {"event_date": date(2026, 8, 6), "flagged_hours": 0, "shifted_hours": 1, "clean_hours": 23,
              "reconciled_green": 24}]  # fmt: skip
    fig = figures.trust(trust)
    assert fig_problems("trust", "trust", fig, TRUTH) == []
    fig["layout"]["shapes"] = []
    assert "shades 0 days" in fig_problems("trust", "trust", fig, TRUTH)[0]
    empty = {"data": [{"type": "bar", "x": [], "y": []}], "layout": {}}
    assert "is empty" in fig_problems("bots", "bot_share", empty, TRUTH)[0]


def test_ranked_charts_label_only_ranked_repos_and_bots() -> None:
    fig = figures.top_bots([{"rank": 1, "bot": "someone", "events": 3}])
    assert "labels something other" in fig_problems("bots", "top_bots", fig, TRUTH)[0]
