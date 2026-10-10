"""The stakeholder dashboard's numbers and names, anchored to the marts (M5-S2, ADR-0013). Pure.

The prose lives in `dashboard/*.qmd` templates written for people; this module fills their slots:

- `{{n:<id>}}`: a number. It renders as `<span class="num" data-q="<id>">text</span>`, where the
  text is NUMBERS[id]'s query over the mart snapshots, formatted. `dashboard_check` re-runs the
  query, re-formats it and must read back the same text; a digit outside such a span fails.
- `{{e:<id>}}`: a named thing, a repo or a bot only (people are never named); it renders as
  `<span class="entity" data-kind="repo|bot" data-q="<id>">name</span>`.
- `{{fig:<id>}}`: a chart (`ghflow.figures`).

Queries run in DuckDB over tables named after the marts (`mart_daily_activity`, ...).
"""

from __future__ import annotations

import html
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

PAGES = ["index", "trends", "when", "rising", "bots", "trust"]


@dataclass(frozen=True)
class Spec:
    sql: str
    fmt: str  # millions | thousands | int | pct0 | pct1 | date | label | plus_pct


NUMBERS: dict[str, Spec] = {
    "window_start": Spec("select min(event_date) from mart_trust", "date"),
    "window_end": Spec("select max(event_date) from mart_daily_activity", "date"),
    "complete_days": Spec("select count(*) from mart_daily_activity", "int"),
    "total_events": Spec("select sum(events) from mart_daily_activity", "millions"),
    "typical_day": Spec("select median(events_per_day_clean) from mart_daily_activity", "millions"),
    "typical_actors": Spec("select median(actors_clean_hours) from mart_daily_activity", "thousands"),
    "typical_repos": Spec("select median(repos_clean_hours) from mart_daily_activity", "thousands"),
    "push_share": Spec("select sum(events) filter (where type = 'PushEvent') / sum(events) from mart_event_mix", "pct0"),
    "second_type_share": Spec("select sum(events) / (select sum(events) from mart_event_mix) from mart_event_mix "
                              "group by type order by sum(events) desc limit 1 offset 1", "pct1"),
    "busiest_slot": Spec("select weekday_name || ' ' || lpad(hour_utc::varchar, 2, '0') || ':00 UTC' from mart_hour_of_week "
                         "order by avg_events desc, weekday, hour_utc limit 1", "label"),
    "busiest_avg": Spec("select max(avg_events) from mart_hour_of_week", "int"),
    "quietest_slot": Spec("select weekday_name || ' ' || lpad(hour_utc::varchar, 2, '0') || ':00 UTC' from mart_hour_of_week "
                          "order by avg_events, weekday, hour_utc limit 1", "label"),
    "quietest_avg": Spec("select min(avg_events) from mart_hour_of_week", "int"),
    "weekday_lift": Spec("select avg(avg_events) filter (where weekday <= 5) / avg(avg_events) filter (where weekday > 5) - 1 "
                         "from mart_hour_of_week", "plus_pct"),
    "rising_count": Spec("select count(*) from mart_rising_repos", "int"),
    "top_repo_stars": Spec("select stars_recent from mart_rising_repos where rank = 1", "int"),
    "rising_end": Spec("select max(window_end) from mart_rising_repos", "date"),
    "bot_share": Spec("select sum(bot_events) / sum(events) from mart_bot_share", "pct0"),
    "top_bot_events": Spec("select events from mart_top_bots where rank = 1", "millions"),
    "hours_ingested": Spec("select sum(hours_ingested) from mart_trust", "int"),
    "flagged_hours": Spec("select sum(flagged_hours) from mart_trust", "int"),
    "shifted_hours": Spec("select sum(shifted_hours) from mart_trust", "int"),
    "reconciled_share": Spec("select sum(reconciled_green) / sum(hours_ingested) from mart_trust", "pct1"),
}  # fmt: skip

ENTITIES: dict[str, tuple[str, str]] = {
    "top_repo": ("repo", "select repo_name from mart_rising_repos where rank = 1"),
    "top_bot": ("bot", "select bot from mart_top_bots where rank = 1"),
}

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
          "October", "November", "December"]  # fmt: skip


def fmt(kind: str, value: Any) -> str:
    """The one way a number is written on the dashboard; the checker uses it too."""
    if value is None:
        raise ValueError("a dashboard number cannot be empty")
    formats: dict[str, Callable[[Any], str]] = {
        "millions": lambda v: f"{float(v) / 1e6:.2f} million",
        "int": lambda v: f"{round(float(v)):,}",
        "thousands": lambda v: f"{round(float(v), -3):,.0f}",
        "pct0": lambda v: f"{float(v) * 100:.0f}%",
        "pct1": lambda v: f"{float(v) * 100:.1f}%",
        "plus_pct": lambda v: f"{abs(float(v)) * 100:.0f}% {'more' if float(v) >= 0 else 'less'}",
        "date": lambda v: (
            f"{v.day} {MONTHS[v.month - 1]} {v.year}" if isinstance(v, date) else str(v)
        ),
        "label": str,
    }
    return formats[kind](value)


def num_span(qid: str, text: str) -> str:
    return f'<span class="num" data-q="{qid}">{html.escape(text)}</span>'


def entity_span(qid: str, kind: str, name: str) -> str:
    return f'<span class="entity" data-kind="{kind}" data-q="{qid}">{html.escape(name)}</span>'


SLOT = re.compile(r"\{\{\s*(n|e|fig):([a-z_]+)\s*\}\}")


def fill(template: str, numbers: Mapping[str, str], entities: Mapping[str, str],
         figures: Mapping[str, str]) -> str:  # fmt: skip
    """Replace every slot; an unknown slot is an error, never left on the page."""

    def one(match: re.Match[str]) -> str:
        kind, qid = match.groups()
        if kind == "n":
            return num_span(qid, numbers[qid])
        if kind == "e":
            return entity_span(qid, ENTITIES[qid][0], entities[qid])
        return "\n```{=html}\n" + figures[qid] + "\n```\n"

    return SLOT.sub(one, template)
