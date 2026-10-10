"""The dashboard's charts, as Plotly figure dicts built from mart rows (M5-S2, ADR-0013). Pure.

Each figure is embedded as JSON (`<script type="application/json" data-fig=...>`) and drawn by
`dashboard/assets/figs.js` with the vendored plotly.min.js, so a page works offline. Interactive
by default: hover, zoom, pan, and a click on a legend entry hides or shows that series.

Charts over days shade every day that has a flagged hour (shape names start `flagged:`):
`dashboard_check` requires a shade for each such day in `mart_trust`, and nothing else.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

Rows = list[dict[str, Any]]
SHADED = ("gist", "trends", "trust")
FIGURES = ("gist", "trends", "when", "rising", "bot_share", "top_bots", "trust")
FLAG_FILL = "rgba(214, 39, 40, 0.13)"
BASE_LAYOUT: dict[str, Any] = {"margin": {"l": 60, "r": 20, "t": 30, "b": 50}, "height": 420,
                               "hovermode": "x unified", "legend": {"orientation": "h", "y": -0.18},
                               "font": {"family": "Inter, Segoe UI, sans-serif"}}  # fmt: skip


def flagged_days(trust: Rows) -> list[date]:
    return sorted(r["event_date"] for r in trust if r["flagged_hours"])


def shades(days: list[date]) -> list[dict[str, Any]]:
    return [{"type": "rect", "xref": "x", "yref": "paper", "x0": d.isoformat(),
             "x1": (d + timedelta(days=1)).isoformat(), "y0": 0, "y1": 1, "layer": "below",
             "line": {"width": 0}, "fillcolor": FLAG_FILL, "name": f"flagged:{d.isoformat()}"}
            for d in days]  # fmt: skip


def gist(daily: Rows, trust: Rows) -> dict[str, Any]:
    xs = [r["event_date"].isoformat() for r in daily]
    trace = {"type": "scatter", "mode": "lines", "name": "events per day (broken hours left out)",
             "x": xs, "y": [r["events_per_day_clean"] for r in daily], "line": {"width": 3}}  # fmt: skip
    return {"data": [trace], "layout": {**BASE_LAYOUT, "shapes": shades(flagged_days(trust)),
                                        "yaxis": {"title": "events per day"}}}  # fmt: skip


def trends(mix: Rows, trust: Rows, top: int = 6) -> dict[str, Any]:
    totals: dict[str, int] = defaultdict(int)
    for r in mix:
        totals[r["type"]] += r["events"]
    keep = sorted(totals, key=lambda t: -totals[t])[:top]
    series: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in mix:
        series[r["type"] if r["type"] in keep else "Everything else"][
            r["event_date"].isoformat()
        ] += r["events"]
    days = sorted({r["event_date"].isoformat() for r in mix})
    data = [{"type": "scatter", "mode": "lines", "stackgroup": "one", "name": name.replace("Event", ""),
             "x": days, "y": [series[name][d] for d in days]} for name in [*keep, "Everything else"]]  # fmt: skip
    return {"data": data, "layout": {**BASE_LAYOUT, "shapes": shades(flagged_days(trust)),
                                     "yaxis": {"title": "events per day"}}}  # fmt: skip


def when(hours: Rows) -> dict[str, Any]:
    names = {r["weekday"]: r["weekday_name"] for r in hours}
    grid = {(r["weekday"], r["hour_utc"]): r["avg_events"] for r in hours}
    z = [[grid.get((d, h)) for h in range(24)] for d in sorted(names)]
    trace = {"type": "heatmap", "x": [f"{h:02d}:00" for h in range(24)], "y": [names[d] for d in sorted(names)],
             "z": z, "colorscale": "Blues", "reversescale": True, "hovertemplate": "%{y} %{x} UTC: %{z:,.0f} events/hour<extra></extra>"}  # fmt: skip
    return {"data": [trace], "layout": {**BASE_LAYOUT, "hovermode": "closest", "xaxis": {"title": "hour (UTC)"},
                                        "yaxis": {"autorange": "reversed"}}}  # fmt: skip


def rising(repos: Rows) -> dict[str, Any]:
    ordered = sorted(repos, key=lambda r: r["rank"], reverse=True)
    names = [r["repo_name"] for r in ordered]
    data = [{"type": "bar", "orientation": "h", "name": "the newest two weeks", "y": names,
             "x": [r["stars_recent"] for r in ordered]},
            {"type": "bar", "orientation": "h", "name": "the two weeks before", "y": names,
             "x": [r["stars_prior"] for r in ordered]}]  # fmt: skip
    return {"data": data, "layout": {**BASE_LAYOUT, "barmode": "group", "height": 760, "hovermode": "y unified",
                                     "margin": {**BASE_LAYOUT["margin"], "l": 260}, "xaxis": {"title": "new stars"}}}  # fmt: skip


def bot_share(share: Rows) -> dict[str, Any]:
    trace = {"type": "scatter", "mode": "lines", "name": "share of events made by bots",
             "x": [r["event_date"].isoformat() for r in share], "y": [r["bot_share"] for r in share]}  # fmt: skip
    return {
        "data": [trace],
        "layout": {**BASE_LAYOUT, "yaxis": {"tickformat": ".0%", "rangemode": "tozero"}},
    }


def top_bots(bots: Rows) -> dict[str, Any]:
    ordered = sorted(bots, key=lambda r: r["rank"], reverse=True)
    trace = {"type": "bar", "orientation": "h", "name": "events", "y": [r["bot"] for r in ordered],
             "x": [r["events"] for r in ordered]}  # fmt: skip
    return {"data": [trace], "layout": {**BASE_LAYOUT, "height": 620, "hovermode": "y unified",
                                        "margin": {**BASE_LAYOUT["margin"], "l": 260}}}  # fmt: skip


def trust(rows: Rows) -> dict[str, Any]:
    xs = [r["event_date"].isoformat() for r in rows]
    # Explicit colours: Plotly's default palette drew "flagged" in green, which reads as good.
    bars = (("clean hours", "clean_hours", "#1f77b4"), ("shifted (kept)", "shifted_hours", "#f2a900"),
            ("flagged (left out)", "flagged_hours", "#d62728"))  # fmt: skip
    data = [{"type": "bar", "name": label, "x": xs, "y": [r[col] for r in rows], "marker": {"color": colour}}
            for label, col, colour in bars]  # fmt: skip
    data.append(
        {
            "type": "scatter",
            "mode": "markers",
            "name": "hours reconciled green",
            "x": xs,
            "y": [r["reconciled_green"] for r in rows],
            "marker": {"color": "#1f2328", "size": 5},
        }
    )
    return {"data": data, "layout": {**BASE_LAYOUT, "barmode": "stack", "shapes": shades(flagged_days(rows)),
                                     "yaxis": {"title": "hours"}}}  # fmt: skip


def build(tables: dict[str, Rows]) -> dict[str, dict[str, Any]]:
    t = tables
    return {"gist": gist(t["mart_daily_activity"], t["mart_trust"]),
            "trends": trends(t["mart_event_mix"], t["mart_trust"]), "when": when(t["mart_hour_of_week"]),
            "rising": rising(t["mart_rising_repos"]), "bot_share": bot_share(t["mart_bot_share"]),
            "top_bots": top_bots(t["mart_top_bots"]), "trust": trust(t["mart_trust"])}  # fmt: skip


def embed(fig_id: str, figure: dict[str, Any]) -> str:
    """A div to draw into and the figure as JSON; `</` is escaped so it cannot end the script."""
    body = json.dumps(figure, separators=(",", ":"), default=str).replace("</", "<\\/")
    return (f'<div class="fig" id="fig-{fig_id}"></div>\n'
            f'<script type="application/json" data-fig="{fig_id}">{body}</script>')  # fmt: skip
