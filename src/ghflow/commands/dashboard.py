"""Build, check and publish the stakeholder dashboard (M5-S2, ADR-0013).

    python -m ghflow.commands.dashboard build  --out DIR          # fill templates, quarto render
    python -m ghflow.commands.dashboard check  --site DIR/_site   # every number read back
    python -m ghflow.commands.dashboard redteam --site DIR/_site  # three plants, each must fail
    python -m ghflow.commands.dashboard render --not-before ISO   # the DAG: build, check, publish

Marts come from the snapshots in s3://marts/latest (never the warehouse). `render` refuses when
those snapshots are older than the build that triggered it, and publishes only a site that
passed `check`: into a new release folder, then an atomic switch of the `current` link nginx
serves, plus a zip that opens without the stack.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ghflow import dashboard, figures
from ghflow.commands.local import REPO_ROOT, s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.storage import Store

TEMPLATES = REPO_ROOT / "dashboard"
SITE_ROOT = Path(os.environ.get("GHFLOW_DASHBOARD_SITE", "/opt/ghflow/dashboard-site"))
MARTS = ["mart_daily_activity", "mart_event_mix", "mart_hour_of_week", "mart_rising_repos",
         "mart_bot_share", "mart_top_bots", "mart_trust"]  # fmt: skip


def load_marts() -> tuple[Any, dict[str, Any]]:
    """The latest snapshots, as DuckDB tables (for the anchored queries) and rows (for charts)."""
    import duckdb

    store, con, rows = Store(s3_config()), duckdb.connect(), {}
    for name in MARTS:
        table = pq.read_table(io.BytesIO(store.get(f"marts/latest/{name}.parquet")))
        con.register(name, table)
        rows[name] = table.to_pylist()
    manifest = json.loads(store.get("marts/latest/manifest.json"))
    return con, {"rows": rows, "manifest": manifest}


def scalar(con: Any, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


def numbers(con: Any) -> dict[str, str]:
    return {
        q: dashboard.fmt(spec.fmt, scalar(con, spec.sql)) for q, spec in dashboard.NUMBERS.items()
    }


def build(out: Path) -> Path:
    con, marts = load_marts()
    shutil.rmtree(out, ignore_errors=True)
    shutil.copytree(TEMPLATES, out, ignore=shutil.ignore_patterns("_site", ".quarto"))
    import plotly  # type: ignore[import-untyped]

    shutil.copy(Path(plotly.__file__).parent / "package_data" / "plotly.min.js", out / "assets")
    nums = numbers(con)
    names = {q: str(scalar(con, sql)) for q, (_, sql) in dashboard.ENTITIES.items()}
    figs = {k: figures.embed(k, f) for k, f in figures.build(marts["rows"]).items()}
    for page in dashboard.PAGES:
        path = out / f"{page}.qmd"
        path.write_text(
            dashboard.fill(path.read_text("utf-8"), nums, names, figs), encoding="utf-8"
        )
    done = subprocess.run(["quarto", "render", str(out)], capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"quarto render failed:\n{done.stderr[-3000:]}")
    (out / "_site" / "built.json").write_text(
        json.dumps({"marts_built_at": marts["manifest"]["built_at"]})
    )
    return out / "_site"


# ------------------------------------------------------------------ reading a page back


class Page(HTMLParser):
    """Visible text outside anchored spans, the anchored spans, and the embedded figures."""

    def __init__(self) -> None:
        super().__init__()
        self.free: list[str] = []
        self.spans: list[dict[str, str]] = []
        self.figs: dict[str, Any] = {}
        self._skip = 0
        self._span: dict[str, str] | None = None
        self._fig: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k: v or "" for k, v in attrs}
        if tag in ("script", "style", "noscript"):
            self._skip += 1
            self._fig = a.get("data-fig") or None
        elif tag == "span" and a.get("class") in ("num", "entity"):
            self._span = {**a, "text": ""}

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "noscript"):
            self._skip, self._fig = max(0, self._skip - 1), None
        elif tag == "span" and self._span is not None:
            self.spans.append(self._span)
            self._span = None

    def handle_data(self, data: str) -> None:
        if self._fig:
            self.figs[self._fig] = json.loads(data)
        elif self._skip:
            return
        elif self._span is not None:
            self._span["text"] += data
        else:
            self.free.append(data)


def read_pages(site: Path) -> dict[str, Page]:
    pages = {}
    for name in dashboard.PAGES:
        page = Page()
        page.feed((site / f"{name}.html").read_text("utf-8"))
        pages[name] = page
    return pages


@dataclass
class Truth:
    """What the marts say: the numbers as they must read, and who may be named."""

    numbers: dict[str, str]
    repos: set[str]
    bots: set[str]
    flagged: set[str]


def span_problems(name: str, span: dict[str, str], truth: Truth) -> list[str]:
    text, q = span["text"], span.get("data-q", "")
    if span["class"] == "num":
        want = truth.numbers.get(q)
        return [] if text == want else [f"{name}: {q} reads {text!r}, the marts say {want!r}"]
    kind = span.get("data-kind")
    ok = (kind == "repo" and text in truth.repos) or (
        kind == "bot" and text in truth.bots and text.endswith("[bot]")
    )
    return (
        []
        if ok
        else [f"{name}: names {text!r} as {kind!r}: only ranked repos and bots may be named"]
    )


def fig_problems(name: str, fig_id: str, fig: dict[str, Any], truth: Truth) -> list[str]:
    out = []
    if not any(len(t.get("x") or t.get("z") or []) for t in fig["data"]):
        out.append(f"{name}: chart {fig_id} is empty")
    shades = {s.get("name") for s in fig["layout"].get("shapes", [])}
    if fig_id in figures.SHADED and shades != truth.flagged:
        out.append(
            f"{name}: chart {fig_id} shades {len(shades)} days, the marts flag {len(truth.flagged)}"
        )
    labels = {y for t in fig["data"] for y in (t.get("y") or []) if isinstance(y, str)}
    allowed = {"rising": truth.repos, "top_bots": truth.bots}.get(fig_id)
    if allowed is not None and not labels <= allowed:
        out.append(f"{name}: chart {fig_id} labels something other than the ranked repos or bots")
    return out


def check(site: Path) -> dict[str, Any]:
    con, marts = load_marts()
    rows = marts["rows"]
    truth = Truth(numbers(con), {r["repo_name"] for r in rows["mart_rising_repos"]},
                  {r["bot"] for r in rows["mart_top_bots"]},
                  {f"flagged:{d.isoformat()}" for d in figures.flagged_days(rows["mart_trust"])})  # fmt: skip
    pages, problems = read_pages(site), []
    for name, page in pages.items():
        problems += [
            f"{name}: unanchored number {m!r}"
            for m in re.findall(r"\d[\d,.]*", " ".join(page.free))
        ]
        for span in page.spans:
            problems += span_problems(name, span, truth)
        for fig_id, fig in page.figs.items():
            problems += fig_problems(name, fig_id, fig, truth)
    return {"pages": len(pages), "numbers_and_names": sum(len(p.spans) for p in pages.values()),
            "charts": sum(len(p.figs) for p in pages.values()), "problems": problems, "passed": not problems}  # fmt: skip


# ------------------------------------------------------------------ red team, publish, render


def plant(site: Path, kind: str, into: Path) -> Path:
    """A copy of the site with one fault planted: a changed number, a dropped shading, a person."""
    shutil.rmtree(into, ignore_errors=True)
    shutil.copytree(site, into)
    page = into / {"number": "index.html", "shading": "trends.html", "person": "rising.html"}[kind]
    text = page.read_text("utf-8")
    if kind == "number":
        text = re.sub(
            r'(data-q="typical_day">)([\d.]+)',
            lambda m: m.group(1) + f"{float(m.group(2)) + 0.01:.2f}",
            text,
            count=1,
        )
    elif kind == "shading":
        # one shade (a dict with one nested dict, its "line") removed from the trends chart
        shade = r'\{"type":"rect"(?:[^{}]|\{[^{}]*\})*?"name":"flagged:[^"]*"\},?'
        text = re.sub(shade, "", text, count=1)
    else:
        text = text.replace(
            "</main>",
            '<p><span class="entity" data-kind="person" data-q="planted">octocat</span></p></main>',
            1,
        )
    page.write_text(text, encoding="utf-8")
    return into


def redteam(site: Path) -> dict[str, Any]:
    out = {}
    for kind in ("number", "shading", "person"):
        result = check(plant(site, kind, site.parent / f"_planted_{kind}"))
        out[kind] = {"caught": not result["passed"], "named": result["problems"][:2]}
        shutil.rmtree(site.parent / f"_planted_{kind}", ignore_errors=True)
    return {"plants": out, "all_caught": all(v["caught"] for v in out.values())}


def publish(site: Path) -> dict[str, Any]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    release = SITE_ROOT / "releases" / stamp
    shutil.copytree(site, release)
    bundle = io.BytesIO()
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(site.rglob("*")):
            if f.is_file():
                z.write(f, f"ghflow-dashboard/{f.relative_to(site).as_posix()}")
    (release / "ghflow-dashboard.zip").write_bytes(bundle.getvalue())
    Store(s3_config()).put(f"marts/dashboard/ghflow-dashboard-{stamp}.zip", bundle.getvalue())
    link = SITE_ROOT / "current.next"
    link.unlink(missing_ok=True)
    link.symlink_to(Path("releases") / stamp)
    link.replace(SITE_ROOT / "current")  # atomic: nginx serves the old or the new site, never half
    for old in sorted((SITE_ROOT / "releases").iterdir())[:-3]:
        shutil.rmtree(old, ignore_errors=True)
    return {"release": stamp, "bundle_bytes": len(bundle.getvalue())}


def render(not_before: str | None) -> dict[str, Any]:
    _, marts = load_marts()
    built = datetime.strptime(marts["manifest"]["built_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    if not_before and built < datetime.fromisoformat(not_before):
        return {"refused": f"marts built {built.isoformat()} are older than the build {not_before}"}
    site = build(Path("/tmp/ghflow-dashboard-build"))
    result = check(site)
    if not result["passed"]:
        return {"refused": "check failed; nothing published", **result}
    return {**result, **publish(site)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["build", "check", "redteam", "render"])
    parser.add_argument("--out", type=Path, default=Path("/tmp/ghflow-dashboard-build"))
    parser.add_argument("--site", type=Path)
    parser.add_argument("--not-before", default=None)
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "build":
            print(json.dumps({"site": str(build(args.out))}))
            return ExitCode.PASSED
        site = args.site or args.out / "_site"
        record = {"check": lambda: check(site), "redteam": lambda: redteam(site),
                  "render": lambda: render(args.not_before)}[args.command]()  # fmt: skip
        print(json.dumps(record, indent=2, default=str))
        ok = record.get("passed", record.get("all_caught", False)) and "refused" not in record
        return ExitCode.PASSED if ok else ExitCode.REFUSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())
