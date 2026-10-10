"""Render the README's "How it got better" table from the Refine stories' records (M5-S4).

    uv run python scripts/readme_table.py          # rewrite the table between its markers
    uv run python scripts/readme_table.py --check  # exit 1 if the README's table differs

Every number comes from a JSON record under automation/runs/; nothing is typed by hand. Changes
that missed their committed prediction are listed too: they are part of how it got better.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
START, END = "<!-- how-it-got-better:start -->", "<!-- how-it-got-better:end -->"


@dataclass(frozen=True)
class Row:
    story: str
    change: str
    record: str  # under automation/runs/
    before: tuple[str | int, ...]  # path into the record
    after: tuple[str | int, ...]
    unit: str  # s | ms | min | GB | MB
    outcome: str  # kept | reverted (missed its prediction) | ...


ROWS = [
    Row("M0-S5", "contract parse, one typical hour", "m0-s5/bench-verdict.json",
        ("pooled", "2026-10-02-15", "before_median_s"), ("pooled", "2026-10-02-15", "after_median_s"), "s",
        "reverted: 21% faster, predicted 28%"),
    Row("M1-S5", "ingest one day (orjson parser)", "m1-s5/verdict-parser.json",
        ("interleaved_ab", "stdlib_median_s"), ("interleaved_ab", "orjson_median_s"), "s", "reverted: missed its bar"),
    Row("M2-S4", "ingest one day (timestamp cast)", "m2-s4/verdict-contract.json",
        ("C1", "before_median_s"), ("C1", "after_median_s"), "s", "kept"),
    Row("M2-S4", "warehouse after a full refresh (compaction)", "m2-s4/verdict-warehouse.json",
        ("W1", "after_full_refresh_bytes"), ("W1", "after_compact_bytes"), "GB", "kept"),
    Row("M3-S3", "SCD2 build (incremental)", "m3-s3/scd2.json",
        ("before", "median_s"), ("after", "median_s"), "s", "reverted: 43.5% saved, predicted 50%"),
    Row("M3-S3", "repo-state restart (persisted store)", "m3-s3/restore.json",
        ("before", "median_s"), ("after", "median_s"), "s", "kept"),
    Row("M3-S3", "lander, one hour (no per-row copy)", "m3-s3/verdict-l1.json",
        ("lander_s", "before", "median"), ("lander_s", "after", "median"), "s", "kept"),
    Row("M4-S3", "Grafana events-per-hour panel", "m4-s3/panel.json",
        ("before", "median_ms"), ("after", "median_ms"), "ms", "kept"),
    Row("M4-S3", "repo-state restart (short timeout)", "m4-s3/restore-tail.json",
        ("before", "median_s"), ("after", "median_s"), "s", "reverted: 1.52 s, predicted 1.0 s"),
    Row("M5-S3", "a day's last hour to the dashboard, judged", "m5-s3/latency.json",
        ("before", "minutes"), ("after", "minutes"), "min", "kept"),
    Row("M5-S3", "dashboard render", "m5-s3/dashboard.json",
        ("before", "median_s"), ("after", "median_s"), "s", "kept"),
    Row("M5-S3", "dashboard bundle (zip)", "m5-s3/dashboard.json",
        ("before", "zip_bytes", 0), ("after", "zip_bytes", 0), "MB", "kept"),
    Row("M5-S3", "repo-state restart (skip when caught up)", "m5-s3/restore-tail.json",
        ("before", "median_s"), ("after", "median_s"), "s", "kept"),
]  # fmt: skip


def value(record: str, path: tuple[str | int, ...]) -> Any:
    node: Any = json.loads((ROOT / "automation" / "runs" / record).read_text(encoding="utf-8"))
    for key in path:
        node = node[key]
    return node


def show(v: float, unit: str) -> str:
    if unit == "GB":
        return f"{v / 1e9:.1f} GB"
    if unit == "MB":
        return f"{v / 1e6:.1f} MB"
    if unit == "min":
        return f"{v / 60:.1f} h" if v >= 120 else f"{v:.0f} min"
    return f"{v:,.2f} {unit}" if v < 100 else f"{v:,.0f} {unit}"


def table() -> str:
    lines = ["| Story | What was measured | Before | After | Outcome | Record |",
             "|---|---|---|---|---|---|"]  # fmt: skip
    for r in ROWS:
        b, a = float(value(r.record, r.before)), float(value(r.record, r.after))
        lines.append(f"| {r.story} | {r.change} | {show(b, r.unit)} | {show(a, r.unit)} | {r.outcome} | "
                     f"`automation/runs/{r.record}` |")  # fmt: skip
    return "\n".join(lines)


def block() -> str:
    return f"{START}\n{table()}\n{END}"


def current() -> str:
    text = README.read_text(encoding="utf-8")
    return text[text.index(START) : text.index(END) + len(END)]


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        return 0 if current() == block() else 1
    text = README.read_text(encoding="utf-8")
    README.write_bytes(text.replace(current(), block()).encode("utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
