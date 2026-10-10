"""The KPI register and the marts (M5-S1): one mart per KPI, and one definition of a broken hour."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from ghflow.commands import marts

ROOT = Path(__file__).resolve().parents[2]
MARTS_DIR = ROOT / "dbt" / "ghflow" / "models" / "marts"
KPIS = ROOT / "docs" / "KPIS.md"


def register() -> dict[str, str]:
    """KPI id -> mart, from the register's table."""
    rows = re.findall(r"^\| (K\d\d) \| [^|]+ \| `(mart_\w+)` \|", KPIS.read_text("utf-8"), re.M)
    return dict(rows)


def mart_kpis() -> dict[str, str]:
    """mart -> KPI id, from the models' meta."""
    models = yaml.safe_load((MARTS_DIR / "_marts.yml").read_text("utf-8"))["models"]
    return {m["name"]: m["meta"]["kpi"] for m in models}


def mart_files() -> set[str]:
    return {p.stem for p in MARTS_DIR.glob("mart_*.sql")}


def test_every_mart_has_a_kpi_and_every_kpi_a_mart() -> None:
    entries, metas = register(), mart_kpis()
    assert set(metas) == mart_files(), "every mart_*.sql is declared in _marts.yml, and only those"
    assert {kpi: mart for mart, kpi in metas.items()} == entries, (
        "docs/KPIS.md and meta.kpi disagree"
    )
    assert sorted(marts.MARTS) == sorted(mart_files()), "the export covers every mart"
    assert sorted(marts.HEADLINES) == sorted(entries), "every KPI has a headline"


def test_the_red_team_catches_a_mart_without_an_entry(tmp_path: Path) -> None:
    entries = register()
    entries.pop("K06")
    assert {kpi: mart for mart, kpi in mart_kpis().items()} != entries


def test_broken_hours_are_defined_once() -> None:
    """Every mart reads hour trust from the verdict model (directly or through
    int_hourly_events); none reads quality_verdicts itself or invents a threshold."""
    for path in MARTS_DIR.glob("*.sql"):
        sql = path.read_text("utf-8")
        assert "quality_verdicts" not in sql, f"{path.name} reads the verdicts around the model"
        if path.stem.startswith("mart_"):
            assert re.search(r"ref\('(stg_ops__hour_verdicts|int_hourly_events)'\)", sql), path.name
    verdicts = (
        ROOT / "dbt" / "ghflow" / "models" / "staging" / "stg_ops__hour_verdicts.sql"
    ).read_text("utf-8")
    assert "ops.public.quality_verdicts" in verdicts and "arg_max(status, judged_at)" in verdicts


def test_matching_is_exact_for_counts_and_tight_for_ratios() -> None:
    assert marts.same(903, 903) and not marts.same(903, 904)
    assert marts.same(0.8899801377711329, 0.8899801377711329 + 1e-13)
    assert not marts.same(1.0, 1.0048750761730652)
