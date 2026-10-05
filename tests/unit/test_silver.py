"""The generated dbt silver layer: committed files match the generator, and the generator matches
the contract (fields.py) and bronze (bronze.SCHEMA), so an era is declared exactly once."""

from __future__ import annotations

import re

import yaml

from ghflow import bronze, silver
from ghflow.commands.silver import DBT_DIR, stale
from ghflow.fields import FIELDS, KNOWN_TYPES


def test_the_committed_dbt_files_are_exactly_what_the_generator_writes() -> None:
    assert stale() == [], "run: uv run python -m ghflow.commands.silver generate"


def test_one_contracted_view_per_known_type_plus_unknown() -> None:
    models = yaml.safe_load(silver.schema_yaml())["models"]
    names = {m["name"] for m in models}
    assert names == {silver.model_name(t) for t in KNOWN_TYPES} | {silver.UNKNOWN}
    assert all(m["config"]["contract"]["enforced"] for m in models)


def test_every_silver_column_exists_in_bronze_or_is_a_partition() -> None:
    for model in yaml.safe_load(silver.schema_yaml())["models"]:
        columns = {c["name"] for c in model["columns"]}
        assert columns - {"event_date", "event_hour"} <= set(bronze.SCHEMA.names), model["name"]


def test_every_field_with_an_era_has_a_test_named_after_field_and_era() -> None:
    text = silver.schema_yaml()
    for f in (f for f in FIELDS if f.from_ or f.until):
        for event_type in f.types:
            model = silver.model_name(event_type)
            names = re.findall(rf"name: (era__{model}__{f.name}__\S+)", text)
            assert names, f"{event_type}.{f.name} has an era but no era test"
            bound = f"until_{f.until:%Y_%m_%dT%H}" if f.until else f"from_{f.from_:%Y_%m_%dT%H}"
            assert bound in names[0]


def test_a_planted_drift_in_a_generated_file_is_caught(tmp_path: object) -> None:
    """Red team on a copy of the generated tree: one character edited is a stale file."""
    import shutil
    from pathlib import Path

    copy = Path(str(tmp_path)) / "ghflow"
    shutil.copytree(DBT_DIR / "models", copy / "models")
    shutil.copytree(DBT_DIR / "tests", copy / "tests")
    target = copy / "models" / "silver" / "silver_push_events.sql"
    target.write_text(
        target.read_text(encoding="utf-8").replace("push_size", "push_sizes"), encoding="utf-8"
    )
    assert stale(copy) == ["models/silver/silver_push_events.sql"]
