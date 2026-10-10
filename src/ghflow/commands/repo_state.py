"""The repo-state processor and its proofs (M3-S2, ADR-0009).

    python -m ghflow.commands.repo_state run          # the service: restore, then consume
    python -m ghflow.commands.repo_state schema       # write config/schemas/repo_state.avsc (--check)
    python -m ghflow.commands.repo_state progress     # the last hour closed on every partition
    python -m ghflow.commands.repo_state diff         # topic's latest per repo vs dim_repo current
    python -m ghflow.commands.repo_state diff --plant # red team: one stale record, in a copy
    python -m ghflow.commands.repo_state audit        # spurious changes, aborted records

`diff` compares only what both witnesses have seen: the warehouse up to its newest staged hour,
the topic up to the processor's progress (it refuses while that is behind). A topic record newer
than the warehouse is `ahead`, counted, never compared.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import signal
import sys
import threading
from collections.abc import Callable
from datetime import UTC
from pathlib import Path
from typing import Any

import pyarrow as pa
from confluent_kafka import TopicPartition
from fastavro import parse_schema, schemaless_reader

from ghflow import settings
from ghflow.commands.local import REPO_ROOT
from ghflow.commands.stream import register
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour
from ghflow.stream import clients, repo_state, schema

AVSC = REPO_ROOT / "config" / "schemas" / "repo_state.avsc"
STORE = Path(os.environ.get("GHFLOW_REPO_STATE_DB", "/tmp/ghflow-repo-state.sqlite"))
WAREHOUSE = os.environ.get("GHFLOW_WAREHOUSE", "/opt/ghflow/warehouse/ghflow.duckdb")
SCRATCH = os.environ.get("GHFLOW_REPO_STATE_SCRATCH", "/tmp/ghflow-repo-state-diff.duckdb")


def run() -> int:
    """Until SIGTERM/SIGINT; then close the consumer so the group knows at once (gotcha #51)."""
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())
    producer = clients.transactional_producer()
    producer.init_transactions(60)  # fences an older instance; its open transaction is aborted
    store, opened = repo_state.open_store(STORE)
    print(json.dumps({**opened, "repos": store.count()}), flush=True)
    serialize = repo_state.Serializer(
        register(clients.registry(), repo_state.SUBJECT, repo_state.SCHEMA)
    )
    events = parse_schema(schema.schema())
    consumer = clients.repo_state_consumer()
    consumer.subscribe([repo_state.TOPIC])
    processor = repo_state.Processor(consumer, producer, store, serialize)
    try:
        while not stopping.is_set():
            msg = consumer.poll(1.0)
            value = None if msg is None or msg.error() else msg.value()
            if msg is None or value is None:
                continue
            record = schemaless_reader(io_bytes(value), events, events)
            closed = processor.take(msg.partition() or 0, msg.offset() or 0, record)  # type: ignore[arg-type]
            if closed:
                print(json.dumps(closed), flush=True)
    finally:
        consumer.close()  # an open transaction is aborted by the next start's init_transactions
    return ExitCode.PASSED


def io_bytes(value: bytes) -> io.BytesIO:
    """The Avro body of a Confluent wire-format value (after magic byte and schema id)."""
    return io.BytesIO(value[5:])


def progress() -> dict[str, Any]:
    """Per partition, the hour of the marker the group last committed past; the minimum is how
    far the processor has certainly got."""
    consumer = clients.peek_consumer(clients.REPO_STATE_GROUP)
    parts = [TopicPartition(repo_state.TOPIC, p) for p in range(settings.STREAM_PARTITIONS)]
    committed = consumer.committed(parts, timeout=30)
    consumer.close()
    reader, events = clients.changelog_reader(), parse_schema(schema.schema())
    hours: dict[int, str | None] = {}
    for tp in committed:
        if tp.offset <= 0:
            hours[tp.partition] = None
            continue
        reader.assign([TopicPartition(repo_state.TOPIC, tp.partition, tp.offset - 1)])
        msg = reader.consume(1, timeout=30)[0]
        marker: Any = schemaless_reader(io_bytes(msg.value() or b""), events, events)
        hours[tp.partition] = marker["hour"]
    reader.close()
    known = [Hour.parse(h) for h in hours.values() if h]
    done = min(known).key if len(known) == len(hours) else None
    return {"partitions": hours, "closed_through": done}


# ------------------------------------------------------------------ the topic, into DuckDB


def load_topic(con: Any, committed_only: bool = True) -> int:
    """Every readable changelog record into table `topic` (repo_id, repo_name, as_of, off)."""
    con.execute(
        "create or replace table topic (repo_id bigint, repo_name varchar, as_of timestamptz, off bigint)"
    )
    reader, cols, read = clients.changelog_reader(committed_only), _empty(), 0
    for msg in repo_state.read_to_end(reader, repo_state.STATE_TOPIC):
        r = repo_state.decode(msg.value())
        for name, value in (("repo_id", r["repo_id"]), ("repo_name", r["repo_name"]),
                            ("as_of", r["as_of"]), ("off", msg.offset())):  # fmt: skip
            cols[name].append(value)
        read += 1
        if len(cols["off"]) >= 200_000:
            _flush(con, cols)
            cols = _empty()
    _flush(con, cols)
    reader.close()
    return read


def _empty() -> dict[str, list[Any]]:
    return {"repo_id": [], "repo_name": [], "as_of": [], "off": []}


def _flush(con: Any, cols: dict[str, list[Any]]) -> None:
    batch = pa.table(cols)  # noqa: F841 - read by DuckDB through its name
    con.execute("insert into topic select * from batch")


def plant_stale(con: Any, cutoff: Any) -> dict[str, Any]:
    """Red team, in the scratch copy only: the newest record of one renamed repo is replaced by
    its previous name, as a stale processor would leave it."""
    repo, old = con.execute(
        "select d.repo_id, d.repo_name from ghflow.main_gold.dim_repo d "
        "join (select repo_id, max(valid_from) f from ghflow.main_gold.dim_repo where not is_current group by 1) p "
        "on d.repo_id = p.repo_id and d.valid_from = p.f "
        "where d.repo_id in (select repo_id from topic group by 1 having arg_max(as_of, off) < ?) "
        "order by d.repo_id limit 1",
        (cutoff,),
    ).fetchone()
    con.execute(
        "insert into topic select repo_id, ?, as_of, off + 1 from topic where repo_id = ? "
        "order by off desc limit 1",
        (old, repo),
    )
    return {"repo_id": repo, "planted_name": "the repo's previous name (from dim_repo)"}


# Strings stay out of aggregate states (gotcha #52): the latest record per repo is found by its
# max offset and joined back; repo ids are listed only for red verdicts, and only a few.
DIFF_SQL = """
create or replace table verdicts as
with latest as (select repo_id, max(off) as off from topic group by repo_id),
     t as (select repo_id, repo_name as name, as_of from topic join latest using (repo_id, off)),
     d as (select repo_id, repo_name from ghflow.main_gold.dim_repo where is_current)
select coalesce(t.repo_id, d.repo_id) as repo_id,
       case when t.as_of >= $cutoff then 'ahead'
            when t.repo_id is null then 'missing_in_topic'
            when d.repo_id is null then 'extra_in_topic'
            when t.name = d.repo_name then 'equal' else 'stale' end as verdict
from t full join d on t.repo_id = d.repo_id
"""
COUNTS_SQL = "select verdict, count(*) from verdicts group by verdict"
NAMED_SQL = """
select verdict, repo_id from verdicts where verdict not in ('equal', 'ahead')
qualify row_number() over (partition by verdict order by repo_id) <= $named order by verdict, repo_id
"""


def diff(plant: bool, load: Callable[[Any], int] | None = None) -> dict[str, Any]:
    """`load` fills table `topic` with the state to compare: by default the changelog itself;
    `ghflow.commands.clickhouse` passes ClickHouse's repo_current (M4-S1)."""
    import duckdb  # the dbt group's dependency; present wherever the warehouse is

    where = progress()
    Path(SCRATCH).unlink(missing_ok=True)
    with duckdb.connect(
        SCRATCH, config={"memory_limit": "1GB", "preserve_insertion_order": False}
    ) as con:
        con.execute(f"attach '{WAREHOUSE}' as ghflow (read_only)")
        staged = con.execute(
            "select max(created_at) from ghflow.main_staging.stg_gharchive__events"
        ).fetchone()[0]  # type: ignore[index]
        staged = staged.astimezone(UTC)
        cutoff = Hour.of(staged.date(), staged.hour).plus(1).start  # the warehouse's end
        behind = (
            where["closed_through"] is None
            or Hour.parse(where["closed_through"]).plus(1).start < cutoff
        )
        if behind:
            return {"verdict": "pending", "progress": where, "warehouse_through": str(staged)}
        read = (load or load_topic)(con)
        planted = plant_stale(con, cutoff) if plant else None
        con.execute(DIFF_SQL, {"cutoff": cutoff})
        counts: dict[str, Any] = {
            v: {"repos": n, "named": []} for v, n in con.execute(COUNTS_SQL).fetchall()
        }
        for verdict, repo in con.execute(
            NAMED_SQL, {"named": settings.RECONCILE_NAMED_IDS}
        ).fetchall():
            counts[verdict]["named"].append(repo)
    Path(SCRATCH).unlink(missing_ok=True)
    red = {k: v for k, v in counts.items() if k not in ("equal", "ahead")}
    return {"verdict": "red" if red else "green", "records_read": read, "warehouse_until": str(cutoff),
            "progress": where, "counts": counts, "planted": planted}  # fmt: skip


def audit() -> dict[str, Any]:
    """Spurious change records (a repo's record repeating its previous name), non-increasing
    as_of per repo, and records only an uncommitted reader sees (aborted transactions)."""
    import duckdb

    Path(SCRATCH).unlink(missing_ok=True)
    with duckdb.connect(SCRATCH, config={"memory_limit": "1GB"}) as con:  # a file: it can spill
        committed = load_topic(con)
        spurious, backwards = con.execute(
            "select count(*) filter (where repo_name = prev_name), count(*) filter (where as_of <= prev_as_of) "
            "from (select *, lag(repo_name) over w as prev_name, lag(as_of) over w as prev_as_of "
            "from topic window w as (partition by repo_id order by off))"
        ).fetchone()  # type: ignore[misc]
        everything = load_topic(con, committed_only=False)
    Path(SCRATCH).unlink(missing_ok=True)
    return {"committed_records": committed, "spurious_changes": spurious, "as_of_not_increasing": backwards,
            "aborted_records_visible_uncommitted": everything - committed}  # fmt: skip


def schema_file(check_only: bool) -> int:
    if check_only:
        same = AVSC.read_text(encoding="utf-8") == repo_state.schema_json()
        return ExitCode.PASSED if same else ExitCode.REFUSED
    AVSC.write_bytes(repo_state.schema_json().encode())
    return ExitCode.PASSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["run", "schema", "progress", "diff", "audit"])
    parser.add_argument("--check", action="store_true", help="schema: refuse if the file drifted")
    parser.add_argument("--plant", action="store_true", help="diff: plant one stale record")
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "run":
            return run()
        if args.command == "schema":
            return schema_file(args.check)
        record = {"progress": progress, "audit": audit}.get(
            args.command, lambda: diff(args.plant)
        )()
        print(json.dumps(record, indent=2, default=str))
        if args.command == "diff":
            ok = record["verdict"] == "green" or (args.plant and _caught(record))
            return ExitCode.PASSED if ok else ExitCode.REFUSED
        return ExitCode.PASSED

    return guarded(go)


def _caught(record: dict[str, Any]) -> bool:
    """The plant is caught when exactly the planted repo, and nothing else, is red."""
    stale = record["counts"].get("stale", {})
    others = [k for k in record["counts"] if k not in ("equal", "ahead", "stale")]
    return stale.get("named") == [record["planted"]["repo_id"]] and not others


if __name__ == "__main__":
    sys.exit(main())
