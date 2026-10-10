"""The replayed stream's commands (M2-S1, ADR-0005).

python -m ghflow.commands.stream schema [--check]        # write / check config/schemas/gh_event.avsc
python -m ghflow.commands.stream compat --old FILE        # BACKWARD check against an older schema
python -m ghflow.commands.stream register                 # the schema, under BACKWARD, in the registry
python -m ghflow.commands.stream replay 2026-08-03-0      # one hour, checkpointed after the flush
python -m ghflow.commands.stream run [--from KEY]         # the service: hour after hour, in order
python -m ghflow.commands.stream verify 2026-08-03-0      # read the hour back: counts, duplicates, gaps
python -m ghflow.commands.stream redteam                  # a planted incompatible schema; a bad record
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from fastavro import parse_schema, schemaless_reader

from ghflow import audit, bronze, settings
from ghflow.commands.local import REPO_ROOT, ops_dsn, s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour
from ghflow.storage import Store
from ghflow.stream import replayer, schema

AVSC = REPO_ROOT / "config" / "schemas" / "gh_event.avsc"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:39092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:38081")


def register(registry: str) -> int:
    """Register the schema under BACKWARD compatibility; returns its id (idempotent)."""

    def call(method: str, path: str, body: dict[str, Any]) -> Any:
        request = urllib.request.Request(
            registry + path,
            json.dumps(body).encode(),
            {"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method=method,
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    call("PUT", f"/config/{schema.SUBJECT}", {"compatibility": "BACKWARD"})
    registered = call(
        "POST", f"/subjects/{schema.SUBJECT}/versions", {"schema": json.dumps(schema.schema())}
    )
    schema_id = registered.get("id") if isinstance(registered, dict) else None
    if not isinstance(schema_id, int):
        raise RuntimeError(f"the registry returned no schema id: {registered!r}")
    return schema_id


def run(first: Hour, until: Hour | None) -> int:
    """The service: replay every bronze hour after the last checkpoint, in order, forever."""
    store, serialize = Store(s3_config()), schema.Serializer(register(REGISTRY))
    producer = Producer(replayer.producer_config(BOOTSTRAP))
    with audit.connect(ops_dsn()) as conn:
        conn.execute(replayer.CHECKPOINT_SQL)
        while True:
            hour = replayer.next_hour(conn, first)
            if until and hour > until:
                return ExitCode.PASSED
            if not store.exists(bronze.bronze_path(hour)):
                time.sleep(settings.STREAM_IDLE_SLEEP_S)  # caught up with bronze
                continue
            result = replayer.replay_hour(hour, store, producer, serialize)
            replayer.checkpoint(conn, result, hour)
            print(json.dumps(result), flush=True)


def replay_one(hour: Hour) -> dict[str, Any]:
    store, serialize = Store(s3_config()), schema.Serializer(register(REGISTRY))
    with audit.connect(ops_dsn()) as conn:
        conn.execute(replayer.CHECKPOINT_SQL)
        result = replayer.replay_hour(
            hour, store, Producer(replayer.producer_config(BOOTSTRAP)), serialize
        )
        replayer.checkpoint(conn, result, hour)
    return result


def read_topic() -> list[dict[str, Any]]:
    """Every record in gharchive.events, from the beginning to the current end of each partition."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"verify-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    parsed = parse_schema(schema.schema())
    meta = consumer.list_topics(replayer.TOPIC, timeout=30).topics[replayer.TOPIC]
    ends = {
        p: consumer.get_watermark_offsets(TopicPartition(replayer.TOPIC, p), timeout=30)[1]
        for p in meta.partitions
    }
    consumer.assign([TopicPartition(replayer.TOPIC, p, OFFSET_BEGINNING) for p in meta.partitions])
    records, done = [], {p for p, end in ends.items() if end == 0}
    while len(done) < len(ends):
        msg = consumer.poll(5)
        value, offset = (None, None) if msg is None or msg.error() else (msg.value(), msg.offset())
        if msg is None or value is None or offset is None:
            continue
        record = schemaless_reader(io.BytesIO(value[5:]), parsed, parsed)
        records.append({**record, "_partition": msg.partition()})  # type: ignore[dict-item]
        if offset + 1 >= ends[msg.partition()]:
            done.add(msg.partition())
    consumer.close()
    return records


def verify(hour: Hour) -> dict[str, Any]:
    bronze_ids = (
        pq.read_table(io.BytesIO(Store(s3_config()).get(bronze.bronze_path(hour))), columns=["id"])
        .column("id")
        .to_pylist()
    )
    mine = [r for r in read_topic() if r["hour"] == hour.key]
    events = [r for r in mine if r["kind"] == "EVENT"]
    ids = Counter(r["id"] for r in events)
    markers = Counter(r["_partition"] for r in mine if r["kind"] == "END_OF_HOUR")
    missing = set(bronze_ids) - set(ids)
    return {
        "hour": hour.key,
        "bronze_rows": len(bronze_ids),
        "events_in_topic": len(events),
        "distinct_ids": len(ids),
        "duplicates": sum(n - 1 for n in ids.values()),
        "missing_ids": len(missing),
        "extra_ids": len(set(ids) - set(bronze_ids)),
        "markers_by_partition": dict(sorted(markers.items())),
        "nothing_lost": not missing,
    }


def redteam() -> dict[str, Any]:
    """1) the registry refuses a BACKWARD-incompatible schema; 2) the serializer refuses by rule."""
    broken = schema.schema()
    broken["fields"] = [
        f if f["name"] != "repo_id" else {"name": "repo_id", "type": "string"}
        for f in broken["fields"]
    ]
    body = json.dumps({"schema": json.dumps(broken)}).encode()
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    request = urllib.request.Request(
        f"{REGISTRY}/subjects/{schema.SUBJECT}/versions", body, headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            registry = {"accepted": True, "status": response.status}
    except urllib.error.HTTPError as exc:
        registry = {"accepted": False, "status": exc.code, "message": exc.read().decode()[:200]}
    serializer = schema.Serializer(0)
    record: dict[str, Any]
    try:
        serializer(
            {"kind": "EVENT", "hour": "2026-08-03-0", "type": "PushEvent", "created_at": None}
        )
        record = {"refused": False}
    except schema.RecordRefusal as refusal:
        record = {"refused": True, "rule": refusal.rule}
    offline = schema.backward_problems(broken, schema.schema())
    return {"incompatible_schema": {"registry": registry, "offline_check": offline}, "record_missing_id": record,
            "caught": not registry["accepted"] and bool(offline) and record.get("rule") == "EVENT_ID_REQUIRED"}  # fmt: skip


def schema_file(check_only: bool) -> int:
    if check_only:
        same = AVSC.exists() and AVSC.read_text(encoding="utf-8") == schema.schema_json()
        print("schema up to date" if same else f"{AVSC} differs: run `stream schema`")
        return ExitCode.PASSED if same else ExitCode.REFUSED
    AVSC.parent.mkdir(parents=True, exist_ok=True)
    AVSC.write_text(schema.schema_json(), encoding="utf-8", newline="\n")
    return ExitCode.PASSED


def compat(old: Path) -> dict[str, Any]:
    problems = schema.backward_problems(
        schema.schema(), json.loads(old.read_text(encoding="utf-8"))
    )
    return {
        "backward_compatible": not problems,
        "problems": problems,
        "caught": None if not problems else False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "command", choices=["schema", "compat", "register", "replay", "run", "verify", "redteam"]
    )
    parser.add_argument("hour", nargs="?")
    parser.add_argument(
        "--check", action="store_true", help="schema: fail if the committed .avsc drifted"
    )
    parser.add_argument(
        "--old", type=Path, help="compat: the older schema to stay BACKWARD-compatible with"
    )
    parser.add_argument(
        "--from", dest="first", default=os.environ.get("REPLAY_FROM", "2026-08-03-0")
    )
    parser.add_argument("--until", default=os.environ.get("REPLAY_UNTIL"))
    args = parser.parse_args(argv)
    one_shot = {
        "compat": lambda: compat(args.old),
        "register": lambda: {"subject": schema.SUBJECT, "id": register(REGISTRY)},
        "replay": lambda: replay_one(Hour.parse(args.hour)),
        "verify": lambda: verify(Hour.parse(args.hour)),
        "redteam": redteam,
    }

    def go() -> int:
        if args.command == "schema":
            return schema_file(args.check)
        if args.command == "run":
            return run(Hour.parse(args.first), Hour.parse(args.until) if args.until else None)
        record: dict[str, Any] = one_shot[args.command]()
        print(json.dumps(record))
        failed = record.get("caught") is False or record.get("nothing_lost") is False
        return ExitCode.REFUSED if failed else ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())
