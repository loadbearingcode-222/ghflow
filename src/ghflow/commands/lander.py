"""The lander and its proofs (M2-S2, ADR-0006).

python -m ghflow.commands.lander run        # the service: land per (partition, hour), commit after write
python -m ghflow.commands.lander compact    # every closed, uncompacted hour -> one file of distinct events
python -m ghflow.commands.lander coverage   # landed ranges: contiguous, no overlap, up to the commit
python -m ghflow.commands.lander lag        # per-partition lag of the lander group
"""

from __future__ import annotations

import argparse
import io
import json
import signal
import sys
import threading
from itertools import pairwise
from typing import Any

from confluent_kafka import TopicPartition
from fastavro import parse_schema, schemaless_reader

from ghflow import audit, settings
from ghflow.commands.local import ops_dsn, s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.storage import Store
from ghflow.stream import clients, compact, lander, schema


def run() -> int:
    """Until SIGTERM/SIGINT: then close the consumer, so the group knows at once (not after a
    session timeout) that this member left; found when an offset rewind failed in M2-S2."""
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())
    store, parsed = Store(s3_config()), parse_schema(schema.schema())
    consumer = clients.lander_consumer()
    consumer.subscribe([lander.TOPIC])
    try:
        with audit.connect(ops_dsn()) as conn:
            conn.execute(lander.HOURS_SQL)
            land = lander.Lander(consumer, store, conn)
            while not stopping.is_set():
                msg = consumer.poll(1.0)
                value = None if msg is None or msg.error() else msg.value()
                if msg is None or value is None:
                    continue
                record = schemaless_reader(io.BytesIO(value[5:]), parsed, parsed)
                landed = land.take(msg.partition() or 0, msg.offset() or 0, record)  # type: ignore[arg-type]
                if landed:
                    print(json.dumps(landed), flush=True)
    finally:
        consumer.close()  # uncommitted buffers are simply re-read on the next start
    return ExitCode.PASSED


def closed_hours(conn: Any) -> list[str]:
    """Hours whose marker has been committed past on every partition, not yet compacted."""
    rows = conn.execute(
        b"select hour from stream_hours where hour not in (select hour from stream_compacted) "
        b"group by hour having count(*) = %s order by min(landed_at)",
        (settings.STREAM_PARTITIONS,),
    ).fetchall()
    return [r[0] for r in rows]


def compact_hour(conn: Any, store: Store, hour: str) -> dict[str, Any]:
    paths = [
        r[0]
        for r in conn.execute(
            b"select path from stream_hours where hour = %s order by partition", (hour,)
        ).fetchall()
    ]
    landed = [lander.read_landed(store, p) for p in paths]
    result = compact.compact(landed)
    if not compact.content_equal(landed, result.table):
        raise compact.CompactionRefusal(
            f"{hour}: the output is not content-equal to its inputs; inputs kept"
        )
    data = compact.encode(result.table, hour)
    store.put(lander.compact_path(hour), data)
    conn.execute(
        b"insert into stream_compacted (hour, input_files, input_events, duplicates, output_rows, output_sha256) "
        b"values (%s, %s, %s, %s, %s, %s)",
        (
            hour,
            len(paths),
            result.input_events,
            result.duplicates,
            result.table.num_rows,
            compact.sha256(data),
        ),
    )
    for path in paths:  # the small files go only after the compacted file and its row exist
        store.delete(path)
    return {"hour": hour, "input_files": len(paths), "input_events": result.input_events,
            "duplicates": result.duplicates, "output_rows": result.table.num_rows}  # fmt: skip


def coverage(conn: Any) -> dict[str, Any]:
    admin = clients.peek_consumer(clients.LANDER_GROUP)
    parts = [TopicPartition(lander.TOPIC, p) for p in range(settings.STREAM_PARTITIONS)]
    committed = {tp.partition: tp.offset for tp in admin.committed(parts, timeout=30)}
    admin.close()
    report = {}
    for p in range(settings.STREAM_PARTITIONS):
        ranges = conn.execute(
            b"select first_offset, last_offset from stream_hours where partition = %s order by first_offset",
            (p,),
        ).fetchall()
        gaps = [(a[1], b[0]) for a, b in pairwise(ranges) if b[0] != a[1] + 1]
        end = ranges[-1][1] + 1 if ranges else 0
        report[p] = {"files": len(ranges), "from": ranges[0][0] if ranges else None, "to": end - 1,
                     "committed": committed[p], "gaps_or_overlaps": gaps,
                     "exactly_once": not gaps and (not ranges or ranges[0][0] == 0) and committed[p] in (end, -1001)}  # fmt: skip
    return {"partitions": report, "exactly_once": all(r["exactly_once"] for r in report.values())}


def lag() -> dict[str, Any]:
    consumer = clients.peek_consumer(clients.LANDER_GROUP)
    parts = [TopicPartition(lander.TOPIC, p) for p in range(settings.STREAM_PARTITIONS)]
    out = {}
    for tp in consumer.committed(parts, timeout=30):
        high = consumer.get_watermark_offsets(tp, timeout=30)[1]
        done = max(tp.offset, 0)
        out[tp.partition] = {"committed": done, "end": high, "lag": high - done}
    consumer.close()
    return {
        "group": lander.GROUP,
        "partitions": out,
        "total_lag": sum(p["lag"] for p in out.values()),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["run", "compact", "coverage", "lag"])
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "run":
            return run()
        if args.command == "lag":
            print(json.dumps(lag()))
            return ExitCode.PASSED
        with audit.connect(ops_dsn()) as conn:
            conn.execute(lander.HOURS_SQL)
            if args.command == "coverage":
                record = coverage(conn)
                print(json.dumps(record))
                return ExitCode.PASSED if record["exactly_once"] else ExitCode.REFUSED
            store = Store(s3_config())
            for hour in closed_hours(conn):
                print(json.dumps(compact_hour(conn, store, hour)), flush=True)
        return ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())
