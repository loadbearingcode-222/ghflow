"""Sample every consumer group's lag into ClickHouse, for Grafana (M4-S2).

    python -m ghflow.commands.lag_sampler run      # the service: one sample every LAG_SAMPLE_S
    python -m ghflow.commands.lag_sampler once     # one sample, printed and written

Per (group, topic, partition): the group's committed offset, the partition's end, and their
difference. A transactional topic ends in a commit marker no consumer commits past, so a
caught-up group on `gharchive.repo_state` reads a constant lag of 1 per partition: flat, which
is what the panel needs to show.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import TopicPartition

from ghflow import settings
from ghflow.commands.local import clickhouse
from ghflow.exitcodes import ExitCode, guarded
from ghflow.stream import clients

#: (consumer group, topic): every consumer of the stream, each with its own Grafana panel.
GROUPS = (
    (clients.LANDER_GROUP, clients.TOPIC),
    (clients.REPO_STATE_GROUP, clients.TOPIC),
    ("clickhouse", clients.TOPIC),
    ("clickhouse-repo-state", clients.REPO_STATE_TOPIC),
)
COLUMNS = ["ts", "group", "topic", "partition", "committed", "end", "lag"]


def sample(now: datetime) -> list[list[Any]]:
    rows = []
    for group, topic in GROUPS:
        consumer = clients.peek_consumer(group)
        try:
            meta = consumer.list_topics(topic, timeout=30).topics[topic].partitions
            committed = consumer.committed([TopicPartition(topic, p) for p in meta], timeout=30)
            for tp in committed:
                end = consumer.get_watermark_offsets(tp, timeout=30, cached=False)[1]
                done = max(tp.offset, 0)
                rows.append([now, group, topic, tp.partition, done, end, end - done])
        finally:
            consumer.close()
    return rows


def write(rows: list[list[Any]]) -> None:
    clickhouse().insert("ghflow.consumer_lag", rows, column_names=COLUMNS)


def run() -> int:
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())
    while not stopping.is_set():
        try:
            rows = sample(datetime.now(UTC).replace(microsecond=0))
            write(rows)
        except Exception as exc:  # a missed sample must not stop the service
            print(json.dumps({"sample_failed": repr(exc)[:300]}), flush=True)
        stopping.wait(settings.LAG_SAMPLE_S)
    return ExitCode.PASSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["run", "once"])
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "run":
            return run()
        rows = sample(datetime.now(UTC).replace(microsecond=0))
        write(rows)
        totals: dict[str, int] = {}
        for row in rows:
            totals[row[1]] = totals.get(row[1], 0) + row[6]
        print(json.dumps({"rows": len(rows), "lag_by_group": totals}))
        return ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())
