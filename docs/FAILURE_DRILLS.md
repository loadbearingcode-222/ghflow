# Failure drills

Each drill was run against the live stream while it replayed the window, on 2026-10-10. Each
must end with **zero loss**, proven by data rather than by "it came back":
- `lander coverage`: exactly once;
- `reconcile`: zero missing and zero extra ids for the hours in flight.

Evidence: `automation/runs/m2-s3/drills.json`. How to act on each failure: [RUNBOOK.md](RUNBOOK.md).

## A. Kill the lander

| Step | What | Observed |
|---|---|---|
| 1 | `docker kill --signal KILL ghflow-lander-1` while the replayer publishes | killed 05:34:17 |
| 2 | Leave it down 60 s | lag 132,709 → 1,834,638; hours Aug 5 22:00 – Aug 6 11:00 published meanwhile |
| 3 | `docker compose start lander` (a manual stop is not restarted by the policy, gotcha #50) | restarted 05:35:22 |
| 4 | `lander coverage` | exactly once on all 6 partitions |
| 5 | `reconcile` over the hours published while it was down | see drills.json → final |

**Why it holds:** offsets are committed only after a file is written. The restart re-reads from
the last commit and rewrites the same (partition, hour) files.

## B. Kill the replayer

| Step | What | Observed |
|---|---|---|
| 1 | `docker kill --signal KILL ghflow-replayer-1` mid-hour | killed 05:37:36, while publishing 2026-08-07-22 (checkpoint at 21) |
| 2 | Leave it down 30 s | the lander drains (lag 1.54M → 0.49M) |
| 3 | `docker compose start replayer` | it republished 2026-08-07-22 whole, then moved on |
| 4 | Compaction of 2026-08-07-22 | duplicates removed (the partial first publication) |
| 5 | `reconcile` of 2026-08-07-22 | see drills.json → final |

**Why it holds:** the checkpoint moves only after the flush confirms every delivery. An
interrupted hour has no checkpoint and no markers, so it is published again in full.

## C. Restart the broker

| Step | What | Observed |
|---|---|---|
| 1 | `docker compose restart redpanda` with both clients running | 05:38:48 |
| 2 | Watch both clients | the lander logged `Connection refused` and reconnected; the replayer rode through (0 restarts), every hour's deliveries confirmed |
| 3 | `lander coverage` | exactly once |
| 4 | `reconcile` of the hours in flight (2026-08-08-3 .. 5) | see drills.json → final |

**Why it holds:**
- **Replayer:** an idempotent producer with acks=all retries through a short outage without
  duplicates. If retries ran out, the hour would raise, leave no checkpoint, and be republished.
- **Lander:** it reconnects and resumes from its commits.

## Re-running a drill

The commands are in the tables. Snapshot before and after with:
- `uv run python -m ghflow.commands.lander lag`
- `uv run python -m ghflow.commands.lander coverage`
- `select max(hour_start) from replay_checkpoint`

Finish with `uv run python -m ghflow.commands.reconcile run --to <newest closed hour>`, which
must report `"red": 0`.
