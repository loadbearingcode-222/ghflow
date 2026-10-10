# Runbook

How to operate ghflow day to day, and what to do when something is red. Every command runs from
the repo root in PowerShell. The failure drills that proved these steps are in
[FAILURE_DRILLS.md](FAILURE_DRILLS.md).

## The shape of it

- **Batch** (hourly): `gharchive_ingest` → raw and bronze in S3 → `ingest_audit`.
- **Batch** (daily): `dbt_build` → the DuckDB warehouse → `gharchive_quality` → `quality_verdicts`.
- **Stream** (continuous): `replayer` (bronze → `gharchive.events`) → `lander` (→ `stream/landed`)
  → `stream_compact` every 15 min (→ `stream/compact`) → `reconcile` hourly (→ `reconcile_results`).

## Start, stop, look

| Task | Command |
|---|---|
| Preflight (ports, RAM vs budget) | `.\make.ps1 doctor` |
| Start everything, wait for healthy | `.\make.ps1 up` |
| Stop (volumes kept) | `.\make.ps1 down` |
| What is running | `.\make.ps1 ps` |
| Stream lag per partition | `.\make.ps1 watch` (also Console: http://127.0.0.1:38080/groups/lander) |
| Backfill progress | `.\make.ps1 watch -To <last hour>` |
| Airflow UI | http://127.0.0.1:38082 (admin / `AIRFLOW_ADMIN_PASSWORD` in `.env`) |

## When something is red

### `reconcile` fails: an hour is red
1. **Read the named ids.** Run `select hour, missing_count, extra_count, missing, extra from
   reconcile_results order by id desc limit 20` in `ghflow_ops`.
2. **Missing in the stream:**
   - Check the hour is checkpointed: `select * from replay_checkpoint where hour = '<h>'`.
   - If it is, check the lander has the hour on all 6 partitions:
     `select * from stream_hours where hour = '<h>'`.
   - Re-publish it with `.\make.ps1 replay -Hour <h>`. The duplicates are removed at compaction.
3. **Extra in the stream:** an id that bronze no longer has means bronze was re-ingested after
   the replay. Re-publish the hour, then delete its compacted file and its `stream_compacted`
   row so the hour is compacted again.

### The lander is down or lagging
- **Down:** `docker compose start lander`. It resumes from its committed offsets and rewrites the
  files it had not committed. That is safe: the same range gives the same file.
- **Lag that keeps growing** while the replayer runs is normal: the replayer is faster. It drains
  once the replay catches up.
- **Prove the landed set is whole:** `uv run python -m ghflow.commands.lander coverage` must say
  `"exactly_once": true`.

### The replayer died mid-hour
- Run `docker compose start replayer`. It restarts at the hour after its last checkpoint,
  republishing the interrupted hour whole.
- Expect duplicates for that hour in `stream_compacted.duplicates`; never missing ids.

### Rewinding the lander (re-land a range)
1. `docker compose stop lander`, then wait for the group to empty (about 45 s, gotcha #51).
2. Alter the group's offsets (AdminClient `alter_consumer_group_offsets`) and **read the
   per-partition result for errors**.
3. `docker compose start lander`.

### An hour is missing from the batch (downtime)
- Downtime no longer loses hours: `gharchive_ingest` runs with catchup.
- A gap that already exists: `.\make.ps1 backfill -To <last hour>` (it creates only the missing
  runs), then trigger `dbt_build` with `{"full_refresh": true}` (gotcha #48).

### `dbt_build` failed
- `silver_rows_equal_bronze_rows` fails only on a real hole now (gotcha #49): re-ingest that hour,
  then rebuild with a full refresh.
- An `era__…` test failing names the field and the era. That's a contract question
  (`ghflow.fields`), not a data fix.

### `gharchive_quality` flagged hours
- `flagged` means volume or PushEvent broke: the hour is shaded downstream, which is expected for
  the known outages.
- `shifted` means only minor types moved. It's recorded, and needs no action.

## Never

- `.\make.ps1 reset -Force` (it deletes every volume, including `ingest_audit`) without asking
  the owner.
- `docker compose down` on another stack. Stop olist with `docker compose stop` in its own folder.
