# GH Archive — the source

[GH Archive](https://www.gharchive.org/) records the public GitHub timeline and publishes it as
one gzipped file of newline-delimited JSON per hour, at `https://data.gharchive.org/<hour>.json.gz`,
where the hour is named like `2026-09-05-15` (the hour is not zero-padded). Each line is one event:
`id`, `type`, `actor`, `repo`, `org` (when the repo belongs to one), `payload`, `public`,
`created_at`. Attribution: data by GH Archive, from GitHub's public events; ghflow never
redistributes it — raw and derived data live only in Docker volumes.

**Rule for this file:** the prose quotes no measurements. Every number below sits between the
GENERATED markers and is rendered from `automation/runs/m0-s2/profile.json` by
`python -m ghflow.profile render`; `tests/unit/test_profile_doc.py` fails if the two disagree or
if a number appears outside the block.

## What the pipeline must assume

- **A file can be served with HTTP 200 and be nearly empty.** The archive's collector had outages,
  and those hours were published with a fraction of their events and never re-published. "The
  download succeeded" is not a quality check.
- **An hour can have a normal row count and a broken event mix.** Some hours after the outage hold
  the usual number of events but almost no PushEvents. A volume check alone passes them; the
  event-type mix catches them (M1-S4).
- **Payload shapes change without notice.** GitHub trimmed event payloads: PushEvent stopped
  carrying its `commits`, `size` and `distinct_size`, and `pull_request` shrank from the full
  object to a handful of keys. Both trims landed in the same hour — one API change, not two — and
  that changeover hour is itself nearly empty, so a collection gap and a schema change arrived
  together. The hour before still carries the old shape almost everywhere; the hour after carries
  only the new one. Each payload field therefore carries the era it exists in (M0-S3).
- **Some events have no repository.** A few ForkEvents carry an empty `repo` object: no id,
  no name, nothing to join or partition on. The contract rejects them into the sidecar under
  `repo_malformed` (M0-S3 evidence: `automation/runs/m0-s3/real_hours.json`).
- **Event time equals file hour.** In every sampled hour, every event's `created_at` falls inside
  the file's own hour, so the Airflow data interval is an exact watermark for the batch path.
- **Files appear shortly after the hour closes** and carry an `ETag` and `Last-Modified`, which is
  how ghflow notices if an hour is ever re-published (M1-S4).
- **A request that fails is not a missing file.** Only an HTTP 404 means "not published"; a
  timeout, a reset, a server error or Cloudflare's block (HTTP 403) means "we could not see it", and is retried slowly
  and sequentially before being reported (gotchas #31, #32).

## Measurements

<!-- BEGIN GENERATED: python -m ghflow.profile render -->
*Measured 2026-10-04T16:56:42Z by `python -m ghflow.profile measure`; record: `automation/runs/m0-s2/profile.json`.*

#### Download needed to backfill

| Window | Days | Download (gzipped) | Per day |
|---|---|---|---|
| Last 14 days (2026-09-20 → 2026-10-03) | 14 | 6.1 GB | 0.4 GB |
| Last 30 days (2026-09-04 → 2026-10-03) | 30 | 21.3 GB | 0.7 GB |
| Whole window (2026-08-03 → 2026-10-03) | 62 | 32.3 GB | 0.5 GB |

Probed 1,488 hours: 1,488 published, 0 not published, 0 request failed after 3 sequential retries (0 needed a retry), at 4 in flight.

#### Near-empty hours

**111 hours** under 5.0 MB gzipped, all served as HTTP 200:

| Day | Hours (UTC) under the threshold |
|---|---|
| 2026-08-19 | 11 of 24: 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 |
| 2026-08-20 | 18 of 24: 0, 1, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 |
| 2026-08-21 | 5 of 24: 0, 1, 2, 3, 23 |
| 2026-08-27 | 5 of 24: 18, 19, 21, 22, 23 |
| 2026-08-28 | 1 of 24: 0 |
| 2026-08-31 | 7 of 24: 17, 18, 19, 20, 21, 22, 23 |
| 2026-09-01 | 15 of 24: 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 |
| 2026-09-02 | 24 of 24: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 |
| 2026-09-03 | 21 of 24: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 |
| 2026-09-21 | 4 of 24: 14, 15, 16, 17 |

#### Sampled hours, streamed in full

| Hour (UTC) | .gz | Raw JSON | Events | Payload share | PushEvent share | Events outside the hour |
|---|---|---|---|---|---|---|
| 2024-06-15-15 | 69.3 MB | 510.7 MB | 184,260 | 83% | 66% | 0 |
| 2026-09-01-15 | 1.7 MB | 8.5 MB | 1,844 | 90% | 0% | 0 |
| 2026-09-05-15 | 78.7 MB | 383.5 MB | 84,492 | 91% | 14% | 0 |
| 2026-10-02-15 | 20.8 MB | 95.0 MB | 84,804 | 59% | 74% | 0 |

#### Payload drift

Found by bisection; each hour is judged on its first 20 events of the type.

| Change | Last hour with the old shape | First hour with the new shape |
|---|---|---|
| PushEvent loses `commits` | 2025-10-08-16 | 2025-10-08-17 |
| `pull_request` shrinks to 10 keys or fewer | 2025-10-08-16 | 2025-10-08-17 |

Second witness: every event of both types in the boundary hours, by shape.

| Hour (UTC) | PushEvents with `commits` | Full `pull_request` objects |
|---|---|---|
| 2025-10-08-16 | 95,492 of 95,561 | 12,860 of 12,862 |
| 2025-10-08-17 | 0 of 651 | 0 of 62 |
<!-- END GENERATED -->
