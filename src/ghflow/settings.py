"""The source's constants, in one place. Each value carries the reason it is what it is."""

from __future__ import annotations

from datetime import date

BASE_URL = "https://data.gharchive.org"

#: Cloudflare in front of data.gharchive.org answers Python's default `Python-urllib/3.x`
#: User-Agent with HTTP 403 (gotcha #31). A descriptive agent is also simply polite.
USER_AGENT = "ghflow/0.1 (learning project; https://github.com/loadbearingcode-222/ghflow)"

#: Requests in flight at once. 32 produced ~1.7% timeouts that looked like missing files
#: (gotcha #32); the batch path budgets 4 hours in flight, and so does every sweep.
CONCURRENCY = 4

#: Seconds before a request counts as failed (not as "not published").
HTTP_TIMEOUT_S = 30.0

#: A request that failed is retried this many times, sequentially, with a growing pause.
RETRIES = 3
RETRY_PAUSE_S = 2.0

#: An hour smaller than this is "near-empty". Healthy hours in Aug-Oct 2026 were 15-90 MB
#: gzipped; the broken ones were about 2 MB. This is a scoping label for the profile, not the
#: quality bar: M1-S4 argues its own bars from a clean baseline.
NEAR_EMPTY_BYTES = 5_000_000

#: The profiled window: the backfill start the user chose, through the last full day at scoping.
WINDOW_FIRST = date(2026, 8, 3)
WINDOW_LAST = date(2026, 10, 3)

#: Hours streamed in full for their event mix: a 2024 baseline, a near-empty outage hour, a
#: broken-mix hour with a normal row count, and a healthy recent hour.
SAMPLED_HOURS = ("2024-06-15-15", "2026-09-01-15", "2026-09-05-15", "2026-10-02-15")

#: Known-good brackets for the payload drift bisection: the earlier hour still carries the
#: full payloads, the later one is trimmed (both observed during scoping).
DRIFT_BEFORE = "2024-06-15-15"
DRIFT_AFTER = "2026-10-02-15"

#: Payload-era classification (M0-S2): events of a type inspected before calling an hour's
#: shape. Each must agree, so a mixed hour reads as the old shape.
ERA_SAMPLE = 20

#: An hour with none of the event type (the Sep 2026 outage had no PushEvents for days) is
#: undecided; the era probe moves forward hour by hour, at most this far.
ERA_SKIP_LIMIT = 96

#: An in-era field missing from every row of its type refuses the hour only if the type has at
#: least this many rows — one row without an optional key is not a schema change (M0-S3).
VANISHED_MIN_ROWS = 20

#: Bronze Parquet (M1-S1): zstd at a fixed level and one row group per hour, so the same rows
#: always encode to the same bytes (re-running an hour rewrites a byte-identical file).
BRONZE_ZSTD_LEVEL = 3
BRONZE_ROW_GROUP_ROWS = 1_000_000

#: Raw .gz files are kept this many days after their hour (M1-S2, ADR-0002); older ones can be
#: fetched from GH Archive again. Age is the hour's own timestamp, not the upload time: a
#: backfill uploads August today, and August is still old.
RAW_KEEP_DAYS = 14

# ---- Data quality (M1-S4, ADR-0004): the rolling clean baseline and its bars.
#: An hour is judged against up to this many earlier clean hours at the same hour of day ...
QUALITY_REFERENCE_SIZE = 7
#: ... found within this many days back; fewer than QUALITY_MIN_REFERENCE gives no verdict.
QUALITY_LOOKBACK_DAYS = 28
QUALITY_MIN_REFERENCE = 3
#: The seed fortnight: judged against its own other days (it has no clean past).
QUALITY_SEED_FIRST = date(2026, 8, 3)
QUALITY_SEED_LAST = date(2026, 8, 16)
#: Bars: ratios to the reference median, argued from the seed fortnight BEFORE any hour was
#: judged (automation/runs/m1-s4/bars.json has the full argument and the numbers).
#: Volume: natural weekday dips go down to 0.406; the Aug 7 09-15 incident sits at 0.24-0.30 and
#: near-empty hours near 0.01. Highs reach 1.38; spikes cost less, so the high bar is generous.
QUALITY_VOLUME_LOW = 0.35
QUALITY_VOLUME_HIGH = 2.0
#: Event mix: only types making up >= 5% of the reference are judged by share. Types at 1-3%
#: swing 0.24-4.3x from noise alone, which a bar catching real incidents cannot tolerate.
QUALITY_MIX_MIN_SHARE = 0.05
#: PushEvent's share stays within 0.92-1.03 of its reference; the Aug 7 16-19 incident drops it
#: to 0.15-0.55. 0.6 sits between with margin; the high bar is its log-mirror (1/0.6).
QUALITY_MIX_LOW = 0.6
QUALITY_MIX_HIGH = 1.67
#: The type whose collapse makes an hour broken rather than shifted (owner's decision 2026-10-10,
#: M1-S5, #22): PushEvent is 74-95% of every regime in the window; minor types swing by regime.
QUALITY_DOMINANT_TYPE = "PushEvent"

# ---- The replayed stream (M2-S1, ADR-0005).
#: gharchive.events: partitions (keyed by repo id) and retention (from append time, not event time).
STREAM_PARTITIONS = 6
STREAM_RETENTION_DAYS = 7
#: How long the replayer waits for every delivery report of an hour before calling it failed.
STREAM_FLUSH_TIMEOUT_S = 300
#: The replayer looks for the next bronze hour this often once it has caught up.
STREAM_IDLE_SLEEP_S = 60

#: Reconciliation (M2-S3): how many missing / extra event ids a red hour names in full (the counts
#: are always exact).
RECONCILE_NAMED_IDS = 20
