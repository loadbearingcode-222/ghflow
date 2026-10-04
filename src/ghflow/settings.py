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
