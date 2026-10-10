# 0004: Quality is judged against a rolling clean baseline; verdicts live in one table
Date: 2026-10-10 · Story: M1-S4 · Status: accepted, amended in M1-S5 (`shifted`, closing #22)

## Context

The archive has real, unplanted defects:
- near-empty hours served as HTTP 200;
- hours with a normal row count but almost no PushEvents (Sep 2–5);
- an afternoon incident on Aug 7.

The plan argued the quality bars from a fixed "clean baseline (Aug 3–16)". Measured on the
warehouse, that fortnight is itself abnormal: 189 PullRequestEvents/h and 95% pushes, against
9,853/h and 66% in a 2024 reference hour (Oct 1–4: 3,102/h, 74%). A fixed August baseline would
call most of mid-September to October broken.

## Decision

1. **Rolling clean baseline** (owner's decision, verbatim 2026-10-10: "Rolling clean baseline
   (Recommended)"). An hour is compared with the same hour of day on up to 7 earlier *clean* days,
   found within 28 days back. Fewer than 3 references gives no verdict. The seed fortnight
   (Aug 3–16) is judged against its own other days.
2. **Two checks, each finding naming its subject.** Volume: the hour's rows against the reference
   median. Event mix: each type's share against its reference median, for types that make up at
   least 5% of the reference. The bars are 0.35–2.0 for volume and 0.6–1.67 for share. They were
   argued from the seed's natural spread, in `automation/runs/m1-s4/bars.json`, and committed
   before the prediction, which was committed before the run. A test checks the order from git.
3. **One table defines "broken".** Every run writes one row per hour to
   `ghflow_ops.quality_verdicts`, holding the status, the findings (JSON) and the bars the verdict
   was taken under. The M5 marts read this table; nothing else may redefine a broken hour.
4. **Re-publication is detected by ETag.** `gharchive_quality` HEADs the last 14 days daily. A
   live ETag that differs from the one recorded by the hour's latest accepted ingest queues
   the hour in `ghflow_ops.reingest_requests`. Queued hours are re-ingested. If anything was,
   `dbt_build` runs with `full_refresh`, because the incremental model only re-reads its newest
   days. A request records which audit table it read, so a red team on a copy never triggers
   real work.

## Options considered

- **The fixed Aug 3–16 baseline (as planned).** Rejected by the owner after the measurement
  above.
- **The most recent fortnight as the baseline.** Rejected: it bakes in whichever regime is
  newest, and August would read as degraded.
- **Absolute row floors** (e.g. "fewer than 10k rows"). These catch near-empty hours, but not
  a broken mix at a normal count (Sep 5 15:00). The volume-only control reproduces this.

## Consequences

- Every abrupt break the bars were argued for is caught (6 of 6 claims), along with the
  volume-only control and both red teams.
- **Known weakness, measured (issue #22):** a slow, partial recovery can walk the baseline. The
  September recovery hours, at 0.54–0.72 push share, entered it one at a time, so October is
  judged against a half-broken mix: 157 of 216 hours of Oct 1–9 were flagged. The bars were not
  loosened after the run. The fix is a re-argued, anchored baseline in M1-S5, with a new
  prediction, shown to the owner first.
- Until #22 lands, the verdict table over-flags Sep 18 – Oct 9. M5 must not shade those hours
  as broken until M1-S5 re-judges them.

## Amendment (M1-S5, 2026-10-10): verdicts say how an hour differs

The weakness above was fixed by an owner decision, verbatim: "Adopt 'shifted' (Recommended)".
- **`flagged`** means volume broke, or the dominant type (`QUALITY_DOMINANT_TYPE`, PushEvent) did.
  These hours are shaded by M5 and never join the baseline.
- **`shifted`** means only minor types' shares moved. These hours are recorded but not shaded,
  and they join the baseline, so a regime change is followed instead of walked.

Five other candidates were measured first (`automation/runs/m1-s5/`). None kept every abrupt-break
claim while freeing October:
- also judging each type's absolute rate;
- a seed-anchored join;
- a regime re-seed, after 5 or after 7 days;
- a centred median.

On the same frozen counts, all 14 claims of `prediction-baseline.json` held:
- 438 flagged (from 742) and 88 shifted;
- Oct 1–9: 43 of 216 flagged, every one on volume or PushEvent (its real defects);
- the mix red team unchanged.

M1-S4's U3 ("≤ 5% of October flagged") was itself wrong: October judged against itself flags
51 of 216 hours.
