# 0013: The stakeholder dashboard is static Quarto, and every number is anchored to a mart
Date: 2026-10-10 · Story: M5-S2 · Status: accepted

## Context

Non-technical readers will take the pages at their word. A number that drifts from the data, a
chart that silently empties, or a hidden broken hour would mislead them, and nobody downstream
would notice.

## Decision

1. **Static pages:** Quarto 1.10.19 (pinned by version and the release asset's sha256) renders
   six plain-language pages inside the Airflow image. The `.qmd` files in `dashboard/` are
   templates: analysts write the prose there, and `ghflow.dashboard` fills its slots from the
   mart snapshots (`s3://marts/latest`, never the warehouse). No Jupyter engine; charts are
   Plotly figure JSON drawn by the vendored `plotly.min.js`. Each page embeds every resource,
   so the zip bundle opens without the stack.
2. **Every number is anchored:** a slot `{{n:id}}` renders as a span naming its query.
   `dashboard check`:
   - re-runs each query, re-formats it, and must read back the same text;
   - fails on any digit outside such a span; the templates hold no digits either (unit test);
   - fails on any empty chart.

   A direction, such as "more" or "less", is part of the number, never of the prose. The first
   build said "weekdays are busier" while the data said 17% less.
3. **Flagged hours stay visible:** charts over days shade every day with a flagged hour, as
   `mart_trust` says. Missing a shade, or adding one, fails the check.
4. **People are never named or ranked:** only ranked repos and `[bot]` accounts may be named,
   through tagged spans or as chart labels, and the check refuses any other name. No dashboard
   query reads `actor_login` or `actor_id` (unit test).
5. **Publishing:** `dashboard_render` runs on the marts asset that `dbt_build`'s `export` emits.
   - It refuses snapshots older than the triggering build (the asset event carries the build's
     start).
   - It publishes only a site that passed `check`: into a new release folder, then an atomic
     switch of the `current` symlink that nginx serves on 127.0.0.1:33001, plus a zip in
     `s3://marts/dashboard/`.

## Consequences

- Each page is about 6.8 MB, because plotly.min.js is inlined into every page. M5-S3 measures
  render time and bundle size against a prediction (a partial Plotly bundle is the obvious
  candidate).
- The dashboard is as fresh as the daily build, and that is what the pages say.
