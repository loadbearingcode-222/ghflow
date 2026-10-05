# MILESTONES — the plan, in the order it merges

Two sections, in this order of priority: **the repo layout**, then **the stories**. Every pull
request is one story below, titled `M#-S#: <title>`, from branch `feat/m#-s#-<slug>`, merged
with a merge commit in the order listed. Nothing else becomes a pull request.

- A problem found after a story merged is fixed in the **next** story's PR, under "Also fixes";
  a small, behaviour-preserving improvement may ride under "Also refines".
- **Iteration** (revision 2, 2026-10-05): every milestone ends with a **Refine** story that comes
  back to earlier work and carries the release tag (M5: the whole-system refine precedes the
  demo). Improvements found mid-story become GitHub issues labelled `refine`. A later insight that
  changes an earlier *design* becomes a **Realign** story, inserted here by a re-plan the owner
  approves. The rules: [ITERATION.md](ITERATION.md) (from M0-S5).
- Only broken production earns a point story (`M1-S2.1`, branch `feat/m1-s2-1-<slug>`), added
  here in the PR that does the work. This is a local project, so expect none.
- A path not declared in the layout arrives only through a story that edits the layout first.
- A milestone closes inside its **last** story, whose merge commit is tagged with its release.
- Every commit ends with a `Role:` trailer and every PR carries the matching label:
  `data engineer` for M0–M4, `data analyst` for M5; Refine stories also carry `refine`.

Both structural rules are enforced in CI: `.github/scripts/story_lint.py` (branch, title, order)
and `.github/scripts/layout_lint.py` (every tracked path is declared below). The architecture
blueprint is [architecture.md](architecture.md); decisions and budgets live in
[../CLAUDE.md](../CLAUDE.md) and [adr/](adr/).

---

## Repo layout

Each path is tagged with the story that adds it. A directory with nothing listed under it may
hold any files; a directory whose contents are listed may hold only those. One entry per line
(gotcha #37). A Refine story that moves code edits this layout in its own PR, before the move.

```
ghflow/
├─ .github/
│  ├─ workflows/ci.yml                                M0-S1  story-lint · layout-lint · ruff · mypy · pytest; + compose-render, dag-integrity (M0-S4) · import-layers, ratchet (M0-S5) · dbt-parse (M1-S3) · avro-compat (M2-S1)
│  ├─ scripts/story_lint.py · scripts/layout_lint.py  M0-S1  ported from olist-lakehouse
│  ├─ ISSUE_TEMPLATE/refine.yml                       M0-S5  the improvement form: where · evidence · proposed change · expected gain and how it is measured · target milestone
│  └─ pull_request_template.md                        M0-S1  handoff · Accept-when evidence · field note · Also fixes; "Also refines" added in M0-S4
├─ .githooks/pre-push                                 M0-S1  refuses direct pushes to main (mode 100755, asserted by test)
├─ README.md · LICENSE · .gitignore · .gitattributes  init   README grows each milestone; "How it got better" table from M5-S4
├─ CLAUDE.md                                          M0-S1  mode · scope · imports · decisions (verbatim) · env facts · fleet ledger · pins
├─ make.ps1                                           M0-S1  the single task runner; grows every story
├─ pyproject.toml · uv.lock · .python-version         M0-S1  uv, Python 3.12, ruff, mypy, pytest; import-linter and the ratchet thresholds from M0-S5
├─ .pre-commit-config.yaml                            M0-S1
├─ .env.example                                       M0-S4  keys only
├─ .dockerignore                                      M0-S4
├─ compose.yaml                                       M0-S4  grows: SeaweedFS, Postgres, Airflow (M0) → redpanda (M2) → repo-state (M3) → clickhouse, grafana (M4) → dashboard (M5)
├─ docker/
│  ├─ airflow/                                        M0-S4  Airflow 3 image; project venv and dbt isolated (olist ADR-0007 pattern); Quarto CLI added M5-S2
│  ├─ app/                                            M2-S1  one Python image: replayer (M2-S1), lander (M2-S2), repo-state (M3-S2)
│  └─ grafana/                                        M4-S2  provisioning: ClickHouse datasource, dashboards as code
├─ config/
│  ├─ platform/                                       M0-S4  idempotent, additive init: Postgres dbs/roles, S3 buckets (never drops)
│  ├─ redpanda/                                       M2-S1  topic specs as code (partitions, retention, cleanup policy); repo_state added M3-S2
│  ├─ schemas/                                        M2-S1  gh_event.avsc (event + end-of-hour marker); repo_state.avsc added M3-S2
│  └─ clickhouse/                                     M4-S1  server memory caps, users, ordered DDL
├─ src/ghflow/
│  ├─ __init__.py · settings.py                       M0-S2  settings: every knob, one home (enforced from M0-S5)
│  ├─ hours.py · events.py · fields.py · profiling.py  M0-S5  pure core: hour values · the one JSON-line reader · field declarations and eras · profile arithmetic and rendering
│  ├─ archive.py                                      M0-S2  I/O shell: HEAD/GET with a User-Agent · 404 vs timeout as distinct verdicts · download
│  ├─ contract.py · exitcodes.py                      M0-S3  pure core: the one cast · structure refusal · named row rules
│  ├─ commands/                                       M0-S5  I/O shell: the CLIs (profile, contract); every one exits through guarded()
│  ├─ bronze.py · audit.py                            M1-S1  raw → projected Parquet + rejected-row sidecar · one audit row per attempt
│  ├─ quality/                                        M1-S4  baselines · volume · event mix · ETag re-check
│  ├─ stream/
│  │  ├─ __init__.py · replayer.py                    M2-S1  bronze hour → topic; checkpoint after flush; end-of-hour markers
│  │  ├─ lander.py · compact.py                       M2-S2  topic → S3 files named by offset range; small-file compaction
│  │  └─ repo_state.py                                M3-S2  stateful processor: SQLite store + compacted changelog, transactional
│  └─ reconcile.py                                    M2-S3  per-hour event-id sets: batch ↔ stream (↔ ClickHouse from M4-S1)
├─ airflow/dags/                                      M1-S1  gharchive_ingest (M1-S1) · raw_retention (M1-S2) · dbt_build (M1-S3) · gharchive_quality (M1-S4) · stream_compact (M2-S2) · reconcile (M2-S3) · dashboard_render (M5-S2)
├─ dbt/ghflow/
│  ├─ dbt_project.yml · profiles.yml · packages.yml · package-lock.yml   M1-S3  dbt-duckdb over bronze Parquet in S3 (SeaweedFS)
│  ├─ macros/ · tests/                                M1-S3
│  ├─ models/staging/ · models/silver/                M1-S3  typed per event type; drift handled by era; contracts enforced
│  ├─ models/gold/                                    M3-S1  dim_repo, dim_actor (SCD2 from the event log)
│  └─ models/marts/                                   M5-S1  one insight mart per KPI id; exported as Parquet snapshots
├─ dashboard/                                         M5-S2  Quarto project: _quarto.yml · six .qmd pages · theme.scss
├─ tests/
│  ├─ unit/                                           M0-S1  grows every story; the ratchet and layer tests from M0-S5
│  ├─ fixtures/                                       M0-S3  hazard rows: 16 declared, synthetic (no real user data)
│  ├─ dags/                                           M0-S4  DAG integrity, run inside the Airflow image
│  └─ integration/                                    M0-S4  needs the stack; marked; run via make.ps1
├─ scripts/
│  ├─ doctor.ps1                                      M0-S4  port holders · Docker RAM vs budget (foreign stacks at their mem_limit) · path length
│  ├─ bench.py                                        M0-S5  named benchmarks on cached reference hours → automation/runs/m#-s#/bench.json
│  ├─ redteam/                                        M1-S4  one planting script per gate; disposable copies only
│  ├─ dashboard_check.py                              M5-S2  every number on the pages read back from the marts; an empty chart fails
│  └─ demo_acceptance.py                              M5-S4  end-to-end gate → automation/runs/
├─ automation/runs/                                   M0-S1  evidence JSON, tracked from the first story (pattern-based ignores)
└─ docs/
   ├─ MILESTONES.md                                   M0-S1  sections 1 and 2 of this plan (CI parses it)
   ├─ ITERATION.md                                    M0-S5  the iteration model (section 3 of this plan), for readers of the repo
   ├─ gotchas.md                                      M0-S1  GOTCHA_SEED classes A–D and F–I, plus the traps found in scoping
   ├─ adr/                                            M0-S1  one file per decision
   ├─ data/gharchive.md                               M0-S2  measured source facts and attribution; every number re-derived by commands/profile.py
   ├─ RUNBOOK.md · FAILURE_DRILLS.md                  M2-S3
   ├─ KPIS.md                                         M5-S1  KPI id · formula · source · window · owner; a changed formula gets a new id
   ├─ architecture.md                                 M0-S1  the blueprint (Mermaid, rendered by GitHub); refreshed in every Refine story
   └─ img/                                            M0-S1  architecture.svg; dashboard and Grafana screenshots added in M5-S4
```

**Never in the repo**
- GH Archive data in any form: raw `.gz`, bronze or stream Parquet, the DuckDB file, and the
  benchmark's cached reference hours. Data lives in Docker volumes or the user cache directory;
  the archive itself is the backup, and each bench record pins its inputs by sha256.
- Real GitHub user data, fixtures included: the hazard rows are synthetic.
- `.env` and any token or credential.
- A handoff file: the draft PR body is the handoff.
- The improvement backlog: it is GitHub Issues labelled `refine`, never a file (protocol: no
  ledgers in the repo).
- `*.log` and run-status files: only `automation/runs/**/*.json` evidence is tracked.
- `.venv/`, `__pycache__/`, `.idea/`, `.vscode/`, `.claude/`; dbt `target/`, `logs/`, `dbt_packages/`.
- Docker volumes; Redpanda, ClickHouse and Grafana state; transcripts.
- The rendered dashboard (`_site/`, `.quarto/`): Airflow rebuilds it from the marts.

---

## The stories

Each story is one branch → one PR titled `M#-S#: <title>` → one merge commit, in this order, one
open at a time; the branch is deleted on merge. Nothing else becomes a PR. Every commit ends with a
`Role:` trailer before the Co-Authored-By line, and every PR carries the matching label:
`role:data engineer` for M0–M4, `role:data analyst` for M5 (olist's convention: role labels, no
discipline labels). A defect found after a
merge is fixed in the next story's PR under "Also fixes". Each milestone closes inside its last
story, whose merge commit carries the release tag.

### M0 — Foundations

### M0-S1: Repo skeleton, plan and CI guards
Branch `feat/m0-s1-repo-skeleton` · Role: data engineer
Accept when:
- CI is green on the PR: story-lint, layout-lint, ruff, mypy, pytest.
- A planted bad branch name fails story-lint with `BRANCH-NAME`, and a planted undeclared file
  fails layout-lint with `UNDECLARED-PATH` (red-teamed, then reverted).
- The pre-push hook refuses a push to `main` and is tracked as `100755` (a test asserts it);
  `.\make.ps1 check` passes locally.
- `git check-ignore -v` proves, in both directions, that `automation/runs/**/*.json` is tracked
  and `*.log` is not.
- `docs/architecture.md` renders the blueprint as a diagram on GitHub, and
  `docs/img/architecture.svg` shows the same components.

### M0-S2: Source profile, re-derived by script
Branch `feat/m0-s2-source-profile` · Role: data engineer
Accept when:
- `.\make.ps1 profile` re-derives every number in `docs/data/gharchive.md` (daily sizes from
  Aug 3, the 111 near-empty hours, the sampled hours' event mix) into
  `automation/runs/m0-s2/profile.json`, and a test fails on any number in the doc that the record
  does not hold.
- A planted timeout is recorded as "request failed", never as "not published".
- The dates GitHub trimmed the PushEvent and `pull_request` payloads are found by bisection,
  each recorded with the hours on either side.

### M0-S3: Event contract and hazard rows
Branch `feat/m0-s3-event-contract` · Role: data engineer
Accept when:
- `contract.py` is the only module that casts a source field (an AST test fails on a second
  cast site).
- Each of the 16 declared hazard rows produces its declared outcome: accepted, counted under a
  named rule, or refusing the hour.
- An unknown or missing top-level key refuses the hour, writes nothing and exits 1; the
  exit-code vocabulary (0 passed · 1 refused · 2 could not build · 3 no verdict · 4 crashed) is
  defined once.
- Payload fields carry their era (`from`/`until`, set from M0-S2's dates) in the shape itself,
  with no date branching in code.

### M0-S4: Platform core: SeaweedFS, Postgres, Airflow and doctor
Branch `feat/m0-s4-platform-core` · Role: data engineer
Accept when:
- The plan revision lands first: `docs/MILESTONES.md` matches this file, the PR
  template has "Also refines", `CLAUDE.md` records the 2026-10-05 decisions, and the blueprint
  (`architecture.md`, `architecture.svg`, the hygiene test's component list) names SeaweedFS.
- `.\make.ps1 doctor` names the holder of any busy port, counts foreign stacks at their
  `mem_limit`, and refuses while they leave less than this project's budget (red team: olist up).
  If olist is running it is stopped with `docker compose stop` (volumes kept); the PR records the
  stop and the restart command.
- `.\make.ps1 up` brings every service to healthy; an integration test round-trips an object
  through the SeaweedFS S3 API (pyarrow) and a row through the `ghflow_ops` database.
- DAG integrity runs in CI inside the built Airflow image.
- ADR-0001 records every image pinned by tag and digest, checked live and dated, each
  component's backup obligation in both directions, and why MinIO was dropped.

### M0-S5: Refine M0: cohesion, readability and a performance baseline
Branch `feat/m0-s5-refine-foundations` · Role: data engineer · label `refine` · **Release `v0.0.0`**
Accept when (plus the common refine checks, docs/ITERATION.md):
- The iteration machinery exists: `docs/ITERATION.md`, the `refine` issue form and label; every
  improvement noticed in M0-S1–S4 is an issue, triaged into this story or re-targeted.
- One JSON-line reader serves `profile` and `contract` (an AST test refuses a second parsing loop
  over raw lines); every CLI exits through `guarded`; every knob lives in `settings.py` (a test
  refuses a knob-named constant anywhere else).
- The readability ratchet is set: ruff complexity and size rules on, `contract.apply` (complexity
  13 today) and every other function under the new floors, which live in one test.
- Cohesion is enforced: import-linter layers in CI keep the pure core (settings, eras, rules)
  free of I/O; HTTP, files and CLIs live in the shell.
- `scripts/bench.py contract` records the baseline on two cached reference hours (2026-10-05
  scoping: 1.67 s median for 2026-10-02-15); a parser optimisation is kept only if it meets its
  committed prediction and the contract's output hashes are identical.
- Chores: CI actions off the deprecated Node 20 runtime; pins re-verified with the lock diff;
  `make.ps1 help` aligned.

### M1 — Batch path and real-defect checks

### M1-S1: Hourly ingest into bronze
Branch `feat/m1-s1-hourly-ingest` · Role: data engineer
Accept when:
- One live hour runs end to end: the sensor waits for publication, raw `.gz` lands in S3,
  projected bronze Parquet and the rejected-row sidecar are written, and the audit row holds
  ETag, length, Last-Modified and verdict.
- Rows in = bronze rows + sidecar rows for that hour, asserted.
- Re-running the hour rewrites a byte-identical bronze file (sha256 equal) and adds an audit
  row, not a second partition.
- A planted structure change refuses the hour: no bronze file, a refusal audit row, exit code 1.

### M1-S2: Backfill from Aug 3, with park, resume and watch
Branch `feat/m1-s2-backfill` · Role: data engineer
Accept when:
- Every hour from 2026-08-03 00:00 to the backfill end has exactly one final verdict (success
  or refusal); the expected-hours count equals the verdict count.
- `park` mid-backfill lets in-flight hours finish and says whether it is safe to walk away;
  `resume` restarts, and `watch` shows progress within 10 minutes; no hour is duplicated or
  skipped across the break.
- Raw retention's dry run lists exactly the `.gz` files older than 14 days, then the real run
  deletes only those; bronze is untouched.
- `automation/runs/m1-s2/backfill.json` holds hours, bytes, rows, refusals and wall time.

### M1-S3: Silver on DuckDB with drift-aware contracts
Branch `feat/m1-s3-silver` · Role: data engineer
Accept when:
- `dbt build` is green: typed silver per event type, contracts enforced, dbt parse in CI.
- Per hour, silver rows = bronze rows over the whole window (zero difference).
- A field GitHub dropped (e.g. PushEvent `size`) is NULL because of its era, not by accident: a
  planted value in the wrong era fails a test naming the field and the era.

### M1-S4: Event-mix quality checks, proven on the real broken hours
Branch `feat/m1-s4-quality` · Role: data engineer
Accept when:
- The bars are argued from the clean baseline (Aug 3–16, two full weeks) and committed with their
  argument; the prediction of which hours get flagged, including what must stay unflagged, is
  committed after the bars and before the run. The order is checked from git.
- The run is judged against the prediction, and every disagreement is explained in
  `automation/runs/m1-s4/`.
- A row-count-only control passes Sep 5 15:00, reproducing the finding that counts alone miss it.
- Red team: a clean hour planted with a broken hour's event mix goes red naming the event type;
  every other hour keeps its verdict.
- A planted ETag change, in a copy of the audit table, schedules exactly that hour for re-ingest.

### M1-S5: Refine M1: faster ingest, thinner DAGs, same bytes
Branch `feat/m1-s5-refine-batch` · Role: data engineer · label `refine` · **Release `v0.1.0`**
Accept when (plus the common refine checks, docs/ITERATION.md):
- Ingest throughput for a fixed one-day backfill (24 hours) is measured before and after, against
  a committed prediction, in `automation/runs/m1-s5/bench.json`.
- Bronze Parquet is byte-identical (sha256) before and after on the reference hours; dbt row
  counts and test results are unchanged.
- DAG files stay thin: an AST test refuses parsing, Parquet or SQL logic inside `airflow/dags/`
  (it belongs in `src/ghflow`).

### M2 — Replayed stream

### M2-S1: Redpanda, Schema Registry and the replayer
Branch `feat/m2-s1-replayer` · Role: data engineer
Accept when:
- Topics apply idempotently from `config/redpanda/` (`gharchive.events`: 6 partitions keyed by
  repo id, 7-day retention).
- One bronze hour publishes exactly its row count plus one end-of-hour marker per partition,
  confirmed from delivery reports; the checkpoint moves only after the flush.
- The registry refuses a planted incompatible schema (BACKWARD), and the serializer refuses a
  record missing a required field under its rule name; CI checks the schema is compatible with
  its previous version.
- Killed mid-hour and restarted, the replayer republishes the hour: duplicates counted, nothing
  lost.

### M2-S2: Lander: commit after write, compact small files
Branch `feat/m2-s2-lander` · Role: data engineer
Accept when:
- Files are named by (partition, first offset, last offset); re-landing a range overwrites the
  same file.
- Killed mid-batch and restarted, the landed file set covers every consumed offset exactly once.
- An hour closes only when committed offsets pass its markers on all 6 partitions; compaction
  runs only on closed hours, and its output is content-equal to its inputs.
- Consumer lag per partition shows in Console and in `.\make.ps1 watch`.

### M2-S3: Batch-vs-stream reconciliation and failure drills
Branch `feat/m2-s3-reconcile-drills` · Role: data engineer
Accept when:
- For every hour from Aug 3, the stream's event-id set equals the batch's: zero missing, zero
  extra; duplicates reported separately.
- Red team: one message dropped in a disposable run turns reconciliation red naming the hour and
  the event id; every other hour stays green.
- Killing the lander, killing the replayer and restarting the broker each recover with zero loss;
  evidence JSON is under `automation/runs/m2-s3/`; RUNBOOK and FAILURE_DRILLS are written.

### M2-S4: Refine M2: stream throughput and one client configuration
Branch `feat/m2-s4-refine-stream` · Role: data engineer · label `refine` · **Release `v0.2.0`**
Accept when (plus the common refine checks, docs/ITERATION.md):
- Replayer and lander throughput (events/s) are measured before and after any batching or linger
  change, against a committed prediction.
- Reconciliation still shows zero differences over the whole window; compaction output is still
  content-equal to its inputs.
- One Kafka/Avro client configuration module serves the replayer and the lander (and the
  repo-state processor later); a test refuses client settings defined anywhere else.

### M3 — Data that updates

### M3-S1: SCD2 repo and actor history from the event log
Branch `feat/m3-s1-scd2` · Role: data engineer
Accept when:
- The number of repos and actors with more than one name in the window is measured and recorded
  first.
- SCD2 invariants hold: one current row per id, no overlapping validity, `valid_from` strictly
  increasing per id.
- Re-processing an earlier hour after later ones yields a dimension equal, row for row, to a full
  rebuild.

### M3-S2: Compacted repo-state topic with transactional state
Branch `feat/m3-s2-repo-state` · Role: data engineer
Accept when:
- The processor consumes events and writes `gharchive.repo_state` (compacted) in Kafka
  transactions together with its consumed offsets.
- Killed and restarted, it rebuilds its store from the changelog and emits no spurious change
  records.
- The latest value per repo in the topic equals `dim_repo`'s current rows; a planted stale record
  is named by the diff.

### M3-S3: Refine M3: cheaper history, faster restore
Branch `feat/m3-s3-refine-history` · Role: data engineer · label `refine` · **Release `v0.3.0`**
Accept when (plus the common refine checks, docs/ITERATION.md):
- The incremental SCD2 run time and the repo-state restore time are measured before and after,
  against committed predictions.
- `dim_repo` and `dim_actor` are equal row for row before and after; the compacted topic still
  agrees with `dim_repo` with zero differences.

### M4 — ClickHouse

### M4-S1: ClickHouse consumes the topic
Branch `feat/m4-s1-clickhouse` · Role: data engineer
Accept when:
- The Kafka engine (AvroConfluent, its own consumer group) feeds `events` (ReplacingMergeTree by
  event id) and hourly aggregates through materialized views; markers are filtered as data.
- Per hour, distinct event ids agree across batch, stream and ClickHouse — three witnesses.
- Killing ClickHouse mid-consumption loses nothing; duplicates collapse under `FINAL`; the views'
  aggregates equal a recomputation from `events`; `repo_current` equals `dim_repo`'s current rows.

### M4-S2: Grafana ops dashboards
Branch `feat/m4-s2-grafana-ops` · Role: data engineer
Accept when:
- Dashboards provisioned as code over ClickHouse show ingest freshness, consumer lag per group,
  reconciliation status and flagged hours; the gate runs every panel's query and requires rows.
- A planted stopped lander shows rising lag on its panel within 5 minutes, while the other
  consumer groups' panels stay flat.

### M4-S3: Refine M4: table design and panel latency
Branch `feat/m4-s3-refine-realtime` · Role: data engineer · label `refine` · **Release `v0.4.0`**
Accept when (plus the common refine checks, docs/ITERATION.md):
- Every Grafana panel query's median latency (N runs) is measured before and after any change to
  ordering keys, partitions or materialized views, against a committed prediction.
- The three witnesses still agree for every hour; view aggregates still equal a recomputation.

### M5 — Stakeholder story

### M5-S1: Insight marts and the KPI register
Branch `feat/m5-s1-insight-marts` · Role: data analyst
Accept when:
- Each insight is one dbt mart with a KPI id in `docs/KPIS.md` (formula, source, window, owner);
  a test fails on a mart without an entry, or an entry without a mart.
- Every KPI handles DQ-flagged hours through the same verdict table the checks write; there is no
  second definition of "broken".
- Each mart's headline number is recomputed from silver by an independent twin query and matches.
- The marts export as Parquet snapshots, and the export refuses if any mart is empty.

### M5-S2: Quarto stakeholder dashboard
Branch `feat/m5-s2-quarto-dashboard` · Role: data analyst
Accept when:
- Six pages, in plain language: The gist · Trends · When the world codes · What's rising · Bots
  and people · Can we trust it? Each finding sits beside an interactive chart (hover, zoom,
  filters). It is served at http://localhost:33001 and also built as a self-contained HTML bundle
  that opens without the stack.
- `dashboard_check.py` reads every number in the page text back from the marts (precision floor,
  anchored) and fails on any empty chart; a red team that alters one number, or drops the
  shading of flagged hours, fails naming it.
- No individual person is ranked: a test fails if a page names a human account (repos, orgs
  and bots may be named).
- `dashboard_render` rebuilds it after each daily build and refuses when the marts are older
  than that build.

### M5-S3: Refine the whole system
Branch `feat/m5-s3-refine-system` · Role: data analyst · label `refine`
Accept when (plus the common refine checks, docs/ITERATION.md):
- Every open `refine` issue is closed, or carried to the post-v1 list with a written reason.
- Cross-milestone cohesion: no duplicated helper across packages, no unused public function
  (checked by tool, with a reviewed allowlist), every module within the size budget.
- End-to-end latency (an hour's publication → visible on the dashboard) and the dashboard's
  render time and bundle size are measured before and after, against committed predictions.

### M5-S4: One-command demo and front door
Branch `feat/m5-s4-demo` · Role: data analyst · **Release `v1.0.0`**
Accept when:
- From a clean clone, `.\make.ps1 demo` runs a bounded window end to end (one clean day and one
  day with broken hours), renders the dashboard, passes `demo_acceptance.py` and commits its
  evidence JSON.
- The README shows the architecture and dashboard screenshots, links the gotchas, and a
  "How it got better" table of every Refine story's before/after numbers, rendered from their
  bench records; a checker confirms its commands and numbers.
- A checker fails if any compose service or DAG is missing from the architecture diagram.

After `v1.0.0`, further iterations are new milestones (M6 → `v1.1.0`, …) planned from the open
`refine` issues; each is a re-plan you approve.
