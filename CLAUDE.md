PROTOCOL_MODE: learning

**Scope:** chartered work is a story from `docs/MILESTONES.md` (`M#-S#`) and gets the full
ritual: branch, draft PR, acceptance demo, evidence, handoff in the draft PR body, field note.
Ad-hoc questions in this repo get the knowledge here and none of the ceremony.

@~/.claude/templates/UNIVERSAL_PROTOCOL.md
@~/.claude/templates/ENGINEERING_DOCTRINE.md

Protocol version in force at kickoff: **2.18** (2026-10-04). Doctrine: 1.0.

# ghflow

GH Archive (every public GitHub event, one gzipped JSON file per hour) through two paths that must
agree to the event: a **batch path** (Airflow 3 → SeaweedFS (S3) bronze → dbt on DuckDB) where the data
interval is the watermark, and a **replayed stream** (Redpanda + Schema Registry → lander,
repo-state processor, ClickHouse) where committed offsets are. It ends in a Quarto dashboard for
non-technical readers. It is built iteratively: every milestone ends with a Refine story, and a
later insight that changes an earlier design becomes a Realign story (`docs/ITERATION.md`). Plan, layout and stories: `docs/MILESTONES.md`. Blueprint:
`docs/architecture.md`. Traps: `docs/gotchas.md`. Decisions: `docs/adr/`. Source facts:
`docs/data/gharchive.md` (from M0-S2).

## User decisions (verbatim, 2026-10-04)

- Data window: "From Aug 3 onward (Recommended)" → backfill from 2026-08-03 00:00 UTC.
- Mode: "Learning mode (Recommended)".
- Architecture: "Batch + replayed stream (Recommended)"; final add-on "ClickHouse
  (Recommended)". Re-asked once olist-lakehouse was found to cover Kafka, CDC, Spark streaming,
  Iceberg and SCD2: "Keep the plan you chose".
- Added at plan review: "I want to add another requirement for this project, that is the
  interactive dashboard throughout web browser, that display the important insights, findings,
  patterns and trending in this case. It more like a presentation for stake holder, non
  technical work people, to make them learn the gist from the data itself". Tool: "Quarto
  (Recommended)".
- At the second plan review the user asked for "the architecture of this project, graph,
  visualization, blueprint" → `docs/architecture.md` + `docs/img/architecture.svg` from M0-S1.
- Standing grant (approving the plan, protocol §7): Claude creates the private repo, opens AND
  merges its own story PR once CI is green and the story's Accept-when demo has been run and
  observed. NOT granted: deleting volumes (`down -v`), `docker compose down` on another stack,
  touching another repo — ask each time.
- Labels: roles only, as in olist: `role:data engineer` (M0–M4), `role:data analyst` (M5).

## User decisions (verbatim, 2026-10-05) — plan revision 2

- The request: "before proceed to more proceed, I want to add iterative development for this
  project. As the first time we develop the project, I strive to attain the original goal but as
  we keep developing the project, suddenly there is still plenty room for improvement, so we come
  back to the previous work, make changes, chore and refactor code base, improve the performance
  and also increase the readability and cohesion. can you revise this plan again". Then, on
  cross-milestone change: "if somehow M3 as we keep making progress, suddenly we gain epiphany
  from the gods, now we not only simply refine M3 work anymore, but also the prior work, in this
  case for example M1, M2 so it will align with our new vision" → the Realign path.
- Cadence: "Refine story per milestone (Recommended)". Ideas log: "GitHub Issues, 'refine' label
  (Recommended)" — the backlog is issues labelled `refine` (realign proposals also `architecture`),
  never a file.
- S3 store: "SeaweedFS 4.48 (Recommended)" — MinIO images stopped being publicly pullable
  (quay.io 401, Docker Hub denied; checked 2026-10-04). ADR-0001.
- olist: "Yes, stop it (keep data)" → `docker compose stop` in olist-lakehouse when a story needs
  `up` (volumes kept; restart with `docker compose start` there). Never `down`.

## Environment facts (observed 2026-10-04)

- Windows 11, Docker Desktop (WSL2 backend), WSL2 capped at **28 GB** (~27.4 GiB visible to
  Docker). `make` is NOT installed: `make.ps1` is the task runner. PowerShell 5.1.
- The repo path contains a space; quote every path. State lives in named volumes; only code and
  config are bind-mounted (C: bind mounts are slow through the WSL bridge).
- Host Python is 3.9; the project uses uv-managed Python 3.12 (`.python-version`). Node 24 is on
  the host but nothing here depends on it.
- GitHub: the active `gh` account is a different one. Use `loadbearingcode-222` per command
  without switching: `GH_TOKEN=$(gh auth token --user loadbearingcode-222)` (bash) or
  `$env:GH_TOKEN = gh auth token --user loadbearingcode-222` (PowerShell). The repo-local
  credential helper and `user.name`/`user.email` (329835776+loadbearingcode-222 noreply) are set;
  global git and gh config are untouched.
- Clones: this working copy is the only one; GitHub `origin` is refreshed by `git push` at every
  checkpoint. Nobody else writes to it.

## Fleet ledger (ports and RAM on this machine)

| Holder | Ports |
|---|---|
| native Postgres (Windows service) | 5432 |
| unknown listener (seen 2026-10-04) | 30000, 30001 |
| wikiflow / meridian | 5433, 8080, 9000, 9001, 8501 |
| wrenfield-retail | 8082, 5435, 3002, 5002, 8002 (+ 1xxxx prod) |
| olist-lakehouse | 2xxxx family; 20 GB of `mem_limit` |
| **ghflow (this repo): 3xxxx family** | Airflow 38082 · SeaweedFS S3 39000, admin UI 39001 · Postgres 35432 · Redpanda Kafka 39092 · Schema Registry 38081 · Console 38080 · ClickHouse HTTP 38123 · Grafana 33000 · dashboard 33001 |

RAM budget: at most **14 GB** of `mem_limit` in total. `doctor` counts foreign stacks at their
`mem_limit` (what they may take), not their live use. olist (20 GB of limits) cannot run
alongside; it is stopped with `docker compose stop`, volumes kept (granted 2026-10-05).

## Version pins (OBSERVED values, re-verify live before relying on them)

Observed at planning, 2026-10-04. Each is re-verified, and pinned by tag AND digest, in the story
that adopts it (ADR-0001 onward).

| Component | Observed | Adopted in |
|---|---|---|
| Python / uv | 3.12.13 / 0.11.25 (host); uv 0.12.23 in the Airflow image (re-verified 2026-10-05) | M0-S1, M0-S5 |
| Airflow | `apache/airflow:3.3.2-python3.12` | M0-S4 |
| SeaweedFS / Postgres | `chrislusf/seaweedfs:4.48` / `postgres:17.11-bookworm` (MinIO dropped: unpullable) | M0-S4 |
| duckdb / dbt-core / dbt-duckdb | 1.5.6 / 1.12.5 / 1.11.0 | M1-S3 |
| Redpanda / Console | v26.2.3 / v3.12.0 | M2-S1 |
| confluent-kafka / fastavro | 2.15.1 / 1.12.2 | M2-S1 |
| ClickHouse / clickhouse-connect | 26.3 LTS (26.3.41.4) / 1.9.0 | M4-S1 |
| Grafana | 13.2.3 | M4-S2 |
| Quarto | 1.10.18 (stable; 1.11 pre-release) | M5-S2 |

## Commands

```
.\make.ps1 help        # every target
.\make.ps1 hooks       # once per clone: core.hooksPath = .githooks (refuses pushes to main)
.\make.ps1 check       # sync + lint + types + unit tests (what CI's quality job runs)
.\make.ps1 profile     # network, ~2 min: re-measure GH Archive -> automation/runs/m0-s2/profile.json + doc
.\make.ps1 profile-render  # no network: re-render docs/data/gharchive.md from the record
.\make.ps1 contract-check  # network: the contract over seven real hours -> automation/runs/m0-s3/
uv run lint-imports    # the core/shell layers (CI job import-layers)
uv run python scripts/bench.py contract --out automation/runs/<story>/bench.json  # cached reference hours
.\make.ps1 env         # .env with generated secrets (never rewrites an existing one)
.\make.ps1 doctor      # read-only preflight: port holders, RAM vs budget (foreign stacks at mem_limit)
.\make.ps1 up          # env + doctor + compose up --build --wait (~1 min)
.\make.ps1 down        # stop and remove containers; volumes kept
.\make.ps1 reset -Force  # down -v: deletes every volume
.\make.ps1 dags        # build the image's dag-test stage, run tests/dags in it with --network none
.\make.ps1 test-integration  # S3 round-trip, ghflow_ops round-trip, Airflow health (needs up)
```

Platform (M0-S4, ADR-0001): SeaweedFS S3 at http://127.0.0.1:39000 (buckets raw, bronze, stream,
marts; every request signed — pyarrow needs `force_virtual_addressing=False`, i.e. path style);
filer UI at :39001; Postgres at 127.0.0.1:35432 (`airflow` metadata, `ghflow_ops` owned by role
`ghflow`); Airflow at http://127.0.0.1:38082 (admin / `AIRFLOW_ADMIN_PASSWORD`). Project code runs
in the image's `/opt/ghflow-venv` (`GHFLOW_PYTHON`).

Source facts (`docs/data/gharchive.md`, record `automation/runs/m0-s2/profile.json`): the doc's
prose may not quote numbers; they render from the record (`tests/unit/test_profile_doc.py`).
GitHub trimmed PushEvent and `pull_request` payloads in ONE hour, 2025-10-08 17:00 UTC — before
the whole backfill window, so every hour ghflow ingests has the trimmed shape. Probes use
`ghflow.archive` (User-Agent set, 4 in flight, three verdicts: published / not published /
request failed).

Code layers (M0-S5, `docs/ITERATION.md`): pure core `settings`, `hours`, `events`, `fields`,
`contract`, `profiling` (no I/O); shell `archive`, `commands/` (every CLI through `guarded()`).
One cast (`contract.py`), one JSON-line reader (`events.py`), one home per knob (`settings.py`);
ruff floors complexity 8 / args 5 / branches 10 / statements 40, modules <= 300 lines — all in
`tests/unit/test_laws.py`, loosening needs an ADR.

## Conventions

- One story → branch `feat/m#-s#-<slug>` → draft PR `M#-S#: <title>` → merge commit, in plan
  order. CI refuses anything else (`.github/scripts/story_lint.py`, `layout_lint.py`).
- A new path is declared in the layout in `docs/MILESTONES.md` by the story that adds it — one
  entry per line (the layout grammar has no continuation lines; gotcha #37).
- Every commit ends with the `Role:` trailer, then the Co-Authored-By line; the PR carries the
  matching `role:` label.
- The draft PR body is the handoff; the story closes with a field note in the PR body: what was
  built, why this way, the concept, what to look at, what to try.
- `docs/architecture.md` is refreshed in every Refine story.
- **Findings take a lane:** a bug in merged work → "Also fixes" in the next PR; a ≤ ~40-line,
  behaviour-preserving, already-tested improvement → "Also refines" in the next PR; anything
  bigger → a `refine` issue for the milestone's Refine story; an insight that changes an earlier
  design → a Realign proposal (draft ADR + `architecture` issue) and a re-plan the owner approves.
  Never act on an out-of-scope improvement mid-story.
