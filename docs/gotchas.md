# Gotchas — traps, with the ten-second check

Format: **symptom → real cause → ten-second check.** Silent traps first.

Entries above the seed line were paid for by earlier projects on this machine
(`~/.claude/templates/GOTCHA_SEED.md` classes A–D and F–I, wrenfield-retail, wikiflow,
olist-lakehouse). Provenance is in brackets. Entries below it are earned by THIS project.

## Seeded

### Environment
1. **Containers OOM or flap despite plenty of host RAM.** WSL2 has a fixed share (28 GB on this
   machine since 2026-09-26). *Check:* `docker info --format "{{.MemTotal}}"`; the stack's
   `mem_limit` total (14 GB here) must leave headroom for other stacks. [seed A2]
2. **Ports already taken at `up`.** Another project's stack, or the native Postgres on 5432.
   *Check:* `.\make.ps1 doctor` resolves the HOLDER of each port; ours vs foreign are different
   outcomes. [seed A5]
3. **`/bin/bash^M: bad interpreter` inside a container.** CRLF endings from a Windows checkout.
   *Check:* `.gitattributes` forces LF for everything except `*.ps1`. [seed A6]
4. **A PowerShell script breaks on an em-dash or writes UTF-16.** PowerShell 5.1 reads BOM-less
   UTF-8 as cp1252 and writes UTF-16 by default. *Check:* `.ps1` files stay ASCII-only; pass
   `-Encoding utf8` when writing files other tools read. [wrenfield #5]
5. **Paths break at "data engineer".** The repo path contains a space. *Check:* quote every
   path; ruff `PTH` keeps Python on `pathlib`. [wikiflow ADR-0004]
6. **`ImportError: DLL load failed ... The filename or extension is too long`.** confluent-kafka's
   bundled DLLs sit ~100 characters below the repo root and Windows refuses a DLL path at
   MAX_PATH (260). *Check:* the repo path length; `doctor` refuses above 150. [olist #41]

### Shell and parsers
7. **Backticks or `$` in an unquoted heredoc run as commands.** *Check:* quote the delimiter
   (`<<'EOF'`) unless expansion is wanted. [seed B10]
8. **Run parameters spliced into shell text.** *Check:* pass them through env vars, never
   interpolate them into a command string (CI does this for PR titles). [wrenfield #9]
9. **A PowerShell helper named like a tool recurses forever.** Command names are
   case-insensitive, so a function `uv` shadows `uv.exe`. *Check:* verb-noun helper names
   (`Invoke-Uv`). [olist #22]
10. **`docker ps --format '{{.Label "x"}}'` returns nothing from PowerShell 5.1.** It strips the
    inner quotes. *Check:* use `{{.Labels}}` and parse `key=value`. [olist #23]
11. **`docker compose exec kafka /opt/kafka/bin/...` fails with "C:/Program Files/Git/opt/..."**
    Git Bash rewrites arguments that look like POSIX paths. *Check:* `export MSYS_NO_PATHCONV=1`
    first, or run the command from PowerShell. [olist #28]

### Git
12. **A `!` re-include in `.gitignore` is silently inert.** A bare directory exclusion stops git
    descending. *Check:* `git check-ignore -v --no-index <path>` in both directions (pinned by
    `tests/unit/test_repo_hygiene.py` for `automation/runs/**/*.json`). [seed C14]
13. **A hook that is not executable is skipped in silence.** *Check:* `git ls-files -s .githooks`
    shows `100755` (pinned by test). [seed C15/C18]
14. **A sha256 pin and `eol=lf` normalisation fight.** *Check:* never pin the hash of a file git
    normalises; GH Archive data is never committed at all. [seed C17]
15. **The PR checks you watched were from the wrong run.** After `gh pr edit`, `gh pr checks
    --watch` can show the `edited` run, where every job but story-lint is skipped by design.
    *Check:* confirm the full suite on the head commit before calling it green. [olist #31]

### Packaging
16. **A dependency add moved a major version somewhere else.** *Check:* diff `uv.lock` after
    every dependency change; an unexpected moved package is a finding. [seed D20]

### Containers and storage
17. **A storage 403 that reads like a wrong password.** Often a missing LIST permission on the
    bucket. *Check:* the policy includes `s3:ListBucket`. [seed E43]
18. **A `chown -R` at the end of a Dockerfile doubles the image.** *Check:* create the user
    before installing anything. [seed E34]
19. **A named volume mounted into Airflow is root-owned, and the first write fails with an
    error that looks like something else.** *Check:* the init service chowns the exact mount
    target. [wrenfield CLAUDE.md]
20. **Every connection from the Windows host takes seconds, or hangs.** `localhost` resolves to
    `::1` first and the stack publishes on `127.0.0.1` only. *Check:* host-side clients use
    `127.0.0.1`. [olist #27]
21. **`compose up` never re-runs a one-shot that already exited 0**, so a second `up` did not
    exercise the bootstrap. *Check:* prove idempotence with `docker compose run --rm <one-shot>`.
    [olist #26]
22. **`docker compose up --wait` fails with "container … exited (0)" on a healthy stack.** A
    one-shot nobody depends on counts as failed once it exits. *Check:* every one-shot is named
    by some `depends_on: condition: service_completed_successfully`. [olist #29]
23. **A service restarts mid-run and the retry passes, so nobody notices.** A JVM or server sized
    as a percentage of its container is OOM-killed and `restart: unless-stopped` hides it.
    *Check:* `docker inspect <svc> --format '{{.RestartCount}}'`; set explicit memory caps well
    below `mem_limit`. [olist #39]

### Airflow 3
24. **A manual trigger sits `queued` forever after `down -v`.** Every DAG is paused again.
    *Check:* `select dag_id, is_paused from dag`. [wrenfield #1]
25. **A bare cron schedule has a zero-width data interval.** Airflow 3 defaults to
    `CronTriggerTimetable`. *Check:* period DAGs declare `CronDataIntervalTimetable`. [wrenfield #2]
26. **`DagBag.get_dag()` hits the metadata DB even in a parse test**, and
    `DagBag(include_examples=...)` is gone in 3.3. *Check:* tests read `bag.dags[...]`; examples
    follow `AIRFLOW__CORE__LOAD_EXAMPLES`. [wrenfield #6, olist #37]
27. **Backfill ignores the DAG's `max_active_runs`, and `--to-date D` is a `data_interval_start`.**
    *Check:* pass `--max-active-runs N` explicitly; compare stored hours with the expected list,
    never with a count. [wrenfield CLAUDE.md, olist #38]

### dbt
28. **Many models fail with "source and target schemas are out of sync" after one macro edit.**
    A column added in a shared macro changes every dependent model. *Check:* `dbt ls --select`
    the macro's dependents and full-refresh all of them. [olist #35]

### Verifiers
29. **A CLI exits 0 for a run that failed.** *Check:* assert on the artifact the run exists to
    produce, not the exit code. [seed F45]
30. **A guard that fires on correct behaviour.** *Check:* assert properties (a count that must
    be zero, an identity), never this month's values. [seed F50]

---
<!-- ======== SEED LINE: entries below are earned by THIS project ======== -->

## Earned here

31. **Python's `urllib` gets HTTP 403 from data.gharchive.org while `curl` gets 200.** Cloudflare
    refuses the default `Python-urllib/3.x` User-Agent. *Check:* send a descriptive
    `User-Agent`; `curl -sI` the same URL to tell a block from a missing file. (scoping)
32. **A size sweep reports dozens of "missing" hours that all exist.** 32 parallel HEAD requests
    produced ~1.7% timeouts, and a timeout was counted as "not published". *Check:* a missing
    verdict needs an actual 404; retry timeouts slowly and sequentially, and keep concurrency
    low (4). (scoping)
33. **An hour downloads fine (HTTP 200) and holds 2% of its events.** GH Archive served 111
    near-empty hours in Aug–Sep 2026 and never re-published them (`Last-Modified` = 5 minutes
    after the hour). *Check:* volume against a trailing baseline per hour, not "the file
    exists". (scoping)
34. **An hour has a normal row count and is still broken.** 2026-09-05 15:00 had 84,492 events
    (healthy: ~85k) but PushEvent was 14% of rows instead of ~70%. *Check:* the event-type mix,
    not only the total. (scoping)
35. **A metric silently becomes NULL from one date on.** GitHub trimmed event payloads:
    PushEvent lost `commits`/`size`/`distinct_size`; `pull_request` went from 48 keys to 5.
    *Check:* payload fields carry their era (`from`/`until`) in the contract. (scoping)
36. **A project inventory said "new to you" about tools the user had already shipped.** The
    desktop app's recent-folders list is not the list of projects. *Check:* `ls` every project
    root (`C:\Users\longt\data engineer`, `PycharmProjects`, `cloud platform`) before claiming
    novelty. (scoping)
37. **layout-lint refuses files in a directory the layout clearly opens.** The tree grammar is
    one entry per line: a wrapped comment parses as a deeper "child", which turns an open
    directory into a closed one. *Check:* every tree line in `docs/MILESTONES.md` starts a new
    entry; no continuation lines. (M0-S1)
38. **mypy passes on Windows and reports "Statement is unreachable" in CI.** mypy narrows
    `sys.platform` to the host it runs on, so a Windows-only branch is dead code on the Linux
    runner (with `warn_unreachable`). *Check:* branch on `os.name`, which mypy does not narrow,
    or run `mypy --platform linux` locally before pushing. (M0-S1)
39. **Real ForkEvents arrive with `"repo": {}`.** No id, no name — nothing to join or key the
    stream by. *Check:* the contract's `repo_malformed` count per hour (real hours show a
    handful); the rows are in the sidecar, never silently defaulted. (M0-S3)
40. **A stack that ran yesterday cannot be rebuilt today: `pull access denied` / `401` for
    MinIO.** MinIO's images stopped being publicly pullable (quay.io answers 401 for
    `minio/minio` and `minio/mc`; Docker Hub refuses `minio/minio`; checked 2026-10-04). Stacks
    that still "work" run from the local Docker cache and die at the next prune. *Check:*
    `docker buildx imagetools inspect <ref>` resolves every pinned image anonymously (ADR-0001);
    ghflow uses SeaweedFS. (M0-S4)
41. **A GitHub issue (or any file) comes out empty after a script "succeeded".** The host Python
    3.9 prints to a cp1252 console, so `print()` of `≤`, `≥` or `→` raises UnicodeEncodeError —
    and a shell pipeline redirecting that output carries on with an empty file. *Check:* write
    files with `encoding="utf-8"` instead of redirecting `print`, or run through `uv run` (Python
    3.12) with `PYTHONUTF8=1`; read back what you created (`gh issue view N --json body`). (M0-S4)
42. **A generated file breaks only after it reaches GitHub: "no checks reported", or a run that
    "likely failed because of a workflow file issue" with no jobs.** Backslash escapes did not
    survive the trip into generated text: `\n` inside a Python string became a real newline in
    `ci.yml` (GitHub could not parse it, so CI ran nothing), and `\U` in `C:\Users` was a Python
    SyntaxError. *Check:* write generated text with raw strings (`r"""…"""`) or no backslashes
    at all, and parse what you generated before pushing — `tests/unit/test_workflows.py` parses
    every workflow. (M0-S4)
43. **An hourly DAG ingests the hour that is still in progress.** In Airflow 3 a bare cron
    schedule (`"@hourly"`) is a *trigger* timetable: the run at 13:00 has a zero-length interval
    at 13:00, so `data_interval_start` renders hour 13, not the closed hour 12. A publication
    sensor hides it (it waits until hour 13 is published) and the run looks healthy, an hour
    late. *Check:* schedule with `CronDataIntervalTimetable("0 * * * *", timezone="UTC")` and
    render the templated hour of a real run (`airflow tasks render`) before trusting it;
    `tests/dags` asserts the timetable class. (M1-S1)
44. **A DAG run "succeeds" in milliseconds and did nothing.** In Airflow 3 a run whose logical
    date is before the DAG's `start_date` gets no task instances at all, and an empty run is
    marked `success`. A retention run triggered for yesterday's interval on a DAG that "starts
    today" deleted nothing and reported green. *Check:* set `start_date` at or before the
    earliest interval anyone may run (catchup off keeps it from backfilling), and confirm a run
    has task instances before trusting its state; `tests/dags` pins raw_retention's start. (M1-S2)
45. **DuckDB's `memory_limit` does not bound the process.** With `memory_limit` 1 GB (or 1.5 GB,
    2 or 4 threads), a `dbt build` loading 143M rows from 1,528 Parquet files over httpfs peaked at
    ~2.1 GiB RSS: the limit governs the buffer manager, not the readers' own allocations, and
    `docker stats` (which also counts page cache) cannot tell you which. *Check:* measure the
    process (`/proc/<pid>/status` VmRSS, or cgroup `memory.stat` anon vs file) under a real build
    and size the container from that. (M1-S3, ADR-0003)
46. **dbt-duckdb `delete+insert` on a composite `unique_key` crawls.** `unique_key=['event_date',
    'event_hour']` ran >10 minutes deleting one day of 4M rows; `unique_key='event_date'` takes
    seconds. *Check:* key incremental models on one column (re-read whole days if needed), and
    time the second (incremental) run, not just the first. (M1-S3)
47. **A DuckDB view breaks when its file is attached under another name.** dbt-duckdb views name
    their catalog after the database file (`ghflow`); `attach '…/ghflow.duckdb' as prod` gives
    `Catalog "ghflow" does not exist`. *Check:* attach the warehouse as `ghflow` (read-only) in
    any side tool. (M1-S3)
48. **Hours vanish while the machine is off, and nothing says so.** A live DAG with
    `catchup=False` runs only the newest interval when the scheduler comes back: the stack was
    down Oct 5 23:00 - Oct 10 01:00 and those 98 hours were never scheduled. Turning catchup on
    afterwards does not recover them (the scheduler's next-run pointer is already past the gap).
    *Check:* hourly ingest runs with `catchup=True`; fill an existing gap with a backfill
    (`reprocess_behavior=none` creates only the missing runs), then `dbt_build` with
    `full_refresh`, because the incremental model only re-reads its newest 2 days. (M1-S4)
49. **A data test races the pipeline it checks.** `silver_rows_equal_bronze_rows` counted bronze
    *after* the staging load, so an hour ingested while `dbt_build` ran was in bronze but not in
    silver: two scheduled builds (Oct 6, Oct 10) failed on exactly 1 hour, and nothing alerted.
    *Check:* a test that compares a source with a load compares the same snapshot (here: bronze
    up to the newest hour staging loaded), and red-team both directions: the source moving on
    must pass, a hole inside the loaded window must fail. (M1-S5)
50. **`docker kill` is a manual stop: `restart: unless-stopped` will not bring the container
    back.** The M2-S2 kill drill expected Docker to restart the lander like a crash; it stayed
    `exited`. *Check:* to drill a crash, kill the process inside the container (or restart it by
    hand and say so); don't assume a restart policy fired - read `docker inspect .RestartCount`.
    (M2-S2)
51. **A consumer that never closes keeps its group membership until the session times out.**
    The lander, stopped by SIGTERM without `consumer.close()`, stayed a group member for ~45 s;
    an offset rewind in that window did not take (the next start read the old commit). *Check:*
    close consumers on SIGTERM/SIGINT; before altering a group's offsets, confirm the group is
    empty and read the per-partition result for errors. (M2-S2)
52. **DuckDB's `memory_limit` does not bound string aggregate states, and `tmpfs` spill is RAM.**
    `any_value(repo_name)` over 13.4M groups ran a 3 GiB container out of memory under a 1 GB
    limit; the same names as *grouping keys* (`group by repo_id, repo_name`) spilled and peaked
    at 1.1 GiB. A first test also spilled to a `--tmpfs` mount, which counts against the
    container's memory, and to an in-memory database, which cannot spill at all. *Check:* put
    strings in the group key, not in an aggregate; spill to a disk-backed file; try a heavy
    query in a one-off container with its own `--memory`, never in the scheduler. (M3-S1)
53. **The same string-aggregate trap, one story later.** M3-S2's first `diff` used
    `arg_max(repo_name, off)` per repo and `list(repo_id)` per verdict over 13.7M rows and ran out
    of a 1 GB DuckDB limit, the day after gotcha #52 was written. *Check:* for "latest row per
    key", take `max(offset)` per key and join back; list ids only for the red verdicts, capped
    (`RECONCILE_NAMED_IDS`). Grep new SQL for `arg_max(`/`list(` over a string before running it
    on the full window. (M3-S2)
54. **Mounting a directory over ClickHouse's `config.d` removes the image's own settings.** The
    official image ships `config.d/docker_related_config.xml` (`listen_host ::`). A bind mount of
    the whole directory hid it: the server listened only inside the container, and every host
    request was dropped without a response. *Check:* mount single files into `config.d`, never
    the directory. (M4-S1)
55. **ClickHouse's Kafka engine inserts blocks of `max_insert_block_size` (~1M rows) by
    default.** A block of about 1M 35-column rows, pushed through three materialized views, hit a
    1.5 GiB server cap again and again: every insert failed and was retried. *Check:* set
    `kafka_max_block_size` (65,536 here) when the container is small, and read the server log
    for MEMORY_LIMIT_EXCEEDED during the first catch-up. (M4-S1)
56. **A check sized to the whole window fails silently once the window is loaded, and a wait
    loop built on it never ends.** M4-S1 polled `clickhouse status`, which ran `uniqExact(id)`
    over all of `events`. It worked below ~20M rows; with 147M rows it exceeded the 1.5 GiB cap.
    The loop read every failure as "not done yet" and waited about 3.5 hours after ClickHouse
    had finished. The same class of mistake was in the design itself: `events_hourly` kept
    `uniqExact` states, i.e. hash sets of every id (~1.2 GB), and their merges starved the
    server. `groupBitmap` (Roaring bitmaps) keeps the count exact at 275 MiB. *Check:*
    - exact-distinct checks run per day (or per hour) and are summed;
    - a wait loop fails loudly on an error and has a deadline set from the measured rate;
    - a transactional topic is "consumed" when nothing readable is left after the committed
      offset, not when committed equals the high watermark (the trailing commit marker is
      never committed past). (M4-S1)
