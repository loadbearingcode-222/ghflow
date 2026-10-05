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
