# How ghflow is built: first pass, then refine

The first pass of each milestone aims at its goal. The **Refine story** at its end comes back
to what was built, and to anything earlier the new work exposed. It refactors, does the chores,
and improves performance, readability and cohesion. Refine stories carry the release tags, so
every release is a refined state, never a first draft. The plan of record is
[`MILESTONES.md`](MILESTONES.md).

## Capture, don't divert

While building, an improvement outside the story's scope becomes a GitHub issue labelled
`refine`. Use the form in `.github/ISSUE_TEMPLATE/refine.yml`, which asks for:

- where
- evidence
- proposed change
- expected gain and how it is measured
- target milestone

The issue is not acted on mid-story, so build stories stay focused and nothing is forgotten. The
backlog lives in GitHub Issues, never in a file in the repo.

## Which lane a finding takes

| The finding | Lane | When |
|---|---|---|
| A bug in merged work | "Also fixes" in the next story's PR | at once |
| A small improvement: no behaviour change, about 40 lines or fewer, already tested | "Also refines" in the next story's PR, closing its issue | at once |
| A bigger refactor, a performance gain, a chore, or a better design for earlier code | a `refine` issue, then the milestone's Refine story | at milestone end |
| An insight that changes an earlier **design**, a recorded decision, scope or deliverables | a **Realign** proposal, then a re-plan the owner approves (below) | when found |

## Common refine checks

Every Refine story's Accept-when includes these:

1. **Scope frozen at the start.** The draft PR lists the `refine` issues triaged into the story.
   Each one is either closed with evidence or re-targeted with a reason. Anything found during
   the refine goes to the next milestone's refine, so the scope does not creep.
2. **Behaviour preserved unless declared.** No test is weakened or deleted: the collected test
   ids after are a superset of before. Every refactored data path produces byte-identical
   output on the reference hours.
   - A **declared** behaviour change names the old and the new behaviour, and supersedes any ADR
     it reverses.
   - It re-runs the affected story's acceptance evidence.
   - It proves that the before/after diff contains exactly the intended change and nothing else.
3. **Performance is a measurement, not a claim.** A prediction is committed before the change.
   Then `scripts/bench.py` records before/after medians of at least 5 runs on the same cached
   input, with its sha256 in the record. Before and after run alternately in the same session,
   the "before" from a worktree of the base commit. A change is kept only if it **meets its
   committed prediction**, its after and before interquartile ranges do not overlap on every
   reference hour, and every output hash is identical; otherwise it is reverted. A missed
   prediction is recorded as missed, with why, and never re-set after the fact; a retry is a new
   `refine` issue with a new prediction drawn from the measurements. (M0-S5: orjson was 18–21%
   faster, short of its committed 28%, and was reverted.)
4. **Ratchets only tighten.** Complexity, size and import-layer limits live in `pyproject.toml`;
   their floors live in `tests/unit/test_laws.py`. A refine may tighten them. Loosening one needs
   an ADR.
5. **Chores done.**
   - Pins are re-verified live; the lock diff shows exactly what moved.
   - CI warnings are cleared.
   - The docs are refreshed: `architecture.md`, the README, and the commands in `CLAUDE.md`.

## Two levels of iteration

**Refine** is local and runs every milestone. **Realign** is global and runs only when a later
milestone changes the vision: when what M3 teaches means M1 and M2 should be *built*
differently, not just tidied. A realignment is never squeezed into a Refine story.

1. **Proposal, at once.** The story in flight is finished or parked cleanly, and nothing more is
   built on the old design. A draft ADR records:
   - what was learned, and the new design;
   - which milestones' code and data it touches;
   - its cost and its risk;
   - how the new design will be shown to be better.

   A `refine` issue labelled `architecture` tracks it.
2. **The owner decides:** adopt now, defer to a post-v1 iteration, or reject. Adopt now when
   later milestones would otherwise build on the old design (the rework only grows), or when
   correctness is at stake. Defer when it only polishes.
3. **A Realign story is inserted** in the current milestone, before its Refine story. Unstarted
   stories renumber; merged ones never change. A large realignment becomes its own milestone,
   inserted next. Its PR carries the `refine` and `architecture` labels.
4. **Executed safely, never big-bang:**
   - characterise the old behaviour first (golden outputs on the reference hours);
   - build the new design beside the old;
   - compare the two;
   - switch over, then delete the old path;
   - re-derive the affected data from the archive;
   - re-run every affected story's acceptance evidence;
   - supersede the ADRs it reverses, and refresh the blueprint and `MILESTONES.md`.
5. **The milestone's Refine story and release tag then mark the realigned state**, so the next
   milestone builds on the new design.

A realignment stays affordable for four reasons:
- Every artifact regenerates from GH Archive plus code.
- Each story's acceptance is committed as evidence that a later change must re-pass.
- The one-home laws and the core/shell layers keep a design change local.
- The batch-vs-stream reconciliation compares an old path with a new one for free.

## How readability and cohesion are measured

"More readable" should be a fact, not a feeling.

- **Readability:** ruff's complexity (C901), branch (PLR0912), argument (PLR0913) and statement
  (PLR0915) rules, plus a 300-line module budget, all ratcheted.
- **Cohesion:** import-linter layers (`pyproject.toml`, CI job `import-layers`):
  `commands` > `archive | contract | profiling` > `events | fields | exitcodes` > `hours` > `settings`.
  The pure core (`settings`, `hours`, `events`, `fields`, `contract`, `profiling`) does no I/O.
  HTTP, files and CLIs live in the shell (`archive`, `commands`).
- **The one-home laws,** each checked by AST with a red team in `tests/unit/test_laws.py`:
  - one cast: `contract.py`;
  - one JSON-line reader: `events.py`;
  - one home per knob: `settings.py`;
  - every CLI in `ghflow.commands`, exiting through `guarded()`.
- **Performance:** `scripts/bench.py` names each benchmark and writes its record per story under
  `automation/runs/m#-s#/`.
