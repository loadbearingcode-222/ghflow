"""The rolling clean baseline and its checks, on synthetic hour counts (no warehouse)."""

from __future__ import annotations

from ghflow.hours import Hour, hours_between
from ghflow.quality.baseline import HourCounts, reference
from ghflow.quality.checks import judge, judge_all
from ghflow.quality.etag import republished

NORMAL = {"PushEvent": 950, "CreateEvent": 50}


def day_of(
    first: str, days: int, at: int = 12, types: dict[str, int] | None = None
) -> list[HourCounts]:
    start = Hour.parse(first)
    return [HourCounts(start.plus(24 * d + at), types or NORMAL) for d in range(days)]


def test_a_seed_hour_is_judged_against_the_other_seed_days_at_its_hour() -> None:
    history = {c.hour: c for c in day_of("2026-08-03-0", 5)}
    seed = set(history)
    ref = reference(Hour.parse("2026-08-05-12"), history, clean=set(), seed=seed)
    assert [r.hour.key for r in ref] == [
        "2026-08-03-12",
        "2026-08-04-12",
        "2026-08-06-12",
        "2026-08-07-12",
    ]


def test_a_later_hour_uses_only_clean_earlier_days_at_its_hour() -> None:
    counts = day_of("2026-09-01-0", 10)
    history = {c.hour: c for c in counts}
    target = counts[-1].hour
    clean = {c.hour for c in counts[:-1]} - {counts[-2].hour}  # the day before was flagged
    ref = reference(target, history, clean, seed=set())
    assert counts[-2].hour not in {r.hour for r in ref}
    assert len(ref) == 7  # QUALITY_REFERENCE_SIZE, walking back past the flagged day


def test_too_few_clean_references_gives_no_verdict() -> None:
    counts = day_of("2026-09-01-0", 3)
    verdict = judge(counts[-1], counts[:2])
    assert verdict.status == "no_verdict" and verdict.findings == []


def test_a_near_empty_hour_is_flagged_by_volume() -> None:
    ref = day_of("2026-09-01-0", 7)
    tiny = HourCounts(Hour.parse("2026-09-08-12"), {"PushEvent": 19, "CreateEvent": 1})
    [finding] = judge(tiny, ref).findings
    assert (finding.check, finding.subject) == ("volume", "rows")


def test_a_broken_mix_at_a_normal_count_names_the_event_type() -> None:
    ref = day_of("2026-09-01-0", 7)
    broken = HourCounts(Hour.parse("2026-09-08-12"), {"PushEvent": 140, "CreateEvent": 860})
    verdict = judge(broken, ref)
    assert verdict.status == "flagged"
    assert {f.subject for f in verdict.findings} >= {"PushEvent"}
    assert all(f.check == "mix" for f in verdict.findings), "the row count is normal"
    assert judge(broken, ref, checks={"volume"}).status == "clean", "counts alone miss it"


def test_rare_types_are_not_judged_by_share() -> None:
    ref = day_of("2026-09-01-0", 7, types={"PushEvent": 990, "WatchEvent": 10})
    odd = HourCounts(Hour.parse("2026-09-08-12"), {"PushEvent": 990, "WatchEvent": 40})
    assert judge(odd, ref).status == "clean"  # WatchEvent is 1% of the reference: too noisy


def test_a_flagged_hour_does_not_become_the_baseline() -> None:
    hours = hours_between(Hour.parse("2026-09-01-12"), Hour.parse("2026-09-20-12"))[::24]
    counts = [HourCounts(h, NORMAL) for h in hours[:7]]
    counts += [HourCounts(h, {"PushEvent": 10, "CreateEvent": 990}) for h in hours[7:]]
    verdicts = judge_all(counts, seed={c.hour for c in counts[:7]})
    assert [v.status for v in verdicts[:7]] == ["clean"] * 7
    assert all(v.status == "flagged" for v in verdicts[7:]), "an outage stays an outage"


def test_without_a_seed_nothing_can_become_clean() -> None:
    """Why the seed fortnight exists: a rolling clean baseline has to start somewhere."""
    counts = [
        HourCounts(h, NORMAL)
        for h in hours_between(Hour.parse("2026-09-01-12"), Hour.parse("2026-09-10-12"))[::24]
    ]
    assert {v.status for v in judge_all(counts, seed=set())} == {"no_verdict"}


def test_republished_hours_are_those_whose_etag_changed() -> None:
    a, b, c = (Hour.parse(f"2026-09-01-{n}") for n in (1, 2, 3))
    recorded = {a: '"x"', b: '"y"', c: '"z"'}
    assert republished(recorded, {a: '"x"', b: '"Y2"', c: None}) == [b]


def test_a_minor_type_moving_is_shifted_not_broken() -> None:
    ref = day_of(
        "2026-09-01-0", 7, types={"PushEvent": 800, "CreateEvent": 120, "PullRequestEvent": 80}
    )
    moved = HourCounts(
        Hour.parse("2026-09-08-12"), {"PushEvent": 800, "CreateEvent": 40, "PullRequestEvent": 160}
    )
    verdict = judge(moved, ref)
    assert verdict.status == "shifted"
    assert {f.subject for f in verdict.findings} == {"CreateEvent", "PullRequestEvent"}


def test_the_dominant_type_collapsing_is_broken() -> None:
    ref = day_of(
        "2026-09-01-0", 7, types={"PushEvent": 800, "CreateEvent": 120, "PullRequestEvent": 80}
    )
    broken = HourCounts(
        Hour.parse("2026-09-08-12"), {"PushEvent": 100, "CreateEvent": 500, "PullRequestEvent": 400}
    )
    assert judge(broken, ref).status == "flagged"


def test_a_shifted_hour_joins_the_baseline_a_flagged_one_does_not() -> None:
    hours = hours_between(Hour.parse("2026-09-01-12"), Hour.parse("2026-09-20-12"))[::24]
    normal = {"PushEvent": 800, "CreateEvent": 120, "PullRequestEvent": 80}
    regime = {"PushEvent": 800, "CreateEvent": 40, "PullRequestEvent": 160}
    counts = [HourCounts(h, normal) for h in hours[:7]] + [HourCounts(h, regime) for h in hours[7:]]
    statuses = [v.status for v in judge_all(counts, seed={c.hour for c in counts[:7]})]
    assert statuses[7] == "shifted", "the first hour of the new regime is recorded"
    assert statuses[-1] == "clean", "the new regime becomes the baseline instead of staying flagged"
