"""Judge a run against its committed prediction, claim by claim (M1-S4). Pure.

Each claim names hours (day + hour range, or a rule) and what their verdicts must be; the
result lists, per claim, whether it held and every hour that disagreed, so each disagreement can
be explained rather than averaged away.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ghflow.hours import Hour


def hours_of(claim: Mapping[str, Any], rows: Mapping[str, int]) -> list[str]:
    if claim.get("rule") == "every hour with fewer than 10,000 rows":
        return sorted((k for k, n in rows.items() if n < 10_000), key=Hour.parse)
    keys = []
    for span in claim.get("hours", []):
        first, last = span["hours"]
        keys += [f"{span['day']}-{h}" for h in range(first, last + 1)]
    return [k for k in keys if k in rows]


def _matches(claim: Mapping[str, Any], findings: list[dict[str, Any]] | None) -> bool:
    """Does one hour's verdict satisfy a 'flagged' claim (and its check / subject / only)?"""
    if not findings:
        return False
    relevant = [f for f in findings if f["check"] == claim.get("check", f["check"])]
    if claim.get("subject"):
        relevant = [f for f in relevant if f["subject"] == claim["subject"]]
    if claim.get("only"):
        return bool(relevant) and all(f["check"] == claim["only"] for f in findings)
    return bool(relevant)


def assess_claim(
    claim: Mapping[str, Any],
    verdicts: Mapping[str, Any],
    control: Mapping[str, Any],
    rows: Mapping[str, int],
) -> dict[str, Any]:
    flagged, control_flagged = verdicts["flagged"], control["flagged"]
    if claim["expect"] == "no_verdict_count":
        actual: Any = len(verdicts["no_verdict"])
        return {"id": claim["id"], "held": actual == claim["value"], "actual": actual}
    if claim["expect"] == "flagged_total_between":
        low, high = claim["value"]
        actual = len(flagged)
        return {"id": claim["id"], "held": low <= actual <= high, "actual": actual}
    keys = hours_of(claim, rows)
    if claim["expect"] == "flagged":
        good = [k for k in keys if _matches(claim, flagged.get(k))]
    elif claim.get("only_check"):
        good = [k for k in keys if k not in control_flagged]
    else:
        good = [k for k in keys if k not in flagged]
    share = len(good) / len(keys) if keys else 0.0
    return {
        "id": claim["id"],
        "held": bool(keys) and share >= claim.get("at_least", 1.0),
        "hours": len(keys),
        "as_predicted": len(good),
        "disagreeing": {k: flagged.get(k) or "clean" for k in keys if k not in good},
    }


def assess(
    prediction: Mapping[str, Any],
    verdicts: Mapping[str, Any],
    control: Mapping[str, Any],
    rows: Mapping[str, int],
) -> list[dict[str, Any]]:
    return [assess_claim(c, verdicts, control, rows) for c in prediction["claims"]]
