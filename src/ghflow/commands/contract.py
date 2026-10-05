"""Check GH Archive hours against the contract from the command line; write no data.

.\\make.ps1 contract-check   # the M0-S3 real hours, verdicts recorded as evidence
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ghflow.archive import open_hour
from ghflow.contract import HourResult, Refusal, apply
from ghflow.exitcodes import ExitCode, guarded
from ghflow.fields import TRIM
from ghflow.hours import Hour


def check(hour: Hour, file: Path | None) -> dict[str, Any]:
    """One hour's verdict: its report if it passed, the refusal's code and detail if not."""
    result: HourResult
    try:
        if file:
            with file.open("rb") as lines:
                result = apply(lines, hour)
        else:
            with open_hour(hour) as lines:
                result = apply(lines, hour)
    except Refusal as refusal:
        print(f"REFUSED {hour.key}: {refusal}", file=sys.stderr)
        return {
            "hour": hour.key,
            "verdict": "refused",
            "code": refusal.code,
            "detail": refusal.detail,
        }
    return {"verdict": "passed", **result.report()}


def write_record(path: Path, verdicts: list[dict[str, Any]]) -> None:
    record = {"story": "M0-S3", "contract_trim": TRIM.isoformat(), "hours": verdicts}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    """Check hours (streamed, or one local .json file) against the contract; write no data.

    With --record, the per-hour verdicts are written to that JSON file as evidence. A refused
    hour still produces no output data: the record is about the run, not the hour's rows.
    """
    parser = argparse.ArgumentParser(description=(main.__doc__ or "").strip().splitlines()[0])
    parser.add_argument("hours", nargs="+", help="GH Archive hour keys, e.g. 2026-10-02-15")
    parser.add_argument("--file", type=Path, help="read lines from this file (one hour only)")
    parser.add_argument("--record", type=Path, help="write per-hour verdicts here")
    args = parser.parse_args(argv)

    def run() -> int:
        if args.file and len(args.hours) != 1:
            print("--file takes exactly one hour", file=sys.stderr)
            return ExitCode.COULD_NOT_BUILD
        verdicts = [check(Hour.parse(key), args.file) for key in args.hours]
        refused = any(v["verdict"] == "refused" for v in verdicts)
        if args.record:
            write_record(args.record, verdicts)
        elif not refused:
            print(json.dumps(verdicts, indent=2))
        return ExitCode.REFUSED if refused else ExitCode.PASSED

    return guarded(run)


if __name__ == "__main__":
    sys.exit(main())
