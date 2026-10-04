"""The exit-code vocabulary, defined once (doctrine §7).

    0  passed           the run produced its artifact
    1  refused          a rule said no; a record of why exists
    2  could not build  inputs or configuration were not usable
    3  no verdict       the run could not decide (e.g. nothing to judge)
    4  crashed          an exception nobody enumerated

A bare Python exception exits 1, which already means "refused" here — so every entry point runs
through `guarded`, which turns an unenumerated exception into 4.
"""

from __future__ import annotations

import enum
import sys
import traceback
from collections.abc import Callable


class ExitCode(enum.IntEnum):
    PASSED = 0
    REFUSED = 1
    COULD_NOT_BUILD = 2
    NO_VERDICT = 3
    CRASHED = 4


def guarded(main: Callable[[], int | ExitCode]) -> int:
    """Run an entry point; any exception it did not translate becomes CRASHED, never 1."""
    try:
        return int(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc(file=sys.stderr)
        print(
            f"CRASHED: exit {int(ExitCode.CRASHED)} (an exception nobody enumerated)",
            file=sys.stderr,
        )
        return int(ExitCode.CRASHED)
