"""Read every number on the built stakeholder dashboard back from the marts (M5-S2, ADR-0013).

    uv run python scripts/dashboard_check.py --site <build>/_site     # or, in the stack:
    $GHFLOW_PYTHON -m ghflow.commands.dashboard check --site <build>/_site

Fails, naming the page, on a number that does not read back, a digit outside an anchored span,
an empty chart, a missing flagged-day shade, or a name that is not a ranked repo or bot.
"""

from __future__ import annotations

import sys

from ghflow.commands import dashboard

if __name__ == "__main__":
    sys.exit(dashboard.main(["check", *sys.argv[1:]]))
