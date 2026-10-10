"""Judge the one-command demo (M5-S4): every hour ingested, the broken day flagged and the clean
days not, three witnesses agreeing, every KPI matching its twin, the dashboard reading back, the
repo-state topic equal to dim_repo. Run in the stack:

    $GHFLOW_PYTHON scripts/demo_acceptance.py     # == python -m ghflow.commands.demo accept
"""

from __future__ import annotations

import sys

from ghflow.commands import demo

if __name__ == "__main__":
    sys.exit(demo.main(["accept"]))
