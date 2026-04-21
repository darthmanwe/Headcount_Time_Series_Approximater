"""One-off inspection of headcount_estimate_monthly rows for the latest cohort run."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _latest_run() -> Path:
    return sorted((REPO / "data" / "runs" / "harmonic_live").glob("*"))[-1]


def main(argv: list[str]) -> int:
    run_dir = Path(argv[1]).resolve() if len(argv) > 1 else _latest_run()
    db = run_dir / "cohort.sqlite"
    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute(
        "SELECT method, confidence_band, COUNT(*) "
        "FROM headcount_estimate_monthly GROUP BY method, confidence_band"
    )
    print("=== method x band counts ===")
    for r in cur.fetchall():
        print(r)

    print("\n=== Latest non-zero estimate per company ===")
    cur.execute(
        "SELECT c.canonical_name, e.month_start, e.estimated_headcount, "
        "e.method, e.confidence_band, e.confidence_score "
        "FROM headcount_estimate_monthly e "
        "JOIN companies c ON c.id = e.company_id "
        "WHERE e.estimated_headcount > 0 "
        "AND e.month_start = ("
        "  SELECT MAX(month_start) FROM headcount_estimate_monthly "
        "  WHERE company_id = e.company_id AND estimated_headcount > 0"
        ") ORDER BY c.canonical_name"
    )
    for r in cur.fetchall():
        print(r)

    print("\n=== Distinct EstimateMethod values found in latest run ===")
    cur.execute(
        "SELECT DISTINCT method FROM headcount_estimate_monthly "
        "WHERE company_id IN (SELECT company_id FROM headcount_estimate_monthly "
        "GROUP BY company_id HAVING MAX(estimated_headcount) > 0)"
    )
    for r in cur.fetchall():
        print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
