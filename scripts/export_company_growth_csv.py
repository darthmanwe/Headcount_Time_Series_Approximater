"""Export sample-style company growth CSV from the canonical database.

Output columns are aligned with the benchmark workbook shape:
- Company Name
- Headcount
- Headcount % (365d)
- Headcount % (180d)
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

from sqlalchemy import select

from headcount.db.engine import session_scope
from headcount.models.company import Company
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.serving.evidence import compute_growth_windows


def _latest_version_ids() -> dict[str, str]:
    with session_scope() as session:
        stmt = select(EstimateVersion).order_by(
            EstimateVersion.company_id, EstimateVersion.created_at.desc()
        )
        out: dict[str, str] = {}
        for ev in session.execute(stmt).scalars():
            out.setdefault(ev.company_id, ev.id)
        return out


def _latest_headcount_for_version(version_id: str) -> float | None:
    with session_scope() as session:
        stmt = (
            select(HeadcountEstimateMonthly)
            .where(HeadcountEstimateMonthly.estimate_version_id == version_id)
            .order_by(HeadcountEstimateMonthly.month.desc())
            .limit(1)
        )
        row = session.execute(stmt).scalar_one_or_none()
        if row is None:
            return None
        return float(row.estimated_headcount)


def _growth_map_for_version(version_id: str) -> dict[str, Any]:
    with session_scope() as session:
        rows = compute_growth_windows(session, version_id=version_id)
    out: dict[str, Any] = {"1y": None, "6m": None}
    for row in rows:
        window = str(row.get("window"))
        if window in out:
            out[window] = row.get("percent_delta")
    return out


def export_company_growth_csv(output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    version_map = _latest_version_ids()
    if not version_map:
        output_path.write_text("", encoding="utf-8")
        return 0

    with session_scope() as session:
        companies = list(session.execute(select(Company).order_by(Company.canonical_name)).scalars())

    written = 0
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "Company Name",
                "Headcount",
                "Headcount % (365d)",
                "Headcount % (180d)",
            ],
        )
        writer.writeheader()
        for company in companies:
            version_id = version_map.get(company.id)
            if version_id is None:
                continue
            headcount = _latest_headcount_for_version(version_id)
            growth = _growth_map_for_version(version_id)
            writer.writerow(
                {
                    "Company Name": company.canonical_name,
                    "Headcount": headcount,
                    "Headcount % (365d)": growth.get("1y"),
                    "Headcount % (180d)": growth.get("6m"),
                }
            )
            written += 1
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="data/exports/company_growth.csv",
        help="Output CSV path (default: data/exports/company_growth.csv).",
    )
    args = parser.parse_args()
    out_path = Path(args.output).resolve()
    rows = export_company_growth_csv(out_path)
    print(f"wrote {rows} rows -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
