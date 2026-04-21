"""Backfill company.linkedin_resolved_url from existing repo run logs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def _bootstrap() -> None:
    if not os.environ.get("DB_URL", "").strip():
        db = (REPO_ROOT / "data" / "headcount.sqlite").resolve()
        db.parent.mkdir(parents=True, exist_ok=True)
        os.environ["DB_URL"] = f"sqlite:///{db.as_posix()}"


def _extract_pairs(log_path: Path) -> list[tuple[str | None, str | None, str]]:
    pairs: list[tuple[str | None, str | None, str]] = []
    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return pairs
    for line in lines:
        raw = line.strip()
        if not raw or '"event": "linkedin_slug_resolved"' not in raw:
            continue
        try:
            evt = json.loads(raw)
        except json.JSONDecodeError:
            continue
        name = str(evt.get("name") or "").strip() or None
        domain = str(evt.get("domain") or "").strip() or None
        slug = str(evt.get("slug") or "").strip()
        if not slug:
            continue
        url = f"https://www.linkedin.com/company/{slug}/"
        pairs.append((name, domain, url))
    return pairs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--logs-glob",
        default="data/*_run.log",
        help="Glob pattern (relative to repo root) for JSONL run logs.",
    )
    args = parser.parse_args()
    _bootstrap()

    from headcount.db.engine import session_scope
    from headcount.models.company import Company

    logs = sorted(REPO_ROOT.glob(args.logs_glob))
    if not logs:
        print("No logs matched.")
        return 0

    extracted: list[tuple[str | None, str | None, str]] = []
    for p in logs:
        extracted.extend(_extract_pairs(p))

    updates = 0
    with session_scope() as session:
        for name, domain, url in extracted:
            company = None
            if domain:
                company = session.execute(
                    select(Company).where(Company.canonical_domain == domain).limit(1)
                ).scalar_one_or_none()
            if company is None and name:
                company = session.execute(
                    select(Company).where(Company.canonical_name == name).limit(1)
                ).scalar_one_or_none()
            if company is None:
                continue
            changed = False
            if not (company.linkedin_resolved_url or "").strip():
                company.linkedin_resolved_url = url
                changed = True
            if not (company.linkedin_company_url or "").strip():
                company.linkedin_company_url = url
                changed = True
            if changed:
                updates += 1
        session.commit()
    print(f"logs_scanned={len(logs)} extracted={len(extracted)} updated={updates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
