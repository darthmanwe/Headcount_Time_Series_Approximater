"""Collect LinkedIn current headcount only for missing/zero-current companies.

Selection rule:
- include company if it has no current_headcount_anchor at all, OR
- include company if its latest current_headcount_anchor point value <= 0
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
import sys
from pathlib import Path

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def _bootstrap(run_dir: Path) -> None:
    run_artifact_dir = (run_dir / "run_artifacts").resolve()
    run_artifact_dir.mkdir(parents=True, exist_ok=True)
    if not os.environ.get("DB_URL", "").strip():
        canonical_db = (REPO_ROOT / "data" / "headcount.sqlite").resolve()
        canonical_db.parent.mkdir(parents=True, exist_ok=True)
        os.environ["DB_URL"] = f"sqlite:///{canonical_db.as_posix()}"
    if not os.environ.get("CACHE_DIR", "").strip():
        canonical_cache = (REPO_ROOT / "data" / "cache").resolve()
        canonical_cache.mkdir(parents=True, exist_ok=True)
        os.environ["CACHE_DIR"] = str(canonical_cache)
    os.environ["RUN_ARTIFACT_DIR"] = str(run_artifact_dir)
    # Default to fast mode for operational runs unless explicitly enabled.
    os.environ.setdefault("HEADCOUNT_RAW_RESPONSE_ARCHIVE", "0")
    # Conservative LinkedIn pacing defaults to reduce gate/rate risk.
    # Caller-provided env vars always win.
    os.environ.setdefault("LINKEDIN_PUBLIC_MAX_RPM", "2")
    os.environ.setdefault("LINKEDIN_PUBLIC_REQUEST_JITTER_MS_MIN", "5000")
    os.environ.setdefault("LINKEDIN_PUBLIC_REQUEST_JITTER_MS_MAX", "12000")
    os.environ.setdefault("LINKEDIN_PUBLIC_MAX_REQUESTS_PER_RUN", "350")


def _missing_or_zero_company_ids(session) -> list[str]:
    from headcount.db.enums import AnchorType
    from headcount.models.company import Company
    from headcount.models.company_anchor_observation import CompanyAnchorObservation

    companies = list(session.execute(select(Company)).scalars())
    anchors = list(
        session.execute(
            select(CompanyAnchorObservation).where(
                CompanyAnchorObservation.anchor_type == AnchorType.current_headcount_anchor
            )
        ).scalars()
    )
    latest_by_company: dict[str, CompanyAnchorObservation] = {}
    for a in anchors:
        prev = latest_by_company.get(a.company_id)
        if prev is None or a.anchor_month > prev.anchor_month:
            latest_by_company[a.company_id] = a

    out: list[str] = []
    for c in companies:
        latest = latest_by_company.get(c.id)
        if latest is None or float(latest.headcount_value_point) <= 0.0:
            out.append(c.id)
    return out


def _apply_resolved_urls(session, company_ids: list[str]) -> int:
    """Promote stored resolved URLs to active LinkedIn URL when missing."""
    from headcount.models.company import Company

    if not company_ids:
        return 0
    companies = list(
        session.execute(select(Company).where(Company.id.in_(company_ids))).scalars()
    )
    updated = 0
    for c in companies:
        if (c.linkedin_company_url or "").strip():
            continue
        resolved = (getattr(c, "linkedin_resolved_url", None) or "").strip()
        if resolved:
            c.linkedin_company_url = resolved
            updated += 1
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-dir",
        default=str(REPO_ROOT / "data" / "runs" / "linkedin_missing_current"),
        help="Directory for run artifacts/logs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Batch size for each pass (default: all eligible).",
    )
    parser.add_argument(
        "--sleep-minutes",
        type=float,
        default=0.0,
        help="Minutes to sleep between batches (e.g. 15).",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Repeat batches until there are no eligible companies left.",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    _bootstrap(run_dir)

    from headcount.config import get_settings
    from headcount.db.engine import session_scope
    from headcount.ingest.collect import collect_anchors, default_http_configs
    from headcount.ingest.http import FileCache, HttpClient
    from headcount.ingest.linkedin_guard import LinkedInRateGuard
    from headcount.ingest.observers import LinkedInPublicObserver
    from headcount.models.company import Company
    from headcount.resolution.linkedin_resolver import backfill_linkedin_slugs

    batch_no = 0
    while True:
        batch_no += 1
        with session_scope() as session:
            ids = _missing_or_zero_company_ids(session)
            if args.limit is not None:
                ids = ids[: max(0, args.limit)]
            if not ids:
                print("No missing/zero-current companies found.")
                return 0

            promoted = _apply_resolved_urls(session, ids)
            if promoted:
                session.commit()
                print(f"Batch {batch_no}: promoted {promoted} stored resolved URLs.")

            companies = list(
                session.execute(select(Company).where(Company.id.in_(ids))).scalars()
            )
            print(
                f"Batch {batch_no}: selected {len(companies)} companies for LinkedIn recollect."
            )

            settings = get_settings()
            cache = FileCache(settings.cache_dir)
            http = HttpClient(
                cache=cache,
                configs=default_http_configs(),
                transport=None,
                raw_response_sink=None,
            )
            guard = LinkedInRateGuard.from_settings()
            slug_stats = backfill_linkedin_slugs(
                session,
                company_ids=ids,
                http=http,
                rate_guard=guard,
            )
            session.commit()
            print(f"Batch {batch_no}: slug backfill stats: {slug_stats}")
            adapter = LinkedInPublicObserver(rate_guard=guard)

            async def _run():
                return await collect_anchors(
                    session,
                    adapters=[adapter],
                    companies=companies,
                    http_client=http,
                    run_label=f"linkedin_missing_current:{run_dir.name}:b{batch_no}",
                )

            result = asyncio.run(_run())
            session.commit()
            print(f"Batch {batch_no}: {result.summary()}")

        if not args.continuous:
            return 0
        if args.sleep_minutes > 0:
            sleep_s = max(0.0, args.sleep_minutes) * 60.0
            print(f"Sleeping {args.sleep_minutes} minutes before next batch...")
            time.sleep(sleep_s)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
