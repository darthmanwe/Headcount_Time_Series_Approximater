"""Collect anchors + estimate for a specific list of companies by name.

Used when we already know a slug has been resolved but the anchor
collect got rolled back by a downstream error, and we want to replay
only the anchor + estimate stages for a single company.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def _bootstrap(run_dir: Path) -> None:
    db_path = (run_dir / "cohort.sqlite").resolve()
    cache_dir = (run_dir / "http_cache").resolve()
    run_artifact_dir = (run_dir / "run_artifacts").resolve()
    for p in (cache_dir, run_artifact_dir):
        p.mkdir(parents=True, exist_ok=True)
    os.environ["DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    os.environ["CACHE_DIR"] = str(cache_dir)
    os.environ["RUN_ARTIFACT_DIR"] = str(run_artifact_dir)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("names", nargs="+", help="canonical_name values")
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    _bootstrap(run_dir)

    from headcount.db.engine import session_scope
    from headcount.estimate.pipeline import estimate_series
    from headcount.ingest.collect import collect_anchors, default_http_configs
    from headcount.ingest.http import FileCache, HttpClient
    from headcount.ingest.linkedin_guard import LinkedInRateGuard
    from headcount.ingest.observers import (
        CompanyWebObserver,
        LinkedInPublicObserver,
        ManualAnchorObserver,
        SECObserver,
        WikidataObserver,
    )
    from headcount.models.company import Company

    with session_scope() as session:
        comps = (
            session.query(Company)
            .filter(Company.canonical_name.in_(args.names))
            .all()
        )
        print(f"[specific] matched {len(comps)} companies")
        if not comps:
            return 0

        guard = LinkedInRateGuard.from_settings(
            circuit_threshold=20,
            daily_request_budget=120,
        )
        cache = FileCache(Path(os.environ["CACHE_DIR"]))
        http = HttpClient(cache=cache, configs=default_http_configs())
        adapters = [
            ManualAnchorObserver(),
            SECObserver(),
            WikidataObserver(),
            CompanyWebObserver(),
            LinkedInPublicObserver(rate_guard=guard),
        ]

        async def _run():
            return await collect_anchors(
                session,
                adapters=adapters,
                companies=comps,
                http_client=http,
            )

        result = asyncio.run(_run())
        print(f"[specific] anchors: {result.summary()}")
        session.commit()

        ids = [c.id for c in comps]
        est = estimate_series(
            session,
            company_ids=ids,
            start_month=date(2022, 1, 1),
            end_month=date(2026, 4, 1),
            as_of_month=date(2026, 4, 1),
            note="specific_recollect",
        )
        print(
            f"[specific] estimate: attempted={est.companies_attempted}"
            f" succeeded={est.companies_succeeded}"
            f" degraded={est.companies_degraded}"
        )
        session.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
