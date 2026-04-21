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
    """Canonical DB + cache; run_dir for artifacts only. Env overrides
    still win."""

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
        from headcount.ingest.raw_response_store import build_sink_from_session

        http = HttpClient(
            cache=cache,
            configs=default_http_configs(),
            raw_response_sink=build_sink_from_session(session),
        )
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
                run_label=f"specific_recollect:{run_dir.name}",
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
