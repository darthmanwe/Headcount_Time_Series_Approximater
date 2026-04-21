"""Reparse archived raw HTTP bodies with the current parser versions.

This is the Plan C "replay" tool: a fast offline loop over the
``raw_response`` table that re-runs the parsers against previously
archived bytes. Use it when:

- A parser bump (``COMPANY_WEB_PARSER_VERSION`` / ``WAYBACK_PARSER_VERSION``)
  unlocks new extraction patterns on pages you already fetched.
- You want to verify an observer's extraction logic against the
  actual HTML the site served, without burning another LinkedIn /
  Wayback request.
- You're debugging and want to see what a specific page would emit
  today.

Scope
-----
v1 supports:

- ``--source-hint company_web``  - re-parses first-party company-site
  captures by matching the URL's hostname against
  ``company.canonical_domain``.
- ``--source-hint wayback``      - re-parses Wayback snapshots by
  extracting the original (archived) URL from the ``web.archive.org``
  redirect shape, then matching to a company the same way.

LinkedIn reparse is intentionally deferred. The LinkedIn fetch path
is stateful (slug resolution, rate guard, disambiguation) and
replaying just the HTML body risks binding the wrong company to a
slug we fetched for a different one in the original run. Plan C's
first cut deliberately ships the two observers where URL -> company
is a pure function of the stored URL.

All writes go through the same dedup key
``(source_name, raw_content_hash)`` as ``collect_anchors``, so
re-running this script is idempotent: re-parsed pages that haven't
changed produce zero new rows.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, date
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _bootstrap_env() -> None:
    if not os.environ.get("DB_URL", "").strip():
        canonical = (REPO_ROOT / "data" / "headcount.sqlite").resolve()
        os.environ["DB_URL"] = f"sqlite:///{canonical.as_posix()}"


_bootstrap_env()

from sqlalchemy import select  # noqa: E402

from headcount.db.engine import session_scope  # noqa: E402
from headcount.db.enums import (  # noqa: E402
    AnchorType,
    ParseStatus,
    RunKind,
    RunStatus,
    SourceEntityType,
    SourceName,
)
from headcount.ingest.raw_response_store import decompress_body  # noqa: E402
from headcount.models.company import Company  # noqa: E402
from headcount.models.company_anchor_observation import (  # noqa: E402
    CompanyAnchorObservation,
)
from headcount.models.raw_response import RawResponse  # noqa: E402
from headcount.models.run import Run  # noqa: E402
from headcount.models.source_observation import SourceObservation  # noqa: E402
from headcount.parsers.anchors import (  # noqa: E402
    COMPANY_WEB_PARSER_VERSION,
    WAYBACK_PARSER_VERSION,
    clean_html_to_text,
    extract_linkedin_jsonld_employees,
    parse_company_web_jsonld,
    parse_company_web_text,
)
from headcount.utils.time import month_floor  # noqa: E402

# Matches ``https://web.archive.org/web/<14digits>[id_]/<original_url>``.
# We only care about the timestamp + archived URL so the snapshot
# confidence floor / anchor month stay consistent with the live observer.
_WAYBACK_RE = re.compile(
    r"^https?://web\.archive\.org/web/(?P<ts>\d{14})(?:id_)?/(?P<url>.+)$"
)


@dataclass
class ReparseStats:
    raw_rows_considered: int = 0
    raw_rows_matched: int = 0
    signals_written: int = 0
    anchors_written: int = 0
    skipped_no_company: int = 0
    skipped_parse_empty: int = 0
    dedup_hits: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "raw_rows_considered": self.raw_rows_considered,
            "raw_rows_matched": self.raw_rows_matched,
            "signals_written": self.signals_written,
            "anchors_written": self.anchors_written,
            "skipped_no_company": self.skipped_no_company,
            "skipped_parse_empty": self.skipped_parse_empty,
            "dedup_hits": self.dedup_hits,
            "errors": len(self.errors),
        }


def _load_domain_index(session) -> dict[str, Company]:
    """Map canonical_domain -> Company for fast URL -> company lookup."""

    rows = session.execute(select(Company)).scalars().all()
    by_domain: dict[str, Company] = {}
    for c in rows:
        if not c.canonical_domain:
            continue
        by_domain[c.canonical_domain.lower()] = c
    return by_domain


def _domain_of(url: str) -> str | None:
    try:
        host = urlparse(url).hostname
    except ValueError:
        return None
    if not host:
        return None
    host = host.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _company_for_url(
    url: str, *, by_domain: dict[str, Company]
) -> Company | None:
    # Direct hostname match first; then try stripping ``www.`` variants
    # that might be stored with the prefix in the company table.
    host = _domain_of(url)
    if host is None:
        return None
    if host in by_domain:
        return by_domain[host]
    # fall back to suffix match in case a subdomain was served
    for domain, company in by_domain.items():
        if host.endswith("." + domain):
            return company
    return None


def _existing_observation_hashes(session) -> set[tuple[str, str]]:
    rows = session.execute(
        select(SourceObservation.source_name, SourceObservation.raw_content_hash)
    ).all()
    return {(name.value, digest) for name, digest in rows}


def _emit_company_web_signals(
    session,
    *,
    run_id: str,
    raw: RawResponse,
    company: Company,
    body_text: str,
    existing: set[tuple[str, str]],
    stats: ReparseStats,
    dry_run: bool,
) -> None:
    matches = parse_company_web_jsonld(body_text)
    from_jsonld = bool(matches)
    if not matches:
        text = clean_html_to_text(body_text)
        if not text:
            stats.skipped_parse_empty += 1
            return
        matches = parse_company_web_text(text)
    if not matches:
        stats.skipped_parse_empty += 1
        return

    anchor_month: date = month_floor(raw.first_seen_at.date())
    observed_at = raw.first_seen_at
    for m in matches:
        confidence = (
            0.70
            if from_jsonld
            else (0.65 if m.qualifier in (None, "headcount") else 0.55)
        )
        # ``raw_content_hash`` convention: sha256(url + "\n" + body).
        # The stored RawResponse hash is body-only, so recompute the
        # observation-flavoured hash here. This keeps the dedup key
        # compatible with anything ``collect_anchors`` already wrote.
        import hashlib

        raw_text = m.phrase
        content_hash = hashlib.sha256(
            f"{raw.url or ''}\n{raw_text}".encode()
        ).hexdigest()
        key = (SourceName.company_web.value, content_hash)
        if key in existing:
            stats.dedup_hits += 1
            continue
        existing.add(key)
        _persist(
            session,
            run_id=run_id,
            company=company,
            raw=raw,
            source_name=SourceName.company_web,
            parser_version=COMPANY_WEB_PARSER_VERSION,
            anchor_month=anchor_month,
            observed_at=observed_at,
            raw_text=raw_text,
            raw_content_hash=content_hash,
            match=m,
            confidence=confidence,
            note=(
                f"reparse jsonld={int(from_jsonld)} "
                f"qualifier={m.qualifier or 'exact'}"
            ),
            normalized_payload={
                "reparse": True,
                "qualifier": m.qualifier,
                "phrase": m.phrase,
                "jsonld": bool(from_jsonld),
            },
            stats=stats,
            dry_run=dry_run,
        )


def _emit_wayback_signals(
    session,
    *,
    run_id: str,
    raw: RawResponse,
    by_domain: dict[str, Company],
    body_text: str,
    existing: set[tuple[str, str]],
    stats: ReparseStats,
    dry_run: bool,
) -> None:
    match = _WAYBACK_RE.match(raw.url)
    if match is None:
        stats.skipped_no_company += 1
        return
    ts, archived_url = match.group("ts"), match.group("url")
    company = _company_for_url(archived_url, by_domain=by_domain)
    if company is None:
        stats.skipped_no_company += 1
        return
    is_linkedin = "linkedin.com" in (_domain_of(archived_url) or "")
    parser_matches: list = []
    is_jsonld = False
    if is_linkedin:
        parser_matches = extract_linkedin_jsonld_employees(body_text) or []
        is_jsonld = True
    else:
        parser_matches = parse_company_web_jsonld(body_text) or []
        is_jsonld = bool(parser_matches)
        if not parser_matches:
            text = clean_html_to_text(body_text)
            if text:
                parser_matches = parse_company_web_text(text)
    if not parser_matches:
        stats.skipped_parse_empty += 1
        return
    # Derive the snapshot anchor month from the wayback timestamp so
    # growth horizons (6m/1y/2y) align with the live observer's view.
    try:
        anchor_month = date(int(ts[:4]), int(ts[4:6]), 1)
    except ValueError:
        anchor_month = month_floor(raw.first_seen_at.date())
    observed_at = raw.first_seen_at
    import hashlib

    for m in parser_matches:
        raw_text = getattr(m, "phrase", None) or getattr(m, "raw", "") or ""
        if not raw_text:
            continue
        confidence = 0.55 if is_jsonld else 0.45
        content_hash = hashlib.sha256(
            f"{raw.url or ''}\n{raw_text}".encode()
        ).hexdigest()
        key = (SourceName.wayback.value, content_hash)
        if key in existing:
            stats.dedup_hits += 1
            continue
        existing.add(key)
        _persist(
            session,
            run_id=run_id,
            company=company,
            raw=raw,
            source_name=SourceName.wayback,
            parser_version=WAYBACK_PARSER_VERSION,
            anchor_month=anchor_month,
            observed_at=observed_at,
            raw_text=raw_text,
            raw_content_hash=content_hash,
            match=m,
            confidence=confidence,
            note=(
                f"reparse wayback ts={ts} "
                f"origin={'linkedin' if is_linkedin else 'company_web'}"
            ),
            normalized_payload={
                "reparse": True,
                "wayback_timestamp": ts,
                "origin_url": archived_url,
                "jsonld": bool(is_jsonld),
            },
            stats=stats,
            dry_run=dry_run,
        )


def _persist(
    session,
    *,
    run_id: str,  # noqa: ARG001 - reserved for future audit trail linkage
    company: Company,
    raw: RawResponse,
    source_name: SourceName,
    parser_version: str,
    anchor_month: date,
    observed_at: datetime,
    raw_text: str,
    raw_content_hash: str,
    match,
    confidence: float,
    note: str,
    normalized_payload: dict,
    stats: ReparseStats,
    dry_run: bool,
) -> None:
    if dry_run:
        stats.signals_written += 1
        stats.anchors_written += 1
        return
    source_row = SourceObservation(
        source_name=source_name,
        entity_type=SourceEntityType.company,
        source_url=raw.url,
        observed_at=observed_at,
        raw_text=raw_text,
        raw_content_hash=raw_content_hash,
        parser_version=parser_version,
        parse_status=ParseStatus.ok,
        normalized_payload_json=normalized_payload,
    )
    session.add(source_row)
    session.flush()
    anchor = CompanyAnchorObservation(
        company_id=company.id,
        source_observation_id=source_row.id,
        anchor_type=AnchorType.current_headcount_anchor
        if source_name is SourceName.company_web
        else AnchorType.historical_statement,
        headcount_value_min=match.value_min,
        headcount_value_point=match.value_point,
        headcount_value_max=match.value_max,
        headcount_value_kind=match.kind,
        anchor_month=anchor_month,
        confidence=confidence,
        note=note,
    )
    session.add(anchor)
    stats.signals_written += 1
    stats.anchors_written += 1


def _create_reparse_run(session, *, source_hints: Iterable[str]) -> Run:
    from headcount.config.settings import get_settings

    settings = get_settings()
    now = datetime.now(tz=UTC)
    run = Run(
        kind=RunKind.full,
        status=RunStatus.running,
        started_at=now,
        cutoff_month=now.date().replace(day=1),
        method_version=settings.method_version,
        anchor_policy_version=settings.anchor_policy_version,
        coverage_curve_version=settings.coverage_curve_version,
        config_hash="reparse-raw-responses",
        label=f"reparse:{','.join(sorted(source_hints))}",
    )
    session.add(run)
    session.flush()
    return run


def run(
    *,
    source_hints: Iterable[str],
    since: datetime | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> ReparseStats:
    stats = ReparseStats()
    hints = {h.strip() for h in source_hints if h.strip()}
    if not hints:
        stats.errors.append("no source_hints provided")
        return stats

    with session_scope() as session:
        run_row = _create_reparse_run(session, source_hints=hints)
        by_domain = _load_domain_index(session)
        existing = _existing_observation_hashes(session)

        q = select(RawResponse).where(RawResponse.source_hint.in_(hints))
        if since is not None:
            q = q.where(RawResponse.first_seen_at >= since)
        q = q.order_by(RawResponse.first_seen_at.asc())
        if limit is not None:
            q = q.limit(limit)

        for raw in session.execute(q).scalars():
            stats.raw_rows_considered += 1
            try:
                body = decompress_body(raw)
            except Exception as exc:
                stats.errors.append(f"{raw.id}: decompress failed: {exc!r}")
                continue
            if not body:
                stats.skipped_parse_empty += 1
                continue
            if raw.source_hint == SourceName.company_web.value:
                company = _company_for_url(raw.url, by_domain=by_domain)
                if company is None:
                    stats.skipped_no_company += 1
                    continue
                stats.raw_rows_matched += 1
                _emit_company_web_signals(
                    session,
                    run_id=run_row.id,
                    raw=raw,
                    company=company,
                    body_text=body,
                    existing=existing,
                    stats=stats,
                    dry_run=dry_run,
                )
            elif raw.source_hint == SourceName.wayback.value:
                stats.raw_rows_matched += 1
                _emit_wayback_signals(
                    session,
                    run_id=run_row.id,
                    raw=raw,
                    by_domain=by_domain,
                    body_text=body,
                    existing=existing,
                    stats=stats,
                    dry_run=dry_run,
                )
            else:
                stats.errors.append(
                    f"unsupported source_hint: {raw.source_hint!r}"
                )

        run_row.finished_at = datetime.now(tz=UTC)
        run_row.status = (
            RunStatus.succeeded if not stats.errors else RunStatus.partial
        )
        if dry_run:
            session.rollback()
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-hint",
        action="append",
        default=None,
        choices=["company_web", "wayback"],
        help=(
            "Which archived observer to reparse. Pass multiple times to "
            "cover more than one source. Defaults to both."
        ),
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Only reparse responses fetched on/after this ISO date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N raw_response rows (useful for dry runs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write any observation / anchor rows.",
    )
    args = parser.parse_args(argv)

    hints = args.source_hint or ["company_web", "wayback"]
    since_dt: datetime | None = None
    if args.since:
        since_dt = datetime.fromisoformat(args.since)
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=UTC)

    stats = run(
        source_hints=hints,
        since=since_dt,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    import json

    print(json.dumps(stats.as_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
