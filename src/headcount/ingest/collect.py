"""Anchor-collection orchestrator.

Runs one or more :class:`AnchorSourceAdapter` instances over a list of
companies and persists the emitted :class:`RawAnchorSignal` objects into
``source_observation`` + ``company_anchor_observation`` rows. Budget and
circuit-breaker state is held in :class:`SourceBudgetStore` so state
survives across re-runs.

Key invariants
--------------
- One :class:`Run` row per ``collect_anchors`` call. If the caller
  supplies ``run_id`` we reuse it; otherwise we create a new ``running``
  run and mark it ``succeeded`` / ``failed`` / ``partial`` at the end.
- Every adapter call flows through the rate limiter and circuit breaker
  so a misbehaving source cannot exhaust budgets for others.
- Cache hits don't consume budget; only live calls do. That keeps
  re-runs essentially free until a source's cache expires.
- Signals are deduped per ``(source_name, source_url, content_hash)`` so
  replaying an adapter never creates duplicate rows.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.config.settings import get_settings
from headcount.db.enums import (
    CompanyRunStage,
    CompanyRunStageStatus,
    ReviewReason,
    ReviewStatus,
    RunKind,
    RunStatus,
    SourceName,
)
from headcount.ingest.base import (
    AdapterFetchError,
    AdapterGatedError,
    AnchorSourceAdapter,
    CompanyTarget,
    FetchContext,
    FetchReport,
    RawAnchorSignal,
)
from headcount.ingest.http import FileCache, HttpClient, HttpClientConfig
from headcount.ingest.rate_limit import (
    BudgetExhaustedError,
    BudgetTrippedError,
    CircuitBreaker,
    SourceBudgetStore,
)
from headcount.models.company import Company
from headcount.models.company_alias import CompanyAlias
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.review_queue_item import ReviewQueueItem
from headcount.models.run import CompanyRunStatus, Run
from headcount.models.source_observation import SourceObservation
from headcount.review.queue_writer import (
    DbReviewQueueWriter,
    EnqueueRequest,
    ReviewQueueWriter,
)
from headcount.utils.logging import get_logger

_log = get_logger("headcount.ingest.collect")


@dataclass(slots=True)
class CollectResult:
    run_id: str
    reports: dict[SourceName, FetchReport] = field(default_factory=dict)
    signals_written: int = 0
    anchors_written: int = 0
    companies_attempted: int = 0
    companies_with_signals: int = 0
    companies_gated: int = 0
    linkedin_gated_companies: int = 0
    review_items_enqueued: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "companies_attempted": self.companies_attempted,
            "companies_with_signals": self.companies_with_signals,
            "companies_gated": self.companies_gated,
            "linkedin_gated_companies": self.linkedin_gated_companies,
            "review_items_enqueued": self.review_items_enqueued,
            "signals_written": self.signals_written,
            "anchors_written": self.anchors_written,
            "errors": len(self.errors),
            "per_source": {
                k.value: {
                    "attempted": v.companies_attempted,
                    "signals": v.signals_emitted,
                    "gated": v.gated,
                    "errors": v.errors,
                    "cache_hits": v.cache_hits,
                }
                for k, v in self.reports.items()
            },
        }


def _company_target(session: Session, company: Company) -> CompanyTarget:
    aliases = (
        session.execute(
            select(CompanyAlias.alias_name).where(CompanyAlias.company_id == company.id)
        )
        .scalars()
        .all()
    )
    return CompanyTarget(
        company_id=company.id,
        canonical_name=company.canonical_name,
        canonical_domain=company.canonical_domain,
        linkedin_company_url=company.linkedin_company_url,
        country=company.country,
        aliases=tuple(aliases),
    )


def _create_run(session: Session, *, method_version: str) -> Run:
    settings = get_settings()
    now = datetime.now(tz=UTC)
    run = Run(
        kind=RunKind.full,
        status=RunStatus.running,
        started_at=now,
        cutoff_month=now.date().replace(day=1),
        method_version=method_version,
        anchor_policy_version=settings.anchor_policy_version,
        coverage_curve_version=settings.coverage_curve_version,
        config_hash="collect-anchors",
    )
    session.add(run)
    session.flush()
    return run


def _ensure_stage_row(
    session: Session,
    *,
    run_id: str,
    company_id: str,
) -> CompanyRunStatus:
    row = session.execute(
        select(CompanyRunStatus).where(
            CompanyRunStatus.run_id == run_id,
            CompanyRunStatus.company_id == company_id,
            CompanyRunStatus.stage == CompanyRunStage.collect_anchors,
        )
    ).scalar_one_or_none()
    if row is None:
        row = CompanyRunStatus(
            run_id=run_id,
            company_id=company_id,
            stage=CompanyRunStage.collect_anchors,
            status=CompanyRunStageStatus.running,
            attempts=1,
        )
        session.add(row)
        session.flush()
    else:
        row.status = CompanyRunStageStatus.running
        row.attempts += 1
    return row


def _persist_signals(
    session: Session,
    *,
    company_id: str,
    signals: Sequence[RawAnchorSignal],
    existing_hashes: set[tuple[str, str]],
) -> tuple[int, int]:
    """Write source_observation + company_anchor_observation. Returns counts."""
    written_source = 0
    written_anchor = 0
    for signal in signals:
        key = (signal.source_name.value, signal.raw_content_hash)
        if key in existing_hashes:
            continue
        existing_hashes.add(key)
        source_row = SourceObservation(
            source_name=signal.source_name,
            entity_type=signal.entity_type,
            source_url=signal.source_url,
            observed_at=signal.observed_at,
            raw_text=signal.raw_text,
            raw_content_hash=signal.raw_content_hash,
            parser_version=signal.parser_version,
            parse_status=signal.parse_status,
            normalized_payload_json=dict(signal.normalized_payload) or None,
        )
        session.add(source_row)
        session.flush()
        written_source += 1

        anchor_row = CompanyAnchorObservation(
            company_id=company_id,
            source_observation_id=source_row.id,
            anchor_type=signal.anchor_type,
            headcount_value_min=signal.headcount_value_min,
            headcount_value_point=signal.headcount_value_point,
            headcount_value_max=signal.headcount_value_max,
            headcount_value_kind=signal.headcount_value_kind,
            anchor_month=signal.anchor_month,
            confidence=signal.confidence,
            note=signal.note,
        )
        session.add(anchor_row)
        written_anchor += 1
    return written_source, written_anchor


def _existing_hashes(session: Session, company_id: str) -> set[tuple[str, str]]:
    rows = session.execute(
        select(SourceObservation.source_name, SourceObservation.raw_content_hash)
        .join(
            CompanyAnchorObservation,
            CompanyAnchorObservation.source_observation_id == SourceObservation.id,
        )
        .where(CompanyAnchorObservation.company_id == company_id)
    ).all()
    return {(name.value, digest) for name, digest in rows}


def _has_open_linkedin_gate_item(session: Session, *, run_id: str, company_id: str) -> bool:
    """Check whether we already enqueued a linkedin_gated item for this run+company.

    Keeps ``collect_anchors`` idempotent: rerunning the collector must
    not pile duplicate review items for a company that keeps getting
    gated.
    """
    row = session.execute(
        select(ReviewQueueItem.id)
        .where(
            ReviewQueueItem.company_id == company_id,
            ReviewQueueItem.review_reason == ReviewReason.linkedin_gated,
            ReviewQueueItem.status != ReviewStatus.resolved,
            ReviewQueueItem.detail.like(f"run={run_id}%"),
        )
        .limit(1)
    ).first()
    return row is not None


async def collect_anchors(
    session: Session,
    *,
    adapters: Iterable[AnchorSourceAdapter],
    companies: Sequence[Company] | None = None,
    http_client: HttpClient | None = None,
    budget_store: SourceBudgetStore | None = None,
    run: Run | None = None,
    default_budget: int = 1000,
    trip_after: int = 5,
    method_version: str | None = None,
    review_writer: ReviewQueueWriter | None = None,
) -> CollectResult:
    """Collect anchor signals from every adapter for every company.

    The orchestrator owns the lifecycle of the HTTP client if the caller
    doesn't supply one; tests can pass a pre-built client with a
    :class:`httpx.MockTransport` so no network is touched.
    """
    settings = get_settings()
    method_version = method_version or settings.method_version
    run = run or _create_run(session, method_version=method_version)
    session.flush()
    result = CollectResult(run_id=run.id)
    budget_store = budget_store or SourceBudgetStore(
        session,
        run_id=run.id,
        default_allowed=default_budget,
        trip_after_n_failures=trip_after,
    )
    writer = review_writer or DbReviewQueueWriter(session)
    reports: dict[SourceName, FetchReport] = {}
    for adapter in adapters:
        reports.setdefault(adapter.source_name, FetchReport(source_name=adapter.source_name))

    if http_client is None:
        cache = FileCache(settings.cache_dir)
        http_client = HttpClient(cache=cache, configs={})

    companies = list(
        companies
        if companies is not None
        else session.execute(select(Company).order_by(Company.canonical_name)).scalars()
    )

    context = FetchContext(
        run_id=run.id,
        http=http_client,
        budget_store=budget_store,
        method_version=method_version,
        live=True,
    )

    async with http_client:
        for company in companies:
            result.companies_attempted += 1
            target = _company_target(session, company)
            stage_row = _ensure_stage_row(session, run_id=run.id, company_id=company.id)
            existing_hashes = _existing_hashes(session, company.id)
            any_signal = False
            stage_errors: list[str] = []
            adapters_run = 0
            adapters_gated = 0
            adapters_failed = 0
            linkedin_gated_here = False
            for adapter in adapters:
                report = reports[adapter.source_name]
                breaker = CircuitBreaker(store=budget_store, source=adapter.source_name)
                if breaker.should_short_circuit():
                    continue
                try:
                    budget_store.reserve(adapter.source_name)
                except (BudgetTrippedError, BudgetExhaustedError) as exc:
                    stage_errors.append(f"{adapter.source_name.value}:{exc}")
                    continue
                adapters_run += 1
                report.companies_attempted += 1
                try:
                    signals = await adapter.fetch_current_anchor(target, context=context)
                    breaker.record("ok")
                    report.signals_emitted += len(signals)
                    if signals:
                        any_signal = True
                        written_s, written_a = _persist_signals(
                            session,
                            company_id=company.id,
                            signals=signals,
                            existing_hashes=existing_hashes,
                        )
                        result.signals_written += written_s
                        result.anchors_written += written_a
                except AdapterGatedError as exc:
                    report.gated += 1
                    adapters_gated += 1
                    breaker.record("gated")
                    stage_errors.append(f"{adapter.source_name.value} gated: {exc}")
                    _log.warning(
                        "adapter_gated",
                        company_id=company.id,
                        source=adapter.source_name.value,
                        error=repr(exc),
                    )
                    if adapter.source_name is SourceName.linkedin_public:
                        linkedin_gated_here = True
                        if not _has_open_linkedin_gate_item(
                            session, run_id=run.id, company_id=company.id
                        ):
                            writer.enqueue(
                                EnqueueRequest(
                                    company_id=company.id,
                                    reason=ReviewReason.linkedin_gated,
                                    priority=60,
                                    detail=(f"run={run.id} source=linkedin_public reason={exc}")[
                                        :2000
                                    ],
                                )
                            )
                            result.review_items_enqueued += 1
                except AdapterFetchError as exc:
                    report.errors += 1
                    adapters_failed += 1
                    breaker.record("error")
                    stage_errors.append(f"{adapter.source_name.value} error: {exc}")
                    _log.warning(
                        "adapter_error",
                        company_id=company.id,
                        source=adapter.source_name.value,
                        error=repr(exc),
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    report.errors += 1
                    adapters_failed += 1
                    breaker.record("error")
                    stage_errors.append(f"{adapter.source_name.value} unexpected: {exc!r}")
                    _log.error(
                        "adapter_unexpected_error",
                        company_id=company.id,
                        source=adapter.source_name.value,
                        error=repr(exc),
                    )

            stage_row.last_progress_at = datetime.now(tz=UTC)
            if linkedin_gated_here:
                result.linkedin_gated_companies += 1
            if any_signal:
                stage_row.status = CompanyRunStageStatus.succeeded
                stage_row.last_error = "; ".join(stage_errors)[:2000] if stage_errors else None
                result.companies_with_signals += 1
                if stage_errors:
                    result.errors.extend(stage_errors)
            elif adapters_run > 0 and adapters_gated > 0 and adapters_failed == 0:
                # Every adapter that ran was gated - degraded path. We
                # distinguish gated from failed so operators can see at
                # a glance which companies need manual review vs a
                # code/network fix.
                stage_row.status = CompanyRunStageStatus.gated
                stage_row.last_error = "; ".join(stage_errors)[:2000]
                result.companies_gated += 1
                result.errors.extend(stage_errors)
            elif stage_errors:
                stage_row.status = CompanyRunStageStatus.failed
                stage_row.last_error = "; ".join(stage_errors)[:2000]
                result.errors.extend(stage_errors)
            else:
                stage_row.status = CompanyRunStageStatus.succeeded
                stage_row.last_error = None
            session.flush()

    result.reports = reports
    run.finished_at = datetime.now(tz=UTC)
    if result.companies_with_signals == 0 and result.companies_attempted > 0:
        # Distinguish "everyone walled" (partial, operator intervention)
        # from "everyone crashed" (failed, likely a bug or outage).
        if result.companies_gated > 0 and not any("error:" in line for line in result.errors):
            run.status = RunStatus.partial
        elif result.errors:
            run.status = RunStatus.failed
        else:
            run.status = RunStatus.succeeded
    elif result.errors:
        run.status = RunStatus.partial
    else:
        run.status = RunStatus.succeeded
    session.flush()

    _log.info("collect_anchors_done", **result.summary())
    return result


def default_http_configs() -> dict[SourceName, HttpClientConfig]:
    """Per-source polite defaults used by the CLI."""
    settings = get_settings()
    return {
        SourceName.company_web: HttpClientConfig(
            user_agent="Headcount-Estimator/0.1 (+internal-use)",
            max_concurrency=settings.company_web_max_concurrency,
            cache_ttl_seconds=24 * 3600,
        ),
        SourceName.sec: HttpClientConfig(
            user_agent=settings.sec_user_agent,
            max_concurrency=2,
            cache_ttl_seconds=7 * 24 * 3600,
        ),
        SourceName.wikidata: HttpClientConfig(
            user_agent="Headcount-Estimator/0.1 (+internal) contact@example.com",
            max_concurrency=2,
            cache_ttl_seconds=7 * 24 * 3600,
            default_headers={"Accept": "application/sparql-results+json"},
        ),
        SourceName.linkedin_public: HttpClientConfig(
            user_agent=("Headcount-Estimator/0.1 (+internal-use; contact@example.com)"),
            # Logged-out LinkedIn walls aggressive traffic: keep the
            # concurrency at 1 and cache aggressively so re-runs are
            # essentially free until the TTL rolls.
            max_concurrency=1,
            cache_ttl_seconds=settings.linkedin_public_company_ttl_days * 86400,
        ),
    }
