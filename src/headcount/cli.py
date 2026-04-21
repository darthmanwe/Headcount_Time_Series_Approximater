"""Typer CLI for the headcount estimator.

All stages from docs/BUILD_PLAN_V2.md section 14 are registered here as
subcommands. Phase 0 wires up the subcommand surface with explicit
``NotImplementedError`` stubs so downstream phases can fill them in
without touching the surface contract. Global options (``--run-id``,
``--resume``, ``--limit``, ``--priority-tier``, ``--dry-run``) are defined
once on the parent ``hc`` app and threaded through via a Typer context.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Annotated, Any

import typer

from headcount import __version__
from headcount.config import get_settings
from headcount.utils.logging import bind_context, configure_logging, get_logger

app = typer.Typer(
    name="hc",
    help="Evidence-driven headcount time series estimator (internal use).",
    no_args_is_help=True,
    add_completion=False,
)


@dataclass(slots=True)
class GlobalOptions:
    run_id: str | None
    resume: bool
    limit: int | None
    priority_tier: str | None
    dry_run: bool


def _raw_response_archive_enabled() -> bool:
    """Return whether live HTTP responses should be archived."""
    raw = os.environ.get("HEADCOUNT_RAW_RESPONSE_ARCHIVE", "").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    return True


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"hc {__version__}")
        raise typer.Exit()


@app.callback()
def root(
    ctx: typer.Context,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Stable identifier for this run; persisted to runs table."),
    ] = None,
    resume: Annotated[
        bool,
        typer.Option(
            "--resume/--no-resume", help="Resume a prior run instead of creating a new one."
        ),
    ] = False,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Process at most N companies (smoke runs)."),
    ] = None,
    priority_tier: Annotated[
        str | None,
        typer.Option("--priority-tier", help="Restrict to priority tier: P0, P1, P2."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--no-dry-run", help="Do not persist writes; log intended work only."
        ),
    ] = False,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Print version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Set up global options and bind structured logging context."""
    configure_logging()
    ctx.obj = GlobalOptions(
        run_id=run_id,
        resume=resume,
        limit=limit,
        priority_tier=priority_tier,
        dry_run=dry_run,
    )
    bind_context(
        run_id=run_id or "-",
        priority_tier=priority_tier or "-",
        dry_run=dry_run,
    )


@app.command("seed-companies")
def seed_companies(
    ctx: typer.Context,
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", exists=True, dir_okay=False, readable=True),
    ],
    sheet: Annotated[
        str | None,
        typer.Option("--sheet", help="Force a specific sheet name; default is the active sheet."),
    ] = None,
) -> None:
    """Import a priority-company list into ``company_candidate`` (Phase 2)."""
    from headcount.db.engine import session_scope
    from headcount.ingest.seeds import import_candidates

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.seed_companies")
    with session_scope() as session:
        result = import_candidates(session, input_path, sheet_name=sheet)
        if opts.dry_run:
            session.rollback()
    log.info(
        "seed_companies_done",
        workbook=result.workbook,
        sheet=result.sheet,
        scanned=result.rows_scanned,
        imported=result.rows_imported,
        updated=result.rows_updated,
        skipped=result.rows_skipped,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"seeded {result.rows_imported} new, {result.rows_updated} updated, "
        f"{result.rows_skipped} skipped ({result.rows_scanned} scanned) "
        f"from {result.workbook}:{result.sheet}"
    )


@app.command("load-benchmarks")
def load_benchmarks(
    ctx: typer.Context,
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", exists=True, dir_okay=False, readable=True),
    ],
) -> None:
    """Load offline benchmark spreadsheets from ``test_source/`` (Phase 2)."""
    from headcount.db.engine import session_scope
    from headcount.ingest.seeds import load_benchmarks as _load_benchmarks

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.load_benchmarks")
    with session_scope() as session:
        result = _load_benchmarks(session, input_path)
        if opts.dry_run:
            session.rollback()
    log.info(
        "load_benchmarks_done",
        workbook=result.workbook,
        sheets=result.sheets_loaded,
        observations_written=result.observations_written,
        observations_updated=result.observations_updated,
        event_candidates=result.event_candidates_written,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"loaded {result.observations_written} new observations, "
        f"{result.observations_updated} updated, "
        f"{result.event_candidates_written} event hints across "
        f"{len(result.sheets_loaded)} sheets from {result.workbook}"
    )


@app.command("canonicalize")
def canonicalize(
    ctx: typer.Context,
    company_batch: Annotated[
        str, typer.Option("--company-batch", help="Batch name or tag to resolve.")
    ] = "priority",
    priority_tier: Annotated[
        str,
        typer.Option("--priority-tier", help="Default priority tier for new companies."),
    ] = "P1",
    all_candidates: Annotated[
        bool,
        typer.Option(
            "--all/--pending",
            help="Re-run resolution across every candidate instead of pending rows only.",
        ),
    ] = False,
) -> None:
    """Resolve canonical companies for a batch (Phase 3)."""
    from headcount.db.engine import session_scope
    from headcount.db.enums import PriorityTier
    from headcount.resolution import resolve_candidates

    try:
        tier = PriorityTier(priority_tier)
    except ValueError as exc:
        raise typer.BadParameter(f"unknown priority tier {priority_tier!r}") from exc

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.canonicalize")
    with session_scope() as session:
        result = resolve_candidates(
            session,
            default_priority_tier=tier,
            only_pending=not all_candidates,
        )
        if opts.dry_run:
            session.rollback()
    log.info(
        "canonicalize_done",
        batch=company_batch,
        scanned=result.candidates_scanned,
        resolved=result.candidates_resolved,
        already_resolved=result.candidates_already_resolved,
        failed=result.candidates_failed,
        companies_created=result.companies_created,
        aliases_created=result.aliases_created,
        source_links_created=result.source_links_created,
        relations_created=result.relations_created,
        unresolved_acquirers=len(result.unresolved_acquirers),
        unresolved_renames=len(result.unresolved_renames),
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"resolved {result.candidates_resolved} candidates "
        f"({result.companies_created} new companies, "
        f"{result.aliases_created} aliases, "
        f"{result.source_links_created} source links, "
        f"{result.relations_created} relations); "
        f"{result.candidates_failed} failed; "
        f"{len(result.unresolved_acquirers)} unresolved acquirers, "
        f"{len(result.unresolved_renames)} unresolved renames"
    )


@app.command("parse-events")
def parse_events(
    ctx: typer.Context,
    only_pending: Annotated[
        bool,
        typer.Option(
            "--pending/--all",
            help=(
                "Promote only candidates in status=pending_merge (default), or "
                "re-scan everything - useful after bumping the hint->event map."
            ),
        ),
    ] = True,
    company_id: Annotated[
        str | None,
        typer.Option(
            "--company-id",
            help="Restrict merge to a single company; promote still runs globally.",
        ),
    ] = None,
) -> None:
    """Promote benchmark event candidates and merge duplicate events (Phase 6).

    Two-pass deterministic flow:

    1. Promote eligible ``benchmark_event_candidate`` rows to
       ``company_event`` with ``source_class=benchmark``.
    2. Collapse duplicate ``(company_id, event_type, event_month)`` events by
       provenance precedence (``manual`` > ``first_party`` > ``press`` >
       ``benchmark`` > ``manual_hint``).
    """

    from headcount.db.engine import session_scope
    from headcount.parsers import merge_events, promote_benchmark_events

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.parse_events")

    with session_scope() as session:
        promote = promote_benchmark_events(session, only_pending=only_pending)
        merge = merge_events(session, company_id=company_id)
        if opts.dry_run:
            session.rollback()

    log.info(
        "parse_events_done",
        candidates_considered=promote.candidates_considered,
        promoted=promote.promoted,
        duplicates_of_existing=promote.duplicates_of_existing_event,
        skipped_unresolved=promote.skipped_unresolved,
        skipped_unknown_hint=promote.skipped_unknown_hint,
        skipped_missing_month=promote.skipped_missing_month,
        groups_considered=merge.groups_considered,
        groups_collapsed=merge.groups_collapsed,
        rows_deleted=merge.rows_deleted,
        rows_updated=merge.rows_updated,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"promoted {promote.promoted} events "
        f"(+{promote.duplicates_of_existing_event} dup of existing, "
        f"{promote.skipped_unresolved} unresolved, "
        f"{promote.skipped_unknown_hint} unknown hint, "
        f"{promote.skipped_missing_month} missing month); "
        f"merged {merge.groups_collapsed}/{merge.groups_considered} groups "
        f"({merge.rows_deleted} duplicates removed)"
    )


@app.command("collect-anchors")
def collect_anchors(
    ctx: typer.Context,
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
    source: Annotated[
        list[str] | None,
        typer.Option(
            "--source",
            "-s",
            help="Source name to enable (repeatable). Defaults to manual+sec+wikidata.",
        ),
    ] = None,
    live: Annotated[
        bool,
        typer.Option(
            "--live/--offline",
            help="Hit real network endpoints. Off by default; tests run offline.",
        ),
    ] = False,
    manual_path: Annotated[
        Path | None,
        typer.Option("--manual-path", help="YAML file of manual anchors."),
    ] = None,
    company_limit: Annotated[
        int | None,
        typer.Option("--limit", help="Cap the number of companies processed."),
    ] = None,
) -> None:
    """Gather current-headcount anchors from configured sources (Phase 4)."""
    import asyncio

    from sqlalchemy import select

    from headcount.db.engine import session_scope
    from headcount.db.enums import SourceName
    from headcount.ingest.collect import collect_anchors as _collect
    from headcount.ingest.collect import default_http_configs
    from headcount.ingest.http import FileCache, HttpClient
    from headcount.ingest.observers import (
        CompanyWebObserver,
        LinkedInPublicObserver,
        ManualAnchorObserver,
        SECObserver,
        WaybackObserver,
        WikidataObserver,
    )
    from headcount.models.company import Company

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.collect_anchors")

    requested = (
        [SourceName(name) for name in source]
        if source
        else [SourceName.manual, SourceName.sec, SourceName.wikidata]
    )
    # Live-only sources: make this explicit so nobody accidentally fans
    # out logged-out LinkedIn scrapes (or company-web crawls) during a
    # test/smoke run. Wayback is live-only too: it still makes outbound
    # HTTP, just to archive.org rather than to the target.
    _live_only = {
        SourceName.company_web,
        SourceName.linkedin_public,
        SourceName.wayback,
    }
    if not live:
        dropped = [s for s in requested if s in _live_only]
        for s in dropped:
            log.warning(
                "source_requires_live",
                source=s.value,
                message=f"{s.value} is disabled unless --live is set",
            )
        requested = [s for s in requested if s not in _live_only]
        typer.echo("running in offline mode: only fixture/manual sources will execute")

    adapters: list[object] = []
    for src in requested:
        if src is SourceName.manual:
            adapters.append(
                ManualAnchorObserver(path=manual_path) if manual_path else ManualAnchorObserver()
            )
        elif src is SourceName.sec:
            adapters.append(SECObserver())
        elif src is SourceName.wikidata:
            adapters.append(WikidataObserver())
        elif src is SourceName.company_web:
            adapters.append(CompanyWebObserver())
        elif src is SourceName.linkedin_public:
            adapters.append(LinkedInPublicObserver())
        elif src is SourceName.wayback:
            adapters.append(WaybackObserver())
        else:
            log.warning("unknown_source_ignored", source=src.value)

    async def _run() -> dict[str, object]:
        cache_root = opts.cache_dir if hasattr(opts, "cache_dir") else None
        with session_scope() as session:
            companies = list(
                session.execute(select(Company).order_by(Company.canonical_name)).scalars()
            )
            if company_limit is not None:
                companies = companies[:company_limit]
            from headcount.config.settings import get_settings

            settings = get_settings()
            cache = FileCache(cache_root or settings.cache_dir)
            # Only attach the raw-response sink for live fetches.
            # Offline / canned-transport runs would pollute the archive
            # with test fixtures that have no real upstream provenance.
            raw_sink = None
            if live and _raw_response_archive_enabled():
                from headcount.ingest.raw_response_store import (
                    build_sink_from_session,
                )

                raw_sink = build_sink_from_session(session)
            http = HttpClient(
                cache=cache,
                configs=default_http_configs(),
                transport=None if live else _offline_transport(),
                raw_response_sink=raw_sink,
            )
            result = await _collect(
                session,
                adapters=adapters,  # type: ignore[arg-type]
                companies=companies,
                http_client=http,
            )
            if opts.dry_run:
                session.rollback()
            return result.summary()

    summary = asyncio.run(_run())
    log.info("collect_anchors_done", batch=company_batch, **summary, dry_run=opts.dry_run)
    typer.echo(
        f"run={summary['run_id']} companies={summary['companies_attempted']} "
        f"with-signals={summary['companies_with_signals']} "
        f"gated={summary['companies_gated']} "
        f"linkedin-gated={summary['linkedin_gated_companies']} "
        f"review-items={summary['review_items_enqueued']} "
        f"signals={summary['signals_written']} anchors={summary['anchors_written']} "
        f"errors={summary['errors']}"
    )


def _offline_transport() -> Any:
    """MockTransport that always 404s, so offline runs never leak HTTP."""
    import httpx

    def _handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="offline")

    return httpx.MockTransport(_handler)


@app.command("collect-employment")
def collect_employment(
    ctx: typer.Context,
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
    source: Annotated[
        list[str] | None,
        typer.Option(
            "--source",
            "-s",
            help=(
                "Employment-source hint (repeatable). Supported today: "
                "'benchmark' (default), 'linkedin_ocr' (requires --enable-ocr "
                "and pytesseract)."
            ),
        ),
    ] = None,
    profiles_csv: Annotated[
        Path | None,
        typer.Option(
            "--profiles-csv",
            help=(
                "Optional CSV of public-profile employment rows. Columns: "
                "person_source_key, company_id|company_domain, start_month, "
                "end_month, is_current_role, display_name, job_title, "
                "profile_url, confidence."
            ),
        ),
    ] = None,
    enable_ocr: Annotated[
        bool,
        typer.Option(
            "--enable-ocr/--no-ocr",
            help=(
                "Enable the LinkedIn OCR growth-trend observer. Requires the "
                "'[ocr]' optional dependency group."
            ),
        ),
    ] = False,
    company_limit: Annotated[
        int | None,
        typer.Option("--limit", help="Cap the number of companies processed."),
    ] = None,
) -> None:
    """Gather public employment-history observations.

    This stage is the bridge between raw benchmark data and the
    estimator. It promotes benchmark observations to anchors, optionally
    imports an analyst-supplied CSV of public profiles, and - behind
    ``--enable-ocr`` - dispatches the logged-out LinkedIn OCR observer
    for growth-trend extraction.
    """

    from sqlalchemy import select

    from headcount.db.engine import session_scope
    from headcount.ingest.employment import collect_employment as _collect
    from headcount.models.company import Company

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.collect_employment")

    sources = [s.strip().lower() for s in (source or []) if s.strip()]

    ocr_observer: object | None = None
    if enable_ocr or "linkedin_ocr" in sources:
        try:
            from headcount.ingest.observers.linkedin_ocr import (
                LinkedInGrowthTrendObserver,
            )

            ocr_observer = LinkedInGrowthTrendObserver()
            if "linkedin_ocr" not in sources:
                sources.append("linkedin_ocr")
        except Exception as exc:
            log.warning(
                "ocr_observer_unavailable",
                error=str(exc),
                hint="install the '[ocr]' extra to enable LinkedIn OCR",
            )
            ocr_observer = None

    with session_scope() as session:
        companies = list(
            session.execute(select(Company).order_by(Company.canonical_name)).scalars()
        )
        if company_limit is not None:
            companies = companies[:company_limit]
        ids = [c.id for c in companies] if companies else None
        result = _collect(
            session,
            company_ids=ids,
            profiles_csv=profiles_csv,
            sources=sources,
            note=f"collect-employment batch={company_batch}",
            ocr_observer=ocr_observer,
        )
        if opts.dry_run:
            session.rollback()
        summary = result.summary()

    log.info("collect_employment_done", batch=company_batch, **summary, dry_run=opts.dry_run)
    typer.echo(
        f"run={summary['run_id']} companies={summary['companies_attempted']} "
        f"succeeded={summary['companies_succeeded']} "
        f"benchmark_anchors+={result.benchmark.inserted_anchor_rows} "
        f"csv_rows+={result.csv.rows_imported} "
        f"ocr_signals+={summary['ocr_signals']} "
        f"errors={summary['errors']}"
    )


@app.command("estimate-series")
def estimate_series(
    ctx: typer.Context,
    company_batch: Annotated[
        str,
        typer.Option(
            "--company-batch",
            help="Reserved for future batching; currently informational only.",
        ),
    ] = "priority",
    start_month: Annotated[
        str,
        typer.Option(
            "--start",
            help="First month of the output window, YYYY-MM or YYYY-MM-DD.",
        ),
    ] = "2020-01",
    end_month: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Last month of the output window; defaults to --as-of.",
        ),
    ] = None,
    as_of_month: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            help="Reference 'now' month for coverage and open employments.",
        ),
    ] = None,
    sample_floor: Annotated[
        int,
        typer.Option(
            "--sample-floor",
            help="Months with fewer live profiles than this are suppressed.",
        ),
    ] = 5,
    company_id: Annotated[
        list[str] | None,
        typer.Option(
            "--company-id",
            help="Restrict to one or more company IDs (repeat flag).",
        ),
    ] = None,
) -> None:
    """Produce monthly headcount estimates with intervals (Phase 7)."""

    from datetime import date

    from headcount.db.engine import session_scope
    from headcount.estimate.pipeline import estimate_series as run_estimate

    def _parse_month(raw: str) -> date:
        raw = raw.strip()
        if len(raw) == 7:
            raw = f"{raw}-01"
        try:
            return date.fromisoformat(raw).replace(day=1)
        except ValueError as exc:
            raise typer.BadParameter(f"expected YYYY-MM or YYYY-MM-DD, got {raw!r}") from exc

    start = _parse_month(start_month)
    end = _parse_month(end_month) if end_month else None
    as_of = _parse_month(as_of_month) if as_of_month else None

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.estimate_series")

    with session_scope() as session:
        result = run_estimate(
            session,
            start_month=start,
            end_month=end,
            as_of_month=as_of,
            company_ids=list(company_id) if company_id else None,
            sample_floor=sample_floor,
            note=f"batch={company_batch}",
        )
        if opts.dry_run:
            session.rollback()

    log.info(
        "estimate_series_done",
        run_id=result.run_id,
        companies_attempted=result.companies_attempted,
        companies_succeeded=result.companies_succeeded,
        companies_failed=result.companies_failed,
        companies_degraded=result.companies_degraded,
        months_written=result.months_written,
        months_flagged=result.months_flagged,
        review_items_inserted=result.review_items_inserted,
        review_items_refreshed=result.review_items_refreshed,
        overrides_applied=result.overrides_applied,
        final_status=result.final_status.value,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"estimate_series run={result.run_id} "
        f"status={result.final_status.value} "
        f"companies={result.companies_succeeded}/{result.companies_attempted} "
        f"(failed={result.companies_failed}, degraded={result.companies_degraded}); "
        f"months={result.months_written} flagged={result.months_flagged}; "
        f"review_items=+{result.review_items_inserted}/~{result.review_items_refreshed} "
        f"overrides_applied={result.overrides_applied}"
    )


@app.command("score-confidence")
def score_confidence_cmd(
    ctx: typer.Context,
    company_batch: Annotated[
        str,
        typer.Option(
            "--company-batch",
            help="Reserved for future batching; currently informational only.",
        ),
    ] = "priority",
) -> None:
    """Re-score confidence for the latest EstimateVersion per company (Phase 8).

    This is a no-op convenience wrapper around ``estimate-series`` -
    scoring already happens inline there. We keep the dedicated command
    in the CLI surface so analysts can invoke scoring without re-running
    the full estimate pipeline when a scoring-only parameter changes.
    """

    from sqlalchemy import select

    from headcount.db.engine import session_scope
    from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.score_confidence")

    with session_scope() as session:
        n_rows = session.execute(select(HeadcountEstimateMonthly.id)).scalars().all()
        n_scored = (
            session.execute(
                select(HeadcountEstimateMonthly.id).where(
                    HeadcountEstimateMonthly.confidence_score.is_not(None)
                )
            )
            .scalars()
            .all()
        )
        if opts.dry_run:
            session.rollback()

    log.info(
        "score_confidence_status",
        company_batch=company_batch,
        total_monthly_rows=len(n_rows),
        already_scored=len(n_scored),
    )
    typer.echo(
        f"score-confidence batch={company_batch} "
        f"total_monthly_rows={len(n_rows)} already_scored={len(n_scored)}; "
        f"scoring is run inline by `hc estimate-series`."
    )


@app.command("apply-override")
def apply_override_cmd(
    ctx: typer.Context,
    company_id: Annotated[
        str,
        typer.Option("--company-id", help="Target company id."),
    ],
    field_name: Annotated[
        str,
        typer.Option(
            "--field",
            help=(
                "One of: current_anchor, estimate_suppress_window, event_segment, "
                "canonical_company, company_relation, person_identity_merge."
            ),
        ),
    ],
    payload_json: Annotated[
        str,
        typer.Option(
            "--payload",
            help="Inline JSON payload. See headcount.review.overrides for schemas.",
        ),
    ],
    reason: Annotated[
        str | None,
        typer.Option("--reason", help="Free-text justification recorded on the override."),
    ] = None,
    entered_by: Annotated[
        str | None,
        typer.Option("--by", help="Analyst handle recorded in audit log."),
    ] = None,
    expires_at: Annotated[
        str | None,
        typer.Option("--expires-at", help="Optional ISO-8601 UTC expiry timestamp."),
    ] = None,
) -> None:
    """Write a :class:`ManualOverride` row with a paired audit log entry (Phase 8)."""

    import json
    from datetime import UTC, datetime

    from headcount.db.engine import session_scope
    from headcount.db.enums import OverrideField
    from headcount.models.manual_override import ManualOverride
    from headcount.review.audit import record_audit

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.apply_override")

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--payload must be valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter("--payload must decode to a JSON object.")

    try:
        field = OverrideField(field_name)
    except ValueError as exc:
        raise typer.BadParameter(f"unknown --field: {field_name!r}") from exc

    expires_dt: datetime | None = None
    if expires_at:
        try:
            expires_dt = datetime.fromisoformat(expires_at)
            if expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=UTC)
        except ValueError as exc:
            raise typer.BadParameter(f"--expires-at must be ISO-8601: {expires_at!r}") from exc

    with session_scope() as session:
        row = ManualOverride(
            company_id=company_id,
            field_name=field,
            override_value_json=payload,
            reason=reason,
            entered_by=entered_by,
            expires_at=expires_dt,
        )
        session.add(row)
        session.flush()
        record_audit(
            session,
            actor_type="cli",
            actor_id=entered_by,
            action="override_created",
            target_type="manual_override",
            target_id=row.id,
            payload={
                "company_id": company_id,
                "field": field.value,
                "reason": reason,
                "expires_at": expires_dt.isoformat() if expires_dt else None,
                "payload": payload,
            },
        )
        if opts.dry_run:
            override_id = row.id
            session.rollback()
        else:
            override_id = row.id

    log.info(
        "override_created",
        override_id=override_id,
        company_id=company_id,
        field=field.value,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"override_created id={override_id} company={company_id} "
        f"field={field.value} dry_run={opts.dry_run}"
    )


@app.command("review-queue")
def review_queue_cmd(
    ctx: typer.Context,
    status_filter: Annotated[
        str | None,
        typer.Option(
            "--status",
            help="Filter by status: open, assigned, resolved, dismissed.",
        ),
    ] = "open",
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to print (highest priority first)."),
    ] = 25,
) -> None:
    """List the current analyst review queue, highest priority first (Phase 8)."""

    from sqlalchemy import select

    from headcount.db.engine import session_scope
    from headcount.db.enums import ReviewStatus
    from headcount.models.review_queue_item import ReviewQueueItem

    _ = ctx  # ctx unused but keeps the Typer signature uniform.

    status_enum = None
    if status_filter:
        try:
            status_enum = ReviewStatus(status_filter)
        except ValueError as exc:
            raise typer.BadParameter(f"unknown --status: {status_filter!r}") from exc

    with session_scope() as session:
        stmt = select(ReviewQueueItem).order_by(
            ReviewQueueItem.priority.desc(),
            ReviewQueueItem.updated_at.desc(),
        )
        if status_enum is not None:
            stmt = stmt.where(ReviewQueueItem.status == status_enum)
        stmt = stmt.limit(max(1, limit))
        rows = session.execute(stmt).scalars().all()
        formatted = [
            {
                "id": r.id,
                "company_id": r.company_id,
                "reason": r.review_reason.value,
                "priority": r.priority,
                "status": r.status.value,
                "assigned_to": r.assigned_to,
                "detail": r.detail,
            }
            for r in rows
        ]

    if not formatted:
        typer.echo(f"review-queue empty (status={status_filter or 'any'})")
        return

    for item in formatted:
        typer.echo(
            f"  [{item['priority']:02d}] {item['status']} {item['reason']} "
            f"company={item['company_id']} assigned_to={item['assigned_to']}"
        )
        if item["detail"]:
            typer.echo(f"        {item['detail']}")


@app.command("export-growth")
def export_growth(
    ctx: typer.Context,
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Destination file path."),
    ],
    table: Annotated[
        str,
        typer.Option(
            "--table",
            help="One of: monthly_series, anchors, review_queue, growth_windows.",
        ),
    ] = "monthly_series",
    fmt: Annotated[
        str,
        typer.Option("--format", "-f", help="csv | json | parquet"),
    ] = "csv",
    company_id: Annotated[
        list[str] | None,
        typer.Option("--company-id", help="Restrict to one or more company IDs."),
    ] = None,
    include_resolved: Annotated[
        bool,
        typer.Option(
            "--include-resolved/--open-only",
            help="(review_queue only) include resolved/dismissed rows.",
        ),
    ] = False,
) -> None:
    """Export one of the analyst tables as CSV / JSON / Parquet (Phase 9)."""

    from headcount.db.engine import session_scope
    from headcount.serving.exports import ExportFormatError, export_table

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.export_growth")

    try:
        with session_scope() as session:
            result = export_table(
                session,
                table=table,
                path=output,
                fmt=fmt,
                company_ids=list(company_id) if company_id else None,
                include_resolved=include_resolved,
            )
            if opts.dry_run:
                session.rollback()
    except ExportFormatError as exc:
        raise typer.BadParameter(str(exc)) from exc

    log.info(
        "export_done",
        table=table,
        fmt=result.fmt,
        rows=result.rows,
        path=str(result.path),
        dry_run=opts.dry_run,
    )
    typer.echo(f"exported {result.rows} rows table={table} fmt={result.fmt} -> {result.path}")


@app.command("compare-benchmark")
def compare_benchmark(
    ctx: typer.Context,
    company_id: Annotated[
        list[str] | None,
        typer.Option("--company-id", help="Restrict to one or more company IDs."),
    ] = None,
    threshold: Annotated[
        float,
        typer.Option(
            "--threshold",
            help="Relative-gap threshold over which a benchmark disagrees (0..1).",
        ),
    ] = 0.25,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Optional JSON dump of the summary."),
    ] = None,
) -> None:
    """Compare latest estimates against benchmark observations (Phase 9)."""

    import json as _json

    from headcount.db.engine import session_scope
    from headcount.serving.benchmark_comparison import compare_estimates_to_benchmarks

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.compare_benchmark")

    with session_scope() as session:
        summary = compare_estimates_to_benchmarks(
            session,
            company_ids=list(company_id) if company_id else None,
            threshold=threshold,
        )
        if opts.dry_run:
            session.rollback()

    payload = summary.to_dict()

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(_json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    log.info(
        "compare_benchmark_done",
        companies=summary.companies_total,
        with_benchmarks=summary.companies_with_benchmarks,
        benchmarks=summary.benchmarks_total,
        matched=summary.benchmarks_matched,
        disagreements=summary.disagreements_total,
        threshold=threshold,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"compare-benchmark companies={summary.companies_total} "
        f"with_benchmarks={summary.companies_with_benchmarks} "
        f"benchmarks={summary.benchmarks_total} matched={summary.benchmarks_matched} "
        f"disagreements={summary.disagreements_total} threshold={threshold:.2f}"
    )


@app.command("evaluate")
def evaluate_cmd(
    ctx: typer.Context,
    as_of_month: Annotated[
        str | None,
        typer.Option(
            "--as-of-month",
            help="Reference month for translating 6m/1y/2y anchors (YYYY-MM). "
            "Defaults to the first of the current month.",
        ),
    ] = None,
    company_id: Annotated[
        list[str] | None,
        typer.Option("--company-id", help="Restrict scope to one or more company IDs."),
    ] = None,
    persist: Annotated[
        bool,
        typer.Option(
            "--persist/--no-persist",
            help="Write the scoreboard to ``evaluation_run`` (default: persist).",
        ),
    ] = True,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional JSON path to dump the scoreboard.",
        ),
    ] = None,
    top_disagreements: Annotated[
        int,
        typer.Option(
            "--top-disagreements",
            help="Max number of top disagreements to include.",
        ),
    ] = 25,
    high_confidence_ratio: Annotated[
        float,
        typer.Option(
            "--high-confidence-ratio",
            help=(
                "Relative-gap threshold above which a high/medium band row "
                "counts as a 'high-confidence disagreement' (1.0 = 2x gap)."
            ),
        ),
    ] = 1.0,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Free-text note attached to the evaluation_run row."),
    ] = None,
) -> None:
    """Run the Phase 11 evaluation harness against the current DB state.

    Joins the latest :class:`HeadcountEstimateMonthly` rows with every
    :class:`BenchmarkObservation` that resolves to a known company and
    emits a scoreboard covering coverage, per-provider accuracy,
    growth-window error, review-queue state, and top disagreements.

    The full scoreboard is persisted to ``evaluation_run`` by default
    and can additionally be dumped to JSON via ``--output``.
    """

    import json as _json
    from datetime import UTC, date, datetime

    from headcount.db.engine import session_scope
    from headcount.review.evaluation import (
        EvaluationConfig,
        evaluate_against_benchmarks,
        persist_scoreboard,
    )

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.evaluate")

    if as_of_month:
        as_of = date.fromisoformat(
            as_of_month if len(as_of_month) > 7 else f"{as_of_month}-01"
        ).replace(day=1)
    else:
        today = date.today()
        as_of = today.replace(day=1)

    config = EvaluationConfig(
        top_disagreements_limit=top_disagreements,
        high_confidence_disagreement_ratio=high_confidence_ratio,
    )

    with session_scope() as session:
        scoreboard = evaluate_against_benchmarks(
            session,
            as_of_month=as_of,
            config=config,
            company_ids=list(company_id) if company_id else None,
            evaluated_at=datetime.now(tz=UTC),
        )
        evaluation_id: str | None = None
        if persist and not opts.dry_run:
            evaluation_id = persist_scoreboard(session, scoreboard, note=note)
        if opts.dry_run:
            session.rollback()

    payload = scoreboard.to_dict()
    payload["evaluation_run_id"] = evaluation_id

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            _json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )

    headline_mape = scoreboard.headline_mape()
    headline_mae = scoreboard.headline_growth_mae("1y")
    log.info(
        "evaluate_done",
        evaluation_run_id=evaluation_id,
        as_of=as_of.isoformat(),
        companies_in_scope=scoreboard.companies_in_scope,
        companies_evaluated=scoreboard.companies_evaluated,
        headline_mape=headline_mape,
        headline_growth_mae_1y=headline_mae,
        hc_disagreements=scoreboard.high_confidence_disagreements,
        dry_run=opts.dry_run,
    )
    typer.echo(
        " ".join(
            [
                "evaluate",
                f"as_of={as_of.isoformat()}",
                f"companies={scoreboard.companies_evaluated}/{scoreboard.companies_in_scope}",
                f"with_benchmark={scoreboard.companies_with_benchmark}",
                f"coverage_in_scope={scoreboard.coverage_in_scope:.2%}",
                f"mape_current={_fmt_metric(headline_mape)}",
                f"mae_growth_1y={_fmt_metric(headline_mae)}",
                f"hc_disagreements={scoreboard.high_confidence_disagreements}",
                f"run={evaluation_id or '-'}",
            ]
        )
    )


def _fmt_metric(value: float | None) -> str:
    return f"{value:.4f}" if value is not None else "-"


@app.command("rerun-company")
def rerun_company(
    ctx: typer.Context,
    company_id: Annotated[str, typer.Option("--company-id")],
    start_month: Annotated[str, typer.Option("--start")] = "2020-01",
    end_month: Annotated[str | None, typer.Option("--end")] = None,
    as_of_month: Annotated[str | None, typer.Option("--as-of")] = None,
    sample_floor: Annotated[int, typer.Option("--sample-floor")] = 5,
) -> None:
    """Re-run the estimation pipeline for a single company (Phase 9)."""

    from datetime import date

    from headcount.db.engine import session_scope
    from headcount.estimate.pipeline import estimate_series as run_estimate

    def _parse_month(raw: str) -> date:
        raw = raw.strip()
        if len(raw) == 7:
            raw = f"{raw}-01"
        try:
            return date.fromisoformat(raw).replace(day=1)
        except ValueError as exc:
            raise typer.BadParameter(f"expected YYYY-MM or YYYY-MM-DD, got {raw!r}") from exc

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.rerun_company")
    start = _parse_month(start_month)
    end = _parse_month(end_month) if end_month else None
    as_of = _parse_month(as_of_month) if as_of_month else None

    with session_scope() as session:
        result = run_estimate(
            session,
            start_month=start,
            end_month=end,
            as_of_month=as_of,
            company_ids=[company_id],
            sample_floor=sample_floor,
            note=f"rerun-company={company_id}",
        )
        if opts.dry_run:
            session.rollback()

    log.info(
        "rerun_company_done",
        run_id=result.run_id,
        company_id=company_id,
        companies_succeeded=result.companies_succeeded,
        companies_failed=result.companies_failed,
        months_written=result.months_written,
        final_status=result.final_status.value,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"rerun run={result.run_id} company={company_id} "
        f"status={result.final_status.value} months={result.months_written} "
        f"flagged={result.months_flagged}"
    )


@app.command("status")
def status(
    ctx: typer.Context,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Run to inspect; defaults to the latest."),
    ] = None,
) -> None:
    """Show aggregated ``company_run_status`` for a run (Phase 9)."""

    from sqlalchemy import select

    from headcount.db.engine import session_scope
    from headcount.models.run import CompanyRunStatus, Run

    _ = ctx
    log = get_logger("headcount.cli.status")

    with session_scope() as session:
        if run_id is None:
            run = session.execute(
                select(Run).order_by(Run.started_at.desc()).limit(1)
            ).scalar_one_or_none()
        else:
            run = session.get(Run, run_id)
        if run is None:
            typer.echo("no runs found")
            raise typer.Exit(code=1)
        rows = session.execute(
            select(CompanyRunStatus).where(CompanyRunStatus.run_id == run.id)
        ).scalars().all()
        per_stage: dict[tuple[str, str], int] = {}
        for r in rows:
            key = (r.stage.value, r.status.value)
            per_stage[key] = per_stage.get(key, 0) + 1

    log.info(
        "run_status",
        run_id=run.id,
        status=run.status.value,
        kind=run.kind.value,
        cutoff_month=run.cutoff_month.isoformat(),
        stage_rows=len(rows),
    )
    typer.echo(
        f"run={run.id} kind={run.kind.value} status={run.status.value} "
        f"cutoff={run.cutoff_month.isoformat()} stages={len(per_stage)}"
    )
    if not per_stage:
        typer.echo("  (no company-stage rows)")
        return
    for (stage, stat), count in sorted(per_stage.items()):
        typer.echo(f"  {stage:22s} {stat:10s} {count}")


@app.command("serve")
def serve_cmd(
    ctx: typer.Context,
    host: Annotated[str, typer.Option("--host", help="Bind host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", help="Bind port.")] = 8000,
    reload: Annotated[
        bool,
        typer.Option(
            "--reload/--no-reload", help="Enable uvicorn autoreload (dev only)."
        ),
    ] = False,
) -> None:
    """Start the FastAPI server (``uvicorn apps.api.main:app``, Phase 9)."""

    import uvicorn

    _ = ctx
    log = get_logger("headcount.cli.serve")
    log.info("api_server_starting", host=host, port=port, reload=reload)
    uvicorn.run(
        "apps.api.main:app",
        host=host,
        port=port,
        reload=reload,
    )


@app.command("review-ui")
def review_ui_cmd(
    host: Annotated[
        str, typer.Option("--host", help="Bind host for Streamlit.")
    ] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", help="Bind port.")] = 8501,
    api_url: Annotated[
        str | None,
        typer.Option(
            "--api-url",
            help="FastAPI base URL the UI should talk to. "
            "Also sets HEADCOUNT_API_URL in the subprocess env.",
        ),
    ] = None,
) -> None:
    """Start the Streamlit review UI (Phase 10, ``streamlit run``)."""

    import os
    import subprocess
    import sys
    from pathlib import Path

    script = (
        Path(__file__).resolve().parents[2] / "apps" / "review_ui" / "app.py"
    )
    if not script.exists():
        raise typer.BadParameter(f"review UI entry not found at {script}")
    env = os.environ.copy()
    if api_url:
        env["HEADCOUNT_API_URL"] = api_url
    log = get_logger("headcount.cli.review_ui")
    log.info(
        "review_ui_starting",
        host=host,
        port=port,
        api_url=env.get("HEADCOUNT_API_URL"),
    )
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(script),
        "--server.address",
        host,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
    ]
    subprocess.run(cmd, env=env, check=True)


@app.command("run-pipeline")
def run_pipeline(
    ctx: typer.Context,
    mode: Annotated[
        str,
        typer.Option(
            "--mode",
            help=(
                "Execution mode: 'offline' (no network, default), "
                "'live-safe' (network enabled, LinkedIn + OCR disabled), "
                "'live-full' (full live fan-out; for final runs across "
                "250-2000 companies)."
            ),
        ),
    ] = "offline",
    companies_path: Annotated[
        Path | None,
        typer.Option(
            "--companies",
            help="Optional target-company workbook to seed before running.",
        ),
    ] = None,
    benchmarks_path: Annotated[
        Path | None,
        typer.Option(
            "--benchmarks",
            help="Optional benchmark workbook to load before running.",
        ),
    ] = None,
    profiles_csv: Annotated[
        Path | None,
        typer.Option(
            "--profiles-csv",
            help="Optional analyst CSV of public-profile employment rows.",
        ),
    ] = None,
    start_month: Annotated[
        str, typer.Option("--start", help="First estimate month (YYYY-MM).")
    ] = "2020-01",
    end_month: Annotated[
        str | None,
        typer.Option("--end", help="Last estimate month; defaults to --as-of."),
    ] = None,
    as_of_month: Annotated[
        str | None,
        typer.Option(
            "--as-of", help="Reference 'now' month; defaults to today's month."
        ),
    ] = None,
    sample_floor: Annotated[
        int,
        typer.Option(
            "--sample-floor",
            help="Floor for the per-month public-profile count.",
        ),
    ] = 5,
    company_limit: Annotated[
        int | None,
        typer.Option("--limit", help="Cap companies processed (smoke runs)."),
    ] = None,
    json_output: Annotated[
        Path | None,
        typer.Option(
            "--json-out",
            help=(
                "Write the structured pipeline summary to this path. "
                "Stdout always gets a compact one-liner."
            ),
        ),
    ] = None,
) -> None:
    """Run the full pipeline end-to-end with a single command.

    This is the operational lever: ``--mode offline`` is what you run in
    tests, dev, and smoke; ``--mode live-safe`` enables network
    observers (manual/sec/wikidata ``--live`` + company-web) with
    LinkedIn scraping and OCR still disabled; ``--mode live-full``
    turns everything on for a final 250-2000 company production pass.

    Stages executed (each one is idempotent):

    1. ``seed-companies`` (if ``--companies`` supplied).
    2. ``load-benchmarks`` (if ``--benchmarks`` supplied).
    3. ``canonicalize`` (pending candidates only).
    4. ``parse-events`` (promote + merge).
    5. ``collect-anchors`` (live observers in live modes).
    6. ``collect-employment`` (benchmark promotion + CSV + OCR).
    7. ``estimate-series`` across the full window.

    The final JSON summary includes per-stage timing, per-stage row
    counts, and the final ``run_id`` of the estimate-series run so
    the review UI can deep-link to it.
    """

    import asyncio
    import json
    import time
    from datetime import date

    from sqlalchemy import select

    from headcount.db.engine import session_scope
    from headcount.db.enums import PriorityTier, SourceName
    from headcount.estimate.pipeline import estimate_series as run_estimate
    from headcount.ingest.collect import collect_anchors as _collect_anchors
    from headcount.ingest.collect import default_http_configs
    from headcount.ingest.employment import collect_employment as _collect_emp
    from headcount.ingest.http import FileCache, HttpClient
    from headcount.ingest.observers import (
        CompanyWebObserver,
        LinkedInPublicObserver,
        ManualAnchorObserver,
        SECObserver,
        WaybackObserver,
        WikidataObserver,
    )
    from headcount.ingest.seeds import import_candidates, load_benchmarks
    from headcount.models.company import Company
    from headcount.parsers import merge_events, promote_benchmark_events
    from headcount.resolution import resolve_candidates

    opts: GlobalOptions = ctx.obj
    log = get_logger("headcount.cli.run_pipeline")

    mode_norm = mode.strip().lower()
    if mode_norm not in {"offline", "live-safe", "live-full"}:
        raise typer.BadParameter(
            f"unknown --mode {mode!r}; expected offline|live-safe|live-full"
        )
    live = mode_norm in {"live-safe", "live-full"}
    full_live = mode_norm == "live-full"

    def _parse_month(raw: str) -> date:
        raw = raw.strip()
        if len(raw) == 7:
            raw = f"{raw}-01"
        try:
            return date.fromisoformat(raw).replace(day=1)
        except ValueError as exc:
            raise typer.BadParameter(
                f"expected YYYY-MM or YYYY-MM-DD, got {raw!r}"
            ) from exc

    start = _parse_month(start_month)
    end = _parse_month(end_month) if end_month else None
    as_of = _parse_month(as_of_month) if as_of_month else None

    summary: dict[str, Any] = {
        "mode": mode_norm,
        "dry_run": opts.dry_run,
        "stages": {},
    }

    def _record(name: str, started: float, payload: dict[str, Any]) -> None:
        payload["elapsed_ms"] = int((time.time() - started) * 1000)
        summary["stages"][name] = payload

    async def _run_collect_anchors(
        session: Any, companies: list[Company]
    ) -> dict[str, object]:
        settings = get_settings()
        cache = FileCache(settings.cache_dir)
        raw_sink = None
        if live and _raw_response_archive_enabled():
            from headcount.ingest.raw_response_store import (
                build_sink_from_session,
            )

            raw_sink = build_sink_from_session(session)
        http = HttpClient(
            cache=cache,
            configs=default_http_configs(),
            transport=None if live else _offline_transport(),
            raw_response_sink=raw_sink,
        )
        adapters: list[object] = [ManualAnchorObserver(), SECObserver(), WikidataObserver()]
        if live:
            adapters.append(CompanyWebObserver())
            # Wayback is safe to run whenever live HTTP is allowed: it
            # hits archive.org rather than the target. Gating it behind
            # ``live`` keeps offline tests deterministic; gating it
            # before ``full_live`` means we pull historical anchors in
            # the same configuration that grants us current-month
            # company_web anchors.
            adapters.append(WaybackObserver())
        if full_live:
            adapters.append(LinkedInPublicObserver())
        result = await _collect_anchors(
            session,
            adapters=adapters,  # type: ignore[arg-type]
            companies=companies,
            http_client=http,
        )
        return result.summary()

    ocr_observer: object | None = None
    if full_live:
        try:
            from headcount.ingest.observers.linkedin_ocr import (
                LinkedInGrowthTrendObserver,
            )

            ocr_observer = LinkedInGrowthTrendObserver()
        except Exception as exc:
            log.warning("ocr_observer_unavailable", error=str(exc))

    with session_scope() as session:
        # 1) Seed companies (optional).
        if companies_path is not None:
            t0 = time.time()
            seed_result = import_candidates(session, companies_path)
            _record(
                "seed_companies",
                t0,
                {
                    "workbook": seed_result.workbook,
                    "sheet": seed_result.sheet,
                    "rows_imported": seed_result.rows_imported,
                    "rows_updated": seed_result.rows_updated,
                    "rows_skipped": seed_result.rows_skipped,
                },
            )

        # 2) Load benchmarks (optional).
        if benchmarks_path is not None:
            t0 = time.time()
            bench_result = load_benchmarks(session, benchmarks_path)
            _record(
                "load_benchmarks",
                t0,
                {
                    "workbook": bench_result.workbook,
                    "sheets_loaded": list(bench_result.sheets_loaded),
                    "observations_written": bench_result.observations_written,
                    "event_candidates": bench_result.event_candidates_written,
                },
            )

        # 3) Canonicalize.
        t0 = time.time()
        canon = resolve_candidates(
            session, default_priority_tier=PriorityTier.P1, only_pending=True
        )
        _record(
            "canonicalize",
            t0,
            {
                "candidates_scanned": canon.candidates_scanned,
                "candidates_resolved": canon.candidates_resolved,
                "companies_created": canon.companies_created,
                "aliases_created": canon.aliases_created,
            },
        )

        # 4) Parse events.
        t0 = time.time()
        promote = promote_benchmark_events(session, only_pending=True)
        merge = merge_events(session)
        _record(
            "parse_events",
            t0,
            {
                "promoted": promote.promoted,
                "duplicates_of_existing": promote.duplicates_of_existing_event,
                "groups_collapsed": merge.groups_collapsed,
                "rows_deleted": merge.rows_deleted,
            },
        )

        # Resolve the batch after canonicalization so later stages
        # operate on the full set.
        companies = list(
            session.execute(select(Company).order_by(Company.canonical_name)).scalars()
        )
        if company_limit is not None:
            companies = companies[:company_limit]
        company_ids = [c.id for c in companies]
        summary["companies_in_scope"] = len(companies)

        # 5) Collect anchors.
        t0 = time.time()
        anchor_summary = asyncio.run(_run_collect_anchors(session, companies))
        _record("collect_anchors", t0, dict(anchor_summary))

        # 6) Collect employment (benchmark promotion + optional CSV +
        #    optional OCR in live-full).
        t0 = time.time()
        emp_sources: list[str] = []
        if full_live and ocr_observer is not None:
            emp_sources.append("linkedin_ocr")
        emp_result = _collect_emp(
            session,
            company_ids=company_ids or None,
            profiles_csv=profiles_csv,
            sources=emp_sources,
            note=f"run-pipeline mode={mode_norm}",
            ocr_observer=ocr_observer,
        )
        _record("collect_employment", t0, emp_result.summary())

        # 7) Estimate series over the requested window.
        t0 = time.time()
        est = run_estimate(
            session,
            start_month=start,
            end_month=end,
            as_of_month=as_of,
            company_ids=company_ids or None,
            sample_floor=sample_floor,
            note=f"run-pipeline mode={mode_norm}",
        )
        _record(
            "estimate_series",
            t0,
            {
                "run_id": est.run_id,
                "final_status": est.final_status.value,
                "companies_attempted": est.companies_attempted,
                "companies_succeeded": est.companies_succeeded,
                "companies_failed": est.companies_failed,
                "companies_degraded": est.companies_degraded,
                "months_written": est.months_written,
                "months_flagged": est.months_flagged,
                "review_items_inserted": est.review_items_inserted,
                "overrides_applied": est.overrides_applied,
            },
        )
        summary["final_run_id"] = est.run_id
        summary["final_status"] = est.final_status.value

        if opts.dry_run:
            session.rollback()

    # Stderr/stdout surface: a compact one-liner for humans + optional
    # JSON for automation.
    if json_output is not None:
        json_output.write_text(json.dumps(summary, indent=2, default=str))
    _ = SourceName  # silence unused-import lint when SourceName is dropped
    log.info("run_pipeline_done", **{k: v for k, v in summary.items() if k != "stages"})
    typer.echo(
        f"run-pipeline mode={mode_norm} "
        f"companies={summary.get('companies_in_scope', 0)} "
        f"run_id={summary.get('final_run_id')} "
        f"status={summary.get('final_status')} "
        f"months={summary['stages'].get('estimate_series', {}).get('months_written', 0)}"
    )


@app.command("version")
def version_cmd() -> None:
    """Print version and exit."""
    typer.echo(f"hc {__version__}")


@app.command("config")
def config_cmd() -> None:
    """Print the effective configuration (secrets redacted)."""
    settings = get_settings()
    redacted = settings.model_dump()
    typer.echo_via_pager(_format_settings(redacted))


def _format_settings(data: dict[str, object]) -> str:
    lines = []
    for key in sorted(data):
        lines.append(f"{key} = {data[key]!r}")
    return "\n".join(lines) + "\n"


def _not_yet_implemented(ctx: typer.Context, *, stage: str, **fields: object) -> None:
    """Log a structured stub and exit with code 2 to signal "not implemented"."""
    log = get_logger("headcount.cli")
    opts: GlobalOptions = ctx.obj
    log.warning(
        "stage_not_implemented",
        stage=stage,
        resume=opts.resume,
        limit=opts.limit,
        dry_run=opts.dry_run,
        **fields,
    )
    raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
