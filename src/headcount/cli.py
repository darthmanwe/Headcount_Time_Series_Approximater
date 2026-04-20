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
    # test/smoke run.
    _live_only = {SourceName.company_web, SourceName.linkedin_public}
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
            http = HttpClient(
                cache=cache,
                configs=default_http_configs(),
                transport=None if live else _offline_transport(),
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
    source: Annotated[str | None, typer.Option("--source")] = None,
) -> None:
    """Gather public employment-history observations (Phase 5/6)."""
    _not_yet_implemented(
        ctx, stage="collect-employment", company_batch=company_batch, source=source
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
        final_status=result.final_status.value,
        dry_run=opts.dry_run,
    )
    typer.echo(
        f"estimate_series run={result.run_id} "
        f"status={result.final_status.value} "
        f"companies={result.companies_succeeded}/{result.companies_attempted} "
        f"(failed={result.companies_failed}, degraded={result.companies_degraded}); "
        f"months={result.months_written} flagged={result.months_flagged}"
    )


@app.command("score-confidence")
def score_confidence(
    ctx: typer.Context,
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
) -> None:
    """Score confidence components and final bands (Phase 8)."""
    _not_yet_implemented(ctx, stage="score-confidence", company_batch=company_batch)


@app.command("export-growth")
def export_growth(
    ctx: typer.Context,
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
    fmt: Annotated[str, typer.Option("--format", "-f")] = "csv",
) -> None:
    """Export the growth table as CSV or parquet (Phase 9)."""
    _not_yet_implemented(ctx, stage="export-growth", company_batch=company_batch, fmt=fmt)


@app.command("compare-benchmark")
def compare_benchmark(
    ctx: typer.Context,
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
) -> None:
    """Compare system outputs vs ``test_source/`` benchmark providers (Phase 9)."""
    _not_yet_implemented(ctx, stage="compare-benchmark", company_batch=company_batch)


@app.command("rerun-company")
def rerun_company(
    ctx: typer.Context,
    company_id: Annotated[str, typer.Option("--company-id")],
) -> None:
    """Re-run the full pipeline for a single company (Phase 7/9)."""
    _not_yet_implemented(ctx, stage="rerun-company", company_id=company_id)


@app.command("status")
def status(
    ctx: typer.Context,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
) -> None:
    """Show aggregated ``company_run_status`` for a run (Phase 9)."""
    _not_yet_implemented(ctx, stage="status", run_id=run_id)


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
