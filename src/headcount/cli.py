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
from typing import Annotated

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
) -> None:
    """Resolve canonical companies for a batch (Phase 3)."""
    _not_yet_implemented(ctx, stage="canonicalize", company_batch=company_batch)


@app.command("collect-anchors")
def collect_anchors(
    ctx: typer.Context,
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
    source: Annotated[str | None, typer.Option("--source")] = None,
) -> None:
    """Gather current-headcount anchors from configured sources (Phase 4/7)."""
    _not_yet_implemented(ctx, stage="collect-anchors", company_batch=company_batch, source=source)


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
    company_batch: Annotated[str, typer.Option("--company-batch")] = "priority",
) -> None:
    """Produce monthly headcount estimates with intervals (Phase 7)."""
    _not_yet_implemented(ctx, stage="estimate-series", company_batch=company_batch)


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
