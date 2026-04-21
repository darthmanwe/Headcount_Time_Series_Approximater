"""Employment-history collection orchestrator (Phase 10.5b.3).

This module is the live replacement for the ``_not_yet_implemented``
stub that shipped in Phase 4's ``hc collect-employment`` command.

Responsibilities (per call)
---------------------------

1. **Promote benchmark anchors.** Every ``BenchmarkObservation`` row
   whose ``company_id`` is populated is lifted into a historical
   ``CompanyAnchorObservation`` (see
   :mod:`headcount.parsers.benchmark_anchors`). This is what makes the
   estimator operational *without* licensed per-profile data: with
   just the analyst workbook, we get up to four anchors per company.
2. **Import analyst-supplied profiles CSV (optional).** A simple CSV
   shape lets analysts paste LinkedIn search results, scraped rows, or
   internal snapshots into the pipeline without writing a new adapter.
   Duplicate ``(person_source_key, company_id, start_month)`` tuples
   are skipped so re-runs are idempotent.
3. **Record run state.** Creates a ``Run`` row with kind ``refresh`` and
   one ``CompanyRunStatus`` per targeted company with stage
   ``collect_employment`` so the status dashboard reflects reality
   immediately after this command completes.

The third source - :class:`LinkedInGrowthTrendObserver` (OCR-backed) -
is scaffolded in Phase 10.5c.1 and plugged into the same orchestrator
via :func:`collect_employment` 's ``sources`` parameter.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    CompanyRunStage,
    CompanyRunStageStatus,
    ParseStatus,
    RunKind,
    RunStatus,
    SourceEntityType,
    SourceName,
)
from headcount.models.company import Company
from headcount.models.person import Person
from headcount.models.person_employment_observation import (
    PersonEmploymentObservation,
)
from headcount.models.run import CompanyRunStatus, Run
from headcount.models.source_observation import SourceObservation
from headcount.parsers.benchmark_anchors import (
    PromotionResult,
    promote_benchmark_anchors,
)
from headcount.utils.logging import get_logger

EMPLOYMENT_PARSER_VERSION = "employment_csv_v1"

_log = get_logger("headcount.ingest.employment")


@dataclass(slots=True)
class CsvImportStats:
    """Outcome of the analyst-CSV import phase."""

    rows_read: int = 0
    rows_imported: int = 0
    rows_skipped_missing_company: int = 0
    rows_skipped_bad_date: int = 0
    rows_skipped_duplicate: int = 0
    persons_created: int = 0


@dataclass(slots=True)
class EmploymentCollectResult:
    """Summary returned by :func:`collect_employment`."""

    run_id: str
    companies_attempted: int = 0
    companies_succeeded: int = 0
    companies_failed: int = 0
    benchmark: PromotionResult = field(default_factory=PromotionResult)
    csv: CsvImportStats = field(default_factory=CsvImportStats)
    ocr_signals: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "companies_attempted": self.companies_attempted,
            "companies_succeeded": self.companies_succeeded,
            "companies_failed": self.companies_failed,
            "benchmark": self.benchmark.as_dict(),
            "csv": {
                "rows_read": self.csv.rows_read,
                "rows_imported": self.csv.rows_imported,
                "rows_skipped_missing_company": (
                    self.csv.rows_skipped_missing_company
                ),
                "rows_skipped_bad_date": self.csv.rows_skipped_bad_date,
                "rows_skipped_duplicate": self.csv.rows_skipped_duplicate,
                "persons_created": self.csv.persons_created,
            },
            "ocr_signals": self.ocr_signals,
            "errors": len(self.errors),
        }


def _month_floor(d: date) -> date:
    return d.replace(day=1)


def _parse_month(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    # Accept ISO date ``YYYY-MM-DD`` or month-only ``YYYY-MM``.
    try:
        return _month_floor(date.fromisoformat(text))
    except ValueError:
        pass
    try:
        year, month = text.split("-", 1)
        return date(int(year), int(month), 1)
    except (ValueError, TypeError):
        return None


def _parse_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "t"}


def _resolve_company(
    row: dict[str, str],
    *,
    by_id: dict[str, Company],
    by_domain: dict[str, Company],
) -> Company | None:
    cid = (row.get("company_id") or "").strip()
    if cid and cid in by_id:
        return by_id[cid]
    domain = (row.get("company_domain") or "").strip().lower()
    if domain and domain in by_domain:
        return by_domain[domain]
    return None


def _get_or_create_person(
    session: Session,
    *,
    source_name: SourceName,
    source_person_key: str,
    display_name: str | None,
    profile_url: str | None,
) -> tuple[Person, bool]:
    stmt = select(Person).where(
        Person.source_name == source_name,
        Person.source_person_key == source_person_key,
    )
    existing = session.execute(stmt).scalar_one_or_none()
    if existing is not None:
        return existing, False
    person = Person(
        source_name=source_name,
        source_person_key=source_person_key,
        display_name=display_name,
        profile_url=profile_url,
    )
    session.add(person)
    session.flush()
    return person, True


def _employment_already_imported(
    session: Session,
    *,
    person_id: str,
    company_id: str,
    start_month: date,
) -> bool:
    stmt = select(PersonEmploymentObservation.id).where(
        PersonEmploymentObservation.person_id == person_id,
        PersonEmploymentObservation.company_id == company_id,
        PersonEmploymentObservation.start_month == start_month,
    )
    return session.execute(stmt).first() is not None


def import_profiles_csv(
    session: Session,
    *,
    csv_path: Path,
    company_scope: Sequence[Company] | None = None,
) -> CsvImportStats:
    """Import a CSV of public-profile employment rows.

    Expected columns (header row required, case-insensitive):

    - ``person_source_key`` (required): stable per-person key. When
      combined with ``source_name`` it uniquely identifies the person.
    - ``source_name`` (optional; default ``linkedin_public``).
    - ``company_id`` OR ``company_domain`` (one required).
    - ``start_month`` (required; ISO ``YYYY-MM-DD`` or ``YYYY-MM``).
    - ``end_month`` (optional).
    - ``is_current_role`` (optional; ``true``/``false`` - defaults to
      ``true`` iff ``end_month`` is empty).
    - ``display_name`` (optional).
    - ``job_title`` (optional).
    - ``profile_url`` (optional).
    - ``confidence`` (optional float; default 0.55).

    Idempotent: rows whose ``(person, company, start_month)`` already
    exist are counted as duplicates and skipped.
    """

    stats = CsvImportStats()

    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    if company_scope is None:
        companies = list(session.execute(select(Company)).scalars())
    else:
        companies = list(company_scope)
    by_id = {c.id: c for c in companies}
    by_domain = {
        (c.canonical_domain or "").lower(): c
        for c in companies
        if c.canonical_domain
    }

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        normalized_fields = {
            (name or "").strip().lower(): name for name in (reader.fieldnames or [])
        }
        required = {"person_source_key", "start_month"}
        missing = required - set(normalized_fields)
        if missing:
            raise ValueError(
                f"profiles CSV missing required columns: {sorted(missing)}"
            )

        for raw_row in reader:
            stats.rows_read += 1
            row = {
                (k or "").strip().lower(): (v or "").strip()
                for k, v in raw_row.items()
            }

            key = row.get("person_source_key", "")
            if not key:
                stats.rows_skipped_missing_company += 1
                continue

            company = _resolve_company(row, by_id=by_id, by_domain=by_domain)
            if company is None:
                stats.rows_skipped_missing_company += 1
                continue

            start = _parse_month(row.get("start_month"))
            if start is None:
                stats.rows_skipped_bad_date += 1
                continue
            end = _parse_month(row.get("end_month"))
            if end is not None and end < start:
                stats.rows_skipped_bad_date += 1
                continue

            is_current_raw = row.get("is_current_role")
            is_current = _parse_bool(is_current_raw) if is_current_raw else end is None

            source_label = (row.get("source_name") or "linkedin_public").strip()
            try:
                src_enum = SourceName(source_label)
            except ValueError:
                src_enum = SourceName.linkedin_public

            person, created = _get_or_create_person(
                session,
                source_name=src_enum,
                source_person_key=key,
                display_name=row.get("display_name") or None,
                profile_url=row.get("profile_url") or None,
            )
            if created:
                stats.persons_created += 1

            if _employment_already_imported(
                session,
                person_id=person.id,
                company_id=company.id,
                start_month=start,
            ):
                stats.rows_skipped_duplicate += 1
                continue

            confidence_raw = row.get("confidence")
            try:
                confidence = float(confidence_raw) if confidence_raw else 0.55
            except ValueError:
                confidence = 0.55

            session.add(
                PersonEmploymentObservation(
                    person_id=person.id,
                    company_id=company.id,
                    observed_company_name=row.get("display_name") or None,
                    job_title=row.get("job_title") or None,
                    start_month=start,
                    end_month=end,
                    is_current_role=is_current,
                    confidence=max(0.0, min(1.0, confidence)),
                )
            )
            stats.rows_imported += 1

    # Record a single provenance row per CSV batch so downstream audit
    # tooling can trace the import source.
    if stats.rows_imported > 0:
        session.add(
            SourceObservation(
                source_name=SourceName.manual,
                entity_type=SourceEntityType.person_profile,
                source_url=f"file://{csv_path.resolve()}",
                observed_at=datetime.now(tz=UTC),
                raw_text=None,
                raw_content_hash=f"csv:{csv_path.name}:{stats.rows_imported}",
                parser_version=EMPLOYMENT_PARSER_VERSION,
                parse_status=ParseStatus.ok,
                normalized_payload_json={
                    "rows_read": stats.rows_read,
                    "rows_imported": stats.rows_imported,
                    "rows_skipped_missing_company": (
                        stats.rows_skipped_missing_company
                    ),
                    "rows_skipped_bad_date": stats.rows_skipped_bad_date,
                    "rows_skipped_duplicate": stats.rows_skipped_duplicate,
                },
            )
        )
    return stats


def _create_run(
    session: Session,
    *,
    note: str | None,
) -> Run:
    run = Run(
        kind=RunKind.refresh,
        status=RunStatus.running,
        started_at=datetime.now(tz=UTC),
        cutoff_month=_month_floor(date.today()),
        method_version="n/a",
        anchor_policy_version="n/a",
        coverage_curve_version="n/a",
        config_hash="collect-employment",
        note=note or "collect-employment",
    )
    session.add(run)
    session.flush()
    return run


def _ensure_stage_row(
    session: Session, *, run_id: str, company_id: str
) -> CompanyRunStatus:
    row = CompanyRunStatus(
        run_id=run_id,
        company_id=company_id,
        stage=CompanyRunStage.collect_employment,
        status=CompanyRunStageStatus.running,
        attempts=1,
    )
    session.add(row)
    session.flush()
    return row


def collect_employment(
    session: Session,
    *,
    company_ids: Sequence[str] | None = None,
    profiles_csv: Path | None = None,
    sources: Iterable[str] = (),
    note: str | None = None,
    ocr_observer: object | None = None,
    benchmark_skip_providers: Iterable[object] | None = None,
) -> EmploymentCollectResult:
    """Run the employment-collection stage over a batch of companies.

    Ordering within one call:

    1. Promote benchmark observations -> anchors.
    2. Import ``profiles_csv`` (if supplied).
    3. Dispatch OCR observer (if supplied and ``linkedin_ocr`` in
       ``sources``).

    The caller owns the unit of work - no commit is issued here. If any
    individual company fails we mark its ``CompanyRunStatus`` row failed
    and continue; the overall ``Run`` moves to ``partial`` / ``failed``
    as appropriate.
    """

    run = _create_run(session, note=note)
    result = EmploymentCollectResult(run_id=run.id)

    stmt = select(Company).order_by(Company.canonical_name)
    if company_ids is not None:
        stmt = stmt.where(Company.id.in_(list(company_ids)))
    companies = list(session.execute(stmt).scalars())
    result.companies_attempted = len(companies)

    stage_rows: dict[str, CompanyRunStatus] = {}
    for c in companies:
        stage_rows[c.id] = _ensure_stage_row(
            session, run_id=run.id, company_id=c.id
        )

    try:
        result.benchmark = promote_benchmark_anchors(
            session,
            company_ids=[c.id for c in companies] if companies else None,
            skip_providers=benchmark_skip_providers,  # type: ignore[arg-type]
        )
    except Exception as exc:  # pragma: no cover - best-effort safety net
        _log.exception("benchmark_promotion_failed", error=str(exc))
        result.errors.append(f"benchmark_promotion_failed:{exc}")

    if profiles_csv is not None:
        try:
            result.csv = import_profiles_csv(
                session,
                csv_path=profiles_csv,
                company_scope=companies or None,
            )
        except FileNotFoundError as exc:
            result.errors.append(f"profiles_csv_missing:{exc}")
        except Exception as exc:  # pragma: no cover
            _log.exception("profiles_csv_failed", error=str(exc))
            result.errors.append(f"profiles_csv_failed:{exc}")

    if "linkedin_ocr" in {s.strip().lower() for s in sources}:
        try:
            if ocr_observer is None:
                # The OCR observer is intentionally optional; when not
                # supplied we treat the request as a no-op rather than
                # fail the whole stage. Phase 10.5c.1 wires the real
                # observer into the CLI via `--enable-ocr`.
                _log.info("linkedin_ocr_requested_without_observer")
            else:
                run_method = getattr(ocr_observer, "collect", None)
                if callable(run_method):
                    written = run_method(session, companies=companies)
                    try:
                        result.ocr_signals = int(written)
                    except (TypeError, ValueError):
                        result.ocr_signals = 0
        except Exception as exc:  # pragma: no cover
            _log.exception("ocr_observer_failed", error=str(exc))
            result.errors.append(f"ocr_observer_failed:{exc}")

    for c in companies:
        stage = stage_rows[c.id]
        if result.errors and stage.last_error is None:
            # Attach a short summary to each stage so the status
            # dashboard surfaces issues per-company rather than only
            # at the run level.
            stage.last_error = "; ".join(result.errors)[:2048]
            stage.status = CompanyRunStageStatus.failed
            result.companies_failed += 1
        else:
            stage.status = CompanyRunStageStatus.succeeded
            result.companies_succeeded += 1

    if result.companies_failed == 0:
        run.status = RunStatus.succeeded
    elif result.companies_failed == result.companies_attempted:
        run.status = RunStatus.failed
    else:
        run.status = RunStatus.partial
    run.finished_at = datetime.now(tz=UTC)
    session.flush()
    return result


__all__ = [
    "EMPLOYMENT_PARSER_VERSION",
    "CsvImportStats",
    "EmploymentCollectResult",
    "collect_employment",
    "import_profiles_csv",
]
