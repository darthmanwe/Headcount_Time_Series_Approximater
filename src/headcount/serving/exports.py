"""Deterministic export helpers.

CSV / JSON / Parquet dumps of the three analyst-facing tables:

- ``monthly_series``: latest ``HeadcountEstimateMonthly`` per company.
- ``anchors``: ``CompanyAnchorObservation`` with source metadata.
- ``review_queue``: all ``ReviewQueueItem`` rows with company name.

All exporters are pure in/out: they read the DB once, build a list of
``dict[str, Any]`` rows, and write to disk. Parquet is optional and only
triggered when ``pandas`` + a parquet engine are importable; CSV is
always available because we hand-roll it (the stdlib :mod:`csv` module
has no external dependencies).
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.review_queue_item import ReviewQueueItem
from headcount.models.source_observation import SourceObservation

EXPORT_VERSION = "export_v1"


class ExportFormatError(ValueError):
    """Raised when the caller asks for a format we cannot produce."""


@dataclass(slots=True)
class ExportResult:
    path: Path
    rows: int
    fmt: str


def _jsonify(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    return value


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _jsonify(v) for k, v in row.items()})


def _write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(rows, default=_jsonify, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ExportFormatError(
            "parquet export requires pandas + pyarrow (or fastparquet); "
            "install the 'export' extras"
        ) from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)


_WRITERS = {
    "csv": _write_csv,
    "json": _write_json,
    "parquet": _write_parquet,
}


def _latest_version_per_company(session: Session) -> dict[str, str]:
    """Return ``{company_id: latest estimate_version_id}``.

    Ordered by ``created_at DESC`` so the first row per company wins. No
    SQL window function needed; we scan once in Python which keeps the
    query portable across SQLite and Postgres.
    """

    stmt = select(EstimateVersion).order_by(
        EstimateVersion.company_id, EstimateVersion.created_at.desc()
    )
    out: dict[str, str] = {}
    for ev in session.execute(stmt).scalars():
        out.setdefault(ev.company_id, ev.id)
    return out


def _monthly_series_rows(
    session: Session, *, company_ids: Iterable[str] | None = None
) -> list[dict[str, Any]]:
    version_map = _latest_version_per_company(session)
    if not version_map:
        return []
    ids = set(company_ids) if company_ids is not None else None
    version_ids = [v for cid, v in version_map.items() if ids is None or cid in ids]
    if not version_ids:
        return []
    stmt = (
        select(HeadcountEstimateMonthly, Company)
        .join(Company, Company.id == HeadcountEstimateMonthly.company_id)
        .where(HeadcountEstimateMonthly.estimate_version_id.in_(version_ids))
        .order_by(Company.canonical_name, HeadcountEstimateMonthly.month)
    )
    rows: list[dict[str, Any]] = []
    for est, company in session.execute(stmt).all():
        rows.append(
            {
                "company_id": company.id,
                "canonical_name": company.canonical_name,
                "month": est.month.isoformat(),
                "estimated_headcount": float(est.estimated_headcount),
                "estimated_headcount_min": float(est.estimated_headcount_min),
                "estimated_headcount_max": float(est.estimated_headcount_max),
                "public_profile_count": int(est.public_profile_count),
                "method": est.method.value,
                "confidence_band": est.confidence_band.value,
                "confidence_score": est.confidence_score,
                "needs_review": bool(est.needs_review),
                "suppression_reason": est.suppression_reason,
                "estimate_version_id": est.estimate_version_id,
            }
        )
    return rows


def _anchors_rows(
    session: Session, *, company_ids: Iterable[str] | None = None
) -> list[dict[str, Any]]:
    stmt = (
        select(CompanyAnchorObservation, Company, SourceObservation)
        .join(Company, Company.id == CompanyAnchorObservation.company_id)
        .join(
            SourceObservation,
            SourceObservation.id == CompanyAnchorObservation.source_observation_id,
            isouter=True,
        )
        .order_by(Company.canonical_name, CompanyAnchorObservation.anchor_month)
    )
    if company_ids is not None:
        stmt = stmt.where(CompanyAnchorObservation.company_id.in_(list(company_ids)))
    rows: list[dict[str, Any]] = []
    for anchor, company, source in session.execute(stmt).all():
        rows.append(
            {
                "company_id": company.id,
                "canonical_name": company.canonical_name,
                "anchor_month": anchor.anchor_month.isoformat(),
                "anchor_type": anchor.anchor_type.value,
                "value_min": float(anchor.headcount_value_min),
                "value_point": float(anchor.headcount_value_point),
                "value_max": float(anchor.headcount_value_max),
                "value_kind": anchor.headcount_value_kind.value,
                "confidence": float(anchor.confidence),
                "source_name": source.source_name.value if source else None,
                "source_url": source.source_url if source else None,
            }
        )
    return rows


def _review_queue_rows(
    session: Session, *, include_resolved: bool = False
) -> list[dict[str, Any]]:
    from headcount.db.enums import ReviewStatus

    stmt = (
        select(ReviewQueueItem, Company)
        .join(Company, Company.id == ReviewQueueItem.company_id)
        .order_by(ReviewQueueItem.priority.desc(), ReviewQueueItem.updated_at.desc())
    )
    if not include_resolved:
        stmt = stmt.where(
            ReviewQueueItem.status.in_(
                (ReviewStatus.open, ReviewStatus.assigned)
            )
        )
    rows: list[dict[str, Any]] = []
    for item, company in session.execute(stmt).all():
        rows.append(
            {
                "id": item.id,
                "company_id": company.id,
                "canonical_name": company.canonical_name,
                "review_reason": item.review_reason.value,
                "priority": int(item.priority),
                "status": item.status.value,
                "assigned_to": item.assigned_to,
                "estimate_version_id": item.estimate_version_id,
                "detail": item.detail,
                "updated_at": item.updated_at.isoformat() if item.updated_at else None,
            }
        )
    return rows


_ROW_BUILDERS = {
    "monthly_series": _monthly_series_rows,
    "anchors": _anchors_rows,
    "review_queue": _review_queue_rows,
}


def export_table(
    session: Session,
    *,
    table: str,
    path: Path,
    fmt: str = "csv",
    company_ids: Iterable[str] | None = None,
    include_resolved: bool = False,
) -> ExportResult:
    """Dump one of the canonical analyst tables to disk.

    ``table`` must be one of ``monthly_series``, ``anchors``,
    ``review_queue``. Format is ``csv``, ``json``, or ``parquet``.
    """

    fmt = fmt.lower()
    if fmt not in _WRITERS:
        raise ExportFormatError(f"unknown format {fmt!r}; supported: {sorted(_WRITERS)}")
    if table not in _ROW_BUILDERS:
        raise ExportFormatError(f"unknown table {table!r}; supported: {sorted(_ROW_BUILDERS)}")

    if table == "review_queue":
        rows = _review_queue_rows(session, include_resolved=include_resolved)
    else:
        rows = _ROW_BUILDERS[table](session, company_ids=company_ids)  # type: ignore[operator]

    _WRITERS[fmt](path, rows)
    return ExportResult(path=path, rows=len(rows), fmt=fmt)


__all__ = [
    "EXPORT_VERSION",
    "ExportFormatError",
    "ExportResult",
    "export_table",
]
