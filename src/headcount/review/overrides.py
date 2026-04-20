"""Manual override application.

Analysts can write :class:`ManualOverride` rows that pin anchor values,
force event segmentation, or suppress estimate windows. The pipeline
consults this module before reconciling so overrides influence the
output without mutating raw observations.

Override scopes (by :class:`~headcount.db.enums.OverrideField`)
---------------------------------------------------------------

- ``canonical_company``: used by the resolver, not the estimator.
- ``current_anchor``: replace the ``current_headcount_anchor`` for a
  specific month with an analyst-provided interval.
- ``event_segment``: add a synthetic event that forces a hard-break
  segment boundary. Useful when an undocumented acquisition
  invalidates smoothing.
- ``estimate_suppress_window``: mark every month in a
  ``(start_month, end_month)`` window as
  :class:`~headcount.db.enums.EstimateMethod.suppressed_low_sample`
  regardless of what the signals say. Useful when an anchor is known
  to be wrong and we haven't yet collected a correction.
- ``company_relation``: parent/sub relation override, resolver domain.
- ``person_identity_merge``: person-identity override, resolver domain.

Expired overrides (``expires_at < now``) are silently dropped so stale
manual fixes can't linger forever.

This module intentionally does not *apply* the overrides directly; it
loads and validates them, and the pipeline invokes specific helpers
(e.g. :func:`apply_current_anchor_overrides`) at the right moments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    EventSourceClass,
    EventType,
    HeadcountValueKind,
    OverrideField,
)
from headcount.estimate.anchors import AnchorCandidate
from headcount.models.company_event import CompanyEvent
from headcount.models.manual_override import ManualOverride

OVERRIDES_VERSION = "overrides_v1"
"""Stamped into audit payloads when overrides are applied."""


@dataclass(frozen=True, slots=True)
class SuppressWindow:
    start_month: date
    end_month: date
    reason: str


@dataclass(frozen=True, slots=True)
class AnchorPin:
    """Analyst-pinned anchor interval for a specific month."""

    anchor_month: date
    value_min: float
    value_point: float
    value_max: float
    confidence: float
    note: str
    override_id: str


@dataclass(frozen=True, slots=True)
class SyntheticEvent:
    """Synthetic event injected via ``event_segment`` override."""

    event_month: date
    event_type: EventType
    reason: str


@dataclass(frozen=True, slots=True)
class ActiveOverrides:
    """Overrides relevant to one company at one ``as_of`` instant."""

    anchor_pins: tuple[AnchorPin, ...] = field(default_factory=tuple)
    suppress_windows: tuple[SuppressWindow, ...] = field(default_factory=tuple)
    synthetic_events: tuple[SyntheticEvent, ...] = field(default_factory=tuple)
    override_ids: tuple[str, ...] = field(default_factory=tuple)

    def is_suppressed(self, month: date) -> SuppressWindow | None:
        for w in self.suppress_windows:
            if w.start_month <= month <= w.end_month:
                return w
        return None

    def merged_into_anchors(self, anchors: list[AnchorCandidate]) -> list[AnchorCandidate]:
        """Merge pinned anchors into a list of :class:`AnchorCandidate`.

        Manual anchors always win at the reconciliation step because
        :attr:`AnchorType.manual_anchor` has the top precedence score.
        We keep any existing anchors around so review UI can still see
        what the analyst overrode.
        """

        merged = list(anchors)
        for pin in self.anchor_pins:
            merged.append(
                AnchorCandidate(
                    anchor_month=pin.anchor_month,
                    value_min=pin.value_min,
                    value_point=pin.value_point,
                    value_max=pin.value_max,
                    kind=HeadcountValueKind.exact,
                    anchor_type=AnchorType.manual_anchor,
                    confidence=pin.confidence,
                    source_name="manual_override",
                    observation_id=pin.override_id,
                )
            )
        return merged

    def merged_into_events(self, events: list[CompanyEvent]) -> list[CompanyEvent]:
        """Augment canonical events with synthetic ones from overrides.

        These synthetic events are not persisted - they're only used
        by :func:`split_into_segments` during this run. Callers should
        note in the audit log that the pipeline consumed them.
        """

        augmented = list(events)
        for syn in self.synthetic_events:
            augmented.append(
                CompanyEvent(
                    company_id=events[0].company_id if events else None,
                    event_type=syn.event_type,
                    event_month=syn.event_month,
                    source_class=EventSourceClass.manual,
                    confidence=1.0,
                    description=f"synthetic: {syn.reason}",
                )
            )
        return augmented


def _as_float(payload: dict[str, Any], key: str, default: float | None = None) -> float:
    value = payload.get(key, default)
    if value is None:
        raise ValueError(f"override payload missing required key {key!r}")
    return float(value)


def _parse_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"unparseable date in override payload: {value!r}")


def _active(ov: ManualOverride, *, now: datetime) -> bool:
    if ov.expires_at is None:
        return True
    expires = ov.expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    return expires > now


def load_active_overrides(
    session: Session,
    company_id: str,
    *,
    now: datetime | None = None,
) -> ActiveOverrides:
    """Load every non-expired override for a company and normalize it."""

    when = now or datetime.now(tz=UTC)

    rows = (
        session.execute(
            select(ManualOverride)
            .where(ManualOverride.company_id == company_id)
            .order_by(ManualOverride.created_at)
        )
        .scalars()
        .all()
    )

    pins: list[AnchorPin] = []
    windows: list[SuppressWindow] = []
    synthetic: list[SyntheticEvent] = []
    ids: list[str] = []

    for ov in rows:
        if not _active(ov, now=when):
            continue
        ids.append(ov.id)
        payload = dict(ov.override_value_json or {})
        if ov.field_name is OverrideField.current_anchor:
            pins.append(
                AnchorPin(
                    anchor_month=_parse_date(payload["anchor_month"]),
                    value_min=_as_float(payload, "value_min"),
                    value_point=_as_float(payload, "value_point"),
                    value_max=_as_float(payload, "value_max"),
                    confidence=float(payload.get("confidence", 1.0)),  # type: ignore[arg-type]
                    note=ov.reason or "",
                    override_id=ov.id,
                )
            )
        elif ov.field_name is OverrideField.estimate_suppress_window:
            windows.append(
                SuppressWindow(
                    start_month=_parse_date(payload["start_month"]),
                    end_month=_parse_date(payload["end_month"]),
                    reason=ov.reason or "manual_suppress",
                )
            )
        elif ov.field_name is OverrideField.event_segment:
            ev_type_raw = str(payload.get("event_type", "acquisition"))
            try:
                ev_type = EventType(ev_type_raw)
            except ValueError:
                ev_type = EventType.acquisition
            synthetic.append(
                SyntheticEvent(
                    event_month=_parse_date(payload["event_month"]),
                    event_type=ev_type,
                    reason=ov.reason or "manual_event_segment",
                )
            )
        # canonical_company / company_relation / person_identity_merge
        # are resolver-level overrides and are intentionally ignored
        # here.

    return ActiveOverrides(
        anchor_pins=tuple(pins),
        suppress_windows=tuple(windows),
        synthetic_events=tuple(synthetic),
        override_ids=tuple(ids),
    )


__all__ = [
    "OVERRIDES_VERSION",
    "ActiveOverrides",
    "AnchorPin",
    "SuppressWindow",
    "SyntheticEvent",
    "load_active_overrides",
]
