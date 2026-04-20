"""Manual analyst anchors loaded from YAML.

The YAML file is structured as a list of entries keyed on a company
canonical name, domain, or LinkedIn slug. This is the "break-glass"
source analysts use when they hand-research a company from a press
release, an SEC filing the SEC observer couldn't parse, or a news
article we want to cite verbatim. Because entries are hand-written we
trust them more than scraped sources - ``confidence`` defaults to
``0.95``.

Schema (per entry)::

    - canonical_name: Acme Corp           # optional if `domain` given
      domain: acme.com                    # optional if `canonical_name` given
      linkedin_slug: acme                 # optional additional key
      anchor_month: 2026-04-01            # first-of-month UTC
      headcount:
        min: 1200
        point: 1250
        max: 1300
        kind: exact | range | bucket
      confidence: 0.95                    # optional, defaults to 0.95
      note: "2026 Q1 earnings call"       # required - analyst must say why
      source_url: "https://..."           # optional supporting link
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.ingest.base import (
    AnchorSourceAdapter,
    CompanyTarget,
    FetchContext,
    RawAnchorSignal,
)
from headcount.resolution.normalize import (
    normalize_domain,
    normalize_linkedin_slug,
    normalize_name_key,
)
from headcount.utils.logging import get_logger

_log = get_logger("headcount.ingest.observers.manual")


@dataclass(slots=True)
class _ManualEntry:
    canonical_name: str | None
    domain: str | None
    linkedin_slug: str | None
    anchor_month: date
    value_min: float
    value_point: float
    value_max: float
    value_kind: HeadcountValueKind
    confidence: float
    note: str
    source_url: str | None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> _ManualEntry:
        headcount = raw.get("headcount") or {}
        if not headcount:
            raise ValueError(f"entry missing 'headcount' block: {raw!r}")
        value_min = float(headcount["min"])
        value_point = float(headcount["point"])
        value_max = float(headcount["max"])
        kind = HeadcountValueKind(headcount.get("kind", HeadcountValueKind.exact.value))
        note = str(raw.get("note") or "").strip()
        if not note:
            raise ValueError(f"manual entry requires non-empty 'note': {raw!r}")
        month_val = raw.get("anchor_month")
        if isinstance(month_val, date):
            anchor_month = month_val.replace(day=1)
        elif isinstance(month_val, str):
            anchor_month = date.fromisoformat(month_val).replace(day=1)
        else:
            raise ValueError(f"anchor_month must be a date or ISO string: {raw!r}")
        return cls(
            canonical_name=(
                str(raw.get("canonical_name")).strip() if raw.get("canonical_name") else None
            ),
            domain=normalize_domain(raw.get("domain")),
            linkedin_slug=normalize_linkedin_slug(raw.get("linkedin_slug")),
            anchor_month=anchor_month,
            value_min=value_min,
            value_point=value_point,
            value_max=value_max,
            value_kind=kind,
            confidence=float(raw.get("confidence", 0.95)),
            note=note,
            source_url=(str(raw["source_url"]) if raw.get("source_url") else None),
        )

    def matches(self, target: CompanyTarget) -> bool:
        if (
            self.domain
            and target.canonical_domain
            and normalize_domain(target.canonical_domain) == self.domain
        ):
            return True
        if (
            self.linkedin_slug
            and target.linkedin_company_url
            and normalize_linkedin_slug(target.linkedin_company_url) == self.linkedin_slug
        ):
            return True
        if self.canonical_name:
            entry_key = normalize_name_key(self.canonical_name)
            if entry_key and entry_key == normalize_name_key(target.canonical_name):
                return True
            for alias in target.aliases:
                if entry_key and entry_key == normalize_name_key(alias):
                    return True
        return False


def load_manual_entries(path: Path) -> list[_ManualEntry]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise ValueError(f"manual anchors file must be a YAML list, got {type(raw)!r}")
    return [_ManualEntry.from_dict(entry) for entry in raw]


class ManualAnchorObserver(AnchorSourceAdapter):
    """Deterministic observer that reads hand-curated YAML anchors."""

    source_name = SourceName.manual
    parser_version = "manual-v1"

    def __init__(
        self,
        *,
        entries: list[_ManualEntry] | None = None,
        path: Path | None = None,
    ) -> None:
        super().__init__()
        if entries is None:
            if path is None:
                self._entries: list[_ManualEntry] = []
            else:
                self._entries = load_manual_entries(path)
        else:
            self._entries = list(entries)

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        hits = [entry for entry in self._entries if entry.matches(target)]
        if not hits:
            return []
        signals: list[RawAnchorSignal] = []
        for entry in hits:
            signals.append(
                RawAnchorSignal(
                    source_name=self.source_name,
                    entity_type=SourceEntityType.manual,
                    source_url=entry.source_url,
                    anchor_month=entry.anchor_month,
                    anchor_type=AnchorType.manual_anchor,
                    headcount_value_min=entry.value_min,
                    headcount_value_point=entry.value_point,
                    headcount_value_max=entry.value_max,
                    headcount_value_kind=entry.value_kind,
                    confidence=entry.confidence,
                    raw_text=entry.note,
                    parser_version=self.parser_version,
                    parse_status=ParseStatus.ok,
                    note=entry.note,
                    normalized_payload={
                        "entry_canonical_name": entry.canonical_name,
                        "entry_domain": entry.domain,
                        "entry_linkedin_slug": entry.linkedin_slug,
                    },
                )
            )
        _log.info(
            "manual_anchor_hits",
            company_id=target.company_id,
            matched=len(signals),
        )
        return signals
