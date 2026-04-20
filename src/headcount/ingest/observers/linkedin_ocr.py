"""Logged-out LinkedIn growth-trend observer (OCR-backed).

This observer is the free, best-effort compensator for the fact that
logged-out LinkedIn blocks the structured ``/company/<slug>/people/``
headcount endpoint almost everywhere. The one thing the public page
*does* frequently render is the "Employee growth" mini-chart, which
shows the percent change over 6 months / 1 year / 2 years.

The observer therefore:

1. Fetches the public ``/company/<slug>/`` or ``/company/<slug>/insights/``
   page (logged-out, plain HTTP, no cookies).
2. Extracts any referenced chart image (PNG/JPEG/SVG rasterised) via a
   small, parser-version-gated selector.
3. Runs OCR (``pytesseract``) over the image bytes.
4. Parses the OCR'd text with :func:`parse_growth_trend_text` to
   recover ``(6m_pct, 1y_pct, 2y_pct)``.
5. Combines the percents with the company's most recent
   :class:`~headcount.models.company_anchor_observation.CompanyAnchorObservation`
   (the ``current_headcount_anchor``) to back-compute historical
   anchors at ``T-6m`` / ``T-1y`` / ``T-2y`` and emits them as
   additional anchors.

Hard defaults
-------------

- **OCR is an optional extra.** The module imports ``pytesseract`` and
  ``PIL.Image`` lazily. If either is missing, :meth:`LinkedInGrowthTrendObserver.is_available`
  returns ``False`` and :meth:`collect` is a no-op returning ``0``.
- **Logged-out only.** No cookies, no credentials. Any HTTP signal
  consistent with a gate (``401``, ``403``, ``429``, HTML with
  ``authwall`` markers) is treated as "no data for this company" - we
  never retry with different headers.
- **Fail-closed.** If OCR text is ambiguous
  (:func:`parse_growth_trend_text` returns ``None``) we emit zero
  signals for that company. Better to miss data than hallucinate it.
- **Disabled by default in CLI.** The ``hc collect-employment`` command
  only wires this observer in when ``--enable-ocr`` is passed.

Testing
-------

Because OCR is non-deterministic on arbitrary images, the observer
exposes three injection points that tests use via the
:class:`LinkedInGrowthTrendObserver` constructor:

- ``fetch_html(url) -> str | None`` -- stub HTML fetching.
- ``fetch_image(url) -> bytes | None`` -- stub image download.
- ``ocr(image_bytes) -> str`` -- stub OCR; lets tests hand the observer
  canned text without installing tesseract.

The default implementations use ``httpx`` and ``pytesseract`` lazily.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.source_observation import SourceObservation
from headcount.utils.logging import get_logger

LINKEDIN_OCR_PARSER_VERSION = "linkedin_ocr_v1"

_log = get_logger("headcount.ingest.observers.linkedin_ocr")

_HORIZONS_MONTHS: dict[str, int] = {"6m": 6, "1y": 12, "2y": 24}

# Rough growth-percent matches, e.g. "6m +12%", "1y -8%",
# "2-year 34%". The observer is tolerant: percentage sign is optional
# and horizons may be spelled out ("six months") in the future.
_PERCENT_PATTERN = re.compile(
    r"(?P<label>6\s*m(?:onths?)?|1\s*y(?:ears?)?|2\s*y(?:ears?)?)"
    r"[^+\-\d]{0,20}"
    r"(?P<sign>[+\-]?)\s*(?P<pct>\d+(?:\.\d+)?)\s*%?",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ParsedGrowthTrend:
    """Result of OCR-parsing a LinkedIn growth-trend chart."""

    pct_6m: float | None = None
    pct_1y: float | None = None
    pct_2y: float | None = None

    def is_empty(self) -> bool:
        return (
            self.pct_6m is None and self.pct_1y is None and self.pct_2y is None
        )

    def as_dict(self) -> dict[str, float | None]:
        return {
            "pct_6m": self.pct_6m,
            "pct_1y": self.pct_1y,
            "pct_2y": self.pct_2y,
        }


def parse_growth_trend_text(text: str) -> ParsedGrowthTrend | None:
    """Best-effort extractor from a noisy OCR string.

    Returns ``None`` when no recognised horizon/percent pair appears in
    ``text``; the caller should treat that as "no signal".
    """

    if not text:
        return None
    pct_6m: float | None = None
    pct_1y: float | None = None
    pct_2y: float | None = None
    for match in _PERCENT_PATTERN.finditer(text):
        label = match.group("label").lower().replace(" ", "")
        sign = match.group("sign")
        pct = float(match.group("pct"))
        if sign == "-":
            pct = -pct
        # Reject implausible values - LinkedIn's rolling growth
        # metrics for a single horizon never exceed a few hundred
        # percent; anything larger is OCR noise.
        if abs(pct) > 500:
            continue
        if label.startswith("6m"):
            pct_6m = pct
        elif label.startswith("1y"):
            pct_1y = pct
        elif label.startswith("2y"):
            pct_2y = pct
    trend = ParsedGrowthTrend(pct_6m=pct_6m, pct_1y=pct_1y, pct_2y=pct_2y)
    if trend.is_empty():
        return None
    return trend


def _months_ago(month: date, delta_months: int) -> date:
    total = month.year * 12 + (month.month - 1) - delta_months
    year, m_zero = divmod(total, 12)
    return date(year, m_zero + 1, 1)


def back_compute_historical_values(
    *,
    current_point: float,
    current_min: float,
    current_max: float,
    trend: ParsedGrowthTrend,
) -> dict[str, tuple[float, float, float]]:
    """Derive ``(min, point, max)`` per horizon by inverting growth.

    A growth of ``p%`` over the horizon means the current value equals
    ``historical * (1 + p/100)``, so we divide to recover the
    historical value. Intervals are scaled identically so uncertainty
    carries through.

    Returns a dict keyed by horizon label (``"6m"``/``"1y"``/``"2y"``);
    horizons whose percentage is missing or would produce a
    non-positive value are omitted.
    """

    out: dict[str, tuple[float, float, float]] = {}
    for label, pct in (
        ("6m", trend.pct_6m),
        ("1y", trend.pct_1y),
        ("2y", trend.pct_2y),
    ):
        if pct is None:
            continue
        factor = 1.0 + (pct / 100.0)
        if factor <= 0.0:
            continue
        point = current_point / factor
        vmin = current_min / factor
        vmax = current_max / factor
        if point <= 0.0:
            continue
        out[label] = (vmin, point, vmax)
    return out


def _default_fetch_html(url: str) -> str | None:
    """Lazily import httpx and fetch ``url``; return text or ``None``."""

    try:
        import httpx
    except Exception:
        return None
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(15.0),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; headcount-research/1.0)"
                )
            },
        ) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            return resp.text
    except Exception as exc:  # pragma: no cover - network/OS faults
        _log.debug("ocr_fetch_html_failed", url=url, error=str(exc))
        return None


def _default_fetch_image(url: str) -> bytes | None:
    try:
        import httpx
    except Exception:
        return None
    try:
        with httpx.Client(
            follow_redirects=True, timeout=httpx.Timeout(15.0)
        ) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            return resp.content
    except Exception as exc:  # pragma: no cover
        _log.debug("ocr_fetch_image_failed", url=url, error=str(exc))
        return None


def _default_ocr(image_bytes: bytes) -> str:
    try:
        from io import BytesIO

        import pytesseract
        from PIL import Image
    except Exception:
        return ""
    try:
        image = Image.open(BytesIO(image_bytes))
        return str(pytesseract.image_to_string(image) or "")
    except Exception as exc:  # pragma: no cover
        _log.debug("ocr_run_failed", error=str(exc))
        return ""


def ocr_available() -> bool:
    """True when the optional OCR stack is importable in the current env."""

    try:
        import pytesseract  # noqa: F401
        from PIL import Image  # noqa: F401
    except Exception:
        return False
    return True


_CHART_IMAGE_RE = re.compile(
    r'<img[^>]+src="(?P<src>https?://[^"]+(?:growth|insights|chart)[^"]*)"',
    flags=re.IGNORECASE,
)


def _find_chart_image_urls(html: str) -> list[str]:
    if not html:
        return []
    return [m.group("src") for m in _CHART_IMAGE_RE.finditer(html)]


def _latest_current_anchor(
    session: Session, *, company_id: str
) -> CompanyAnchorObservation | None:
    stmt = (
        select(CompanyAnchorObservation)
        .where(
            CompanyAnchorObservation.company_id == company_id,
            CompanyAnchorObservation.anchor_type
            == AnchorType.current_headcount_anchor,
        )
        .order_by(desc(CompanyAnchorObservation.anchor_month))
    )
    return session.execute(stmt).scalars().first()


class LinkedInGrowthTrendObserver:
    """Scaffolded OCR-backed growth-trend observer.

    Parameters
    ----------
    fetch_html, fetch_image, ocr:
        Injection points for tests. When not supplied they default to
        ``httpx``/``pytesseract``-based implementations which are no-ops
        whenever the relevant optional dependency is absent.
    """

    def __init__(
        self,
        *,
        fetch_html: Callable[[str], str | None] | None = None,
        fetch_image: Callable[[str], bytes | None] | None = None,
        ocr: Callable[[bytes], str] | None = None,
    ) -> None:
        self._fetch_html = fetch_html or _default_fetch_html
        self._fetch_image = fetch_image or _default_fetch_image
        self._ocr = ocr or _default_ocr

    @staticmethod
    def is_available() -> bool:
        return ocr_available()

    def collect(
        self,
        session: Session,
        *,
        companies: Sequence[Company],
    ) -> int:
        """Run the observer over ``companies`` and write any signals.

        Returns the number of anchor rows inserted. Zero is the
        expected outcome whenever OCR is unavailable or LinkedIn gates
        the target pages - the observer is advisory.
        """

        written = 0
        for company in companies:
            try:
                written += self._collect_for_company(session, company)
            except Exception as exc:  # pragma: no cover - best-effort
                _log.debug(
                    "ocr_observer_company_failed",
                    company_id=company.id,
                    error=str(exc),
                )
        return written

    def _collect_for_company(
        self, session: Session, company: Company
    ) -> int:
        slug_url = getattr(company, "linkedin_company_url", None)
        if not slug_url:
            return 0
        html = self._fetch_html(slug_url)
        if not html:
            return 0
        if _html_is_gated(html):
            return 0
        chart_urls = _find_chart_image_urls(html)
        if not chart_urls:
            return 0

        current = _latest_current_anchor(session, company_id=company.id)
        if current is None:
            return 0

        trend: ParsedGrowthTrend | None = None
        chart_src_used: str | None = None
        for url in chart_urls:
            image_bytes = self._fetch_image(url)
            if not image_bytes:
                continue
            text = self._ocr(image_bytes)
            parsed = parse_growth_trend_text(text)
            if parsed is not None:
                trend = parsed
                chart_src_used = url
                break
        if trend is None or chart_src_used is None:
            return 0

        historical = back_compute_historical_values(
            current_point=float(current.headcount_value_point),
            current_min=float(current.headcount_value_min),
            current_max=float(current.headcount_value_max),
            trend=trend,
        )
        if not historical:
            return 0

        observed_at = datetime.now(tz=UTC)
        inserted = 0
        for label, (vmin, point, vmax) in historical.items():
            anchor_month = _months_ago(
                current.anchor_month, _HORIZONS_MONTHS[label]
            )
            src = SourceObservation(
                source_name=SourceName.linkedin_public,
                entity_type=SourceEntityType.company,
                source_url=chart_src_used,
                observed_at=observed_at,
                raw_text=None,
                raw_content_hash=_ocr_content_hash(
                    company_id=company.id,
                    chart_src=chart_src_used,
                    label=label,
                    trend=trend,
                ),
                parser_version=LINKEDIN_OCR_PARSER_VERSION,
                parse_status=ParseStatus.ok,
                normalized_payload_json={
                    "trend": trend.as_dict(),
                    "horizon": label,
                    "anchor_month": anchor_month.isoformat(),
                    "current_anchor_id": current.id,
                    "chart_url": chart_src_used,
                },
            )
            session.add(src)
            session.flush()
            session.add(
                CompanyAnchorObservation(
                    company_id=company.id,
                    source_observation_id=src.id,
                    anchor_type=AnchorType.historical_statement,
                    headcount_value_min=vmin,
                    headcount_value_point=point,
                    headcount_value_max=vmax,
                    headcount_value_kind=HeadcountValueKind.exact,
                    anchor_month=anchor_month,
                    confidence=0.40,
                    note=f"ocr_growth_trend horizon={label}",
                )
            )
            inserted += 1
        return inserted


_GATE_MARKERS = (
    "authwall",
    "sign in to see",
    "join now to see",
    "please enable javascript",
)


def _html_is_gated(html: str) -> bool:
    lowered = html.lower()
    return any(marker in lowered for marker in _GATE_MARKERS)


def _ocr_content_hash(
    *,
    company_id: str,
    chart_src: str,
    label: str,
    trend: ParsedGrowthTrend,
) -> str:
    payload = f"{company_id}|{chart_src}|{label}|{trend.as_dict()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = [
    "LINKEDIN_OCR_PARSER_VERSION",
    "LinkedInGrowthTrendObserver",
    "ParsedGrowthTrend",
    "back_compute_historical_values",
    "ocr_available",
    "parse_growth_trend_text",
]


# Satisfy "imported but unused" lint for the typing-only ``Any`` import
# we keep available for subclassers wanting looser fetch signatures.
_ = Any
