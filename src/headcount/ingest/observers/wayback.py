"""Internet Archive Wayback Machine historical-anchor observer.

Purpose
=======

The live observers (``company_web``, ``linkedin_public``) can only give us
*current*-month headcount. That leaves the estimator with a single anchor
per company and forces the whole series into ``degraded_current_only``,
which in turn blanks the product-contract 6m / 1y / 2y growth metrics
(see :mod:`headcount.estimate.growth` - both endpoints must be real
estimates or growth is ``None``).

This observer fills the gap without adding any new bot-wall exposure:

- For every target and every horizon (T-6m / T-1y / T-2y) we issue a
  **single** GET against
  ``https://web.archive.org/web/<YYYYMM>01000000id_/<original_url>``.
  Wayback treats a partial timestamp as "closest snapshot to this
  moment" and follows its own internal redirect to the actual archived
  capture; the ``id_`` flag is preserved across the redirect so the
  body comes back as the raw archived HTML rather than Wayback's
  framed viewer.
- The response URL tells us the real snapshot timestamp for provenance.
- We dispatch the body to the existing parsers by origin:

  - first-party ``/about`` (etc.) -> :func:`parse_company_web_jsonld`
    then :func:`parse_company_web_text`
  - LinkedIn company page         -> :func:`extract_linkedin_jsonld_employees`

- Each recovered value is emitted as a :class:`RawAnchorSignal` with
  ``anchor_type=historical_statement`` and a modest confidence floor
  (archives drift; a snapshot dated 2024-03 can contain copy from
  months later if the host shipped in-place updates).

Why the direct-redirect pattern instead of the availability API?
----------------------------------------------------------------

The documented ``/wayback/available`` JSON endpoint is rate-limited at
~15 req/min per source IP and 429s aggressively in practice (see the
2026-04-21 cohort run - every availability call came back 429, zero
snapshots materialised). The bulk-viewer path
``/web/<ts>id_/<url>`` is served by the same CDX cluster that backs
``https://web.archive.org/*`` and tolerates hundreds of requests per
minute from a single UA. It also halves the HTTP cost (one round trip
instead of two) and gives us the snapshot timestamp for free via the
redirect's final URL.

Design constraints
------------------

- **No authentication.** Wayback is an archival mission; a polite UA and
  moderate concurrency are all that's required.
- **Shares the HttpClient + FileCache** so repeat runs of the cohort
  don't re-hit the Archive.
- **One observer, one source_name**. Every signal is persisted under
  ``SourceName.wayback`` so the scoreboard / production coverage can
  cleanly distinguish "live current" from "archived historical".
- **Fail-closed**: any fetch error / missing snapshot / empty parse is
  a silent zero; we never block the pipeline on the Archive being slow.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urljoin, urlparse

from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.ingest.base import (
    AdapterFetchError,
    AnchorSourceAdapter,
    CompanyTarget,
    FetchContext,
    RawAnchorSignal,
)
from headcount.parsers.anchors import (
    WAYBACK_PARSER_VERSION,
    extract_linkedin_jsonld_employees,
    parse_company_web_jsonld,
    parse_company_web_text,
    clean_html_to_text,
)
from headcount.utils.logging import get_logger
from headcount.utils.time import month_floor

_log = get_logger("headcount.ingest.observers.wayback")

WAYBACK_AVAILABLE_URL = "https://archive.org/wayback/available"
WAYBACK_SNAPSHOT_PREFIX = "https://web.archive.org/web/"
_WAYBACK_SNAPSHOT_URL_RE = re.compile(
    r"^https?://web\.archive\.org/web/(?P<ts>\d{14})(?:id_)?/(?P<url>.+)$"
)

# Horizons in months, matching the product contract (6m / 1y / 2y).
_DEFAULT_HORIZONS: tuple[int, ...] = (6, 12, 24)

# Paths we ask the archive for, first hit wins per horizon per origin.
# Kept narrow on purpose: the "current" ``CompanyWebObserver`` already
# widens the path set, and every extra path multiplies Archive lookups.
_ARCHIVE_ABOUT_PATHS: tuple[str, ...] = ("/about", "/about-us", "/company", "/")

# Confidence floor for archival signals. Below the live LinkedIn JSON-LD
# floor (0.70) and the live company-web floor (0.55..0.65) because a
# snapshot can lag, can contain outdated copy, and we cannot verify the
# "as of" month beyond the Archive's capture timestamp.
_ARCHIVED_JSONLD_CONFIDENCE = 0.55
_ARCHIVED_TEXT_CONFIDENCE = 0.45


def _months_ago(anchor: date, delta_months: int) -> date:
    total = anchor.year * 12 + (anchor.month - 1) - delta_months
    year, m_zero = divmod(total, 12)
    return date(year, m_zero + 1, 1)


def _wayback_timestamp(target_month: date) -> str:
    """Return a 14-digit YYYYMMDDhhmmss timestamp anchored at the target month.

    Wayback snapshot URLs accept shorter prefixes but the canonical URL
    shape used by the bulk viewer is 14 digits; we pad to the first of
    the month at 00:00:00 so the cache key is stable.
    """

    return f"{target_month.year:04d}{target_month.month:02d}01000000"


@dataclass(frozen=True, slots=True)
class WaybackSnapshot:
    """One closest-match snapshot returned by the availability API."""

    url: str
    timestamp: str  # YYYYMMDDhhmmss as returned by the Archive
    status: str
    available: bool


def parse_availability_response(
    payload: str | dict[str, Any],
) -> WaybackSnapshot | None:
    """Flatten a ``/wayback/available`` JSON response.

    Returns the closest snapshot if the Archive reports one, else None.
    Tolerates both the dict and stringly-typed shapes the API ships.
    """

    if isinstance(payload, str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return None
    else:
        data = payload
    if not isinstance(data, dict):
        return None
    snapshots = data.get("archived_snapshots") or {}
    if not isinstance(snapshots, dict):
        return None
    closest = snapshots.get("closest") or {}
    if not isinstance(closest, dict):
        return None
    available = bool(closest.get("available"))
    url = closest.get("url") or ""
    timestamp = str(closest.get("timestamp") or "")
    status = str(closest.get("status") or "")
    if not (available and url and timestamp):
        return None
    return WaybackSnapshot(
        url=str(url),
        timestamp=timestamp,
        status=status,
        available=available,
    )


def _raw_snapshot_url(timestamp: str, original_url: str) -> str:
    """Build the ``web.archive.org/web/<ts>id_/<url>`` URL.

    The ``id_`` flag asks the Archive to serve the original bytes rather
    than the rewritten body (which would otherwise inject a toolbar and
    rewrite embedded resources, breaking JSON-LD blocks). Quoting is
    deliberately minimal because Wayback is tolerant of the original URL
    being passed verbatim after the prefix. A partial ``timestamp``
    (e.g. ``20240401000000``) is accepted: Wayback redirects to the
    nearest real capture and preserves the ``id_`` flag.
    """

    return f"{WAYBACK_SNAPSHOT_PREFIX}{timestamp}id_/{original_url}"


def _extract_snapshot_metadata(final_url: str) -> tuple[str | None, str | None]:
    """Parse the final URL of a followed Wayback redirect.

    Returns ``(snapshot_timestamp, archived_origin_url)`` so callers can
    record provenance pointing at the actual snapshot Wayback served,
    not the bucket timestamp we requested.
    """

    m = _WAYBACK_SNAPSHOT_URL_RE.match(final_url)
    if not m:
        return None, None
    return m.group("ts"), m.group("url")


def _is_linkedin_url(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        return False
    return host.lower().endswith("linkedin.com")


def _canonical_about_urls(
    domain: str, paths: Iterable[str] = _ARCHIVE_ABOUT_PATHS
) -> list[str]:
    base = f"https://{domain.lstrip('/')}".rstrip("/") + "/"
    return [urljoin(base, p.lstrip("/")) for p in paths]


def _normalize_linkedin_url(url: str) -> str:
    """Strip query string / fragment so the Wayback lookup is deterministic."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") + "/"
    return f"{parsed.scheme or 'https'}://{parsed.netloc}{path}"


class WaybackObserver(AnchorSourceAdapter):
    """Emits historical anchors from Internet Archive snapshots.

    For each in-scope company the observer:

    1. Decides the set of *origin URLs* to probe - the company's own
       canonical-domain ``/about`` family plus, if known, the LinkedIn
       company page.
    2. For each (origin, horizon) pair, asks the availability API for
       the closest snapshot near ``anchor_month - horizon``.
    3. Fetches the raw snapshot HTML and runs the parser matching the
       origin kind.
    4. Emits up to one signal per (origin, horizon) pair; deduplication
       by content hash is handled by the persistence layer.

    Every I/O call goes through the shared :class:`HttpClient`, so
    retries / caching / rate-limits are uniform. The observer never
    raises for missing data - a company with no archived snapshots just
    emits zero signals, same as SEC for a private company.
    """

    source_name = SourceName.wayback
    parser_version = WAYBACK_PARSER_VERSION

    def __init__(
        self,
        *,
        anchor_month: date | None = None,
        horizons_months: Iterable[int] = _DEFAULT_HORIZONS,
        include_company_web: bool = True,
        include_linkedin: bool = True,
        about_paths: Iterable[str] = _ARCHIVE_ABOUT_PATHS,
    ) -> None:
        super().__init__()
        self._anchor_month = anchor_month
        self._horizons = tuple(sorted(set(int(h) for h in horizons_months if h > 0)))
        self._include_company_web = include_company_web
        self._include_linkedin = include_linkedin
        self._about_paths = tuple(about_paths)

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        anchor_month = self._anchor_month or month_floor(date.today())
        origins = self._origin_urls(target)
        if not origins:
            return []

        signals: list[RawAnchorSignal] = []
        seen_keys: set[tuple[str, str]] = set()  # (origin_url, snapshot_ts)

        for origin_kind, origin_url in origins:
            for horizon in self._horizons:
                target_month = _months_ago(anchor_month, horizon)
                snapshot = await self._fetch_snapshot_direct(
                    context=context,
                    origin_url=origin_url,
                    target_month=target_month,
                )
                if snapshot is None:
                    continue
                body, actual_timestamp = snapshot
                # Defensive: if the redirect landed more than 12 months
                # away from the horizon we wanted, don't claim the
                # emitted anchor represents ``target_month`` - the
                # estimator will reconcile by asof, but pretending a
                # 2018 snapshot covers a 2024 horizon inflates growth
                # signals. Skip when the drift is material.
                if not _within_drift_tolerance(actual_timestamp, target_month):
                    continue
                key = (origin_url, actual_timestamp or "")
                if key in seen_keys:
                    # Same snapshot satisfies multiple horizons (common
                    # when a company has sparse archive coverage). Skip
                    # duplicate emissions - the persistence layer would
                    # dedup by content hash anyway, but skipping here
                    # saves a reparse.
                    continue
                seen_keys.add(key)

                snapshot_ts = actual_timestamp or _wayback_timestamp(target_month)
                emitted = self._emit_from_body(
                    origin_kind=origin_kind,
                    origin_url=origin_url,
                    horizon=horizon,
                    snapshot_timestamp=snapshot_ts,
                    body=body,
                    anchor_month=target_month,
                )
                signals.extend(emitted)

        _log.info(
            "wayback_anchor_hits",
            company_id=target.company_id,
            matched=len(signals),
            origins=len(origins),
            horizons=self._horizons,
        )
        return signals

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _origin_urls(self, target: CompanyTarget) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        if self._include_company_web and target.canonical_domain:
            for url in _canonical_about_urls(
                target.canonical_domain, paths=self._about_paths
            ):
                out.append(("company_web", url))
        if self._include_linkedin and target.linkedin_company_url:
            out.append(
                ("linkedin_public", _normalize_linkedin_url(target.linkedin_company_url))
            )
        return out

    async def _fetch_snapshot_direct(
        self,
        *,
        context: FetchContext,
        origin_url: str,
        target_month: date,
    ) -> tuple[str, str | None] | None:
        """Ask Wayback for the closest snapshot to ``target_month`` and
        return ``(body, actual_timestamp)`` or None.

        Single GET: Wayback's own redirect resolves "closest snapshot".
        The ``id_`` flag is preserved through the redirect so the body
        comes back raw. HttpClient is configured with
        ``follow_redirects=True``; any non-200 final status (Wayback
        has no snapshot at all) returns None.
        """

        url = _raw_snapshot_url(
            _wayback_timestamp(target_month),
            origin_url,
        )
        try:
            response = await context.http.get(self.source_name, url)
        except Exception as exc:
            raise AdapterFetchError(
                f"wayback snapshot fetch failed: {exc!r}"
            ) from exc
        if response.status_code != 200:
            return None
        body = response.text or ""
        if not body.strip():
            return None
        # Parse the final URL Wayback handed us (post-redirect) to
        # record the actual snapshot timestamp for provenance.
        ts, _arch_url = _extract_snapshot_metadata(response.url)
        return body, ts

    def _emit_from_body(
        self,
        *,
        origin_kind: str,
        origin_url: str,
        horizon: int,
        snapshot_timestamp: str,
        body: str,
        anchor_month: date,
    ) -> list[RawAnchorSignal]:
        if origin_kind == "linkedin_public":
            return self._emit_linkedin(
                origin_url=origin_url,
                horizon=horizon,
                snapshot_timestamp=snapshot_timestamp,
                body=body,
                anchor_month=anchor_month,
            )
        return self._emit_company_web(
            origin_url=origin_url,
            horizon=horizon,
            snapshot_timestamp=snapshot_timestamp,
            body=body,
            anchor_month=anchor_month,
        )

    def _emit_linkedin(
        self,
        *,
        origin_url: str,
        horizon: int,
        snapshot_timestamp: str,
        body: str,
        anchor_month: date,
    ) -> list[RawAnchorSignal]:
        parsed = extract_linkedin_jsonld_employees(body)
        if parsed is None:
            return []
        # Archive-sourced LinkedIn JSON-LD gets the JSON-LD confidence
        # floor rather than the text floor - schema.org is still the
        # external contract even when delivered via Wayback.
        signal = RawAnchorSignal(
            source_name=self.source_name,
            entity_type=SourceEntityType.company,
            source_url=_raw_snapshot_url(snapshot_timestamp, origin_url),
            anchor_month=anchor_month,
            anchor_type=AnchorType.historical_statement,
            headcount_value_min=float(parsed.low),
            headcount_value_point=float(parsed.point),
            headcount_value_max=float(parsed.high),
            headcount_value_kind=parsed.kind,
            confidence=_ARCHIVED_JSONLD_CONFIDENCE,
            raw_text=parsed.phrase,
            parser_version=self.parser_version,
            parse_status=ParseStatus.ok,
            note=(
                f"wayback origin=linkedin horizon={horizon}m "
                f"snapshot_ts={snapshot_timestamp}"
            ),
            normalized_payload={
                "origin_kind": "linkedin_public",
                "origin_url": origin_url,
                "snapshot_timestamp": snapshot_timestamp,
                "horizon_months": horizon,
                "phrase": parsed.phrase,
            },
        )
        return [signal]

    def _emit_company_web(
        self,
        *,
        origin_url: str,
        horizon: int,
        snapshot_timestamp: str,
        body: str,
        anchor_month: date,
    ) -> list[RawAnchorSignal]:
        jsonld_matches = parse_company_web_jsonld(body)
        text_matches: list = []
        if not jsonld_matches:
            cleaned = clean_html_to_text(body)
            if cleaned:
                text_matches = parse_company_web_text(cleaned)
        out: list[RawAnchorSignal] = []
        combined = [(m, True) for m in jsonld_matches] + [
            (m, False) for m in text_matches
        ]
        for match, from_jsonld in combined:
            confidence = (
                _ARCHIVED_JSONLD_CONFIDENCE if from_jsonld else _ARCHIVED_TEXT_CONFIDENCE
            )
            kind = match.kind
            if kind is HeadcountValueKind.bucket and match.value_min == match.value_max:
                # Defensive: a single-value "bucket" should still read as
                # exact for downstream reconcile.
                kind = HeadcountValueKind.exact
            out.append(
                RawAnchorSignal(
                    source_name=self.source_name,
                    entity_type=SourceEntityType.company,
                    source_url=_raw_snapshot_url(snapshot_timestamp, origin_url),
                    anchor_month=anchor_month,
                    anchor_type=AnchorType.historical_statement,
                    headcount_value_min=float(match.value_min),
                    headcount_value_point=float(match.value_point),
                    headcount_value_max=float(match.value_max),
                    headcount_value_kind=kind,
                    confidence=confidence,
                    raw_text=match.phrase,
                    parser_version=self.parser_version,
                    parse_status=ParseStatus.ok,
                    note=(
                        f"wayback origin=company_web horizon={horizon}m "
                        f"snapshot_ts={snapshot_timestamp} "
                        f"qualifier={match.qualifier or 'exact'}"
                        + (" jsonld=1" if from_jsonld else "")
                    ),
                    normalized_payload={
                        "origin_kind": "company_web",
                        "origin_url": origin_url,
                        "snapshot_timestamp": snapshot_timestamp,
                        "horizon_months": horizon,
                        "qualifier": match.qualifier,
                        "phrase": match.phrase,
                        "jsonld": bool(from_jsonld),
                    },
                )
            )
        return out


# ---------------------------------------------------------------------------
# Drift guard
# ---------------------------------------------------------------------------

_DRIFT_TOLERANCE_MONTHS = 12


def _within_drift_tolerance(
    actual_timestamp: str | None,
    target_month: date,
    *,
    tolerance_months: int = _DRIFT_TOLERANCE_MONTHS,
) -> bool:
    """Return True if the actual snapshot is within tolerance of target.

    When ``actual_timestamp`` is None we accept the snapshot optimistically
    (the drift can't be worse than the horizon itself and the estimator
    reconciles by anchor_month).
    """

    if not actual_timestamp or len(actual_timestamp) < 6:
        return True
    try:
        actual_year = int(actual_timestamp[:4])
        actual_month = int(actual_timestamp[4:6])
    except ValueError:
        return True
    months_diff = abs(
        (actual_year * 12 + actual_month) - (target_month.year * 12 + target_month.month)
    )
    return months_diff <= tolerance_months


__all__ = [
    "WaybackObserver",
    "WaybackSnapshot",
    "WAYBACK_AVAILABLE_URL",
    "WAYBACK_SNAPSHOT_PREFIX",
    "parse_availability_response",
]
