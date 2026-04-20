"""First-party company website observer.

Given a company domain we try a short allow-list of common paths
(``/``, ``/about``, ``/company``, ``/careers``) and search the rendered
text for headcount cues. Recognized patterns:

- ``"1,250 employees"`` / ``"1250 people"``
- ``"over 500 employees"``                -> interval ``[500, *, +25%]``
- ``"approximately 700 employees"``        -> interval ``[+/-10%]``
- ``"team of 42"``

We intentionally stay simple: no JS execution, no Playwright, no link
discovery. False positives are preferable to be discarded by the
reconciliation layer than to accept noise from a crawl we can't audit.
Every path is cached by :class:`HttpClient` so re-runs are O(1) per
company until the cache expires.

``robots.txt`` is not fetched per page because the allow-list is small
and public-facing; the UA advertises our intent and we fail-closed on
a ``403``.
"""

from __future__ import annotations

from datetime import date
from urllib.parse import urljoin

from headcount.db.enums import (
    AnchorType,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.ingest.base import (
    AdapterFetchError,
    AdapterGatedError,
    AnchorSourceAdapter,
    CompanyTarget,
    FetchContext,
    RawAnchorSignal,
)
from headcount.parsers.anchors import (
    COMPANY_WEB_PARSER_VERSION,
    clean_html_to_text,
    parse_company_web_text,
)
from headcount.utils.logging import get_logger
from headcount.utils.time import month_floor

_log = get_logger("headcount.ingest.observers.company_web")

_DEFAULT_PATHS: tuple[str, ...] = ("/", "/about", "/about-us", "/company", "/careers")


class CompanyWebObserver(AnchorSourceAdapter):
    """Scrapes a short allow-list of common company-site paths."""

    source_name = SourceName.company_web
    parser_version = COMPANY_WEB_PARSER_VERSION

    def __init__(self, *, paths: tuple[str, ...] = _DEFAULT_PATHS, max_paths: int = 4) -> None:
        super().__init__()
        self._paths = paths
        self._max_paths = max_paths

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        if not target.canonical_domain:
            return []
        base = f"https://{target.canonical_domain}".rstrip("/")
        anchor_month: date = month_floor(date.today())
        signals: list[RawAnchorSignal] = []

        for path in self._paths[: self._max_paths]:
            url = urljoin(base + "/", path.lstrip("/"))
            try:
                response = await context.http.get(self.source_name, url)
            except Exception as exc:  # pragma: no cover - network failure path
                raise AdapterFetchError(f"company_web fetch failed: {exc!r}") from exc
            if response.status_code == 403:
                raise AdapterGatedError(f"{url} returned 403")
            if response.status_code == 404:
                continue
            if response.status_code >= 400:
                continue
            text = clean_html_to_text(response.text or "")
            if not text:
                continue
            for match in parse_company_web_text(text):
                signals.append(
                    RawAnchorSignal(
                        source_name=self.source_name,
                        entity_type=SourceEntityType.company,
                        source_url=url,
                        anchor_month=anchor_month,
                        anchor_type=AnchorType.current_headcount_anchor,
                        headcount_value_min=match.value_min,
                        headcount_value_point=match.value_point,
                        headcount_value_max=match.value_max,
                        headcount_value_kind=match.kind,
                        confidence=0.55 if match.qualifier else 0.65,
                        raw_text=match.phrase,
                        parser_version=self.parser_version,
                        parse_status=ParseStatus.ok,
                        note=f"path={path} qualifier={match.qualifier or 'exact'}",
                        normalized_payload={
                            "path": path,
                            "qualifier": match.qualifier,
                            "phrase": match.phrase,
                        },
                    )
                )
            if signals:
                break
        _log.info(
            "company_web_anchor_hits",
            company_id=target.company_id,
            matched=len(signals),
            domain=target.canonical_domain,
        )
        return signals
