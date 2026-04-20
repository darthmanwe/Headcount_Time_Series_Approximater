"""SEC EDGAR company-facts observer.

The SEC publishes structured XBRL "company facts" for every registered
filer at
``https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json``. Facts
include ``dei:EntityCommonStockSharesOutstanding``, ``us-gaap:Revenues``,
and the one we care about:
``us-gaap:EmployeeEquivalentsFullTimeAndPartTimeTotalNumberOfEmployees``
(and several near-synonyms). Each fact carries ``fy``, ``fp``, ``end``
(period-end date), ``val``, and a ``filed`` date, so we can cite an
exact 10-K/10-Q without scraping prose.

The observer looks up a company's CIK via the public ``company_tickers``
endpoint (a single JSON blob updated daily) using the canonical domain
or name, then pulls the facts JSON. It emits one :class:`RawAnchorSignal`
per distinct fact report, tagged as ``historical_statement`` when the
period-end is older than the cutoff and ``current_headcount_anchor``
when it's the most-recent report.

All responses are cached by :class:`HttpClient`, so re-runs are O(1) in
HTTP calls per unchanged company.
"""

from __future__ import annotations

import json
import re
from typing import Any

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
from headcount.parsers.anchors import SEC_PARSER_VERSION, parse_sec_company_facts
from headcount.resolution.normalize import normalize_name_key
from headcount.utils.logging import get_logger
from headcount.utils.time import month_floor

_log = get_logger("headcount.ingest.observers.sec")

TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

_CIK_PAD = 10


def _pad_cik(cik: int | str) -> str:
    return str(int(cik)).rjust(_CIK_PAD, "0")


class SECObserver(AnchorSourceAdapter):
    """EDGAR company-facts observer."""

    source_name = SourceName.sec
    parser_version = SEC_PARSER_VERSION

    def __init__(self, *, user_agent: str | None = None, max_results: int = 3) -> None:
        super().__init__()
        self._user_agent = user_agent
        self._max_results = max_results
        self._ticker_cache: dict[str, dict[str, Any]] | None = None

    async def _load_tickers(self, context: FetchContext) -> dict[str, dict[str, Any]]:
        if self._ticker_cache is not None:
            return self._ticker_cache
        response = await context.http.get(self.source_name, TICKER_URL)
        if response.status_code >= 400:
            raise AdapterFetchError(f"SEC tickers HTTP {response.status_code}")
        try:
            raw = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise AdapterFetchError(f"SEC tickers not JSON: {exc!r}") from exc
        idx: dict[str, dict[str, Any]] = {}
        rows = raw.values() if isinstance(raw, dict) else raw
        for row in rows:
            title = str(row.get("title", "")).strip()
            ticker = str(row.get("ticker", "")).strip()
            cik = row.get("cik_str") or row.get("cik")
            if not title or cik is None:
                continue
            title_key = normalize_name_key(title)
            entry = {"cik": _pad_cik(cik), "ticker": ticker, "title": title}
            if title_key:
                idx.setdefault(title_key, entry)
            if ticker:
                idx.setdefault(f"ticker:{ticker.lower()}", entry)
        self._ticker_cache = idx
        return idx

    def _lookup_cik(
        self,
        ticker_index: dict[str, dict[str, Any]],
        target: CompanyTarget,
    ) -> dict[str, Any] | None:
        keys: list[str] = []
        keys.append(normalize_name_key(target.canonical_name))
        for alias in target.aliases:
            keys.append(normalize_name_key(alias))
        if target.canonical_domain:
            stem = re.sub(r"\.[^.]+$", "", target.canonical_domain)
            keys.append(normalize_name_key(stem))
        for key in keys:
            if not key:
                continue
            entry = ticker_index.get(key)
            if entry is not None:
                return entry
        return None

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        tickers = await self._load_tickers(context)
        entry = self._lookup_cik(tickers, target)
        if entry is None:
            return []
        cik = entry["cik"]
        facts_url = FACTS_URL.format(cik=cik)
        response = await context.http.get(self.source_name, facts_url)
        if response.status_code == 404:
            return []
        if response.status_code >= 400:
            raise AdapterFetchError(f"SEC company-facts HTTP {response.status_code} for CIK {cik}")
        try:
            facts = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise AdapterFetchError(f"SEC facts not JSON: {exc!r}") from exc
        entity_name = str(facts.get("entityName", entry["title"]))
        employee_rows = parse_sec_company_facts(facts)
        if not employee_rows:
            return []
        signals: list[RawAnchorSignal] = []
        for row in employee_rows[: self._max_results]:
            signals.append(
                RawAnchorSignal(
                    source_name=self.source_name,
                    entity_type=SourceEntityType.company,
                    source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}",
                    anchor_month=month_floor(row.end),
                    anchor_type=AnchorType.historical_statement,
                    headcount_value_min=row.value,
                    headcount_value_point=row.value,
                    headcount_value_max=row.value,
                    headcount_value_kind=HeadcountValueKind.exact,
                    confidence=0.85,
                    raw_text=f"{entity_name} {row.concept} FY{row.fy} {row.fp}={int(row.value)}",
                    parser_version=self.parser_version,
                    parse_status=ParseStatus.ok,
                    note=f"cik={cik} ticker={entry['ticker']}",
                    normalized_payload={
                        "cik": cik,
                        "concept": row.concept,
                        "fy": row.fy,
                        "fp": row.fp,
                        "end": row.end.isoformat(),
                        "filed": row.filed,
                        "ticker": entry["ticker"],
                    },
                )
            )
        _log.info(
            "sec_anchor_hits",
            company_id=target.company_id,
            cik=cik,
            matched=len(signals),
        )
        return signals
