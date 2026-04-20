"""Wikidata SPARQL anchor observer.

Wikidata's ``P1128`` property records ``number of employees`` statements,
usually with a ``P585`` (point in time) qualifier. That gives us a rare
open, cite-able source for headcount even when a company has no SEC
filing. The observer queries the public WDQS endpoint with a narrow
SPARQL query that prefers:

1. An exact match on the official website (``P856``) against the
   company's canonical domain.
2. A language-tagged exact label match otherwise.

It returns at most the three most-recent dated P1128 statements. Because
Wikidata values are already exact integers we emit
``HeadcountValueKind.exact`` with a single-point interval.

The raw JSON is stored verbatim on the signal so Phase 8 can expose the
citation URL to analysts.
"""

from __future__ import annotations

import json
from datetime import date, datetime
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
from headcount.utils.logging import get_logger
from headcount.utils.time import month_floor

_log = get_logger("headcount.ingest.observers.wikidata")

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

_SPARQL_BY_DOMAIN = """
SELECT ?company ?companyLabel ?employees ?asof WHERE {{
  ?company wdt:P856 ?website.
  ?company p:P1128 ?stmt.
  ?stmt ps:P1128 ?employees.
  OPTIONAL {{ ?stmt pq:P585 ?asof. }}
  FILTER(CONTAINS(LCASE(STR(?website)), "{domain}"))
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?asof)
LIMIT 5
"""

_SPARQL_BY_NAME = """
SELECT ?company ?companyLabel ?employees ?asof WHERE {{
  ?company rdfs:label "{name}"@en.
  ?company p:P1128 ?stmt.
  ?stmt ps:P1128 ?employees.
  OPTIONAL {{ ?stmt pq:P585 ?asof. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?asof)
LIMIT 5
"""


class WikidataObserver(AnchorSourceAdapter):
    """Query Wikidata for ``P1128`` number-of-employees statements."""

    source_name = SourceName.wikidata
    parser_version = "wikidata-v1"

    def __init__(
        self,
        *,
        endpoint: str = WIKIDATA_SPARQL_URL,
        max_results: int = 3,
    ) -> None:
        super().__init__()
        self._endpoint = endpoint
        self._max_results = max_results

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        queries: list[tuple[str, str]] = []
        if target.canonical_domain:
            queries.append(
                ("domain", _SPARQL_BY_DOMAIN.format(domain=_escape(target.canonical_domain)))
            )
        queries.append(("name", _SPARQL_BY_NAME.format(name=_escape(target.canonical_name))))

        results: list[RawAnchorSignal] = []
        seen_company_qids: set[str] = set()
        for reason, sparql in queries:
            if len(results) >= self._max_results:
                break
            try:
                response = await context.http.get(
                    self.source_name,
                    self._endpoint,
                    params={"query": sparql, "format": "json"},
                    headers={"Accept": "application/sparql-results+json"},
                )
            except Exception as exc:  # pragma: no cover - network failure path
                raise AdapterFetchError(f"wikidata query failed: {exc!r}") from exc
            if response.status_code >= 400:
                raise AdapterFetchError(f"wikidata returned HTTP {response.status_code}")
            try:
                payload = json.loads(response.text)
            except json.JSONDecodeError as exc:
                raise AdapterFetchError(f"wikidata returned non-JSON: {exc!r}") from exc
            for row in payload.get("results", {}).get("bindings", []):
                signal = _row_to_signal(row, reason=reason, parser_version=self.parser_version)
                if signal is None:
                    continue
                qid = row.get("company", {}).get("value", "")
                if qid in seen_company_qids:
                    continue
                seen_company_qids.add(qid)
                results.append(signal)
                if len(results) >= self._max_results:
                    break
        _log.info(
            "wikidata_anchor_hits",
            company_id=target.company_id,
            matched=len(results),
        )
        return results


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').lower()


def _row_to_signal(
    row: dict[str, Any],
    *,
    reason: str,
    parser_version: str,
) -> RawAnchorSignal | None:
    try:
        employees_raw = row["employees"]["value"]
        employees = float(employees_raw)
    except (KeyError, ValueError, TypeError):
        return None
    if employees <= 0:
        return None
    qid = row.get("company", {}).get("value", "")
    label = row.get("companyLabel", {}).get("value", "")
    asof_raw = row.get("asof", {}).get("value")
    anchor_month = _parse_asof(asof_raw)
    return RawAnchorSignal(
        source_name=SourceName.wikidata,
        entity_type=SourceEntityType.company,
        source_url=qid or None,
        anchor_month=anchor_month,
        anchor_type=AnchorType.historical_statement
        if asof_raw
        else AnchorType.current_headcount_anchor,
        headcount_value_min=employees,
        headcount_value_point=employees,
        headcount_value_max=employees,
        headcount_value_kind=HeadcountValueKind.exact,
        confidence=0.7 if reason == "domain" else 0.55,
        raw_text=f"{label} P1128={int(employees)} asof={asof_raw or 'n/a'}",
        parser_version=parser_version,
        parse_status=ParseStatus.ok,
        note=f"wikidata match by {reason}",
        normalized_payload={
            "qid": qid,
            "label": label,
            "employees": employees,
            "asof": asof_raw,
            "match_reason": reason,
        },
    )


def _parse_asof(value: str | None) -> date:
    if not value:
        return month_floor(date.today())
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return month_floor(parsed.date())
    except ValueError:
        return month_floor(date.today())
