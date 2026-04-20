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
from headcount.parsers.anchors import WIKIDATA_PARSER_VERSION, parse_wikidata_row
from headcount.utils.logging import get_logger

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
    parser_version = WIKIDATA_PARSER_VERSION

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
                anchor = parse_wikidata_row(row, reason=reason)
                if anchor is None:
                    continue
                if anchor.qid in seen_company_qids:
                    continue
                seen_company_qids.add(anchor.qid)
                results.append(
                    RawAnchorSignal(
                        source_name=self.source_name,
                        entity_type=SourceEntityType.company,
                        source_url=anchor.qid or None,
                        anchor_month=anchor.anchor_month,
                        anchor_type=(
                            AnchorType.historical_statement
                            if anchor.is_historical
                            else AnchorType.current_headcount_anchor
                        ),
                        headcount_value_min=anchor.employees,
                        headcount_value_point=anchor.employees,
                        headcount_value_max=anchor.employees,
                        headcount_value_kind=HeadcountValueKind.exact,
                        confidence=0.7 if reason == "domain" else 0.55,
                        raw_text=f"{anchor.label} P1128={int(anchor.employees)} asof={anchor.asof or 'n/a'}",
                        parser_version=self.parser_version,
                        parse_status=ParseStatus.ok,
                        note=f"wikidata match by {reason}",
                        normalized_payload={
                            "qid": anchor.qid,
                            "label": anchor.label,
                            "employees": anchor.employees,
                            "asof": anchor.asof,
                            "match_reason": reason,
                        },
                    )
                )
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
