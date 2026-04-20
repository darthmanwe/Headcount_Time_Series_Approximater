"""Deterministic parsing core.

Parsers are *pure* functions (no I/O, no DB) that convert raw source
artifacts into structured, versioned DTOs. Observers and orchestrators
call them. This keeps parsing separate from fetching / estimation so:

- Parser behavior is independently testable with fixtures only.
- ``parser_version`` is carried alongside every parsed value so a
  version bump can be replayed over existing raw observations (Phase 7+
  ``hc reparse``) without re-fetching anything.
- Downstream estimation never needs to think about gate detection,
  HTTP caching, or site-specific quirks - those stay behind the
  parser boundary.

Modules
-------

- :mod:`headcount.parsers.dates` - month / month-range / quarter /
  year-only parsing with a conservative confidence-reduction flag.
- :mod:`headcount.parsers.anchors` - per-source anchor parsers shared
  between the observer fast path and the replay/reparse path.
- :mod:`headcount.parsers.events` - benchmark-event-candidate to
  :class:`CompanyEvent` promotion.
- :mod:`headcount.parsers.event_merge` - deterministic merge and
  precedence policy that collapses duplicate events from multiple
  provenances into a single canonical row.
"""

from headcount.parsers.anchors import (
    COMPANY_WEB_PARSER_VERSION,
    LINKEDIN_PUBLIC_PARSER_VERSION,
    SEC_PARSER_VERSION,
    WIKIDATA_PARSER_VERSION,
    CompanyWebMatch,
    ParsedBadge,
    SecEmployeeRow,
    WikidataAnchor,
    clean_html_to_text,
    extract_linkedin_badge,
    extract_linkedin_exact_count,
    linkedin_bucket_label,
    looks_gated_linkedin,
    parse_company_web_text,
    parse_sec_company_facts,
    parse_wikidata_row,
)
from headcount.parsers.dates import (
    DATES_PARSER_VERSION,
    ParsedMonth,
    ParsedMonthRange,
    parse_month,
    parse_month_range,
)
from headcount.parsers.event_merge import (
    EVENT_MERGE_PARSER_VERSION,
    MergeResult,
    merge_events,
)
from headcount.parsers.events import (
    BENCHMARK_EVENT_DEFAULT_CONFIDENCE,
    EVENTS_PARSER_VERSION,
    PromoteResult,
    map_hint_to_event_type,
    promote_benchmark_events,
)

__all__ = [
    "BENCHMARK_EVENT_DEFAULT_CONFIDENCE",
    "COMPANY_WEB_PARSER_VERSION",
    "DATES_PARSER_VERSION",
    "EVENTS_PARSER_VERSION",
    "EVENT_MERGE_PARSER_VERSION",
    "LINKEDIN_PUBLIC_PARSER_VERSION",
    "SEC_PARSER_VERSION",
    "WIKIDATA_PARSER_VERSION",
    "CompanyWebMatch",
    "MergeResult",
    "ParsedBadge",
    "ParsedMonth",
    "ParsedMonthRange",
    "PromoteResult",
    "SecEmployeeRow",
    "WikidataAnchor",
    "clean_html_to_text",
    "extract_linkedin_badge",
    "extract_linkedin_exact_count",
    "linkedin_bucket_label",
    "looks_gated_linkedin",
    "map_hint_to_event_type",
    "merge_events",
    "parse_company_web_text",
    "parse_month",
    "parse_month_range",
    "parse_sec_company_facts",
    "parse_wikidata_row",
    "promote_benchmark_events",
]
