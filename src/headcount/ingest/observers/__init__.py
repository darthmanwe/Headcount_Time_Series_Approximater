"""Phase 4 anchor observers.

Each observer is a concrete :class:`AnchorSourceAdapter` that extracts
current-headcount anchors from a single kind of source. Keeping one
class per source module means the orchestrator can enable / disable
sources independently and analysts can audit parser behavior in
isolation.
"""

from headcount.ingest.observers.company_web import CompanyWebObserver
from headcount.ingest.observers.manual import ManualAnchorObserver
from headcount.ingest.observers.sec import SECObserver
from headcount.ingest.observers.wikidata import WikidataObserver

__all__ = [
    "CompanyWebObserver",
    "ManualAnchorObserver",
    "SECObserver",
    "WikidataObserver",
]
