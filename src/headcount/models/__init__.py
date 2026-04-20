"""SQLAlchemy ORM models, one aggregate per file.

Importing this package materializes every model on ``Base.metadata`` so
Alembic autogenerate sees the full schema and ``Base.metadata.create_all``
works for in-memory SQLite tests.
"""

from __future__ import annotations

from headcount.db.base import Base
from headcount.models.anchor_reconciliation import AnchorReconciliation
from headcount.models.audit_log import AuditLog
from headcount.models.company import Company
from headcount.models.company_alias import CompanyAlias
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.company_candidate import CompanyCandidate
from headcount.models.company_event import CompanyEvent
from headcount.models.company_relation import CompanyRelation
from headcount.models.company_source_link import CompanySourceLink
from headcount.models.confidence_component_score import ConfidenceComponentScore
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.manual_override import ManualOverride
from headcount.models.person import Person
from headcount.models.person_employment_observation import PersonEmploymentObservation
from headcount.models.person_identity_merge import PersonIdentityMerge
from headcount.models.review_queue_item import ReviewQueueItem
from headcount.models.run import CompanyRunStatus, Run
from headcount.models.source_budget import SourceBudget
from headcount.models.source_observation import SourceObservation

__all__ = [
    "AnchorReconciliation",
    "AuditLog",
    "Base",
    "Company",
    "CompanyAlias",
    "CompanyAnchorObservation",
    "CompanyCandidate",
    "CompanyEvent",
    "CompanyRelation",
    "CompanyRunStatus",
    "CompanySourceLink",
    "ConfidenceComponentScore",
    "EstimateVersion",
    "HeadcountEstimateMonthly",
    "ManualOverride",
    "Person",
    "PersonEmploymentObservation",
    "PersonIdentityMerge",
    "ReviewQueueItem",
    "Run",
    "SourceBudget",
    "SourceObservation",
]
