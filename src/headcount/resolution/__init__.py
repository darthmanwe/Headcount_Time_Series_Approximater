"""Canonical company resolution, alias store, relations (Phase 3)."""

from headcount.resolution.normalize import (
    NormalizedCompany,
    clean_display_name,
    extract_legal_suffix,
    normalize_company,
    normalize_domain,
    normalize_linkedin_slug,
    normalize_name_key,
)
from headcount.resolution.resolver import ResolveResult, resolve_candidates

__all__ = [
    "NormalizedCompany",
    "ResolveResult",
    "clean_display_name",
    "extract_legal_suffix",
    "normalize_company",
    "normalize_domain",
    "normalize_linkedin_slug",
    "normalize_name_key",
    "resolve_candidates",
]
