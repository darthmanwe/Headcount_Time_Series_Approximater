"""Unit tests for the logged-out public LinkedIn observer parser."""

from __future__ import annotations

import pytest

from headcount.ingest.base import CompanyTarget
from headcount.ingest.observers.linkedin_public import (
    _extract_badge,
    _extract_exact_count,
    _looks_gated,
    _resolve_slug,
)


@pytest.mark.parametrize(
    ("text", "low", "high", "open_ended"),
    [
        ("Company size: 51-200 employees", 51, 200, False),
        ("Company size: 1,001-5,000 employees", 1001, 5000, False),
        ("Company size: 201\u2013500 employees", 201, 500, False),
        ("company size 11 to 50 employees", 11, 50, False),
        ("Employees: 51 - 200 employees", 51, 200, False),
        ("Company size: 10,001+ employees", 10001, 50005, True),
        ("Company size: 5,001-10,000 employees", 5001, 10000, False),
    ],
)
def test_extract_badge_happy_paths(text: str, low: int, high: int, open_ended: bool) -> None:
    match = _extract_badge(text)
    assert match is not None
    assert match.low == low
    assert match.high == high
    assert match.open_ended is open_ended


@pytest.mark.parametrize(
    "text",
    [
        "",
        "We are a growing company.",
        "Series A: $51-200 million raised",
        # Missing the required 'employees' terminator: must not match.
        "Company size: 51-200",
        # High <= low is rejected to avoid garbage.
        "Company size: 500-200 employees",
    ],
)
def test_extract_badge_negative(text: str) -> None:
    assert _extract_badge(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1,250 associated members", 1250),
        ("320 employees", 320),
        ("1250 employees", 1250),
        ("About 500 members listed", 500),
    ],
)
def test_extract_exact_count(text: str, expected: int) -> None:
    result = _extract_exact_count(text)
    assert result is not None
    assert result[0] == expected


def test_extract_exact_count_ignores_too_small() -> None:
    # Single-digit matches are noisy on public pages.
    assert _extract_exact_count("1 employee") is None


@pytest.mark.parametrize(
    ("status", "body", "final_url", "expected_prefix"),
    [
        (429, "", "https://www.linkedin.com/company/x/", "rate_limited"),
        (403, "", "https://www.linkedin.com/company/x/", "forbidden"),
        (401, "", "https://www.linkedin.com/company/x/", "auth_required"),
        (407, "", "https://www.linkedin.com/company/x/", "auth_required"),
        (
            200,
            '<html class="authwall"><body>Sign in to see more</body></html>',
            "https://www.linkedin.com/company/x/",
            "marker:authwall",
        ),
        (
            200,
            "Please verify you are a human",
            "https://www.linkedin.com/company/x/",
            "marker:please_verify_you_are_a_human",
        ),
        (
            200,
            "<html><body>Join LinkedIn to see who you know.</body></html>",
            "https://www.linkedin.com/company/x/",
            "marker:join_linkedin_to_see",
        ),
        (
            200,
            "<html>regular page</html>",
            "https://www.linkedin.com/login?session_redirect=/company/x/",
            "login_redirect",
        ),
    ],
)
def test_looks_gated_positive(status: int, body: str, final_url: str, expected_prefix: str) -> None:
    reason = _looks_gated(status, body, final_url)
    assert reason is not None
    assert reason.startswith(expected_prefix)


def test_looks_gated_clean_page() -> None:
    body = "<html><body><dd>Company size</dd><dd>51-200 employees</dd></body></html>"
    assert _looks_gated(200, body, "https://www.linkedin.com/company/x/") is None


def test_resolve_slug_from_url() -> None:
    target = CompanyTarget(
        company_id="c-1",
        canonical_name="Acme",
        canonical_domain="acme.example",
        linkedin_company_url="https://www.linkedin.com/company/acme-inc/",
    )
    assert _resolve_slug(target) == "acme-inc"


def test_resolve_slug_none_when_missing_url() -> None:
    target = CompanyTarget(
        company_id="c-1",
        canonical_name="Acme",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    assert _resolve_slug(target) is None


def test_resolve_slug_none_for_bare_name() -> None:
    # We refuse to invent a slug from a name - scraping unrelated orgs
    # is explicitly out of scope.
    target = CompanyTarget(
        company_id="c-1",
        canonical_name="Ambiguous Holdings LLC",
        canonical_domain=None,
        linkedin_company_url="",
    )
    assert _resolve_slug(target) is None
