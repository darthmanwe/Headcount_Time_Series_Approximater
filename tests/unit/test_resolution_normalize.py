"""Normalization helper tests."""

from __future__ import annotations

import pytest

from headcount.resolution.normalize import (
    clean_display_name,
    extract_legal_suffix,
    normalize_company,
    normalize_domain,
    normalize_linkedin_slug,
    normalize_name_key,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Acme, Inc.", "acme"),
        ("ACME", "acme"),
        ("acme", "acme"),
        ("  Acme   Corporation  ", "acme"),
        ("Rocket Industries, LLC", "rocketindustries"),
        ("Rocket  Industries LLC", "rocketindustries"),
        ("Symphony AI", "symphonyai"),
        ("Globex Corp", "globex"),
        ("Globex Corp.", "globex"),
        ("Initech Ltd.", "initech"),
        ("BASF SE", "basfse"),
        ("\u00dcbercorp AG", "ubercorp"),
        ("", ""),
    ],
)
def test_normalize_name_key(raw: str, expected: str) -> None:
    assert normalize_name_key(raw) == expected


def test_collapses_whitespace_in_display_name() -> None:
    assert clean_display_name("  Acme   \nCorporation  ") == "Acme Corporation"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Acme, Inc.", "Inc."),
        ("Rocket Industries, LLC", "LLC"),
        ("Symphony AI", None),
        ("Globex Corp.", "Corp."),
    ],
)
def test_extract_legal_suffix(raw: str, expected: str | None) -> None:
    assert extract_legal_suffix(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("acme.com", "acme.com"),
        ("ACME.COM", "acme.com"),
        ("www.acme.com", "acme.com"),
        ("https://www.acme.com/about", "acme.com"),
        ("https://ACME.co.uk/", "acme.co.uk"),
        ("  acme.io  ", "acme.io"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_domain(raw: str | None, expected: str | None) -> None:
    assert normalize_domain(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("linkedin.com/company/acme", "acme"),
        ("https://www.linkedin.com/company/Symphony-AI/", "symphony-ai"),
        ("https://linkedin.com/company/globex?foo=1", "globex"),
        ("acme-bare-slug", "acme-bare-slug"),
        ("acme.com", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_linkedin_slug(raw: str | None, expected: str | None) -> None:
    assert normalize_linkedin_slug(raw) == expected


def test_normalize_company_builds_consistent_keys() -> None:
    n = normalize_company(
        "Acme, Inc.",
        raw_domain="https://www.ACME.com/",
        raw_linkedin="linkedin.com/company/ACME",
    )
    assert n.display_name == "Acme, Inc."
    assert n.name_key == "acme"
    assert n.domain == "acme.com"
    assert n.domain_key == "acme.com"
    assert n.linkedin_slug == "acme"
    assert n.legal_suffix == "Inc."
