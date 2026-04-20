"""Unit tests for :mod:`headcount.parsers.anchors`.

Extends the coverage already provided by ``test_linkedin_public_parser``
(which imports through the parser module) with tests for the other
per-source helpers that previously lived inside their respective
observers.
"""

from __future__ import annotations

from datetime import date

import pytest

from headcount.db.enums import HeadcountValueKind
from headcount.parsers.anchors import (
    COMPANY_WEB_PARSER_VERSION,
    LINKEDIN_PUBLIC_PARSER_VERSION,
    SEC_PARSER_VERSION,
    WIKIDATA_PARSER_VERSION,
    clean_html_to_text,
    linkedin_bucket_label,
    parse_company_web_text,
    parse_sec_company_facts,
    parse_wikidata_row,
)


def test_parser_versions_exposed() -> None:
    # Observers pin their parser_version to these constants; making a
    # regression noisy is the point of this test.
    assert LINKEDIN_PUBLIC_PARSER_VERSION == "linkedin_public_v1"
    assert COMPANY_WEB_PARSER_VERSION == "company_web_v1"
    assert SEC_PARSER_VERSION == "sec_v1"
    assert WIKIDATA_PARSER_VERSION == "wikidata_v1"


# ---------------------------------------------------------------------------
# clean_html_to_text
# ---------------------------------------------------------------------------


def test_clean_html_strips_tags_and_collapses_whitespace() -> None:
    html = (
        "<html><head><title>X</title>"
        "<style>body{}</style></head>"
        "<body><script>alert(1)</script>"
        "<p>Hello <b>world</b> we have 320 employees.</p></body></html>"
    )
    text = clean_html_to_text(html)
    assert "alert" not in text
    assert "style" not in text
    assert "Hello world we have 320 employees." in text


# ---------------------------------------------------------------------------
# parse_company_web_text
# ---------------------------------------------------------------------------


def test_company_web_parses_exact() -> None:
    matches = parse_company_web_text("We have 1,250 employees across 3 offices.")
    assert len(matches) == 1
    m = matches[0]
    assert m.kind is HeadcountValueKind.exact
    assert m.value_min == m.value_point == m.value_max == 1250
    assert m.qualifier is None


def test_company_web_parses_over_qualifier_as_range() -> None:
    matches = parse_company_web_text("Our team is over 500 employees strong.")
    assert len(matches) == 1
    m = matches[0]
    assert m.kind is HeadcountValueKind.range
    assert m.value_min == 500
    assert m.value_max == pytest.approx(500 * 1.25)
    assert m.qualifier == "over"


def test_company_web_approximately() -> None:
    matches = parse_company_web_text("We employ approximately 700 people.")
    assert len(matches) == 1
    m = matches[0]
    assert m.qualifier == "approximately"
    assert m.value_min == pytest.approx(630.0)
    assert m.value_max == pytest.approx(770.0)
    assert m.value_point == 700


def test_company_web_team_of() -> None:
    matches = parse_company_web_text("We are a team of 42 builders.")
    assert len(matches) == 1
    m = matches[0]
    assert m.qualifier == "team of"
    assert m.kind is HeadcountValueKind.range
    assert m.value_point == 42


def test_company_web_dedups_qualified_tail() -> None:
    # "over 500 employees" must not ALSO yield a bare "500 employees"
    # exact match.
    text = "We are over 500 employees and growing. Visit us."
    matches = parse_company_web_text(text)
    kinds = [m.kind for m in matches]
    assert HeadcountValueKind.exact not in kinds
    assert kinds.count(HeadcountValueKind.range) == 1


def test_company_web_empty() -> None:
    assert parse_company_web_text("Nothing to see here.") == []


# ---------------------------------------------------------------------------
# parse_sec_company_facts
# ---------------------------------------------------------------------------


def _sec_fixture() -> dict:
    return {
        "entityName": "Acme",
        "facts": {
            "dei": {
                "EntityNumberOfEmployees": {
                    "units": {
                        "pure": [
                            {
                                "end": "2022-09-30",
                                "val": 154000,
                                "fy": 2022,
                                "fp": "FY",
                                "filed": "2022-10-28",
                            },
                            {
                                "end": "2023-09-30",
                                "val": 161000,
                                "fy": 2023,
                                "fp": "FY",
                                "filed": "2023-11-03",
                            },
                            {
                                "end": "2024-09-30",
                                "val": 164000,
                                "fy": 2024,
                                "fp": "FY",
                                "filed": "2024-11-01",
                            },
                            {"end": "bogus", "val": 999},
                            {"val": 1},  # no end
                            {"end": "2020-09-30"},  # no val
                        ]
                    }
                }
            }
        },
    }


def test_sec_parse_sorts_newest_first() -> None:
    rows = parse_sec_company_facts(_sec_fixture())
    assert [r.end for r in rows] == [
        date(2024, 9, 30),
        date(2023, 9, 30),
        date(2022, 9, 30),
    ]
    assert rows[0].concept == "dei:EntityNumberOfEmployees"
    assert rows[0].value == 164000
    assert rows[0].fy == 2024


def test_sec_parse_accepts_json_string() -> None:
    import json

    rows = parse_sec_company_facts(json.dumps(_sec_fixture()))
    assert len(rows) == 3


def test_sec_parse_rejects_unknown_concept() -> None:
    payload = {
        "facts": {
            "dei": {"EntityCentralIndexKey": {"units": {"pure": [{"end": "2024-01-01", "val": 1}]}}}
        }
    }
    assert parse_sec_company_facts(payload) == []


def test_sec_parse_handles_bad_json_gracefully() -> None:
    assert parse_sec_company_facts("{ not json") == []


# ---------------------------------------------------------------------------
# parse_wikidata_row
# ---------------------------------------------------------------------------


def test_wikidata_row_with_asof_is_historical() -> None:
    row = {
        "company": {"value": "http://www.wikidata.org/entity/Q312"},
        "companyLabel": {"value": "Apple Inc."},
        "employees": {"value": "164000"},
        "asof": {"value": "2024-09-30T00:00:00Z"},
    }
    parsed = parse_wikidata_row(row, reason="domain")
    assert parsed is not None
    assert parsed.employees == 164000.0
    assert parsed.is_historical is True
    assert parsed.anchor_month == date(2024, 9, 1)
    assert parsed.match_reason == "domain"


def test_wikidata_row_without_asof_is_current() -> None:
    row = {
        "company": {"value": "http://www.wikidata.org/entity/Q1"},
        "companyLabel": {"value": "Some Co"},
        "employees": {"value": "500"},
    }
    parsed = parse_wikidata_row(row, reason="name")
    assert parsed is not None
    assert parsed.is_historical is False


def test_wikidata_row_rejects_non_numeric() -> None:
    row = {
        "company": {"value": "q"},
        "companyLabel": {"value": "L"},
        "employees": {"value": "many"},
    }
    assert parse_wikidata_row(row, reason="name") is None


def test_wikidata_row_rejects_zero() -> None:
    row = {
        "company": {"value": "q"},
        "companyLabel": {"value": "L"},
        "employees": {"value": "0"},
    }
    assert parse_wikidata_row(row, reason="name") is None


# ---------------------------------------------------------------------------
# linkedin_bucket_label
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("low", "high", "label"),
    [
        (51, 200, "51-200"),
        (1001, 5000, "1,001-5,000"),
        (10001, 50000, "10,001+"),
        (77, 88, "77-88"),
    ],
)
def test_linkedin_bucket_label(low: int, high: int, label: str) -> None:
    assert linkedin_bucket_label(low, high) == label
