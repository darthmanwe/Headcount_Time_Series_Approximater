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
    extract_linkedin_jsonld_employees,
    linkedin_bucket_label,
    parse_company_web_jsonld,
    parse_company_web_text,
    parse_sec_company_facts,
    parse_wikidata_row,
)


def test_parser_versions_exposed() -> None:
    # Observers pin their parser_version to these constants; making a
    # regression noisy is the point of this test.
    assert LINKEDIN_PUBLIC_PARSER_VERSION == "linkedin_public_v2"
    assert COMPANY_WEB_PARSER_VERSION == "company_web_v2"
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
# extract_linkedin_jsonld_employees (lever L2)
# ---------------------------------------------------------------------------


def _jsonld_block(payload: str) -> str:
    return (
        '<html><head><script type="application/ld+json">'
        + payload
        + "</script></head><body>no badge</body></html>"
    )


def test_jsonld_extracts_exact_value() -> None:
    html = _jsonld_block(
        '{"@context":"https://schema.org","@type":"Organization",'
        '"name":"Acme","url":"https://www.linkedin.com/company/acme/",'
        '"numberOfEmployees":{"@type":"QuantitativeValue","value":1250}}'
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.kind is HeadcountValueKind.exact
    assert result.low == result.high == 1250
    assert result.point == 1250.0
    assert result.org_name == "Acme"


def test_jsonld_extracts_min_max_range() -> None:
    html = _jsonld_block(
        '{"@type":"Organization","name":"Acme",'
        '"numberOfEmployees":{"@type":"QuantitativeValue",'
        '"minValue":51,"maxValue":200}}'
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.kind is HeadcountValueKind.bucket
    assert result.low == 51
    assert result.high == 200
    assert result.point == 125.5
    assert "51-200" in result.phrase


def test_jsonld_extracts_bare_integer() -> None:
    html = _jsonld_block(
        '{"@type":"Organization","numberOfEmployees":42}'
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.low == result.high == 42


def test_jsonld_extracts_string_number() -> None:
    html = _jsonld_block(
        '{"@type":"Organization","numberOfEmployees":"12,345"}'
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.low == result.high == 12345


def test_jsonld_prefers_exact_over_bucket_across_blocks() -> None:
    # Two blocks: first a bucket, second an exact value. Exact must win
    # even though it appears later in the document.
    html = (
        _jsonld_block(
            '{"@type":"Organization","numberOfEmployees":'
            '{"minValue":501,"maxValue":1000}}'
        )
        + _jsonld_block(
            '{"@type":"Organization","numberOfEmployees":777}'
        )
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.kind is HeadcountValueKind.exact
    assert result.low == 777


def test_jsonld_handles_graph_list() -> None:
    html = _jsonld_block(
        '[{"@type":"WebPage","name":"x"},'
        '{"@type":"Organization","numberOfEmployees":{"value":500}}]'
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.low == result.high == 500


def test_jsonld_missing_numberofemployees_returns_none() -> None:
    html = _jsonld_block(
        '{"@type":"Organization","name":"Acme","url":"x"}'
    )
    assert extract_linkedin_jsonld_employees(html) is None


def test_jsonld_non_organization_ignored() -> None:
    html = _jsonld_block(
        '{"@type":"WebPage","numberOfEmployees":999}'
    )
    assert extract_linkedin_jsonld_employees(html) is None


def test_jsonld_invalid_json_is_skipped_silently() -> None:
    html = (
        '<script type="application/ld+json">{not json</script>'
        + _jsonld_block(
            '{"@type":"Organization","numberOfEmployees":100}'
        )
    )
    result = extract_linkedin_jsonld_employees(html)
    assert result is not None
    assert result.low == 100


def test_jsonld_rejects_absurd_values() -> None:
    html = _jsonld_block(
        '{"@type":"Organization","numberOfEmployees":-5}'
    )
    assert extract_linkedin_jsonld_employees(html) is None
    html2 = _jsonld_block(
        '{"@type":"Organization","numberOfEmployees":{"value":0}}'
    )
    assert extract_linkedin_jsonld_employees(html2) is None


def test_jsonld_no_script_returns_none() -> None:
    assert extract_linkedin_jsonld_employees("<html></html>") is None


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
# company-web v2: wider stems + new phrasings + JSON-LD
# ---------------------------------------------------------------------------


def test_company_web_matches_fte_stem() -> None:
    matches = parse_company_web_text("Global workforce of 1,450 FTEs as of Q3.")
    assert any(m.value_point == 1450 for m in matches)


def test_company_web_matches_colleagues_stem() -> None:
    matches = parse_company_web_text("Our 12,000 colleagues deliver for clients.")
    assert any(m.value_point == 12000 for m in matches)


def test_company_web_matches_associates_stem() -> None:
    matches = parse_company_web_text("We employ 230 associates in 4 offices.")
    assert any(m.value_point == 230 for m in matches)


def test_company_web_matches_n_person_phrasing() -> None:
    matches = parse_company_web_text("We are a 50-person engineering organization.")
    phrases = [m.phrase.lower() for m in matches]
    assert any("50-person" in p for p in phrases)
    hit = next(m for m in matches if "50-person" in m.phrase.lower())
    assert hit.qualifier == "n-person"
    assert hit.value_point == 50


def test_company_web_matches_crew_of_and_squad_of() -> None:
    assert any(
        m.value_point == 24 for m in parse_company_web_text("Led by a crew of 24.")
    )
    assert any(
        m.value_point == 33 for m in parse_company_web_text("A squad of 33 engineers.")
    )


def test_company_web_matches_strong_idiom_with_plus() -> None:
    matches = parse_company_web_text("Now 500+ strong across 5 countries.")
    m = next(m for m in matches if m.qualifier == "strong")
    assert m.value_min == 500
    # "+ strong" widens the upper bound.
    assert m.value_max == pytest.approx(500 * 1.25)


def test_company_web_matches_headcount_idiom() -> None:
    matches = parse_company_web_text("Current headcount: 312 (as of 2024).")
    m = next(m for m in matches if m.qualifier == "headcount")
    assert m.kind is HeadcountValueKind.exact
    assert m.value_point == 312


def test_company_web_jsonld_returns_match_for_exact_number() -> None:
    html = (
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","@type":"Organization",'
        '"name":"Acme","numberOfEmployees":427}'
        "</script>"
    )
    matches = parse_company_web_jsonld(html)
    assert len(matches) == 1
    m = matches[0]
    assert m.qualifier == "jsonld"
    assert m.kind is HeadcountValueKind.exact
    assert m.value_point == 427


def test_company_web_jsonld_returns_match_for_quantitative_value_range() -> None:
    html = (
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","@type":"Organization",'
        '"name":"Acme","numberOfEmployees":'
        '{"@type":"QuantitativeValue","minValue":51,"maxValue":200}}'
        "</script>"
    )
    matches = parse_company_web_jsonld(html)
    assert len(matches) == 1
    m = matches[0]
    assert m.kind is HeadcountValueKind.bucket
    assert m.value_min == 51
    assert m.value_max == 200
    assert m.value_point == pytest.approx(125.5)


def test_company_web_jsonld_empty_when_no_organization() -> None:
    html = '<script type="application/ld+json">{"@type":"WebSite","name":"X"}</script>'
    assert parse_company_web_jsonld(html) == []


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
