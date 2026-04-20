# AGENTS.md

## Project purpose

This repository builds an internal-use headcount estimation system for companies.

The system must estimate:
- monthly headcount time series
- 6-month growth
- 1-year growth
- 2-year growth

It must be evidence-driven, deterministic, and reviewable.

## Core engineering principles

1. Prefer explicit interfaces over cleverness.
2. Prefer deterministic methods over opaque heuristics.
3. Every estimate must be traceable to evidence.
4. Entity resolution is more important than fancy modeling.
5. Confidence scoring is mandatory.
6. Manual review is a first-class feature, not an afterthought.
7. Fail closed on weak source evidence.
8. Use fixtures for parser tests. Do not rely on live network in tests.
9. Keep modules small and typed.
10. Make reruns idempotent and auditable.

## Required architecture assumptions

Use:
- Python
- Postgres
- DuckDB
- FastAPI
- Typer CLI
- SQLAlchemy
- Alembic
- Pydantic
- httpx
- BeautifulSoup / lxml
- Playwright only when a public page requires rendered DOM access
- Streamlit for initial review UI

Do not introduce heavyweight infrastructure without strong justification.

## Source acquisition policy

This project supports the following acquisition modes:

1. first-party public web
2. free-tier APIs / public datasets
3. logged-out public LinkedIn observation path
4. manual analyst validation

### Logged-out public LinkedIn path policy

This path is allowed only for pages publicly accessible without authentication.

Hard constraints:
- Do not implement login automation.
- Do not implement CAPTCHA solving.
- Do not implement rotating proxies.
- Do not implement stealth or fingerprint-evasion tooling.
- Do not implement retry loops intended to push through blocking.
- If a page is gated, blocked, or unstable, fail closed.

Expected behavior:
- fetch sparingly
- cache aggressively
- persist raw evidence snapshots
- persist normalized observations
- version all parsers
- route low-confidence cases to review

The system should reproduce the data-product behavior, not depend on a brittle access tactic.

## Estimation methodology

The base estimator should follow this shape:

1. resolve canonical company
2. gather current headcount anchors
3. choose a current anchor by policy
4. gather employment observations
5. expand employment intervals to month-level activity
6. count active public profiles by month
7. scale historical counts from the current anchor
8. segment around company events
9. detect anomalies
10. compute confidence
11. suppress or review weak long-window outputs

Never bypass canonical company resolution.

## When to use a plan

For multi-file, multi-stage, or architecture changes, use `plans/PLANS.md`.

An ExecPlan should include:
- scope
- assumptions
- module/file changes
- schema changes
- test plan
- risks
- rollback notes if relevant

For large tasks, do not code first.
Plan first.

## Coding standards

- use type hints
- use small pure functions where possible
- keep I/O at the edges
- keep parsing separate from estimation
- keep estimation separate from serving
- do not mix raw HTML handling with business logic
- prefer dataclasses or Pydantic models for contracts
- add docstrings to non-obvious modules
- log decisions that affect confidence or review state

## Testing rules

Every parser must have fixture tests.
Every estimator change must have golden tests or snapshot tests.
Never merge a parser change without at least one representative fixture.

## Definition of done

A feature is done when:
- code exists
- tests exist
- evidence trace is preserved
- confidence logic is covered
- review behavior is defined
- docs are updated
