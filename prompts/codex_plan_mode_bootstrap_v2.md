You are acting as a principal engineer and system architect.

Read AGENTS.md, docs/BUILD_PLAN_V2.md, docs/METHODOLOGY_AND_ASSUMPTIONS_V2.md, docs/SOURCE_MATRIX_V2.md, docs/ACCEPTANCE_CRITERIA_V2.md, and plans/PLANS.md first.

Task:
Create a full implementation plan for the narrow-slice internal headcount estimation system.

Primary goal:
Reconstruct evidence-backed monthly employee headcount time series and 6m / 1y / 2y growth estimates for a priority company list.

Required architecture assumptions:
- Python monorepo
- Postgres + DuckDB
- FastAPI + Typer
- Pydantic models
- deterministic pipelines
- evidence-first design
- manual review queue
- local-first execution

Required source modes:
1. first-party public web
2. free-tier APIs and public datasets
3. logged-out public LinkedIn observation path
4. manual analyst validation

Critical source policy:
The logged-out LinkedIn path is allowed only for publicly accessible pages without authentication.
Do not implement login automation, CAPTCHA solving, rotating proxies, stealth browsers, or retry logic meant to bypass blocking.
If blocked or gated, fail closed and route to review.

Method the plan must preserve:
- canonical company resolution
- current anchor capture and selection
- public employment-history interval extraction
- month-level public active profile reconstruction
- scaled historical estimate using current anchor
- event-aware segmentation
- anomaly detection
- confidence scoring
- manual review

Deliverables for the plan:
- repo structure
- module boundaries
- database schema
- source adapter contracts
- parser contracts
- estimation contracts
- review workflow
- API and CLI surfaces
- testing plan
- benchmark and golden-test strategy
- implementation order
- major risks and mitigations

Rules:
- do not hand-wave
- make policy decisions explicit
- prefer least-resistance practical choices
- call out assumptions and unresolved questions clearly
- include acceptance criteria for each phase
- output the plan as a concrete ExecPlan in PLANS.md style
