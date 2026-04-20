Plan Mode task:

You are designing the narrow-slice implementation of an internal company headcount time series estimator.

Read these first:
- AGENTS.md
- docs/BUILD_PLAN_V2.md
- docs/METHODOLOGY_AND_ASSUMPTIONS_V2.md
- docs/SOURCE_MATRIX_V2.md
- docs/ACCEPTANCE_CRITERIA_V2.md
- plans/PLANS.md

Produce a reviewable implementation plan that includes:
- repo/module structure
- schema design
- source adapters
- logged-out public LinkedIn observer design with fail-closed constraints
- company resolution approach
- estimation pipeline
- confidence model
- review queue and overrides
- API and CLI design
- testing strategy
- file-by-file implementation order

Hard constraints:
- public pages only for LinkedIn path
- no login automation
- no CAPTCHA solving
- no rotating proxies
- no stealth evasion
- no gating-bypass retry logic

Prefer:
- deterministic code
- fixture-based tests
- low operational complexity
- explicit contracts
- small modules
