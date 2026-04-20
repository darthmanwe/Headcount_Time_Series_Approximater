# Testing rule

Required test layers:
- unit tests for parsing and interval logic
- integration tests for adapter -> parser -> storage flow
- golden tests for estimation outputs on benchmark fixtures
- offline benchmark comparisons seeded from `test_source/`

Tests must use stored fixtures.
Do not depend on live web pages in CI tests.
Preserve workbook, sheet, row, and column provenance for benchmark-derived expectations.
