Implement the approved plan.

Before writing code:
- list files to create or change
- note any unresolved assumption
- keep the architecture boundaries from the rules
- state whether the `test_source/` benchmark spreadsheets need fixture, parser, or expected-output updates

During implementation:
- keep source adapters separate from parsers
- keep parsers separate from estimation
- keep estimation separate from serving
- add tests and fixtures for every parser added
- preserve workbook, sheet, row, and column provenance for any benchmark-derived artifacts
- if estimation or export behavior changes, update the benchmark comparison path against the offline Harmonic.ai-style spreadsheets

After implementation:
- summarize changes
- summarize test coverage
- summarize benchmark coverage changed from `test_source/`
- note follow-up tasks
