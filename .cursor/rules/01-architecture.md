# Architecture rule

Keep these boundaries clean:

- source adapters fetch and store evidence
- parsers normalize observations
- resolution resolves canonical companies
- estimation produces monthly estimates and growth metrics
- review handles manual overrides and queueing
- serving exposes read APIs and exports

Never let raw HTML flow directly into estimation logic.
Never hide important policy decisions in ad hoc utility functions.
