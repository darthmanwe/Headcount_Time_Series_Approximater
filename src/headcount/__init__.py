"""Internal-use, evidence-driven headcount time series estimator.

Top-level package. Submodules follow the pipeline boundaries:
    ingest -> parsers -> resolution -> estimation -> review -> serving
with ``config``, ``db``, ``models``, ``schemas``, ``clients``, ``utils``
as cross-cutting concerns.
"""

from __future__ import annotations

__version__ = "0.1.0"
