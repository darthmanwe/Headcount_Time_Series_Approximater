"""FastAPI entry point.

The application factory lives in :mod:`headcount.serving.api`; this
module just re-exports it so ``uvicorn apps.api.main:app`` and the
Phase 0 smoke tests (which import ``from apps.api.main import create_app``)
keep working without knowing about Phase 9's internal layout.
"""

from __future__ import annotations

from headcount.serving.api import create_app

__all__ = ["app", "create_app"]


app = create_app()
