"""FastAPI entry point.

Phase 0 only wires up ``/healthz`` and ``/metrics``. Domain endpoints for
companies, runs, reviews, and exports land in Phase 9. Keeping this file
thin means later phases add routers without touching the app factory.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from headcount import __version__
from headcount.config import get_settings
from headcount.utils.logging import configure_logging, get_logger
from headcount.utils.metrics import REGISTRY


def create_app() -> FastAPI:
    """Application factory. Kept pure so tests can instantiate fresh apps."""
    configure_logging()
    settings = get_settings()
    log = get_logger("headcount.api")

    app = FastAPI(
        title="Headcount Estimator API",
        version=__version__,
        docs_url="/docs",
        redoc_url=None,
    )

    @app.get("/healthz", tags=["infra"])
    def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "app_env": settings.app_env,
            "method_version": settings.method_version,
        }

    if settings.metrics_enabled:

        @app.get("/metrics", tags=["infra"])
        def metrics() -> Response:
            payload = generate_latest(REGISTRY)
            return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    log.info("api_ready", version=__version__, metrics_enabled=settings.metrics_enabled)
    return app


app = create_app()
