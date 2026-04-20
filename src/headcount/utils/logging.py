"""Structured logging via ``structlog``.

Every log record carries a stable set of contextual keys so a 2000-company
run can be filtered by ``run_id``, ``company_id``, ``stage``, ``source_name``
and ``parser_version`` without bespoke grep. JSON output is the default so
logs are machine-consumable in analyst workflows.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor

from headcount.config import get_settings

_CONFIGURED = False


def _drop_color_message(_: Any, __: str, event_dict: EventDict) -> EventDict:
    event_dict.pop("color_message", None)
    return event_dict


def configure_logging(*, force: bool = False) -> None:
    """Idempotently configure ``structlog`` and the stdlib root logger.

    Call from entry points (CLI, API, scripts). Safe to call repeatedly;
    ``force=True`` re-runs configuration (used in tests).
    """
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    settings = get_settings()
    level = getattr(logging, settings.log_level, logging.INFO)

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        _drop_color_message,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_format == "json":
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                renderer,
            ],
        )
    )

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)

    _CONFIGURED = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a configured ``structlog`` logger."""
    configure_logging()
    return structlog.stdlib.get_logger(name) if name else structlog.stdlib.get_logger()


def bind_context(**kwargs: Any) -> None:
    """Bind contextvars merged into every subsequent log record in this task."""
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound contextvars."""
    structlog.contextvars.clear_contextvars()
