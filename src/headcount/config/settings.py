"""Typed configuration surface, env-driven via pydantic-settings.

All tunable knobs for the narrow-slice estimator live here. Values are
deliberately explicit so runs are reproducible: ``METHOD_VERSION``,
``ANCHOR_POLICY_VERSION`` and ``COVERAGE_CURVE_VERSION`` are persisted into
``estimate_version`` rows so historical outputs can be reconstructed even if
defaults drift later.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogFormat = Literal["json", "console"]
AppEnv = Literal["development", "test", "staging", "production"]


class Settings(BaseSettings):
    """Central runtime configuration.

    All paths are resolved relative to the process working directory so the
    tool stays portable. ``Settings`` is intentionally a plain container: no
    I/O happens at construction time.
    """

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: AppEnv = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: LogFormat = Field(default="json", alias="LOG_FORMAT")

    db_url: str = Field(default="sqlite:///./data/headcount.sqlite", alias="DB_URL")
    duckdb_path: Path = Field(
        default=Path("./data/outputs/headcount.duckdb"),
        alias="DUCKDB_PATH",
    )
    cache_dir: Path = Field(default=Path("./data/cache"), alias="CACHE_DIR")
    run_artifact_dir: Path = Field(
        default=Path("./data/outputs/runs"),
        alias="RUN_ARTIFACT_DIR",
    )
    seed_dir: Path = Field(default=Path("./data/seeds"), alias="SEED_DIR")
    fixture_dir: Path = Field(default=Path("./data/fixtures"), alias="FIXTURE_DIR")

    method_version: str = Field(default="hc-v1", alias="METHOD_VERSION")
    anchor_policy_version: str = Field(default="anchor-v1", alias="ANCHOR_POLICY_VERSION")
    coverage_curve_version: str = Field(default="coverage-v1", alias="COVERAGE_CURVE_VERSION")

    linkedin_public_max_requests_per_run: int = Field(
        default=400, alias="LINKEDIN_PUBLIC_MAX_REQUESTS_PER_RUN", ge=0
    )
    linkedin_public_max_rpm: int = Field(default=6, alias="LINKEDIN_PUBLIC_MAX_RPM", ge=1)
    linkedin_public_circuit_breaker_n: int = Field(
        default=5, alias="LINKEDIN_PUBLIC_CIRCUIT_BREAKER_N", ge=1
    )
    linkedin_public_company_ttl_days: int = Field(
        default=30, alias="LINKEDIN_PUBLIC_COMPANY_TTL_DAYS", ge=0
    )
    linkedin_public_profile_ttl_days: int = Field(
        default=90, alias="LINKEDIN_PUBLIC_PROFILE_TTL_DAYS", ge=0
    )

    company_web_max_concurrency: int = Field(default=4, alias="COMPANY_WEB_MAX_CONCURRENCY", ge=1)
    sec_user_agent: str = Field(
        default="Headcount Estimator internal-use contact@example.com",
        alias="SEC_USER_AGENT",
    )
    wikidata_endpoint: str = Field(
        default="https://query.wikidata.org/sparql", alias="WIKIDATA_ENDPOINT"
    )

    min_current_profile_sample_6m: int = Field(
        default=30, alias="MIN_CURRENT_PROFILE_SAMPLE_6M", ge=1
    )
    min_current_profile_sample_1y: int = Field(
        default=50, alias="MIN_CURRENT_PROFILE_SAMPLE_1Y", ge=1
    )
    min_current_profile_sample_2y: int = Field(
        default=80, alias="MIN_CURRENT_PROFILE_SAMPLE_2Y", ge=1
    )
    benchmark_disagreement_pct_current: float = Field(
        default=0.15, alias="BENCHMARK_DISAGREEMENT_PCT_CURRENT", ge=0.0, le=1.0
    )
    benchmark_disagreement_pct_2y: float = Field(
        default=0.25, alias="BENCHMARK_DISAGREEMENT_PCT_2Y", ge=0.0, le=1.0
    )

    api_host: str = Field(default="127.0.0.1", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT", ge=1, le=65535)
    metrics_enabled: bool = Field(default=True, alias="METRICS_ENABLED")

    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if normalized not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(allowed)}, got {value!r}")
        return normalized

    def ensure_runtime_dirs(self) -> None:
        """Create data directories on demand. Safe to call repeatedly."""
        for path in (
            self.cache_dir,
            self.run_artifact_dir,
            self.seed_dir,
            self.fixture_dir,
            self.duckdb_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-cached ``Settings`` instance."""
    return Settings()
