"""Alembic migration tests against a fresh SQLite file.

Guarantees: exactly one head revision, upgrade to head creates every model
table, and downgrade to base removes them.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect

from headcount.models import Base

REPO_ROOT = Path(__file__).resolve().parents[2]


def _alembic_config(db_url: str) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def test_single_head_revision() -> None:
    cfg = _alembic_config("sqlite:///./.alembic-head-probe.sqlite")
    script = ScriptDirectory.from_config(cfg)
    heads = script.get_heads()
    assert len(heads) == 1, f"expected one head revision, got {heads}"


def test_upgrade_creates_all_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "alembic.sqlite"
    url = f"sqlite:///{db_path}"
    monkeypatch.setenv("DB_URL", url)

    cfg = _alembic_config(url)
    command.upgrade(cfg, "head")

    engine = create_engine(url, future=True)
    insp = inspect(engine)
    existing = set(insp.get_table_names())
    expected = {tbl.name for tbl in Base.metadata.sorted_tables}
    assert expected.issubset(existing), expected - existing


def test_downgrade_base_drops_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "alembic-down.sqlite"
    url = f"sqlite:///{db_path}"
    monkeypatch.setenv("DB_URL", url)

    cfg = _alembic_config(url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")

    engine = create_engine(url, future=True)
    insp = inspect(engine)
    model_tables = {tbl.name for tbl in Base.metadata.sorted_tables}
    remaining = set(insp.get_table_names()) & model_tables
    assert not remaining, remaining
