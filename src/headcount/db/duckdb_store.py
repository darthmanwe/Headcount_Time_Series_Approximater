"""Thin helpers around the local DuckDB analytical store.

Used by benchmark imports and exports to write parquet files with stable
schemas. The connection is constructed on demand to keep tests isolated.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from headcount.config import get_settings


def resolve_duckdb_path(override: Path | None = None) -> Path:
    path = override or get_settings().duckdb_path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def duckdb_connection(path: Path | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context-managed connection to the local DuckDB store."""
    target = resolve_duckdb_path(path)
    conn = duckdb.connect(str(target))
    try:
        yield conn
    finally:
        conn.close()


def write_parquet(
    df_rel: duckdb.DuckDBPyRelation,
    destination: Path,
) -> None:
    """Write a DuckDB relation to parquet, creating parent dirs as needed."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    df_rel.write_parquet(str(destination))
