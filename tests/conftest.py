"""Shared pytest fixtures.

Tests fall into two categories:

* **Unit tests** for the SQL validator and plan templates. Run without a
  database.
* **Integration tests** marked with the ``db`` marker that require a
  DuckDB file populated from ``examples/sample_data.sql``. We bootstrap
  this in-process: a writable ``duckdb.connect`` runs the script
  against a freshly-created temp file, then the ``Database`` class
  re-opens that file with ``read_only=True``. Spawning the CLI is
  unnecessary because everything we need is in the embedded Python API.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DATA = REPO_ROOT / "examples" / "sample_data.sql"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-db",
        action="store_true",
        default=False,
        help="run integration tests that need a populated DuckDB fixture.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-db"):
        return
    skip_db = pytest.mark.skip(
        reason="needs --run-db; populates a temp DuckDB from examples/sample_data.sql"
    )
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip_db)


@pytest.fixture
def database(tmp_path) -> Iterator:
    """Yield a Database backed by a freshly-loaded sample DuckDB file."""
    import duckdb

    from duckdb_spatial_docsbox_mcp.db import Database, DuckConfig

    db_path = tmp_path / "sample.duckdb"

    # Bootstrap: writable conn loads spatial + the sample DDL/DML.
    sql = SAMPLE_DATA.read_text("utf-8")
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(sql)
    finally:
        con.close()

    cfg = DuckConfig(path=str(db_path), statement_timeout_s=5.0, load_spatial=True)
    db = Database(cfg)
    try:
        yield db
    finally:
        db.close()
