"""Unit tests for the static SQL validator (DuckDB dialect).

The validator is the most important security boundary. These tests pin
the contract: only top-level SELECT/WITH/EXPLAIN, no multi-statement, no
denylisted keywords (DDL, DML, ATTACH/DETACH/INSTALL/LOAD/EXPORT/IMPORT/
PRAGMA/CHECKPOINT/USE) or denylisted functions (read_text, read_blob,
shell, system), auto-LIMIT injection on bare SELECTs.
"""

from __future__ import annotations

import pytest

from duckdb_spatial_docsbox_mcp.sql_validator import static_validate


# ---- Allowed shapes -------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select 1, 2, 3",
        "SELECT * FROM foo WHERE x = 1",
        "WITH a AS (SELECT 1) SELECT * FROM a",
        "EXPLAIN SELECT * FROM foo",
        "EXPLAIN ANALYZE SELECT * FROM foo",
        "  \n  SELECT 1  \n  ",
    ],
)
def test_allows_basic_selects(sql: str) -> None:
    r = static_validate(sql)
    assert r.ok, r.error


def test_auto_injects_limit() -> None:
    r = static_validate("SELECT * FROM foo")
    assert r.ok
    assert r.auto_limit_applied is True
    assert r.sql.rstrip().upper().endswith("LIMIT 500")


def test_keeps_existing_limit() -> None:
    r = static_validate("SELECT * FROM foo LIMIT 10")
    assert r.ok
    assert r.auto_limit_applied is False
    assert "LIMIT 10" in r.sql.upper()


def test_explain_does_not_get_limit() -> None:
    r = static_validate("EXPLAIN SELECT * FROM foo")
    assert r.ok
    # EXPLAIN already returns one row per plan node; LIMIT is meaningless.
    assert "LIMIT 500" not in r.sql.upper().replace(" ", "")


# ---- Forbidden shapes -----------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE foo",
        "DELETE FROM foo",
        "INSERT INTO foo VALUES (1)",
        "UPDATE foo SET x = 1",
        "ALTER TABLE foo ADD COLUMN y int",
        "TRUNCATE foo",
        "CREATE TABLE foo (x int)",
        "GRANT ALL ON foo TO bar",
        "REVOKE ALL ON foo FROM bar",
        "VACUUM",
        "COPY foo FROM '/tmp/x'",
        "CALL my_proc()",
    ],
)
def test_rejects_dml_ddl(sql: str) -> None:
    r = static_validate(sql)
    assert not r.ok
    assert r.error


@pytest.mark.parametrize(
    "sql",
    [
        "INSTALL spatial",
        "LOAD spatial",
        "ATTACH '/tmp/other.duckdb'",
        "DETACH other",
        "EXPORT DATABASE '/tmp/dump'",
        "IMPORT DATABASE '/tmp/dump'",
        "PRAGMA show_tables",
        "CHECKPOINT",
        "USE other",
    ],
)
def test_rejects_duckdb_specific_keywords(sql: str) -> None:
    r = static_validate(sql)
    assert not r.ok, sql
    assert r.error


def test_rejects_multistatement() -> None:
    r = static_validate("SELECT 1; SELECT 2")
    assert not r.ok


def test_rejects_dangerous_functions() -> None:
    for fn in ("read_text", "read_blob", "read_text_auto", "read_blob_auto", "shell", "system"):
        r = static_validate(f"SELECT {fn}('x')")
        assert not r.ok, fn


def test_rejects_garbage() -> None:
    r = static_validate("not even sql")
    assert not r.ok


def test_empty_sql_rejected() -> None:
    r = static_validate("")
    assert not r.ok
