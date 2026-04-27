"""Plan-only tool returns structured shell command plans for DuckDB.

These tests do not invoke shell commands; they verify that the planner
produces the right shape for each task.
"""

from __future__ import annotations

import base64
import re

import pytest
from mcp.server.fastmcp import FastMCP

from duckdb_spatial_docsbox_mcp.tools import run_locally as run_locally_mod


@pytest.fixture
def planner():
    """Return the run_locally tool callable."""
    mcp = FastMCP(name="t", host="127.0.0.1", port=0)
    run_locally_mod.register(mcp)
    tool = mcp._tool_manager.get_tool("run_locally")
    return tool.fn


def _decoded_sql(plan: dict) -> str:
    """Decode the base64 SQL staged in the write_sql step, if present."""
    for step in plan["steps"]:
        if step.get("name") == "write_sql":
            m = re.search(r"echo ([A-Za-z0-9+/=]+) \| base64 -d", step["shell"])
            if m:
                return base64.b64decode(m.group(1)).decode("utf-8")
    return ""


@pytest.mark.asyncio
async def test_connect_plan(planner) -> None:
    out = await planner(task="connect")
    assert out["ok"] is True
    plan = out["plan"]
    assert plan["task"] == "connect"
    assert any("duckdb" in s["shell"] for s in plan["steps"])


@pytest.mark.asyncio
async def test_install_spatial_plan(planner) -> None:
    out = await planner(task="install_spatial")
    assert out["ok"] is True
    joined = " ".join(s["shell"] for s in out["plan"]["steps"])
    assert "INSTALL spatial" in joined
    assert "LOAD spatial" in joined


@pytest.mark.asyncio
async def test_query_plan(planner) -> None:
    out = await planner(task="query", sql="SELECT 1")
    assert out["ok"] is True
    plan = out["plan"]
    assert plan["task"] == "query"
    # SQL should be base64-injected via mktemp; should not appear raw.
    joined = " ".join(s["shell"] for s in plan["steps"])
    assert "duckdb" in joined
    assert "SELECT 1" not in joined  # base64-encoded, not raw


@pytest.mark.asyncio
async def test_query_requires_sql(planner) -> None:
    out = await planner(task="query")
    assert out["ok"] is False
    assert "sql" in out["error"].lower()


@pytest.mark.asyncio
async def test_dump_plan(planner) -> None:
    out = await planner(task="dump", path="/tmp/duckdb-dump")
    assert out["ok"] is True
    sql = _decoded_sql(out["plan"])
    assert "EXPORT DATABASE" in sql
    assert "/tmp/duckdb-dump" in sql


@pytest.mark.asyncio
async def test_export_geojson_plan(planner) -> None:
    out = await planner(
        task="export_geojson",
        sql="SELECT * FROM suburbs",
        path="/tmp/suburbs.geojson",
    )
    assert out["ok"] is True
    steps = out["plan"]["steps"]
    joined = " ".join(s["shell"] for s in steps)
    assert "duckdb" in joined
    # Cleanup step rms the temp .sql.
    assert any(s.get("name") == "cleanup" for s in steps)


@pytest.mark.asyncio
async def test_export_geojson_requires_args(planner) -> None:
    out = await planner(task="export_geojson")
    assert out["ok"] is False
    assert "sql" in out["error"].lower()


@pytest.mark.asyncio
async def test_import_csv_plan(planner) -> None:
    out = await planner(task="import_csv", path="/tmp/data.csv", table="visits")
    assert out["ok"] is True
    sql = _decoded_sql(out["plan"])
    assert "read_csv_auto" in sql
    assert "visits" in sql


@pytest.mark.asyncio
async def test_import_parquet_plan(planner) -> None:
    out = await planner(task="import_parquet", path="/tmp/data.parquet")
    assert out["ok"] is True
    sql = _decoded_sql(out["plan"])
    assert "read_parquet" in sql


@pytest.mark.asyncio
async def test_import_geojson_plan(planner) -> None:
    out = await planner(task="import_geojson", path="/tmp/data.geojson")
    assert out["ok"] is True
    sql = _decoded_sql(out["plan"])
    assert "ST_Read" in sql
    assert "data.geojson" in sql


@pytest.mark.asyncio
async def test_import_shapefile_plan(planner) -> None:
    out = await planner(task="import_shapefile", path="/tmp/data.shp")
    assert out["ok"] is True
    plan = out["plan"]
    sql = _decoded_sql(plan)
    assert "ST_Read" in sql
    # Sidecar note should be surfaced.
    assert any("sidecar" in n.lower() for n in plan["notes"])


@pytest.mark.asyncio
async def test_import_csv_quotes_unsafe_path(planner) -> None:
    """A path with shell-meta or SQL-quote chars must not appear unescaped."""
    out = await planner(
        task="import_csv",
        path="/tmp/weird\";rm -rf $HOME;.csv",
        table="weird",
    )
    assert out["ok"] is True
    # Raw shell never carries the path; only base64 + the temp-file runner.
    joined = " ".join(s["shell"] for s in out["plan"]["steps"])
    assert "rm -rf $HOME" not in joined
    sql = _decoded_sql(out["plan"])
    # SQL literal escapes: single quotes doubled; the path lands intact.
    assert "weird" in sql
    assert "read_csv_auto" in sql


@pytest.mark.asyncio
async def test_import_csv_quotes_unsafe_table_identifier(planner) -> None:
    """A table name with non-identifier chars must be double-quoted."""
    out = await planner(task="import_csv", path="/tmp/data.csv", table="weird table")
    assert out["ok"] is True
    sql = _decoded_sql(out["plan"])
    assert '"weird table"' in sql


@pytest.mark.asyncio
async def test_unknown_task(planner) -> None:
    out = await planner(task="not_a_task")
    assert out["ok"] is False
    assert "error" in out
    assert "available_templates" in out
