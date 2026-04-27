"""Integration tests against a freshly-loaded DuckDB file.

Loaded fixture: examples/sample_data.sql (Sydney suburbs/schools/hospitals).

Skipped unless --run-db is passed.
"""

from __future__ import annotations

import pytest
from mcp.server.fastmcp import FastMCP

from duckdb_spatial_docsbox_mcp.tools import execute as execute_mod
from duckdb_spatial_docsbox_mcp.tools import introspect as introspect_mod
from duckdb_spatial_docsbox_mcp.tools import spatial as spatial_mod

pytestmark = pytest.mark.db


@pytest.fixture
def tools(database):
    mcp = FastMCP(name="t", host="127.0.0.1", port=0)
    introspect_mod.register(mcp, database)
    execute_mod.register(mcp, database)
    spatial_mod.register(mcp, database)
    tm = mcp._tool_manager
    return {
        "list_tables": tm.get_tool("list_tables").fn,
        "get_table_schema": tm.get_tool("get_table_schema").fn,
        "get_column_values": tm.get_tool("get_column_values").fn,
        "list_srids": tm.get_tool("list_srids").fn,
        "get_relationships": tm.get_tool("get_relationships").fn,
        "list_extensions": tm.get_tool("list_extensions").fn,
        "pick_interesting_tables": tm.get_tool("pick_interesting_tables").fn,
        "validate_sql": tm.get_tool("validate_sql").fn,
        "explain_sql": tm.get_tool("explain_sql").fn,
        "execute_sql": tm.get_tool("execute_sql").fn,
        "list_gdal_drivers": tm.get_tool("list_gdal_drivers").fn,
        "probe_external_file": tm.get_tool("probe_external_file").fn,
    }


@pytest.mark.asyncio
async def test_list_extensions_has_spatial(tools) -> None:
    out = await tools["list_extensions"]()
    assert out["ok"] is True
    names = {e["name"] for e in out["extensions"]}
    assert "spatial" in names


@pytest.mark.asyncio
async def test_list_tables_finds_sample(tools) -> None:
    out = await tools["list_tables"]()
    assert out["ok"]
    names = {f"{t['schema']}.{t['name']}" for t in out["tables"]}
    assert {"main.suburbs", "main.schools", "main.hospitals"} <= names


@pytest.mark.asyncio
async def test_get_table_schema_suburbs(tools) -> None:
    out = await tools["get_table_schema"](table="main.suburbs", sample_rows=2)
    assert out["ok"]
    assert out["geom_column"] == "geom"
    assert "gid" in out["primary_key"]
    assert len(out["sample"]) == 2
    # geometry should have been wrapped to GeoJSON in samples
    assert isinstance(out["sample"][0]["geom"], dict)
    assert out["sample"][0]["geom"]["type"] in {"Polygon", "MultiPolygon"}


@pytest.mark.asyncio
async def test_get_column_values(tools) -> None:
    out = await tools["get_column_values"](
        table="main.schools", column="sector", limit=10
    )
    assert out["ok"]
    sectors = {v["value"] for v in out["values"]}
    assert sectors <= {"government", "catholic", "independent"}


@pytest.mark.asyncio
async def test_get_column_values_rejects_geometry(tools) -> None:
    out = await tools["get_column_values"](
        table="main.suburbs", column="geom", limit=5
    )
    assert out["ok"] is False
    assert out["error"]


@pytest.mark.asyncio
async def test_list_srids_warns_about_no_storage(tools) -> None:
    out = await tools["list_srids"]()
    assert out["ok"]
    # DuckDB-spatial does not persist SRIDs; every geometry reports 0.
    srids = {s["srid"] for s in out["srids"]}
    assert 0 in srids
    assert "warning" in out
    assert "SRID" in out["warning"]


@pytest.mark.asyncio
async def test_get_relationships(tools) -> None:
    out = await tools["get_relationships"](table="main.schools")
    assert out["ok"]
    targets = {e["to_table"] for e in out["edges"]}
    assert "main.suburbs" in targets


@pytest.mark.asyncio
async def test_pick_interesting_tables(tools) -> None:
    out = await tools["pick_interesting_tables"](limit=5, compute_extent=True)
    assert out["ok"]
    names = {t["table"] for t in out["tables"]}
    assert "main.suburbs" in names
    sub = next(t for t in out["tables"] if t["table"] == "main.suburbs")
    assert sub["geom_column"] == "geom"
    # extent should be a GeoJSON object
    assert isinstance(sub["extent"], dict)
    assert sub["extent"]["type"] in {"Polygon", "MultiPolygon"}


@pytest.mark.asyncio
async def test_execute_sql_spatial_join(tools) -> None:
    sql = (
        "SELECT s.name AS school, sub.name AS suburb "
        "FROM schools s "
        "JOIN suburbs sub ON ST_Contains(sub.geom, s.geom) "
        "ORDER BY school"
    )
    out = await tools["execute_sql"](sql=sql, max_rows=20)
    assert out["ok"], out
    assert out["row_count"] > 0
    # Each row should have school + suburb keys
    assert {"school", "suburb"} <= set(out["rows"][0].keys())


@pytest.mark.asyncio
async def test_execute_sql_rejects_ddl(tools) -> None:
    out = await tools["execute_sql"](sql="DROP TABLE suburbs")
    assert out["ok"] is False
    assert out["error"]


@pytest.mark.asyncio
async def test_execute_sql_rejects_attach(tools) -> None:
    out = await tools["execute_sql"](sql="ATTACH '/tmp/other.duckdb'")
    assert out["ok"] is False
    assert out["error"]


@pytest.mark.asyncio
async def test_explain_sql(tools) -> None:
    out = await tools["explain_sql"](sql="SELECT * FROM suburbs")
    assert out["ok"], out
    assert isinstance(out["plan"], str)
    assert len(out["plan"]) > 0


@pytest.mark.asyncio
async def test_validate_sql_works_without_db(tools) -> None:
    out = await tools["validate_sql"](sql="SELECT * FROM suburbs")
    assert out["ok"]
    assert out["auto_limit_applied"]


@pytest.mark.asyncio
async def test_list_gdal_drivers(tools) -> None:
    out = await tools["list_gdal_drivers"]()
    assert out["ok"], out
    short_names = {d["short_name"] for d in out["drivers"]}
    # GeoJSON ships with the spatial extension's GDAL build.
    assert "GeoJSON" in short_names


@pytest.mark.asyncio
async def test_probe_external_file_rejects_url(tools) -> None:
    out = await tools["probe_external_file"](path="https://example.com/foo.geojson")
    assert out["ok"] is False
    assert "error" in out


@pytest.mark.asyncio
async def test_probe_external_file_rejects_missing(tools) -> None:
    out = await tools["probe_external_file"](path="/nonexistent/path.geojson")
    assert out["ok"] is False
    assert "error" in out
