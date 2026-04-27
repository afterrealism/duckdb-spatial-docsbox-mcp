"""DuckDB-spatial-specific situational-awareness tools.

* ``list_gdal_drivers``     — every driver bundled with the loaded spatial
                              extension (via ``ST_Drivers()``). Tells the
                              LLM *what file types it can ingest*.
* ``probe_external_file``   — peek at the first few rows of a Shapefile,
                              GeoJSON, GeoParquet, CSV, etc. *without*
                              loading it into the database. Routes to
                              ``ST_Read`` for GDAL-native formats and to
                              ``read_parquet`` / ``read_csv_auto`` for the
                              tabular ones. The path must be a local file
                              (no URLs); HTTP/S3 is blocked because that
                              would require ``INSTALL httpfs`` which is in
                              the denylist.

Both tools refuse to run if ``Database`` is ``None`` (no path configured)
because they need a connection to invoke the spatial functions.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Annotated, Any

import duckdb
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..db import Database, TimeoutError as DuckTimeoutError

logger = logging.getLogger(__name__)

# Extensions we route through read_csv_auto / read_parquet / ST_Read.
_TABULAR_EXTS = {".csv", ".tsv", ".txt"}
_PARQUET_EXTS = {".parquet", ".pq"}
_GDAL_EXTS = {
    ".geojson",
    ".json",
    ".shp",
    ".gpkg",
    ".kml",
    ".gml",
    ".gpx",
    ".fgb",
    ".geojsonl",
}


def _err(kind: str, message: str, hint: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "error": kind, "message": message}
    if hint is not None:
        out["hint"] = hint
    return out


def _safe_path(p: str) -> Path | None:
    """Reject URLs and unreadable files. Return absolute Path on success."""
    if "://" in p:
        return None
    path = Path(p).expanduser().resolve()
    if not path.is_file():
        return None
    if not os.access(path, os.R_OK):
        return None
    return path


def register(mcp: FastMCP, db: Database | None) -> None:
    if db is None:
        _register_stubs(mcp)
        return
    _register_list_gdal_drivers(mcp, db)
    _register_probe_external_file(mcp, db)


def _register_stubs(mcp: FastMCP) -> None:
    msg = (
        "DUCKDB_DOCSBOX_PATH is not set; spatial probe tools are disabled. "
        "Set DUCKDB_DOCSBOX_PATH (':memory:' is fine) and restart."
    )

    async def _stub() -> dict[str, Any]:
        return _err("not_configured", msg)

    for name in ("list_gdal_drivers", "probe_external_file"):
        mcp.tool(name=name, description=msg)(_stub)


def _register_list_gdal_drivers(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_gdal_drivers",
        description=(
            "List GDAL drivers available through the DuckDB spatial extension "
            "(via ST_Drivers()). Each driver row has short_name, long_name, "
            "can_create, can_copy, can_open. Use this before calling "
            "probe_external_file to know which formats are supported.\n\n"
            "Example: list_gdal_drivers() -> {\"ok\": true, \"drivers\": "
            "[{\"short_name\": \"GeoJSON\", \"long_name\": \"GeoJSON\", "
            "\"can_open\": true, ...}], \"count\": 80}"
        ),
    )
    async def list_gdal_drivers(
        filter_substr: Annotated[
            str | None,
            Field(description="Optional case-insensitive filter on short_name/long_name."),
        ] = None,
    ) -> dict[str, Any]:
        try:
            with db.readonly() as conn:
                conn.execute("SELECT * FROM ST_Drivers()")
                cols = [d[0] for d in conn.description]
                rows = [dict(zip(cols, r, strict=False)) for r in conn.fetchall()]
        except duckdb.Error as exc:
            return _err(
                "query_failed",
                str(exc),
                hint=(
                    "If you got 'function ST_Drivers does not exist', the "
                    "spatial extension is not loaded; restart with "
                    "DUCKDB_DOCSBOX_LOAD_SPATIAL=1."
                ),
            )
        if filter_substr:
            needle = filter_substr.lower()
            rows = [
                r
                for r in rows
                if needle in str(r.get("short_name", "")).lower()
                or needle in str(r.get("long_name", "")).lower()
            ]
        return {"ok": True, "drivers": rows, "count": len(rows)}


def _register_probe_external_file(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="probe_external_file",
        description=(
            "Peek at the first few rows of an on-disk file *without* "
            "importing it into the database. Detects format by extension and "
            "routes to read_csv_auto / read_parquet / ST_Read. Use this when "
            "the user wants to know 'what's in this file' before deciding "
            "how to query it.\n\n"
            "Local files only — URLs are rejected because httpfs is denied. "
            "Use run_locally for remote files.\n\n"
            "Example: probe_external_file(path='/data/cities.geojson', limit=3) "
            "-> {\"ok\": true, \"format\": \"gdal\", \"columns\": [...], "
            "\"rows\": [...], \"row_count\": 3}"
        ),
    )
    async def probe_external_file(
        path: Annotated[str, Field(description="Absolute or ~-expanded local path.")],
        format: Annotated[  # noqa: A002 - shadowing builtin is fine here
            str,
            Field(
                description=(
                    "'auto' (by extension), 'csv', 'parquet', or 'gdal' "
                    "(forces ST_Read). Default 'auto'."
                ),
            ),
        ] = "auto",
        limit: Annotated[
            int,
            Field(description="Rows to fetch (1-50)."),
        ] = 5,
    ) -> dict[str, Any]:
        limit = max(1, min(50, int(limit)))
        safe = _safe_path(path)
        if safe is None:
            return _err(
                "invalid_path",
                f"path {path!r} is not a readable local file or contains a URL scheme",
                hint=(
                    "Remote/object-storage files need INSTALL httpfs which is "
                    "blocked here. Stage the file locally first, or use "
                    "run_locally to fetch it."
                ),
            )

        ext = safe.suffix.lower()
        chosen = format.lower()
        if chosen == "auto":
            if ext in _PARQUET_EXTS:
                chosen = "parquet"
            elif ext in _TABULAR_EXTS:
                chosen = "csv"
            elif ext in _GDAL_EXTS:
                chosen = "gdal"
            else:
                chosen = "gdal"  # let ST_Read decide; it errors gracefully

        if chosen == "csv":
            sql = f"SELECT * FROM read_csv_auto(?) LIMIT {limit}"
        elif chosen == "parquet":
            sql = f"SELECT * FROM read_parquet(?) LIMIT {limit}"
        elif chosen == "gdal":
            sql = f"SELECT * FROM ST_Read(?) LIMIT {limit}"
        else:
            return _err("invalid_arg", f"format={format!r} not in {{auto,csv,parquet,gdal}}")

        try:
            with db.readonly() as conn:
                db.run_with_timeout(conn, sql, [str(safe)])
                cols = [d[0] for d in conn.description]
                rows = [dict(zip(cols, r, strict=False)) for r in conn.fetchall()]
        except DuckTimeoutError as exc:
            return _err("timeout", str(exc))
        except duckdb.Error as exc:
            return _err(
                "probe_failed",
                str(exc),
                hint=(
                    "Confirm the file extension matches its content; for "
                    "exotic formats pass format='gdal' to force ST_Read."
                ),
            )

        # Format any geometry cells as text for readability.
        for row in rows:
            for k, v in row.items():
                if hasattr(v, "wkt"):  # duckdb GeoSeries / Geometry repr
                    row[k] = str(v)
        return {
            "ok": True,
            "path": str(safe),
            "format": chosen,
            "columns": cols,
            "rows": rows,
            "row_count": len(rows),
        }


__all__ = ["register"]
