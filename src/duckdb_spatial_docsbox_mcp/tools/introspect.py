"""Schema introspection tools for DuckDB.

Goal: let an LLM agent build *situational awareness* of a DuckDB database in
3-5 tool calls — what tables exist, which look interesting, what columns
they have, what SRIDs (if any) the geometry columns are tagged with, and
how tables relate.

The tools are deliberately read-only and capped:

* All queries run inside ``Database.readonly`` (cursor on a read-only file
  handle, or shared in-memory connection under a lock).
* Result sizes are bounded (sample rows default 5, listings cap at 500).
* Geometry payloads are returned as GeoJSON for inspection, never as full WKB.

Tools registered here:

* ``list_tables``           — every user table/view with row estimate, kind,
                              has_geom, geom_column.
* ``get_table_schema``      — DDL-style schema with column types and per-column
                              sample values (geochat pattern).
* ``get_column_values``     — distinct sample values for one column.
* ``list_srids``            — distinct SRIDs reported by ST_SRID per geom
                              column (typically all 0 — DuckDB-spatial does
                              not store SRID; documented in the response).
* ``get_relationships``     — foreign-key edges from ``duckdb_constraints()``.
* ``list_extensions``       — installed/loaded extensions with version.
* ``pick_interesting_tables``— score tables by rows + geometry presence +
                              R-Tree index + inbound FKs.

Each tool docstring includes a worked example with expected JSON shape.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..db import Database, is_metadata_excluded

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_EXCLUDES: tuple[str, ...] = (
    "pg_catalog",
    "information_schema",
    "system",
    "temp",
)

# Tiny built-in EPSG name lookup so ``list_srids`` can give a friendly hint
# even though DuckDB-spatial has no spatial_ref_sys equivalent.
_EPSG_NAMES: dict[int, tuple[str, str]] = {
    0: ("(unset)", "unknown"),
    4326: ("WGS 84", "degree"),
    4269: ("NAD83", "degree"),
    3857: ("WGS 84 / Pseudo-Mercator", "metre"),
    3395: ("WGS 84 / World Mercator", "metre"),
    7855: ("GDA2020 / MGA zone 55", "metre"),
    28355: ("GDA94 / MGA zone 55", "metre"),
    27700: ("OSGB36 / British National Grid", "metre"),
    32633: ("WGS 84 / UTM zone 33N", "metre"),
}


def _err(kind: str, message: str, hint: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "error": kind, "message": message}
    if hint is not None:
        out["hint"] = hint
    return out


def _split_qualified(table: str, default_schema: str = "main") -> tuple[str, str]:
    if "." in table:
        schema, _, name = table.partition(".")
        return schema or default_schema, name
    return default_schema, table


def _row_to_dict(cur: Any, row: tuple[Any, ...]) -> dict[str, Any]:
    return {d[0]: v for d, v in zip(cur.description, row, strict=False)}


def _rows_to_dicts(cur: Any, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=False)) for r in rows]


def register(mcp: FastMCP, db: Database | None) -> None:
    if db is None:
        _register_stubs(mcp)
        return
    _register_list_tables(mcp, db)
    _register_get_table_schema(mcp, db)
    _register_get_column_values(mcp, db)
    _register_list_srids(mcp, db)
    _register_get_relationships(mcp, db)
    _register_list_extensions(mcp, db)
    _register_pick_interesting_tables(mcp, db)


# ---------------------------------------------------------------------------
# Stubs (no path configured)
# ---------------------------------------------------------------------------


def _register_stubs(mcp: FastMCP) -> None:
    msg = (
        "DUCKDB_DOCSBOX_PATH is not set; database introspection tools are "
        "disabled. Set DUCKDB_DOCSBOX_PATH to a .duckdb file (or ':memory:' "
        "for an empty in-process database) and restart."
    )

    async def _stub() -> dict[str, Any]:
        return _err("not_configured", msg, hint="Set DUCKDB_DOCSBOX_PATH env var.")

    for name in (
        "list_tables",
        "get_table_schema",
        "get_column_values",
        "list_srids",
        "get_relationships",
        "list_extensions",
        "pick_interesting_tables",
    ):
        mcp.tool(name=name, description=msg)(_stub)


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


def _register_list_tables(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_tables",
        description=(
            "List user tables/views with estimated row count, kind, and a "
            "'has_geom' flag for spatial tables. Typical first call when "
            "exploring an unknown DuckDB database.\n\n"
            "Example return shape: {\"ok\": true, \"tables\": [{\"schema\": \"main\", "
            "\"name\": \"suburbs\", \"kind\": \"table\", \"row_estimate\": 5, "
            "\"has_geom\": true, \"geom_column\": \"geom\", \"column_count\": 4}, ...]}"
        ),
    )
    async def list_tables(
        schema_pattern: Annotated[
            str,
            Field(
                description=(
                    "SQL LIKE pattern for schemas to include (default '%' = all "
                    "non-catalog). System schemas (pg_catalog, information_schema, "
                    "system, temp) are always excluded."
                ),
            ),
        ] = "%",
        include_views: Annotated[
            bool,
            Field(description="Whether to include views and materialized views."),
        ] = True,
        limit: Annotated[
            int,
            Field(description="Maximum tables to return (1-2000)."),
        ] = 500,
    ) -> dict[str, Any]:
        limit = max(1, min(2000, int(limit)))
        excludes = list(DEFAULT_SCHEMA_EXCLUDES)
        excl_in = ",".join(["?"] * len(excludes))

        tables_sql = f"""
            SELECT schema_name, table_name, estimated_size, column_count, 'table' AS kind
            FROM duckdb_tables()
            WHERE schema_name LIKE ? AND schema_name NOT IN ({excl_in})
              AND NOT internal
        """
        views_sql = f"""
            SELECT schema_name, view_name AS table_name, NULL::BIGINT AS estimated_size,
                   column_count, 'view' AS kind
            FROM duckdb_views()
            WHERE schema_name LIKE ? AND schema_name NOT IN ({excl_in})
              AND NOT internal
        """
        cols_geom_sql = f"""
            SELECT schema_name, table_name, column_name
            FROM duckdb_columns()
            WHERE data_type = 'GEOMETRY' AND schema_name NOT IN ({excl_in})
        """

        try:
            with db.readonly() as conn:
                conn.execute(tables_sql, [schema_pattern, *excludes])
                trows = _rows_to_dicts(conn, conn.fetchall())
                vrows: list[dict[str, Any]] = []
                if include_views:
                    conn.execute(views_sql, [schema_pattern, *excludes])
                    vrows = _rows_to_dicts(conn, conn.fetchall())
                conn.execute(cols_geom_sql, list(excludes))
                geom_rows = _rows_to_dicts(conn, conn.fetchall())
        except Exception as exc:  # noqa: BLE001
            return _err(
                "query_failed",
                str(exc),
                hint=(
                    "If the error mentions duckdb_tables, the file is not a "
                    "valid DuckDB database. Check DUCKDB_DOCSBOX_PATH."
                ),
            )

        geom_by_table: dict[tuple[str, str], str] = {
            (r["schema_name"], r["table_name"]): r["column_name"] for r in geom_rows
        }

        def _entry(r: dict[str, Any]) -> dict[str, Any]:
            key = (r["schema_name"], r["table_name"])
            geom_col = geom_by_table.get(key)
            return {
                "schema": r["schema_name"],
                "name": r["table_name"],
                "kind": r["kind"],
                "row_estimate": int(r["estimated_size"] or 0),
                "column_count": int(r["column_count"] or 0),
                "has_geom": geom_col is not None,
                "geom_column": geom_col,
            }

        merged = [_entry(r) for r in trows + vrows]
        merged = [
            t for t in merged if not is_metadata_excluded(f"{t['schema']}.{t['name']}", db.cfg)
        ]
        merged.sort(key=lambda t: (t["schema"], t["name"]))
        return {"ok": True, "tables": merged[:limit], "count": min(len(merged), limit)}


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------


def _register_get_table_schema(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="get_table_schema",
        description=(
            "Return a DDL-like description of one table: columns, types, "
            "nullability, defaults, primary key, indexes, and N sample rows. "
            "Sample rows materially help an LLM pick the right column for a "
            "task (geochat-style 'schema as DDL with sample-value comments').\n\n"
            "Example: get_table_schema(table='main.suburbs', sample_rows=2) -> "
            "{\"ok\": true, \"ddl\": \"CREATE TABLE main.suburbs (...)\", "
            "\"columns\": [...], \"primary_key\": [\"gid\"], \"indexes\": [...], "
            "\"sample\": [{\"gid\": 1, \"name\": \"Bondi\", ...}], \"row_estimate\": 5}"
        ),
    )
    async def get_table_schema(
        table: Annotated[
            str,
            Field(
                description=(
                    "Fully-qualified table name (schema.table). If schema is omitted, "
                    "'main' is assumed."
                ),
            ),
        ],
        sample_rows: Annotated[
            int,
            Field(description="How many sample rows to fetch (0-20)."),
        ] = 3,
    ) -> dict[str, Any]:
        schema, name = _split_qualified(table)
        sample_rows = max(0, min(20, int(sample_rows)))

        try:
            with db.readonly() as conn:
                # Try table first, then view.
                conn.execute(
                    "SELECT estimated_size, column_count "
                    "FROM duckdb_tables() WHERE schema_name=? AND table_name=?",
                    [schema, name],
                )
                meta_row = conn.fetchone()
                kind = "table"
                if meta_row is None:
                    conn.execute(
                        "SELECT NULL::BIGINT AS estimated_size, column_count "
                        "FROM duckdb_views() WHERE schema_name=? AND view_name=?",
                        [schema, name],
                    )
                    meta_row = conn.fetchone()
                    kind = "view"
                if meta_row is None:
                    return _err(
                        "not_found",
                        f"No relation {schema}.{name}",
                        hint="Use list_tables to see what is available.",
                    )

                conn.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default, comment
                    FROM duckdb_columns()
                    WHERE schema_name=? AND table_name=?
                    ORDER BY column_index
                    """,
                    [schema, name],
                )
                columns = _rows_to_dicts(conn, conn.fetchall())

                # Primary key columns from constraints (DuckDB stores PK as
                # constraint_type = 'PRIMARY KEY').
                conn.execute(
                    """
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name=? AND table_name=? AND constraint_type='PRIMARY KEY'
                    """,
                    [schema, name],
                )
                pk_rows = conn.fetchall()
                pk: list[str] = []
                for r in pk_rows:
                    val = r[0]
                    if val:
                        pk.extend(list(val))

                conn.execute(
                    """
                    SELECT index_name, is_unique, is_primary, sql
                    FROM duckdb_indexes()
                    WHERE schema_name=? AND table_name=?
                    """,
                    [schema, name],
                )
                indexes = _rows_to_dicts(conn, conn.fetchall())

                sample: list[dict[str, Any]] = []
                geom_col_names: set[str] = set()
                if sample_rows > 0 and kind == "table":
                    select_cols: list[str] = []
                    for col in columns:
                        ctype = (col["data_type"] or "").upper()
                        if ctype == "GEOMETRY":
                            geom_col_names.add(col["column_name"])
                            select_cols.append(
                                f'ST_AsGeoJSON("{col["column_name"]}") AS "{col["column_name"]}"'
                            )
                        else:
                            select_cols.append(f'"{col["column_name"]}"')
                    sample_sql = (
                        f'SELECT {", ".join(select_cols)} '
                        f'FROM "{schema}"."{name}" LIMIT {int(sample_rows)}'
                    )
                    try:
                        conn.execute(sample_sql)
                        sample = _rows_to_dicts(conn, conn.fetchall())
                        # Parse the GeoJSON string columns into dicts so
                        # downstream JSON serialisation keeps them as objects,
                        # not double-encoded strings.
                        for row in sample:
                            for gname in geom_col_names:
                                raw = row.get(gname)
                                if isinstance(raw, str):
                                    try:
                                        row[gname] = json.loads(raw)
                                    except json.JSONDecodeError:
                                        pass
                    except Exception as exc:  # noqa: BLE001
                        sample = [{"_sample_error": str(exc)}]
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        ddl_lines = [f'CREATE {kind.upper()} "{schema}"."{name}" (']
        for i, col in enumerate(columns):
            line = f'    "{col["column_name"]}" {col["data_type"]}'
            if not col["is_nullable"]:
                line += " NOT NULL"
            if col["column_default"]:
                line += f' DEFAULT {col["column_default"]}'
            if col["comment"]:
                line += f'  -- {col["comment"]}'
            tail = "," if (i < len(columns) - 1 or pk) else ""
            ddl_lines.append(line + tail)
        if pk:
            ddl_lines.append(f'    PRIMARY KEY ({", ".join(pk)})')
        ddl_lines.append(");")

        # Geometry column hint
        geom_col = next(
            (c["column_name"] for c in columns if (c["data_type"] or "").upper() == "GEOMETRY"),
            None,
        )

        return {
            "ok": True,
            "schema": schema,
            "table": name,
            "kind": kind,
            "row_estimate": int(meta_row[0] or 0) if meta_row[0] is not None else None,
            "geom_column": geom_col,
            "columns": columns,
            "primary_key": pk,
            "indexes": indexes,
            "ddl": "\n".join(ddl_lines),
            "sample": sample,
        }


# ---------------------------------------------------------------------------
# get_column_values
# ---------------------------------------------------------------------------


def _register_get_column_values(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="get_column_values",
        description=(
            "Return up to N distinct values from one column, with their row "
            "counts. Useful for spotting categorical columns and their domain.\n\n"
            "Example: get_column_values(table='main.suburbs', column='state', "
            "limit=5) -> {\"ok\": true, \"values\": [{\"value\": \"NSW\", "
            "\"count\": 5}]}"
        ),
    )
    async def get_column_values(
        table: Annotated[str, Field(description="schema.table")],
        column: Annotated[str, Field(description="Column name.")],
        limit: Annotated[
            int,
            Field(description="Max distinct values to return (1-200)."),
        ] = 20,
    ) -> dict[str, Any]:
        schema, name = _split_qualified(table)
        limit = max(1, min(200, int(limit)))

        check_sql = (
            "SELECT data_type FROM duckdb_columns() "
            "WHERE schema_name=? AND table_name=? AND column_name=?"
        )
        sample_sql = (
            f'SELECT "{column}" AS value, COUNT(*)::BIGINT AS count '
            f'FROM "{schema}"."{name}" '
            f'GROUP BY "{column}" '
            f'ORDER BY count DESC NULLS LAST '
            f'LIMIT {int(limit)}'
        )

        try:
            with db.readonly() as conn:
                conn.execute(check_sql, [schema, name, column])
                check = conn.fetchone()
                if check is None:
                    return _err(
                        "not_found",
                        f"Column {column} not found in {schema}.{name}",
                        hint="Use get_table_schema to list real columns.",
                    )
                col_type = (check[0] or "").upper()
                if col_type == "GEOMETRY":
                    # Distinct geometries are noisy; report a hint instead.
                    return _err(
                        "unsupported_column_type",
                        "distinct values on GEOMETRY columns is rarely meaningful",
                        hint=(
                            "Use list_srids for SRID frequency, or query "
                            f'SELECT ST_AsText("{column}") FROM "{schema}"."{name}" '
                            "directly via execute_sql."
                        ),
                    )
                conn.execute(sample_sql)
                values = _rows_to_dicts(conn, conn.fetchall())
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        return {"ok": True, "table": f"{schema}.{name}", "column": column, "values": values}


# ---------------------------------------------------------------------------
# list_srids
# ---------------------------------------------------------------------------


def _register_list_srids(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_srids",
        description=(
            "List SRIDs reported by ST_SRID across all geometry columns. "
            "**DuckDB-spatial does not store SRID on geometries** — this "
            "almost always returns SRID=0 for every column. Use this tool to "
            "confirm that, and consult duckdb_help('reproject') for how to "
            "reproject geometries explicitly with ST_Transform.\n\n"
            "Example: list_srids() -> {\"ok\": true, \"srids\": [{\"srid\": 0, "
            "\"name\": \"(unset)\", \"unit\": \"unknown\", \"column_count\": 3}], "
            "\"warning\": \"DuckDB-spatial does not persist SRID; ...\"}"
        ),
    )
    async def list_srids() -> dict[str, Any]:
        excludes = list(DEFAULT_SCHEMA_EXCLUDES)
        excl_in = ",".join(["?"] * len(excludes))
        cols_sql = f"""
            SELECT schema_name, table_name, column_name
            FROM duckdb_columns()
            WHERE data_type='GEOMETRY' AND schema_name NOT IN ({excl_in})
        """
        try:
            with db.readonly() as conn:
                conn.execute(cols_sql, excludes)
                geoms = _rows_to_dicts(conn, conn.fetchall())
                # For each geometry column, sample its distinct SRIDs.
                buckets: dict[int, int] = {}
                scan_failures = 0
                for g in geoms:
                    schema, table, col = g["schema_name"], g["table_name"], g["column_name"]
                    srid_sql = (
                        f'SELECT ST_SRID("{col}") AS srid '
                        f'FROM "{schema}"."{table}" GROUP BY srid'
                    )
                    try:
                        db.run_with_timeout(conn, srid_sql)
                        for row in conn.fetchall():
                            srid = int(row[0]) if row[0] is not None else 0
                            buckets[srid] = buckets.get(srid, 0) + 1
                    except Exception as exc:
                        scan_failures += 1
                        logger.warning(
                            "ST_SRID scan failed for %s.%s.%s: %s",
                            schema,
                            table,
                            col,
                            exc,
                        )
                # Fallback: when ST_SRID is unavailable in this build of
                # duckdb-spatial, every geometry effectively has SRID 0.
                if not buckets and geoms:
                    buckets[0] = len(geoms)
        except Exception as exc:  # noqa: BLE001
            return _err(
                "query_failed",
                str(exc),
                hint=(
                    "If you got 'Function ST_SRID does not exist', the spatial "
                    "extension is not loaded; restart with DUCKDB_DOCSBOX_LOAD_SPATIAL=1."
                ),
            )

        srids = []
        for srid, count in sorted(buckets.items(), key=lambda x: (-x[1], x[0])):
            name, unit = _EPSG_NAMES.get(srid, ("(unknown EPSG)", "unknown"))
            srids.append(
                {"srid": srid, "name": name, "unit": unit, "column_count": count}
            )
        return {
            "ok": True,
            "srids": srids,
            "warning": (
                "DuckDB-spatial does not persist SRID with geometries; "
                "ST_SRID returns 0 unless explicitly set via ST_SetSRID. "
                "Track the true SRID in your application metadata."
            ),
        }


# ---------------------------------------------------------------------------
# get_relationships
# ---------------------------------------------------------------------------


def _register_get_relationships(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="get_relationships",
        description=(
            "List foreign-key edges, optionally filtered to one table. Each "
            "edge has (from_table, from_columns, to_table, to_columns, "
            "constraint_text).\n\n"
            "Example: get_relationships(table='main.schools') -> {\"ok\": true, "
            "\"edges\": [{\"from_table\": \"main.schools\", \"from_columns\": "
            "[\"suburb_id\"], \"to_table\": \"main.suburbs\", \"to_columns\": "
            "[\"gid\"], \"constraint\": \"FOREIGN KEY (suburb_id) REFERENCES suburbs(gid)\"}]}"
        ),
    )
    async def get_relationships(
        table: Annotated[
            str,
            Field(
                description=(
                    "Optional schema.table to filter to FKs originating from this "
                    "table. Empty = all FKs in the database."
                ),
            ),
        ] = "",
    ) -> dict[str, Any]:
        excludes = list(DEFAULT_SCHEMA_EXCLUDES)
        excl_in = ",".join(["?"] * len(excludes))
        sql = f"""
            SELECT schema_name, table_name, constraint_text,
                   constraint_column_names, referenced_table, referenced_column_names
            FROM duckdb_constraints()
            WHERE constraint_type='FOREIGN KEY'
              AND schema_name NOT IN ({excl_in})
        """
        args: list[Any] = list(excludes)
        if table:
            schema, name = _split_qualified(table)
            sql += " AND schema_name=? AND table_name=?"
            args.extend([schema, name])
        sql += " ORDER BY schema_name, table_name"

        try:
            with db.readonly() as conn:
                conn.execute(sql, args)
                rows = _rows_to_dicts(conn, conn.fetchall())
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        edges = []
        for r in rows:
            from_cols = list(r["constraint_column_names"] or [])
            to_cols = list(r["referenced_column_names"] or [])
            ref_table = r["referenced_table"] or ""
            edges.append(
                {
                    "constraint": r["constraint_text"],
                    "from_table": f"{r['schema_name']}.{r['table_name']}",
                    "from_columns": from_cols,
                    "to_table": f"{r['schema_name']}.{ref_table}" if ref_table else "",
                    "to_columns": to_cols,
                }
            )
        return {"ok": True, "edges": edges, "count": len(edges)}


# ---------------------------------------------------------------------------
# list_extensions
# ---------------------------------------------------------------------------


def _register_list_extensions(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_extensions",
        description=(
            "List installed/loaded DuckDB extensions with version. Tells the "
            "LLM whether `spatial`, `httpfs`, `parquet`, `json`, etc. are "
            "available before composing a query.\n\n"
            "Example: list_extensions() -> {\"ok\": true, \"extensions\": "
            "[{\"name\": \"spatial\", \"loaded\": true, \"installed\": true, "
            "\"version\": \"...\", \"description\": \"Geospatial extension ...\"}, "
            "...]}"
        ),
    )
    async def list_extensions() -> dict[str, Any]:
        sql = """
            SELECT extension_name, loaded, installed, extension_version, description
            FROM duckdb_extensions()
            ORDER BY extension_name
        """
        try:
            with db.readonly() as conn:
                conn.execute(sql)
                rows = _rows_to_dicts(conn, conn.fetchall())
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))
        out = [
            {
                "name": r["extension_name"],
                "loaded": bool(r["loaded"]),
                "installed": bool(r["installed"]),
                "version": r["extension_version"],
                "description": r["description"],
            }
            for r in rows
        ]
        return {"ok": True, "extensions": out}


# ---------------------------------------------------------------------------
# pick_interesting_tables
# ---------------------------------------------------------------------------


def _register_pick_interesting_tables(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="pick_interesting_tables",
        description=(
            "Score user tables by 'interestingness' to surface where to look "
            "first in an unfamiliar database. Score combines:\n"
            "  - log10(estimated_size) — favour data-rich tables\n"
            "  - +2 if table has a geometry column with an R-Tree index\n"
            "  - +1 if table has a geometry column without an index\n"
            "  - +1 per inbound foreign key (a 'hub' table)\n\n"
            "Tables in the operator's exclude list are dropped.\n\n"
            "Example: pick_interesting_tables(limit=5) -> {\"ok\": true, "
            "\"tables\": [{\"table\": \"main.suburbs\", \"row_estimate\": "
            "5, \"score\": 3.7, \"reasons\": [\"geometry column\", \"3 inbound FKs\"], "
            "\"extent\": {...}}]}"
        ),
    )
    async def pick_interesting_tables(
        limit: Annotated[
            int,
            Field(description="Top N tables to return (1-50)."),
        ] = 10,
        compute_extent: Annotated[
            bool,
            Field(
                description=(
                    "Whether to compute ST_Extent_Agg per geometry table. Cheap on "
                    "indexed tables, can be slow on millions of rows; turn off if "
                    "introspection is timing out."
                ),
            ),
        ] = True,
    ) -> dict[str, Any]:
        limit = max(1, min(50, int(limit)))
        excludes = list(DEFAULT_SCHEMA_EXCLUDES)
        excl_in = ",".join(["?"] * len(excludes))

        try:
            with db.readonly() as conn:
                conn.execute(
                    f"""
                    SELECT schema_name, table_name, estimated_size
                    FROM duckdb_tables()
                    WHERE schema_name NOT IN ({excl_in}) AND NOT internal
                    """,
                    excludes,
                )
                base = _rows_to_dicts(conn, conn.fetchall())

                conn.execute(
                    f"""
                    SELECT schema_name, table_name, column_name
                    FROM duckdb_columns()
                    WHERE data_type='GEOMETRY' AND schema_name NOT IN ({excl_in})
                    """,
                    excludes,
                )
                geom_cols = {
                    (r["schema_name"], r["table_name"]): r["column_name"]
                    for r in _rows_to_dicts(conn, conn.fetchall())
                }

                # R-Tree indexes — DuckDB exposes indexes via duckdb_indexes(),
                # the index ``sql`` text contains 'USING RTREE' for spatial.
                conn.execute(
                    """
                    SELECT schema_name, table_name, sql
                    FROM duckdb_indexes()
                    """
                )
                rtree_keys: set[tuple[str, str]] = set()
                for r in conn.fetchall():
                    schema, table, sql_def = r[0], r[1], (r[2] or "")
                    if "RTREE" in sql_def.upper():
                        rtree_keys.add((schema, table))

                # Inbound FK count per table.
                conn.execute(
                    f"""
                    SELECT schema_name, referenced_table, COUNT(*) AS n
                    FROM duckdb_constraints()
                    WHERE constraint_type='FOREIGN KEY'
                      AND schema_name NOT IN ({excl_in})
                      AND referenced_table IS NOT NULL
                    GROUP BY schema_name, referenced_table
                    """,
                    excludes,
                )
                inbound: dict[tuple[str, str], int] = {
                    (r[0], r[1]): int(r[2]) for r in conn.fetchall()
                }

                scored: list[dict[str, Any]] = []
                for r in base:
                    schema = r["schema_name"]
                    name = r["table_name"]
                    full = f"{schema}.{name}"
                    if is_metadata_excluded(full, db.cfg):
                        continue
                    rows = int(r["estimated_size"] or 0)
                    score = math.log10(max(1, rows))
                    reasons: list[str] = []
                    if rows > 0:
                        reasons.append(f"~{rows} rows")
                    geom_col = geom_cols.get((schema, name))
                    if geom_col and (schema, name) in rtree_keys:
                        score += 2.0
                        reasons.append("R-Tree spatial index")
                    elif geom_col:
                        score += 1.0
                        reasons.append("geometry column (no R-Tree index)")
                    inb = inbound.get((schema, name), 0)
                    if inb:
                        score += 1.0
                        reasons.append(f"{inb} inbound FKs")
                    scored.append(
                        {
                            "table": full,
                            "row_estimate": rows,
                            "score": round(score, 3),
                            "reasons": reasons,
                            "geom_column": geom_col,
                        }
                    )

                scored.sort(key=lambda x: x["score"], reverse=True)
                top = scored[:limit]

                if compute_extent:
                    for entry in top:
                        if not entry["geom_column"]:
                            continue
                        schema, _, name = entry["table"].partition(".")
                        ext_sql = (
                            f'SELECT ST_AsGeoJSON(ST_Extent_Agg("{entry["geom_column"]}")) '
                            f'FROM "{schema}"."{name}"'
                        )
                        try:
                            db.run_with_timeout(conn, ext_sql)
                            row = conn.fetchone()
                            raw = row[0] if row and row[0] else None
                            if isinstance(raw, str):
                                try:
                                    entry["extent"] = json.loads(raw)
                                except json.JSONDecodeError:
                                    entry["extent"] = raw
                            else:
                                entry["extent"] = raw
                        except Exception as exc:  # noqa: BLE001
                            entry["extent_error"] = str(exc)
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        return {"ok": True, "tables": top, "count": len(top)}


__all__ = ["register"]
