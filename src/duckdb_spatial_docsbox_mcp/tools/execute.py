"""SQL execution tools for DuckDB.

These are the only tools that take *user-authored SQL*; every other tool
composes its own SQL internally. Hence the heaviest defence here:

* ``validate_sql``  — static check (sqlglot + denylist + structural rules);
                      cheap, never touches the database.
* ``explain_sql``   — static check then ``EXPLAIN`` (no actual execution
                      unless ``analyze=True``).
* ``execute_sql``   — static + run inside ``Database.readonly`` with a
                      threading-Timer watchdog that calls ``con.interrupt()``
                      on expiry. File-mode connections are open with
                      ``read_only=True`` so DDL/DML at the storage layer is
                      blocked even before we look at it.

Only top-level ``SELECT`` / ``WITH`` / ``EXPLAIN`` is allowed. Multi-statement
inputs are rejected. ``LIMIT`` is auto-injected at 500 if not present.

Geometry columns are returned by their textual ``str()`` representation
unless the user wraps them in ``ST_AsGeoJSON`` / ``ST_AsText`` themselves.
The ``geometry_format`` parameter is a documentation hint for that pattern.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import duckdb
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..db import Database, TimeoutError as DuckTimeoutError
from ..sql_validator import static_validate

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
MAX_CELL_BYTES = 64 * 1024


def _err(kind: str, message: str, hint: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "error": kind, "message": message}
    if hint is not None:
        out["hint"] = hint
    return out


def _format_value(val: Any) -> Any:
    """Truncate giant byte/string cells so a single row can't blow the budget."""
    if isinstance(val, (bytes, bytearray, memoryview)):
        b = bytes(val)
        if len(b) > MAX_CELL_BYTES:
            return f"<{len(b)} bytes, truncated>"
        return b.hex()
    if isinstance(val, str) and len(val) > MAX_CELL_BYTES:
        return val[:MAX_CELL_BYTES] + "... [truncated]"
    return val


def register(mcp: FastMCP, db: Database | None) -> None:
    if db is None:
        _register_validate_only(mcp)
        return
    _register_validate(mcp)
    _register_explain(mcp, db)
    _register_execute(mcp, db)


# ---------------------------------------------------------------------------
# validate_sql (always available, no DB needed)
# ---------------------------------------------------------------------------


def _register_validate_only(mcp: FastMCP) -> None:
    _register_validate(mcp)
    msg = (
        "DUCKDB_DOCSBOX_PATH not set; only validate_sql is available. "
        "Set DUCKDB_DOCSBOX_PATH to enable explain_sql and execute_sql."
    )

    async def _stub() -> dict[str, Any]:
        return _err("not_configured", msg)

    for name in ("explain_sql", "execute_sql"):
        mcp.tool(name=name, description=msg)(_stub)


def _register_validate(mcp: FastMCP) -> None:
    @mcp.tool(
        name="validate_sql",
        description=(
            "Statically validate a SQL string without executing it. Returns "
            "the (possibly LIMIT-augmented) SQL plus an ok/error verdict. "
            "Use this before execute_sql for cheap fast-fail.\n\n"
            "Example: validate_sql(sql='SELECT 1') -> {\"ok\": true, "
            "\"sql\": \"SELECT 1 LIMIT 500\", \"auto_limit_applied\": true}\n"
            "Example: validate_sql(sql='ATTACH \\'evil.db\\'') -> {\"ok\": false, "
            "\"error\": \"disallowed keyword: 'attach'\"}"
        ),
    )
    async def validate_sql(
        sql: Annotated[str, Field(description="SQL string to validate.")],
        default_limit: Annotated[
            int,
            Field(description="LIMIT to inject when none is present (1-1000)."),
        ] = 500,
    ) -> dict[str, Any]:
        default_limit = max(1, min(MAX_ROWS, int(default_limit)))
        result = static_validate(sql, default_limit=default_limit)
        out: dict[str, Any] = {
            "ok": result.ok,
            "sql": result.sql,
            "auto_limit_applied": result.auto_limit_applied,
        }
        if not result.ok:
            out["error"] = result.error or "invalid"
        if result.hint:
            out["hint"] = result.hint
        return out


# ---------------------------------------------------------------------------
# explain_sql
# ---------------------------------------------------------------------------


def _register_explain(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="explain_sql",
        description=(
            "Run EXPLAIN against a SELECT/WITH query without executing it "
            "(unless `analyze=True`, in which case EXPLAIN ANALYZE actually "
            "runs the query in a read-only context and reports timings). "
            "Returns the plan as text.\n\n"
            "Example: explain_sql(sql='SELECT * FROM suburbs WHERE name=\\'Bondi\\'')"
            " -> {\"ok\": true, \"plan\": \"...\", \"sql\": \"...\"}"
        ),
    )
    async def explain_sql(
        sql: Annotated[str, Field(description="SQL to explain (read-only only).")],
        analyze: Annotated[
            bool,
            Field(
                description=(
                    "If true, EXPLAIN ANALYZE — actually executes the query. "
                    "Useful but slower; off by default."
                ),
            ),
        ] = False,
    ) -> dict[str, Any]:
        result = static_validate(sql, default_limit=MAX_ROWS)
        if not result.ok:
            return {
                "ok": False,
                "error": result.error or "invalid",
                "hint": result.hint,
                "sql": result.sql,
            }
        wrapped = (
            f"EXPLAIN ANALYZE {result.sql}" if analyze else f"EXPLAIN {result.sql}"
        )
        try:
            with db.readonly() as conn:
                db.run_with_timeout(conn, wrapped)
                rows = conn.fetchall()
        except DuckTimeoutError as exc:
            return _err(
                "timeout",
                str(exc),
                hint="Tighten the query or raise DUCKDB_DOCSBOX_STATEMENT_TIMEOUT_S.",
            )
        except duckdb.Error as exc:
            return _err("explain_failed", str(exc))
        # DuckDB EXPLAIN returns 2 columns: explain_key, explain_value.
        plan_text = "\n".join(
            f"{row[0]}: {row[1]}" if len(row) >= 2 else str(row) for row in rows
        )
        return {"ok": True, "sql": result.sql, "analyzed": analyze, "plan": plan_text}


# ---------------------------------------------------------------------------
# execute_sql
# ---------------------------------------------------------------------------


def _register_execute(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="execute_sql",
        description=(
            "Execute a SELECT/WITH/EXPLAIN query against the configured "
            "DuckDB. The connection is opened read-only at the storage layer "
            "(file mode) and a watchdog interrupts queries that exceed "
            "DUCKDB_DOCSBOX_STATEMENT_TIMEOUT_S. To make geometry columns "
            "human-readable, wrap them in your SELECT (e.g. `ST_AsGeoJSON(geom)`).\n\n"
            "Example: execute_sql(sql='SELECT name, ST_AsText(geom) "
            "FROM suburbs LIMIT 2') -> {\"ok\": true, \"columns\": "
            "[\"name\", \"st_astext(geom)\"], \"rows\": [...], \"row_count\": 2}"
        ),
    )
    async def execute_sql(
        sql: Annotated[
            str, Field(description="SQL to execute (SELECT / WITH / EXPLAIN only).")
        ],
        max_rows: Annotated[
            int,
            Field(description="Cap on rows materialised in the response (1-1000)."),
        ] = 200,
        geometry_format: Annotated[
            str,
            Field(
                description=(
                    "Documentation hint only. Recognised values: 'raw' (return "
                    "DuckDB's textual geometry, the default), 'geojson' "
                    "(reminder to wrap with ST_AsGeoJSON yourself), 'wkt' "
                    "(reminder to wrap with ST_AsText). The server does NOT "
                    "rewrite your SELECT — wrap geometry columns explicitly."
                ),
            ),
        ] = "raw",
    ) -> dict[str, Any]:
        max_rows = max(1, min(MAX_ROWS, int(max_rows)))
        if geometry_format not in {"raw", "geojson", "wkt"}:
            return _err(
                "invalid_arg",
                f"geometry_format={geometry_format!r} not in {{raw, geojson, wkt}}",
            )

        result = static_validate(sql, default_limit=max_rows)
        if not result.ok:
            return {
                "ok": False,
                "error": result.error or "invalid",
                "hint": result.hint,
                "sql": result.sql,
            }

        try:
            with db.readonly() as conn:
                db.run_with_timeout(conn, result.sql)
                if conn.description is None:
                    return _err(
                        "no_rowset",
                        "Query did not return a rowset.",
                        hint="Only SELECT / WITH / EXPLAIN return rows here.",
                    )
                columns = [d[0] for d in conn.description]
                raw_rows = conn.fetchmany(max_rows)
                # DuckDB does not expose total rowcount before fetch; probe
                # for an extra row to detect truncation.
                extra = conn.fetchone()
                truncated = extra is not None
        except DuckTimeoutError as exc:
            return _err(
                "timeout",
                str(exc),
                hint=(
                    "Statement timeout. Add WHERE/LIMIT, ensure an R-Tree "
                    "index exists (CREATE INDEX ... USING RTREE(geom)), or "
                    "raise DUCKDB_DOCSBOX_STATEMENT_TIMEOUT_S."
                ),
            )
        except duckdb.CatalogException as exc:
            return _err(
                "undefined_object",
                str(exc),
                hint=(
                    "Use list_tables / get_table_schema to verify table and "
                    "column names. ST_* functions require LOAD spatial."
                ),
            )
        except duckdb.BinderException as exc:
            return _err(
                "binder_error",
                str(exc),
                hint="Often a column-not-found or type-mismatch error.",
            )
        except duckdb.IOException as exc:
            return _err(
                "io_error",
                str(exc),
                hint=(
                    "If reading external files, the path must be readable by "
                    "the server process; remote URLs need INSTALL httpfs which "
                    "is denied here — use run_locally for that."
                ),
            )
        except duckdb.Error as exc:
            return _err("execute_failed", str(exc))

        out_rows: list[dict[str, Any]] = []
        for row in raw_rows:
            out_rows.append(
                {col: _format_value(val) for col, val in zip(columns, row, strict=False)}
            )

        return {
            "ok": True,
            "sql": result.sql,
            "auto_limit_applied": result.auto_limit_applied,
            "columns": columns,
            "rows": out_rows,
            "row_count": len(out_rows),
            "truncated": truncated,
        }


__all__ = ["register"]
