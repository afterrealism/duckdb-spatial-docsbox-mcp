"""Local-execution planner for DuckDB.

The MCP server itself never invokes the duckdb CLI or spawns ogr2ogr — that
would turn it into a remote shell. Instead this tool returns a structured
*plan* (a list of shell steps) that the calling agent dispatches through
its own bash tool, on the user's machine. The agent's host is the trust
boundary.

Templates supported (case-insensitive substring match on ``task``):

- ``connect``         -> open a duckdb CLI session
- ``query``           -> ad-hoc SQL via ``duckdb -c`` (base64-staged)
- ``script``          -> run a SQL file via ``duckdb -f``
- ``dump``            -> EXPORT DATABASE 'dir/'
- ``import_csv``      -> CREATE TABLE ... AS SELECT * FROM read_csv_auto(...)
- ``import_parquet``  -> CREATE TABLE ... AS SELECT * FROM read_parquet(...)
- ``import_geojson``  -> CREATE TABLE ... AS SELECT * FROM ST_Read(...)
- ``import_shapefile`` -> CREATE TABLE ... AS SELECT * FROM ST_Read('x.shp')
- ``install_spatial`` -> INSTALL spatial; LOAD spatial;
- ``export_geojson``  -> COPY (SELECT ...) TO 'out.geojson' WITH (FORMAT GDAL, ...)

Quoting strategy: every template that interpolates a user-supplied path or
SQL stages the full SQL into a base64-encoded temp file (``mktemp``) and
runs ``duckdb -f "$TMPSQL"``. We never embed user input inside a
shell-double-quoted ``-c "..."`` argument — that would compose two
quoting languages (SQL + shell) and break on paths containing ``"``,
``$``, or backticks.
"""

from __future__ import annotations

import base64
import re
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="run_locally",
        description=(
            "Return a deterministic execution plan (list of shell steps) "
            "the calling agent can run on the user's host with its own "
            "bash tool. Templates: connect, query, script, dump, "
            "import_csv, import_parquet, import_geojson, import_shapefile, "
            "install_spatial, export_geojson. The MCP server does NOT "
            "execute these commands itself."
        ),
    )
    async def run_locally(
        task: Annotated[
            str,
            Field(description="Free-form description; matched against template keywords."),
        ],
        db_env: Annotated[
            str,
            Field(
                description=(
                    "Env var the agent should pass as the DuckDB database "
                    "path. Defaults to DUCKDB_PATH; the value must point at "
                    "a .duckdb file (or ':memory:' for ephemeral)."
                ),
            ),
        ] = "DUCKDB_PATH",
        sql: Annotated[
            str | None,
            Field(description="SQL string, used by query / export_geojson templates."),
        ] = None,
        path: Annotated[
            str | None,
            Field(
                description=(
                    "File path argument used by script/dump/import_* and "
                    "export_geojson templates."
                ),
            ),
        ] = None,
        table: Annotated[
            str,
            Field(description="Target table name for import_* templates."),
        ] = "imported",
        timeout_s: Annotated[
            int,
            Field(description="Suggested timeout the agent should pass to its bash tool."),
        ] = 60,
    ) -> dict[str, Any]:
        t = task.lower()
        steps: list[dict[str, Any]] = []
        notes: list[str] = []

        if any(k in t for k in ("connect", "shell", "interactive")):
            steps.append(
                {
                    "name": "connect",
                    "shell": f'duckdb "${{{db_env}}}"',
                    "purpose": "Open an interactive duckdb CLI session.",
                    "interactive": True,
                }
            )
        elif "install_spatial" in t or ("install" in t and "spatial" in t):
            steps.append(
                {
                    "name": "install_spatial",
                    "shell": (
                        f'duckdb "${{{db_env}}}" -c '
                        '"INSTALL spatial; LOAD spatial; SELECT '
                        "extension_name, installed, loaded FROM duckdb_extensions() "
                        "WHERE extension_name='spatial';\""
                    ),
                    "purpose": "One-shot install + load of the spatial extension.",
                }
            )
        elif "dump" in t or "export_database" in t:
            target = path or "duckdb-dump"
            steps.extend(
                _b64_sql_steps(
                    "export_database",
                    f"EXPORT DATABASE '{_q_lit(target)}' (FORMAT PARQUET);",
                    db_env,
                    timeout_s,
                    purpose="Export every schema/table to Parquet under the given dir.",
                )
            )
        elif "export_geojson" in t or ("export" in t and "geojson" in t):
            if not sql or not path:
                return {
                    "ok": False,
                    "error": "export_geojson requires both `sql` (SELECT ...) and `path` (out.geojson)",
                }
            wrapped = (
                f"INSTALL spatial; LOAD spatial; "
                f"COPY ({sql.rstrip(';').strip()}) TO '{_q_lit(path)}' "
                f"WITH (FORMAT GDAL, DRIVER 'GeoJSON');"
            )
            steps.extend(_b64_sql_steps("export_geojson", wrapped, db_env, timeout_s))
        elif "query" in t or "select" in t or "sql" in t:
            if not sql:
                return {"ok": False, "error": "query template requires `sql`"}
            steps.extend(_b64_sql_steps("run_sql", sql, db_env, timeout_s))
        elif "script" in t:
            if not path:
                return {"ok": False, "error": "script template requires `path` to a .sql file"}
            steps.append(
                {
                    "name": "run_script",
                    "shell": f'duckdb "${{{db_env}}}" -f {_q(path)}',
                    "timeout_s": int(timeout_s),
                }
            )
        elif "import_csv" in t or t.endswith(".csv"):
            if not path:
                return {"ok": False, "error": "import_csv requires `path` to a .csv"}
            tbl = _q_ident(table)
            steps.extend(
                _b64_sql_steps(
                    "import_csv",
                    f"CREATE OR REPLACE TABLE {tbl} AS "
                    f"SELECT * FROM read_csv_auto('{_q_lit(path)}');",
                    db_env,
                    timeout_s,
                    purpose=f"Auto-detect schema of {path} and load into table {table}.",
                )
            )
        elif "import_parquet" in t or t.endswith(".parquet"):
            if not path:
                return {"ok": False, "error": "import_parquet requires `path` to a .parquet"}
            tbl = _q_ident(table)
            steps.extend(
                _b64_sql_steps(
                    "import_parquet",
                    f"CREATE OR REPLACE TABLE {tbl} AS "
                    f"SELECT * FROM read_parquet('{_q_lit(path)}');",
                    db_env,
                    timeout_s,
                    purpose=(
                        f"Load Parquet at {path} into table {table}. "
                        "GeoParquet is auto-detected when the spatial extension is loaded."
                    ),
                )
            )
        elif "import_geojson" in t or t.endswith(".geojson"):
            if not path:
                return {"ok": False, "error": "import_geojson requires `path` to a .geojson"}
            tbl = _q_ident(table)
            steps.extend(
                _b64_sql_steps(
                    "import_geojson",
                    f"INSTALL spatial; LOAD spatial; "
                    f"CREATE OR REPLACE TABLE {tbl} AS "
                    f"SELECT * FROM ST_Read('{_q_lit(path)}');",
                    db_env,
                    timeout_s,
                    purpose=f"Read GeoJSON via ST_Read into table {table}.",
                )
            )
        elif "import_shapefile" in t or t.endswith(".shp"):
            if not path:
                return {"ok": False, "error": "import_shapefile requires `path` to a .shp"}
            tbl = _q_ident(table)
            steps.extend(
                _b64_sql_steps(
                    "import_shapefile",
                    f"INSTALL spatial; LOAD spatial; "
                    f"CREATE OR REPLACE TABLE {tbl} AS "
                    f"SELECT * FROM ST_Read('{_q_lit(path)}');",
                    db_env,
                    timeout_s,
                    purpose=f"Read shapefile via ST_Read into table {table}.",
                )
            )
            notes.append("Shapefile sidecars (.dbf/.shx/.prj) must sit beside the .shp.")
        else:
            return {
                "ok": False,
                "error": "no template matched",
                "available_templates": [
                    "connect",
                    "query",
                    "script",
                    "dump",
                    "import_csv",
                    "import_parquet",
                    "import_geojson",
                    "import_shapefile",
                    "install_spatial",
                    "export_geojson",
                ],
            }

        return {
            "ok": True,
            "plan": {
                "task": task,
                "db_env": db_env,
                "timeout_s": int(timeout_s),
                "steps": steps,
                "notes": notes
                + [
                    "Dispatch each step through your own bash tool, in order.",
                    f"Make sure ${db_env} is exported and points at a .duckdb file or ':memory:'.",
                    "Review SQL before running — DuckDB will not roll back DDL.",
                ],
            },
        }


def _q(s: str) -> str:
    """Shell-quote helper."""
    if not s:
        return "''"
    if all(c.isalnum() or c in "-_./=+,@:" for c in s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"


def _q_lit(s: str) -> str:
    """Escape single quotes for embedding inside a SQL string literal."""
    return s.replace("'", "''")


def _q_ident(s: str) -> str:
    """Quote a SQL identifier. Bare-safe identifiers pass through; anything
    else is wrapped in double quotes with embedded ``"`` escaped."""
    if _IDENT_RE.match(s) and "." not in s:
        return s
    return '"' + s.replace('"', '""') + '"'


def _b64_sql_steps(
    name: str,
    sql: str,
    db_env: str,
    timeout_s: int,
    *,
    purpose: str | None = None,
) -> list[dict[str, Any]]:
    """Return the ``write_sql`` -> ``run`` -> ``cleanup`` triple that runs
    arbitrary SQL via a temp file, avoiding any shell-quoting of paths or
    user-supplied SQL fragments."""
    encoded = base64.b64encode(sql.encode("utf-8")).decode("ascii")
    steps: list[dict[str, Any]] = [
        {
            "name": "write_sql",
            "shell": (
                'TMPSQL="$(mktemp -t duckdbdocsbox-XXXXXX.sql)" && '
                f'echo {encoded} | base64 -d > "$TMPSQL" && echo "$TMPSQL"'
            ),
            "captures": "TMPSQL",
        },
        {
            "name": name,
            "shell": f'duckdb "${{{db_env}}}" -f "$TMPSQL"',
            "timeout_s": int(timeout_s),
        },
        {"name": "cleanup", "shell": 'rm -f "$TMPSQL"', "best_effort": True},
    ]
    if purpose:
        steps[1]["purpose"] = purpose
    return steps
