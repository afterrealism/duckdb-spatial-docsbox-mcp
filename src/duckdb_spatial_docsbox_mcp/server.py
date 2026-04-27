"""FastMCP streamable-HTTP server for duckdb-spatial-docsbox-mcp.

This server gives an LLM agent safe, bounded tools for exploring and
querying a DuckDB database (with the ``spatial`` extension), plus a
curated DuckDB+spatial reference and a generic docs/manifest lookup.

Safety model
------------

* **No mutation paths** are exposed. ``execute_sql`` runs against a
  ``read_only=True`` DuckDB connection (when a file path is configured)
  with a per-statement wall-clock timeout enforced via
  ``threading.Timer`` + ``connection.interrupt()``.
* **SQL is statically validated** (sqlglot + denylist) before it is sent
  to the database; multi-statement payloads, DDL, DML, and dangerous
  functions (``read_text``, ``read_blob``, ``shell``, ``system`` ...)
  are refused. Top-level keywords ``ATTACH``/``DETACH``/``INSTALL``/
  ``LOAD``/``EXPORT``/``IMPORT``/``PRAGMA``/``CHECKPOINT``/``USE`` are
  refused too.
* **Result sizes are capped** (rows, cell bytes, payload bytes).
* **No subprocess fan-out**: the only subprocess is the optional
  ``run_locally`` tool, which is *plan-only* — it never executes.

Configuration (environment variables)
-------------------------------------

* ``DUCKDB_DOCSBOX_PATH``                 — DuckDB file path (required for DB tools).
                                            Use ``:memory:`` for an ephemeral session.
* ``DUCKDB_DOCSBOX_STATEMENT_TIMEOUT_S``  (default 10.0)
* ``DUCKDB_DOCSBOX_LOAD_SPATIAL``         (default 1; set to 0 to skip)
* ``DUCKDB_DOCSBOX_METADATA_EXCLUDES``    — comma-separated ``schema.table``
                                            names to drop from listings.
* ``DUCKDB_DOCSBOX_BIND``                 (default 127.0.0.1:7821)
* ``DUCKDB_DOCSBOX_CORPUS_DIR``           — override packaged corpus.
* ``DUCKDB_DOCSBOX_DISABLE_DNS_PROTECTION`` — set to 1 in tests only.
* ``DUCKDB_DOCSBOX_ALLOWED_HOSTS`` / ``DUCKDB_DOCSBOX_ALLOWED_ORIGINS``
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse

from .corpus import Corpus, load_corpus
from .db import Database, config_from_env
from .tools import (
    docs as docs_tool,
)
from .tools import (
    duckdb_help as duckdb_help_tool,
)
from .tools import (
    execute as execute_tool,
)
from .tools import (
    introspect as introspect_tool,
)
from .tools import (
    run_locally as run_locally_tool,
)
from .tools import (
    spatial as spatial_tool,
)
from .web import landing_page, llms_full_txt, llms_txt, robots_txt, sitemap_xml

logger = logging.getLogger("duckdb-spatial-docsbox-mcp")


def _default_security(host: str, port: int) -> TransportSecuritySettings:
    if os.environ.get("DUCKDB_DOCSBOX_DISABLE_DNS_PROTECTION") == "1":
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)

    extra_hosts = [
        h.strip()
        for h in os.environ.get("DUCKDB_DOCSBOX_ALLOWED_HOSTS", "").split(",")
        if h.strip()
    ]
    extra_origins = [
        o.strip()
        for o in os.environ.get("DUCKDB_DOCSBOX_ALLOWED_ORIGINS", "").split(",")
        if o.strip()
    ]
    base_hosts = [
        f"127.0.0.1:{port}",
        f"localhost:{port}",
        "127.0.0.1:*",
        "localhost:*",
        "duckdb-mcp.afterrealism.com",
        "duckdb-mcp.afterrealism.com:*",
    ]
    base_origins = [
        f"http://127.0.0.1:{port}",
        f"http://localhost:{port}",
        "https://duckdb-mcp.afterrealism.com",
    ]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(dict.fromkeys(base_hosts + extra_hosts)),
        allowed_origins=list(dict.fromkeys(base_origins + extra_origins)),
    )


def _build_mcp(
    corpus: Corpus,
    http: httpx.AsyncClient,
    db: Database | None,
    *,
    host: str,
    port: int,
) -> FastMCP:
    mcp = FastMCP(
        name="duckdb-spatial-docsbox",
        instructions=(
            "DuckDB + spatial extension exploration tools. Database access is "
            "read-only (storage-layer read_only=True for file databases; "
            "validator-enforced for :memory:) with a per-statement wall-clock "
            "timeout. Tools: list_tables, get_table_schema, get_column_values, "
            "list_srids, get_relationships, list_extensions, "
            "pick_interesting_tables, validate_sql, explain_sql, execute_sql, "
            "list_gdal_drivers, probe_external_file, duckdb_help, "
            "list_sections, get_documentation, run_locally (plan-only)."
        ),
        host=host,
        port=port,
        json_response=True,
        stateless_http=True,
        transport_security=_default_security(host, port),
    )

    docs_tool.register(mcp, corpus, http)
    duckdb_help_tool.register(mcp)
    introspect_tool.register(mcp, db)
    execute_tool.register(mcp, db)
    spatial_tool.register(mcp, db)
    run_locally_tool.register(mcp)

    @mcp.custom_route("/", methods=["GET"])
    async def _index(_: Request) -> HTMLResponse:
        return HTMLResponse(landing_page())

    @mcp.custom_route("/health", methods=["GET"])
    async def _health(_: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "service": "duckdb-spatial-docsbox-mcp",
                "db_configured": db is not None,
            }
        )

    @mcp.custom_route("/robots.txt", methods=["GET"])
    async def _robots(_: Request) -> PlainTextResponse:
        return PlainTextResponse(robots_txt(), media_type="text/plain; charset=utf-8")

    @mcp.custom_route("/sitemap.xml", methods=["GET"])
    async def _sitemap(_: Request) -> PlainTextResponse:
        return PlainTextResponse(sitemap_xml(), media_type="application/xml; charset=utf-8")

    @mcp.custom_route("/llms.txt", methods=["GET"])
    async def _llms(_: Request) -> PlainTextResponse:
        return PlainTextResponse(llms_txt(), media_type="text/markdown; charset=utf-8")

    @mcp.custom_route("/llms-full.txt", methods=["GET"])
    async def _llms_full(_: Request) -> PlainTextResponse:
        return PlainTextResponse(llms_full_txt(), media_type="text/markdown; charset=utf-8")

    return mcp


def _build_app(mcp: FastMCP) -> Any:
    return mcp.streamable_http_app()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("DUCKDB_DOCSBOX_LOG", "info").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    bind = os.environ.get("DUCKDB_DOCSBOX_BIND", "127.0.0.1:7821")
    host, _, port_s = bind.partition(":")
    port = int(port_s or "7821")

    corpus = load_corpus(os.environ.get("DUCKDB_DOCSBOX_CORPUS_DIR"))
    http = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        headers={"user-agent": "duckdb-spatial-docsbox-mcp/0.1"},
    )

    cfg = config_from_env()
    db = Database(cfg) if cfg is not None else None
    if db is None:
        logger.warning(
            "DUCKDB_DOCSBOX_PATH not set; database tools will return not_configured. "
            "Doc tools (duckdb_help, list_sections, get_documentation, validate_sql) "
            "remain available."
        )

    mcp = _build_mcp(corpus, http, db, host=host, port=port)

    logger.info("duckdb-spatial-docsbox-mcp listening on %s:%d (mcp at /mcp)", host, port)
    try:
        mcp.run(transport="streamable-http")
    finally:
        import asyncio

        try:
            asyncio.run(http.aclose())
        except (RuntimeError, OSError) as exc:
            logger.debug("ignoring httpx shutdown error: %s", exc)
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()
