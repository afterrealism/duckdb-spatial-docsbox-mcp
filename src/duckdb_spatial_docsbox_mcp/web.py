"""Static-asset loader for the landing page and SEO files.

Files are shipped under ``duckdb_spatial_docsbox_mcp/web/`` in the wheel.
"""

from __future__ import annotations

from importlib import resources


def _read(name: str) -> str:
    try:
        return resources.files("duckdb_spatial_docsbox_mcp").joinpath(f"web/{name}").read_text(
            "utf-8"
        )
    except (FileNotFoundError, ModuleNotFoundError):
        return ""


def landing_page() -> str:
    text = _read("index.html")
    return text or "<!doctype html><meta charset=utf-8><title>duckdb-spatial-docsbox-mcp</title><h1>duckdb-spatial-docsbox-mcp</h1><p>Streamable-HTTP MCP server. POST <code>/mcp</code>; GET <code>/health</code>.</p>"


def robots_txt() -> str:
    return _read("robots.txt") or "User-agent: *\nAllow: /\n"


def sitemap_xml() -> str:
    return _read("sitemap.xml") or '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"/>'


def llms_txt() -> str:
    return _read("llms.txt") or "# duckdb-spatial-docsbox-mcp\n"


def llms_full_txt() -> str:
    return _read("llms-full.txt") or llms_txt()
