"""Docs lookup: list_sections, get_documentation."""

from __future__ import annotations

import logging
from typing import Annotated, Any

import httpx
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..corpus import Corpus

logger = logging.getLogger(__name__)


def register(mcp: FastMCP, corpus: Corpus, http: httpx.AsyncClient) -> None:
    @mcp.tool(
        name="list_sections",
        description=(
            "List documentation sections in the offline corpus. Filter by "
            "package id (e.g. 'duckdb', 'spatial', 'gdal', 'geoparquet'). "
            "Returns id, title, package, and canonical url for each section."
        ),
    )
    async def list_sections(
        package: Annotated[
            str | None,
            Field(description="Optional package filter (e.g. 'duckdb', 'spatial', 'gdal')."),
        ] = None,
    ) -> dict[str, Any]:
        sections = corpus.list(package=package)
        return {
            "count": len(sections),
            "sections": [
                {"id": s.id, "title": s.title, "package": s.package, "url": s.url}
                for s in sections
            ],
        }

    @mcp.tool(
        name="get_documentation",
        description=(
            "Fetch a documentation section by id (see list_sections). "
            "Returns the bundled offline body when available, otherwise "
            "fetches the canonical URL over HTTP and returns the raw HTML."
        ),
    )
    async def get_documentation(
        section_id: Annotated[str, Field(description="Section id from list_sections.")],
    ) -> dict[str, Any]:
        section = corpus.get(section_id)
        if section is None:
            return {"found": False, "id": section_id, "error": "section not found in corpus"}
        if section.body:
            return {
                "found": True, "id": section.id, "title": section.title,
                "package": section.package, "url": section.url,
                "body": section.body, "source": "offline",
            }
        try:
            resp = await http.get(section.url, follow_redirects=True)
            resp.raise_for_status()
            text = resp.text
            if len(text) > 200_000:
                text = text[:200_000] + "\n<!-- truncated -->\n"
            return {
                "found": True, "id": section.id, "title": section.title,
                "package": section.package, "url": section.url,
                "body": text, "source": "http",
            }
        except httpx.HTTPError as exc:
            return {
                "found": True, "id": section.id, "title": section.title,
                "package": section.package, "url": section.url,
                "body": None, "source": "error", "error": f"fetch failed: {exc}",
            }
