"""duckdb_help tool — return the intent-organised DuckDB-spatial reference."""

from __future__ import annotations

from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..duckdb_spatial_reference import DUCKDB_SPATIAL_REFERENCE


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="duckdb_help",
        description=(
            "Return an intent-organised DuckDB-spatial function reference. "
            "Use this when a SELECT errored on a spatial function, when "
            "you're unsure how to compute metric distance/area without a "
            "geography type, when you need the R-Tree pattern instead of "
            "GiST, or when you need to read external files via ST_Read / "
            "GeoParquet. Reference is concise (~150 lines) and bundles "
            "common gotchas with worked examples."
        ),
    )
    async def duckdb_help(
        section: Annotated[
            str | None,
            Field(
                description=(
                    "Optional case-insensitive substring filter applied to "
                    "headings (e.g. 'distance', 'nearest', 'rtree', "
                    "'external'). When omitted, returns the full reference."
                ),
            ),
        ] = None,
    ) -> dict[str, Any]:
        if not section:
            return {"reference": DUCKDB_SPATIAL_REFERENCE, "filtered": False}
        needle = section.lower()
        keep: list[str] = []
        current: list[str] = []
        emit = False
        for line in DUCKDB_SPATIAL_REFERENCE.splitlines():
            if line.startswith("## "):
                if emit and current:
                    keep.extend(current)
                current = [line]
                emit = needle in line.lower()
            else:
                current.append(line)
        if emit and current:
            keep.extend(current)
        body = "\n".join(keep) if keep else f"(no section title matched {section!r})"
        return {"reference": body, "filtered": True, "filter": section}
