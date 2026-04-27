"""Intent-organised DuckDB-spatial function reference, modeled on geochat.

This is the single best return value for "I'm stuck on a spatial query in
DuckDB". Indexed by the *user intent*, not by function name. Each entry
tells the LLM the right function, the common wrong way, and the gotcha.

References:
- https://duckdb.org/docs/extensions/spatial/overview
- https://duckdb.org/docs/extensions/spatial/functions
- https://github.com/duckdb/duckdb-spatial
"""

from __future__ import annotations

DUCKDB_SPATIAL_REFERENCE = """\
# DuckDB-spatial query reference (intent-organised)

## Universal rules
- Load the extension first: `INSTALL spatial; LOAD spatial;` (the docsbox MCP does this for you).
- DuckDB-spatial geometries **do not store an SRID**. `ST_SRID(geom)` always returns 0. Track the SRID in metadata, not the geometry.
- There is **no GiST**; use the R-Tree index for bbox-accelerated lookup:
  `CREATE INDEX idx_t_geom ON t USING RTREE(geom);` (DuckDB 1.1+).
- There is **no `<->` KNN operator**. Use `ORDER BY ST_Distance(geom, p) LIMIT k`. With an R-Tree index DuckDB will use it for the bbox prefilter; the k-NN itself is brute force on the surviving rows.
- For metric distances use the spheroidal helpers: `ST_Distance_Sphere(a, b)` (metres on a perfect sphere) or `ST_Distance_Spheroid(a, b)` (WGS84). DuckDB has **no `geography` type**.
- Reprojection (`ST_Transform`) requires the `proj4` strings or EPSG codes; e.g. `ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')`.
- The geometry catalog: filter `duckdb_columns()` by `data_type = 'GEOMETRY'`. There is **no `geometry_columns` view**.

## "How far is X from Y?" — distance in metres
- WRONG: `ST_Distance(a, b)` — returns *Cartesian* distance in the input units (degrees if your data is lon/lat).
- RIGHT (sphere): `ST_Distance_Sphere(a, b)` — metres on a sphere.
- RIGHT (WGS84): `ST_Distance_Spheroid(a, b)` — metres on the WGS84 ellipsoid.

## "Find features within N metres" — radius search
- WRONG: `WHERE ST_Distance(a, b) < 0.001` — wrong units, no R-Tree.
- RIGHT (cheap, indexed): `WHERE ST_Intersects(a, ST_Buffer(b, deg)) AND ST_Distance_Sphere(a, b) < METRES`.
  The `ST_Intersects` against the buffer uses the R-Tree bbox; the spheroidal check refines.
- DuckDB has no `ST_DWithin`. Emulate with the bbox-prefilter pattern above.

## "Nearest k features" — KNN
- WRONG: `ORDER BY a.geom <-> p` — the operator does not exist in DuckDB-spatial.
- RIGHT: `SELECT * FROM t ORDER BY ST_Distance(geom, p) LIMIT k`.
- For metric ordering: `SELECT * FROM t ORDER BY ST_Distance_Sphere(geom, p) LIMIT k`.
- Pre-filter with a bbox to avoid scanning the whole table:
  `WHERE ST_Intersects(geom, ST_Buffer(p, 0.05)) ORDER BY ST_Distance_Sphere(geom, p) LIMIT 5`.

## "Inside polygon X" — point-in-polygon
- WRONG: `ST_Contains(point, poly)` — argument order matters; `Contains(A,B)` means A contains B.
- RIGHT: `ST_Contains(poly, point)` or `ST_Within(point, poly)`.
- For boundary-tolerant checks: `ST_Covers(poly, point)`.

## "Touches/overlaps/intersects" — relations
- `ST_Intersects(a, b)` — any shared point (R-Tree-friendly).
- `ST_Touches(a, b)` — share boundary, no interior overlap.
- `ST_Overlaps(a, b)` — share interior, neither contained.
- `ST_Crosses(a, b)` — line crossing line/polygon.
- `ST_Disjoint(a, b)` — share nothing.

## "Buffer / dilate" — geometric grow
- `ST_Buffer(geom, distance)` — buffer in the *input units*. For lon/lat data the distance is in degrees.
- For a metric buffer on lon/lat data: `ST_Transform(ST_Buffer(ST_Transform(geom, 'EPSG:4326', 'EPSG:3857'), 100), 'EPSG:3857', 'EPSG:4326')`.

## "Centroid / interior point"
- `ST_Centroid(geom)` — geometric centroid (may be outside concave shapes).
- `ST_PointOnSurface(geom)` — guaranteed-inside representative point.

## "Bounding box / extent"
- `ST_Envelope(geom)` — per-row bbox as polygon.
- `ST_Extent(geom)` — aggregate bbox over a query, returns a `BOX_2D`.
- `ST_Extent_Agg(geom)` — alias in some builds.
- For a JSON-friendly extent: `ST_AsGeoJSON(ST_Envelope(ST_Union_Agg(geom)))` (heavier but exact).

## "Area / length / perimeter" — measurements
- `ST_Area(geom)` — area in input units squared (square degrees for lon/lat — almost never what you want).
- `ST_Area_Spheroid(geom)` — square metres on WGS84.
- `ST_Length(geom)` — length in input units.
- `ST_Length_Spheroid(geom)` — metres on WGS84.
- `ST_Perimeter(geom)` / `ST_Perimeter_Spheroid(geom)`.

## "Read GeoJSON in / out"
- In:  `ST_GeomFromGeoJSON(text)`.
- Out: `ST_AsGeoJSON(geom)` — returns the geometry only.
- Bulk to FeatureCollection: build with DuckDB JSON functions, e.g.
  `SELECT json_object('type', 'FeatureCollection', 'features', json_group_array(json_object('type','Feature','geometry', ST_AsGeoJSON(geom)::JSON, 'properties', to_json(t) - 'geom'))) FROM t`.

## "Read WKT in / out"
- In:  `ST_GeomFromText('POINT(151.2 -33.9)')` (no SRID parameter — DuckDB ignores SRID).
- Out: `ST_AsText(geom)`.
- WKB: `ST_AsHEXWKB(geom)` (hex string) or `ST_AsWKB(geom)` (BLOB).

## "Snap to grid / simplify"
- `ST_SimplifyPreserveTopology(geom, tolerance)` — keep validity.
- `ST_Simplify(geom, tolerance)` — Douglas-Peucker, may produce invalid output.
- `ST_ReducePrecision(geom, gridSize)` — quantise.

## "Reproject"
- `ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')` — Web Mercator.
- `ST_Transform(geom, 'EPSG:3857', 'EPSG:4326')` — WGS84.
- `ST_Transform` requires PROJ; bundled with the spatial extension since 0.10.

## "Read external files" — DuckDB superpower
- GeoParquet:    `SELECT * FROM 'data/cities.parquet';`  (auto-detected).
- CSV with WKT:  `SELECT *, ST_GeomFromText(wkt_col) AS geom FROM read_csv('data/x.csv');`
- GeoJSON:       `SELECT * FROM ST_Read('data/x.geojson');`
- Shapefile:     `SELECT * FROM ST_Read('data/x.shp');`
- KML/GPX/...:   anything in `ST_Drivers()` works through `ST_Read`.
- List drivers:  `SELECT * FROM ST_Drivers();`
- Dump to GeoJSON: not via SELECT — use the DuckDB CLI `COPY ... TO 'x.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');` (denied in this MCP; see `run_locally`).

## Validate / repair geometry
- `ST_IsValid(geom)`.
- `ST_MakeValid(geom)` — best-effort fix.
- `ST_Multi(geom)` — coerce single → multi (useful before union or normalised storage).

## Spatial joins — the cookbook
```sql
-- All schools inside the suburb 'Surry Hills' (R-Tree-accelerated):
SELECT s.*
FROM schools s
JOIN suburbs sub ON sub.name = 'Surry Hills'
WHERE ST_Intersects(s.geom, sub.geom) AND ST_Within(s.geom, sub.geom);

-- Nearest 5 hospitals to a point (sphere distance, no `<->`):
WITH p AS (SELECT ST_Point(151.21, -33.87) AS g)
SELECT h.id, h.name, ST_Distance_Sphere(h.geom, p.g) AS metres
FROM hospitals h, p
ORDER BY metres
LIMIT 5;

-- Suburbs and the count of schools within each:
SELECT sub.name, COUNT(s.*) AS n_schools
FROM suburbs sub
LEFT JOIN schools s ON ST_Intersects(s.geom, sub.geom) AND ST_Within(s.geom, sub.geom)
GROUP BY sub.name
ORDER BY n_schools DESC;

-- Streaming a GeoParquet from S3 (httpfs needed):
-- INSTALL httpfs; LOAD httpfs; -- denied here, do it in `run_locally`.
SELECT id, name, ST_AsGeoJSON(geom) AS geojson
FROM 's3://bucket/cities.parquet'
WHERE ST_Intersects(geom, ST_MakeEnvelope(150, -34, 152, -33))
LIMIT 100;
```

## R-Tree index — the only spatial index
- Create:  `CREATE INDEX idx_t_geom ON t USING RTREE(geom);`
- Drop:    `DROP INDEX idx_t_geom;`
- Inspect: `SELECT * FROM duckdb_indexes() WHERE table_name = 't';`
- Used automatically by `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Equals` when the predicate appears in a `WHERE` and the index covers the column.
- Not used for `ST_DWithin` (does not exist) or for `ORDER BY ST_Distance` directly — pre-filter with a bbox/buffer to enable.

## Catalog tables worth knowing
- `duckdb_tables()`        — base tables.
- `duckdb_views()`         — views.
- `duckdb_columns()`       — every column with `data_type` (filter `'GEOMETRY'` to find geom columns).
- `duckdb_indexes()`       — index list including R-Tree.
- `duckdb_constraints()`   — PK/FK/UNIQUE/CHECK.
- `duckdb_extensions()`    — `installed`/`loaded` flags per extension.
- `duckdb_functions()`     — every callable, including all `ST_*`.
- `duckdb_settings()`      — current session settings.

## Common errors and the fix
- `Catalog Error: Function with name "st_dwithin" does not exist` → use `ST_Intersects(a, ST_Buffer(b, d)) AND ST_Distance_Sphere(a, b) < metres`.
- `Binder Error: No function matches the given name and argument types '<->'` → DuckDB has no KNN operator; use `ORDER BY ST_Distance(geom, p) LIMIT k`.
- `IO Error: Cannot open file ... extension 'spatial' has not been loaded` → run `LOAD spatial;` once per connection (the docsbox does this automatically).
- `Conversion Error: Could not convert string '...' to GEOMETRY` → check WKT/GeoJSON; try `ST_GeomFromText` with explicit prefix.
- `Catalog Error: Table "geometry_columns" does not exist` → DuckDB-spatial has no such view; query `duckdb_columns()` filtered to `data_type='GEOMETRY'`.
- `column "geom" does not exist` → call `get_table_schema` to verify the geometry column name (often `the_geom`, `wkb_geometry`, `shape`, `geometry`).
- `ST_Transform requires PROJ` → upgrade to spatial >= 0.10; bundled by default in DuckDB 1.0+.
"""
