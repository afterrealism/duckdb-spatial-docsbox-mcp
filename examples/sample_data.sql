-- Sample dataset for duckdb-spatial-docsbox-mcp tests and demos.
--
-- Greater Sydney mini-fixture: suburbs (polygons), schools (points),
-- hospitals (points), with one foreign key from schools -> suburbs.
--
-- Loadable via:
--   duckdb "$DUCKDB_DOCSBOX_PATH" -f examples/sample_data.sql
--
-- All coordinates are EPSG:4326 (WGS84) lon/lat. DuckDB-spatial does NOT
-- persist SRIDs -- every geometry is reported as SRID 0 by ST_SRID.
-- Downstream queries that need a coordinate system must call
-- ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') explicitly.

INSTALL spatial;
LOAD spatial;

DROP TABLE IF EXISTS schools;
DROP TABLE IF EXISTS hospitals;
DROP TABLE IF EXISTS suburbs;

CREATE TABLE suburbs (
    gid         INTEGER PRIMARY KEY,
    name        VARCHAR NOT NULL,
    state       VARCHAR NOT NULL DEFAULT 'NSW',
    population  INTEGER,
    geom        GEOMETRY NOT NULL
);

CREATE INDEX suburbs_geom_idx ON suburbs USING RTREE(geom);

CREATE TABLE schools (
    sid         INTEGER PRIMARY KEY,
    name        VARCHAR NOT NULL,
    sector      VARCHAR NOT NULL CHECK (sector IN ('government','catholic','independent')),
    enrolments  INTEGER,
    suburb_id   INTEGER REFERENCES suburbs(gid),
    geom        GEOMETRY NOT NULL
);

CREATE INDEX schools_geom_idx ON schools USING RTREE(geom);

CREATE TABLE hospitals (
    hid         INTEGER PRIMARY KEY,
    name        VARCHAR NOT NULL,
    beds        INTEGER,
    geom        GEOMETRY NOT NULL
);

CREATE INDEX hospitals_geom_idx ON hospitals USING RTREE(geom);

-- Suburbs (axis-aligned envelopes around real Sydney suburbs).
INSERT INTO suburbs (gid, name, population, geom) VALUES
  (1, 'Bondi',       11000, ST_MakeEnvelope(151.260, -33.900, 151.290, -33.880)),
  (2, 'Surry Hills', 17000, ST_MakeEnvelope(151.205, -33.890, 151.225, -33.875)),
  (3, 'Parramatta',  28000, ST_MakeEnvelope(151.000, -33.825, 151.030, -33.805)),
  (4, 'Manly',       16000, ST_MakeEnvelope(151.275, -33.805, 151.300, -33.785)),
  (5, 'Newtown',     14000, ST_MakeEnvelope(151.170, -33.905, 151.190, -33.885));

-- Schools (points; suburb_id resolved manually so the FK is satisfied).
INSERT INTO schools (sid, name, sector, enrolments, suburb_id, geom) VALUES
  (1, 'Bondi Beach Public School',       'government',   650, 1, ST_Point(151.275, -33.890)),
  (2, 'Bondi Public School',             'government',   480, 1, ST_Point(151.270, -33.892)),
  (3, 'Surry Hills Public School',       'government',   320, 2, ST_Point(151.215, -33.882)),
  (4, 'Reddam House',                    'independent', 1100, 2, ST_Point(151.218, -33.885)),
  (5, 'Parramatta Marist High',          'catholic',     950, 3, ST_Point(151.015, -33.815)),
  (6, 'Arthur Phillip High',             'government',  1200, 3, ST_Point(151.012, -33.818)),
  (7, 'Manly West Public School',        'government',   720, 4, ST_Point(151.286, -33.795)),
  (8, 'Newtown High of Performing Arts', 'government',   980, 5, ST_Point(151.180, -33.895));

-- Hospitals
INSERT INTO hospitals (hid, name, beds, geom) VALUES
  (1, 'St Vincents Hospital', 400, ST_Point(151.220, -33.880)),
  (2, 'Westmead Hospital',    980, ST_Point(150.985, -33.802)),
  (3, 'Royal North Shore',    700, ST_Point(151.190, -33.825)),
  (4, 'Prince of Wales',      440, ST_Point(151.240, -33.918));
