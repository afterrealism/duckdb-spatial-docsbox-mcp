# duckdb-spatial-docsbox-mcp

A Model Context Protocol (MCP) server giving an LLM agent **safe, bounded
read-only access** to a DuckDB database with the `spatial` extension,
plus a curated DuckDB + spatial reference and a docs/manifest lookup.

Sibling project to
[`postgres-postgis-docsbox-mcp`](../postgres-postgis-docsbox-mcp),
[`python-docsbox-mcp`](../python-docsbox-mcp), and
[`rust-docsbox-mcp`](../rust-docsbox-mcp); shares the same FastMCP
streamable-HTTP shape.

## Why

A generic "duckdb MCP" with raw CLI access is a footgun: an agent can
`ATTACH` an arbitrary file, `INSTALL httpfs` and exfiltrate, or
`EXPORT DATABASE` to a writable path. This server takes the opposite
stance.

Defence in depth:

1. **Static SQL validation** &mdash; sqlglot parse with `dialect="duckdb"` +
   denylist of statement kinds (DDL, DML, `ATTACH`, `DETACH`, `INSTALL`,
   `LOAD`, `EXPORT`, `IMPORT`, `PRAGMA`, `CHECKPOINT`, `USE`, ...) and
   dangerous functions (`read_text`, `read_blob`, `shell`, `system`).
   Refuses anything that isn't top-level `SELECT` / `WITH` / `EXPLAIN`.
   Auto-injects `LIMIT` if missing.
2. **Read-only connection** &mdash; file-mode databases are opened with
   `read_only=True` at the storage layer; DuckDB itself rejects DDL/DML.
   `:memory:` mode falls back to validator-only enforcement (documented).
3. **Per-statement timeout** &mdash; DuckDB has no SQL-level
   `statement_timeout`, so a `threading.Timer` invokes
   `connection.interrupt()` and the resulting `InterruptException` is
   mapped to a typed `TimeoutError`.
4. **Bounded results** &mdash; row caps, per-cell byte caps, geometry
   wrapped to GeoJSON via `ST_AsGeoJSON` for inspection.
5. **No process fan-out** &mdash; the only subprocess-shaped tool is
   `run_locally`, which is plan-only.

## Tools

| Tool | Purpose |
|------|---------|
| `list_tables` | User tables/views with row count, geometry flag |
| `get_table_schema` | DDL + sample rows + indexes |
| `get_column_values` | Distinct sample values for a column |
| `list_srids` | SRIDs in active use (DuckDB stores all geometry as SRID 0) |
| `get_relationships` | Foreign-key edges |
| `list_extensions` | Loaded/installed DuckDB extensions |
| `pick_interesting_tables` | Score by rows + geom + R-Tree + FK hubness |
| `validate_sql` | Static safety check (no DB needed) |
| `explain_sql` | EXPLAIN [ANALYZE] text plan |
| `execute_sql` | SELECT/WITH/EXPLAIN with row caps |
| `list_gdal_drivers` | Installed GDAL drivers via `ST_Drivers()` |
| `probe_external_file` | Peek schema/sample of CSV/Parquet/GeoJSON/SHP/GPKG |
| `duckdb_help` | Curated DuckDB+spatial recipe reference |
| `list_sections` / `get_documentation` | Doc manifest browser |
| `run_locally` | Plan-only execution recipes (duckdb CLI, ST_Read, ...) |

Each tool's docstring carries a worked example with the expected JSON shape;
the agent doesn't have to guess.

## Quick start

### 1. Install

```sh
pip install -e .
```

### 2. Load the sample dataset

```sh
duckdb /tmp/sydney.duckdb -f examples/sample_data.sql
```

### 3. Run the MCP server

```sh
export DUCKDB_DOCSBOX_PATH=/tmp/sydney.duckdb
duckdb-spatial-docsbox-mcp
# listening on 127.0.0.1:7821 (mcp at /mcp)
```

### 4. Wire into your client

`opencode`:

```json
{
  "mcp": {
    "duckdb-spatial-docsbox": {
      "type": "remote",
      "url": "http://127.0.0.1:7821/mcp",
      "enabled": true
    }
  }
}
```

Claude Code (`~/.claude.json`):

```json
{
  "mcpServers": {
    "duckdb-spatial-docsbox": {
      "type": "http",
      "url": "http://127.0.0.1:7821/mcp"
    }
  }
}
```

## Worked examples (against the sample dataset)

```
list_extensions()
-> {"ok": true, "extensions": [
     {"name":"spatial","loaded":true,"installed":true, "version":"..."},
     ...
   ]}

list_tables()
-> {"ok": true, "tables": [
     {"schema":"main","name":"hospitals","kind":"BASE TABLE","row_count":4,
      "has_geom":true,"geom_columns":["geom"]},
     {"schema":"main","name":"schools","kind":"BASE TABLE","row_count":8,
      "has_geom":true,"geom_columns":["geom"]},
     {"schema":"main","name":"suburbs","kind":"BASE TABLE","row_count":5,
      "has_geom":true,"geom_columns":["geom"]}
   ], "count": 3}

pick_interesting_tables(limit=3)
-> top tables ordered by log10(rows) + R-Tree/FK-hub bonuses,
   each with a GeoJSON extent.

get_table_schema(table='main.suburbs', sample_rows=2)
-> DDL + columns + sample rows with geometry as GeoJSON.

execute_sql(sql=
  'SELECT s.name AS school, sub.name AS suburb '
  'FROM schools s JOIN suburbs sub ON ST_Contains(sub.geom, s.geom) '
  'ORDER BY school')
-> rows pairing each school with the suburb whose polygon contains it.

list_srids()
-> {"ok": true, "srids":[{"srid":0,"count":3,"name":"unknown"}],
    "warning":"DuckDB-spatial does not persist SRIDs; every geometry
               reports SRID 0. Use ST_Transform(geom,'EPSG:src','EPSG:dst')
               explicitly when you need a coordinate-system change."}

duckdb_help(section='rtree')
-> the R-Tree recipe (CREATE INDEX ... USING RTREE(geom)).

probe_external_file(path='/data/buildings.parquet', limit=3)
-> column names, types, sample rows from a Parquet file.
```

## Configuration

| Var | Default | Purpose |
|-----|---------|---------|
| `DUCKDB_DOCSBOX_PATH` | (required) | DuckDB file path or `:memory:` |
| `DUCKDB_DOCSBOX_STATEMENT_TIMEOUT_S` | 10.0 | per-statement wall-clock bound |
| `DUCKDB_DOCSBOX_LOAD_SPATIAL` | 1 | set to 0 to skip spatial bootstrap |
| `DUCKDB_DOCSBOX_METADATA_EXCLUDES` | "" | comma-separated `schema.table` to hide |
| `DUCKDB_DOCSBOX_BIND` | 127.0.0.1:7821 | host:port |
| `DUCKDB_DOCSBOX_CORPUS_DIR` | packaged | override doc manifest dir |
| `DUCKDB_DOCSBOX_DISABLE_DNS_PROTECTION` | unset | tests only |

### Spatial extension bootstrap

DuckDB extensions install to `~/.duckdb/extensions/`. This server runs
a one-shot transient writable connection at startup to
`INSTALL spatial; LOAD spatial;`, then reopens the database as
read-only. Subsequent `LOAD spatial` calls on the read-only connection
work because the binary is already cached on disk.

### Recommended hardening

Even though the storage layer is opened `read_only=True`, point the
server at a *copy* of the database (or a snapshot) when serving
untrusted agents. DuckDB read-only mode prevents writes through the
opened handle but not file-system-level mutations from outside.

## Docker

```sh
docker build -t duckdb-spatial-docsbox-mcp .
docker run --rm -p 7821:7821 \
  -v /srv/data:/data:ro \
  -e DUCKDB_DOCSBOX_PATH=/data/sydney.duckdb \
  duckdb-spatial-docsbox-mcp
```

## Development

```sh
pip install -e '.[dev]'
pytest -q                 # unit tests only
pytest -q --run-db        # full suite with the DuckDB fixture
ruff check src tests
```

## License

MIT
