# AGENTS.md - kryptosm

Guidance for AI agents working on **kryptosm**, the OSM → Iceberg utility.

## What this project is

A tiny utility that turns OpenStreetMap data into a single Apache Iceberg table.
It runs at scale on Spark + Sedona. The whole point is to **stay simple**.

- **Initial load** from OSM Parquet (nodes / ways / relations).
- **Incremental update** from an OSC change file (download from OSM replication
  or read a local `.osc.gz`).
- **Geometries** built with Apache Sedona.
- **One Iceberg table** partitioned by `type`, supporting Glue or Hadoop catalogs.

## Design rules (do not break these)

1. **Always use DataFrames and SQL.** No pandas, no Python UDFs. Business
   logic - filtering, joins, geometry construction - lives in SQL strings
   that build temp views.
2. **Chain views; don't materialize.** Every step in the pipeline is a
   `createOrReplaceTempView`. The query optimizer plans the whole DAG;
   Spark materializes only at write/MERGE time. **Never call `.count()` /
   `.collect()` for progress prints** - those force eager jobs and ruin
   scaling.
3. **No backwards compatibility.** When something is wrong or unused, delete
   it. Don't add deprecation shims.
4. **Read the SQL.** A reader should be able to scan a function's SQL and
   understand the transformation without chasing helpers.

## Repository layout

```
kryptosm/
├── pyproject.toml
├── Makefile
├── AGENTS.md                    # this file
│
├── kryptosm/                    # package
│   ├── __init__.py              # public API re-exports
│   ├── iceberg.py               # CREATE / MERGE / DELETE, index tables, maintenance
│   ├── osc.py                   # OSC parsing, fetch (next_osc_path), apply (apply_osc)
│   ├── replication.py           # Geofabrik OSC download via pyosmium
│   ├── inspect.py               # snapshot diff → GeoJSON + HTML map viewer
│   └── geometry/                # the SQL pipeline, one stage per file
│       ├── __init__.py
│       ├── nodes.py             # Point per OSM node
│       ├── ways.py              # LineString / Polygon per OSM way
│       ├── relations.py         # MultiPolygon / MultiLineString per relation
│       ├── osc_apply.py         # dirty-set computation using index tables
│       └── iceberg_prep.py      # geom → WKB+bbox layout for Iceberg write
│
└── tests/
    ├── __init__.py              # Spark session factory, constants, helpers
    ├── test_e2e_init.py         # Build table from Parquet
    ├── test_e2e_osc.py          # Fetch + apply next pending OSC (idempotent)
    ├── test_e2e_osc_all.py      # Fetch + apply all pending OSCs
    ├── test_inspect.py          # Snapshot inspector
    ├── test_replication.py      # Replication unit + live tests
    └── data/
        ├── WashingtonDC/
        │   ├── dc.parquet/      # Test input (DC OSM extract)
        │   └── osc/             # Test OSC files
        └── output/              # Test output (gitignored)
```

## Data flow

Each arrow is a `createOrReplaceTempView`. Spark plans the whole DAG and
only materializes at the final write / MERGE.

```
Input Parquet
  ├── type=node/      [nodes.py]
  │     build_node_geometry                         → nodes_with_geom
  ├── type=way/       [ways.py]
  │     build_linestring_for_ways                   → ways_linestrings
  │     build_ways_geometry_from_linestring         → ways_with_geom
  └── type=relation/  [relations.py]
        relations_need_geometry                     → relations_need_geom
        construct_multipolygon                      → relations_geom
        relation_merge_geometry_data                → relations_with_geom

  each → prepare_for_iceberg [iceberg_prep.py]
       → writeTo(table).append()

Iceberg table (partitioned by type)
```

For incremental updates, the flow adds:

```
osc_raw → osc_dedup [osc.py] → osc_latest

  ├── nodes:     direct upserts → build_node_geometry [nodes.py]
  ├── ways:      all_dirty_ways [osc_apply.py]   (direct + ways with dirty nodes)
  │              → build_linestring_for_ways
  │              → build_ways_geometry_from_linestring [ways.py]
  └── relations: all_dirty_relations [osc_apply.py]   (direct + dirty-way deps)
                 → relations_need_geometry
                 → construct_multipolygon
                 → relation_merge_geometry_data [relations.py]

  each → apply_osc_with_geometry [osc_apply.py]   (overlay updates, drop deletes)
       → prepare_for_iceberg [iceberg_prep.py]
       → MERGE INTO + delete MERGE [iceberg.py]
```

## Module reference

### `main.py`

Two functions:

- `run_init_mode(spark, args)` - Parquet → Iceberg, full table.
- `run_update_mode(spark, args)` - OSC → Iceberg, MERGE-based incremental.

Both are flat sequences of view-creation calls followed by writes / merges.
There is exactly one debug helper `_print_counts` that runs a single SQL
aggregation at the end.

### `spark.py`

- `create_spark_session(...)` - production session, Glue or Hadoop catalog.
  Uses cached JARs from `~/.cache/kryptosm/jars/` if present, else lets Spark
  resolve via Maven.
- `create_spark_session_for_testing(warehouse_dir)` - local-mode session used
  by the E2E tests.

### `iceberg.py`

Thin SQL wrappers, nothing more:

- `table_exists(spark, table_name)` - DESCRIBE / catch.
- `create_iceberg_table(spark, table_name, table_location=None)` - DROP +
  CREATE with the canonical schema below.
- `get_table_count(spark, table_name)` - `{type: count}` summary.
- `merge_into_table(spark, table_name, source_view, match_condition)` -
  upsert MERGE.
- `delete_from_table(spark, table_name, source_view, match_condition)` -
  delete MERGE.

### `geometry/` (the SQL pipeline)

Every function in this package takes view names in/out and runs a single
SQL statement (occasionally two or three when intermediate views are
clearer). Functions live in the submodule that matches their stage so a
reader can find them without grepping. Each function's docstring lists
**input view columns**, **output view columns**, and **why** the SQL is
shaped the way it is.

Import from the submodule directly - the package `__init__.py` is empty
on purpose.

- **`geometry/nodes.py`** - `build_node_geometry`. Projects (lat, lon) to
  ST_Point with 7-digit precision.
- **`geometry/ways.py`** - `build_linestring_for_ways`,
  `build_ways_geometry_from_linestring`. Joins ways to their node
  geometries, sorts by node position, and promotes closed area-tagged
  ways to polygons.
- **`geometry/relations.py`** - `relations_need_geometry`,
  `construct_multipolygon`, `relation_merge_geometry_data`. Polygon types
  (`multipolygon`, `boundary`) get outer-minus-inner MultiPolygons; line
  types (`route`, `waterway`) get a unioned MultiLineString. The set of
  built types lives in `GEOMETRY_RELATION_TYPES`. Other relation types
  are still written, just with NULL geometry.
- **`geometry/osc_apply.py`** - `all_dirty_ways`, `all_dirty_relations`,
  `apply_osc_with_geometry`. Compute the dependency-aware dirty set, then
  overlay (COALESCE) updates and drop deletes (LEFT ANTI JOIN).
- **`geometry/iceberg_prep.py`** - `prepare_for_iceberg`,
  `MAXIMUM_RELATION_GEOMETRY_SIZE`,
  `HUGE_GEOMETRY_SIMPLIFICATION_FACTOR`. Serializes `geom` to WKB, adds
  the bbox struct, pins the `type` partition column, and simplifies
  oversized relation geometries inline.

### `osc.py`

- `OSC_SCHEMA` - the single source of truth for the OSC DataFrame schema.
- `osc_dedup` - SQL: keep the latest `(id, type)` per OSC.
- `download_osc_to_dataframe(spark, publish_date)` - download a daily OSC
  by date and parse to DataFrame.
- `read_osc_from_file(spark, file_path)` - parse a local `.osc[.gz]`.
- `read_osc_from_parquet(spark, path)` - read an already-parsed OSC.

XML parsing is pure Python because the input format demands it; the moment
we have records, we hand them to Spark and never look back.

### `replication.py`

Geofabrik OSC replication downloads using pyosmium's `ReplicationServer`.
`fetch_osc_files()` downloads pending `.osc.gz` files given a last-applied sequence
or table timestamp.

### `inspect.py`

Snapshot inspector. Compares two Iceberg snapshots via a FULL OUTER JOIN
on `(id, type)` and generates GeoJSON + an interactive HTML map viewer
(MapLibre GL JS with a timeline slider). Key functions:

- `list_snapshots(spark, table_name)` - query the `.snapshots` metadata table.
- `diff_snapshots(spark, table_name, before, after)` - collect changed rows.
- `inspect_snapshots(spark, table_name, output_dir, ...)` - orchestrate
  one or more diffs, write `.geojson` files and `inspector.html`.

## Iceberg table schema

```sql
CREATE TABLE table_name (
    id        BIGINT,
    type      STRING,                 -- 'node' | 'way' | 'relation'
    version   BIGINT,
    timestamp TIMESTAMP,
    changeset BIGINT,
    uid       BIGINT,
    user      STRING,
    tags      MAP<STRING, STRING>,
    lat       DOUBLE,                 -- nodes
    lon       DOUBLE,                 -- nodes
    refs      ARRAY<BIGINT>,          -- ways
    members   ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,  -- relations
    latest_ts TIMESTAMP,
    geometry  BINARY,                 -- WKB
    bbox      STRUCT<xmin: FLOAT, xmax: FLOAT, ymin: FLOAT, ymax: FLOAT>
)
USING iceberg
PARTITIONED BY (type)
```

Data is sorted by `id ASC` within each partition with bloom filters on
`id` for fast point lookups during MERGE and geometry joins. Distribution
mode is `range` so writes produce sorted files.

The two index tables (`node_to_ways`, `way_to_relations`) use the same
sort-order and bloom-filter settings via `TableConfig`.

`TableConfig` controls these Iceberg table properties at creation time:
- `distribution_mode` — `'range'` (production) or `'none'` (testing)
- `bloom_filter_enabled` — `True` for production, `False` for testing
- `bloom_filter_max_bytes` — `1048576` (1 MB) for production, `None` for testing

Use `TableConfig.testing()` for local/CI runs and `TableConfig.production()`
for planet-scale data.

## Tests

There are no CLI entry points. The E2E tests **are** the sample scripts —
they demonstrate exactly how to wire up a Spark session, call the library
functions, and validate the output. A production cron job would look
nearly identical.

All stages write to the same Iceberg table (`hadoop_catalog.test_db.dc`);
each persists its output to `tests/data/output/warehouse`, so you can
run any stage standalone once its predecessors have run.

```bash
make test-e2e-init           # build table from Parquet
make test-e2e-fetch-and-apply # fetch OSC from Geofabrik + apply (network)
make test-inspect            # snapshot inspector
```

## Common changes

### New geometry rule

Edit the SQL in the right `geometry/` submodule (`nodes.py`, `ways.py`,
`relations.py`). There is no other place for it.

### New relation type that needs geometry

Add it to `GEOMETRY_RELATION_TYPES` in `geometry/relations.py`, then
either extend `construct_multipolygon` (if it's a new shape kind) or rely
on the existing polygon / line branches.

### Schema change

Edit `create_iceberg_table` in `iceberg.py`, then make sure
`prepare_for_iceberg` in `geometry/iceberg_prep.py` produces a matching
shape, and update the schema block in this file. There is no migration
story - we recreate the table.

## Dependencies

Pinned in `pyproject.toml`:

- `pyspark==3.5.0`
- `apache-sedona==1.8.1`
- `boto3>=1.35.47`
- `requests>=2.28.0`

JARs auto-cached at `~/.cache/kryptosm/jars/`:

- `sedona-spark-shaded-3.5_2.12-1.8.1.jar`
- `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`
- `iceberg-aws-bundle-1.6.1.jar`

If they aren't cached, Spark resolves them via Maven on session start.

## License

Apache 2.0.
