# kryptosm

Turn OpenStreetMap data into the **Krypton** Iceberg database — three
per-type tables (`nodes`, `ways`, `relations`) plus reverse-index and
OSC-archive tables — kept up to date with incremental OSC change files.

Built on PySpark + Apache Sedona for geometry construction and Apache Iceberg
for versioned, time-travel-capable table storage.

## The name

Kal-El was born on **Krypton** — a planet of ice — and sent to Earth as an
infant. He grew up in Kansas, moved to Metropolis, and landed a job at
**The Daily Planet**. But somewhere between saving the world and filing copy,
Clark Kent discovered a quiet obsession: **open map data**. Every node, every
way, every relation — the whole planet, mapped by volunteers, versioned down
to the centimeter. He couldn't look away.

He needed a database that could hold the entire planet and keep up with every
edit, stored on **Apache Iceberg** — because of course the guy from the ice
planet would pick the ice table format. He called it **Krypton**.

The `osm` suffix grounds it in **OpenStreetMap**: **krypt**on + **osm** =
`kryptosm`. The Daily Planet, published daily.

## What it does

1. **Initial load** — reads an OSM Parquet extract (nodes, ways, relations),
   builds geometries with Sedona SQL functions (`ST_Point`, `ST_LineString`,
   `ST_Polygon`, `ST_MultiPolygon`), and writes each type to its own Iceberg
   table with type-specific tuning (bloom filters, sort orders, distribution).

2. **Incremental update** — fetches OSC change files from a replication feed,
   computes the dependency-aware "dirty set" (if a node moves, every way
   referencing it and every relation referencing those ways or that node
   gets its geometry rebuilt), and MERGEs the changes into the relevant
   per-type tables. Each OSC file produces its own Iceberg snapshot per
   table, so each table's history steps through every change.

## Tenets

- **Everything is SQL.** Business logic lives in SQL strings that build temp
  views. No pandas, no Python UDFs. A reader should be able to scan a
  function's SQL and understand the transformation.

- **No CLI.** This is a library. The caller owns the Spark session — a
  cloud deployment (EMR, Glue, Databricks) provides its own. The E2E tests
  _are_ the sample scripts; a production cron job looks nearly identical.

- **Atomic, one-at-a-time updates.** Each OSC file is fetched and applied
  independently. `next_osc_path` returns the next pending file (downloading
  it if needed), `apply_osc` applies exactly one. The table property
  `last-applied-osc-sequence` tracks progress, so a crash mid-batch
  resumes cleanly.

- **Only rebuild what changed.** Reverse-index tables (`node_to_ways`,
  `way_to_relations`) make dirty-set computation O(dirty features) instead
  of O(all features). Changed features are MERGEd directly — no full
  partition rewrites.

- **Views, not materializations.** Each step registers a
  `createOrReplaceTempView`. Spark plans the whole DAG and materializes
  only at write/MERGE time. Between types (nodes → ways → relations), the
  pipeline re-binds from Iceberg so downstream phases read materialized
  data rather than re-executing upstream views.

- **Delete what's unused.** No backwards compatibility shims, no
  deprecation layers. If something is wrong, fix it.

## Database schema

Krypton is a per-type Iceberg layout. Each table has its own bloom-filter
budget, sort order, and compaction cadence — tuned to its row count and
shape.

### `nodes` table

| Column                  | Type                  | Description                         |
| ----------------------- | --------------------- | ----------------------------------- |
| `id`                    | `BIGINT`              | OSM node ID                         |
| `version`               | `BIGINT`              | OSM version number                  |
| `timestamp`             | `TIMESTAMP`           | Last edit timestamp                 |
| `changeset`             | `BIGINT`              | OSM changeset ID                    |
| `uid`                   | `BIGINT`              | Editor user ID                      |
| `user`                  | `STRING`              | Editor username                     |
| `tags`                  | `MAP<STRING, STRING>` | Key-value tag pairs                 |
| `lat`                   | `DOUBLE`              | Latitude                            |
| `lon`                   | `DOUBLE`              | Longitude                           |
| `latest_ts`             | `TIMESTAMP`           | Max timestamp across self           |
| `additional_changesets` | `ARRAY<BIGINT>`       | Always `[]` for nodes (no children) |
| `geometry`              | `BINARY`              | WKB-encoded ST_Point                |

No `bbox` — lat/lon already define a node's footprint.

### `ways` table

| Column                  | Type                                    | Description                                            |
| ----------------------- | --------------------------------------- | ------------------------------------------------------ |
| `id`                    | `BIGINT`                                | OSM way ID                                             |
| `version`               | `BIGINT`                                | OSM version number                                     |
| `timestamp`             | `TIMESTAMP`                             | Last edit timestamp                                    |
| `changeset`             | `BIGINT`                                | OSM changeset ID                                       |
| `uid`                   | `BIGINT`                                | Editor user ID                                         |
| `user`                  | `STRING`                                | Editor username                                        |
| `tags`                  | `MAP<STRING, STRING>`                   | Key-value tag pairs                                    |
| `refs`                  | `ARRAY<BIGINT>`                         | Ordered node references                                |
| `latest_ts`             | `TIMESTAMP`                             | Max timestamp across self + member nodes               |
| `additional_changesets` | `ARRAY<BIGINT>`                         | Member-node changesets strictly newer than `changeset` |
| `geometry`              | `BINARY`                                | WKB-encoded LineString or Polygon                      |
| `bbox`                  | `STRUCT<xmin, xmax, ymin, ymax: FLOAT>` | Bounding box                                           |

### `relations` table

| Column                  | Type                                    | Description                                                  |
| ----------------------- | --------------------------------------- | ------------------------------------------------------------ |
| `id`                    | `BIGINT`                                | OSM relation ID                                              |
| `version`               | `BIGINT`                                | OSM version number                                           |
| `timestamp`             | `TIMESTAMP`                             | Last edit timestamp                                          |
| `changeset`             | `BIGINT`                                | OSM changeset ID                                             |
| `uid`                   | `BIGINT`                                | Editor user ID                                               |
| `user`                  | `STRING`                                | Editor username                                              |
| `tags`                  | `MAP<STRING, STRING>`                   | Key-value tag pairs                                          |
| `members`               | `ARRAY<STRUCT<type, ref, role>>`        | Member references                                            |
| `latest_ts`             | `TIMESTAMP`                             | Max timestamp across self + members                          |
| `additional_changesets` | `ARRAY<BIGINT>`                         | Member-way / member-node changesets strictly newer than self |
| `geometry`              | `BINARY`                                | WKB-encoded MultiPolygon / MultiLineString / Collection      |
| `bbox`                  | `STRUCT<xmin, xmax, ymin, ymax: FLOAT>` | Bounding box                                                 |

### Index tables

- **`node_to_ways`** (`node_id`, `way_id`) — which ways reference each node
- **`way_to_relations`** (`way_id`, `relation_id`) — which relations reference each way
- **`node_to_relations`** (`node_id`, `relation_id`) — which relations reference each node directly
- **`relation_to_relations`** (`child_relation_id`, `parent_relation_id`) — sub-relation membership edges

### OSC archive

- **`osc_changes`** — one row per OSC change record, partitioned by `sequence`.
  Also holds the `last-applied-osc-sequence` and `current-osc-file` table
  properties (the single source of truth for "where in the replication feed
  are we?").

## How it works

### Init

```python
from kryptosm import *

# Caller provides the Spark session (cloud or local).
spark = ...

DB         = "glue_catalog.kryptosm"
NODES      = f"{DB}.nodes"
WAYS       = f"{DB}.ways"
RELATIONS  = f"{DB}.relations"
N2W        = f"{DB}.node_to_ways"
W2R        = f"{DB}.way_to_relations"
N2R        = f"{DB}.node_to_relations"
R2R        = f"{DB}.relation_to_relations"
ARCHIVE    = f"{DB}.osc_changes"

create_nodes_table(spark, NODES, config=TableConfig.nodes_production())
create_ways_table(spark, WAYS, config=TableConfig.ways_production())
create_relations_table(spark, RELATIONS, config=TableConfig.relations_production())
create_index_tables(spark, N2W, W2R, node_to_relations=N2R, relation_to_relations=R2R)
create_osc_archive_table(spark, ARCHIVE)

# Read raw Parquet
spark.read.parquet("s3://bucket/dc.parquet/type=node").createOrReplaceTempView("input_nodes")
spark.read.parquet("s3://bucket/dc.parquet/type=way").createOrReplaceTempView("input_ways_raw")
flatten_way_refs(spark, "input_ways_raw", "input_ways")
spark.read.parquet("s3://bucket/dc.parquet/type=relation").createOrReplaceTempView("input_relations")

# Nodes
build_node_geometry(spark, "input_nodes", "nodes_with_geom")
prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
spark.sql("SELECT * FROM nodes_final").writeTo(NODES).using("iceberg").append()
load_with_geom(spark, NODES, "nodes_with_geom")

# Ways
flatten_way_refs(spark, "input_ways_raw", "input_ways")
build_way_linestrings(spark, "input_ways", "nodes_with_geom", "ways_lines")
promote_closed_ways_to_areas(spark, "ways_lines", "ways_with_geom")
prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
spark.sql("SELECT * FROM ways_final").writeTo(WAYS).using("iceberg").append()
load_with_geom(spark, WAYS, "ways_with_geom")
populate_node_to_ways(spark, WAYS, N2W)

# Relations
relations_need_geometry(spark, "input_relations", "rels_need_geom")
construct_multipolygon(spark, "rels_need_geom", "ways_with_geom", "rels_geom")
relation_merge_geometry_data(spark, "input_relations", "rels_geom", "rels_with_geom")
prepare_for_iceberg(spark, "rels_with_geom", "relation", "rels_final")
spark.sql("SELECT * FROM rels_final").writeTo(RELATIONS).using("iceberg").append()
populate_way_to_relations(spark, RELATIONS, W2R)
populate_node_to_relations(spark, RELATIONS, N2R)
populate_relation_to_relations(spark, RELATIONS, R2R)
```

### Incremental update

```python
# Apply the next pending OSC (fetch + apply are separate)
path = next_osc_path(spark, NODES, WAYS, RELATIONS, ARCHIVE,
                     "/tmp/osc", base_url=GEOFABRIK_URL)
if path:
    apply_osc(spark, path,
              NODES, WAYS, RELATIONS,
              N2W, W2R, N2R, R2R,
              ARCHIVE)

# Or loop until current
while path := next_osc_path(spark, NODES, WAYS, RELATIONS, ARCHIVE,
                            "/tmp/osc", base_url=GEOFABRIK_URL):
    apply_osc(spark, path,
              NODES, WAYS, RELATIONS,
              N2W, W2R, N2R, R2R,
              ARCHIVE)
```

## Repository layout

```
kryptosm/
    __init__.py          — public API re-exports
    iceberg.py           — CREATE / MERGE / DELETE, index table operations
    osc.py               — OSC parsing, fetch (next_osc_path), apply (apply_osc)
    replication.py       — Geofabrik OSC download via pyosmium
    geometry/
        nodes.py         — ST_Point per node
        ways.py          — LineString / Polygon per way
        relations.py     — MultiPolygon / MultiLineString per relation
        osc_apply.py     — dirty-set computation using index tables
        iceberg_prep.py  — geom → WKB + bbox for Iceberg write
        samples.py       — GeoJSON sample writer

tests/
    __init__.py              — Spark session factory for tests
    test_e2e_init.py         — build table from Parquet
    test_e2e_osc.py          — fetch + apply the next OSC (idempotent)
    test_e2e_osc_all.py      — fetch + apply all pending OSCs
    test_replication.py      — replication unit + live tests
    data/WashingtonDC/       — DC OSM extract as Parquet
```

## Running the tests

```bash
uv sync                    # install dependencies
make test-e2e-init         # build the table from DC Parquet extract
make test-e2e-osc          # fetch + apply the next pending OSC
make test-e2e-osc-all      # fetch + apply all pending OSCs
```

`test-e2e-osc` is idempotent: run it repeatedly and each invocation
applies exactly one file. When the table is current, it prints
"already current" and exits.

## Data flow

### Init

```
Raw Parquet (type=node/way/relation)
  ├── nodes.py     → ST_Point per node              → nodes_with_geom
  ├── ways.py      → join refs → node geom          → ways_with_geom
  └── relations.py → join members → way geom        → relations_with_geom
      each → iceberg_prep.py (geom→WKB+bbox) → writeTo(per-type Iceberg table)
```

Each step is a `createOrReplaceTempView`. Spark plans the full DAG and materializes only at `writeTo`. Between types, the pipeline re-binds from the materialized Iceberg table so downstream stages read storage rather than re-executing upstream views.

### Incremental update (per OSC file)

```
OSC XML → parse → dedup (latest version per id+type)

  ├── nodes:     build geometry → MERGE into nodes table
  ├── ways:      node_to_ways index → dirty set → rebuild → MERGE into ways table
  └── relations: way_to_relations + node_to_relations + relation_to_relations
                 → dirty set → rebuild → MERGE into relations table

  Re-bind from Iceberg between types so each phase reads materialized data.
  Update index tables after each type. Stamp last-applied-osc-sequence per table.
```

## Dependencies

- `pyspark==3.5.0`
- `apache-sedona==1.9.0`
- `osmium>=4.0.0`
- `boto3>=1.35.47` (for S3/Glue catalog)
- `requests>=2.28.0`

JARs (auto-cached at `~/.cache/kryptosm/jars/`):

- `sedona-spark-shaded-3.5_2.12-1.9.0.jar`
- `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`
- `iceberg-aws-bundle-1.6.1.jar`

## License

Apache 2.0
