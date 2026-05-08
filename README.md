# kryptosm

Turn OpenStreetMap data into a single Apache Iceberg table, kept up to date
with incremental OSC change files.

Built on PySpark + Apache Sedona for geometry construction and Apache Iceberg
for versioned, time-travel-capable table storage.

## What it does

1. **Initial load** — reads an OSM Parquet extract (nodes, ways, relations),
   builds geometries with Sedona SQL functions (`ST_Point`, `ST_LineString`,
   `ST_Polygon`, `ST_MultiPolygon`), and writes everything to a single
   Iceberg table partitioned by `type`.

2. **Incremental update** — fetches OSC change files from a Geofabrik
   replication feed, computes the dependency-aware "dirty set" (if a node
   moves, every way referencing it and every relation referencing those ways
   gets its geometry rebuilt), and MERGEs the changes into the table. Each
   OSC file produces its own Iceberg snapshot, so the table's history steps
   through every change.

3. **Inspection** — compares Iceberg snapshots to produce GeoJSON +
   interactive HTML maps showing what changed: geometry diffs, tag diffs,
   added/modified/deleted features with `@valid_since` / `@valid_until`
   timestamps.

## Tenets

- **Everything is SQL.** Business logic lives in SQL strings that build temp
  views. No pandas, no Python UDFs. A reader should be able to scan a
  function's SQL and understand the transformation.

- **No CLI.** This is a library. The caller owns the Spark session — a
  cloud deployment (EMR, Glue, Databricks) provides its own. The E2E tests
  *are* the sample scripts; a production cron job looks nearly identical.

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

## Table schema

One table, partitioned by `type` (`node` | `way` | `relation`):

| Column | Type | Description |
|--------|------|-------------|
| `id` | `BIGINT` | OSM element ID |
| `type` | `STRING` | Partition key |
| `version` | `BIGINT` | OSM version number |
| `timestamp` | `TIMESTAMP` | Last edit timestamp |
| `changeset` | `BIGINT` | OSM changeset ID |
| `uid` | `BIGINT` | Editor user ID |
| `user` | `STRING` | Editor username |
| `tags` | `MAP<STRING, STRING>` | Key-value tag pairs |
| `lat` | `DOUBLE` | Latitude (nodes only) |
| `lon` | `DOUBLE` | Longitude (nodes only) |
| `refs` | `ARRAY<BIGINT>` | Ordered node references (ways only) |
| `members` | `ARRAY<STRUCT<type, ref, role>>` | Member references (relations only) |
| `latest_ts` | `TIMESTAMP` | Max timestamp across feature + dependencies |
| `geometry` | `BINARY` | WKB-encoded geometry |
| `bbox` | `STRUCT<xmin, xmax, ymin, ymax: FLOAT>` | Bounding box |

Two sibling index tables in the same database:

- **`node_to_ways`** (`node_id`, `way_id`) — which ways reference each node
- **`way_to_relations`** (`way_id`, `relation_id`) — which relations reference each way

## How it works

### Init

```python
from kryptosm import *

# Caller provides the Spark session (cloud or local).
spark = ...

TABLE = "hadoop_catalog.dc.osm"
N2W   = "hadoop_catalog.dc.node_to_ways"
W2R   = "hadoop_catalog.dc.way_to_relations"

create_iceberg_table(spark, TABLE)
create_index_tables(spark, N2W, W2R)

# Read raw Parquet
spark.read.parquet("s3://bucket/dc.parquet/type=node").createOrReplaceTempView("input_nodes")
spark.read.parquet("s3://bucket/dc.parquet/type=way").createOrReplaceTempView("input_ways_raw")
flatten_way_refs(spark, "input_ways_raw", "input_ways")
spark.read.parquet("s3://bucket/dc.parquet/type=relation").createOrReplaceTempView("input_relations")

# Nodes
build_node_geometry(spark, "input_nodes", "nodes_with_geom")
prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
spark.sql("SELECT * FROM nodes_final").writeTo(TABLE).using("iceberg").append()
load_with_geom(spark, TABLE, "node", "nodes_with_geom")

# Ways
build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_lines")
build_ways_geometry_from_linestring(spark, "ways_lines", "ways_with_geom")
prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
spark.sql("SELECT * FROM ways_final").writeTo(TABLE).using("iceberg").append()
load_with_geom(spark, TABLE, "way", "ways_with_geom")
populate_node_to_ways(spark, TABLE, N2W)

# Relations
relations_need_geometry(spark, "input_relations", "rels_need_geom")
construct_multipolygon(spark, "rels_need_geom", "ways_with_geom", "rels_geom")
relation_merge_geometry_data(spark, "input_relations", "rels_geom", "rels_with_geom")
prepare_for_iceberg(spark, "rels_with_geom", "relation", "rels_final")
spark.sql("SELECT * FROM rels_final").writeTo(TABLE).using("iceberg").append()
populate_way_to_relations(spark, TABLE, W2R)
```

### Incremental update

```python
# Apply the next pending OSC (fetch + apply are separate)
path = next_osc_path(spark, TABLE, "/tmp/osc", base_url=GEOFABRIK_URL)
if path:
    apply_osc(spark, TABLE, path, N2W, W2R)

# Or loop until current
while path := next_osc_path(spark, TABLE, "/tmp/osc", base_url=GEOFABRIK_URL):
    apply_osc(spark, TABLE, path, N2W, W2R)
```

### Inspect changes

```python
snapshots = list_snapshots(spark, TABLE)
inspect_snapshots(spark, TABLE, "./output")
# Produces .geojson files + inspector.html (MapLibre GL JS timeline viewer)
```

## Repository layout

```
kryptosm/
    __init__.py          — public API re-exports
    iceberg.py           — CREATE / MERGE / DELETE, index table operations
    osc.py               — OSC parsing, fetch (next_osc_path), apply (apply_osc)
    replication.py       — Geofabrik OSC download via pyosmium
    inspect.py           — snapshot diff → GeoJSON + HTML map viewer
    geometry/
        nodes.py         — ST_Point per node
        ways.py          — LineString / Polygon per way
        relations.py     — MultiPolygon / MultiLineString per relation
        osc_apply.py     — dirty-set computation using index tables
        iceberg_prep.py  — geom → WKB + bbox for Iceberg write

tests/
    __init__.py              — Spark session factory for tests
    test_e2e_init.py         — build table from Parquet
    test_e2e_osc.py          — fetch + apply the next OSC (idempotent)
    test_e2e_osc_all.py      — fetch + apply all pending OSCs
    test_inspect.py          — snapshot inspector
    test_e2e_nodes.py        — stage 1: nodes only
    test_e2e_ways.py         — stage 2: ways only
    test_e2e_relations.py    — stage 3: relations only
    test_replication.py      — replication unit + live tests
    data/WashingtonDC/       — DC OSM extract as Parquet
```

## Running the tests

```bash
uv sync                    # install dependencies
make test-e2e-init         # build the table from DC Parquet extract
make test-e2e-osc          # fetch + apply the next pending OSC
make test-e2e-osc-all      # fetch + apply all pending OSCs
make test-inspect          # run the snapshot inspector
```

`test-e2e-osc` is idempotent: run it repeatedly and each invocation
applies exactly one file. When the table is current, it prints
"already current" and exits.

## Data flow

### Init

```
Raw Parquet (type=node/way/relation)
  ├── nodes.py     → ST_Point per node            → nodes_with_geom
  ├── ways.py      → join refs → node geom        → ways_with_geom
  └── relations.py → join members → way geom       → relations_with_geom
      each → iceberg_prep.py (geom→WKB+bbox) → writeTo(iceberg)
```

### Incremental update (per OSC file)

```
OSC XML → parse → dedup (latest version per id+type)

  ├── nodes:     build geometry → MERGE
  ├── ways:      node_to_ways index → dirty set → build geometry → MERGE
  └── relations: way_to_relations index → dirty set → build geometry → MERGE

  Re-bind from Iceberg between types so each phase reads materialized data.
  Update index tables after each type.
```

## Dependencies

- `pyspark==3.5.0`
- `apache-sedona==1.8.1`
- `osmium>=4.0.0`
- `boto3>=1.35.47` (for S3/Glue catalog)
- `requests>=2.28.0`

JARs (auto-cached at `~/.cache/kryptosm/jars/`):
- `sedona-spark-shaded-3.5_2.12-1.8.1.jar`
- `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`
- `iceberg-aws-bundle-1.6.1.jar`

## License

Apache 2.0
