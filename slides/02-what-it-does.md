## What It Does

<div class="two-col">
<div markdown="1">

#### Three operations

1. **Init** — read an OSM Parquet extract, build geometries with Sedona SQL, write to three per-type Iceberg tables with type-specific tuning

2. **Incremental update** — fetch OSC change files from Geofabrik, compute the dependency-aware dirty set, MERGE into the per-type tables — one Iceberg snapshot per OSC file, per table

3. **Inspect** — diff snapshots to produce GeoJSON + an interactive HTML map showing geometry and tag changes over time

</div>
<div markdown="1">

#### Stack

| Layer | Tool |
|---|---|
| Geometry | Apache Sedona |
| Storage | Apache Iceberg |
| Engine | PySpark 3.5 |
| OSC parsing | pyosmium |
| Replication | Geofabrik feeds |

</div>
</div>

Note:
Everything is a library — no CLI. The caller owns the Spark session. Cloud deployments (EMR, Glue, Databricks) provide their own session.
