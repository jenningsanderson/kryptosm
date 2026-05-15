---
class: hero
---
# kryptosm

## OpenStreetMap → Apache Iceberg

Three per-type tables. Every node, way, and relation. Kept current with incremental OSC change files.

Note:
PySpark + Apache Sedona for geometry construction. Apache Iceberg for versioned, time-travel-capable table storage. Runs on Glue, EMR, Databricks, or local Spark.
