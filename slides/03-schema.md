## Table Schema

One table, partitioned by `type` (`node` | `way` | `relation`):

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGINT` | OSM element ID |
| `type` | `STRING` | Partition key |
| `version` | `BIGINT` | OSM version |
| `timestamp` | `TIMESTAMP` | Last edit |
| `tags` | `MAP<STRING,STRING>` | Key-value tags |
| `geometry` | `BINARY` | WKB-encoded |
| `bbox` | `STRUCT<xmin,xmax,ymin,ymax>` | Bounding box |
| `latest_ts` | `TIMESTAMP` | Max ts across feature + deps |

Plus two index tables: **`node_to_ways`** and **`way_to_relations`** — used to find dirty dependents during incremental updates.

Note:
lat/lon and refs/members columns are also present for nodes and ways respectively. The geometry column stores WKB so Spark can write it as binary without a custom UDT.
