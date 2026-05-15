## Database Schema

Three separate Iceberg tables — each tuned for its shape and row count.

<div class="two-col">
<div markdown="1">

#### `nodes` (~10B rows on planet)

| Column | Type |
|---|---|
| `id` | `BIGINT` |
| `version`, `timestamp`, `changeset` | OSM metadata |
| `uid`, `user`, `tags` | OSM metadata |
| `lat`, `lon` | `DOUBLE` |
| `latest_ts` | `TIMESTAMP` |
| `additional_changesets` | `ARRAY<BIGINT>` |
| `geometry` | `BINARY` (WKB ST_Point) |

No `bbox` — lat/lon already define the footprint.

</div>
<div markdown="1">

#### `ways` (~1.2B rows) and `relations` (~12M rows)

Type-specific columns differ:

| Column | `ways` | `relations` |
|---|---|---|
| type-specific | `refs ARRAY<BIGINT>` | `members ARRAY<STRUCT<type,ref,role>>` |
| `geometry` | LineString or Polygon | MultiPolygon, MultiLineString, or NULL |
| `bbox` | `STRUCT<xmin,xmax,ymin,ymax>` | same |

`latest_ts` = MAX timestamp across the feature and all its members.

</div>
</div>

Note:
Per-type tables enable type-specific Iceberg tuning: 8 MB bloom filter budget for nodes (huge keyspace, frequent joins during way rebuilds), 1 MB for ways, 256 KB for relations (few but wide rows with large members arrays). A single unified table forces one-size-fits-all settings that hurt every type.
