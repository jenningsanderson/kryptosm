## Inspect Changes

Diff any two Iceberg snapshots to see exactly what changed.

```python
# List available snapshots for a table
snapshots = list_snapshots(spark, RELATIONS)

# Diff all consecutive snapshots → GeoJSON files + HTML viewer
inspect_snapshots(spark, RELATIONS, "relation", "./output")
```

Produces for each consecutive snapshot pair:

- **`.geojson`** — GeoJSON FeatureCollection; each feature carries:
  - `@change`: `added` / `modified` / `deleted`
  - `@geometry_changed`: boolean
  - `@valid_since` / `@valid_until`: ISO timestamps
  - `@tags_added`, `@tags_changed`, `@tags_removed`: maps of diffs
- **`inspector_relation.html`** — embedded MapLibre GL JS timeline viewer

#### How the diff works

```sql
-- Full outer join on id between two Iceberg time-travel timestamps
SELECT a.id, b.id, a.geometry, b.geometry, a.tags, b.tags ...
FROM relations TIMESTAMP AS OF snap_before
FULL OUTER JOIN relations TIMESTAMP AS OF snap_after ON a.id = b.id
-- a IS NULL → added, b IS NULL → deleted, both present → modified
```

The HTML viewer is fully self-contained — GeoJSON embedded inline, works offline, shareable as a single file.

Note:
Time-travel is native to Iceberg — no extra infrastructure needed. Each OSC apply produces one snapshot per table, so you can diff "before OSC #1234" vs "after OSC #1234" to isolate exactly what that change file touched.
