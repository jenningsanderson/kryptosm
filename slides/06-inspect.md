## Inspect Changes

Diff any two Iceberg snapshots to see what changed.

```python
snapshots = list_snapshots(spark, TABLE)
inspect_snapshots(spark, TABLE, "./output")
```

Produces:

- **`.geojson` files** — added / modified / deleted features with `@valid_since` and `@valid_until` timestamps
- **`inspector.html`** — MapLibre GL JS timeline viewer for geometry and tag diffs

#### What the diff captures

| Change type | Detected |
|---|---|
| Geometry moved / reshaped | Yes |
| Tags added / changed / removed | Yes |
| Feature added | Yes |
| Feature deleted | Yes |

Note:
The inspector HTML is fully self-contained — it embeds the GeoJSON inline so it works offline and can be shared as a single file.
