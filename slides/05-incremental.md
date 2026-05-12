## Incremental Updates

Each OSC file produces its own Iceberg snapshot.

```python
# Apply one file at a time
path = next_osc_path(spark, TABLE, "/tmp/osc", base_url=GEOFABRIK_URL)
if path:
    apply_osc(spark, TABLE, path, N2W, W2R)

# Or loop until current
while path := next_osc_path(spark, TABLE, "/tmp/osc", base_url=GEOFABRIK_URL):
    apply_osc(spark, TABLE, path, N2W, W2R)
```

#### Dirty-set computation

```
OSC XML → parse → dedup (latest version per id+type)

  ├── nodes:     build geometry → MERGE
  ├── ways:      node_to_ways   → dirty ways  → rebuild → MERGE
  └── relations: way_to_relations → dirty rels → rebuild → MERGE
```

`node_to_ways` and `way_to_relations` make dirty-set computation **O(dirty features)**, not O(all features).

Note:
next_osc_path fetches the file if not already cached locally and returns its path. The table property last-applied-osc-sequence tracks progress so a crash mid-batch resumes cleanly.
