## Incremental Updates

Each OSC file produces one new Iceberg snapshot per table.

```python
# Fetch the next pending OSC and apply it (idempotent)
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
    apply_osc(spark, path, NODES, WAYS, RELATIONS, N2W, W2R, N2R, R2R, ARCHIVE)
```

#### OSC dedup — multiple versions in one file

An OSC file can contain node N at version 5 and version 6. `osc_dedup` keeps the highest version and captures the loser's changeset into `additional_changesets` so attribution isn't lost.

#### Resumable per-table applies

Each table carries `last-applied-osc-sequence`. A mid-flight crash leaves some tables at `seq − 1`. The next call reads each table's stamp independently and skips sections that are already done — no duplicate work, no data loss.

Note:
`next_osc_path` bootstraps from `MAX(timestamp)` across all nodes when the table has no sequence stamp, so you don't need to manually find the right replication sequence after the first init.
