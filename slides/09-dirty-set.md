## Dirty-Set Propagation

When a node moves, every way containing it — and every relation containing those ways — needs geometry rebuilt.

```
OSC input: node 42 moved, way 99 tag changed

dirty_nodes    = {42}                    (from OSC directly)
dirty_ways     = {99}                    (from OSC)
             ∪ {ways where node 42 ∈ refs}   (via node_to_ways index)

dirty_relations = {relations where dirty way ∈ members}  (via way_to_relations)
              ∪ {relations where node 42 ∈ members}       (via node_to_relations)
              ∪ {parent relations of OSC-touched rels}    (via relation_to_relations)
```

#### Implementation: FULL OUTER JOIN, not a scan

```sql
-- all_dirty_ways (osc_apply.py)
SELECT COALESCE(a.id, b.id) AS id, ...
FROM osc_way_upserts a
FULL OUTER JOIN (
    SELECT * FROM ways_table
    WHERE id IN (
        SELECT way_id FROM node_to_ways
        WHERE node_id IN (SELECT id FROM dirty_nodes)
    )
) b ON a.id = b.id
```

Both sides of the join contain only dirty features. The full ways table is **never scanned** — Iceberg uses bloom filters on `id` to skip non-matching files.

`additional_changesets` carry-forwards across applies: the OSC-side array is unioned with the base-row array so a re-MERGEd row preserves every previously-stored changeset.

Note:
Relation-to-relation widening is single-level only — parent relations of OSC-touched relations get rebuilt, but grandparents don't. In practice OSM hierarchies are shallow (1–2 levels) and deeper changes catch up on the next apply that touches the chain.
