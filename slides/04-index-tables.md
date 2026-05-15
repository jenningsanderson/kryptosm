## Index Tables

Four reverse-index tables power O(dirty) dirty-set computation.

| Table | Columns | Used for |
|---|---|---|
| `node_to_ways` | `node_id, way_id` | When a node moves → find all ways to rebuild |
| `way_to_relations` | `way_id, relation_id` | When a way changes → find all relations to rebuild |
| `node_to_relations` | `node_id, relation_id` | Relations that directly reference a changed node |
| `relation_to_relations` | `child_relation_id, parent_relation_id` | Sub-relation membership edges |

#### Why not just JOIN the full table?

Without indexes, finding which ways reference a changed node requires scanning every way's `refs` array — **O(all ways)** on every OSC apply.

With `node_to_ways`, it's a keyed lookup:

```sql
SELECT way_id FROM node_to_ways
WHERE node_id IN (SELECT id FROM dirty_nodes)
```

On a planet-scale dataset with ~1B ways, this is the difference between **seconds and hours** per OSC file.

Note:
Index tables are maintained incrementally: after each OSC apply, only rows for dirty features are refreshed (delete + re-insert). The full index never needs to be rebuilt after init.
