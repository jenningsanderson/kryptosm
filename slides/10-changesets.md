## Change Attribution

Two derived columns track provenance that OSM's raw data doesn't surface.

#### `latest_ts` — "how recently was this feature affected?"

| Type | Value |
|---|---|
| `nodes` | own `timestamp` (nodes have no children) |
| `ways` | `MAX(way.timestamp, MAX(member_nodes.timestamp))` |
| `relations` | `MAX(self, MAX(member_ways.timestamp), MAX(member_nodes.timestamp))` |

A way last edited in 2015 but containing a node moved in 2024 gets `latest_ts = 2024`.  
Enables "show me features touched since date X" without geometry joins.

#### `additional_changesets` — "who else shaped this feature?"

| Type | Content |
|---|---|
| `nodes` | always `[]` — own changeset is in the `changeset` column |
| `ways` | member-node changesets **strictly newer** than `way.changeset` |
| `relations` | member-way/node changesets **strictly newer** than `relation.changeset` |

Also accumulates OSC-dedup losers (collapsed versions within one file) and carry-forwards across applies so a re-MERGEd row doesn't lose previously-stored changesets.

**Why "strictly newer"?** The `> self.changeset` filter bounds the array from growing unboundedly — it only records child edits that postdate the feature's own last explicit edit.

Note:
Both columns are computed entirely in the same SQL that builds geometry — no extra scans. They're derived at write time, not stored in OSM source data.
