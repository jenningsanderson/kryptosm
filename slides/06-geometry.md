## Geometry Construction

Each OSM type needs a different strategy.

<div class="two-col">
<div markdown="1">

#### Nodes — trivial

```sql
ST_ReducePrecision(ST_Point(lon, lat), 7)
```

Drop rows where lat/lon is NULL (deleted nodes have no coordinates).

#### Ways — collect nodes in order

```sql
-- posexplode + struct sort preserves node order
ST_LineFromMultiPoint(
  ST_Collect(node_geom SORTED BY position)
)
```

**Closed way → Polygon** when all three hold:
- `ST_IsClosed(geom) AND ST_NumPoints > 3`
- area-tagging key present (`building`, `landuse`, `natural`, `amenity`, `waterway`… 22 families)
- `tags['area'] != 'no'`

</div>
<div markdown="1">

#### Relations — multipolygon assembly

```sql
-- outer rings minus holes
ST_BuildArea(outer_ways_union)        AS outer_poly
ST_Difference(
  outer_poly,
  ST_BuildArea(inner_ways_union)
)                                      AS geom
```

Handled relation types: `multipolygon`, `boundary`, `building`, `route`, `waterway`, `site`

Line types (route, waterway) → `ST_Union` of member ways.  
Site relations → `GeometryCollection` of ways + nodes.  
Sub-relations already have built geometries, unioned as complete pieces.

Non-geometric relations (e.g. `restriction`, `public_transport`) get `geometry = NULL` — kept in the table, not dropped.

</div>
</div>

Note:
The area tag list is sourced from the OSM wiki. The `area:*` prefix catch-all (e.g. area:highway) is also checked via a map_keys + aggregate expression. Way geometry construction uses a LEFT OUTER JOIN on nodes so ways with missing nodes get NULL geometry rather than crashing.
