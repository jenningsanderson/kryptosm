## Geometry Hygiene Pipeline

Applied at the WKB serialization boundary — every geometry, every type.

```python
# iceberg_prep.py — pipeline order is deliberate
expr = ST_Force_2D(geom)               # 1. strip Z coordinates
expr = ST_MakeValid(expr)              # 2. fix topology before snapping
expr = ST_ReducePrecision(expr, 7)     # 3. snap to 1.1 cm grid (7 decimals)
expr = ST_MakeValid(expr)              # 4. fix topology after snap
expr = ST_ForcePolygonCCW(expr)        # 5. canonical winding order
```

#### Why this exact order?

| Step | Rationale |
|---|---|
| `Force2D` first | Strips Z before any geometric op; avoids 3D/2D type mismatches |
| `MakeValid` before snap | `GeometryPrecisionReducer` throws on invalid input — must fix first |
| `ReducePrecision` | 7 decimals ≈ 1.1 cm; idempotent for nodes, corrects join-artifact drift in ways/relations |
| `MakeValid` again | Snapping can collapse near-coincident vertices, occasionally re-introducing invalidity |
| `ForcePolygonCCW` last | Canonical OGC/RFC 7946 winding; no-op on lines, idempotent on already-CCW polygons |

**Oversized relations:** WKB > 30 MB gets `ST_SimplifyPreserveTopology(geom, 1e-6)` — ~10 cm tolerance, imperceptible at country scale. Prevents Parquet page-size issues on continent-sized boundaries.

Note:
The two MakeValid calls are not wasted work: on already-valid input both are short-circuit idempotent. The cost is only paid for geometries that were actually invalid — which does happen for relation geometries built from self-intersecting member ways.
