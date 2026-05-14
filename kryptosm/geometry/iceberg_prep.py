"""
Convert a `geom` view into the layout the Iceberg OSM table expects.

This is the last step of the pipeline. It serializes geometries to WKB,
adds the bbox struct, pins the partition column (`type`), and repartitions
for parallel write.

Hygiene applied at the WKB boundary (see ``_hygienic_geom``):
    * ``ST_Force_2D``          — strip any Z value
    * ``ST_ReducePrecision``   — snap to a fixed decimal grid
    * ``ST_MakeValid``         — repair topology (self-intersections,
                                 sliver artifacts from ST_Difference,
                                 ring orientation problems)
    * ``ST_ForcePolygonCCW``   — canonical OGC / RFC 7946 winding

The pipeline order is deliberate; see ``_hygienic_geom`` for rationale.
"""

from pyspark.sql import SparkSession

# Relation WKB above this size gets simplified before write so the BINARY
# column doesn't blow up. ~30 MB is well below Parquet page-size pain points.
MAXIMUM_RELATION_GEOMETRY_SIZE = 30_000_000

# ST_SimplifyPreserveTopology tolerance for oversized relations. ~0.000001
# degrees is ~10 cm at the equator - imperceptible at country scale.
HUGE_GEOMETRY_SIMPLIFICATION_FACTOR = 0.000001

# Decimal-degree precision applied to every geometry at the WKB boundary.
# 7 decimals ≈ 1.1 cm at the equator — finer than OSM source precision and
# matches the per-node ST_ReducePrecision applied in nodes.py, so this is
# idempotent for nodes and additionally protects ways/relations whose
# coordinates are derived through joins, unions, and overlay operations.
WKB_PRECISION_DECIMALS = 7


def _hygienic_geom(geom_expr: str = "geom") -> str:
    """SQL fragment producing a 2D, valid, precision-snapped, CCW-wound geometry.

    Pipeline order is deliberate:

      1. ``ST_Force_2D``         — strip any Z value (cheap, harmless on 2D).
      2. ``ST_MakeValid`` (1)    — fix any topology issues from the source
                                   pipeline (self-intersecting closed ways,
                                   sliver artifacts from ST_Difference, etc.).
                                   Must run BEFORE ReducePrecision because
                                   GeometryPrecisionReducer throws
                                   "Reduction failed, possible invalid input"
                                   on invalid geometries.
      3. ``ST_ReducePrecision``  — snap coordinates to a fixed grid. Cheap and
                                   reliable on the now-valid input.
      4. ``ST_MakeValid`` (2)    — defensive: snap can collapse near-coincident
                                   vertices into duplicates, occasionally
                                   re-introducing invalidity. Idempotent and
                                   nearly free on already-valid inputs.
      5. ``ST_ForcePolygonCCW``  — canonical OGC / RFC 7946 winding (CCW
                                   exterior, CW interiors). No-op on
                                   non-polygon geometries, idempotent on
                                   already-CCW polygons.

    The two MakeValid calls add cost only on geometries that were actually
    invalid; on valid input both are short-circuit idempotent.
    """
    expr = f"ST_Force_2D({geom_expr})"
    expr = f"ST_MakeValid({expr})"
    expr = f"ST_ReducePrecision({expr}, {WKB_PRECISION_DECIMALS})"
    expr = f"ST_MakeValid({expr})"
    expr = f"ST_ForcePolygonCCW({expr})"
    return expr


def _geometry_expr(osm_type: str) -> str:
    """SQL expression that serializes `geom` to WKB, simplifying huge relations."""
    clean = _hygienic_geom("geom")
    if osm_type != "relation":
        return f"ST_AsBinary({clean})"
    return (
        f"IF (LENGTH(ST_AsBinary({clean})) < {MAXIMUM_RELATION_GEOMETRY_SIZE}, "
        f"    ST_AsBinary({clean}), "
        f"    ST_AsBinary(ST_SimplifyPreserveTopology({clean}, {HUGE_GEOMETRY_SIMPLIFICATION_FACTOR})))"
    )


def prepare_for_iceberg(
    spark: SparkSession,
    data_view: str,
    osm_type: str,
    result_view: str,
):
    """
    Project a geom-bearing view into the Iceberg table's column layout.

    For nodes and ways, rows with NULL geom are dropped (they failed
    geometry construction). For relations, NULL geom is kept — many
    relation types intentionally have no geometry.

    Relations use a UNION ALL split instead of CASE WHEN because Sedona
    1.8.x can produce JTS-null-in-UDT objects that pass ``IS NOT NULL``
    but NPE when any geometry function touches them. The WHERE filter in
    the first branch runs at the scan level, before expression evaluation.
    """
    _columns = f"""
            id,
            CAST('{osm_type}' AS STRING)        AS type,
            CAST(version AS BIGINT)             AS version,
            CAST(timestamp AS TIMESTAMP)        AS timestamp,
            CAST(changeset AS BIGINT)           AS changeset,
            CAST(uid AS BIGINT)                 AS uid,
            user,
            tags,
            lat,
            lon,
            refs,
            members,
            CAST(latest_ts AS TIMESTAMP)        AS latest_ts"""

    if osm_type != "relation":
        spark.sql(f"""
            SELECT
                {_columns},
                ST_AsBinary({_hygienic_geom("geom")})  AS geometry,
                STRUCT(
                    CAST(ST_XMin(geom) AS FLOAT) AS xmin,
                    CAST(ST_XMax(geom) AS FLOAT) AS xmax,
                    CAST(ST_YMin(geom) AS FLOAT) AS ymin,
                    CAST(ST_YMax(geom) AS FLOAT) AS ymax
                )                                   AS bbox
            FROM {data_view}
            WHERE geom IS NOT NULL
        """).createOrReplaceTempView(result_view)
    else:
        spark.sql(f"""
            SELECT
                {_columns},
                {_geometry_expr(osm_type)}          AS geometry,
                STRUCT(
                    CAST(ST_XMin(geom) AS FLOAT) AS xmin,
                    CAST(ST_XMax(geom) AS FLOAT) AS xmax,
                    CAST(ST_YMin(geom) AS FLOAT) AS ymin,
                    CAST(ST_YMax(geom) AS FLOAT) AS ymax
                )                                   AS bbox
            FROM {data_view}
            WHERE geom IS NOT NULL
            UNION ALL
            SELECT
                {_columns},
                CAST(NULL AS BINARY)                AS geometry,
                CAST(NULL AS
                    STRUCT<xmin: FLOAT, xmax: FLOAT,
                           ymin: FLOAT, ymax: FLOAT>
                )                                   AS bbox
            FROM {data_view}
            WHERE geom IS NULL
        """).createOrReplaceTempView(result_view)
