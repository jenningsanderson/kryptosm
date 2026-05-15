"""
Convert a `geom` view into the layout each per-type Krypton table expects.

This is the last step of the pipeline. It serializes geometries to WKB, adds
the bbox struct (ways/relations only — nodes carry lat/lon directly), and
projects only the columns relevant to the target type:

- nodes:     id, version, timestamp, changeset, uid, user, tags, lat, lon,
             latest_ts, additional_changesets, geometry
- ways:      id, version, timestamp, changeset, uid, user, tags, refs,
             latest_ts, additional_changesets, geometry, bbox
- relations: id, version, timestamp, changeset, uid, user, tags, members,
             latest_ts, additional_changesets, geometry, bbox

Hygiene applied at the WKB boundary (see ``_hygienic_geom``):
    * ``ST_Force_2D``          — strip any Z value
    * ``ST_ReducePrecision``   — snap to a fixed decimal grid
    * ``ST_MakeValid``         — repair topology (self-intersections,
                                 sliver artifacts from ST_Difference,
                                 ring orientation problems)
    * ``ST_ForcePolygonCCW``   — canonical OGC / RFC 7946 winding

The pipeline order is deliberate; see ``_hygienic_geom`` for rationale.
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# Per-type column projections
# ---------------------------------------------------------------------------

# Common columns every per-type table carries (excluding `geometry` and `bbox`,
# which are emitted separately because they involve geometry-engine calls).
_COMMON_COLUMNS = """
            id,
            CAST(version AS BIGINT)             AS version,
            CAST(timestamp AS TIMESTAMP)        AS timestamp,
            CAST(changeset AS BIGINT)           AS changeset,
            CAST(uid AS BIGINT)                 AS uid,
            user,
            tags,
            CAST(latest_ts AS TIMESTAMP)        AS latest_ts,
            COALESCE(additional_changesets, CAST(array() AS ARRAY<BIGINT>))
                                                AS additional_changesets"""

_NODE_TYPE_COLUMNS = """
            lat,
            lon"""

_WAY_TYPE_COLUMNS = """
            refs"""

_RELATION_TYPE_COLUMNS = """
            members"""

_BBOX_STRUCT = """STRUCT(
                CAST(ST_XMin(geom) AS FLOAT) AS xmin,
                CAST(ST_XMax(geom) AS FLOAT) AS xmax,
                CAST(ST_YMin(geom) AS FLOAT) AS ymin,
                CAST(ST_YMax(geom) AS FLOAT) AS ymax
            )"""


def prepare_for_iceberg(
    spark: SparkSession,
    data_view: str,
    osm_type: str,
    result_view: str,
):
    """
    Project a geom-bearing view into the per-type Krypton table column layout.

    For nodes and ways, rows with NULL geom are dropped (they failed
    geometry construction). For relations, NULL geom is kept — many
    relation types intentionally have no geometry.

    Relations use a UNION ALL split instead of CASE WHEN because Sedona
    1.8.x can produce JTS-null-in-UDT objects that pass ``IS NOT NULL``
    but NPE when any geometry function touches them. The WHERE filter in
    the first branch runs at the scan level, before expression evaluation.

    A defensive ``ROW_NUMBER() = 1`` filter guarantees uniqueness by ``id``
    at the MERGE-source boundary. Upstream stages should already be unique
    per id, but if the table picked up duplicates from a previous failed
    run, this filter prevents ``MERGE_CARDINALITY_VIOLATION`` errors. We
    keep the row with the highest version, breaking ties by latest_ts.
    """
    logger.info("prepare_for_iceberg: %s (type=%s) → %s", data_view, osm_type, result_view)
    if osm_type == "node":
        # Nodes: lat/lon, no bbox. NULL geom rows dropped.
        spark.sql(f"""
            WITH _ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY id
                           ORDER BY version DESC NULLS LAST,
                                    latest_ts DESC NULLS LAST
                       ) AS _rn
                FROM {data_view}
                WHERE geom IS NOT NULL
            )
            SELECT
                {_COMMON_COLUMNS},
                {_NODE_TYPE_COLUMNS},
                ST_AsBinary({_hygienic_geom("geom")})  AS geometry
            FROM _ranked
            WHERE _rn = 1
        """).createOrReplaceTempView(result_view)
    elif osm_type == "way":
        # Ways: refs, bbox. NULL geom rows dropped.
        spark.sql(f"""
            WITH _ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY id
                           ORDER BY version DESC NULLS LAST,
                                    latest_ts DESC NULLS LAST
                       ) AS _rn
                FROM {data_view}
                WHERE geom IS NOT NULL
            )
            SELECT
                {_COMMON_COLUMNS},
                {_WAY_TYPE_COLUMNS},
                ST_AsBinary({_hygienic_geom("geom")})  AS geometry,
                {_BBOX_STRUCT}                          AS bbox
            FROM _ranked
            WHERE _rn = 1
        """).createOrReplaceTempView(result_view)
    elif osm_type == "relation":
        # Relations: members, bbox. NULL geom kept (relations may have no geometry).
        spark.sql(f"""
            WITH _ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY id
                           ORDER BY version DESC NULLS LAST,
                                    latest_ts DESC NULLS LAST
                       ) AS _rn
                FROM {data_view}
            )
            SELECT
                {_COMMON_COLUMNS},
                {_RELATION_TYPE_COLUMNS},
                {_geometry_expr(osm_type)}              AS geometry,
                {_BBOX_STRUCT}                          AS bbox
            FROM _ranked
            WHERE _rn = 1 AND geom IS NOT NULL
            UNION ALL
            SELECT
                {_COMMON_COLUMNS},
                {_RELATION_TYPE_COLUMNS},
                CAST(NULL AS BINARY)                    AS geometry,
                CAST(NULL AS
                    STRUCT<xmin: FLOAT, xmax: FLOAT,
                           ymin: FLOAT, ymax: FLOAT>
                )                                       AS bbox
            FROM _ranked
            WHERE _rn = 1 AND geom IS NULL
        """).createOrReplaceTempView(result_view)
    else:
        raise ValueError(f"Unknown osm_type {osm_type!r}; expected node/way/relation")
