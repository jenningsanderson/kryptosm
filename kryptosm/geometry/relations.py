"""
Relation geometry: MultiPolygon or MultiLineString per OSM relation.

Relations bundle multiple ways (and sometimes nodes/relations) under a
single ID with role labels. The `tags['type']` decides what we build:

    multipolygon, boundary  -> MultiPolygon (outer ways minus inner ways)
    route, waterway         -> MultiLineString (all member ways unioned)

Anything else gets a NULL geometry and is still written to the table.
"""

from pyspark.sql import SparkSession


# Relation `tags['type']` values for which we build geometry.
GEOMETRY_RELATION_TYPES = ('multipolygon', 'boundary', 'route', 'waterway')

_POLYGON_TYPES = ('multipolygon', 'boundary')
_LINE_TYPES = ('route', 'waterway')


def _quote_csv(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


def relations_need_geometry(spark: SparkSession, relations_data: str, result_view: str):
    """
    Filter relations down to the types we know how to build geometry for.

    Input view (`relations_data`) columns:
        id, members, tags, timestamp (others ignored)

    Output view (`result_view`) columns:
        id, members, tags, latest_ts (TIMESTAMP)

    Why:
        - Other relation types (`route_master`, `restriction`, ...) still
          land in the Iceberg table later via `relation_merge_geometry_data`,
          but they get a NULL geometry. Filtering early keeps the heavy
          ST_BuildArea / ST_Union_Aggr work focused.
    """
    spark.sql(f"""
        SELECT id, members, tags,
               COALESCE(timestamp, current_timestamp()) AS latest_ts
        FROM {relations_data}
        WHERE tags['type'] IN ({_quote_csv(GEOMETRY_RELATION_TYPES)})
    """).createOrReplaceTempView(result_view)


def construct_multipolygon(
    spark: SparkSession, relations_data: str, ways_geometry: str, result_view: str
):
    """
    Build geometries for relations - polygon and line types in one shot.

    Input view (`relations_data`) columns:
        id, members (ARRAY<STRUCT<type, ref, role>>), tags, latest_ts
        (typically the output of `relations_need_geometry`)
    Input view (`ways_geometry`) columns:
        id, latest_ts, geom

    Output view (`result_view`) columns:
        id, latest_ts, geom (ST_MultiPolygon for polygon types,
                             ST_MultiLineString for line types)

    Why:
        - We posexplode `relations_data.members` exactly once into
          `_rel_member_ways(relation_id, relation_type, relation_latest_ts,
          way_id, role)`. All four downstream branches (outer / inner / line /
          combine) read from that one view instead of re-scanning + re-exploding
          `relations_data` each time.
        - Outer / inner are unioned then `ST_BuildArea`'d to handle the common
          case where multiple ways together form a single ring.
        - Inner polygons (holes) are subtracted with ST_Difference.
        - Members with empty / NULL `role` are normalized to 'outer' per OSM
          convention. Line types ignore role entirely.
        - Intermediate views are prefixed with `_` so they don't collide with
          caller-managed view names.
    """
    # ----- pre-explode members once -----------------------------------------
    # This is the only scan of `relations_data` in this function.
    spark.sql(f"""
        SELECT
            r.id                                  AS relation_id,
            r.tags['type']                        AS relation_type,
            r.latest_ts                           AS relation_latest_ts,
            m.member.ref                          AS way_id,
            COALESCE(NULLIF(m.member.role, ''), 'outer') AS role
        FROM {relations_data} r
        LATERAL VIEW posexplode(r.members) m AS pos, member
        WHERE m.member.type = 'way'
    """).createOrReplaceTempView("_rel_member_ways")

    polygon_types = _quote_csv(_POLYGON_TYPES)
    line_types = _quote_csv(_LINE_TYPES)

    # ----- polygon side: outer rings ----------------------------------------
    spark.sql(f"""
        SELECT
            rm.relation_id                          AS id,
            MAX(rm.relation_latest_ts)              AS latest_ts,
            ST_BuildArea(ST_Union_Aggr(lines.geom)) AS geom
        FROM (
            SELECT DISTINCT relation_id, relation_latest_ts, way_id
            FROM _rel_member_ways
            WHERE relation_type IN ({polygon_types}) AND role = 'outer'
        ) rm
        JOIN {ways_geometry} lines ON rm.way_id = lines.id
        WHERE lines.geom IS NOT NULL
        GROUP BY rm.relation_id
    """).createOrReplaceTempView("_outer_polygons")

    # ----- polygon side: inner rings (holes) --------------------------------
    spark.sql(f"""
        SELECT
            rm.relation_id                          AS id,
            MAX(rm.relation_latest_ts)              AS latest_ts,
            ST_BuildArea(ST_Union_Aggr(lines.geom)) AS geom
        FROM (
            SELECT DISTINCT relation_id, relation_latest_ts, way_id
            FROM _rel_member_ways
            WHERE relation_type IN ({polygon_types}) AND role = 'inner'
        ) rm
        JOIN {ways_geometry} lines ON rm.way_id = lines.id
        WHERE lines.geom IS NOT NULL
        GROUP BY rm.relation_id
    """).createOrReplaceTempView("_inner_polygons")

    # ----- polygon relations: outer minus inner, validated ------------------
    # FULL OUTER JOIN keeps relations that have only outers OR only inners.
    spark.sql("""
        SELECT
            COALESCE(o.id, i.id)                   AS id,
            COALESCE(o.latest_ts, i.latest_ts)     AS latest_ts,
            ST_ReducePrecision(
                ST_MakeValid(
                    CASE
                        WHEN o.geom IS NOT NULL AND i.geom IS NOT NULL
                            THEN ST_Difference(o.geom, i.geom)
                        WHEN o.geom IS NOT NULL THEN o.geom
                        ELSE i.geom
                    END
                ),
                7
            ) AS geom
        FROM _outer_polygons o
        FULL OUTER JOIN _inner_polygons i ON o.id = i.id
    """).createOrReplaceTempView("_polygon_relations")

    # ----- line relations: union of all member-way geometries ---------------
    spark.sql(f"""
        SELECT
            rm.relation_id                          AS id,
            MAX(rm.relation_latest_ts)              AS latest_ts,
            ST_Union_Aggr(lines.geom)               AS geom
        FROM (
            SELECT DISTINCT relation_id, relation_latest_ts, way_id
            FROM _rel_member_ways
            WHERE relation_type IN ({line_types})
        ) rm
        JOIN {ways_geometry} lines ON rm.way_id = lines.id
        WHERE lines.geom IS NOT NULL
        GROUP BY rm.relation_id
    """).createOrReplaceTempView("_line_relations")

    spark.sql("""
        SELECT id, latest_ts, geom FROM _polygon_relations
        UNION ALL
        SELECT id, latest_ts, geom FROM _line_relations
    """).createOrReplaceTempView(result_view)


def relation_merge_geometry_data(
    spark: SparkSession, relations_data: str, geometry_only_data: str, result_view: str
):
    """
    Left-join all relations with their built geometry.

    Input view (`relations_data`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon, members
        (the FULL relation set, not just the geometry-bearing ones)
    Input view (`geometry_only_data`) columns:
        id, latest_ts, geom (output of `construct_multipolygon`)

    Output view (`result_view`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon,
        refs (NULL ARRAY<BIGINT>), members, latest_ts, geom (or NULL)

    Why:
        - Relations whose `tags['type']` isn't in GEOMETRY_RELATION_TYPES
          still belong in the table - they just carry NULL geometry.
        - Empty geometries (e.g. ST_Difference produced no area) collapse
          to NULL so consumers don't see `EMPTY` shapes.
    """
    spark.sql(f"""
        SELECT
            a.id,
            CAST(a.version AS BIGINT)            AS version,
            CAST(a.timestamp AS TIMESTAMP)       AS timestamp,
            CAST(a.uid AS BIGINT)                AS uid,
            a.user,
            CAST(a.changeset AS BIGINT)          AS changeset,
            a.tags,
            a.lat,
            a.lon,
            CAST(NULL AS ARRAY<BIGINT>)          AS refs,
            a.members,
            GREATEST(
                COALESCE(a.timestamp, current_timestamp()),
                COALESCE(b.latest_ts, timestamp_seconds(0))
            ) AS latest_ts,
            CASE
                WHEN b.geom IS NULL OR ST_IsEmpty(b.geom) THEN NULL
                ELSE ST_MakeValid(b.geom)
            END AS geom
        FROM {relations_data} a
        LEFT OUTER JOIN {geometry_only_data} b ON a.id = b.id
    """).createOrReplaceTempView(result_view)
