"""
Relation geometry: MultiPolygon or MultiLineString per OSM relation.

Relations bundle multiple ways (and sometimes nodes/relations) under a
single ID with role labels. The `tags['type']` decides what we build:

    multipolygon, boundary, building, bridge -> MultiPolygon
    route, waterway                          -> MultiLineString

All other relation types are kept in the table with NULL geometry.
"""

from pyspark.sql import SparkSession

# Relation `tags['type']` values for which we build geometry.
GEOMETRY_RELATION_TYPES = (
    "multipolygon", "boundary", "building", "bridge",
    "route", "waterway", "site",
)

_POLYGON_TYPES = ("multipolygon", "boundary", "building", "bridge")
_LINE_TYPES = ("route", "waterway")
_COLLECTION_TYPES = ("site",)


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
    spark: SparkSession, relations_data: str, ways_geometry: str, result_view: str,
    nodes_geometry: str = None,
):
    """
    Build geometries for relations — polygon, line, and collection types.

    ``nodes_geometry`` is optional. When provided, site relations can include
    node members as points in a GeometryCollection alongside any way members.
    """
    # ----- pre-explode direct way members ------------------------------------
    # Normalize roles: 'outline' (building/bridge) → 'outer',
    # 'part' (building) → 'outer' (each part is a polygon in the MultiPolygon).
    # Empty/missing roles default to 'outer' per OSM convention.
    spark.sql(f"""
        SELECT
            r.id                                  AS relation_id,
            r.tags['type']                        AS relation_type,
            r.latest_ts                           AS relation_latest_ts,
            m.member.ref                          AS way_id,
            CASE
                WHEN m.member.role IN ('inner')           THEN 'inner'
                WHEN m.member.role IN ('outer', 'outline',
                                       'part', '')        THEN 'outer'
                WHEN m.member.role IS NULL                 THEN 'outer'
                ELSE m.member.role
            END AS role
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

    # ----- polygon relations from direct ways: outer minus inner ------------
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
    """).createOrReplaceTempView("_polygon_from_ways")

    # ----- include sub-relation geometries as complete pieces ---------------
    # Sub-relations already have their geometry built (from their own ways).
    # We collect them and union with the parent's direct-way geometry.
    spark.sql(f"""
        SELECT
            parent_id,
            ST_Union_Aggr(sub_geom)                AS sub_geom,
            MAX(sub_latest_ts)                     AS sub_latest_ts
        FROM (
            SELECT
                r.id                               AS parent_id,
                sub.geom                           AS sub_geom,
                sub.latest_ts                      AS sub_latest_ts
            FROM (
                SELECT id, explode(members) AS member
                FROM {relations_data}
            ) r
            JOIN _polygon_from_ways sub ON sub.id = r.member.ref
            WHERE r.member.type = 'relation'
        )
        GROUP BY parent_id
    """).createOrReplaceTempView("_sub_relation_geoms")

    spark.sql("""
        SELECT
            COALESCE(p.id, s.parent_id)            AS id,
            GREATEST(
                COALESCE(p.latest_ts, s.sub_latest_ts),
                COALESCE(s.sub_latest_ts, p.latest_ts)
            )                                      AS latest_ts,
            ST_MakeValid(
                CASE
                    WHEN p.geom IS NOT NULL AND s.sub_geom IS NOT NULL
                        THEN ST_Union(p.geom, s.sub_geom)
                    WHEN p.geom IS NOT NULL THEN p.geom
                    ELSE s.sub_geom
                END
            ) AS geom
        FROM _polygon_from_ways p
        FULL OUTER JOIN _sub_relation_geoms s ON p.id = s.parent_id
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

    # ----- collection relations (site): ways + nodes as GeometryCollection ---
    collection_types = _quote_csv(_COLLECTION_TYPES)

    if nodes_geometry:
        # Collect way geometries for collection-type relations
        spark.sql(f"""
            SELECT rm.relation_id AS id,
                   MAX(rm.relation_latest_ts) AS latest_ts,
                   ST_Union_Aggr(lines.geom) AS geom
            FROM (
                SELECT DISTINCT relation_id, relation_latest_ts, way_id
                FROM _rel_member_ways
                WHERE relation_type IN ({collection_types})
            ) rm
            JOIN {ways_geometry} lines ON rm.way_id = lines.id
            WHERE lines.geom IS NOT NULL
            GROUP BY rm.relation_id
        """).createOrReplaceTempView("_collection_ways")

        # Collect node geometries for collection-type relations
        spark.sql(f"""
            SELECT r.id AS relation_id,
                   MAX(r.latest_ts) AS latest_ts,
                   ST_Union_Aggr(n.geom) AS geom
            FROM (
                SELECT id, latest_ts, explode(members) AS member
                FROM {relations_data}
                WHERE tags['type'] IN ({collection_types})
            ) r
            JOIN {nodes_geometry} n ON n.id = r.member.ref
            WHERE r.member.type = 'node'
              AND n.geom IS NOT NULL
            GROUP BY r.id
        """).createOrReplaceTempView("_collection_nodes")

        # Merge way and node geometries
        spark.sql("""
            SELECT
                COALESCE(w.id, n.relation_id) AS id,
                GREATEST(
                    COALESCE(w.latest_ts, n.latest_ts),
                    COALESCE(n.latest_ts, w.latest_ts)
                ) AS latest_ts,
                CASE
                    WHEN w.geom IS NOT NULL AND n.geom IS NOT NULL
                        THEN ST_Union(w.geom, n.geom)
                    WHEN w.geom IS NOT NULL THEN w.geom
                    ELSE n.geom
                END AS geom
            FROM _collection_ways w
            FULL OUTER JOIN _collection_nodes n ON w.id = n.relation_id
        """).createOrReplaceTempView("_collection_relations")
    else:
        spark.sql(f"""
            SELECT rm.relation_id AS id,
                   MAX(rm.relation_latest_ts) AS latest_ts,
                   ST_Union_Aggr(lines.geom) AS geom
            FROM (
                SELECT DISTINCT relation_id, relation_latest_ts, way_id
                FROM _rel_member_ways
                WHERE relation_type IN ({collection_types})
            ) rm
            JOIN {ways_geometry} lines ON rm.way_id = lines.id
            WHERE lines.geom IS NOT NULL
            GROUP BY rm.relation_id
        """).createOrReplaceTempView("_collection_relations")

    spark.sql("""
        SELECT id, latest_ts, geom FROM _polygon_relations
        UNION ALL
        SELECT id, latest_ts, geom FROM _line_relations
        UNION ALL
        SELECT id, latest_ts, geom FROM _collection_relations
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
