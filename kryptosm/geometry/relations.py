"""
Relation geometry: MultiPolygon or MultiLineString per OSM relation.

Relations bundle multiple ways (and sometimes nodes/relations) under a
single ID with role labels. The `tags['type']` decides what we build:

    multipolygon, boundary, building, bridge -> MultiPolygon
    route, waterway                          -> MultiLineString

All other relation types are kept in the table with NULL geometry.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

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
    logger.info("relations_need_geometry: %s → %s", relations_data, result_view)
    spark.sql(f"""
        SELECT id, members, tags,
               CAST(timestamp AS TIMESTAMP) AS latest_ts
        FROM {relations_data}
        WHERE tags['type'] IN ({_quote_csv(GEOMETRY_RELATION_TYPES)})
    """).createOrReplaceTempView(result_view)


def construct_multipolygon(
    spark: SparkSession, relations_data: str, ways_geometry: str, result_view: str,
    nodes_geometry: Optional[str] = None,
):
    """
    Build geometries for relations — polygon, line, and collection types.

    ``nodes_geometry`` is optional. When provided, site relations can include
    node members as points in a GeometryCollection alongside any way members.
    """
    logger.info("construct_multipolygon: %s + %s → %s", relations_data, ways_geometry, result_view)
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
    # Step 1: union member-way geometries per relation.
    spark.sql(f"""
        SELECT
            rm.relation_id                          AS id,
            MAX(rm.relation_latest_ts)              AS latest_ts,
            ST_Union_Aggr(lines.geom)               AS union_geom
        FROM (
            SELECT DISTINCT relation_id, relation_latest_ts, way_id
            FROM _rel_member_ways
            WHERE relation_type IN ({polygon_types}) AND role = 'outer'
        ) rm
        JOIN {ways_geometry} lines ON rm.way_id = lines.id
        WHERE lines.geom IS NOT NULL
              AND NOT ST_IsEmpty(lines.geom)
        GROUP BY rm.relation_id
    """).createOrReplaceTempView("_outer_unions")

    # Step 2: build polygons only from closed unions.  ST_BuildArea returns
    # JTS null when the input lines don't form closed rings, and Sedona
    # 1.8.x doesn't null-check function return values → NPE in the
    # serializer.  Filtering on ST_IsClosed prevents that.
    spark.sql("""
        SELECT id, latest_ts,
            ST_BuildArea(union_geom) AS geom
        FROM _outer_unions
        WHERE ST_IsClosed(union_geom)
    """).createOrReplaceTempView("_outer_polygons")

    # ----- polygon side: inner rings (holes) --------------------------------
    spark.sql(f"""
        SELECT
            rm.relation_id                          AS id,
            MAX(rm.relation_latest_ts)              AS latest_ts,
            ST_Union_Aggr(lines.geom)               AS union_geom
        FROM (
            SELECT DISTINCT relation_id, relation_latest_ts, way_id
            FROM _rel_member_ways
            WHERE relation_type IN ({polygon_types}) AND role = 'inner'
        ) rm
        JOIN {ways_geometry} lines ON rm.way_id = lines.id
        WHERE lines.geom IS NOT NULL
              AND NOT ST_IsEmpty(lines.geom)
        GROUP BY rm.relation_id
    """).createOrReplaceTempView("_inner_unions")

    spark.sql("""
        SELECT id, latest_ts,
            ST_BuildArea(union_geom) AS geom
        FROM _inner_unions
        WHERE ST_IsClosed(union_geom)
    """).createOrReplaceTempView("_inner_polygons")

    # ----- polygon relations from direct ways: outer minus inner ------------
    spark.sql("""
        SELECT
            COALESCE(o.id, i.id)                   AS id,
            COALESCE(o.latest_ts, i.latest_ts)     AS latest_ts,
            CASE
                WHEN o.geom IS NOT NULL AND i.geom IS NOT NULL
                    THEN ST_Difference(o.geom, i.geom)
                WHEN o.geom IS NOT NULL THEN o.geom
                ELSE i.geom
            END AS geom
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
                  AND sub.geom IS NOT NULL
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
            CASE
                WHEN p.geom IS NOT NULL AND s.sub_geom IS NOT NULL
                    THEN ST_Collect(p.geom, s.sub_geom)
                WHEN p.geom IS NOT NULL THEN p.geom
                ELSE s.sub_geom
            END AS geom
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
              AND NOT ST_IsEmpty(lines.geom)
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
                  AND NOT ST_IsEmpty(lines.geom)
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

        # Merge way and node geometries into a GeometryCollection.
        # ST_Collect instead of ST_Union: way geometries are a mix of
        # polygons and linestrings, and ST_Union rejects GeometryCollection
        # inputs.
        spark.sql("""
            SELECT
                COALESCE(w.id, n.relation_id) AS id,
                GREATEST(
                    COALESCE(w.latest_ts, n.latest_ts),
                    COALESCE(n.latest_ts, w.latest_ts)
                ) AS latest_ts,
                CASE
                    WHEN w.geom IS NOT NULL AND n.geom IS NOT NULL
                        THEN ST_Collect(w.geom, n.geom)
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
                  AND NOT ST_IsEmpty(lines.geom)
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
    spark: SparkSession, relations_data: str, geometry_only_data: str, result_view: str,
    ways_geometry: Optional[str] = None, nodes_geometry: Optional[str] = None,
):
    """
    Left-join all relations with their built geometry.

    Relations that already have typed geometry (from ``construct_multipolygon``)
    keep it. Relations without geometry get a fallback GeometryCollection built
    from whatever member way/node geometries are available, giving them a
    meaningful bbox.

    Also computes ``additional_changesets`` — the union of:
      (1) the input relation row's already-accumulated array (direct edits to
          this relation: OSC-dedup losers + prior-apply carry-forward), and
      (2) member-way / member-node changesets STRICTLY greater than the
          relation's own changeset (child changesets that postdate this
          relation's last edit; the "> self" filter bounds growth over time).
    Sub-relation members are not recursed; their own changesets are reachable
    via their own ``additional_changesets`` columns.
    """
    logger.info("relation_merge_geometry_data: %s + %s → %s", relations_data, geometry_only_data, result_view)
    if ways_geometry:
        # Only compute fallback for relations that construct_multipolygon
        # didn't produce geometry for — avoids exploding all members.
        spark.sql(f"""
            SELECT id, members FROM {relations_data}
            WHERE id NOT IN (
                SELECT id FROM {geometry_only_data}
                WHERE geom IS NOT NULL
            )
        """).createOrReplaceTempView("_rels_needing_fallback")

        parts = [f"""
            SELECT r.id AS relation_id,
                   ST_Union_Aggr(w.geom) AS member_geom
            FROM (
                SELECT id, explode(members) AS member
                FROM _rels_needing_fallback
            ) r
            JOIN {ways_geometry} w ON w.id = r.member.ref
            WHERE r.member.type = 'way'
                  AND w.geom IS NOT NULL
            GROUP BY r.id
        """]
        if nodes_geometry:
            parts.append(f"""
                SELECT r.id AS relation_id,
                       ST_Union_Aggr(n.geom) AS member_geom
                FROM (
                    SELECT id, explode(members) AS member
                    FROM _rels_needing_fallback
                ) r
                JOIN {nodes_geometry} n ON n.id = r.member.ref
                WHERE r.member.type = 'node'
                      AND n.geom IS NOT NULL
                GROUP BY r.id
            """)
        union_parts = " UNION ALL ".join(parts)
        spark.sql(f"""
            SELECT relation_id,
                   ST_Union_Aggr(member_geom) AS geom
            FROM ({union_parts})
            GROUP BY relation_id
        """).createOrReplaceTempView("_fallback_geom")
        fallback_join = "LEFT OUTER JOIN _fallback_geom f ON a.id = f.relation_id"
        fallback_expr = """
            COALESCE(b.geom, f.geom)"""
    else:
        fallback_join = ""
        fallback_expr = """
            b.geom"""

    # ----- additional_changesets: collect_set from direct way/node members ---
    cs_parts = []
    if ways_geometry:
        cs_parts.append(f"""
            SELECT r.id AS relation_id,
                   w.changeset AS member_changeset
            FROM (
                SELECT id, explode(members) AS member
                FROM {relations_data}
            ) r
            JOIN {ways_geometry} w ON w.id = r.member.ref
            WHERE r.member.type = 'way'
                  AND w.changeset IS NOT NULL
        """)
    if nodes_geometry:
        cs_parts.append(f"""
            SELECT r.id AS relation_id,
                   n.changeset AS member_changeset
            FROM (
                SELECT id, explode(members) AS member
                FROM {relations_data}
            ) r
            JOIN {nodes_geometry} n ON n.id = r.member.ref
            WHERE r.member.type = 'node'
                  AND n.changeset IS NOT NULL
        """)

    if cs_parts:
        cs_union = " UNION ALL ".join(cs_parts)
        spark.sql(f"""
            SELECT relation_id,
                   collect_set(member_changeset) AS member_changesets
            FROM ({cs_union})
            GROUP BY relation_id
        """).createOrReplaceTempView("_rel_member_changesets")
        cs_join = "LEFT OUTER JOIN _rel_member_changesets c ON a.id = c.relation_id"
        # Union the input relation's already-accumulated additional_changesets
        # (direct edits: OSC-dedup losers + prior-apply carry-forward, passed
        # through unfiltered) with member changesets STRICTLY greater than the
        # relation's own changeset. The "> self.changeset" filter on the
        # member-derived side keeps that side bounded — older child
        # changesets are already implicit at the time of the relation's last
        # edit, so they don't add attribution information.
        cs_expr = (
            "array_distinct(array_union(\n"
            "                COALESCE(a.additional_changesets,\n"
            "                         CAST(array() AS ARRAY<BIGINT>)),\n"
            "                COALESCE(filter(c.member_changesets, x -> x > a.changeset),\n"
            "                         CAST(array() AS ARRAY<BIGINT>))\n"
            "            ))"
        )
    else:
        cs_join = ""
        cs_expr = (
            "COALESCE(a.additional_changesets,\n"
            "                 CAST(array() AS ARRAY<BIGINT>))"
        )

    spark.sql(f"""
        SELECT
            a.id,
            CAST(a.version AS BIGINT)            AS version,
            CAST(a.timestamp AS TIMESTAMP)       AS timestamp,
            CAST(a.uid AS BIGINT)                AS uid,
            a.user,
            CAST(a.changeset AS BIGINT)          AS changeset,
            a.tags,
            a.members,
            GREATEST(
                a.timestamp,
                b.latest_ts
            ) AS latest_ts,
            {cs_expr} AS additional_changesets,
            {fallback_expr} AS geom
        FROM {relations_data} a
        LEFT OUTER JOIN {geometry_only_data} b ON a.id = b.id
        {fallback_join}
        {cs_join}
    """).createOrReplaceTempView(result_view)
