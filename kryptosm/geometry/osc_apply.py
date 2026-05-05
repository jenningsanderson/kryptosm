"""
Apply an OSC change file on top of a base view.

OpenStreetMap publishes daily change files (OSC) listing creates / modifies /
deletes. To incrementally update the table we need to find every feature
whose geometry might change, rebuild it, and overlay the result.

A feature is "dirty" if:
  - it appears in the OSC directly (any op), OR
  - it depends on something that's dirty (e.g. a way whose nodes moved).

These functions compute the dirty set and apply the overlay - all in SQL.
"""

from pyspark.sql import SparkSession

from .relations import GEOMETRY_RELATION_TYPES


def all_dirty_ways(
    spark: SparkSession,
    base_ways: str,
    new_or_property_updated_ways: str,
    dirty_nodes: str,
    result_view: str,
):
    """
    Union of (a) ways changed in the OSC and (b) base ways whose nodes moved.

    Input view (`base_ways`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon,
        refs (ARRAY<BIGINT>), members, latest_ts
    Input view (`new_or_property_updated_ways`) columns:
        same shape - ways from the OSC `create`/`modify` ops
    Input view (`dirty_nodes`) columns:
        id (BIGINT) - any node touched by the OSC

    Output view (`result_view`) columns:
        same shape as the inputs - the union, OSC values winning on conflict.

    Why:
        - The FULL OUTER JOIN with COALESCE means: prefer OSC data when present,
          fall back to base. This handles ways that are both directly modified
          AND have dirty nodes.
        - `array_contains` would also work but is slower than the join+explode.
    """
    spark.sql(f"""
        SELECT
            COALESCE(a.id, b.id)               AS id,
            COALESCE(a.version, b.version)     AS version,
            COALESCE(a.timestamp, b.timestamp) AS timestamp,
            COALESCE(a.uid, b.uid)             AS uid,
            COALESCE(a.user, b.user)           AS user,
            COALESCE(a.changeset, b.changeset) AS changeset,
            COALESCE(a.tags, b.tags)           AS tags,
            COALESCE(a.lat, b.lat)             AS lat,
            COALESCE(a.lon, b.lon)             AS lon,
            COALESCE(a.refs, b.refs)           AS refs,
            COALESCE(a.members, b.members)     AS members,
            COALESCE(a.latest_ts, b.latest_ts) AS latest_ts
        FROM {new_or_property_updated_ways} a
        FULL OUTER JOIN (
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon,
                   refs, members, latest_ts
            FROM {base_ways}
            WHERE id IN (
                SELECT bw.id
                FROM (SELECT id, explode(refs) AS node_id FROM {base_ways}) bw
                JOIN {dirty_nodes} dn ON bw.node_id = dn.id
            )
        ) b ON a.id = b.id
    """).createOrReplaceTempView(result_view)


def all_dirty_relations(
    spark: SparkSession,
    base_relations: str,
    new_or_property_updated_relations: str,
    dirty_ways: str,
    result_view: str,
):
    """
    Union of (a) relations changed in the OSC and (b) geometry-bearing
    relations from base whose member ways are dirty.

    Input view (`base_relations`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon,
        refs, members (ARRAY<STRUCT<type, ref, role>>), latest_ts
    Input view (`new_or_property_updated_relations`) columns:
        same shape - relations from the OSC create/modify ops
    Input view (`dirty_ways`) columns:
        id (BIGINT) - any way touched directly or via node propagation

    Output view (`result_view`) columns:
        same shape as the inputs - OSC values winning on conflict.

    Why:
        - We restrict the dependency check to GEOMETRY_RELATION_TYPES because
          other relation types don't carry geometry, so they don't need rebuild
          when their members change.
    """
    types = ", ".join(f"'{t}'" for t in GEOMETRY_RELATION_TYPES)
    spark.sql(f"""
        SELECT
            COALESCE(a.id, b.id)               AS id,
            COALESCE(a.version, b.version)     AS version,
            COALESCE(a.timestamp, b.timestamp) AS timestamp,
            COALESCE(a.uid, b.uid)             AS uid,
            COALESCE(a.user, b.user)           AS user,
            COALESCE(a.changeset, b.changeset) AS changeset,
            COALESCE(a.tags, b.tags)           AS tags,
            COALESCE(a.lat, b.lat)             AS lat,
            COALESCE(a.lon, b.lon)             AS lon,
            COALESCE(a.refs, b.refs)           AS refs,
            COALESCE(a.members, b.members)     AS members,
            COALESCE(a.latest_ts, b.latest_ts) AS latest_ts
        FROM {new_or_property_updated_relations} a
        FULL OUTER JOIN (
            SELECT
                rel.id, version, timestamp, uid, user, changeset, tags, lat, lon,
                refs, members,
                GREATEST(rel.latest_ts, COALESCE(way_ts.latest_ts, timestamp_seconds(0)))
                    AS latest_ts
            FROM {base_relations} rel
            JOIN (
                SELECT m.id, MAX(dw.latest_ts) AS latest_ts
                FROM (
                    SELECT id, explode(members) AS member
                    FROM {base_relations}
                    WHERE tags['type'] IN ({types})
                ) m
                JOIN {dirty_ways} dw ON m.member.ref = dw.id
                WHERE m.member.type = 'way'
                GROUP BY m.id
            ) way_ts ON rel.id = way_ts.id
        ) b ON a.id = b.id
    """).createOrReplaceTempView(result_view)


def apply_osc_with_geometry(
    spark: SparkSession,
    base_data: str,
    updated_data: str,
    deleted_data: str,
    result_view: str,
):
    """
    Overlay updates on base (updated wins where present), then drop deletes.

    Input view (`base_data`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon, refs,
        members, latest_ts, geom
    Input view (`updated_data`) columns:
        same shape - rebuilt features for the dirty set
    Input view (`deleted_data`) columns:
        id (BIGINT) - features the OSC marked as deleted

    Output view (`result_view`) columns:
        same shape as inputs.

    Why:
        - FULL OUTER JOIN + COALESCE(b.X, a.X) means: take the updated row
          if it exists, otherwise the base row. This covers creates, updates,
          and untouched rows in one pass.
        - LEFT ANTI JOIN cleanly drops deletes without an extra MERGE.
    """
    spark.sql(f"""
        SELECT m.id, m.version, m.timestamp, m.uid, m.user, m.changeset, m.tags,
               m.lat, m.lon, m.refs, m.members, m.latest_ts, m.geom
        FROM (
            SELECT
                COALESCE(b.id, a.id)               AS id,
                COALESCE(b.version, a.version)     AS version,
                COALESCE(b.timestamp, a.timestamp) AS timestamp,
                COALESCE(b.uid, a.uid)             AS uid,
                COALESCE(b.user, a.user)           AS user,
                COALESCE(b.changeset, a.changeset) AS changeset,
                COALESCE(b.tags, a.tags)           AS tags,
                COALESCE(b.lat, a.lat)             AS lat,
                COALESCE(b.lon, a.lon)             AS lon,
                COALESCE(b.refs, a.refs)           AS refs,
                COALESCE(b.members, a.members)     AS members,
                COALESCE(b.latest_ts, a.latest_ts) AS latest_ts,
                COALESCE(b.geom, a.geom)           AS geom
            FROM {base_data} a
            FULL OUTER JOIN {updated_data} b ON a.id = b.id
        ) m
        LEFT ANTI JOIN {deleted_data} d ON m.id = d.id
    """).createOrReplaceTempView(result_view)
