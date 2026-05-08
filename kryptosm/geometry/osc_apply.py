"""
Compute the dirty set for an OSC change file.

A feature is "dirty" if it appears in the OSC directly, or if it depends
on something dirty (e.g. a way whose nodes moved).

Dirty-set computation uses reverse-index tables (node_to_ways,
way_to_relations) so we never need to explode the entire way or relation
partition.
"""

from pyspark.sql import SparkSession


def all_dirty_ways(
    spark: SparkSession,
    base_ways: str,
    osc_way_upserts: str,
    dirty_nodes: str,
    node_to_ways_table: str,
    result_view: str,
):
    """
    Ways that need geometry rebuilt: direct OSC changes + ways whose nodes moved.

    Uses the ``node_to_ways`` index table instead of exploding every way's refs.
    The FULL OUTER JOIN is small: both sides contain only dirty features.
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
        FROM {osc_way_upserts} a
        FULL OUTER JOIN (
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon,
                   refs, members, latest_ts
            FROM {base_ways}
            WHERE id IN (
                SELECT DISTINCT way_id
                FROM {node_to_ways_table}
                WHERE node_id IN (SELECT id FROM {dirty_nodes})
            )
        ) b ON a.id = b.id
    """).createOrReplaceTempView(result_view)


def all_dirty_relations(
    spark: SparkSession,
    base_relations: str,
    osc_relation_upserts: str,
    dirty_ways: str,
    way_to_relations_table: str,
    result_view: str,
):
    """
    Relations that need geometry rebuilt: direct OSC changes + relations
    whose member ways are dirty.

    Uses the ``way_to_relations`` index table instead of exploding every
    relation's members.
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
        FROM {osc_relation_upserts} a
        FULL OUTER JOIN (
            SELECT rel.id, rel.version, rel.timestamp, rel.uid, rel.user, rel.changeset,
                   rel.tags, rel.lat, rel.lon, rel.refs, rel.members, rel.latest_ts
            FROM {base_relations} rel
            WHERE rel.id IN (
                SELECT DISTINCT relation_id
                FROM {way_to_relations_table}
                WHERE way_id IN (SELECT id FROM {dirty_ways})
            )
        ) b ON a.id = b.id
    """).createOrReplaceTempView(result_view)
