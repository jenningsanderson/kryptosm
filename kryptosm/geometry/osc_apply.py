"""
Compute the dirty set for an OSC change file.

A feature is "dirty" if it appears in the OSC directly, or if it depends
on something dirty (e.g. a way whose nodes moved).

Dirty-set computation uses reverse-index tables (node_to_ways,
way_to_relations, node_to_relations, relation_to_relations) so we never
need to explode the entire way or relation partition.

The dirty-set views project only the columns the downstream geometry
pipeline actually uses for that type. ``dirty_ways`` carries no
``lat``/``lon``/``members``; ``dirty_relations`` carries no
``lat``/``lon``/``refs`` — Krypton's per-type tables don't have those
columns either.
"""

from typing import Optional

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
            COALESCE(a.refs, b.refs)           AS refs,
            COALESCE(a.latest_ts, b.latest_ts) AS latest_ts
        FROM (
            SELECT id, version, timestamp, uid, user, changeset, tags, refs, latest_ts
            FROM {osc_way_upserts}
        ) a
        FULL OUTER JOIN (
            SELECT id, version, timestamp, uid, user, changeset, tags, refs, latest_ts
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
    dirty_nodes: Optional[str] = None,
    node_to_relations_table: Optional[str] = None,
    relation_to_relations_table: Optional[str] = None,
):
    """
    Relations that need geometry rebuilt: direct OSC changes + relations
    whose member ways are dirty + (optionally) relations whose member nodes
    are dirty + (optionally) relations whose member sub-relations are dirty.

    Uses the ``way_to_relations``, ``node_to_relations``, and
    ``relation_to_relations`` index tables instead of exploding every
    relation's members.

    Notes on ``relation_to_relations`` widening:
      * Single-level only — a parent relation whose dirty sub-relation has
        its own dirty sub-relation does NOT get rebuilt automatically. In
        practice OSM relation hierarchies are shallow (1–2 levels), and
        any deeper changes will catch up on the next OSC apply that
        touches the chain.
      * "Dirty sub-relation" means any relation in ``osc_relation_upserts``
        — i.e. one that the OSC itself touched. We don't recursively widen
        with already-dirty-by-way relations because that would require
        fixed-point iteration.
    """
    union_parts = [
        f"""SELECT DISTINCT relation_id
            FROM {way_to_relations_table}
            WHERE way_id IN (SELECT id FROM {dirty_ways})"""
    ]
    if dirty_nodes is not None and node_to_relations_table is not None:
        union_parts.append(
            f"""SELECT DISTINCT relation_id
                FROM {node_to_relations_table}
                WHERE node_id IN (SELECT id FROM {dirty_nodes})"""
        )
    if relation_to_relations_table is not None:
        union_parts.append(
            f"""SELECT DISTINCT parent_relation_id AS relation_id
                FROM {relation_to_relations_table}
                WHERE child_relation_id IN (SELECT id FROM {osc_relation_upserts})"""
        )

    widen_subquery = "\n            UNION\n            ".join(union_parts)
    widen_clause = f"""
            rel.id IN (
                {widen_subquery}
            )
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
            COALESCE(a.members, b.members)     AS members,
            COALESCE(a.latest_ts, b.latest_ts) AS latest_ts
        FROM (
            SELECT id, version, timestamp, uid, user, changeset, tags, members, latest_ts
            FROM {osc_relation_upserts}
        ) a
        FULL OUTER JOIN (
            SELECT
                rel.id, rel.version, rel.timestamp, rel.uid, rel.user, rel.changeset,
                rel.tags, rel.members, rel.latest_ts
            FROM {base_relations} rel
            WHERE {widen_clause}
        ) b ON a.id = b.id
    """).createOrReplaceTempView(result_view)
