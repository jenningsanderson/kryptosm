"""
Node geometry: Point per OSM node.

A node is the simplest OSM primitive - just a coordinate. Everything else
(ways, relations) is built from references to nodes.
"""

from pyspark.sql import SparkSession


def build_node_geometry(spark: SparkSession, nodes_data: str, result_view: str):
    """
    Project an OSM node onto an ST_Point.

    Input view (`nodes_data`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon
        (extra columns like `members`/`refs` are tolerated but ignored —
         OSC records carry them as NULLs for nodes)

    Output view (`result_view`) columns:
        id, version (BIGINT), timestamp (TIMESTAMP), uid (BIGINT), user,
        changeset (BIGINT), tags, lat, lon, latest_ts (TIMESTAMP),
        additional_changesets (empty ARRAY<BIGINT>), geom (ST_Point)

    Why:
        - Coordinates are reduced to 7 decimal places (~1cm at the equator)
          to match OSM's storage precision and stabilize downstream joins.
        - Rows with NULL lat/lon are silently dropped (deleted nodes).
        - Nodes have no children, so ``additional_changesets`` is always
          an empty array.
        - ``latest_ts`` is the feature's own ``timestamp``. We never
          fall back to wall-clock — feature temporal data must come from
          OSM (base or OSC), never from ``current_timestamp()``.
    """
    spark.sql(f"""
        SELECT id,
               CAST(version AS BIGINT)              AS version,
               CAST(timestamp AS TIMESTAMP)         AS timestamp,
               CAST(uid AS BIGINT)                  AS uid,
               user,
               CAST(changeset AS BIGINT)            AS changeset,
               tags,
               lat,
               lon,
               CAST(timestamp AS TIMESTAMP)         AS latest_ts,
               CAST(array() AS ARRAY<BIGINT>)       AS additional_changesets,
               ST_ReducePrecision(ST_Point(lon, lat), 7) AS geom
        FROM {nodes_data}
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).createOrReplaceTempView(result_view)
