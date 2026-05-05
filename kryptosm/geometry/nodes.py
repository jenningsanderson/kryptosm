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
        id, version, timestamp, uid, user, changeset, tags, lat, lon, members

    Output view (`result_view`) columns:
        id, version (BIGINT), timestamp (TIMESTAMP), uid (BIGINT), user,
        changeset (BIGINT), tags, lat, lon, refs (NULL ARRAY<BIGINT>),
        members, latest_ts (TIMESTAMP), geom (ST_Point)

    Why:
        - Coordinates are reduced to 7 decimal places (~1cm at the equator)
          to match OSM's storage precision and stabilize downstream joins.
        - Rows with NULL lat/lon are silently dropped (deleted nodes).
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
               CAST(NULL AS ARRAY<BIGINT>)          AS refs,
               members,
               CAST(COALESCE(timestamp, current_timestamp()) AS TIMESTAMP) AS latest_ts,
               ST_ReducePrecision(ST_Point(lon, lat), 7) AS geom
        FROM {nodes_data}
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).createOrReplaceTempView(result_view)
