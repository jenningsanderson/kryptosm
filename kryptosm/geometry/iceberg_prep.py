"""
Convert a `geom` view into the layout the Iceberg OSM table expects.

This is the last step of the pipeline. It serializes geometries to WKB,
adds the bbox struct, pins the partition column (`type`), and repartitions
for parallel write.
"""

from pyspark.sql import SparkSession

# Relation WKB above this size gets simplified before write so the BINARY
# column doesn't blow up. ~30 MB is well below Parquet page-size pain points.
MAXIMUM_RELATION_GEOMETRY_SIZE = 30_000_000

# ST_SimplifyPreserveTopology tolerance for oversized relations. ~0.000001
# degrees is ~10 cm at the equator - imperceptible at country scale.
HUGE_GEOMETRY_SIMPLIFICATION_FACTOR = 0.000001


def _geometry_expr(osm_type: str) -> str:
    """SQL expression that serializes `geom` to WKB, simplifying huge relations."""
    if osm_type != "relation":
        return "ST_AsBinary(geom)"
    return (
        f"IF (LENGTH(ST_AsBinary(geom)) < {MAXIMUM_RELATION_GEOMETRY_SIZE}, "
        f"    ST_AsBinary(geom), "
        f"    ST_AsBinary(ST_SimplifyPreserveTopology(geom, {HUGE_GEOMETRY_SIMPLIFICATION_FACTOR})))"
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
                ST_AsBinary(geom)                   AS geometry,
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
