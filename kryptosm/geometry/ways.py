"""
Way geometry: LineString or Polygon per OSM way.

A way is an ordered list of node references. Closed ways with area-like
tags become polygons; everything else stays a linestring.
"""

from pyspark.sql import SparkSession

# Tag keys that, on a closed way, mark it as an area (polygon).
# Source: https://wiki.openstreetmap.org/wiki/Key:area
_AREA_TAG_KEYS = (
    "aeroway",
    "amenity",
    "building",
    "building:part",
    "golf",
    "harbour",
    "historic",
    "landuse",
    "man_made",
    "military",
    "natural",
    "office",
    "place",
    "power",
    "public_transport",
    "shop",
    "sport",
    "tourism",
    "water",
    "waterway",
    "wetland",
    "area",
)


def _area_tag_predicate() -> str:
    """SQL predicate fragment matching tags that imply 'this closed way is an area'."""
    base = " OR ".join(f"tags['{k}'] IS NOT NULL" for k in _AREA_TAG_KEYS)
    return (
        f"({base} "
        # Special cases - boundary=place and leisure=track are NOT areas.
        " OR (tags['boundary'] IS NOT NULL AND tags['boundary'] <> 'place')"
        " OR (tags['leisure']  IS NOT NULL AND tags['leisure']  <> 'track')"
        # Catch any tag whose key starts with `area:` (e.g. area:highway).
        " OR aggregate(transform(map_keys(tags), k -> position('area:', k) = 1),"
        "              FALSE, (acc, x) -> acc OR x))"
    )


def build_way_linestrings(
    spark: SparkSession, ways_data: str, nodes_geometry: str, result_view: str
):
    """
    Build a LineString per way by collecting its node geometries in order.

    Input view (`ways_data`) columns:
        id, version, timestamp, uid, user, changeset, tags, refs (ARRAY<BIGINT>)
        (extra columns are tolerated but ignored)
    Input view (`nodes_geometry`) columns:
        id, latest_ts, changeset, geom (ST_Point)

    Output view (`result_view`) columns:
        id, version, timestamp, uid, user, changeset, tags, refs (ARRAY<BIGINT>),
        latest_ts, additional_changesets, geom (ST_LineString or NULL)

    Why:
        - `refs` is the canonical flat-array form. OSM Parquet stores node
          refs as `nds: ARRAY<STRUCT<ref>>` - callers normalize that into
          `refs` once at the boundary (see `flatten_way_refs`).
        - `posexplode` + `sort_array(struct(pos, geom))` preserves the OSM
          node order, which matters for line direction.
        - A way needs >= 2 distinct nodes to form a line; otherwise geom is NULL.
        - We trust the input data: OSM ways do not contain adjacent duplicate
          node refs, so we don't pay for window-function dedup here.
    """
    spark.sql(f"""
        SELECT
            a.id, a.version, a.timestamp, a.uid, a.user, a.changeset, a.tags,
            a.refs,
            GREATEST(a.timestamp, b.latest_ts) AS latest_ts,
            COALESCE(
                filter(b.member_changesets, x -> x > a.changeset),
                CAST(array() AS ARRAY<BIGINT>)
            ) AS additional_changesets,
            IF (
                b.node_geom IS NOT NULL AND cardinality(b.node_geom) > 1,
                ST_LineFromMultiPoint(ST_Collect(b.node_geom.geom)),
                NULL
            ) AS geom
        FROM {ways_data} a
        LEFT OUTER JOIN (
            SELECT
                w.id,
                MAX(n.latest_ts) AS latest_ts,
                sort_array(collect_list(struct(w.node_pos, n.geom))) AS node_geom,
                collect_set(n.changeset) AS member_changesets
            FROM (
                SELECT id, posexplode(refs) AS (node_pos, node_id)
                FROM {ways_data}
            ) w
            JOIN {nodes_geometry} n ON w.node_id = n.id
            GROUP BY w.id
        ) b ON a.id = b.id
    """).createOrReplaceTempView(result_view)


def flatten_way_refs(spark: SparkSession, raw_ways: str, result_view: str):
    """
    Flatten OSM Parquet's `nds: ARRAY<STRUCT<ref>>` into our canonical `refs`.

    Input view (`raw_ways`) columns:
        id, version, timestamp, uid, user, changeset, tags, lat, lon,
        nds (ARRAY<STRUCT<ref: BIGINT>>), members
    Output view (`result_view`) columns:
        same shape, but with `refs (ARRAY<BIGINT>)` instead of `nds`.

    Why:
        - Every downstream function (and the Iceberg schema) uses `refs`.
          Doing the flatten once here keeps the rest of the pipeline working
          on a single canonical shape.
    """
    spark.sql(f"""
        SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon,
               transform(nds, x -> x.ref) AS refs,
               members
        FROM {raw_ways}
    """).createOrReplaceTempView(result_view)


def promote_closed_ways_to_areas(
    spark: SparkSession, ways_linestrings: str, result_view: str
):
    """
    Promote closed ways with area-like tags from LineString to Polygon.

    Input view (`ways_linestrings`) columns:
        id, version, timestamp, uid, user, changeset, tags, refs,
        latest_ts, additional_changesets, geom (ST_LineString or NULL)

    Output view (`result_view`) columns:
        same shape, but `geom` is ST_Polygon when the way is closed and
        tagged as an area, otherwise unchanged.

    Why:
        - OSM has no native polygon type. Closed ways become polygons only
          when their tags imply an area (see _AREA_TAG_KEYS) and they have
          at least 4 points (3 unique + the closing node).
        - `area=no` is an explicit override that keeps a way as a line.
        - ST_ForcePolygonCCW normalizes winding order so consumers can
          render exteriors consistently.
    """
    spark.sql(f"""
        SELECT
            id, version, timestamp, uid, user, changeset, tags,
            refs, latest_ts, additional_changesets,
            IF (
                geom IS NOT NULL
                AND ST_NumPoints(geom) > 3
                AND ST_IsClosed(geom)
                AND {_area_tag_predicate()}
                AND (tags['area'] IS NULL OR tags['area'] <> 'no'),
                ST_ForcePolygonCCW(ST_MakePolygon(geom)),
                geom
            ) AS geom
        FROM {ways_linestrings}
    """).createOrReplaceTempView(result_view)
