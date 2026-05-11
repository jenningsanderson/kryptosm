"""
Geometry inspection helpers — write sample features to GeoJSON for
manual verification of polygon construction, holes, etc.
"""

import json
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def sample_geojson(
    spark: SparkSession,
    table_name: str,
    output_path: str,
    geom_type: str = "Polygon",
    limit: int = 10,
    osm_type: Optional[str] = None,
) -> str:
    """Write the N most recently edited features of a given geometry type to GeoJSON.

    Args:
        geom_type: Sedona geometry type to filter on (e.g. "Polygon",
                   "MultiPolygon", "LineString", "Point").
        limit: Number of features to write.
        osm_type: OSM type partition to filter ("node", "way", "relation").
                  If None, searches all types.

    Returns the output path.
    """
    type_filter = f"AND type = '{osm_type}'" if osm_type else ""

    rows = spark.sql(f"""
        SELECT
            id, type, version, timestamp, tags,
            ST_AsGeoJSON(ST_GeomFromWKB(geometry)) AS geojson,
            ST_GeometryType(ST_GeomFromWKB(geometry)) AS geom_type,
            ST_NumGeometries(ST_GeomFromWKB(geometry)) AS num_parts,
            ST_NumInteriorRings(ST_GeomFromWKB(geometry)) AS num_holes
        FROM {table_name}
        WHERE geometry IS NOT NULL
          AND ST_GeometryType(ST_GeomFromWKB(geometry)) LIKE '%{geom_type}%'
          {type_filter}
        ORDER BY timestamp DESC
        LIMIT {limit}
    """).collect()

    features = []
    for row in rows:
        props = {
            "@id": row["id"],
            "@type": row["type"],
            "@version": row["version"],
            "@timestamp": str(row["timestamp"]),
            "@geom_type": row["geom_type"],
            "@num_parts": row["num_parts"],
            "@num_holes": row["num_holes"],
            "@tags": dict(row["tags"]) if row["tags"] else {},
        }

        features.append({
            "type": "Feature",
            "geometry": json.loads(row["geojson"]),
            "properties": props,
        })

    geojson = {"type": "FeatureCollection", "features": features}

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f, indent=2, default=str)

    for feat in features:
        p = feat["properties"]
        name = p.get("name", "")
        logger.info(
            "  %s/%s v%s  %s  parts=%s holes=%s  %s",
            p["@type"], p["@id"], p["@version"], p["@geom_type"],
            p["@num_parts"], p["@num_holes"], name,
        )

    logger.info("Wrote %d features to %s", len(features), output_path)
    return output_path
