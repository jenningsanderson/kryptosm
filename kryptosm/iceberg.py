"""
Iceberg table operations for the OSM table.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Tunable Iceberg table properties for different data scales."""

    distribution_mode: str = "range"
    bloom_filter_enabled: bool = True
    bloom_filter_max_bytes: Optional[int] = 1_048_576

    @classmethod
    def testing(cls) -> "TableConfig":
        """Lightweight settings for local tests (< 10M records)."""
        return cls(
            distribution_mode="none",
            bloom_filter_enabled=False,
            bloom_filter_max_bytes=None,
        )

    @classmethod
    def production(cls) -> "TableConfig":
        """Full optimization for planet-scale data (10B+ records)."""
        return cls(
            distribution_mode="range",
            bloom_filter_enabled=True,
            bloom_filter_max_bytes=1_048_576,
        )

    def _main_table_props(self) -> str:
        lines = [
            "'format'='parquet'",
            "'write.parquet.compression-codec'='snappy'",
            "'format-version'='2'",
            "'write.metadata.compression-codec'='none'",
            f"'write.distribution-mode'='{self.distribution_mode}'",
            "'write.delete.mode'='merge-on-read'",
            "'write.update.mode'='merge-on-read'",
            "'write.merge.mode'='merge-on-read'",
        ]
        if self.distribution_mode == "range":
            lines.append("'write.sort-order'='id ASC'")
        if self.bloom_filter_enabled:
            lines.append("'write.parquet.bloom-filter-enabled.column.id'='true'")
            if self.bloom_filter_max_bytes:
                lines.append(
                    f"'write.parquet.bloom-filter-max-bytes.column.id'='{self.bloom_filter_max_bytes}'"
                )
        return ",\n            ".join(lines)

    def _index_table_props(self, sort_col: str) -> str:
        lines = [
            "'format'='parquet'",
            "'write.parquet.compression-codec'='snappy'",
            "'format-version'='2'",
            f"'write.distribution-mode'='{self.distribution_mode}'",
        ]
        if self.distribution_mode == "range":
            lines.append(f"'write.sort-order'='{sort_col} ASC'")
        if self.bloom_filter_enabled:
            lines.append(f"'write.parquet.bloom-filter-enabled.column.{sort_col}'='true'")
            if self.bloom_filter_max_bytes:
                lines.append(
                    f"'write.parquet.bloom-filter-max-bytes.column.{sort_col}'"
                    f"='{self.bloom_filter_max_bytes}'"
                )
        return ",\n            ".join(lines)


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Return True if the Iceberg table exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def load_with_geom(spark: SparkSession, table_name: str, osm_type: str, view_name: str):
    """Read one OSM type from the table, decoding WKB geometry back to Sedona geom."""
    spark.sql(f"""
        SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon,
               refs, members, latest_ts,
               ST_GeomFromWKB(geometry) AS geom
        FROM {table_name}
        WHERE type = '{osm_type}'
    """).createOrReplaceTempView(view_name)


def _ensure_db(spark: SparkSession, table_name: str):
    parts = table_name.split(".")
    if len(parts) >= 2:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {'.'.join(parts[:-1])}")


def create_iceberg_table(
    spark: SparkSession,
    table_name: str,
    table_location: Optional[str] = None,
    config: Optional[TableConfig] = None,
):
    """Create the OSM Iceberg table (idempotent: drops first to guarantee schema)."""
    if config is None:
        config = TableConfig()
    _ensure_db(spark, table_name)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    location_clause = (
        f"LOCATION '{table_location}'"
        if table_location and table_location.startswith("s3://")
        else ""
    )

    spark.sql(f"""
        CREATE TABLE {table_name} (
            id        BIGINT,
            type      STRING,
            version   BIGINT,
            timestamp TIMESTAMP,
            changeset BIGINT,
            uid       BIGINT,
            user      STRING,
            tags      MAP<STRING, STRING>,
            lat       DOUBLE,
            lon       DOUBLE,
            refs      ARRAY<BIGINT>,
            members   ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
            latest_ts TIMESTAMP,
            geometry  BINARY,
            bbox      STRUCT<xmin: FLOAT, xmax: FLOAT, ymin: FLOAT, ymax: FLOAT>
        )
        USING iceberg
        PARTITIONED BY (type)
        {location_clause}
        TBLPROPERTIES (
            {config._main_table_props()}
        )
    """)
    logger.info("Created table %s (config=%s)", table_name, config)


# ---------------------------------------------------------------------------
# Index tables: node_to_ways, way_to_relations
# ---------------------------------------------------------------------------


def create_index_tables(
    spark: SparkSession,
    node_to_ways: str,
    way_to_relations: str,
    config: Optional[TableConfig] = None,
):
    """Create the reverse-index tables used for dirty-set computation."""
    if config is None:
        config = TableConfig()
    for idx in (node_to_ways, way_to_relations):
        _ensure_db(spark, idx)

    for idx, cols, sort_col in [
        (node_to_ways, "node_id BIGINT, way_id BIGINT", "node_id"),
        (way_to_relations, "way_id BIGINT, relation_id BIGINT", "way_id"),
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {idx}")
        spark.sql(f"""
            CREATE TABLE {idx} ({cols})
            USING iceberg
            TBLPROPERTIES (
                {config._index_table_props(sort_col)}
            )
        """)


def populate_node_to_ways(spark: SparkSession, table_name: str, node_to_ways: str):
    """Bulk-populate node_to_ways from the main table's way partition."""
    spark.sql(f"""
        INSERT INTO {node_to_ways}
        SELECT explode(refs) AS node_id, id AS way_id
        FROM {table_name}
        WHERE type = 'way' AND refs IS NOT NULL
    """)


def populate_way_to_relations(spark: SparkSession, table_name: str, way_to_relations: str):
    """Bulk-populate way_to_relations from the main table's relation partition."""
    from .geometry.relations import GEOMETRY_RELATION_TYPES
    types = ", ".join(f"'{t}'" for t in GEOMETRY_RELATION_TYPES)
    spark.sql(f"""
        INSERT INTO {way_to_relations}
        SELECT member.ref AS way_id, id AS relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {table_name}
            WHERE type = 'relation' AND tags['type'] IN ({types})
        )
        WHERE member.type = 'way'
    """)


def refresh_node_to_ways(spark: SparkSession, table_name: str, node_to_ways: str, dirty_way_ids: str):
    """Delete and re-insert index entries for dirty ways after a MERGE."""
    spark.sql(f"DELETE FROM {node_to_ways} WHERE way_id IN (SELECT id FROM {dirty_way_ids})")
    spark.sql(f"""
        INSERT INTO {node_to_ways}
        SELECT explode(refs) AS node_id, id AS way_id
        FROM {table_name}
        WHERE type = 'way' AND id IN (SELECT id FROM {dirty_way_ids})
    """)


def refresh_way_to_relations(spark: SparkSession, table_name: str, way_to_relations: str, dirty_rel_ids: str):
    """Delete and re-insert index entries for dirty relations after a MERGE."""
    from .geometry.relations import GEOMETRY_RELATION_TYPES
    types = ", ".join(f"'{t}'" for t in GEOMETRY_RELATION_TYPES)
    spark.sql(f"DELETE FROM {way_to_relations} WHERE relation_id IN (SELECT id FROM {dirty_rel_ids})")
    spark.sql(f"""
        INSERT INTO {way_to_relations}
        SELECT member.ref AS way_id, id AS relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {table_name}
            WHERE type = 'relation' AND id IN (SELECT id FROM {dirty_rel_ids})
                  AND tags['type'] IN ({types})
        )
        WHERE member.type = 'way'
    """)


# ---------------------------------------------------------------------------
# Table properties
# ---------------------------------------------------------------------------

def get_table_count(spark: SparkSession, table_name: str) -> dict:
    """Return {osm_type: count} for the OSM table."""
    rows = spark.sql(f"SELECT type, COUNT(*) AS count FROM {table_name} GROUP BY type").collect()
    return {row["type"]: row["count"] for row in rows}


def get_table_max_timestamp(spark: SparkSession, table_name: str) -> Optional[datetime]:
    """Return the newest ``timestamp`` in the table, or ``None`` if empty."""
    row = spark.sql(f"SELECT MAX(timestamp) AS max_ts FROM {table_name}").collect()[0]
    return row["max_ts"]


_OSC_SEQ_PROPERTY = "last-applied-osc-sequence"


def get_last_applied_sequence(spark: SparkSession, table_name: str) -> Optional[int]:
    """Return the last-applied OSC sequence number, or ``None`` if never set."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_name} ('{_OSC_SEQ_PROPERTY}')").collect()
    if not rows:
        return None
    value = rows[0][1]
    if value.startswith("Table"):
        return None
    return int(value)


def set_last_applied_sequence(spark: SparkSession, table_name: str, seq: int) -> None:
    """Stamp the table with the last-applied OSC sequence number."""
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{_OSC_SEQ_PROPERTY}' = '{seq}')"
    )


_OSC_FILE_PROPERTY = "current-osc-file"


def set_current_osc_file(spark: SparkSession, table_name: str, filename: str) -> None:
    """Stamp the table with the OSC file currently being applied."""
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{_OSC_FILE_PROPERTY}' = '{filename}')"
    )


# ---------------------------------------------------------------------------
# MERGE / DELETE
# ---------------------------------------------------------------------------

def merge_into_table(
    spark: SparkSession,
    table_name: str,
    source_view: str,
    match_condition: str,
):
    """MERGE source_view into table_name (upsert by match_condition)."""
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING {source_view} s
        ON {match_condition}
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def delete_from_table(
    spark: SparkSession,
    table_name: str,
    source_view: str,
    match_condition: str,
):
    """Delete rows from table_name where match_condition holds against source_view."""
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING {source_view} s
        ON {match_condition}
        WHEN MATCHED THEN DELETE
    """)
