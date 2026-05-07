"""
Iceberg table operations for the OSM table.
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Return True if the Iceberg table exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def create_iceberg_table(
    spark: SparkSession,
    table_name: str,
    table_location: Optional[str] = None,
):
    """Create the OSM Iceberg table (idempotent: drops first to guarantee schema)."""
    parts = table_name.split(".")
    if len(parts) >= 2:
        full_db_name = ".".join(parts[:-1])
    else:
        full_db_name = "default"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")
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
            'format'='parquet',
            'write.parquet.compression-codec'='snappy',
            'format-version'='2',
            'write.metadata.compression-codec'='none'
        )
    """)


def get_table_count(spark: SparkSession, table_name: str) -> dict:
    """Return {osm_type: count} for the OSM table."""
    rows = spark.sql(f"SELECT type, COUNT(*) AS count FROM {table_name} GROUP BY type").collect()
    return {row["type"]: row["count"] for row in rows}


def get_table_max_timestamp(spark: SparkSession, table_name: str) -> Optional[datetime]:
    """Return the newest ``timestamp`` in the table, or ``None`` if empty."""
    row = spark.sql(f"SELECT MAX(timestamp) AS max_ts FROM {table_name}").collect()[0]
    return row["max_ts"]


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
