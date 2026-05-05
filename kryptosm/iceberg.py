"""
Iceberg table operations for OSM data.
"""

from typing import Optional
from pyspark.sql import SparkSession


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if an Iceberg table exists.

    Args:
        spark: Spark session
        table_name: Full table name

    Returns:
        True if table exists, False otherwise
    """
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        return True
    except Exception as e:
        # Table doesn't exist or other error
        return False


def create_iceberg_table(
    spark: SparkSession,
    table_name: str,
    table_location: Optional[str] = None,
    partition_by: str = "type",
):
    """
    Create the OSM Iceberg table if it doesn't exist.

    Args:
        spark: Spark session
        table_name: Full table name
        table_location: S3 location for table data (optional, uses warehouse if not specified)
        partition_by: Column to partition by (default: 'type')
    """
    print(f"Creating Iceberg table: {table_name}")

    # Extract database and catalog from table name
    parts = table_name.split(".")
    if len(parts) == 3:
        catalog_name = parts[0]
        database_name = parts[1]
        table = parts[2]
        full_db_name = f"{catalog_name}.{database_name}"
    elif len(parts) == 2:
        catalog_name = "spark_catalog"
        database_name = parts[0]
        table = parts[1]
        full_db_name = database_name
    else:
        catalog_name = "spark_catalog"
        database_name = "default"
        table = table_name
        full_db_name = database_name

    print(f"Creating database if not exists: {full_db_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}").collect()

    # Drop table if it exists (to ensure it's an Iceberg table)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    # Create table with OSM schema
    # Note: For Hadoop catalog, LOCATION should not be specified for managed tables
    # The table will be created in the warehouse directory
    create_sql = f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            type STRING,
            version BIGINT,
            timestamp TIMESTAMP,
            changeset BIGINT,
            uid BIGINT,
            user STRING,
            tags MAP<STRING, STRING>,
            lat DOUBLE,
            lon DOUBLE,
            refs ARRAY<BIGINT>,
            members ARRAY<STRUCT<
                type: STRING,
                ref: BIGINT,
                role: STRING
            >>,
            latest_ts TIMESTAMP,
            geometry BINARY,
            bbox STRUCT<
                xmin: FLOAT,
                xmax: FLOAT,
                ymin: FLOAT,
                ymax: FLOAT
            >
        ) USING iceberg
        PARTITIONED BY ({partition_by})
    """

    # Add LOCATION if specified
    # For S3 paths, always use LOCATION
    # For local paths with Hadoop catalog, let Iceberg manage the location
    # (it will be created under the warehouse directory)
    if table_location and table_location.startswith("s3://"):
        create_sql += f" LOCATION '{table_location}'"

    create_sql += """
        TBLPROPERTIES (
            'format'='parquet',
            'write.parquet.compression-codec'='snappy',
            'format-version'='2',
            'write.metadata.compression-codec'='none'
        )
    """

    spark.sql(create_sql).collect()
    print(f"Iceberg table {table_name} created successfully")


def get_table_count(spark: SparkSession, table_name: str) -> dict:
    """
    Get count of features by type from the table.

    Args:
        spark: Spark session
        table_name: Full table name

    Returns:
        Dictionary with type as key and count as value
    """
    result = spark.sql(f"SELECT type, COUNT(*) as count FROM {table_name} GROUP BY type").collect()

    return {row["type"]: row["count"] for row in result}


def merge_into_table(
    spark: SparkSession,
    table_name: str,
    source_view: str,
    match_condition: str,
    update_set: str = "*",
    insert: bool = True,
):
    """
    Merge data from a source view into an Iceberg table.

    Args:
        spark: Spark session
        table_name: Target table name
        source_view: Source view name
        match_condition: ON clause condition
        update_set: SET clause for UPDATE (default: '*' for all columns)
        insert: Whether to include INSERT clause
    """
    merge_sql = f"""
        MERGE INTO {table_name} t
        USING {source_view} s
        ON {match_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
    """

    if insert:
        merge_sql += " WHEN NOT MATCHED THEN INSERT *"

    spark.sql(merge_sql)


def delete_from_table(spark: SparkSession, table_name: str, source_view: str, match_condition: str):
    """
    Delete records from an Iceberg table based on source view.

    Args:
        spark: Spark session
        table_name: Target table name
        source_view: Source view with IDs to delete
        match_condition: ON clause condition
    """
    delete_sql = f"""
        MERGE INTO {table_name} t
        USING {source_view} s
        ON {match_condition}
        WHEN MATCHED THEN DELETE
    """

    spark.sql(delete_sql)


def get_table_snapshots(spark: SparkSession, table_name: str):
    """
    Get snapshots for a table (for time travel).

    Args:
        spark: Spark session
        table_name: Full table name

    Returns:
        DataFrame with snapshot information
    """
    return spark.sql(f"SELECT * FROM {table_name}.snapshots")


def optimize_table(spark: SparkSession, table_name: str):
    """
    Optimize Iceberg table (rewrite data files).

    Args:
        spark: Spark session
        table_name: Full table name
    """
    print(f"Optimizing table {table_name}...")
    spark.sql(f"CALL catalog.system.rewrite_data_files('{table_name}')").collect()
    print("Table optimization complete")


def expire_snapshots(spark: SparkSession, table_name: str, older_than_days: int = 30):
    """
    Expire old snapshots from the table.

    Args:
        spark: Spark session
        table_name: Full table name
        older_than_days: Expire snapshots older than this many days
    """
    print(f"Expiring snapshots older than {older_than_days} days...")
    spark.sql(
        f"CALL catalog.system.expire_snapshots('{table_name}', "
        f"TIMESTAMP '{older_than_days} days ago')"
    ).collect()
    print("Snapshot expiration complete")
