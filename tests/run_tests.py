#!/usr/bin/env python3
"""
Test runner for kryptosm.
"""

import sys
import os
import tempfile
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.spark import create_spark_session_for_testing
from kryptosm.iceberg import create_iceberg_table, table_exists, get_table_count
from kryptosm.geometry import build_node_geometry, prepare_for_iceberg

import pyspark.sql.types as T


def test_basic_workflow():
    """Test basic workflow with sample data."""
    print("=" * 60)
    print("Running basic workflow test...")
    print("=" * 60)

    # Create temporary warehouse
    warehouse_dir = tempfile.mkdtemp(prefix="iceberg_test_")
    print(f"Warehouse directory: {warehouse_dir}")

    try:
        # Create Spark session
        print("\nCreating Spark session...")
        spark = create_spark_session_for_testing(warehouse_dir)
        print("Spark session created")

        # Create test data
        print("\nCreating test data...")
        data = [
            (
                1,
                1,
                "2024-01-01T00:00:00Z",
                100,
                "testuser",
                1000,
                {"amenity": "cafe"},
                37.7749,
                -122.4194,
                None,
                None,
                "2024-01-01T00:00:00Z",
            ),
            (
                2,
                1,
                "2024-01-01T00:00:00Z",
                101,
                "testuser",
                1001,
                {"shop": "supermarket"},
                37.7750,
                -122.4195,
                None,
                None,
                "2024-01-01T00:00:00Z",
            ),
            (
                3,
                1,
                "2024-01-01T00:00:00Z",
                102,
                "testuser",
                1002,
                {},
                37.7751,
                -122.4196,
                None,
                None,
                "2024-01-01T00:00:00Z",
            ),
        ]

        schema = T.StructType(
            [
                T.StructField("id", T.LongType(), False),
                T.StructField("version", T.LongType(), True),
                T.StructField("timestamp", T.StringType(), True),
                T.StructField("uid", T.LongType(), True),
                T.StructField("user", T.StringType(), True),
                T.StructField("changeset", T.LongType(), True),
                T.StructField("tags", T.MapType(T.StringType(), T.StringType()), True),
                T.StructField("lat", T.DoubleType(), True),
                T.StructField("lon", T.DoubleType(), True),
                T.StructField("refs", T.ArrayType(T.LongType()), True),
                T.StructField("members", T.ArrayType(T.StringType()), True),
                T.StructField("latest_ts", T.StringType(), True),
            ]
        )

        df = spark.createDataFrame(data, schema)
        df.createOrReplaceTempView("test_nodes")
        print(f"Created test data with {df.count()} nodes")

        # Test geometry building
        print("\nBuilding node geometries...")
        build_node_geometry(spark, "test_nodes", "test_nodes_geom")
        geom_count = spark.sql("SELECT COUNT(*) as c FROM test_nodes_geom").collect()[0]["c"]
        print(f"Built geometries for {geom_count} nodes")

        # Test Iceberg preparation
        print("\nPreparing for Iceberg...")
        prepare_for_iceberg(
            spark, "test_nodes_geom", "node", "test_nodes_iceberg", partition_number=2
        )
        iceberg_count = spark.sql("SELECT COUNT(*) as c FROM test_nodes_iceberg").collect()[0]["c"]
        print(f"Prepared {iceberg_count} nodes for Iceberg")

        # Test table creation
        print("\nCreating Iceberg table...")
        table_name = "test_db.test_osm"
        create_iceberg_table(spark, table_name)
        assert table_exists(spark, table_name), "Table should exist"
        print("Table created successfully")

        # Test writing to table
        print("\nWriting to Iceberg table...")
        spark.sql("SELECT * FROM test_nodes_iceberg").writeTo(table_name).using("iceberg").append()
        print("Data written to table")

        # Verify
        print("\nVerifying data...")
        counts = get_table_count(spark, table_name)
        print(f"Table counts: {counts}")
        assert counts.get("node", 0) == 3, f"Expected 3 nodes, got {counts.get('node', 0)}"
        print("Data verification passed")

        # Clean up
        print("\nCleaning up...")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

        print("\n" + "=" * 60)
        print("All tests passed!")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Clean up warehouse
        shutil.rmtree(warehouse_dir, ignore_errors=True)
        print(f"\nCleaned up warehouse directory: {warehouse_dir}")


def main():
    """Run tests."""
    success = test_basic_workflow()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
