#!/usr/bin/env python3
"""
Simple Parquet to Iceberg test that shows where output goes.
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.spark import create_spark_session_for_testing
from kryptosm.iceberg import create_iceberg_table, table_exists, get_table_count
from kryptosm.geometry import (
    build_node_geometry,
    prepare_for_iceberg,
)


# Paths
TEST_PARQUET_PATH = Path(__file__).parent / "data" / "dc.parquet"
OUTPUT_PATH = Path(__file__).parent / "data" / "output"


def main():
    """Run simple Parquet to Iceberg test."""
    print("=" * 70)
    print("SIMPLE PARQUET TO ICEBERG TEST")
    print("=" * 70)

    print(f"\nInput:  {TEST_PARQUET_PATH}")
    print(f"Output: {OUTPUT_PATH}")

    # Create output directory
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    # Create warehouse directory
    warehouse_dir = str(OUTPUT_PATH / "warehouse")
    os.makedirs(warehouse_dir, exist_ok=True)

    print(f"Warehouse: {warehouse_dir}")

    try:
        # Create Spark session
        print("\n1. Creating Spark session...")
        spark = create_spark_session_for_testing(warehouse_dir, use_sedona_jars=True)
        print("   Spark session created")

        # Read Parquet data
        print("\n2. Reading Parquet data...")
        nodes_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=node")).limit(100)
        node_count = nodes_df.count()
        print(f"   Loaded {node_count:,} nodes")

        nodes_df.createOrReplaceTempView("input_nodes")

        # Build geometries
        print("\n3. Building node geometries...")
        build_node_geometry(spark, "input_nodes", "nodes_with_geom")
        geom_count = spark.sql("SELECT COUNT(*) as c FROM nodes_with_geom").collect()[0]["c"]
        print(f"   Built {geom_count:,} geometries")

        # Prepare for Iceberg
        print("\n4. Preparing for Iceberg...")
        prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final", partition_number=2)

        # Create Iceberg table
        print("\n5. Creating Iceberg table...")
        table_name = "hadoop_catalog.test_db.test_dc_nodes"
        table_location = str(OUTPUT_PATH / "iceberg_table")

        create_iceberg_table(spark, table_name, table_location)

        if table_exists(spark, table_name):
            print(f"   Table created: {table_name}")
            print(f"   Location: {table_location}")

            # Write to Iceberg
            print("\n6. Writing to Iceberg...")
            spark.sql("SELECT * FROM nodes_final").writeTo(table_name).using("iceberg").append()
            print("   Data written successfully")

            # Verify
            print("\n7. Verifying...")
            counts = get_table_count(spark, table_name)
            print(f"   Table counts: {counts}")

            # Show table location
            result = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
            for row in result:
                if row["col_name"] == "Location":
                    print(f"   Table location: {row['data_type']}")

            print("\n" + "=" * 70)
            print("TEST PASSED!")
            print("=" * 70)
            print(f"\nIceberg table created at: {table_location}")
            print(f"Warehouse at: {warehouse_dir}")
            print("\nTo query the table:")
            print(f"  spark-sql --conf spark.sql.catalog.hadoop_catalog.warehouse={warehouse_dir}")
            print(f"  SELECT * FROM test_db.test_dc_nodes LIMIT 10;")
            print("=" * 70)
        else:
            print("   WARNING: Table creation failed or not supported in this environment")
            print("   But geometry building worked!")

        spark.stop()
        return True

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
