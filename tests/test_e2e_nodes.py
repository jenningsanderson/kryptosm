#!/usr/bin/env python3
"""
E2E Test Stage 1: Build Nodes
Tests building node geometries from Parquet and writing to Iceberg.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.geometry.iceberg_prep import prepare_for_iceberg
from kryptosm.geometry.nodes import build_node_geometry
from kryptosm.iceberg import create_iceberg_table, get_table_count, table_exists
from kryptosm.spark import create_spark_session_for_testing


# Paths
TEST_PARQUET_PATH = Path(__file__).parent / "data" / "dc.parquet"
OUTPUT_DIR = Path(__file__).parent / "data" / "output"
WAREHOUSE_DIR = OUTPUT_DIR / "warehouse"
TABLE_NAME = "hadoop_catalog.test_db.e2e_osm"


def test_build_nodes():
    """Test building nodes from Parquet to Iceberg."""
    print("=" * 70)
    print("E2E TEST STAGE 1: BUILD NODES")
    print("=" * 70)

    # Setup output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)

    print(f"\nInput:  {TEST_PARQUET_PATH}/type=node/")
    print(f"Output: {WAREHOUSE_DIR}")
    print(f"Table:  {TABLE_NAME}")

    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))
    print("   Spark session created")

    try:
        # Read Parquet data
        print("\n2. Reading node Parquet data...")
        nodes_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=node"))
        node_count = nodes_df.count()
        print(f"   Loaded {node_count:,} nodes")

        nodes_df.createOrReplaceTempView("input_nodes")

        # Create Iceberg table
        print("\n3. Creating Iceberg table...")
        create_iceberg_table(spark, TABLE_NAME)
        print(f"   Table created: {TABLE_NAME}")

        # Build node geometries
        print("\n4. Building node geometries...")
        build_node_geometry(spark, "input_nodes", "nodes_with_geom")
        geom_count = spark.sql("SELECT COUNT(*) as c FROM nodes_with_geom").collect()[0]["c"]
        print(f"   Built {geom_count:,} geometries")

        # Prepare for Iceberg
        print("\n5. Preparing for Iceberg...")
        prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
        final_count = spark.sql("SELECT COUNT(*) as c FROM nodes_final").collect()[0]["c"]
        print(f"   Prepared {final_count:,} nodes")

        # Write to Iceberg
        print("\n6. Writing to Iceberg...")
        spark.sql("SELECT * FROM nodes_final").writeTo(TABLE_NAME).using("iceberg").append()
        print("   Write complete")

        # Verify
        print("\n7. Verifying...")
        counts = get_table_count(spark, TABLE_NAME)
        print(f"   Table counts: {counts}")

        assert counts.get("node", 0) == node_count, (
            f"Expected {node_count} nodes, got {counts.get('node', 0)}"
        )

        # Show sample
        print("\n8. Sample data:")
        spark.sql(f"""
            SELECT id, tags, bbox.xmin as lon, bbox.ymin as lat
            FROM {TABLE_NAME}
            WHERE cardinality(tags) > 0
            LIMIT 5
        """).show(truncate=False)

        print("\n" + "=" * 70)
        print("STAGE 1 PASSED: Nodes built successfully!")
        print("=" * 70)
        print(f"\nOutput location: {WAREHOUSE_DIR}")
        print(f"Table: {TABLE_NAME}")
        print("\nNext step: Run test_e2e_ways.py to build ways")
        print("=" * 70)

        spark.stop()
        return

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        try:
            spark.stop()
        except:
            pass
        assert False, "Test failed"


if __name__ == "__main__":
    try:
        test_build_nodes(); sys.exit(0)
    except Exception:
        sys.exit(1)
