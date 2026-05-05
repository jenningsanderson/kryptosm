#!/usr/bin/env python3
"""
E2E Test Stage 2: Build Ways
Tests building way geometries by reading nodes from Iceberg and ways from Parquet.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.spark import create_spark_session_for_testing
from kryptosm.iceberg import table_exists, get_table_count
from kryptosm.geometry import (
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    prepare_for_iceberg,
)


# Paths
TEST_PARQUET_PATH = Path(__file__).parent / "data" / "dc.parquet"
OUTPUT_DIR = Path(__file__).parent / "data" / "output"
WAREHOUSE_DIR = OUTPUT_DIR / "warehouse"
TABLE_NAME = "hadoop_catalog.test_db.e2e_nodes"  # Same table as nodes


def test_build_ways():
    """Test building ways from Parquet and Iceberg nodes."""
    print("=" * 70)
    print("E2E TEST STAGE 2: BUILD WAYS")
    print("=" * 70)

    print(f"\nInput:  {TEST_PARQUET_PATH}/type=way/")
    print(f"Nodes:  {TABLE_NAME} (from Stage 1)")
    print(f"Output: {WAREHOUSE_DIR}")
    print(f"Table:  {TABLE_NAME}")

    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session_for_testing(str(WAREHOUSE_DIR), use_sedona_jars=True)
    print("   Spark session created")

    try:
        # Check if nodes table exists
        print("\n2. Checking for nodes from Stage 1...")
        if not table_exists(spark, TABLE_NAME):
            print(f"   ERROR: Table {TABLE_NAME} does not exist!")
            print("   Please run test_e2e_nodes.py first.")
            spark.stop()
            assert False, "Test failed"

        counts = get_table_count(spark, TABLE_NAME)
        node_count = counts.get("node", 0)
        print(f"   Found {node_count:,} nodes in Iceberg table")

        if node_count == 0:
            print("   ERROR: No nodes found. Run Stage 1 first.")
            spark.stop()
            assert False, "Test failed"

        # Load nodes from Iceberg
        print("\n3. Loading nodes from Iceberg...")
        spark.sql(f"""
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon, 
                   refs, members, latest_ts, 
                   ST_GeomFromWKB(geometry) AS geom
            FROM {TABLE_NAME}
            WHERE type = 'node'
        """).createOrReplaceTempView("nodes_with_geom")
        print(f"   Loaded {node_count:,} node geometries")

        # Read ways from Parquet
        print("\n4. Reading way Parquet data...")
        ways_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=way"))
        way_count = ways_df.count()
        print(f"   Loaded {way_count:,} ways")
        ways_df.createOrReplaceTempView("input_ways")

        # Build way geometries
        print("\n5. Building way linestrings...")
        build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
        lines_count = spark.sql(
            "SELECT COUNT(*) as c FROM ways_linestrings WHERE geom IS NOT NULL"
        ).collect()[0]["c"]
        print(f"   Built {lines_count:,} linestrings")

        print("\n6. Building way polygons...")
        build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
        geom_count = spark.sql(
            "SELECT COUNT(*) as c FROM ways_with_geom WHERE geom IS NOT NULL"
        ).collect()[0]["c"]
        print(f"   Built {geom_count:,} way geometries")

        # Prepare for Iceberg
        print("\n7. Preparing ways for Iceberg...")
        prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final", partition_number=4)
        final_count = spark.sql("SELECT COUNT(*) as c FROM ways_final").collect()[0]["c"]
        print(f"   Prepared {final_count:,} ways")

        # Write to Iceberg
        print("\n8. Writing ways to Iceberg...")
        spark.sql("SELECT * FROM ways_final").writeTo(TABLE_NAME).using("iceberg").append()
        print("   Write complete")

        # Verify
        print("\n9. Verifying...")
        counts = get_table_count(spark, TABLE_NAME)
        print(f"   Table counts: {counts}")

        assert counts.get("node", 0) == node_count, "Node count should be unchanged"
        assert counts.get("way", 0) > 0, "Should have ways in table"

        # Show sample
        print("\n10. Sample ways:")
        spark.sql(f"""
            SELECT id, tags, ST_GeometryType(ST_GeomFromWKB(geometry)) as geom_type
            FROM {TABLE_NAME}
            WHERE type = 'way' AND geometry IS NOT NULL
            LIMIT 5
        """).show(truncate=False)

        print("\n" + "=" * 70)
        print("STAGE 2 PASSED: Ways built successfully!")
        print("=" * 70)
        print(f"\nOutput location: {WAREHOUSE_DIR}")
        print(f"Table: {TABLE_NAME}")
        print(f"Total features: {sum(counts.values()):,}")
        print("\nNext step: Run test_e2e_relations.py to build relations")
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
    test_build_ways()
    sys.exit(0)
