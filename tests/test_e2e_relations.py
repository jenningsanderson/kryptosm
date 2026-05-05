#!/usr/bin/env python3
"""
E2E Test Stage 3: Build Relations
Tests building relation geometries by reading nodes and ways from Iceberg
and relations from Parquet.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.spark import create_spark_session_for_testing
from kryptosm.iceberg import table_exists, get_table_count
from kryptosm.geometry import (
    relations_need_geometry,
    construct_multipolygon,
    relation_merge_geometry_data,
    prepare_for_iceberg,
)


# Paths
TEST_PARQUET_PATH = Path(__file__).parent / "data" / "dc.parquet"
OUTPUT_DIR = Path(__file__).parent / "data" / "output"
WAREHOUSE_DIR = OUTPUT_DIR / "warehouse"
TABLE_NAME = "hadoop_catalog.test_db.e2e_nodes"  # Same table as nodes and ways


def test_build_relations():
    """Test building relations from Parquet and Iceberg data."""
    print("=" * 70)
    print("E2E TEST STAGE 3: BUILD RELATIONS")
    print("=" * 70)

    print(f"\nInput:  {TEST_PARQUET_PATH}/type=relation/")
    print(f"Nodes:  {TABLE_NAME} (from Stage 1)")
    print(f"Ways:   {TABLE_NAME} (from Stage 2)")
    print(f"Output: {WAREHOUSE_DIR}")
    print(f"Table:  {TABLE_NAME}")

    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session_for_testing(str(WAREHOUSE_DIR), use_sedona_jars=True)
    print("   Spark session created")

    try:
        # Check if nodes and ways exist
        print("\n2. Checking for nodes and ways from previous stages...")
        if not table_exists(spark, TABLE_NAME):
            print(f"   ERROR: Table {TABLE_NAME} does not exist!")
            print("   Please run test_e2e_nodes.py and test_e2e_ways.py first.")
            spark.stop()
            assert False, "Table does not exist"

        counts = get_table_count(spark, TABLE_NAME)
        node_count = counts.get("node", 0)
        way_count = counts.get("way", 0)
        print(f"   Found {node_count:,} nodes and {way_count:,} ways in Iceberg table")

        if node_count == 0 or way_count == 0:
            print("   ERROR: Missing nodes or ways. Run Stages 1 and 2 first.")
            spark.stop()
            assert False, "Missing nodes or ways"

        # Load ways from Iceberg
        print("\n3. Loading ways from Iceberg...")
        spark.sql(f"""
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon, 
                   refs, members, latest_ts, 
                   ST_GeomFromWKB(geometry) AS geom
            FROM {TABLE_NAME}
            WHERE type = 'way' AND geometry IS NOT NULL
        """).createOrReplaceTempView("ways_with_geom")
        way_geom_count = spark.sql("SELECT COUNT(*) as c FROM ways_with_geom").collect()[0]["c"]
        print(f"   Loaded {way_geom_count:,} way geometries")

        # Read relations from Parquet
        print("\n4. Reading relation Parquet data...")
        relations_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=relation"))
        relation_count = relations_df.count()
        print(f"   Loaded {relation_count:,} relations")
        relations_df.createOrReplaceTempView("input_relations")

        # Filter relations that need geometry
        print("\n5. Filtering relations that need geometry...")
        relations_need_geometry(spark, "input_relations", "relations_need_geom")
        rel_need_count = spark.sql("SELECT COUNT(*) as c FROM relations_need_geom").collect()[0][
            "c"
        ]
        print(f"   Relations needing geometry: {rel_need_count:,} (boundary/multipolygon)")

        if rel_need_count == 0:
            print("   No relations need geometry building. Test complete!")
            spark.stop()
            return

        # Build relation geometries
        print("\n6. Constructing multipolygons...")
        construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom")
        rel_geom_count = spark.sql(
            "SELECT COUNT(*) as c FROM relations_geom WHERE geom IS NOT NULL"
        ).collect()[0]["c"]
        print(f"   Built {rel_geom_count:,} relation geometries")

        print("\n7. Merging relation data...")
        relation_merge_geometry_data(
            spark, "input_relations", "relations_geom", "relations_with_geom"
        )

        # Prepare for Iceberg
        print("\n8. Preparing relations for Iceberg...")
        prepare_for_iceberg(
            spark, "relations_with_geom", "relation", "relations_final", partition_number=2
        )
        final_count = spark.sql("SELECT COUNT(*) as c FROM relations_final").collect()[0]["c"]
        print(f"   Prepared {final_count:,} relations")

        # Write to Iceberg
        print("\n9. Writing relations to Iceberg...")
        spark.sql("SELECT * FROM relations_final").writeTo(TABLE_NAME).using("iceberg").append()
        print("    Write complete")

        # Verify
        print("\n10. Verifying...")
        counts = get_table_count(spark, TABLE_NAME)
        print(f"\n    Final table contents:")
        for osm_type, count in counts.items():
            print(f"      {osm_type}: {count:,} features")

        assert counts.get("node", 0) == node_count, "Node count should be unchanged"
        assert counts.get("way", 0) == way_count, "Way count should be unchanged"
        assert counts.get("relation", 0) > 0, "Should have relations in table"

        # Show sample
        print("\n11. Sample relations:")
        spark.sql(f"""
            SELECT id, tags['type'] as rel_type, tags['name'] as name,
                   ST_GeometryType(ST_GeomFromWKB(geometry)) as geom_type
            FROM {TABLE_NAME}
            WHERE type = 'relation' AND geometry IS NOT NULL
            LIMIT 5
        """).show(truncate=False)

        print("\n" + "=" * 70)
        print("STAGE 3 PASSED: Relations built successfully!")
        print("=" * 70)
        print(f"\nOutput location: {WAREHOUSE_DIR}")
        print(f"Table: {TABLE_NAME}")
        print(f"Total features: {sum(counts.values()):,}")
        print("\nAll stages complete! Full OSM dataset built.")
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
        raise


if __name__ == "__main__":
    try:
        test_build_relations()
        sys.exit(0)
    except Exception:
        sys.exit(1)
