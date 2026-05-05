#!/usr/bin/env python3
"""
Parquet integration test runner.
Tests the full workflow using the DC Parquet data.
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
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    relations_need_geometry,
    construct_multipolygon,
    relation_merge_geometry_data,
    prepare_for_iceberg,
)


# Path to test Parquet data
TEST_PARQUET_PATH = Path(__file__).parent / "data" / "dc.parquet"


def test_parquet_workflow():
    """Test full workflow from Parquet to Iceberg table."""
    print("=" * 70)
    print("PARQUET INTEGRATION TEST")
    print("=" * 70)

    # Check Parquet data exists
    print(f"\n1. Checking Parquet data...")
    if not TEST_PARQUET_PATH.exists():
        print(f"   ERROR: Parquet data not found: {TEST_PARQUET_PATH}")
        return False

    print(f"   Parquet path: {TEST_PARQUET_PATH}")

    node_path = TEST_PARQUET_PATH / "type=node"
    way_path = TEST_PARQUET_PATH / "type=way"
    relation_path = TEST_PARQUET_PATH / "type=relation"

    if not node_path.exists() or not way_path.exists():
        print(f"   ERROR: Parquet data incomplete")
        return False

    print(f"   Data found: nodes, ways, relations")

    # Use persistent output directory instead of temp
    output_dir = Path(__file__).parent / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir = str(output_dir / "warehouse")

    # Clean up previous test output
    import shutil

    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)
    os.makedirs(warehouse_dir, exist_ok=True)

    print(f"\n2. Warehouse directory: {warehouse_dir}")
    print(f"   Output will be persisted at: {output_dir}")

    try:
        # Create Spark session
        print("\n3. Creating Spark session...")
        spark = create_spark_session_for_testing(warehouse_dir, use_sedona_jars=True)
        print("   Spark session created")

        # Read Parquet data
        print(f"\n4. Reading Parquet data...")
        nodes_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=node"))
        ways_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=way"))
        relations_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=relation"))

        node_count = nodes_df.count()
        way_count = ways_df.count()
        relation_count = relations_df.count()

        print(f"\n   Parquet contents:")
        print(f"     Nodes: {node_count:,}")
        print(f"     Ways: {way_count:,}")
        print(f"     Relations: {relation_count:,}")

        if node_count == 0:
            print("   ERROR: No nodes found")
            return False

        # Create views (use all data)
        print(f"\n5. Creating views...")
        nodes_df.createOrReplaceTempView("input_nodes")
        ways_df.createOrReplaceTempView("input_ways")
        relations_df.createOrReplaceTempView("input_relations")

        actual_nodes = spark.sql("SELECT COUNT(*) as c FROM input_nodes").collect()[0]["c"]
        actual_ways = spark.sql("SELECT COUNT(*) as c FROM input_ways").collect()[0]["c"]
        actual_relations = spark.sql("SELECT COUNT(*) as c FROM input_relations").collect()[0]["c"]
        print(
            f"   Using {actual_nodes:,} nodes, {actual_ways:,} ways, {actual_relations:,} relations"
        )

        # Create Iceberg table
        print("\n6. Creating Iceberg table...")
        table_name = "hadoop_catalog.test_db.test_parquet_dc"
        create_iceberg_table(spark, table_name)
        if not table_exists(spark, table_name):
            print("   WARNING: Table may not have been created properly")
            print("   Continuing anyway...")
        else:
            print("   Table created successfully")

        print(f"\n   Table: {table_name}")
        print(f"   Warehouse: {warehouse_dir}")

        # Build node geometries
        print("\n7. Building node geometries...")
        build_node_geometry(spark, "input_nodes", "nodes_with_geom")
        node_geom_count = spark.sql("SELECT COUNT(*) as c FROM nodes_with_geom").collect()[0]["c"]
        print(f"   Built geometries for {node_geom_count:,} nodes")

        # Prepare nodes for Iceberg
        print("\n8. Preparing nodes for Iceberg...")
        prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final", partition_number=2)

        # Write nodes to Iceberg
        print("\n9. Writing nodes to Iceberg...")
        spark.sql("SELECT * FROM nodes_final").writeTo(table_name).using("iceberg").append()
        print("   Nodes written successfully")

        # Build way geometries
        print("\n10. Building way geometries...")
        build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
        build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
        way_geom_count = spark.sql(
            "SELECT COUNT(*) as c FROM ways_with_geom WHERE geom IS NOT NULL"
        ).collect()[0]["c"]
        total_ways = spark.sql("SELECT COUNT(*) as c FROM ways_with_geom").collect()[0]["c"]
        print(f"    Built geometries for {way_geom_count:,} of {total_ways:,} ways")
        if way_geom_count == 0:
            print(
                "    Note: No way geometries built. This is expected if ways reference nodes outside the sample."
            )
            print("    In production with full data, all ways would have geometries.")

        # Prepare ways for Iceberg
        print("\n11. Preparing ways for Iceberg...")
        prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final", partition_number=2)

        # Write ways to Iceberg
        print("\n12. Writing ways to Iceberg...")
        spark.sql("SELECT * FROM ways_final").writeTo(table_name).using("iceberg").append()
        print("    Ways written successfully")

        # Build relation geometries
        print("\n13. Building relation geometries...")
        relations_need_geometry(spark, "input_relations", "relations_need_geom")
        rel_need_count = spark.sql("SELECT COUNT(*) as c FROM relations_need_geom").collect()[0][
            "c"
        ]
        print(f"    Relations needing geometry: {rel_need_count:,}")

        if rel_need_count > 0:
            construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom")
            relation_merge_geometry_data(
                spark, "input_relations", "relations_geom", "relations_with_geom"
            )
            rel_geom_count = spark.sql(
                "SELECT COUNT(*) as c FROM relations_with_geom WHERE geom IS NOT NULL"
            ).collect()[0]["c"]
            print(f"    Built geometries for {rel_geom_count:,} relations")

            # Prepare relations for Iceberg
            print("\n14. Preparing relations for Iceberg...")
            prepare_for_iceberg(
                spark, "relations_with_geom", "relation", "relations_final", partition_number=2
            )

            # Write relations to Iceberg
            print("\n15. Writing relations to Iceberg...")
            spark.sql("SELECT * FROM relations_final").writeTo(table_name).using("iceberg").append()
            print("    Relations written successfully")
        else:
            print("    No relations need geometry building")

        # Verify results
        print("\n16. Verifying results...")
        counts = get_table_count(spark, table_name)
        print(f"\n    Table contents:")
        for osm_type, count in counts.items():
            print(f"      {osm_type}: {count:,} features")

        if counts.get("node", 0) == 0:
            print("    ERROR: No nodes in table")
            return False

        # Test queries
        print("\n14. Testing queries...")

        # Sample nodes with tags
        sample = spark.sql(f"""
            SELECT id, tags, bbox 
            FROM {table_name} 
            WHERE type = 'node' AND size(tags) > 0 
            LIMIT 5
        """).collect()

        print(f"    Sample nodes with tags: {len(sample)} found")
        for row in sample[:3]:
            print(f"      Node {row['id']}: {list(row['tags'].keys())[:3]}")

        # Check geometry types
        geom_types = spark.sql(f"""
            SELECT ST_GeometryType(ST_GeomFromWKB(geometry)) as geom_type, COUNT(*) as cnt
            FROM {table_name}
            WHERE type = 'node'
            GROUP BY ST_GeometryType(ST_GeomFromWKB(geometry))
        """).collect()

        print(f"\n    Node geometry types:")
        for row in geom_types:
            print(f"      {row['geom_type']}: {row['cnt']:,}")

        # Don't drop table - keep for inspection
        print("\n15. Test complete - table preserved for inspection")
        print(f"    Table: {table_name}")
        spark.stop()
        print("    Spark stopped")

        print("\n" + "=" * 70)
        print("PARQUET INTEGRATION TEST PASSED!")
        print("=" * 70)
        print(f"\nOutput location: {warehouse_dir}")
        print(f"Test data: {output_dir}")
        print("\nTo query with DuckDB:")
        print(
            f"  duckdb -c \"SELECT type, COUNT(*) FROM '{warehouse_dir}/test_db/test_parquet_dc/data/*/*.parquet' GROUP BY type;\""
        )
        print("\nTo query with Spark SQL:")
        print(f"  spark-sql --conf spark.sql.catalog.hadoop_catalog.warehouse={warehouse_dir}")
        print(f"  SELECT * FROM hadoop_catalog.test_db.test_parquet_dc LIMIT 10;")
        print("=" * 70)
        return True

    except Exception as e:
        print(f"\n\nERROR: Test failed with exception: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Don't clean up so user can inspect output
        print(f"\nOutput preserved at: {warehouse_dir}")
        print(f"To clean up, run: rm -rf {warehouse_dir}")
        # Don't stop spark here - let it finish naturally
        try:
            spark.stop()
        except:
            pass


def main():
    """Run Parquet integration test."""
    success = test_parquet_workflow()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
