"""
Integration test using OSM Parquet data.
Tests the full workflow from Parquet to Iceberg table.
"""

import os
import tempfile
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure output directory exists
OUTPUT_DIR = Path(__file__).parent / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

from kryptosm.spark import create_spark_session_for_testing

pytestmark = pytest.mark.spark
from kryptosm.iceberg import (
    create_iceberg_table,
    table_exists,
    get_table_count,
)
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


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    # Use persistent directory for output
    warehouse_dir = "/tmp/iceberg_parquet_test_output"
    os.makedirs(warehouse_dir, exist_ok=True)
    spark = create_spark_session_for_testing(warehouse_dir)
    yield spark
    spark.stop()
    # Don't clean up - preserve output for inspection
    print(f"\nTest warehouse preserved at: {warehouse_dir}")


def test_parquet_data_exists():
    """Test that the Parquet data exists."""
    assert TEST_PARQUET_PATH.exists(), f"Parquet data not found: {TEST_PARQUET_PATH}"

    node_path = TEST_PARQUET_PATH / "type=node"
    way_path = TEST_PARQUET_PATH / "type=way"
    relation_path = TEST_PARQUET_PATH / "type=relation"

    assert node_path.exists(), f"Node data not found: {node_path}"
    assert way_path.exists(), f"Way data not found: {way_path}"
    assert relation_path.exists(), f"Relation data not found: {relation_path}"

    print(f"\nParquet data found:")
    print(f"  Nodes: {node_path}")
    print(f"  Ways: {way_path}")
    print(f"  Relations: {relation_path}")


def test_parquet_read(spark):
    """Test reading Parquet data."""
    print("\n" + "=" * 60)
    print("Testing Parquet read")
    print("=" * 60)

    # Read nodes
    print("\nReading nodes...")
    nodes_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=node"))
    node_count = nodes_df.count()
    print(f"  Nodes: {node_count:,}")
    assert node_count > 0, "Should have at least one node"

    # Check schema
    nodes_schema = nodes_df.schema
    field_names = [f.name for f in nodes_schema.fields]
    assert "id" in field_names
    assert "lat" in field_names
    assert "lon" in field_names
    assert "tags" in field_names
    print(f"  Node schema: {field_names}")

    # Read ways
    print("\nReading ways...")
    ways_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=way"))
    way_count = ways_df.count()
    print(f"  Ways: {way_count:,}")
    assert way_count > 0, "Should have at least one way"

    # Read relations
    print("\nReading relations...")
    relations_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=relation"))
    relation_count = relations_df.count()
    print(f"  Relations: {relation_count:,}")

    print("\nParquet read test passed!")


def test_geometry_building(spark):
    """Test geometry building from Parquet data."""
    print("\n" + "=" * 60)
    print("Testing geometry building")
    print("=" * 60)

    # Read Parquet data (use all data, limit output only)
    print("\n1. Reading Parquet data...")
    nodes_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=node"))
    ways_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=way"))
    relations_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=relation"))

    node_count = nodes_df.count()
    way_count = ways_df.count()
    relation_count = relations_df.count()

    print(f"   Loaded {node_count:,} nodes, {way_count:,} ways, {relation_count:,} relations")

    # Create views
    nodes_df.createOrReplaceTempView("input_nodes")
    ways_df.createOrReplaceTempView("input_ways")
    relations_df.createOrReplaceTempView("input_relations")

    # Build node geometries
    print("\n2. Building node geometries...")
    build_node_geometry(spark, "input_nodes", "nodes_with_geom")
    node_geom_count = spark.sql("SELECT COUNT(*) as c FROM nodes_with_geom").collect()[0]["c"]
    print(f"   Built geometries for {node_geom_count:,} nodes")
    assert node_geom_count > 0, "Should have built node geometries"

    # Prepare nodes for Iceberg
    print("\n3. Preparing nodes for Iceberg...")
    prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final", partition_number=2)
    nodes_final_count = spark.sql("SELECT COUNT(*) as c FROM nodes_final").collect()[0]["c"]
    print(f"   Prepared {nodes_final_count:,} nodes")
    assert nodes_final_count > 0, "Should have prepared nodes"

    # Verify nodes_final schema
    nodes_final = spark.sql("SELECT * FROM nodes_final LIMIT 1")
    field_names = [f.name for f in nodes_final.schema.fields]
    assert "id" in field_names
    assert "type" in field_names
    assert "geometry" in field_names
    assert "bbox" in field_names
    print(f"   Schema verified: {field_names}")

    # Build way geometries
    print("\n4. Building way geometries...")
    build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
    build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
    way_geom_count = spark.sql(
        "SELECT COUNT(*) as c FROM ways_with_geom WHERE geom IS NOT NULL"
    ).collect()[0]["c"]
    print(f"   Built geometries for {way_geom_count:,} ways")

    # Prepare ways for Iceberg
    print("\n5. Preparing ways for Iceberg...")
    prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final", partition_number=2)
    ways_final_count = spark.sql("SELECT COUNT(*) as c FROM ways_final").collect()[0]["c"]
    print(f"   Prepared {ways_final_count:,} ways")

    # Build relation geometries
    print("\n6. Building relation geometries...")
    relations_need_geometry(spark, "input_relations", "relations_need_geom")
    rel_need_count = spark.sql("SELECT COUNT(*) as c FROM relations_need_geom").collect()[0]["c"]
    print(f"   Relations needing geometry: {rel_need_count:,}")

    if rel_need_count > 0:
        construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom")
        relation_merge_geometry_data(
            spark, "input_relations", "relations_geom", "relations_with_geom"
        )
        rel_geom_count = spark.sql(
            "SELECT COUNT(*) as c FROM relations_with_geom WHERE geom IS NOT NULL"
        ).collect()[0]["c"]
        print(f"   Built geometries for {rel_geom_count:,} relations")

        # Prepare relations for Iceberg
        print("\n7. Preparing relations for Iceberg...")
        prepare_for_iceberg(
            spark, "relations_with_geom", "relation", "relations_final", partition_number=2
        )
        rel_final_count = spark.sql("SELECT COUNT(*) as c FROM relations_final").collect()[0]["c"]
        print(f"   Prepared {rel_final_count:,} relations")
    else:
        print("   No relations need geometry building")

    print("\n" + "=" * 60)
    print("Geometry building test PASSED!")
    print("=" * 60)


def test_geometry_types(spark):
    """Test that geometries are correctly typed."""
    print("\n" + "=" * 60)
    print("Testing geometry types")
    print("=" * 60)

    # Read samples (limit for speed, but enough to get valid data)
    nodes_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=node")).limit(1000)
    ways_df = spark.read.parquet(str(TEST_PARQUET_PATH / "type=way")).limit(1000)

    nodes_df.createOrReplaceTempView("sample_nodes")
    ways_df.createOrReplaceTempView("sample_ways")

    # Build geometries
    build_node_geometry(spark, "sample_nodes", "sample_nodes_geom")
    build_linestring_for_ways(spark, "sample_ways", "sample_nodes_geom", "sample_ways_lines")
    build_ways_geometry_from_linestring(spark, "sample_ways_lines", "sample_ways_geom")

    # Check node geometries are Points
    node_geoms = spark.sql("""
        SELECT ST_GeometryType(geom) as geom_type, COUNT(*) as cnt
        FROM sample_nodes_geom
        GROUP BY ST_GeometryType(geom)
    """).collect()

    print("\nNode geometry types:")
    for row in node_geoms:
        print(f"  {row['geom_type']}: {row['cnt']}")
        assert row["geom_type"] == "ST_Point", f"Expected ST_Point, got {row['geom_type']}"

    # Check way geometries
    way_geoms = spark.sql("""
        SELECT ST_GeometryType(geom) as geom_type, COUNT(*) as cnt
        FROM sample_ways_geom
        WHERE geom IS NOT NULL
        GROUP BY ST_GeometryType(geom)
    """).collect()

    print("\nWay geometry types:")
    for row in way_geoms:
        print(f"  {row['geom_type']}: {row['cnt']}")
        assert row["geom_type"] in ("ST_LineString", "ST_Polygon"), (
            f"Expected LineString or Polygon, got {row['geom_type']}"
        )

    print("\nGeometry types test passed!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
