#!/usr/bin/env python3
"""
E2E Init Test: build the full OSM table from Parquet with per-stage timing.

Set KRYPTOSM_REGION=oregon to use Oregon data (default: dc).
"""

from kryptosm import (
    build_linestring_for_ways,
    build_node_geometry,
    build_ways_geometry_from_linestring,
    construct_multipolygon,
    create_iceberg_table,
    create_index_tables,
    flatten_way_refs,
    get_table_count,
    load_with_geom,
    populate_node_to_ways,
    populate_way_to_relations,
    prepare_for_iceberg,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from pyspark.sql.functions import col
from tests import (
    WAREHOUSE_DIR,
    configure_logging,
    create_spark_session_for_testing,
    get_region,
    stage,
)


def test_init():
    configure_logging()
    region = get_region()

    print(f"\n{'=' * 70}")
    print(f"E2E INIT TEST — {region.db_name}")
    print(f"{'=' * 70}")
    print(f"Input:     {region.parquet_path}")
    print(f"Table:     {region.table_name}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))

    try:
        with stage("Create Iceberg table + index tables"):
            create_iceberg_table(spark, region.table_name)
            create_index_tables(spark, region.node_to_ways, region.way_to_relations)

        with stage("Register input Parquet views"):
            spark.read.parquet(str(region.parquet_path / "type=node")).createOrReplaceTempView(
                "input_nodes"
            )
            spark.read.parquet(str(region.parquet_path / "type=way")).createOrReplaceTempView(
                "input_ways_raw"
            )
            flatten_way_refs(spark, "input_ways_raw", "input_ways")
            spark.read.parquet(str(region.parquet_path / "type=relation")).createOrReplaceTempView(
                "input_relations"
            )

        with stage("Build + write nodes"):
            build_node_geometry(spark, "input_nodes", "nodes_with_geom")
            prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
            spark.sql("SELECT * FROM nodes_final") \
                .repartitionByRange(20, col("id")) \
                .writeTo(region.table_name).using("iceberg").append()
            load_with_geom(spark, region.table_name, "node", "nodes_with_geom")

        with stage("Build + write ways"):
            build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
            build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
            prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
            spark.sql("SELECT * FROM ways_final") \
                .repartitionByRange(10, col("id")) \
                .writeTo(region.table_name).using("iceberg").append()
            load_with_geom(spark, region.table_name, "way", "ways_with_geom")

        with stage("Populate node_to_ways index"):
            populate_node_to_ways(spark, region.table_name, region.node_to_ways)

        with stage("Build + write relations"):
            relations_need_geometry(spark, "input_relations", "relations_need_geom")
            construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom",
                                   nodes_geometry="nodes_with_geom")
            relation_merge_geometry_data(
                spark, "input_relations", "relations_geom", "relations_with_geom"
            )
            prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
            spark.sql("SELECT * FROM relations_final") \
                .writeTo(region.table_name).using("iceberg").append()

        with stage("Populate way_to_relations index"):
            populate_way_to_relations(spark, region.table_name, region.way_to_relations)

        with stage("Final count summary"):
            counts = get_table_count(spark, region.table_name)

        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            print(f"  {osm_type:9}: {counts.get(osm_type, 0):>14,}")
        print(f"  {'total':9}: {sum(counts.values()):>14,}")
        print("=" * 70)

        assert counts.get("node", 0) > 0
        assert counts.get("way", 0) > 0
        assert counts.get("relation", 0) > 0
    finally:
        spark.stop()


if __name__ == "__main__":
    test_init()
