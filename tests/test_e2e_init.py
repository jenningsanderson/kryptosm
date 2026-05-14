#!/usr/bin/env python3
"""
E2E Init Test: build the full Krypton database from Parquet with per-stage timing.

Set KRYPTOSM_REGION=oregon to use Oregon data (default: dc).
"""

from kryptosm import (
    TableConfig,
    build_way_linestrings,
    build_node_geometry,
    promote_closed_ways_to_areas,
    construct_multipolygon,
    create_index_tables,
    create_nodes_table,
    create_osc_archive_table,
    create_relations_table,
    create_ways_table,
    flatten_way_refs,
    get_table_count,
    load_with_geom,
    populate_node_to_relations,
    populate_node_to_ways,
    populate_relation_to_relations,
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
    print(f"Input:        {region.parquet_path}")
    print(f"Nodes:        {region.nodes_table}")
    print(f"Ways:         {region.ways_table}")
    print(f"Relations:    {region.relations_table}")
    print(f"OSC archive:  {region.osc_archive}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(
            str(WAREHOUSE_DIR),
            driver_memory=region.driver_memory,
            parallelism=region.parallelism,
        )

    try:
        with stage("Create per-type tables + indexes + OSC archive"):
            cfg = TableConfig.testing()
            create_nodes_table(spark, region.nodes_table, config=cfg)
            create_ways_table(spark, region.ways_table, config=cfg)
            create_relations_table(spark, region.relations_table, config=cfg)
            create_index_tables(
                spark,
                region.node_to_ways,
                region.way_to_relations,
                node_to_relations=region.node_to_relations,
                relation_to_relations=region.relation_to_relations,
                config=cfg,
            )
            create_osc_archive_table(spark, region.osc_archive, config=cfg)

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
                .writeTo(region.nodes_table).using("iceberg").append()
            load_with_geom(spark, region.nodes_table, "nodes_with_geom")

        with stage("Build + write ways"):
            build_way_linestrings(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
            promote_closed_ways_to_areas(spark, "ways_linestrings", "ways_with_geom")
            prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
            spark.sql("SELECT * FROM ways_final") \
                .repartitionByRange(10, col("id")) \
                .writeTo(region.ways_table).using("iceberg").append()
            load_with_geom(spark, region.ways_table, "ways_with_geom")

        with stage("Populate node_to_ways index"):
            populate_node_to_ways(spark, region.ways_table, region.node_to_ways)

        with stage("Build + write relations"):
            spark.sql("""
                SELECT DISTINCT member.ref AS id
                FROM (SELECT explode(members) AS member FROM input_relations)
                WHERE member.type = 'way'
            """).createOrReplaceTempView("_rel_way_ids")
            ways_for_rels = spark.sql("""
                SELECT w.*
                FROM ways_with_geom w
                JOIN _rel_way_ids r ON w.id = r.id
            """).persist()
            ways_for_rels.createOrReplaceTempView("ways_for_relations")

            spark.sql("""
                SELECT DISTINCT member.ref AS id
                FROM (SELECT explode(members) AS member FROM input_relations)
                WHERE member.type = 'node'
            """).createOrReplaceTempView("_rel_node_ids")
            nodes_for_rels = spark.sql("""
                SELECT n.*
                FROM nodes_with_geom n
                JOIN _rel_node_ids r ON n.id = r.id
            """).persist()
            nodes_for_rels.createOrReplaceTempView("nodes_for_relations")

            relations_need_geometry(spark, "input_relations", "relations_need_geom")
            construct_multipolygon(spark, "relations_need_geom", "ways_for_relations", "relations_geom",
                                   nodes_geometry="nodes_for_relations")
            relation_merge_geometry_data(
                spark, "input_relations", "relations_geom", "relations_with_geom",
                ways_geometry="ways_for_relations", nodes_geometry="nodes_for_relations",
            )
            prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
            spark.sql("SELECT * FROM relations_final") \
                .repartitionByRange(20, col("id")) \
                .writeTo(region.relations_table).using("iceberg").append()

            ways_for_rels.unpersist()
            nodes_for_rels.unpersist()

        with stage("Populate way_to_relations index"):
            populate_way_to_relations(spark, region.relations_table, region.way_to_relations)

        with stage("Populate node_to_relations index"):
            populate_node_to_relations(spark, region.relations_table, region.node_to_relations)

        with stage("Populate relation_to_relations index"):
            populate_relation_to_relations(
                spark, region.relations_table, region.relation_to_relations
            )

        with stage("Final count summary"):
            counts = get_table_count(
                spark,
                region.nodes_table,
                region.ways_table,
                region.relations_table,
            )

        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            print(f"  {osm_type:9}: {counts.get(osm_type, 0):>14,}")
        print(f"  {'total':9}: {sum(counts.values()):>14,}")
        print("=" * 70)

        assert counts.get("node", 0) > 0
        assert counts.get("way", 0) > 0
        assert counts.get("relation", 0) > 0

        # additional_changesets sanity: every row in every per-type table
        # should have a non-NULL array value (possibly empty).
        with stage("Assert additional_changesets column is well-formed"):
            for kind, t in (
                ("nodes", region.nodes_table),
                ("ways", region.ways_table),
                ("relations", region.relations_table),
            ):
                n_null = spark.sql(
                    f"SELECT COUNT(*) AS n FROM {t} WHERE additional_changesets IS NULL"
                ).collect()[0]["n"]
                print(f"  rows with NULL additional_changesets in {kind}: {n_null:,}")
                assert n_null == 0, (
                    f"Every row in {t} should have a non-NULL additional_changesets array"
                )

        # All four index tables should be populated.
        with stage("Assert indexes populated"):
            for name, tbl in (
                ("node_to_ways", region.node_to_ways),
                ("way_to_relations", region.way_to_relations),
                ("node_to_relations", region.node_to_relations),
                ("relation_to_relations", region.relation_to_relations),
            ):
                n_idx = spark.sql(f"SELECT COUNT(*) AS n FROM {tbl}").collect()[0]["n"]
                print(f"  {name:25} rows: {n_idx:,}")
                # node_to_ways and way_to_relations should always be non-empty
                # for any region with ways/relations. node_to_relations and
                # relation_to_relations may legitimately be empty in small
                # regions; just check that the table exists and is queryable.
                if name in ("node_to_ways", "way_to_relations"):
                    assert n_idx > 0, f"{name} index is empty"

        # OSC archive table should exist and be empty after init.
        with stage("Assert OSC archive empty after init"):
            n_arch = spark.sql(
                f"SELECT COUNT(*) AS n FROM {region.osc_archive}"
            ).collect()[0]["n"]
            print(f"  osc_archive rows: {n_arch:,}")
            assert n_arch == 0, "OSC archive should be empty after init"
    finally:
        spark.stop()


if __name__ == "__main__":
    test_init()
