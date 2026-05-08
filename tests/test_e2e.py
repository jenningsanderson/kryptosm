#!/usr/bin/env python3
"""
E2E Full Pipeline Test: init + OSC apply, all in one Spark session.

Combines what `test_e2e_init.py` and `test_e2e_osc.py` do, but reuses a
single Spark session across both phases - so we pay the JVM/Sedona/Iceberg
startup cost (the slowest single thing in local testing) exactly once.

Same per-stage wall-clock timing and no-eager-counts policy as the other
e2e tests. The only Spark actions are the writes/MERGEs/DELETEs each stage
performs and the count snapshots that bracket each phase.
"""

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.geometry.iceberg_prep import prepare_for_iceberg
from kryptosm.geometry.nodes import build_node_geometry
from kryptosm.geometry.osc_apply import (
    all_dirty_relations,
    all_dirty_ways,
    apply_osc_with_geometry,
)
from kryptosm.geometry.relations import (
    construct_multipolygon,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from kryptosm.geometry.ways import (
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    flatten_way_refs,
)
from kryptosm.iceberg import (
    create_iceberg_table,
    delete_from_table,
    get_table_count,
    merge_into_table,
)
from kryptosm.main import load_with_geom
from kryptosm.osc import osc_dedup, read_osc_from_file
from kryptosm.spark import create_spark_session_for_testing

TEST_PARQUET_PATH = Path(__file__).parent / "data" / "WashingtonDC" / "dc.parquet"
OSC_FILE = Path(__file__).parent / "data" / "WashingtonDC" / "osc" / "changeset_1.xml"
WAREHOUSE_DIR = Path(__file__).parent / "data" / "output" / "warehouse"
TABLE_NAME = "hadoop_catalog.test_db.dc"


@contextmanager
def stage(name: str):
    """Wall-clock timer that prints a stage marker. No Spark eval."""
    print(f"┌─ {name} ...")
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"└─ {name}: {elapsed:.2f}s\n")


@contextmanager
def phase(name: str):
    """Bigger banner around a group of stages, also wall-clock timed."""
    bar = "━" * 70
    print(f"\n{bar}\n{name}\n{bar}\n")
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"{bar}\n{name} TOTAL: {elapsed:.2f}s\n{bar}\n")


def _setup_osc_views(spark, table_name: str):
    """Bind base_<type>s, osc_<type>_upserts, osc_<type>_deletes for all 3 types."""
    for osm_type in ("node", "way", "relation"):
        load_with_geom(spark, table_name, osm_type, f"base_{osm_type}s")
        spark.sql(f"""
            SELECT * FROM osc_latest
            WHERE type = '{osm_type}' AND op IN ('create', 'modify')
        """).createOrReplaceTempView(f"osc_{osm_type}_upserts")
        spark.sql(f"""
            SELECT id FROM osc_latest WHERE type = '{osm_type}' AND op = 'delete'
        """).createOrReplaceTempView(f"osc_{osm_type}_deletes")


def test_e2e_full():
    print(f"\n{'=' * 70}")
    print("E2E FULL TEST (init + OSC, one Spark session)")
    print(f"{'=' * 70}")
    print(f"Parquet:   {TEST_PARQUET_PATH}")
    print(f"OSC:       {OSC_FILE}")
    print(f"Warehouse: {WAREHOUSE_DIR}")
    print(f"Table:     {TABLE_NAME}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))

    try:
        # =====================================================================
        # PHASE 1: INIT - build the table from Parquet
        # =====================================================================
        with phase("PHASE 1: INIT"):
            with stage("Create Iceberg table"):
                create_iceberg_table(spark, TABLE_NAME)

            with stage("Register input Parquet views"):
                spark.read.parquet(str(TEST_PARQUET_PATH / "type=node")).createOrReplaceTempView(
                    "input_nodes"
                )
                spark.read.parquet(str(TEST_PARQUET_PATH / "type=way")).createOrReplaceTempView(
                    "input_ways_raw"
                )
                flatten_way_refs(spark, "input_ways_raw", "input_ways")
                spark.read.parquet(
                    str(TEST_PARQUET_PATH / "type=relation")
                ).createOrReplaceTempView("input_relations")

            with stage("Build + write nodes"):
                build_node_geometry(spark, "input_nodes", "nodes_with_geom")
                prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
                spark.sql("SELECT * FROM nodes_final").writeTo(TABLE_NAME).using("iceberg").append()
                load_with_geom(spark, TABLE_NAME, "node", "nodes_with_geom")

            with stage("Build + write ways"):
                build_linestring_for_ways(
                    spark, "input_ways", "nodes_with_geom", "ways_linestrings"
                )
                build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
                prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
                spark.sql("SELECT * FROM ways_final").writeTo(TABLE_NAME).using("iceberg").append()
                load_with_geom(spark, TABLE_NAME, "way", "ways_with_geom")

            with stage("Build + write relations"):
                relations_need_geometry(spark, "input_relations", "relations_need_geom")
                construct_multipolygon(
                    spark, "relations_need_geom", "ways_with_geom", "relations_geom"
                )
                relation_merge_geometry_data(
                    spark, "input_relations", "relations_geom", "relations_with_geom"
                )
                prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
                spark.sql("SELECT * FROM relations_final").writeTo(TABLE_NAME).using(
                    "iceberg"
                ).append()

            with stage("Snapshot post-init counts"):
                after_init = get_table_count(spark, TABLE_NAME)

            print("Post-init contents:")
            for osm_type in ("node", "way", "relation"):
                print(f"  {osm_type:9}: {after_init.get(osm_type, 0):>14,}")

        # =====================================================================
        # PHASE 2: OSC apply
        # =====================================================================
        with phase("PHASE 2: OSC APPLY"):
            with stage("Read OSC + dedup"):
                read_osc_from_file(spark, str(OSC_FILE)).createOrReplaceTempView("osc_raw")
                osc_dedup(spark, "osc_raw", "osc_latest")

            with stage("Setup per-type base/upsert/delete views"):
                _setup_osc_views(spark, TABLE_NAME)

            with stage("Apply node updates"):
                build_node_geometry(spark, "osc_node_upserts", "updated_nodes_geom")
                apply_osc_with_geometry(
                    spark,
                    "base_nodes",
                    "updated_nodes_geom",
                    "osc_node_deletes",
                    "nodes_final_geom",
                )
                prepare_for_iceberg(spark, "nodes_final_geom", "node", "nodes_iceberg")
                merge_into_table(
                    spark,
                    TABLE_NAME,
                    "nodes_iceberg",
                    "t.id = s.id AND t.type = 'node'",
                )
                delete_from_table(
                    spark,
                    TABLE_NAME,
                    "osc_node_deletes",
                    "t.id = s.id AND t.type = 'node'",
                )

            with stage("Apply way updates"):
                all_dirty_ways(
                    spark,
                    "base_ways",
                    "osc_way_upserts",
                    "osc_node_upserts",
                    "dirty_ways",
                )
                build_linestring_for_ways(
                    spark, "dirty_ways", "nodes_final_geom", "dirty_ways_lines"
                )
                build_ways_geometry_from_linestring(spark, "dirty_ways_lines", "dirty_ways_geom")
                apply_osc_with_geometry(
                    spark,
                    "base_ways",
                    "dirty_ways_geom",
                    "osc_way_deletes",
                    "ways_final_geom",
                )
                prepare_for_iceberg(spark, "ways_final_geom", "way", "ways_iceberg")
                merge_into_table(
                    spark,
                    TABLE_NAME,
                    "ways_iceberg",
                    "t.id = s.id AND t.type = 'way'",
                )
                delete_from_table(
                    spark,
                    TABLE_NAME,
                    "osc_way_deletes",
                    "t.id = s.id AND t.type = 'way'",
                )

            with stage("Apply relation updates"):
                all_dirty_relations(
                    spark,
                    "base_relations",
                    "osc_relation_upserts",
                    "dirty_ways",
                    "dirty_relations",
                )
                relations_need_geometry(spark, "dirty_relations", "rels_need_geom")
                construct_multipolygon(spark, "rels_need_geom", "ways_final_geom", "rels_geom")
                relation_merge_geometry_data(
                    spark, "dirty_relations", "rels_geom", "dirty_rels_geom"
                )
                apply_osc_with_geometry(
                    spark,
                    "base_relations",
                    "dirty_rels_geom",
                    "osc_relation_deletes",
                    "relations_final_geom",
                )
                prepare_for_iceberg(spark, "relations_final_geom", "relation", "relations_iceberg")
                merge_into_table(
                    spark,
                    TABLE_NAME,
                    "relations_iceberg",
                    "t.id = s.id AND t.type = 'relation'",
                )
                delete_from_table(
                    spark,
                    TABLE_NAME,
                    "osc_relation_deletes",
                    "t.id = s.id AND t.type = 'relation'",
                )

            with stage("Snapshot post-OSC counts"):
                after_osc = get_table_count(spark, TABLE_NAME)

        # =====================================================================
        # Final delta
        # =====================================================================
        print("=" * 70)
        print("Init → post-OSC delta")
        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            b = after_init.get(osm_type, 0)
            a = after_osc.get(osm_type, 0)
            delta = a - b
            sign = "+" if delta > 0 else ("-" if delta < 0 else " ")
            print(f"  {osm_type:9}: {b:>14,} → {a:>14,}  ({sign}{abs(delta):,})")
        print("=" * 70)

        # Sanity assertions.
        for osm_type in ("node", "way", "relation"):
            assert after_init.get(osm_type, 0) > 0, f"expected {osm_type}s after init"
            assert after_osc.get(osm_type, 0) >= after_init.get(osm_type, 0), (
                f"{osm_type} count decreased: {after_init[osm_type]} -> {after_osc[osm_type]}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    test_e2e_full()
    sys.exit(0)
