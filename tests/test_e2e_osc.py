#!/usr/bin/env python3
"""
E2E OSC Test: apply an OSC change file with per-stage timing.

Mirrors `kryptosm.main.run_update_mode` step-by-step but instruments each
stage with a wall-clock stopwatch. Same no-eager-counts policy as
test_e2e_init.py - the only Spark actions are the writes/MERGEs each stage
performs and the two `get_table_count` snapshots that bracket the run.

Depends on the table already existing - run `make test-e2e-init` (or
stages 1-3) to build it first.
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
)
from kryptosm.iceberg import (
    delete_from_table,
    get_table_count,
    merge_into_table,
    table_exists,
)
from kryptosm.main import load_with_geom
from kryptosm.osc import osc_dedup, read_osc_from_file
from kryptosm.spark import create_spark_session_for_testing

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


def _setup_per_type_views(spark, table_name: str):
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


def test_osc_update():
    print(f"\n{'=' * 70}")
    print("E2E OSC TEST (timed, no per-stage counts)")
    print(f"{'=' * 70}")
    print(f"OSC:       {OSC_FILE}")
    print(f"Warehouse: {WAREHOUSE_DIR}")
    print(f"Table:     {TABLE_NAME}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))

    try:
        if not table_exists(spark, TABLE_NAME):
            print(f"ERROR: {TABLE_NAME} does not exist. Run `make test-e2e-init` first.")
            assert False, "table missing"

        with stage("Snapshot counts BEFORE"):
            before = get_table_count(spark, TABLE_NAME)

        with stage("Read OSC + dedup"):
            read_osc_from_file(spark, str(OSC_FILE)).createOrReplaceTempView("osc_raw")
            osc_dedup(spark, "osc_raw", "osc_latest")

        with stage("Setup per-type base/upsert/delete views"):
            _setup_per_type_views(spark, TABLE_NAME)

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
            merge_into_table(spark, TABLE_NAME, "nodes_iceberg", "t.id = s.id AND t.type = 'node'")
            delete_from_table(
                spark, TABLE_NAME, "osc_node_deletes", "t.id = s.id AND t.type = 'node'"
            )

        with stage("Apply way updates"):
            all_dirty_ways(spark, "base_ways", "osc_way_upserts", "osc_node_upserts", "dirty_ways")
            build_linestring_for_ways(spark, "dirty_ways", "nodes_final_geom", "dirty_ways_lines")
            build_ways_geometry_from_linestring(spark, "dirty_ways_lines", "dirty_ways_geom")
            apply_osc_with_geometry(
                spark,
                "base_ways",
                "dirty_ways_geom",
                "osc_way_deletes",
                "ways_final_geom",
            )
            prepare_for_iceberg(spark, "ways_final_geom", "way", "ways_iceberg")
            merge_into_table(spark, TABLE_NAME, "ways_iceberg", "t.id = s.id AND t.type = 'way'")
            delete_from_table(
                spark, TABLE_NAME, "osc_way_deletes", "t.id = s.id AND t.type = 'way'"
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
            relation_merge_geometry_data(spark, "dirty_relations", "rels_geom", "dirty_rels_geom")
            apply_osc_with_geometry(
                spark,
                "base_relations",
                "dirty_rels_geom",
                "osc_relation_deletes",
                "relations_final_geom",
            )
            prepare_for_iceberg(spark, "relations_final_geom", "relation", "relations_iceberg")
            merge_into_table(
                spark, TABLE_NAME, "relations_iceberg", "t.id = s.id AND t.type = 'relation'"
            )
            delete_from_table(
                spark,
                TABLE_NAME,
                "osc_relation_deletes",
                "t.id = s.id AND t.type = 'relation'",
            )

        with stage("Snapshot counts AFTER"):
            after = get_table_count(spark, TABLE_NAME)

        print("=" * 70)
        print("OSC application delta")
        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            b = before.get(osm_type, 0)
            a = after.get(osm_type, 0)
            delta = a - b
            sign = "+" if delta > 0 else ("-" if delta < 0 else " ")
            print(f"  {osm_type:9}: {b:>14,} → {a:>14,}  ({sign}{abs(delta):,})")
        print("=" * 70)

        # Sanity: nothing should silently disappear (the test OSC is creates-only).
        for osm_type, count_before in before.items():
            assert after.get(osm_type, 0) >= count_before, (
                f"{osm_type} count went from {count_before} to {after.get(osm_type, 0)}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    test_osc_update()
    sys.exit(0)
