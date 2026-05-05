#!/usr/bin/env python3
"""
E2E Init Test: build the full OSM table from Parquet with per-stage timing.

Mimics `kryptosm.main.run_init_mode` end-to-end but instruments each stage
with a wall-clock stopwatch. There are deliberately NO `.count()` /
`.collect()` calls between stages - those force eager Spark jobs and pull
the cost of upstream lazy views into the *next* stage's timer, hiding what
actually took the time. Only one Spark action runs per stage (the writeTo)
plus a single final `get_table_count` summary at the very end.

Run after a clean warehouse to get meaningful numbers, e.g.:

    rm -rf tests/data/output/warehouse
    make test-e2e-init
"""

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.geometry.iceberg_prep import prepare_for_iceberg
from kryptosm.geometry.nodes import build_node_geometry
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
from kryptosm.iceberg import create_iceberg_table, get_table_count
from kryptosm.main import load_with_geom
from kryptosm.spark import create_spark_session_for_testing

TEST_PARQUET_PATH = Path(__file__).parent / "data" / "dc.parquet"
WAREHOUSE_DIR = Path(__file__).parent / "data" / "output" / "warehouse"
TABLE_NAME = "hadoop_catalog.test_db.e2e_osm"


@contextmanager
def stage(name: str):
    """Wall-clock timer that prints a stage marker. No Spark eval."""
    print(f"┌─ {name} ...")
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"└─ {name}: {elapsed:.2f}s\n")


def test_init():
    print(f"\n{'=' * 70}")
    print("E2E INIT TEST (timed, no per-stage counts)")
    print(f"{'=' * 70}")
    print(f"Input:     {TEST_PARQUET_PATH}")
    print(f"Warehouse: {WAREHOUSE_DIR}")
    print(f"Table:     {TABLE_NAME}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))

    try:
        with stage("Create Iceberg table"):
            create_iceberg_table(spark, TABLE_NAME)

        with stage("Register input Parquet views"):
            spark.read.parquet(
                str(TEST_PARQUET_PATH / "type=node")
            ).createOrReplaceTempView("input_nodes")
            spark.read.parquet(
                str(TEST_PARQUET_PATH / "type=way")
            ).createOrReplaceTempView("input_ways_raw")
            flatten_way_refs(spark, "input_ways_raw", "input_ways")
            spark.read.parquet(
                str(TEST_PARQUET_PATH / "type=relation")
            ).createOrReplaceTempView("input_relations")

        with stage("Build + write nodes"):
            build_node_geometry(spark, "input_nodes", "nodes_with_geom")
            prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
            spark.sql("SELECT * FROM nodes_final").writeTo(TABLE_NAME).using("iceberg").append()
            # Re-bind so ways read from Iceberg instead of recomputing nodes.
            load_with_geom(spark, TABLE_NAME, "node", "nodes_with_geom")

        with stage("Build + write ways"):
            build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
            build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
            prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
            spark.sql("SELECT * FROM ways_final").writeTo(TABLE_NAME).using("iceberg").append()
            # Re-bind so relations read from Iceberg instead of recomputing ways.
            load_with_geom(spark, TABLE_NAME, "way", "ways_with_geom")

        with stage("Build + write relations"):
            relations_need_geometry(spark, "input_relations", "relations_need_geom")
            construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom")
            relation_merge_geometry_data(
                spark, "input_relations", "relations_geom", "relations_with_geom"
            )
            prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
            spark.sql(
                "SELECT * FROM relations_final"
            ).writeTo(TABLE_NAME).using("iceberg").append()

        with stage("Final count summary"):
            counts = get_table_count(spark, TABLE_NAME)

        print("=" * 70)
        print("Final table contents")
        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            print(f"  {osm_type:9}: {counts.get(osm_type, 0):>14,}")
        print(f"  {'total':9}: {sum(counts.values()):>14,}")
        print("=" * 70)

        assert counts.get("node", 0) > 0,     "expected nodes in table"
        assert counts.get("way", 0) > 0,      "expected ways in table"
        assert counts.get("relation", 0) > 0, "expected relations in table"
    finally:
        spark.stop()


if __name__ == "__main__":
    test_init()
    sys.exit(0)
