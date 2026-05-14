#!/usr/bin/env python3
"""
Apply ALL pending OSC files, one at a time, until the database is current.

Set KRYPTOSM_REGION=oregon to use Oregon data (default: dc).
"""

import os

import pytest
from kryptosm import apply_osc, get_table_count, next_osc_path
from kryptosm.iceberg import get_min_applied_sequence, table_exists
from tests import (
    WAREHOUSE_DIR,
    configure_logging,
    create_spark_session_for_testing,
    get_region,
    stage,
)


@pytest.mark.integration
def test_apply_all_pending():
    configure_logging()
    region = get_region()

    print(f"\n{'=' * 70}")
    print(f"APPLY ALL PENDING OSC FILES — {region.db_name}")
    print(f"{'=' * 70}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(
            str(WAREHOUSE_DIR),
            driver_memory=region.driver_memory,
            parallelism=region.parallelism,
        )

    try:
        for tbl in (region.nodes_table, region.ways_table, region.relations_table):
            assert table_exists(spark, tbl), \
                f"{tbl} does not exist. Run `make test-e2e-init` first."

        with stage("Counts BEFORE"):
            before = get_table_count(
                spark,
                region.nodes_table,
                region.ways_table,
                region.relations_table,
            )
            seq_before = get_min_applied_sequence(
                spark, region.nodes_table, region.ways_table, region.relations_table,
            )
            print(f"  sequence: {seq_before}")

        applied = 0
        while True:
            osc_path = next_osc_path(
                spark,
                region.nodes_table,
                region.ways_table,
                region.relations_table,
                str(region.osc_dir),
                base_url=region.replication_url,
            )
            if osc_path is None:
                break

            label = os.path.basename(osc_path)
            with stage(f"Apply {label} (#{applied + 1})"):
                apply_osc(
                    spark, osc_path,
                    region.nodes_table, region.ways_table, region.relations_table,
                    region.node_to_ways, region.way_to_relations,
                    region.node_to_relations, region.relation_to_relations,
                    region.osc_archive,
                )
            applied += 1

        if applied == 0:
            print("Already current — nothing to apply.")
            return

        with stage("Counts AFTER"):
            after = get_table_count(
                spark,
                region.nodes_table,
                region.ways_table,
                region.relations_table,
            )
            seq_after = get_min_applied_sequence(
                spark, region.nodes_table, region.ways_table, region.relations_table,
            )

        print("=" * 70)
        print(f"Applied {applied} OSC file(s)")
        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            b = before.get(osm_type, 0)
            a = after.get(osm_type, 0)
            delta = a - b
            sign = "+" if delta > 0 else ("-" if delta < 0 else " ")
            print(f"  {osm_type:9}: {b:>14,} -> {a:>14,}  ({sign}{abs(delta):,})")
        print(f"  sequence:  {seq_before} -> {seq_after}")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    test_apply_all_pending()
