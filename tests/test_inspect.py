#!/usr/bin/env python3
"""
E2E test for the snapshot inspector. Runs across all three per-type tables.

Set KRYPTOSM_REGION=oregon to use Oregon data (default: dc).
"""

import json
from pathlib import Path

from kryptosm.inspect import inspect_snapshots, list_snapshots
from tests import (
    WAREHOUSE_DIR,
    configure_logging,
    create_spark_session_for_testing,
    get_region,
    stage,
)

_OUTPUT_DIR = Path(__file__).parent / "data" / "output"


def test_inspect():
    configure_logging()
    region = get_region()
    inspect_dir = _OUTPUT_DIR / "inspect"

    print(f"\n{'=' * 70}")
    print(f"E2E INSPECT TEST — {region.db_name}")
    print(f"{'=' * 70}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(
            str(WAREHOUSE_DIR),
            driver_memory=region.driver_memory,
            parallelism=region.parallelism,
        )

    if inspect_dir.exists():
        for f in inspect_dir.iterdir():
            f.unlink()
    else:
        inspect_dir.mkdir(parents=True)

    try:
        targets = [
            ("node",     region.nodes_table),
            ("way",      region.ways_table),
            ("relation", region.relations_table),
        ]

        any_diff_emitted = False
        for osm_type, table_name in targets:
            with stage(f"list_snapshots({osm_type})"):
                snapshots = list_snapshots(spark, table_name)

            print(f"  {osm_type:9}: {len(snapshots)} snapshots")
            for i, s in enumerate(snapshots, 1):
                print(f"    {i}. {s['operation']}  +{s['summary'].get('added-records', '?')}")

            assert len(snapshots) >= 1

            if all(s["operation"] == "append" for s in snapshots):
                with stage(f"inspect_snapshots({osm_type}) — no OSC, expect empty"):
                    paths = inspect_snapshots(
                        spark, table_name, osm_type, str(inspect_dir),
                    )
                assert len(paths) == 0, (
                    f"Expected no output after init-only on {osm_type}, got {len(paths)}"
                )
                print(f"  {osm_type:9}: no changes detected (correct — no OSC applied)")
            else:
                with stage(f"inspect_snapshots({osm_type})"):
                    paths = inspect_snapshots(
                        spark, table_name, osm_type, str(inspect_dir),
                    )
                if paths:
                    any_diff_emitted = True
                    gj_paths = [p for p in paths if p.endswith(".geojson")]
                    if gj_paths:
                        with open(gj_paths[0]) as f:
                            gj = json.load(f)
                        print(
                            f"  {osm_type:9}: "
                            f"{len(gj['features'])} features in {Path(gj_paths[0]).name}"
                        )

        print(f"\n{'=' * 70}")
        print(
            "INSPECT TEST PASSED"
            + (" (diffs emitted)" if any_diff_emitted else " (init-only, no diffs)")
        )
        print(f"{'=' * 70}")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_inspect()
