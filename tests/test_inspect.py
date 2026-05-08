#!/usr/bin/env python3
"""
E2E test for the snapshot inspector.

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
    print(f"{'=' * 70}")
    print(f"Table: {region.table_name}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))

    if inspect_dir.exists():
        for f in inspect_dir.iterdir():
            f.unlink()
    else:
        inspect_dir.mkdir(parents=True)

    try:
        with stage("list_snapshots"):
            snapshots = list_snapshots(spark, region.table_name)

        print(f"  {len(snapshots)} snapshots:")
        for i, s in enumerate(snapshots, 1):
            print(f"    {i}. {s['operation']}  +{s['summary'].get('added-records', '?')}")

        assert len(snapshots) >= 2

        if all(s["operation"] == "append" for s in snapshots):
            with stage("inspect_snapshots (no OSC — expect empty)"):
                paths = inspect_snapshots(spark, region.table_name, str(inspect_dir))
            assert len(paths) == 0, f"Expected no output after init-only, got {len(paths)}"
            print("  No changes detected (correct — no OSC applied)")
        else:
            with stage("inspect_snapshots"):
                paths = inspect_snapshots(spark, region.table_name, str(inspect_dir))
            if paths:
                gj_path = [p for p in paths if p.endswith(".geojson")][0]
                with open(gj_path) as f:
                    gj = json.load(f)
                print(f"  {len(gj['features'])} features in {Path(gj_path).name}")

        print(f"\n{'=' * 70}")
        print("INSPECT TEST PASSED")
        print(f"{'=' * 70}")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_inspect()
