#!/usr/bin/env python3
"""
E2E Sync + Apply Test: fetch pending OSC files from the Geofabrik DC
replication server and apply them all to the Iceberg table in one pass.

Depends on the Iceberg table already existing — run ``make test-e2e-init``
(or stages 1-3) first.  Hits the network, so marked ``@pytest.mark.integration``.
"""

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kryptosm.iceberg import get_table_count, get_table_max_timestamp, table_exists
from kryptosm.main import apply_osc_files, fetch_pending_osc_files
from kryptosm.replication import DC_REPLICATION_URL
from kryptosm.spark import create_spark_session_for_testing

OUTPUT_DIR = Path(__file__).parent / "data" / "output"
WAREHOUSE_DIR = OUTPUT_DIR / "warehouse"
TABLE_NAME = "hadoop_catalog.test_db.dc"


def _osc_dir_for_table(table_name: str) -> Path:
    """Derive ``output/osc/<short_table_name>`` from a fully-qualified table name."""
    short_name = table_name.rsplit(".", 1)[-1]
    return OUTPUT_DIR / "osc" / short_name


@contextmanager
def stage(name: str):
    print(f"┌─ {name} ...")
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"└─ {name}: {elapsed:.2f}s\n")


@pytest.mark.integration
def test_fetch_and_apply_osc_files():
    osc_dir = _osc_dir_for_table(TABLE_NAME)

    print(f"\n{'=' * 70}")
    print("E2E SYNC + APPLY TEST")
    print(f"{'=' * 70}")
    print(f"Warehouse:       {WAREHOUSE_DIR}")
    print(f"Table:           {TABLE_NAME}")
    print(f"Replication URL: {DC_REPLICATION_URL}")
    print(f"Download dir:    {osc_dir}\n")

    with stage("Spark session"):
        spark = create_spark_session_for_testing(str(WAREHOUSE_DIR))

    try:
        if not table_exists(spark, TABLE_NAME):
            print(f"ERROR: {TABLE_NAME} does not exist. Run `make test-e2e-init` first.")
            assert False, "table missing"

        with stage("Snapshot counts BEFORE"):
            before = get_table_count(spark, TABLE_NAME)

        with stage("Read table MAX(timestamp)"):
            ts_before = get_table_max_timestamp(spark, TABLE_NAME)
            assert ts_before is not None, "table is empty"
            print(f"   MAX(timestamp) = {ts_before}")

        # ---- Fetch ----------------------------------------------------------
        with stage("fetch_pending_osc_files"):
            paths = fetch_pending_osc_files(
                spark,
                TABLE_NAME,
                str(osc_dir),
                base_url=DC_REPLICATION_URL,
            )

        print(f"Downloaded {len(paths)} file(s):")
        for p in paths:
            size = os.path.getsize(p)
            print(f"  {os.path.basename(p):>20}  {size:>10,} bytes")

        assert len(paths) > 0, "expected at least one pending OSC file"

        # ---- Apply ----------------------------------------------------------
        with stage(f"apply_osc_files ({len(paths)} files)"):
            apply_osc_files(spark, TABLE_NAME, paths)

        with stage("Snapshot counts AFTER"):
            after = get_table_count(spark, TABLE_NAME)

        with stage("Read table MAX(timestamp) AFTER"):
            ts_after = get_table_max_timestamp(spark, TABLE_NAME)
            print(f"   MAX(timestamp) = {ts_after}")

        # ---- Report ---------------------------------------------------------
        print("=" * 70)
        print("OSC application delta")
        print("=" * 70)
        for osm_type in ("node", "way", "relation"):
            b = before.get(osm_type, 0)
            a = after.get(osm_type, 0)
            delta = a - b
            sign = "+" if delta > 0 else ("-" if delta < 0 else " ")
            print(f"  {osm_type:9}: {b:>14,} → {a:>14,}  ({sign}{abs(delta):,})")
        print(f"\n  timestamp: {ts_before} → {ts_after}")
        print("=" * 70)

        assert ts_after >= ts_before, "timestamp should not go backwards"

        # After apply, the table timestamp advanced so at most one file
        # might still appear "pending" due to the gap between edit timestamps
        # in the OSC data and the replication server's state.txt timestamp.
        with stage("fetch_pending_osc_files (post-apply)"):
            paths_after = fetch_pending_osc_files(
                spark,
                TABLE_NAME,
                str(osc_dir),
                base_url=DC_REPLICATION_URL,
            )
        assert len(paths_after) <= 1, f"expected at most 1 pending file, got {len(paths_after)}"

        print("=" * 70)
        print("SYNC + APPLY TEST PASSED")
        print("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    test_fetch_and_apply_osc_files()
    sys.exit(0)
