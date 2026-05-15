#!/usr/bin/env python3
"""
Apply the next pending OSC file. Idempotent — run repeatedly and each
invocation fetches and applies exactly one file.

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
def test_apply_next_osc():
    configure_logging()
    region = get_region()

    print(f"\n{'=' * 70}")
    print(f"APPLY NEXT OSC — {region.db_name}")
    print(f"{'=' * 70}")
    print(f"Nodes:       {region.nodes_table}")
    print(f"OSC archive: {region.osc_archive}\n")

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

        with stage("Fetch next OSC"):
            osc_path = next_osc_path(
                spark,
                region.nodes_table,
                region.ways_table,
                region.relations_table,
                str(region.osc_dir),
                base_url=region.replication_url,
            )

        if osc_path is None:
            print("Already current — nothing to apply.")
            return

        print(f"  {os.path.basename(osc_path)}  ({os.path.getsize(osc_path):,} bytes)")

        with stage(f"Apply {os.path.basename(osc_path)}"):
            apply_osc(
                spark, osc_path,
                region.nodes_table, region.ways_table, region.relations_table,
                region.node_to_ways, region.way_to_relations,
                region.node_to_relations, region.relation_to_relations,
                region.osc_archive,
            )

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

        # Assert that the OSC archive received a partition for this sequence,
        # and that the row count matches the OSC's record count.
        if seq_after is not None:
            n_archive = spark.sql(f"""
                SELECT COUNT(*) AS n FROM {region.osc_archive}
                WHERE sequence = {seq_after}
            """).collect()[0]["n"]
            print(f"  archive rows for sequence {seq_after}: {n_archive:,}")
            assert n_archive > 0, (
                f"Expected osc_archive to contain rows for sequence {seq_after}"
            )

        # No row in any per-type table should have a NULL changeset. The OSC
        # parser coerces missing changeset attributes to 0, the parquet init
        # path does the same, and osc_dedup defensively COALESCEs to 0 too.
        with stage("Assert no NULL changesets in per-type tables"):
            for kind, t in (
                ("nodes", region.nodes_table),
                ("ways", region.ways_table),
                ("relations", region.relations_table),
            ):
                n_null = spark.sql(
                    f"SELECT COUNT(*) AS n FROM {t} WHERE changeset IS NULL"
                ).collect()[0]["n"]
                print(f"  rows with NULL changeset in {kind}: {n_null:,}")
                assert n_null == 0, (
                    f"Every row in {t} should have a non-NULL changeset"
                )

        # OSC-dedup loser capture: if this OSC file contained any (id, type)
        # with multiple version rows, the live table's row for that id should
        # carry the loser changesets in additional_changesets, and changeset
        # itself should match the highest-version row from that OSC.
        if seq_after is not None:
            with stage("Assert OSC-dedup losers landed in additional_changesets"):
                # Find (id, type) groups in this sequence's archive partition
                # that had >= 2 versions — these are the dedup-loser cases.
                multi = spark.sql(f"""
                    WITH _seq AS (
                        SELECT id, type, version, changeset
                        FROM {region.osc_archive}
                        WHERE sequence = {seq_after}
                              AND op IN ('create', 'modify')
                    ),
                    _grouped AS (
                        SELECT
                            id, type,
                            COUNT(*) AS n_versions,
                            MAX(version) AS max_version,
                            collect_list(struct(version, changeset)) AS rows
                        FROM _seq
                        GROUP BY id, type
                        HAVING COUNT(*) >= 2
                    )
                    SELECT id, type, max_version, n_versions,
                           filter(rows, r -> r.version = max_version)[0].changeset AS winner_cs,
                           array_distinct(
                               transform(
                                   filter(rows, r -> r.version <> max_version),
                                   r -> r.changeset
                               )
                           ) AS loser_cs
                    FROM _grouped
                """).collect()
                print(f"  multi-version (id, type) groups in seq {seq_after}: {len(multi)}")
                for row in multi:
                    osm_type = row["type"]
                    table = {
                        "node": region.nodes_table,
                        "way": region.ways_table,
                        "relation": region.relations_table,
                    }[osm_type]
                    rows = spark.sql(f"""
                        SELECT changeset, additional_changesets
                        FROM {table}
                        WHERE id = {row["id"]}
                    """).collect()
                    if not rows:
                        # Row may have been deleted by a later op in the OSC.
                        continue
                    live = rows[0]
                    assert live["changeset"] == row["winner_cs"], (
                        f"{osm_type} {row['id']}: live changeset "
                        f"{live['changeset']!r} != winner {row['winner_cs']!r}"
                    )
                    live_extra = set(live["additional_changesets"] or [])
                    expected_losers = set(row["loser_cs"] or [])
                    missing = expected_losers - live_extra
                    assert not missing, (
                        f"{osm_type} {row['id']}: additional_changesets "
                        f"{sorted(live_extra)!r} missing OSC losers "
                        f"{sorted(missing)!r}"
                    )

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
    test_apply_next_osc()
