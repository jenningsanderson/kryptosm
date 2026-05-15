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

        # Carry-forward assertion: pick an id that appeared in multiple
        # archived sequences (i.e. was re-MERGE'd by a later OSC). For each,
        # gather every loser changeset across ALL its archive sequences and
        # assert they are still present in the live row's additional_changesets.
        # If an id was edited only once across the whole run, the assertion
        # is vacuously skipped.
        with stage("Assert additional_changesets carry-forward across applies"):
            multi_seq = spark.sql(f"""
                WITH _all AS (
                    SELECT id, type, sequence, version, changeset
                    FROM {region.osc_archive}
                    WHERE op IN ('create', 'modify')
                ),
                _seq_groups AS (
                    SELECT id, type, sequence,
                           COUNT(*) AS n_versions,
                           MAX(version) AS max_version,
                           collect_list(struct(version, changeset)) AS rows
                    FROM _all
                    GROUP BY id, type, sequence
                    HAVING COUNT(*) >= 2
                ),
                _ids_with_dedup AS (
                    SELECT DISTINCT id, type FROM _seq_groups
                )
                SELECT g.id, g.type,
                       array_distinct(
                           flatten(
                               collect_list(
                                   transform(
                                       filter(g.rows, r -> r.version <> g.max_version),
                                       r -> r.changeset
                                   )
                               )
                           )
                       ) AS all_loser_cs,
                       COUNT(DISTINCT g.sequence) AS n_sequences
                FROM _seq_groups g
                JOIN _ids_with_dedup d ON d.id = g.id AND d.type = g.type
                GROUP BY g.id, g.type
                HAVING COUNT(DISTINCT g.sequence) >= 1
            """).collect()
            checked = 0
            for row in multi_seq:
                osm_type = row["type"]
                table = {
                    "node": region.nodes_table,
                    "way": region.ways_table,
                    "relation": region.relations_table,
                }[osm_type]
                live_rows = spark.sql(f"""
                    SELECT additional_changesets
                    FROM {table}
                    WHERE id = {row["id"]}
                """).collect()
                if not live_rows:
                    # Subsequently deleted; skip.
                    continue
                live_extra = set(live_rows[0]["additional_changesets"] or [])
                expected = set(row["all_loser_cs"] or [])
                missing = expected - live_extra
                assert not missing, (
                    f"{osm_type} {row['id']}: additional_changesets "
                    f"{sorted(live_extra)!r} missing carry-forward losers "
                    f"{sorted(missing)!r}"
                )
                checked += 1
            print(f"  carry-forward checked on {checked} multi-loser ids")

        # No row in any per-type table should have a NULL changeset after a
        # full sequential apply, regardless of the input source.
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
