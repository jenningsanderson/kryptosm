"""
Main orchestration for KryptOSM.
"""

import sys
from pyspark.sql import SparkSession

from .spark import create_spark_session
from .iceberg import (
    create_iceberg_table,
    table_exists,
    get_table_count,
    merge_into_table,
    delete_from_table,
)
from .geometry import (
    build_node_geometry,
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    relations_need_geometry,
    construct_multipolygon,
    relation_merge_geometry_data,
    all_dirty_ways,
    all_dirty_relations,
    apply_osc_with_geometry,
    prepare_for_iceberg,
)
from .osc import (
    osc_dedup,
    download_osc_to_dataframe,
    read_osc_from_parquet,
    read_osc_from_file,
)


def run_init_mode(spark: SparkSession, args):
    """Run initial load mode."""
    print("=" * 60)
    print("Running initial load mode...")
    print(f"Input path: {args.input_path}")
    print("=" * 60)

    # Create table if not exists
    create_iceberg_table(spark, args.table_name, args.table_location)

    # Load OSM data from Parquet
    print("\nLoading OSM data from Parquet...")

    print("Loading nodes...")
    nodes_path = f"{args.input_path}/type=node/"
    spark.read.parquet(nodes_path).createOrReplaceTempView("input_nodes")
    print("Nodes loaded")

    print("Loading ways...")
    ways_path = f"{args.input_path}/type=way/"
    spark.read.parquet(ways_path).createOrReplaceTempView("input_ways")
    print("Ways loaded")

    print("Loading relations...")
    relations_path = f"{args.input_path}/type=relation/"
    spark.read.parquet(relations_path).createOrReplaceTempView("input_relations")
    print("Relations loaded")

    # Build node geometries
    print("\n" + "=" * 60)
    print("Building node geometries...")
    print("=" * 60)
    build_node_geometry(spark, "input_nodes", "nodes_with_geom")
    prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final", args.partition_number)
    print("Node geometries built")

    # Write nodes to Iceberg
    print("Writing nodes to Iceberg table...")
    spark.sql(f"SELECT * FROM nodes_final").writeTo(args.table_name).using("iceberg").append()
    print("Nodes written to Iceberg")

    # Build way geometries
    print("\n" + "=" * 60)
    print("Building way geometries...")
    print("=" * 60)
    build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
    build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
    prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final", args.partition_number)
    print("Way geometries built")

    # Write ways to Iceberg
    print("Writing ways to Iceberg table...")
    spark.sql(f"SELECT * FROM ways_final").writeTo(args.table_name).using("iceberg").append()
    print("Ways written to Iceberg")

    # Build relation geometries
    print("\n" + "=" * 60)
    print("Building relation geometries...")
    print("=" * 60)
    relations_need_geometry(spark, "input_relations", "relations_need_geom")
    construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom")
    relation_merge_geometry_data(spark, "input_relations", "relations_geom", "relations_with_geom")
    prepare_for_iceberg(
        spark, "relations_with_geom", "relation", "relations_final", args.partition_number
    )
    print("Relation geometries built")

    # Write relations to Iceberg
    print("Writing relations to Iceberg table...")
    spark.sql(f"SELECT * FROM relations_final").writeTo(args.table_name).using("iceberg").append()
    print("Relations written to Iceberg")

    # Print summary
    print("\n" + "=" * 60)
    print("Initial load completed successfully!")
    print("=" * 60)
    counts = get_table_count(spark, args.table_name)
    for osm_type, count in counts.items():
        print(f"  {osm_type}: {count:,} features")
    print("=" * 60)


def run_update_mode(spark: SparkSession, args):
    """Run update mode with OSC files."""
    print("=" * 60)
    print("Running update mode...")
    print("=" * 60)

    # Check if table exists
    if not table_exists(spark, args.table_name):
        raise ValueError(f"Table {args.table_name} does not exist. Run init mode first.")

    # Load OSC data
    print("\nLoading OSC data...")
    if args.download_osc:
        print(f"Downloading OSC for date: {args.osc_date}")
        osc_df = download_osc_to_dataframe(spark, args.osc_date)
    else:
        print(f"Reading OSC from path: {args.osc_path}")
        if args.osc_path.endswith(".osc.gz") or args.osc_path.endswith(".osc"):
            osc_df = read_osc_from_file(spark, args.osc_path)
        else:
            osc_df = read_osc_from_parquet(spark, args.osc_path)

    osc_df.createOrReplaceTempView("osc_raw")
    osc_count = osc_df.count()
    print(f"OSC data loaded: {osc_count:,} records")

    if osc_count == 0:
        print("No OSC records to process. Exiting.")
        return

    # Deduplicate OSC data
    print("\nDeduplicating OSC data...")
    osc_dedup(spark, "osc_raw", "osc_latest")

    # Process each OSM type
    for osm_type in ["node", "way", "relation"]:
        print(f"\nProcessing {osm_type}s...")

        # Load current data from Iceberg
        print(f"  Loading current {osm_type}s from Iceberg...")
        spark.sql(f"""
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon, 
                   refs, members, latest_ts, 
                   ST_GeomFromWKB(geometry) AS geom
            FROM {args.table_name}
            WHERE type = '{osm_type}'
        """).createOrReplaceTempView(f"base_{osm_type}s")

        # Create OSC views for this type
        spark.sql(f"""
            SELECT *
            FROM osc_latest
            WHERE type = '{osm_type}' AND op IN ('create', 'modify')
        """).createOrReplaceTempView(f"osc_{osm_type}_upserts")

        spark.sql(f"""
            SELECT id
            FROM osc_latest
            WHERE type = '{osm_type}' AND op = 'delete'
        """).createOrReplaceTempView(f"osc_{osm_type}_deletes")

        upsert_count = spark.sql(f"SELECT COUNT(*) as c FROM osc_{osm_type}_upserts").collect()[0][
            "c"
        ]
        delete_count = spark.sql(f"SELECT COUNT(*) as c FROM osc_{osm_type}_deletes").collect()[0][
            "c"
        ]
        print(f"  OSC {osm_type}s: {upsert_count} upserts, {delete_count} deletes")

    # Process nodes
    print("\n" + "=" * 60)
    print("Processing Nodes")
    print("=" * 60)
    process_nodes_update(spark, args)

    # Process ways
    print("\n" + "=" * 60)
    print("Processing Ways")
    print("=" * 60)
    process_ways_update(spark, args)

    # Process relations
    print("\n" + "=" * 60)
    print("Processing Relations")
    print("=" * 60)
    process_relations_update(spark, args)

    # Print summary
    print("\n" + "=" * 60)
    print("Update completed successfully!")
    print("=" * 60)
    counts = get_table_count(spark, args.table_name)
    for osm_type, count in counts.items():
        print(f"  {osm_type}: {count:,} features")
    print("=" * 60)


def process_nodes_update(spark: SparkSession, args):
    """Process node updates."""
    # Build geometries for updated nodes
    build_node_geometry(spark, "osc_node_upserts", "updated_nodes_geom")

    # Apply OSC to nodes
    apply_osc_with_geometry(
        spark,
        base_data="base_nodes",
        updated_data="updated_nodes_geom",
        deleted_data="osc_node_deletes",
        result_view="nodes_final_geom",
    )

    # Prepare for Iceberg
    prepare_for_iceberg(spark, "nodes_final_geom", "node", "nodes_iceberg", args.partition_number)

    # Merge into Iceberg table
    print("  Merging nodes into Iceberg table...")
    merge_into_table(spark, args.table_name, "nodes_iceberg", "t.id = s.id AND t.type = 'node'")

    # Handle deletes
    delete_count = spark.sql("SELECT COUNT(*) as c FROM osc_node_deletes").collect()[0]["c"]
    if delete_count > 0:
        print(f"  Deleting {delete_count} nodes...")
        delete_from_table(
            spark,
            args.table_name,
            "osc_node_deletes",
            "t.id = s.id AND t.type = 'node'",
        )

    print("  Nodes update complete")


def process_ways_update(spark: SparkSession, args):
    """Process way updates."""
    # Identify dirty ways
    all_dirty_ways(
        spark,
        base_ways="base_ways",
        new_or_property_updated_ways="osc_way_upserts",
        dirty_nodes="osc_node_upserts",
        result_view="dirty_ways",
    )

    dirty_count = spark.sql("SELECT COUNT(*) as c FROM dirty_ways").collect()[0]["c"]
    print(f"  Dirty ways to rebuild: {dirty_count}")

    if dirty_count > 0:
        # Build geometries for dirty ways
        build_linestring_for_ways(spark, "dirty_ways", "nodes_final_geom", "dirty_ways_lines")
        build_ways_geometry_from_linestring(spark, "dirty_ways_lines", "dirty_ways_geom")
    else:
        # Create empty view with correct schema
        spark.sql("""
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon, 
                   refs, members, latest_ts, CAST(NULL AS STRING) AS geom
            FROM dirty_ways
            WHERE 1=0
        """).createOrReplaceTempView("dirty_ways_geom")

    # Apply OSC to ways
    apply_osc_with_geometry(
        spark,
        base_data="base_ways",
        updated_data="dirty_ways_geom",
        deleted_data="osc_way_deletes",
        result_view="ways_final_geom",
    )

    # Prepare for Iceberg
    prepare_for_iceberg(spark, "ways_final_geom", "way", "ways_iceberg", args.partition_number)

    # Merge into Iceberg table
    print("  Merging ways into Iceberg table...")
    merge_into_table(spark, args.table_name, "ways_iceberg", "t.id = s.id AND t.type = 'way'")

    # Handle deletes
    delete_count = spark.sql("SELECT COUNT(*) as c FROM osc_way_deletes").collect()[0]["c"]
    if delete_count > 0:
        print(f"  Deleting {delete_count} ways...")
        delete_from_table(
            spark, args.table_name, "osc_way_deletes", "t.id = s.id AND t.type = 'way'"
        )

    print("  Ways update complete")


def process_relations_update(spark: SparkSession, args):
    """Process relation updates."""
    # Identify dirty relations
    all_dirty_relations(
        spark,
        base_relations="base_relations",
        new_or_property_updated_relations="osc_relation_upserts",
        dirty_ways="dirty_ways",
        result_view="dirty_relations",
    )

    dirty_count = spark.sql("SELECT COUNT(*) as c FROM dirty_relations").collect()[0]["c"]
    print(f"  Dirty relations to rebuild: {dirty_count}")

    if dirty_count > 0:
        # Build geometries for dirty relations
        relations_need_geometry(spark, "dirty_relations", "rels_need_geom")
        construct_multipolygon(spark, "rels_need_geom", "ways_final_geom", "rels_geom")
        relation_merge_geometry_data(spark, "dirty_relations", "rels_geom", "dirty_rels_geom")
    else:
        # Create empty view with correct schema
        spark.sql("""
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon, 
                   refs, members, latest_ts, CAST(NULL AS STRING) AS geom
            FROM dirty_relations
            WHERE 1=0
        """).createOrReplaceTempView("dirty_rels_geom")

    # Apply OSC to relations
    apply_osc_with_geometry(
        spark,
        base_data="base_relations",
        updated_data="dirty_rels_geom",
        deleted_data="osc_relation_deletes",
        result_view="relations_final_geom",
    )

    # Prepare for Iceberg
    prepare_for_iceberg(
        spark,
        "relations_final_geom",
        "relation",
        "relations_iceberg",
        args.partition_number,
    )

    # Merge into Iceberg table
    print("  Merging relations into Iceberg table...")
    merge_into_table(
        spark,
        args.table_name,
        "relations_iceberg",
        "t.id = s.id AND t.type = 'relation'",
    )

    # Handle deletes
    delete_count = spark.sql("SELECT COUNT(*) as c FROM osc_relation_deletes").collect()[0]["c"]
    if delete_count > 0:
        print(f"  Deleting {delete_count} relations...")
        delete_from_table(
            spark,
            args.table_name,
            "osc_relation_deletes",
            "t.id = s.id AND t.type = 'relation'",
        )

    print("  Relations update complete")


def main(args):
    """Main entry point."""
    print(f"Starting OSM Iceberg Sync in {args.mode} mode")
    print(f"Table: {args.table_name}")

    # Create Spark session
    print("\nCreating Spark session...")
    spark = create_spark_session(
        app_name="OSM Iceberg Sync",
        master=args.spark_master,
        catalog_type=args.catalog_type,
        catalog_name=args.catalog_name,
        warehouse=args.catalog_warehouse,
        table_location=args.table_location,
    )
    print("Spark session created successfully")

    try:
        if args.mode == "init":
            run_init_mode(spark, args)
        elif args.mode == "update":
            run_update_mode(spark, args)

        print("\n" + "=" * 60)
        print("OSM Iceberg Sync completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"\nError during execution: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    from .cli import parse_args

    args = parse_args()
    main(args)
