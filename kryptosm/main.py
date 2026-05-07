"""
Orchestration: chains the SQL views in geometry/ and writes the result to Iceberg.

The pipeline is intentionally lazy - each step registers a view; the optimizer
plans the whole DAG and Spark materializes only at write/MERGE time.
"""

import sys

from pyspark.sql import SparkSession

from .geometry.iceberg_prep import prepare_for_iceberg
from .geometry.nodes import build_node_geometry
from .geometry.osc_apply import (
    all_dirty_relations,
    all_dirty_ways,
    apply_osc_with_geometry,
)
from .geometry.relations import (
    construct_multipolygon,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from .geometry.ways import (
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    flatten_way_refs,
)
from .iceberg import (
    create_iceberg_table,
    delete_from_table,
    get_table_count,
    merge_into_table,
    table_exists,
)
from .osc import (
    download_osc_to_dataframe,
    osc_dedup,
    read_osc_from_file,
    read_osc_from_parquet,
)
from .spark import create_spark_session


def load_with_geom(spark: SparkSession, table_name: str, osm_type: str, view_name: str):
    """
    Bind `view_name` to a fresh read of `table_name` filtered to one OSM type,
    decoding the WKB geometry back into a Sedona `geom` column.

    This is how we hand the just-written rows from one stage to the next:
    Iceberg becomes the materialization point, eliminating recompute of
    upstream view DAGs (e.g. node geometries don't get rebuilt 3x).
    """
    spark.sql(f"""
        SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon,
               refs, members, latest_ts,
               ST_GeomFromWKB(geometry) AS geom
        FROM {table_name}
        WHERE type = '{osm_type}'
    """).createOrReplaceTempView(view_name)


# ============================================================================
# Init mode: build the table from raw OSM Parquet
# ============================================================================


def run_init_mode(spark: SparkSession, args):
    """
    Build node, way and relation geometries from Parquet and write to Iceberg.

    After each stage's write, we re-bind the downstream input view (e.g.
    `nodes_with_geom`) to the freshly-written Iceberg rows. That stops Spark
    from recomputing upstream geometry pipelines on each subsequent write.
    """
    create_iceberg_table(spark, args.table_name, args.table_location)

    spark.read.parquet(f"{args.input_path}/type=node/").createOrReplaceTempView("input_nodes")
    spark.read.parquet(f"{args.input_path}/type=way/").createOrReplaceTempView("input_ways_raw")
    flatten_way_refs(spark, "input_ways_raw", "input_ways")
    spark.read.parquet(f"{args.input_path}/type=relation/").createOrReplaceTempView(
        "input_relations"
    )

    # ---- Nodes ----------------------------------------------------------------
    build_node_geometry(spark, "input_nodes", "nodes_with_geom")
    prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
    spark.sql("SELECT * FROM nodes_final").writeTo(args.table_name).using("iceberg").append()
    # Re-bind: ways will now read nodes from Iceberg, not recompute them.
    load_with_geom(spark, args.table_name, "node", "nodes_with_geom")

    # ---- Ways -----------------------------------------------------------------
    build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
    build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
    prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
    spark.sql("SELECT * FROM ways_final").writeTo(args.table_name).using("iceberg").append()
    # Re-bind: relations will now read ways from Iceberg, not recompute them.
    load_with_geom(spark, args.table_name, "way", "ways_with_geom")

    # ---- Relations ------------------------------------------------------------
    relations_need_geometry(spark, "input_relations", "relations_need_geom")
    construct_multipolygon(spark, "relations_need_geom", "ways_with_geom", "relations_geom")
    relation_merge_geometry_data(spark, "input_relations", "relations_geom", "relations_with_geom")
    prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
    spark.sql("SELECT * FROM relations_final").writeTo(args.table_name).using("iceberg").append()

    _print_counts(spark, args.table_name)


# ============================================================================
# Update mode: apply an OSC change file to the existing table
# ============================================================================


def run_update_mode(spark: SparkSession, args):
    """Apply an OSC change file. Each step is SQL; Spark plans + executes the DAG."""
    if not table_exists(spark, args.table_name):
        raise ValueError(f"Table {args.table_name} does not exist. Run init mode first.")

    # Load OSC into a Spark view.
    if args.download_osc:
        osc_df = download_osc_to_dataframe(spark, args.osc_date)
    elif args.osc_path.endswith((".osc.gz", ".osc", ".xml")):
        osc_df = read_osc_from_file(spark, args.osc_path)
    else:
        osc_df = read_osc_from_parquet(spark, args.osc_path)
    osc_df.createOrReplaceTempView("osc_raw")
    osc_dedup(spark, "osc_raw", "osc_latest")

    # Per-type base / upsert / delete views.
    for osm_type in ("node", "way", "relation"):
        load_with_geom(spark, args.table_name, osm_type, f"base_{osm_type}s")

        spark.sql(f"""
            SELECT * FROM osc_latest
            WHERE type = '{osm_type}' AND op IN ('create', 'modify')
        """).createOrReplaceTempView(f"osc_{osm_type}_upserts")

        spark.sql(f"""
            SELECT id FROM osc_latest WHERE type = '{osm_type}' AND op = 'delete'
        """).createOrReplaceTempView(f"osc_{osm_type}_deletes")

    # Nodes: rebuild geometry for upserts, then apply (overlay + drop deletes).
    build_node_geometry(spark, "osc_node_upserts", "updated_nodes_geom")
    apply_osc_with_geometry(
        spark, "base_nodes", "updated_nodes_geom", "osc_node_deletes", "nodes_final_geom"
    )
    prepare_for_iceberg(spark, "nodes_final_geom", "node", "nodes_iceberg")
    merge_into_table(spark, args.table_name, "nodes_iceberg", "t.id = s.id AND t.type = 'node'")
    delete_from_table(spark, args.table_name, "osc_node_deletes", "t.id = s.id AND t.type = 'node'")

    # Ways: dirty = direct OSC changes + ways whose nodes moved.
    all_dirty_ways(spark, "base_ways", "osc_way_upserts", "osc_node_upserts", "dirty_ways")
    build_linestring_for_ways(spark, "dirty_ways", "nodes_final_geom", "dirty_ways_lines")
    build_ways_geometry_from_linestring(spark, "dirty_ways_lines", "dirty_ways_geom")
    apply_osc_with_geometry(
        spark, "base_ways", "dirty_ways_geom", "osc_way_deletes", "ways_final_geom"
    )
    prepare_for_iceberg(spark, "ways_final_geom", "way", "ways_iceberg")
    merge_into_table(spark, args.table_name, "ways_iceberg", "t.id = s.id AND t.type = 'way'")
    delete_from_table(spark, args.table_name, "osc_way_deletes", "t.id = s.id AND t.type = 'way'")

    # Relations: dirty = direct OSC changes + relations whose member ways are dirty.
    all_dirty_relations(
        spark, "base_relations", "osc_relation_upserts", "dirty_ways", "dirty_relations"
    )
    relations_need_geometry(spark, "dirty_relations", "rels_need_geom")
    construct_multipolygon(spark, "rels_need_geom", "ways_final_geom", "rels_geom")
    relation_merge_geometry_data(spark, "dirty_relations", "rels_geom", "dirty_rels_geom")
    apply_osc_with_geometry(
        spark, "base_relations", "dirty_rels_geom", "osc_relation_deletes", "relations_final_geom"
    )
    prepare_for_iceberg(spark, "relations_final_geom", "relation", "relations_iceberg")
    merge_into_table(
        spark, args.table_name, "relations_iceberg", "t.id = s.id AND t.type = 'relation'"
    )
    delete_from_table(
        spark, args.table_name, "osc_relation_deletes", "t.id = s.id AND t.type = 'relation'"
    )

    _print_counts(spark, args.table_name)


def _print_counts(spark: SparkSession, table_name: str):
    counts = get_table_count(spark, table_name)
    for osm_type, count in counts.items():
        print(f"  {osm_type}: {count:,} features")


# ============================================================================
# Entry point
# ============================================================================


def main(args):
    """Wire up Spark, dispatch to init or update mode, always stop the session."""
    spark = create_spark_session(
        app_name="KryptOSM",
        master=args.spark_master,
        catalog_type=args.catalog_type,
        catalog_name=args.catalog_name,
        warehouse=args.catalog_warehouse,
        table_location=args.table_location,
    )
    try:
        if args.mode == "init":
            run_init_mode(spark, args)
        else:
            run_update_mode(spark, args)
    except Exception as e:
        print(f"Error during execution: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    from .cli import parse_args

    main(parse_args())
