"""
Glue init: build the full OSM Iceberg table from Parquet on S3.

Paste this into the Glue Job "Script" editor (Spark 3.5 / Glue 5.0 / Sedona 1.8).

Set RESUME = True to skip stages whose data is already present in the table
(useful after a failed run that wrote nodes + ways but not relations).
"""

import logging
import sys

from kryptosm import (
    TableConfig,
    build_linestring_for_ways,
    build_node_geometry,
    build_ways_geometry_from_linestring,
    construct_multipolygon,
    create_iceberg_table,
    create_index_tables,
    flatten_way_refs,
    get_table_count,
    load_with_geom,
    populate_node_to_ways,
    populate_way_to_relations,
    prepare_for_iceberg,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from kryptosm.iceberg import table_exists
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sedona.spark import SedonaContext

# ---------------------------------------------------------------------------
# Config — edit these for your environment
# ---------------------------------------------------------------------------
INPUT_PARQUET = "s3://meta-overture-staging/planet-iceberg/raw/"
WAREHOUSE     = "s3://meta-overture-staging/transportation-splitter/planet-iceberg/warehouse/"
CATALOG       = "glue_catalog"
DB_NAME       = "daily_planet"
TABLE         = "osm"

# When True, do NOT drop/recreate existing tables, and skip any per-type write
# step whose rows already exist. Set to False for a clean rebuild from scratch.
RESUME        = True

TABLE_NAME       = f"{CATALOG}.{DB_NAME}.{TABLE}"
NODE_TO_WAYS     = f"{CATALOG}.{DB_NAME}.node_to_ways"
WAY_TO_RELATIONS = f"{CATALOG}.{DB_NAME}.way_to_relations"

# ---------------------------------------------------------------------------
# Logging — sent to stdout so Glue/CloudWatch picks it up
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logging.getLogger("kryptosm").setLevel(logging.INFO)
logger = logging.getLogger("kryptosm.glue_init")

logger.info("kryptosm INIT (Glue) — table=%s  RESUME=%s", TABLE_NAME, RESUME)
logger.info("  input:     %s", INPUT_PARQUET)
logger.info("  warehouse: %s", WAREHOUSE)

# ---------------------------------------------------------------------------
# Spark / Sedona / Iceberg session
# ---------------------------------------------------------------------------
spark = SedonaContext.create(
    SparkSession.builder.appName(f"kryptosm-init-{DB_NAME}")
    .config("spark.driver.extraJavaOptions", "-Djts.overlay=ng")
    .config("spark.executor.extraJavaOptions", "-Djts.overlay=ng")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)


def _has_rows(name: str) -> bool:
    """True if Iceberg table `name` exists and has at least one row."""
    if not table_exists(spark, name):
        return False
    return spark.sql(f"SELECT 1 FROM {name} LIMIT 1").count() > 0


# ---------------------------------------------------------------------------
# [1/8] Create Iceberg + index tables (skipped on RESUME if they already exist)
# ---------------------------------------------------------------------------
cfg = TableConfig.production()
existing = (
    get_table_count(spark, TABLE_NAME)
    if RESUME and table_exists(spark, TABLE_NAME)
    else {}
)
have_nodes     = existing.get("node", 0) > 0
have_ways      = existing.get("way", 0) > 0
have_relations = existing.get("relation", 0) > 0

if RESUME and table_exists(spark, TABLE_NAME):
    logger.info("[1/8] RESUME — table exists, counts: %s", existing)
else:
    logger.info("[1/8] Create Iceberg + index tables")
    create_iceberg_table(spark, TABLE_NAME, config=cfg)
    create_index_tables(spark, NODE_TO_WAYS, WAY_TO_RELATIONS, config=cfg)
    have_nodes = have_ways = have_relations = False

# ---------------------------------------------------------------------------
# [2/8] Register input Parquet views
# ---------------------------------------------------------------------------
logger.info("[2/8] Register input Parquet views")
base = INPUT_PARQUET.rstrip("/")
spark.read.parquet(f"{base}/type=node").createOrReplaceTempView("input_nodes")
spark.read.parquet(f"{base}/type=way").createOrReplaceTempView("input_ways_raw")
flatten_way_refs(spark, "input_ways_raw", "input_ways")
spark.read.parquet(f"{base}/type=relation").createOrReplaceTempView("input_relations")

# ---------------------------------------------------------------------------
# [3/8] Build + write nodes  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_nodes:
    logger.info("[3/8] RESUME — nodes already loaded (%d), loading view from table",
                existing.get("node", 0))
    load_with_geom(spark, TABLE_NAME, "node", "nodes_with_geom")
else:
    logger.info("[3/8] Build + write nodes")
    build_node_geometry(spark, "input_nodes", "nodes_with_geom")
    prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
    (
        spark.sql("SELECT * FROM nodes_final")
        .repartitionByRange(2000, col("id"))
        .writeTo(TABLE_NAME)
        .using("iceberg")
        .append()
    )
    load_with_geom(spark, TABLE_NAME, "node", "nodes_with_geom")

# ---------------------------------------------------------------------------
# [4/8] Build + write ways  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_ways:
    logger.info("[4/8] RESUME — ways already loaded (%d), loading view from table",
                existing.get("way", 0))
    load_with_geom(spark, TABLE_NAME, "way", "ways_with_geom")
else:
    logger.info("[4/8] Build + write ways")
    build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
    build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
    prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
    (
        spark.sql("SELECT * FROM ways_final")
        .repartitionByRange(500, col("id"))
        .writeTo(TABLE_NAME)
        .using("iceberg")
        .append()
    )
    load_with_geom(spark, TABLE_NAME, "way", "ways_with_geom")

# ---------------------------------------------------------------------------
# [5/8] Populate node_to_ways index  (skipped if already populated)
# ---------------------------------------------------------------------------
if RESUME and _has_rows(NODE_TO_WAYS):
    logger.info("[5/8] RESUME — node_to_ways already populated, skipping")
else:
    logger.info("[5/8] Populate node_to_ways index")
    populate_node_to_ways(spark, TABLE_NAME, NODE_TO_WAYS)

# ---------------------------------------------------------------------------
# [6/8] Build + write relations  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_relations:
    logger.info("[6/8] RESUME — relations already loaded (%d), skipping",
                existing.get("relation", 0))
else:
    logger.info("[6/8] Build + write relations")
    relations_need_geometry(spark, "input_relations", "relations_need_geom")
    construct_multipolygon(
        spark,
        "relations_need_geom",
        "ways_with_geom",
        "relations_geom",
        nodes_geometry="nodes_with_geom",
    )
    relation_merge_geometry_data(
        spark,
        "input_relations",
        "relations_geom",
        "relations_with_geom",
        ways_geometry="ways_with_geom",
        nodes_geometry="nodes_with_geom",
    )
    prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
    spark.sql("SELECT * FROM relations_final").writeTo(TABLE_NAME).using("iceberg").append()

# ---------------------------------------------------------------------------
# [7/8] Populate way_to_relations index  (skipped if already populated)
# ---------------------------------------------------------------------------
if RESUME and _has_rows(WAY_TO_RELATIONS):
    logger.info("[7/8] RESUME — way_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate way_to_relations index")
    populate_way_to_relations(spark, TABLE_NAME, WAY_TO_RELATIONS)

# ---------------------------------------------------------------------------
# [8/8] Final counts
# ---------------------------------------------------------------------------
logger.info("[8/8] Final counts")
counts = get_table_count(spark, TABLE_NAME)
for osm_type in ("node", "way", "relation"):
    logger.info("  %-9s %16d", osm_type, counts.get(osm_type, 0))
logger.info("  %-9s %16d", "total", sum(counts.values()))
logger.info("kryptosm INIT complete — %s", TABLE_NAME)

spark.stop()
