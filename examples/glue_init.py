"""
Glue init: build the full Krypton-on-Iceberg database from Parquet on S3.

Paste this into the Glue Job "Script" editor (Spark 3.5 / Glue 5.0 / Sedona 1.8).

Set RESUME = True to skip stages whose data is already present (useful after a
failed run that wrote nodes + ways but not relations).

Production database name is ``kryptosm`` (Python package name; matches our
"krypton" / Superman / ice-planet theming).
"""

import logging
import sys

from kryptosm import (
    TableConfig,
    build_linestring_for_ways,
    build_node_geometry,
    build_ways_geometry_from_linestring,
    construct_multipolygon,
    create_index_tables,
    create_nodes_table,
    create_osc_archive_table,
    create_relations_table,
    create_ways_table,
    flatten_way_refs,
    get_table_count,
    load_with_geom,
    populate_node_to_relations,
    populate_node_to_ways,
    populate_relation_to_relations,
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
DB_NAME       = "kryptosm"

# When True, do NOT drop/recreate existing tables, and skip any per-type write
# step whose rows already exist. Set to False for a clean rebuild from scratch.
RESUME        = True

NODES_TABLE            = f"{CATALOG}.{DB_NAME}.nodes"
WAYS_TABLE             = f"{CATALOG}.{DB_NAME}.ways"
RELATIONS_TABLE        = f"{CATALOG}.{DB_NAME}.relations"
NODE_TO_WAYS           = f"{CATALOG}.{DB_NAME}.node_to_ways"
WAY_TO_RELATIONS       = f"{CATALOG}.{DB_NAME}.way_to_relations"
NODE_TO_RELATIONS      = f"{CATALOG}.{DB_NAME}.node_to_relations"
RELATION_TO_RELATIONS  = f"{CATALOG}.{DB_NAME}.relation_to_relations"
OSC_ARCHIVE            = f"{CATALOG}.{DB_NAME}.osc_changes"

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

logger.info("kryptosm INIT (Glue) — db=%s  RESUME=%s", DB_NAME, RESUME)
logger.info("  input:     %s", INPUT_PARQUET)
logger.info("  warehouse: %s", WAREHOUSE)

# ---------------------------------------------------------------------------
# Spark / Sedona / Iceberg session
#
# NOTE on `-Djts.overlay=ng`: this flag MUST be set as a Glue Job parameter
# (`--conf spark.driver.extraJavaOptions=-Djts.overlay=ng` and the executor
# equivalent) — the in-script `.config(...)` calls below are silently ignored
# on Glue because Glue starts the JVM before this script runs. The lines are
# kept here so the script also works under a fresh `spark-submit` locally.
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

# Confirm the JTS overlay engine that's actually active on the driver JVM.
spark.sparkContext._jvm.System.setProperty("jts.overlay", "ng")
_jts_overlay = spark.sparkContext._jvm.System.getProperty("jts.overlay")
logger.info("jts.overlay (driver) = %r  (expect 'ng')", _jts_overlay)
if _jts_overlay != "ng":
    logger.warning(
        "jts.overlay is not 'ng' on the driver — programmatic setProperty "
        "did not take effect. Relations stage may fail."
    )


def _has_rows(name: str) -> bool:
    """True if Iceberg table `name` exists and has at least one row."""
    if not table_exists(spark, name):
        return False
    return spark.sql(f"SELECT 1 FROM {name} LIMIT 1").count() > 0


# ---------------------------------------------------------------------------
# [1/8] Create per-type tables + indexes + OSC archive (skipped on RESUME)
# ---------------------------------------------------------------------------
have_nodes     = RESUME and _has_rows(NODES_TABLE)
have_ways      = RESUME and _has_rows(WAYS_TABLE)
have_relations = RESUME and _has_rows(RELATIONS_TABLE)

if have_nodes and have_ways and have_relations:
    logger.info("[1/8] RESUME — all per-type tables exist with data")
else:
    logger.info("[1/8] Create per-type tables + indexes + OSC archive")
    if not (RESUME and table_exists(spark, NODES_TABLE)):
        create_nodes_table(spark, NODES_TABLE, config=TableConfig.nodes_production())
    if not (RESUME and table_exists(spark, WAYS_TABLE)):
        create_ways_table(spark, WAYS_TABLE, config=TableConfig.ways_production())
    if not (RESUME and table_exists(spark, RELATIONS_TABLE)):
        create_relations_table(
            spark, RELATIONS_TABLE, config=TableConfig.relations_production()
        )
    if not (RESUME and table_exists(spark, NODE_TO_WAYS)):
        create_index_tables(
            spark, NODE_TO_WAYS, WAY_TO_RELATIONS,
            node_to_relations=NODE_TO_RELATIONS,
            relation_to_relations=RELATION_TO_RELATIONS,
            config=TableConfig.ways_production(),
        )
    if not (RESUME and table_exists(spark, OSC_ARCHIVE)):
        create_osc_archive_table(
            spark, OSC_ARCHIVE, config=TableConfig.ways_production()
        )

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
    logger.info("[3/8] RESUME — nodes already loaded, loading view from table")
    load_with_geom(spark, NODES_TABLE, "nodes_with_geom")
else:
    logger.info("[3/8] Build + write nodes")
    build_node_geometry(spark, "input_nodes", "nodes_with_geom")
    prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
    (
        spark.sql("SELECT * FROM nodes_final")
        .repartitionByRange(2000, col("id"))
        .writeTo(NODES_TABLE)
        .using("iceberg")
        .append()
    )
    load_with_geom(spark, NODES_TABLE, "nodes_with_geom")

# ---------------------------------------------------------------------------
# [4/8] Build + write ways  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_ways:
    logger.info("[4/8] RESUME — ways already loaded, loading view from table")
    load_with_geom(spark, WAYS_TABLE, "ways_with_geom")
else:
    logger.info("[4/8] Build + write ways")
    build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
    build_ways_geometry_from_linestring(spark, "ways_linestrings", "ways_with_geom")
    prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
    (
        spark.sql("SELECT * FROM ways_final")
        .repartitionByRange(500, col("id"))
        .writeTo(WAYS_TABLE)
        .using("iceberg")
        .append()
    )
    load_with_geom(spark, WAYS_TABLE, "ways_with_geom")

# ---------------------------------------------------------------------------
# [5/8] Populate node_to_ways index  (skipped if already populated)
# ---------------------------------------------------------------------------
if RESUME and _has_rows(NODE_TO_WAYS):
    logger.info("[5/8] RESUME — node_to_ways already populated, skipping")
else:
    logger.info("[5/8] Populate node_to_ways index")
    populate_node_to_ways(spark, WAYS_TABLE, NODE_TO_WAYS)

# ---------------------------------------------------------------------------
# [6/8] Build + write relations  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_relations:
    logger.info("[6/8] RESUME — relations already loaded, skipping")
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
    spark.sql("SELECT * FROM relations_final").writeTo(RELATIONS_TABLE).using("iceberg").append()

# ---------------------------------------------------------------------------
# [7/8] Populate way_to_relations / node_to_relations / relation_to_relations
# ---------------------------------------------------------------------------
if RESUME and _has_rows(WAY_TO_RELATIONS):
    logger.info("[7/8] RESUME — way_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate way_to_relations index")
    populate_way_to_relations(spark, RELATIONS_TABLE, WAY_TO_RELATIONS)

if RESUME and _has_rows(NODE_TO_RELATIONS):
    logger.info("[7/8] RESUME — node_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate node_to_relations index")
    populate_node_to_relations(spark, RELATIONS_TABLE, NODE_TO_RELATIONS)

if RESUME and _has_rows(RELATION_TO_RELATIONS):
    logger.info("[7/8] RESUME — relation_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate relation_to_relations index")
    populate_relation_to_relations(spark, RELATIONS_TABLE, RELATION_TO_RELATIONS)

# ---------------------------------------------------------------------------
# [8/8] Final counts
# ---------------------------------------------------------------------------
logger.info("[8/8] Final counts")
counts = get_table_count(spark, NODES_TABLE, WAYS_TABLE, RELATIONS_TABLE)
for osm_type in ("node", "way", "relation"):
    logger.info("  %-9s %16d", osm_type, counts.get(osm_type, 0))
logger.info("  %-9s %16d", "total", sum(counts.values()))
logger.info("kryptosm INIT complete — %s.%s", CATALOG, DB_NAME)

spark.stop()
