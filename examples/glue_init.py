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
    KryptonDatabase,
    TableConfig,
    build_node_geometry,
    build_way_linestrings,
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
    promote_closed_ways_to_areas,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from kryptosm.iceberg import table_exists
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, coalesce, col, lit
from sedona.spark import SedonaContext

# ---------------------------------------------------------------------------
# Config — edit these for your environment
# ---------------------------------------------------------------------------
INPUT_PARQUET = "s3://meta-overture-staging/planet-iceberg/raw/"
WAREHOUSE     = "s3://meta-overture-staging/transportation_splitter/planet-iceberg/warehouse/"
db = KryptonDatabase(catalog="glue_catalog", db_name="kryptosm")

# When True, do NOT drop/recreate existing tables, and skip any per-type write
# step whose rows already exist. Set to False for a clean rebuild from scratch.
RESUME        = True

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

logger.info("kryptosm INIT (Glue) — db=%s  RESUME=%s", db.db_name, RESUME)
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
    SparkSession.builder.appName(f"kryptosm-init-{db.db_name}")
    .config("spark.driver.extraJavaOptions", "-Djts.overlay=ng")
    .config("spark.executor.extraJavaOptions", "-Djts.overlay=ng")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(f"spark.sql.catalog.{db.catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{db.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{db.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{db.catalog}.warehouse", WAREHOUSE)
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
have_nodes     = RESUME and _has_rows(db.nodes)
have_ways      = RESUME and _has_rows(db.ways)
have_relations = RESUME and _has_rows(db.relations)

if have_nodes and have_ways and have_relations:
    logger.info("[1/8] RESUME — all per-type tables exist with data")
else:
    logger.info("[1/8] Create per-type tables + indexes + OSC archive")
    if not (RESUME and table_exists(spark, db.nodes)):
        create_nodes_table(spark, db.nodes, config=TableConfig.nodes_production())
    if not (RESUME and table_exists(spark, db.ways)):
        create_ways_table(spark, db.ways, config=TableConfig.ways_production())
    if not (RESUME and table_exists(spark, db.relations)):
        create_relations_table(
            spark, db.relations, config=TableConfig.relations_production()
        )
    if not (RESUME and table_exists(spark, db.node_to_ways)):
        create_index_tables(
            spark, db.node_to_ways, db.way_to_relations,
            node_to_relations=db.node_to_relations,
            relation_to_relations=db.relation_to_relations,
            config=TableConfig.ways_production(),
        )
    if not (RESUME and table_exists(spark, db.osc_archive)):
        create_osc_archive_table(
            spark, db.osc_archive, config=TableConfig.ways_production()
        )

# ---------------------------------------------------------------------------
# [2/8] Register input Parquet views
# ---------------------------------------------------------------------------
logger.info("[2/8] Register input Parquet views")
base = INPUT_PARQUET.rstrip("/")
spark.read.parquet(f"{base}/type=node") \
    .withColumn("changeset", coalesce(col("changeset"), lit(0))) \
    .withColumn("additional_changesets", array().cast("array<bigint>")) \
    .createOrReplaceTempView("input_nodes")
spark.read.parquet(f"{base}/type=way") \
    .withColumn("changeset", coalesce(col("changeset"), lit(0))) \
    .withColumn("additional_changesets", array().cast("array<bigint>")) \
    .createOrReplaceTempView("input_ways_raw_pq")
flatten_way_refs(spark, "input_ways_raw_pq", "input_ways")
spark.read.parquet(f"{base}/type=relation") \
    .withColumn("changeset", coalesce(col("changeset"), lit(0))) \
    .withColumn("additional_changesets", array().cast("array<bigint>")) \
    .createOrReplaceTempView("input_relations")

# ---------------------------------------------------------------------------
# [3/8] Build + write nodes  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_nodes:
    logger.info("[3/8] RESUME — nodes already loaded, loading view from table")
    load_with_geom(spark, db.nodes, "nodes_with_geom")
else:
    logger.info("[3/8] Build + write nodes")
    build_node_geometry(spark, "input_nodes", "nodes_with_geom")
    prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
    (
        spark.sql("SELECT * FROM nodes_final")
        .repartitionByRange(2000, col("id"))
        .writeTo(db.nodes)
        .using("iceberg")
        .append()
    )
    load_with_geom(spark, db.nodes, "nodes_with_geom")

# ---------------------------------------------------------------------------
# [4/8] Build + write ways  (skipped on RESUME if already present)
# ---------------------------------------------------------------------------
if have_ways:
    logger.info("[4/8] RESUME — ways already loaded, loading view from table")
    load_with_geom(spark, db.ways, "ways_with_geom")
else:
    logger.info("[4/8] Build + write ways")
    build_way_linestrings(spark, "input_ways", "nodes_with_geom", "ways_linestrings")
    promote_closed_ways_to_areas(spark, "ways_linestrings", "ways_with_geom")
    prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
    (
        spark.sql("SELECT * FROM ways_final")
        .repartitionByRange(500, col("id"))
        .writeTo(db.ways)
        .using("iceberg")
        .append()
    )
    load_with_geom(spark, db.ways, "ways_with_geom")

# ---------------------------------------------------------------------------
# [5/8] Populate node_to_ways index  (skipped if already populated)
# ---------------------------------------------------------------------------
if RESUME and _has_rows(db.node_to_ways):
    logger.info("[5/8] RESUME — node_to_ways already populated, skipping")
else:
    logger.info("[5/8] Populate node_to_ways index")
    populate_node_to_ways(spark, db.ways, db.node_to_ways)

# ---------------------------------------------------------------------------
# [6/8] Build + write relations  (skipped on RESUME if already present)
#
# Pre-filter ways and nodes to only those referenced by relation members.
# construct_multipolygon joins the ways view ~4 times (outer/inner polygon
# rings, line relations, collection relations) and relation_merge_geometry_data
# joins it ~2 more (fallback geometry, additional_changesets).  Without
# filtering, each join independently scans the full ways Iceberg table from S3.
# Persisting the filtered subset (typically <2% of all ways) turns those
# repeated full scans into one scan + cached lookups.  Same logic for nodes.
# ---------------------------------------------------------------------------
if have_relations:
    logger.info("[6/8] RESUME — relations already loaded, skipping")
else:
    logger.info("[6/8] Build + write relations")

    spark.sql("""
        SELECT DISTINCT member.ref AS id
        FROM (SELECT explode(members) AS member FROM input_relations)
        WHERE member.type = 'way'
    """).createOrReplaceTempView("_rel_way_ids")
    ways_for_rels = spark.sql("""
        SELECT w.*
        FROM ways_with_geom w
        JOIN _rel_way_ids r ON w.id = r.id
    """).persist()
    ways_for_rels.createOrReplaceTempView("ways_for_relations")

    spark.sql("""
        SELECT DISTINCT member.ref AS id
        FROM (SELECT explode(members) AS member FROM input_relations)
        WHERE member.type = 'node'
    """).createOrReplaceTempView("_rel_node_ids")
    nodes_for_rels = spark.sql("""
        SELECT n.*
        FROM nodes_with_geom n
        JOIN _rel_node_ids r ON n.id = r.id
    """).persist()
    nodes_for_rels.createOrReplaceTempView("nodes_for_relations")

    relations_need_geometry(spark, "input_relations", "relations_need_geom")
    construct_multipolygon(
        spark,
        "relations_need_geom",
        "ways_for_relations",
        "relations_geom",
        nodes_geometry="nodes_for_relations",
    )
    relation_merge_geometry_data(
        spark,
        "input_relations",
        "relations_geom",
        "relations_with_geom",
        ways_geometry="ways_for_relations",
        nodes_geometry="nodes_for_relations",
    )
    prepare_for_iceberg(spark, "relations_with_geom", "relation", "relations_final")
    (
        spark.sql("SELECT * FROM relations_final")
        .repartitionByRange(20, col("id"))
        .writeTo(db.relations)
        .using("iceberg")
        .append()
    )

    ways_for_rels.unpersist()
    nodes_for_rels.unpersist()

# ---------------------------------------------------------------------------
# [7/8] Populate way_to_relations / node_to_relations / relation_to_relations
# ---------------------------------------------------------------------------
if RESUME and _has_rows(db.way_to_relations):
    logger.info("[7/8] RESUME — way_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate way_to_relations index")
    populate_way_to_relations(spark, db.relations, db.way_to_relations)

if RESUME and _has_rows(db.node_to_relations):
    logger.info("[7/8] RESUME — node_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate node_to_relations index")
    populate_node_to_relations(spark, db.relations, db.node_to_relations)

if RESUME and _has_rows(db.relation_to_relations):
    logger.info("[7/8] RESUME — relation_to_relations already populated, skipping")
else:
    logger.info("[7/8] Populate relation_to_relations index")
    populate_relation_to_relations(spark, db.relations, db.relation_to_relations)

# ---------------------------------------------------------------------------
# [8/8] Final counts
# ---------------------------------------------------------------------------
logger.info("[8/8] Final counts")
counts = get_table_count(spark, db.nodes, db.ways, db.relations)
for osm_type in ("node", "way", "relation"):
    logger.info("  %-9s %16d", osm_type, counts.get(osm_type, 0))
logger.info("  %-9s %16d", "total", sum(counts.values()))
logger.info("kryptosm INIT complete — %s.%s", db.catalog, db.db_name)

spark.stop()
