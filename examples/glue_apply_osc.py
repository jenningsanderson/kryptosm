"""
Glue OSC apply: fetch the next pending OSC file and apply it to Krypton.

Idempotent — each invocation downloads exactly one OSC, MERGEs it into the
three per-type tables, and stamps the new sequence number on the OSC archive
table's properties. Schedule this on a Glue trigger and it will catch up
automatically.

Paste this into the Glue Job "Script" editor (Spark 3.5 / Glue 5.0 / Sedona 1.8).
"""

import logging
import os
import sys
import tempfile

from kryptosm import KryptonDatabase, apply_osc, get_table_count, next_osc_path
from kryptosm.iceberg import get_min_applied_sequence, table_exists
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# ---------------------------------------------------------------------------
# Config — edit these for your environment
# ---------------------------------------------------------------------------
WAREHOUSE        = "s3://meta-overture-staging/transportation_splitter/planet-iceberg/warehouse/"
db = KryptonDatabase(catalog="glue_catalog", db_name="kryptosm")

# /tmp on Glue is fast local SSD, wiped between runs — fine, OSC files are tiny.
OSC_STAGING_DIR  = tempfile.gettempdir()

# Planet replication (day). Use a country/region URL for sub-planet jobs.
REPLICATION_URL  = "https://planet.openstreetmap.org/replication/day/"

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
logger = logging.getLogger("kryptosm.glue_apply_osc")

logger.info("kryptosm OSC APPLY (Glue) — db=%s.%s", db.catalog, db.db_name)
logger.info("  replication: %s", REPLICATION_URL)

# ---------------------------------------------------------------------------
# Spark / Sedona / Iceberg session
# ---------------------------------------------------------------------------
spark = SedonaContext.create(
    SparkSession.builder.appName(f"kryptosm-osc-{db.db_name}")
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

# ---------------------------------------------------------------------------
# Verify the per-type tables exist (init must have run first)
# ---------------------------------------------------------------------------
for tbl in (db.nodes, db.ways, db.relations, db.osc_archive):
    assert table_exists(spark, tbl), f"{tbl} does not exist. Run glue_init.py first."

# ---------------------------------------------------------------------------
# Counts BEFORE
# ---------------------------------------------------------------------------
before = get_table_count(spark, db.nodes, db.ways, db.relations)
seq_before = get_min_applied_sequence(spark, db.nodes, db.ways, db.relations)
logger.info("sequence BEFORE: %s", seq_before)

# ---------------------------------------------------------------------------
# Fetch + apply next OSC
# ---------------------------------------------------------------------------
os.makedirs(OSC_STAGING_DIR, exist_ok=True)
osc_path = next_osc_path(
    spark,
    db.nodes, db.ways, db.relations,
    OSC_STAGING_DIR,
    base_url=REPLICATION_URL,
)

if osc_path is None:
    logger.info("Already current — nothing to apply.")
    spark.stop()
    sys.exit(0)

logger.info("Applying %s (%d bytes)", os.path.basename(osc_path), os.path.getsize(osc_path))
apply_osc(
    spark, osc_path,
    db.nodes, db.ways, db.relations,
    db.node_to_ways, db.way_to_relations,
    db.node_to_relations, db.relation_to_relations,
    db.osc_archive,
)

# ---------------------------------------------------------------------------
# Counts AFTER
# ---------------------------------------------------------------------------
after = get_table_count(spark, db.nodes, db.ways, db.relations)
seq_after = get_min_applied_sequence(spark, db.nodes, db.ways, db.relations)

for osm_type in ("node", "way", "relation"):
    b = before.get(osm_type, 0)
    a = after.get(osm_type, 0)
    delta = a - b
    sign = "+" if delta > 0 else ("-" if delta < 0 else " ")
    logger.info("  %-9s %16d -> %16d  (%s%d)", osm_type, b, a, sign, abs(delta))
logger.info("sequence: %s -> %s", seq_before, seq_after)

# Clean up the staged OSC so /tmp doesn't fill up across runs.
if os.path.exists(osc_path):
    os.remove(osc_path)

logger.info("kryptosm OSC APPLY complete")
spark.stop()
