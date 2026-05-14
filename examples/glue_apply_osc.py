"""
Glue OSC apply: fetch the next pending OSC file and apply it.

Idempotent — each invocation downloads exactly one OSC, MERGEs it into the
table, and stamps the new sequence number on the table properties. Schedule
this on a Glue trigger and it will catch up automatically.

Paste this into the Glue Job "Script" editor (Spark 3.5 / Glue 5.0 / Sedona 1.8).
"""

import logging
import os
import sys
import tempfile

from kryptosm import apply_osc, get_table_count, next_osc_path
from kryptosm.iceberg import get_last_applied_sequence, table_exists
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# ---------------------------------------------------------------------------
# Config — edit these for your environment
# ---------------------------------------------------------------------------
WAREHOUSE        = "s3://meta-overture-staging/transportation_splitter/planet-iceberg/warehouse/"
CATALOG          = "glue_catalog"
DB_NAME          = "daily_planet"
TABLE            = "osm"

# /tmp on Glue is fast local SSD, wiped between runs — fine, OSC files are tiny.
OSC_STAGING_DIR  = tempfile.gettempdir()

# Planet replication (day). Use a country/region URL for sub-planet jobs.
REPLICATION_URL  = "https://planet.openstreetmap.org/replication/day/"

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
logger = logging.getLogger("kryptosm.glue_apply_osc")

logger.info("kryptosm OSC APPLY (Glue) — table=%s", TABLE_NAME)
logger.info("  replication: %s", REPLICATION_URL)

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
    SparkSession.builder.appName(f"kryptosm-osc-{DB_NAME}")
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
_jts_overlay = spark.sparkContext._jvm.System.getProperty("jts.overlay")
logger.info("jts.overlay (driver) = %r  (expect 'ng')", _jts_overlay)
if _jts_overlay != "ng":
    logger.warning(
        "jts.overlay is not 'ng' on the driver — set it via Glue job param "
        "`--conf spark.driver.extraJavaOptions=-Djts.overlay=ng` "
        "(and the executor equivalent). OSC apply may fail on relations."
    )

# ---------------------------------------------------------------------------
# Verify the table exists (init must have run first)
# ---------------------------------------------------------------------------
assert table_exists(spark, TABLE_NAME), \
    f"{TABLE_NAME} does not exist. Run glue_init.py first."

# ---------------------------------------------------------------------------
# Counts BEFORE
# ---------------------------------------------------------------------------
before = get_table_count(spark, TABLE_NAME)
seq_before = get_last_applied_sequence(spark, TABLE_NAME)
logger.info("sequence BEFORE: %s", seq_before)

# ---------------------------------------------------------------------------
# Fetch + apply next OSC
# ---------------------------------------------------------------------------
os.makedirs(OSC_STAGING_DIR, exist_ok=True)
osc_path = next_osc_path(
    spark,
    TABLE_NAME,
    OSC_STAGING_DIR,
    base_url=REPLICATION_URL,
)

if osc_path is None:
    logger.info("Already current — nothing to apply.")
    spark.stop()
    sys.exit(0)

logger.info("Applying %s (%d bytes)", os.path.basename(osc_path), os.path.getsize(osc_path))
apply_osc(spark, TABLE_NAME, osc_path, NODE_TO_WAYS, WAY_TO_RELATIONS)

# ---------------------------------------------------------------------------
# Counts AFTER
# ---------------------------------------------------------------------------
after = get_table_count(spark, TABLE_NAME)
seq_after = get_last_applied_sequence(spark, TABLE_NAME)

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
