"""
Test utilities — shared Spark session factory, region config, and helpers.

Set KRYPTOSM_REGION=oregon to test with Oregon data (default: dc).
"""

import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

_JAR_DIR = Path.home() / ".cache" / "kryptosm" / "jars"
_JARS = (
    "sedona-spark-shaded-3.5_2.12-1.9.0.jar",
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
    "iceberg-aws-bundle-1.6.1.jar",
)
_MAVEN_PACKAGES = (
    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.0,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
)

_DATA_DIR = Path(__file__).parent / "data"
_OUTPUT_DIR = _DATA_DIR / "output"
WAREHOUSE_DIR = _OUTPUT_DIR / "warehouse"


@dataclass
class Region:
    short_name: str
    parquet_path: Path
    replication_url: str
    driver_memory: str = "2g"
    parallelism: int = 8

    @property
    def db_name(self) -> str:
        """Per-region test database name (production uses ``kryptosm``)."""
        return f"krypton_{self.short_name}"

    @property
    def nodes_table(self) -> str:
        return f"hadoop_catalog.{self.db_name}.nodes"

    @property
    def ways_table(self) -> str:
        return f"hadoop_catalog.{self.db_name}.ways"

    @property
    def relations_table(self) -> str:
        return f"hadoop_catalog.{self.db_name}.relations"

    @property
    def node_to_ways(self) -> str:
        return f"hadoop_catalog.{self.db_name}.node_to_ways"

    @property
    def way_to_relations(self) -> str:
        return f"hadoop_catalog.{self.db_name}.way_to_relations"

    @property
    def node_to_relations(self) -> str:
        return f"hadoop_catalog.{self.db_name}.node_to_relations"

    @property
    def relation_to_relations(self) -> str:
        return f"hadoop_catalog.{self.db_name}.relation_to_relations"

    @property
    def osc_archive(self) -> str:
        return f"hadoop_catalog.{self.db_name}.osc_changes"

    @property
    def osc_dir(self) -> Path:
        return _OUTPUT_DIR / "osc" / self.short_name


REGIONS = {
    "dc": Region(
        short_name="dc",
        parquet_path=_DATA_DIR / "WashingtonDC" / "dc.parquet",
        replication_url="https://download.geofabrik.de/north-america/us/district-of-columbia-updates/",
    ),
    "oregon": Region(
        short_name="oregon",
        parquet_path=_DATA_DIR / "Oregon" / "oregon.parquet",
        replication_url="https://download.geofabrik.de/north-america/us/oregon-updates/",
        driver_memory="8g",
        parallelism=4,
    ),
}


def get_region() -> Region:
    """Return the active region from KRYPTOSM_REGION env var (default: dc)."""
    name = os.environ.get("KRYPTOSM_REGION", "dc").lower()
    if name not in REGIONS:
        raise ValueError(f"Unknown region '{name}'. Available: {', '.join(REGIONS)}")
    return REGIONS[name]


def create_spark_session_for_testing(
    warehouse_dir: str = str(WAREHOUSE_DIR),
    driver_memory: str = "2g",
    parallelism: int = 8,
) -> SparkSession:
    """Local-mode Spark+Sedona+Iceberg session for E2E tests.

    Larger datasets (Oregon, planet) need more heap and fewer concurrent
    writers to avoid OOM during bloom-filter allocation and range-sort
    shuffle spills.  Callers can tune via ``driver_memory`` and
    ``parallelism``.
    """
    from sedona.spark import SedonaContext

    # Clear any stopped session so getOrCreate doesn't return a dead one.
    SparkSession._instantiatedSession = None

    local_dir = os.path.join(warehouse_dir, ".spark-local")
    os.makedirs(local_dir, exist_ok=True)
    os.makedirs(warehouse_dir, exist_ok=True)
    _JAR_DIR.mkdir(parents=True, exist_ok=True)

    cached = [str(_JAR_DIR / j) for j in _JARS if (_JAR_DIR / j).exists()]

    builder = (
        SparkSession.builder.appName("KryptOSM Test")
        .master(f"local[{parallelism}]")
        .config("spark.driver.extraJavaOptions", "-Djts.overlay=ng")
        .config("spark.executor.extraJavaOptions", "-Djts.overlay=ng")
        .config("sedona.join.numpartition", "4000")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", warehouse_dir)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if cached:
        builder = builder.config("spark.jars", ",".join(cached))
    else:
        builder = builder.config("spark.jars.packages", _MAVEN_PACKAGES)

    return SedonaContext.create(builder.getOrCreate())


@contextmanager
def stage(name: str):
    """Wall-clock timer for test phases."""
    print(f"┌─ {name} ...")
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"└─ {name}: {elapsed:.2f}s\n")


def configure_logging():
    """Enable kryptosm log output for tests."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s: %(message)s",
    )
