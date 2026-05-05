"""
Spark session factory: configures Sedona + Iceberg.
"""

import os
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

JAR_DIR = Path.home() / ".cache" / "kryptosm" / "jars"
JARS = (
    "sedona-spark-shaded-3.5_2.12-1.8.1.jar",
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
    "iceberg-aws-bundle-1.6.1.jar",
)
MAVEN_PACKAGES = (
    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
)


def create_spark_session(
    app_name: str = "KryptOSM",
    master: str = "local[*]",
    catalog_type: str = "hadoop",
    catalog_name: str = "glue_catalog",
    warehouse: Optional[str] = None,
    table_location: Optional[str] = None,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    """Build a Spark session with Sedona + Iceberg configured for the chosen catalog."""
    from sedona.spark import SedonaContext

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.extraJavaOptions", "-Djts.overlay=ng")
        .config("spark.executor.extraJavaOptions", "-Djts.overlay=ng")
        .config("sedona.join.numpartition", "4000")
        .config("spark.kryoserializer.buffer", "128m")
        .config("spark.driver.maxResultSize", "5g")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    )

    # Prefer locally-cached JARs; otherwise let Spark resolve via Maven.
    JAR_DIR.mkdir(parents=True, exist_ok=True)
    cached = [str(JAR_DIR / j) for j in JARS if (JAR_DIR / j).exists()]
    if cached:
        builder = builder.config("spark.jars", ",".join(cached))
    else:
        builder = builder.config("spark.jars.packages", MAVEN_PACKAGES)

    if catalog_type == "glue":
        builder = (
            builder
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        )
        if table_location:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", table_location)
    else:
        builder = (
            builder
            .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        )
        if warehouse:
            builder = builder.config("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    return SedonaContext.create(builder.getOrCreate())


def create_spark_session_for_testing(
    warehouse_dir: str = "/tmp/iceberg_warehouse",
) -> SparkSession:
    """Local-mode Spark session used by the E2E tests."""
    os.makedirs(warehouse_dir, exist_ok=True)
    return create_spark_session(
        app_name="KryptOSM Test",
        master="local[1]",
        catalog_type="hadoop",
        warehouse=warehouse_dir,
        extra_configs={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "127.0.0.1",
            "spark.blockManager.port": "0",
            "spark.driver.port": "0",
            "spark.ui.enabled": "false",
            # AQE coalesces tiny shuffle partitions, switches to broadcast joins
            # dynamically, and handles skew - all of which matter at small local
            # scale where 200 default shuffle partitions over a few thousand rows
            # is pure overhead.
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
        },
    )
