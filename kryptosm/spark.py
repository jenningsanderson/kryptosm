"""
Spark session management with Sedona and Iceberg support.
"""

import os
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession


def get_sedona_jars():
    """
    Get Sedona JAR files. Downloads them if not present.

    Returns:
        List of JAR file paths
    """
    # Check if JARs are already downloaded
    jar_dir = Path.home() / ".cache" / "kryptosm" / "jars"
    jar_dir.mkdir(parents=True, exist_ok=True)

    sedona_jar = jar_dir / "sedona-spark-shaded-3.5_2.12-1.8.1.jar"

    jars = []

    # Check if Sedona JAR exists
    if sedona_jar.exists():
        jars.append(str(sedona_jar))
    else:
        print(f"Warning: Sedona JAR not found at {sedona_jar}")
        print("Run: python download_jars.py")

    return jars


def get_iceberg_jars():
    """
    Get Iceberg JAR files.

    Returns:
        List of JAR file paths
    """
    jar_dir = Path.home() / ".cache" / "kryptosm" / "jars"
    jar_dir.mkdir(parents=True, exist_ok=True)

    iceberg_jar = jar_dir / "iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
    iceberg_aws_jar = jar_dir / "iceberg-aws-bundle-1.6.1.jar"

    jars = []

    if iceberg_jar.exists():
        jars.append(str(iceberg_jar))
    else:
        print(f"Warning: Iceberg JAR not found at {iceberg_jar}")
        print("Run: python download_jars.py")

    if iceberg_aws_jar.exists():
        jars.append(str(iceberg_aws_jar))
    else:
        print(f"Warning: Iceberg AWS JAR not found at {iceberg_aws_jar}")
        print("Run: python download_jars.py")

    return jars


def create_spark_session(
    app_name: str = "KryptOSM",
    master: str = "local[*]",
    catalog_type: str = "hadoop",
    catalog_name: str = "glue_catalog",
    warehouse: Optional[str] = None,
    table_location: Optional[str] = None,
    extra_configs: Optional[dict] = None,
    use_sedona_jars: bool = True,
) -> SparkSession:
    """
    Create a Spark session with Sedona and Iceberg support.

    Args:
        app_name: Spark application name
        master: Spark master URL
        catalog_type: Iceberg catalog type ('glue' or 'hadoop')
        catalog_name: Catalog name for Glue
        warehouse: Warehouse path for Hadoop catalog
        table_location: Table location (used as warehouse for Glue)
        extra_configs: Additional Spark configurations
        use_sedona_jars: Whether to download and use Sedona JARs

    Returns:
        SparkSession with Sedona and Iceberg configured
    """
    from sedona.spark import SedonaContext

    builder = SparkSession.builder.appName(app_name).master(master)

    # Sedona configuration
    builder = (
        builder.config("spark.driver.extraJavaOptions", "-Djts.overlay=ng")
        .config("spark.executor.extraJavaOptions", "-Djts.overlay=ng")
        .config("sedona.join.numpartition", "4000")
        .config("spark.kryoserializer.buffer", "128m")
        .config("spark.driver.maxResultSize", "5g")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    # Kryo serialization for Sedona
    builder = builder.config(
        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
    ).config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")

    # Add Sedona and Iceberg JARs if requested
    if use_sedona_jars:
        # Try to use local JARs first, then fall back to Maven
        jar_dir = Path.home() / ".cache" / "kryptosm" / "jars"
        sedona_jar = jar_dir / "sedona-spark-shaded-3.5_2.12-1.8.1.jar"
        iceberg_jar = jar_dir / "iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
        iceberg_aws_jar = jar_dir / "iceberg-aws-bundle-1.6.1.jar"

        jars = []
        if sedona_jar.exists():
            jars.append(str(sedona_jar))
        if iceberg_jar.exists():
            jars.append(str(iceberg_jar))
        if iceberg_aws_jar.exists():
            jars.append(str(iceberg_aws_jar))

        if jars:
            # Use local JARs
            builder = builder.config("spark.jars", ",".join(jars))
        else:
            # Use Maven packages - let Spark download them
            # Note: This requires internet access
            builder = builder.config(
                "spark.jars.packages",
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
            )

    if catalog_type == "glue":
        # Glue catalog configuration
        builder = (
            builder.config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
        )

        if table_location:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", table_location)
    else:
        # Hadoop catalog configuration
        builder = builder.config(
            "spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog"
        ).config("spark.sql.catalog.hadoop_catalog.type", "hadoop")

        if warehouse:
            builder = builder.config("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)

    # Apply extra configs
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Initialize Sedona
    spark = SedonaContext.create(spark)

    return spark


def create_spark_session_for_testing(
    warehouse_dir: str = "/tmp/iceberg_warehouse",
    use_sedona_jars: bool = True,
) -> SparkSession:
    """
    Create a Spark session for local testing with Hadoop catalog.

    Args:
        warehouse_dir: Local directory for Iceberg warehouse
        use_sedona_jars: Whether to download and use Sedona JARs

    Returns:
        SparkSession configured for local testing
    """
    import os

    os.makedirs(warehouse_dir, exist_ok=True)

    return create_spark_session(
        app_name="KryptOSM Test",
        master="local[1]",  # Use 1 core to avoid networking issues
        catalog_type="hadoop",
        warehouse=warehouse_dir,
        use_sedona_jars=use_sedona_jars,
        extra_configs={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "127.0.0.1",
            "spark.blockManager.port": "0",
            "spark.driver.port": "0",
            "spark.ui.enabled": "false",
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
        },
    )
