# Glue Examples

Standalone scripts for running kryptosm on **AWS Glue** (Spark) jobs against
the **AWS Glue Data Catalog** + **S3**.

| Script              | Purpose                                                                                                                     |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `glue_init.py`      | One-shot: read OSM Parquet from S3, build node/way/relation geometries, and write the full Iceberg table + reverse indexes. |
| `glue_apply_osc.py` | Idempotent: fetch the next pending OSC file from a replication URL and apply it. Run on a schedule.                         |

Both scripts have a small `Config` block at the top — edit the constants there
for your environment. The defaults point at:

- Input parquet: `s3://meta-overture-staging/planet-iceberg/raw/`
- Warehouse: `s3://meta-overture-staging/planet-iceberg/warehouse/`
- Glue database: `daily_planet`
- Tables: `glue_catalog.daily_planet.osm`, `.node_to_ways`, `.way_to_relations`

## Catalog naming

Glue uses the same three-part identifier as Hadoop:

```text
<catalog>.<database>.<table>
```

The catalog name is **defined in your Spark config** (see `--catalog` flag).
The database becomes a **Glue Database** and the table becomes a **Glue Table**
under it. Glue rejects hyphens in db/table names — stick to lowercase letters,
digits, and underscores.

## Submitting as a Glue 5.0 job

These scripts target **Glue 5.0** (Spark 3.5, Python 3.11) with **Apache Sedona 1.8**.
Glue 5.0 ships Iceberg 1.6 out of the box, so you only need to pull in Sedona
yourself.

Required job parameters:

```text
--datalake-formats              iceberg

--additional-python-modules     apache-sedona==1.8.0,kryptosm

--extra-jars                    s3://YOUR-BUCKET/jars/sedona-spark-shaded-3.5_2.12-1.8.1.jar,s3://YOUR-BUCKET/jars/geotools-wrapper-1.7.0-28.5.jar

--conf                          spark.serializer=org.apache.spark.serializer.KryoSerializer
--conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator
                                  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions
                                  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
                                  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
                                  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
                                  --conf spark.sql.catalog.glue_catalog.warehouse=s3://meta-overture-staging/planet-iceberg/warehouse/
```

Notes:

- The Sedona shaded JAR (`sedona-spark-shaded-3.5_2.12-1.8.1.jar`) and its
  `geotools-wrapper` companion must be uploaded to S3 and referenced via
  `--extra-jars`. Glue's package mirror does not ship them.
- `apache-sedona==1.8.0` provides the Python bindings used by `SedonaContext`.
- `--datalake-formats iceberg` ensures the Iceberg runtime + AWS bundle are on
  the classpath.

Both scripts also work with `spark-submit` outside Glue if you set the same
configs yourself.
