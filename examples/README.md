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

These scripts target **Glue 5.0** (Spark 3.5, Python 3.11) with **Apache
Sedona 1.9+** (which bundles JTS 1.20+, where the robust OverlayNG engine is
the default — no JTS system property needed). Glue 5.0 ships Iceberg 1.6 out
of the box, so you only need to pull in Sedona yourself.

### Job parameters

In the Glue console: **Job details → Advanced properties → Job parameters.**
Each row is one parameter (a `Key` + a `Value`). Add these one row at a time:

| Key                           | Value                                                                                                                |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `--datalake-formats`          | `iceberg`                                                                                                            |
| `--additional-python-modules` | `apache-sedona==1.9.0,kryptosm,urllib3==1.26.20`                                                                     |
| `--extra-jars`                | `s3://YOUR-BUCKET/jars/sedona-spark-shaded-3.5_2.12-1.9.0.jar,s3://YOUR-BUCKET/jars/geotools-wrapper-1.7.0-28.5.jar` |
| `--conf`                      | _(see below)_                                                                                                        |

Pin the Sedona Python wheel and the shaded JAR to the **same** version (and
make sure that version's the one you uploaded to S3).

> ℹ️ **Why the `urllib3>=1.26.0` pin?** `pyosmium`'s `ReplicationServer`
> (which we use to fetch OSC files) builds a `requests.Session` with
> `Retry(allowed_methods=...)`. The `allowed_methods` keyword was renamed
> from `method_whitelist` in **urllib3 1.26** (Nov 2020). Glue 5.0 ships an
> older urllib3 in its base image (pinned to keep botocore happy), and
> without this override `glue_apply_osc.py` crashes with
> `TypeError: Retry.__init__() got an unexpected keyword argument 'allowed_methods'`
> the first time it tries to fetch a replication state file.

### The single `--conf` value

The `--conf` parameter is the one that's easy to get wrong, and the wrong
format produces `IllegalArgumentException: Invalid input to --conf`.

> ⚠️ **Use exactly ONE `--conf` job-parameter row.** Its value is a
> **space-separated** chain of `key=value` pairs, with the literal text
> `--conf` between each pair. Do **not** add multiple rows whose key is
> `--conf`, and do **not** include line breaks inside the value field.

Paste this whole thing as the single value of the one `--conf` parameter:

```text
spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://meta-overture-staging/transportation_splitter/planet-iceberg/warehouse/
```

Note that the value **starts directly with the first key=value** (no leading
`--conf`) and uses `--conf` as the separator from the second pair onward.
This is AWS Glue's convention, not standard Spark CLI.

> ℹ️ **No `extraJavaOptions` needed.** Sedona 1.9+ bundles JTS 1.20+ where
> the robust OverlayNG engine is the default.

### After it runs

Watch CloudWatch for the per-stage `[N/8]` log lines emitted by the script.
A successful relations stage looks like:

```text
kryptosm.glue_init: [6/8] Build + write relations
...
kryptosm.glue_init: [8/8] Final counts
kryptosm.glue_init:   node           ...
kryptosm.glue_init:   way            ...
kryptosm.glue_init:   relation       ...
kryptosm.glue_init: kryptosm INIT complete — glue_catalog.daily_planet.osm
```

### Other notes

- The Sedona shaded JAR (`sedona-spark-shaded-3.5_2.12-1.9.0.jar`) and its
  `geotools-wrapper` companion must be uploaded to S3 and referenced via
  `--extra-jars`. Glue's package mirror does not ship them.
- `apache-sedona==1.9.0` provides the Python bindings used by `SedonaContext`.
  Keep the Python wheel version and the JAR version in lockstep.
- `--datalake-formats iceberg` ensures the Iceberg runtime + AWS bundle are on
  the classpath.
- After editing `kryptosm` source, **bump the version in `pyproject.toml`** and
  rebuild the wheel before re-running the Glue job. `--additional-python-modules`
  caches by package name + version; without a version bump Glue may serve the
  previously installed wheel and your fixes won't take effect.

Both scripts also work with `spark-submit` outside Glue if you set the same
configs yourself.
