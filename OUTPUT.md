# Output Locations

This document explains where data is stored when using kryptosm.

## Test Output

### Test Data (Input)
- **Location**: `tests/data/dc.parquet/`
- **Structure**:
  ```
  dc.parquet/
    type=node/       # 36 MB, 1.9M nodes
    type=way/        # 12 MB, 280K ways  
    type=relation/   # 840 KB, 3K relations
  ```

### Test Output (Generated)
- **Location**: `tests/data/output/`
- **Structure**:
  ```
  output/
    .gitkeep
    warehouse/       # Iceberg warehouse (when using tests)
      test_db/
        test_dc_nodes/
          data/      # Parquet data files
          metadata/  # Iceberg metadata
    iceberg_table/   # Explicit table locations
  ```

## Production Output

### Using Hadoop Catalog (Local/S3)

When you run:
```bash
kryptosm --mode init \
  --input-path s3://bucket/osm/parquet/ \
  --table-name hadoop_catalog.osm.dc \
  --table-location s3://bucket/iceberg/dc \
  --catalog-type hadoop \
  --catalog-warehouse s3://bucket/iceberg/warehouse
```

**Output locations:**
- **Table data**: `s3://bucket/iceberg/dc/` (or warehouse if not specified)
- **Warehouse**: `s3://bucket/iceberg/warehouse/`
- **Structure**:
  ```
  s3://bucket/iceberg/warehouse/
    osm/
      dc/
        data/           # Parquet files partitioned by type
          type=node/
          type=way/
          type=relation/
        metadata/       # Iceberg metadata
          *.json
          snap-*.avro
  ```

### Using Glue Catalog (AWS)

When you run:
```bash
kryptosm --mode init \
  --input-path s3://bucket/osm/parquet/ \
  --table-name glue_catalog.osm.dc \
  --table-location s3://bucket/iceberg/dc \
  --catalog-type glue
```

**Output locations:**
- **Table data**: `s3://bucket/iceberg/dc/`
- **Glue catalog**: `glue_catalog.osm.dc` (metadata in AWS Glue)
- **Structure**:
  ```
  s3://bucket/iceberg/dc/
    data/
      type=node/
      type=way/
      type=relation/
    metadata/
      *.json
      snap-*.avro
  ```

## Understanding Iceberg Table Structure

### Data Files
```
data/
  type=node/
    00000-0-abc123.parquet    # Parquet files with actual data
    00001-1-def456.parquet
  type=way/
    00000-0-ghi789.parquet
  type=relation/
    00000-0-jkl012.parquet
```

### Metadata Files
```
metadata/
  v1.metadata.json           # Table schema and properties
  v2.metadata.json           # Updated schema (after changes)
  snap-123456789.avro        # Snapshot 1
  snap-987654321.avro        # Snapshot 2 (after update)
  ...
```

## Querying Output

### Using DuckDB (Direct Parquet)

```bash
# Query Parquet files directly
duckdb -c "
SELECT type, COUNT(*) 
FROM 'tests/data/output/warehouse/test_db/test_dc_nodes/data/*/*.parquet' 
GROUP BY type;
"
```

### Using Spark SQL

```bash
# Start spark-sql with Iceberg
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=/path/to/warehouse

# Query table
SELECT * FROM hadoop_catalog.test_db.test_dc_nodes LIMIT 10;
```

### Using Python

```python
from kryptosm.spark import create_spark_session

spark = create_spark_session(
    catalog_type="hadoop",
    warehouse="/path/to/warehouse"
)

df = spark.sql("SELECT * FROM hadoop_catalog.test_db.test_dc_nodes")
df.show()
```

## Disk Space Requirements

### Input (DC Parquet)
- Nodes: 36 MB
- Ways: 12 MB  
- Relations: 840 KB
- **Total**: ~49 MB

### Output (Iceberg with geometries)
- Nodes: ~50 MB (with WKB geometries + bbox)
- Ways: ~20 MB (with geometries)
- Relations: ~2 MB (with geometries)
- **Total**: ~72 MB
- **With overhead**: ~80-100 MB (metadata, snapshots)

### Growth with Updates
Each OSC update creates new data files and snapshots:
- Daily update: ~1-5 MB (depending on changes)
- Monthly growth: ~30-150 MB
- Use `expire_snapshots` to clean old data

## Cleaning Up

### Remove test output
```bash
rm -rf tests/data/output/*
```

### Remove all temporary data
```bash
rm -rf /tmp/iceberg_*
rm -rf /tmp/test_*
```

### Expire old Iceberg snapshots
```sql
-- Remove snapshots older than 7 days
CALL hadoop_catalog.system.expire_snapshots('test_db.test_dc_nodes', TIMESTAMP '2024-01-01 00:00:00');
```

## Best Practices

1. **Use separate locations** for different environments:
   - Dev: `s3://bucket/dev/iceberg/`
   - Prod: `s3://bucket/prod/iceberg/`

2. **Partition by date** for time-series data (if needed):
   ```sql
   PARTITIONED BY (type, days(timestamp))
   ```

3. **Regular maintenance**:
   - Expire old snapshots weekly
   - Rewrite small files monthly
   - Monitor storage costs

4. **Backup strategy**:
   - Iceberg metadata is critical - back it up
   - Data files are immutable - easy to backup
   - Consider cross-region replication for prod
