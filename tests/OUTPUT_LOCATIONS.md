# Output Locations

This document explains where test output and Iceberg tables are stored.

## Test Data Locations

### Input Data
- **Parquet data**: `tests/data/dc.parquet/`
  - `type=node/` - Node data (36 MB)
  - `type=way/` - Way data (12 MB)
  - `type=relation/` - Relation data (840 KB)

### Output Data

When running tests, output is stored in:

#### 1. Pytest Tests (`test_parquet_integration.py`)
- **Warehouse**: Temporary directory (auto-deleted after test)
- **Location**: `/tmp/iceberg_parquet_test_*/`
- **Tables**: Created in `hadoop_catalog.test_db.*`
- **Cleanup**: Automatic (temp directory deleted)

#### 2. Standalone Runner (`test_parquet_runner.py`)
- **Warehouse**: Temporary directory (auto-deleted after test)
- **Location**: `/tmp/iceberg_parquet_test_*/`
- **Tables**: Created in `hadoop_catalog.test_db.test_parquet_dc`
- **Cleanup**: Automatic (temp directory deleted)

#### 3. Simple Test (`test_parquet_simple.py`)
- **Warehouse**: `tests/data/output/warehouse/`
- **Tables**: `tests/data/output/iceberg_table/`
- **Cleanup**: Manual (files persist for inspection)

## Running Tests with Persistent Output

To keep output for inspection, use the simple test:

```bash
cd /Users/jenningsa/Overture/tf-data-platform/kryptosm
uv run python tests/test_parquet_simple.py
```

Output will be in:
```
tests/data/output/
├── warehouse/           # Iceberg warehouse
│   └── test_db/
│       └── test_dc_nodes/
│           ├── data/
│           └── metadata/
└── iceberg_table/       # Table location (if specified)
```

## Querying Output Tables

After running tests, you can query the Iceberg tables using spark-sql:

```bash
# For simple test output
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=/path/to/warehouse \
  -e "SELECT * FROM hadoop_catalog.test_db.test_dc_nodes LIMIT 10;"
```

## Manual Testing with Output

To run a test and keep the output:

```python
from kryptosm.spark import create_spark_session_for_testing
from kryptosm.iceberg import create_iceberg_table
from kryptosm.geometry import build_node_geometry, prepare_for_iceberg

# Create session with persistent warehouse
warehouse = "/tmp/my_iceberg_test"
spark = create_spark_session_for_testing(warehouse)

# Create table
table_name = "hadoop_catalog.test.my_table"
table_location = "/tmp/my_iceberg_test/table"
create_iceberg_table(spark, table_name, table_location)

# ... load data, build geometries, write to table ...

# Query results
spark.sql(f"SELECT * FROM {table_name} LIMIT 10").show()

# Cleanup when done
spark.stop()
# rm -rf /tmp/my_iceberg_test
```

## Cleaning Up

### Automatic Cleanup (Tests)
Pytest tests automatically clean up temporary directories.

### Manual Cleanup
```bash
# Remove test output
rm -rf /Users/jenningsa/Overture/tf-data-platform/kryptosm/tests/data/output/

# Remove all temporary warehouses
rm -rf /tmp/iceberg_*
rm -rf /tmp/test_*
```

## Disk Space

The DC Parquet data is ~48 MB. When converted to Iceberg with geometries:
- Nodes: ~36 MB → ~50 MB (with WKB geometries and bbox)
- Ways: ~12 MB → ~20 MB (with geometries)
- Total: ~70 MB for full dataset

Tests limit to 1000 features, so output is much smaller (~5-10 MB).

## Using the CLI with Output

When using the CLI, specify output locations:

```bash
# Initial load
uv run kryptosm --mode init \
  --input-path tests/data/dc.parquet \
  --table-name hadoop_catalog.osm.dc \
  --table-location /tmp/iceberg/dc_osm \
  --catalog-type hadoop \
  --catalog-warehouse /tmp/iceberg/warehouse

# Output will be at:
#   /tmp/iceberg/dc_osm/           (table data)
#   /tmp/iceberg/warehouse/        (warehouse metadata)
```

## Inspecting Output

### List files
```bash
ls -lh /tmp/iceberg/warehouse/test_db/
```

### Check Parquet files
```bash
# Install parquet-tools if needed
pip install parquet-tools

# Inspect Parquet file
parquet-tools inspect /tmp/iceberg/warehouse/test_db/test_dc_nodes/data/...
parquet-tools show /tmp/iceberg/warehouse/test_db/test_dc_nodes/data/... --limit 5
```

### Query with DuckDB
```bash
# Install duckdb if needed
pip install duckdb

# Query Parquet files directly
duckdb -c "SELECT type, COUNT(*) FROM '/tmp/iceberg/warehouse/test_db/test_dc_nodes/data/*/*.parquet' GROUP BY type;"
```
