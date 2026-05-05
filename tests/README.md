# Tests for kryptosm

## Important Note

**If you encounter "Operation not permitted" errors when running Spark tests**, this is likely due to network restrictions blocking localhost socket connections. See [TROUBLESHOOTING.md](../TROUBLESHOOTING.md) for solutions.

You can still run import tests without Spark:
```bash
uv run python tests/test_imports.py
```

## Running Tests

### Prerequisites

Make sure you have the required dependencies installed:

**Using uv (recommended):**
```bash
uv sync
uv pip install pytest
```

**Using pip:**
```bash
pip install -e .
pip install pytest
```

### Run Tests

**Using uv:**

```bash
# Run import tests only (no Spark needed)
uv run pytest tests/test_imports.py -v

# Run all tests (requires Spark)
uv run pytest tests/ -v

# Run only non-Spark tests
uv run pytest tests/ -v -m "not spark"

# Run only Spark tests
uv run pytest tests/ -v -m spark

# Or use make
make test              # Runs non-Spark tests
make test-all          # Runs all tests
```

**Using pip:**

```bash
# Run import tests only
pytest tests/test_imports.py -v

# Run all tests
pytest tests/ -v

# Run only non-Spark tests
pytest tests/ -v -m "not spark"
```

### Test Categories

**Non-Spark Tests** (always work):
- `tests/test_imports.py` - Verify all modules import correctly

**Spark Tests** (require Spark with network access):
- `tests/test_basic.py` - Basic Spark and Iceberg functionality
- `tests/test_parquet_integration.py` - Full Parquet to Iceberg workflow

## Test Structure

- `test_basic.py`: Basic unit tests using pytest
- `run_tests.py`: Standalone test runner for basic workflow
- `test_parquet_integration.py`: Parquet integration tests using pytest
- `test_parquet_runner.py`: Standalone Parquet test runner
- `data/`: Test data files
  - `dc.parquet/`: DC OSM extract in Parquet format
- `README.md`: This file

## Test Coverage

The tests cover:

### Basic Tests (`test_basic.py`, `run_tests.py`)

1. **Spark Session**: Creating Spark sessions with Sedona and Iceberg
2. **Table Operations**: Creating Iceberg tables and checking existence
3. **Geometry Building**: Building node geometries from lat/lon
4. **Iceberg Preparation**: Converting geometries to WKB and adding bounding boxes
5. **Full Workflow**: End-to-end test from data creation to Iceberg table

### Parquet Integration Tests (`test_parquet_integration.py`, `test_parquet_runner.py`)

1. **Parquet Reading**: Read Parquet files with OSM data
2. **Data Extraction**: Verify nodes, ways, and relations are extracted
3. **Geometry Building**: Build geometries from real OSM data
4. **Iceberg Write**: Write real OSM data to Iceberg table
5. **Query Testing**: Test spatial queries on real data
6. **Geometry Types**: Verify correct geometry types (Point, LineString, Polygon)

## Test Data

### District of Columbia Parquet

The tests use a real OSM Parquet dataset for Washington DC:
- **Location**: `data/dc.parquet/`
- **Source**: OSM extract converted to Parquet format
- **Contents**: Real OSM data for Washington DC area

This dataset contains:
- ~1.9M nodes (points of interest, addresses, etc.)
- ~280K ways (roads, buildings, boundaries)
- ~3K relations (multipolygons, boundaries)

## Manual Testing

### Test with Sample Parquet Data

1. Run initial load:

```bash
kryptosm --mode init \
  --input-path tests/data/dc.parquet \
  --table-name test.osm_dc \
  --table-location /tmp/iceberg/dc \
  --catalog-type hadoop \
  --catalog-warehouse /tmp/iceberg/warehouse
```

2. Query the table:

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=/tmp/iceberg/warehouse \
  -e "SELECT type, COUNT(*) FROM test.osm_dc GROUP BY type"
```

## Integration Testing

For full integration testing with AWS Glue:

1. Configure AWS credentials
2. Create S3 buckets for input and output
3. Run tests with Glue catalog

Example:

```bash
export AWS_PROFILE=your-profile
export AWS_REGION=us-west-2

kryptosm --mode init \
  --input-path s3://your-bucket/osm/parquet/ \
  --table-name glue_catalog.test.osm \
  --table-location s3://your-bucket/iceberg/osm/ \
  --catalog-type glue
```

## Troubleshooting

### Sedona Not Found

If you get errors about Sedona not being available, make sure you have the Sedona JARs. They should be included with the pip installation of apache-sedona.

### Iceberg Errors

Make sure you have the Iceberg Spark runtime JAR. This should be automatically downloaded by Spark when using the Iceberg Maven coordinates.

### Memory Issues

If tests fail with out-of-memory errors, increase Spark memory:

```bash
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```
