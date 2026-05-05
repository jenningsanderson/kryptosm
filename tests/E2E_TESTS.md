# E2E Tests

The E2E tests build OSM data in 3 independent stages that can be run separately.

## Overview

### Stage 1: Nodes (`test_e2e_nodes.py`)
- **Input**: `tests/data/dc.parquet/type=node/` (1.9M nodes)
- **Output**: Iceberg table with node geometries
- **Table**: `hadoop_catalog.test_db.e2e_nodes`
- **Location**: `tests/data/output/warehouse/`

### Stage 2: Ways (`test_e2e_ways.py`)
- **Input**: 
  - Nodes from Stage 1 (Iceberg table)
  - Ways from `tests/data/dc.parquet/type=way/` (280K ways)
- **Output**: Adds way geometries to Iceberg table
- **Table**: `hadoop_catalog.test_db.e2e_nodes` (same table)
- **Location**: `tests/data/output/warehouse/`

### Stage 3: Relations (`test_e2e_relations.py`)
- **Input**:
  - Nodes and ways from Stages 1-2 (Iceberg table)
  - Relations from `tests/data/dc.parquet/type=relation/` (3K relations)
- **Output**: Adds relation geometries to Iceberg table
- **Table**: `hadoop_catalog.test_db.e2e_nodes` (same table)
- **Location**: `tests/data/output/warehouse/`

## Running the Tests

### Run All Stages

```bash
cd /Users/jenningsa/Overture/tf-data-platform/kryptosm

# Run all stages in order
make test-e2e-all

# Or individually
make test-e2e-nodes
make test-e2e-ways
make test-e2e-relations
```

### Run Individual Stages

Each stage can be run independently as long as previous stages have been completed:

```bash
# Stage 1: Build nodes (required first)
uv run python tests/test_e2e_nodes.py

# Stage 2: Build ways (requires Stage 1)
uv run python tests/test_e2e_ways.py

# Stage 3: Build relations (requires Stages 1 and 2)
uv run python tests/test_e2e_relations.py
```

### Re-running Stages

You can re-run any stage:

- **Stage 1**: Drops and recreates the table, rebuilds all nodes
- **Stage 2**: Adds ways to existing table (reads nodes from table)
- **Stage 3**: Adds relations to existing table (reads nodes and ways from table)

## Output Locations

After running tests, output is in:

```
tests/data/output/
└── warehouse/
    └── test_db/
        └── e2e_nodes/
            ├── data/
            │   ├── type=node/
            │   ├── type=way/
            │   └── type=relation/
            └── metadata/
                ├── v1.metadata.json
                ├── v2.metadata.json
                ├── v3.metadata.json
                └── snap-*.avro
```

## Querying Results

### Using DuckDB

```bash
# Count by type
duckdb -c "
SELECT type, COUNT(*) 
FROM 'tests/data/output/warehouse/test_db/e2e_nodes/data/*/*.parquet' 
GROUP BY type;
"

# Sample nodes
duckdb -c "
SELECT id, tags['name'] as name, bbox
FROM 'tests/data/output/warehouse/test_db/e2e_nodes/data/*/*.parquet'
WHERE type = 'node' AND tags['name'] IS NOT NULL
LIMIT 10;
"

# Sample ways
duckdb -c "
SELECT id, tags['highway'] as highway, ST_GeometryType(ST_GeomFromWKB(geometry)) as geom_type
FROM 'tests/data/output/warehouse/test_db/e2e_nodes/data/*/*.parquet'
WHERE type = 'way' AND geometry IS NOT NULL
LIMIT 10;
"

# Sample relations
duckdb -c "
SELECT id, tags['type'] as rel_type, tags['name'] as name
FROM 'tests/data/output/warehouse/test_db/e2e_nodes/data/*/*.parquet'
WHERE type = 'relation'
LIMIT 10;
"
```

### Using Spark SQL

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=/path/to/tests/data/output/warehouse

# Then query
SELECT type, COUNT(*) FROM hadoop_catalog.test_db.e2e_nodes GROUP BY type;
```

## Expected Results

For Washington DC data:

| Type | Count | Geometry |
|------|-------|----------|
| node | ~1.9M | Point |
| way | ~280K | LineString or Polygon |
| relation | ~1.7K | MultiPolygon (boundary/multipolygon only) |

## Cleaning Up

```bash
# Remove test output
rm -rf tests/data/output/warehouse/
rm -rf /tmp/iceberg_*
```

## Troubleshooting

### Stage 2 fails: "Table does not exist"
Run Stage 1 first:
```bash
uv run python tests/test_e2e_nodes.py
```

### Stage 3 fails: "No nodes or ways found"
Run Stages 1 and 2 first:
```bash
uv run python tests/test_e2e_nodes.py
uv run python tests/test_e2e_ways.py
```

### Out of memory
Reduce data size or increase Spark memory:
```bash
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```

## Benefits of Staged Tests

1. **Independent**: Each stage can run separately
2. **Incremental**: Build on previous results
3. **Debuggable**: Test each stage in isolation
4. **Flexible**: Re-run failed stages without starting over
5. **Realistic**: Mimics production incremental updates
