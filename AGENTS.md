# AGENTS.md - OSM Iceberg Sync

This file provides guidance for AI agents working on the kryptosm project.

## Project Overview

**kryptosm** is a standalone utility for processing OpenStreetMap (OSM) data into Apache Iceberg tables. It converts OSM Parquet files into a unified Iceberg table with geometries built using Apache Sedona.

### Key Features
- Initial load from OSM Parquet files (nodes, ways, relations)
- Incremental updates via OSC (OpenStreetMap Change) files
- Geometry building with Apache Sedona
- Single Iceberg table partitioned by type
- Support for Glue and Hadoop catalogs

## Repository Structure

```
kryptosm/
├── pyproject.toml              # UV project configuration
├── Makefile                    # Task automation
├── README.md                   # User documentation
├── UV_INSTALL.md              # UV setup guide
├── TROUBLESHOOTING.md         # Common issues
├── STATUS.md                  # Project status
├── OUTPUT.md                  # Output locations
├── AGENTS.md                  # This file
│
├── kryptosm/          # Main package
│   ├── __init__.py
│   ├── cli.py                 # Command-line interface
│   ├── main.py                # Orchestration logic
│   ├── spark.py               # Spark session management
│   ├── iceberg.py             # Iceberg table operations
│   ├── geometry.py            # Geometry building functions
│   └── osc.py                 # OSC file handling
│
└── tests/                     # Test suite
    ├── test_imports.py        # Import tests (no Spark needed)
    ├── test_basic.py          # Basic Spark tests
    ├── test_parquet_integration.py  # Parquet integration tests
    ├── test_parquet_runner.py       # Standalone Parquet test
    ├── test_e2e_nodes.py      # E2E Stage 1: Nodes
    ├── test_e2e_ways.py       # E2E Stage 2: Ways
    ├── test_e2e_relations.py  # E2E Stage 3: Relations
    ├── E2E_TESTS.md           # E2E test documentation
    └── data/
        ├── dc.parquet/        # Test data (DC OSM extract)
        └── output/            # Test output (gitignored)
```

## Architecture

### Data Flow

```
Input Parquet Files
    ├── type=node/      → build_node_geometry() → nodes with Point geometries
    ├── type=way/       → build_linestring_for_ways() → ways with LineString/Polygon
    └── type=relation/  → construct_multipolygon() → relations with MultiPolygon
                              ↓
                    prepare_for_iceberg()
                              ↓
                    Iceberg Table (partitioned by type)
```

### Module Responsibilities

#### `cli.py` - Command-Line Interface
- Argument parsing and validation
- No business logic
- Delegates to `main.py`

**Key functions:**
- `create_parser()` - Creates argument parser
- `validate_args()` - Validates arguments
- `parse_args()` - Parse and validate

#### `main.py` - Orchestration
- High-level workflow coordination
- `run_init_mode()` - Initial load from Parquet
- `run_update_mode()` - Incremental updates from OSC
- `process_nodes_update()` - Node update logic
- `process_ways_update()` - Way update logic
- `process_relations_update()` - Relation update logic

#### `spark.py` - Spark Session Management
- Creates Spark sessions with Sedona and Iceberg
- Handles JAR downloads and configuration
- Supports both Glue and Hadoop catalogs

**Key functions:**
- `create_spark_session()` - Main session factory
- `create_spark_session_for_testing()` - Test session factory
- `get_sedona_jars()` - Downloads Sedona JARs if needed

#### `iceberg.py` - Iceberg Operations
- Table creation and management
- MERGE operations for upserts/deletes
- Table maintenance (optimize, expire snapshots)

**Key functions:**
- `create_iceberg_table()` - Creates OSM table with schema
- `table_exists()` - Checks if table exists
- `get_table_count()` - Gets feature counts by type
- `merge_into_table()` - MERGE for upserts
- `delete_from_table()` - MERGE for deletes

#### `geometry.py` - Geometry Building
- Core geometry construction logic using Sedona
- Ported from `omf.utilities.osm_geometry`

**Node functions:**
- `build_node_geometry()` - Creates Points from lat/lon

**Way functions:**
- `build_linestring_for_ways()` - Builds linestrings from nodes
- `build_ways_geometry_from_linestring()` - Converts closed ways to polygons
- `fix_invalid_geometries()` - Fixes invalid geometries

**Relation functions:**
- `relations_need_geometry()` - Filters relations needing geometry
- `construct_multipolygon()` - Main entry point for relation geometries
- `merge_ways_into_multipolygon()` - Merges member ways
- `aggregate_polygons_for_relation()` - Aggregates with ST_SymDifference
- `relation_merge_geometry_data()` - Merges relation data with geometries

**Helper functions:**
- `_get_all_cycles()` - Extracts cycles from linestrings
- `_merge_lines()` - UDF for merging lines into polygons

**OSC functions:**
- `all_dirty_ways()` - Identifies ways needing rebuild
- `all_dirty_relations()` - Identifies relations needing rebuild
- `apply_osc_with_geometry()` - Applies OSC changes

**Output functions:**
- `prepare_for_iceberg()` - Converts to WKB, adds bbox and type

#### `osc.py` - OSC File Handling
- Downloads and parses OSC files
- Converts to Spark DataFrames

**Key functions:**
- `osc_dedup()` - Deduplicates OSC records
- `OSCData` class - Downloads and parses OSC XML
- `get_osc_day_sequence_number()` - Calculates sequence from date
- `download_osc_to_dataframe()` - Downloads and converts to DataFrame
- `read_osc_from_parquet()` - Reads OSC from Parquet
- `read_osc_from_file()` - Reads OSC from .osc.gz file

## Iceberg Table Schema

```sql
CREATE TABLE table_name (
    id BIGINT,
    type STRING,                    -- 'node', 'way', 'relation'
    version BIGINT,
    timestamp TIMESTAMP,
    changeset BIGINT,
    uid BIGINT,
    user STRING,
    tags MAP<STRING, STRING>,
    lat DOUBLE,                     -- nodes only
    lon DOUBLE,                     -- nodes only
    refs ARRAY<BIGINT>,             -- ways only
    members ARRAY<STRUCT<           -- relations only
        type: STRING,
        ref: BIGINT,
        role: STRING
    >>,
    latest_ts TIMESTAMP,
    geometry BINARY,                -- WKB format
    bbox STRUCT<                    -- Bounding box
        xmin: FLOAT,
        xmax: FLOAT,
        ymin: FLOAT,
        ymax: FLOAT
    >
) USING iceberg
PARTITIONED BY (type)
```

## Testing

### Test Categories

1. **Import Tests** (`test_imports.py`)
   - No Spark required
   - Verifies all modules import correctly
   - Fast (seconds)

2. **Basic Tests** (`test_basic.py`)
   - Requires Spark
   - Tests table creation and basic operations
   - Uses synthetic data

3. **Parquet Integration Tests** (`test_parquet_integration.py`)
   - Requires Spark
   - Tests with real DC Parquet data
   - Tests geometry building

4. **E2E Tests** (3 stages)
   - `test_e2e_nodes.py` - Stage 1: Build nodes
   - `test_e2e_ways.py` - Stage 2: Build ways (depends on Stage 1)
   - `test_e2e_relations.py` - Stage 3: Build relations (depends on Stages 1-2)
   - Each stage can run independently
   - Output persists in `tests/data/output/`

### Running Tests

```bash
# Import tests only (always works)
uv run pytest tests/test_imports.py -v

# Spark tests (requires working Spark)
uv run pytest tests/test_basic.py -v -m spark

# E2E tests (run in order)
make test-e2e-nodes
make test-e2e-ways
make test-e2e-relations

# Or all at once
make test-e2e-all
```

## Development Guidelines

### Code Style
- Follow existing patterns in the codebase
- Use Black for formatting (line length: 100)
- Use Ruff for linting
- Add docstrings for public functions
- Keep functions focused and modular

### Adding New Features

1. **New geometry operations**: Add to `geometry.py`
2. **New Iceberg operations**: Add to `iceberg.py`
3. **New CLI options**: Add to `cli.py`, handle in `main.py`
4. **New data sources**: Create new module for additional input formats

### Testing New Features

1. Add unit tests to `test_basic.py` or create new test file
2. Mark Spark tests with `@pytest.mark.spark`
3. Update `tests/README.md` if needed
4. Ensure import tests still pass

### Documentation

- Update `README.md` for user-facing changes
- Update `AGENTS.md` (this file) for architectural changes
- Add docstrings for new functions
- Update `STATUS.md` for project status changes

## Common Tasks

### Adding a New Geometry Function

1. Add function to `geometry.py`:
```python
def my_new_geometry_function(spark: SparkSession, input_view: str, result_view: str):
    """
    Brief description.
    
    Args:
        spark: Spark session
        input_view: Input view name
        result_view: Output view name
    """
    spark.sql(f"""
        SELECT ..., ST_SomeFunction(...) as geom
        FROM {input_view}
    """).createOrReplaceTempView(result_view)
```

2. Add test in `test_basic.py` or `test_parquet_integration.py`

3. Update this AGENTS.md file

### Modifying the Iceberg Schema

1. Update `create_iceberg_table()` in `iceberg.py`
2. Update `prepare_for_iceberg()` in `geometry.py` if needed
3. Update schema documentation in README.md and this file
4. Test with `test_basic.py::test_table_creation`

### Adding CLI Options

1. Add argument in `cli.py` `create_parser()`
2. Add validation in `cli.py` `validate_args()`
3. Handle in `main.py` `run_init_mode()` or `run_update_mode()`
4. Update README.md examples

## Dependencies

### Core Dependencies
- `pyspark==3.5.0` - Spark framework
- `apache-sedona==1.8.1` - Geospatial processing
- `boto3>=1.35.47` - AWS integration (Glue, S3)
- `requests>=2.28.0` - HTTP requests (OSC downloads)

### Optional Dependencies
- `pytest>=7.0.0` - Testing
- `black>=23.0.0` - Code formatting
- `ruff>=0.1.0` - Linting
- `mypy>=1.0.0` - Type checking

### JAR Dependencies (auto-downloaded)
- `sedona-spark-shaded-3.5_2.12-1.8.1.jar` (80 MB)
- `iceberg-spark-runtime-3.5_2.12-1.6.1.jar` (40 MB)
- `iceberg-aws-bundle-1.6.1.jar` (30 MB)

JARs are cached at `~/.cache/kryptosm/jars/`

## Performance Considerations

### Geometry Building
- Nodes: Fast (simple point creation)
- Ways: Medium (requires node join)
- Relations: Slow (requires way join and polygon operations)

### Optimization Tips
- Partition input data by geographic region
- Increase `partition_number` for large datasets
- Use `ST_ReducePrecision` to reduce coordinate precision
- Simplify large relation geometries

### Incremental Updates
- Only dirty features are rebuilt
- Nodes: Only new/modified nodes
- Ways: New/modified ways + ways with dirty nodes
- Relations: New/modified relations + relations with dirty ways

## Troubleshooting

See `TROUBLESHOOTING.md` for common issues.

### Quick Diagnostics

```bash
# Check if package imports work
uv run python tests/test_imports.py

# Check if JARs are downloaded
ls -lh ~/.cache/kryptosm/jars/

# Check Parquet data
ls -lh tests/data/dc.parquet/

# Verify installation
uv run python quickstart.py
```

## Related Projects

- **omf** - Overture Maps Foundation utilities (source of original code)
- **Apache Sedona** - Geospatial processing
- **Apache Iceberg** - Table format
- **OpenStreetMap** - Data source

## License

Apache License 2.0 - Same as parent tf-data-platform repository.
