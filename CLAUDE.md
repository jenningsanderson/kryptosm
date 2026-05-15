# CLAUDE.md - kryptosm

See AGENTS.md for full project architecture, data flow, and module reference.

## Testing

Do NOT run tests (`pytest`, `make test-*`, etc.) after making changes. The user
runs integration tests in a separate window and will report results back. The
test suite is E2E with Spark+Iceberg and takes too long for iterative feedback.

## Quick reference

- **Package:** `kryptosm/` — OSM data → Apache Iceberg tables via Spark + Sedona
- **All business logic is SQL** — no pandas, no Python UDFs
- **Pipeline pattern:** chain `createOrReplaceTempView` calls; Spark materializes only at write/MERGE
- **Dependencies:** pyspark 3.5.0, apache-sedona 1.9.0, osmium, pandas (transitive for Sedona)
- **Build/run:** `uv sync` to install, `uv run pytest` to test
