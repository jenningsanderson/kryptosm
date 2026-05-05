"""
SQL-based geometry building for OSM data using Apache Sedona.

Each submodule owns one stage of the pipeline. Import directly from the
submodule — there are deliberately no re-exports here, so a reader can
see at a glance which file owns each function.

    nodes        : Point geometries from lat/lon
    ways         : LineString / Polygon geometries from member nodes
    relations    : MultiPolygon / MultiLineString geometries from member ways
    osc_apply    : Apply an OSC change file on top of a base view
    iceberg_prep : Convert a `geom` view to the Iceberg WKB+bbox layout

All transformations are Spark SQL views chained via createOrReplaceTempView.
No Python UDFs, no driver-side actions.
"""
