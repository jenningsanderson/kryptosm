## Initial Load

Build the table from a raw OSM Parquet extract.

```python
from kryptosm import *

TABLE = "hadoop_catalog.dc.osm"
N2W   = "hadoop_catalog.dc.node_to_ways"
W2R   = "hadoop_catalog.dc.way_to_relations"

create_iceberg_table(spark, TABLE)
create_index_tables(spark, N2W, W2R)

# Stage 1 — Nodes
spark.read.parquet("s3://bucket/dc.parquet/type=node") \
     .createOrReplaceTempView("input_nodes")
build_node_geometry(spark, "input_nodes", "nodes_with_geom")
prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
spark.sql("SELECT * FROM nodes_final").writeTo(TABLE).using("iceberg").append()

# Stage 2 — Ways  (join refs → node geometries)
build_linestring_for_ways(spark, "input_ways", "nodes_with_geom", "ways_lines")
build_ways_geometry_from_linestring(spark, "ways_lines", "ways_with_geom")
# ... writeTo, populate_node_to_ways

# Stage 3 — Relations  (join members → way geometries)
construct_multipolygon(spark, "rels_need_geom", "ways_with_geom", "rels_geom")
# ... writeTo, populate_way_to_relations
```

Note:
Between stages the pipeline re-binds from Iceberg so each phase reads materialized data rather than re-executing upstream views.
