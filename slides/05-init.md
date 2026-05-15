## Initial Load

Build all three tables from a raw OSM Parquet extract.

```python
from kryptosm import *

create_nodes_table(spark, NODES, config=TableConfig.nodes_production())
create_ways_table(spark, WAYS, config=TableConfig.ways_production())
create_relations_table(spark, RELATIONS, config=TableConfig.relations_production())
create_index_tables(spark, N2W, W2R, node_to_relations=N2R, relation_to_relations=R2R)
create_osc_archive_table(spark, ARCHIVE)

# Stage 1 — Nodes
spark.read.parquet("s3://bucket/osm.parquet/type=node").createOrReplaceTempView("input_nodes")
build_node_geometry(spark, "input_nodes", "nodes_with_geom")
prepare_for_iceberg(spark, "nodes_with_geom", "node", "nodes_final")
spark.sql("SELECT * FROM nodes_final").writeTo(NODES).using("iceberg").append()
load_with_geom(spark, NODES, "nodes_with_geom")         # re-bind from Iceberg

# Stage 2 — Ways (join refs → materialized node geometries)
flatten_way_refs(spark, "input_ways_raw", "input_ways")
build_way_linestrings(spark, "input_ways", "nodes_with_geom", "ways_lines")
promote_closed_ways_to_areas(spark, "ways_lines", "ways_with_geom")
prepare_for_iceberg(spark, "ways_with_geom", "way", "ways_final")
spark.sql("SELECT * FROM ways_final").writeTo(WAYS).using("iceberg").append()
populate_node_to_ways(spark, WAYS, N2W)

# Stage 3 — Relations (join members → materialized way geometries)
relations_need_geometry(spark, "input_relations", "rels_need_geom")
construct_multipolygon(spark, "rels_need_geom", "ways_with_geom", "rels_geom")
relation_merge_geometry_data(spark, "input_relations", "rels_geom", "rels_with_geom")
prepare_for_iceberg(spark, "rels_with_geom", "relation", "rels_final")
spark.sql("SELECT * FROM rels_final").writeTo(RELATIONS).using("iceberg").append()
```

Note:
`load_with_geom` re-binds from the materialized Iceberg table so ways and relations read node/way geometries from storage rather than re-executing the upstream view chain. This is the boundary between pipeline stages.
