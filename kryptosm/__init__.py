"""
kryptosm — turn OpenStreetMap data into the Krypton Iceberg database.

Krypton is a per-type-table Iceberg layout: one table each for nodes, ways,
and relations, plus three reverse-index tables and an OSC archive table.
"""

from .geometry.iceberg_prep import prepare_for_iceberg
from .geometry.nodes import build_node_geometry
from .geometry.relations import (
    construct_multipolygon,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from .geometry.samples import sample_geojson
from .geometry.ways import (
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    flatten_way_refs,
)
from .iceberg import (
    TableConfig,
    append_osc_archive,
    create_index_tables,
    create_nodes_table,
    create_osc_archive_table,
    create_relations_table,
    create_ways_table,
    get_table_count,
    load_with_geom,
    populate_node_to_relations,
    populate_node_to_ways,
    populate_relation_to_relations,
    populate_way_to_relations,
    refresh_node_to_relations,
    refresh_relation_to_relations,
    table_exists,
)
from .inspect import inspect_snapshots, list_snapshots
from .osc import apply_osc, next_osc_path, read_osc_from_file
