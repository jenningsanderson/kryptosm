"""
kryptosm — turn OpenStreetMap data into a single Apache Iceberg table.
"""

from .iceberg import (
    TableConfig,
    create_iceberg_table,
    create_index_tables,
    get_table_count,
    load_with_geom,
    populate_node_to_ways,
    populate_way_to_relations,
    table_exists,
)
from .osc import apply_osc, next_osc_path, read_osc_from_file
from .inspect import inspect_snapshots, list_snapshots
from .geometry.iceberg_prep import prepare_for_iceberg
from .geometry.nodes import build_node_geometry
from .geometry.ways import (
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
    flatten_way_refs,
)
from .geometry.relations import (
    construct_multipolygon,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from .geometry.samples import sample_geojson
