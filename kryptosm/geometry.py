"""
Geometry building functions for OSM data using Apache Sedona.
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

# Constants
MAXIMUM_RELATION_GEOMETRY_SIZE = 30000000
HUGE_GEOMETRY_SIMPLIFICATION_FACTOR = 0.000001


def next_float_below(d):
    """Get the next float below the given value."""
    return F.expr("ST_XMin(geom)").cast("float")


def next_float_above(d):
    """Get the next float above the given value."""
    return F.expr("ST_XMax(geom)").cast("float")


# ============================================================================
# Node Geometry Functions
# ============================================================================


def build_node_geometry(spark: SparkSession, nodes_data: str, result_view: str):
    """
    Builds geometries for all nodes by creating a point from lat/lon.

    Args:
        spark: Spark session
        nodes_data: Name of the view containing node data
        result_view: Name of the view to create with geometries
    """
    spark.sql(f"""
        SELECT id, 
               CAST(version AS BIGINT) as version, 
               CAST(timestamp AS TIMESTAMP) as timestamp, 
               CAST(uid AS BIGINT) as uid, 
               user, 
               CAST(changeset AS BIGINT) as changeset, 
               tags, 
               lat, 
               lon, 
               CAST(NULL AS ARRAY<BIGINT>) as refs, 
               members, 
               CAST(COALESCE(timestamp, current_timestamp()) AS TIMESTAMP) as latest_ts,
               ST_ReducePrecision(ST_Point(lon, lat), 7) AS geom
        FROM {nodes_data}
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        """).createOrReplaceTempView(result_view)


# ============================================================================
# Way Geometry Functions
# ============================================================================


def build_linestring_for_ways(
    spark: SparkSession, ways_data: str, nodes_geometry: str, result_view: str
):
    """
    Builds linestrings for ways by collecting node geometries.

    Args:
        spark: Spark session
        ways_data: Name of the view containing way data
        nodes_geometry: Name of the view containing node geometries
        result_view: Name of the view to create with linestrings
    """
    spark.sql(f"""
        SELECT
            a.id, a.version, a.timestamp, a.uid, a.user, a.changeset, a.tags, 
            a.lat, a.lon, 
            transform(a.nds, x -> x.ref) as refs,
            a.members,
            GREATEST(a.timestamp, b.latest_ts) AS latest_ts,
            IF (
                b.node_geom IS NOT NULL AND cardinality(b.node_geom) > 1,
                ST_LineFromMultiPoint(ST_Collect(b.node_geom.geom)),
                NULL
            ) AS geom
        FROM {ways_data} a
        LEFT OUTER JOIN (
            SELECT
                id,
                MAX(latest_ts) AS latest_ts,
                sort_array(collect_list(struct(node_pos, geom))) AS node_geom
            FROM (
                -- Remove adjacent duplicate nodes
                SELECT
                    a.id,
                    a.node_pos,
                    b.latest_ts,
                    b.geom,
                    LAG(b.geom, 1) OVER (PARTITION BY a.id ORDER BY a.node_pos) AS prev_node_geom
                FROM (
                    SELECT
                        id,
                        posexplode(transform(nds, x -> x.ref)) AS (node_pos, node_id)
                    FROM {ways_data}
                ) a
                JOIN {nodes_geometry} b
                ON a.node_id = b.id
            )
            WHERE
                geom != prev_node_geom OR prev_node_geom IS NULL
            GROUP BY id
        ) b
        ON a.id = b.id
        """).createOrReplaceTempView(result_view)


def build_ways_geometry_from_linestring(
    spark: SparkSession, ways_linestrings: str, result_view: str
):
    """
    Builds polygons for ways with specific tags.

    Args:
        spark: Spark session
        ways_linestrings: Name of the view containing way linestrings
        result_view: Name of the view to create with final geometries
    """
    spark.sql(f"""
        SELECT
            id, version, timestamp, uid, user, changeset, tags, lat, lon, refs, members, latest_ts,
            IF (
                geom IS NOT NULL
                AND ST_NumPoints(geom) > 3
                AND ST_IsClosed(geom)
                AND (
                    tags['aeroway'] IS NOT NULL
                    OR tags['amenity'] IS NOT NULL
                    OR (tags['boundary'] IS NOT NULL AND tags['boundary'] <> 'place')
                    OR tags['building'] IS NOT NULL
                    OR tags['building:part'] IS NOT NULL
                    OR tags['golf'] IS NOT NULL
                    OR tags['harbour'] IS NOT NULL
                    OR tags['historic'] IS NOT NULL
                    OR tags['landuse'] IS NOT NULL
                    OR (tags['leisure'] IS NOT NULL AND tags['leisure'] <> 'track')
                    OR tags['man_made'] IS NOT NULL
                    OR tags['military'] IS NOT NULL
                    OR tags['natural'] IS NOT NULL
                    OR tags['office'] IS NOT NULL
                    OR tags['place'] IS NOT NULL
                    OR tags['power'] IS NOT NULL
                    OR tags['public_transport'] IS NOT NULL
                    OR tags['shop'] IS NOT NULL
                    OR tags['sport'] IS NOT NULL
                    OR tags['tourism'] IS NOT NULL
                    OR tags['water'] IS NOT NULL
                    OR tags['waterway'] IS NOT NULL
                    OR tags['wetland'] IS NOT NULL
                    OR tags['area'] IS NOT NULL
                    OR aggregate(transform(map_keys(tags), k -> position('area:', k) = 1), FALSE, (acc, x) -> acc OR x)
                )
                AND (
                    tags['area'] IS NULL
                    OR tags['area'] <> 'no'
                ),
                ST_ForcePolygonCCW(ST_MakePolygon(geom)),
                geom
            ) AS geom
        FROM {ways_linestrings}
        """).createOrReplaceTempView(result_view)


def fix_invalid_geometries(
    spark: SparkSession,
    ways_geometry: str,
    result_view: str,
):
    """
    Fix invalid geometries in ways.

    Args:
        spark: Spark session
        ways_geometry: Name of the view containing way geometries
        result_view: Name of the view to create with fixed geometries
    """
    spark.sql(f"""
        SELECT
            id, version, timestamp, uid, user, changeset, tags, lat, lon, refs, members, latest_ts,
            IF(
                ST_IsValid(geom),
                geom,
                ST_MakeValid(geom)
            ) AS geom
        FROM {ways_geometry}
        """).createOrReplaceTempView(result_view)


# ============================================================================
# Relation Geometry Functions
# ============================================================================


def relations_need_geometry(spark: SparkSession, relations_data: str, result_view: str):
    """
    Filters relations that need geometry calculations.

    Currently includes:
    - type='multipolygon' - Standard multipolygon relations
    Note: 'boundary' type is intentionally excluded for now

    Args:
        spark: Spark session
        relations_data: Name of the view containing relation data
        result_view: Name of the view to create with filtered relations
    """
    spark.sql(f"""
        SELECT id, members, 
               COALESCE(timestamp, current_timestamp()) as latest_ts
        FROM {relations_data}
        WHERE tags['type'] = 'multipolygon'
        """).createOrReplaceTempView(result_view)


def construct_multipolygon(
    spark: SparkSession, relations_data: str, ways_geometry: str, result_view: str
):
    """
    Construct multipolygon geometries for relations.

    This function builds multipolygon geometries from relation members.
    It processes relations that have type='multipolygon' or type='boundary'.

    Args:
        spark: Spark session
        relations_data: Name of the view containing relations (must have id, members, latest_ts)
        ways_geometry: Name of the view containing way geometries (must have id, geom, latest_ts)
        result_view: Name of the view to create with result (id, latest_ts, geom)
    """
    # Step 1: Merge ways into polygons
    merge_ways_into_multipolygon(spark, relations_data, ways_geometry, "temp_member_polygons")

    # Step 2: Aggregate polygons for each relation
    aggregate_polygons_for_relation(spark, "temp_member_polygons", result_view)

    # Clean up temp view
    try:
        spark.sql("DROP VIEW IF EXISTS temp_member_polygons")
    except:
        pass


def _get_all_cycles(line):
    """Extract all cycles from a linestring."""
    results = []
    while True:
        unique_elements = set(line)
        if len(line) - len(unique_elements) == 1 and len(line) > 3 and line[0] == line[-1]:
            # If all elements form a cycle, add the line as it is and then break
            results.append(line)
            break
        else:
            visited = {}  # Key: node, Value: index of first occurrence
            for i, node in enumerate(line):
                if node in visited:
                    # Found a cycle, update results and source accordingly
                    start_index = visited[node]
                    if i - start_index > 2:  # needs at lease 4 nodes to form a cycle
                        results.append(line[start_index : i + 1])
                        # Remove the found cycle, then handle consecutive cycles
                        line[start_index:i] = []
                        break
                else:
                    visited[node] = i
            else:
                # If no cycle is found, exit the loop
                break
    return results


def _build_rings_from_refs(ways):
    """
    UDF that builds rings from ways using node IDs (refs).

    This function takes a list of ways with their refs and returns
    the indices of ways that form each ring. This allows the SQL layer
    to handle the actual geometry operations using Sedona functions.

    Args:
        ways: List of structs with 'refs' (list of node IDs) and 'id' (way ID)

    Returns:
        List of lists, where each inner list contains way IDs that form a ring
    """
    if ways is None or len(ways) == 0:
        return []

    # Build a graph of node connections
    # Map: node_id -> list of (way_idx, position_in_way, is_start)
    node_to_ways = {}
    way_refs = []
    way_ids = []

    for way_idx, way in enumerate(ways):
        refs = way["refs"]
        way_id = way["id"]
        if refs is None or len(refs) < 2:
            continue

        way_refs.append(refs)
        way_ids.append(way_id)

        # Add both start and end nodes to the graph
        start_node = refs[0]
        end_node = refs[-1]

        if start_node not in node_to_ways:
            node_to_ways[start_node] = []
        node_to_ways[start_node].append((way_idx, 0, True))

        if end_node not in node_to_ways:
            node_to_ways[end_node] = []
        node_to_ways[end_node].append((way_idx, len(refs) - 1, False))

    # Find rings by traversing the graph
    used_ways = set()
    rings = []

    for way_idx in range(len(way_refs)):
        if way_idx in used_ways:
            continue

        # Start a new ring
        refs = way_refs[way_idx]
        if len(refs) < 2:
            continue

        # Check if this way is already a closed ring
        if refs[0] == refs[-1] and len(refs) >= 4:
            # Closed way - single way forms a ring
            rings.append([way_ids[way_idx]])
            used_ways.add(way_idx)
            continue

        # Try to build a ring by following connections
        ring_way_ids = [way_ids[way_idx]]
        ring_refs = list(refs)
        used_ways.add(way_idx)
        current_end = refs[-1]
        start_node = refs[0]

        # Follow connections until we return to start or can't continue
        max_iterations = len(way_refs) * 2
        iterations = 0

        while current_end != start_node and iterations < max_iterations:
            iterations += 1

            # Find ways connected to current_end
            if current_end not in node_to_ways:
                break

            next_way = None
            next_refs = None
            reverse = False

            for w_idx, pos, is_start in node_to_ways[current_end]:
                if w_idx in used_ways:
                    continue

                w_refs = way_refs[w_idx]
                if is_start:
                    # This way starts at current_end
                    next_way = w_idx
                    next_refs = w_refs
                    reverse = False
                    break
                else:
                    # This way ends at current_end, need to reverse it
                    next_way = w_idx
                    next_refs = w_refs
                    reverse = True
                    break

            if next_way is None:
                break

            # Add the way to the ring
            used_ways.add(next_way)
            ring_way_ids.append(way_ids[next_way])

            if reverse:
                # Add refs in reverse order (excluding first node which is current_end)
                ring_refs.extend(reversed(next_refs[:-1]))
                current_end = next_refs[0]
            else:
                # Add refs in forward order (excluding first node which is current_end)
                ring_refs.extend(next_refs[1:])
                current_end = next_refs[-1]

        # If we made a closed ring, save it
        if current_end == start_node and len(ring_refs) >= 4:
            rings.append(ring_way_ids)

    return rings


def _merge_lines(wkt_list):
    """
    UDF that merges line strings into polygon rings.

    It takes pipe-separated WKT strings as input and returns polygon strings.
    It parses each WKT, extracts coordinates, merges connected lines,
    and converts closed loops into polygons.
    """
    if wkt_list is None:
        return []

    # Parse WKT strings - handle both LINESTRING and POLYGON inputs
    all_lines = []
    for wkt in wkt_list.split("|"):
        wkt = wkt.strip()
        if not wkt:
            continue

        # Extract coordinates from WKT
        if wkt.startswith("LINESTRING"):
            # LINESTRING (x1 y1, x2 y2, ...)
            coords_str = wkt[12:-1]  # Remove 'LINESTRING (' and ')'
            coords = coords_str.split(",")
            all_lines.append([c.strip() for c in coords])
        elif wkt.startswith("POLYGON"):
            # POLYGON ((x1 y1, x2 y2, ...)) - extract exterior ring
            # Find the first ring
            start = wkt.find("((") + 2
            end = wkt.find("))", start)
            if start > 1 and end > start:
                coords_str = wkt[start:end]
                coords = coords_str.split(",")
                all_lines.append([c.strip() for c in coords])
        elif wkt.startswith("MULTILINESTRING"):
            # Handle legacy MULTILINESTRING format
            if wkt[:15] == "MULTILINESTRING":
                wkt = wkt[18:-2]
                for line in wkt.split("), ("):
                    all_lines.append(line.split(", "))

    # Closed lines do not need to be merged with other lines
    merged_lines = [line for line in all_lines if len(line) > 3 and line[0] == line[-1]]
    lines = [line for line in all_lines if line[0] != line[-1]]

    # Merge open lines
    while lines:
        current_line = lines.pop()
        while True:
            for target in lines:
                # Try to find another line that can be connected to the current line
                if current_line[-1] == target[0]:
                    current_line.extend(target[1:])
                    lines.remove(target)
                    break

                if current_line[-1] == target[-1]:
                    current_line.extend(target[::-1][1:])
                    lines.remove(target)
                    break
            else:
                # No other lines can be connected to current line
                break

        # If the current line has become a closed line, add it to the merged_lines list
        if len(current_line) > 3 and current_line[0] == current_line[-1]:
            merged_lines.append(current_line)
        else:
            return []  # do not create geometry if lines cannot construct rings

    # A closed line may contain multiple smaller cycles, we need to seperate them
    if merged_lines:
        result_lines = []
        for line in merged_lines:
            if len(line) - len(set(line)) == 1:
                result_lines.append(line)
            else:
                # If the line has self-intersections, get all cycles (polygons) from the line
                cycles = _get_all_cycles(line)
                result_lines.extend(cycles)

        return [f"POLYGON(({','.join(line)}))" for line in result_lines]
    else:
        return []  # no valid rings found


def merge_ways_into_multipolygon(
    spark: SparkSession, relations_data: str, ways_geometry: str, result_view: str
):
    """
    Builds multiple polygons for all ways in a relation.

    This implementation uses ST_Union to combine way geometries,
    then ST_BuildArea to construct polygons from the linework.
    This avoids WKT serialization and uses Sedona geometry types throughout.

    Args:
        spark: Spark session
        relations_data: Name of the view containing relation data
        ways_geometry: Name of the view containing way geometries
        result_view: Name of the view to create with polygons
    """
    # Build polygons from outer ways using Sedona geometry operations
    spark.sql(f"""
        SELECT
            id,
            MAX(latest_ts) AS latest_ts,
            ST_BuildArea(ST_Union_Aggr(geom)) AS geom
        FROM (
            SELECT
                mapping.id,
                lines.latest_ts,
                lines.geom
            FROM (
                SELECT DISTINCT
                    id,
                    member.ref AS way_id
                FROM (
                    SELECT
                        id,
                        posexplode(members) AS (pos, member)
                    FROM {relations_data}
                ) a
                WHERE member.type = 'way'
                  AND (member.role IS NULL OR member.role = '' OR member.role = 'outer')
            ) mapping
            JOIN {ways_geometry} lines
            ON mapping.way_id = lines.id
            WHERE lines.geom IS NOT NULL
        ) outer_ways
        GROUP BY id
        """).createOrReplaceTempView("temp_outer_polygons")

    # Build polygons from inner ways (holes)
    spark.sql(f"""
        SELECT
            id,
            ST_BuildArea(ST_Union_Aggr(geom)) AS geom
        FROM (
            SELECT
                mapping.id,
                lines.geom
            FROM (
                SELECT DISTINCT
                    id,
                    member.ref AS way_id
                FROM (
                    SELECT
                        id,
                        posexplode(members) AS (pos, member)
                    FROM {relations_data}
                ) a
                WHERE member.type = 'way'
                  AND member.role = 'inner'
            ) mapping
            JOIN {ways_geometry} lines
            ON mapping.way_id = lines.id
            WHERE lines.geom IS NOT NULL
        ) inner_ways
        GROUP BY id
        """).createOrReplaceTempView("temp_inner_polygons")

    # Combine outer and inner polygons
    # Use ST_Difference to subtract inner polygons (holes) from outer polygons
    spark.sql("""
        SELECT
            COALESCE(o.id, i.id) AS id,
            o.latest_ts,
            CASE 
                WHEN o.geom IS NOT NULL AND i.geom IS NOT NULL THEN
                    ST_Difference(o.geom, i.geom)
                WHEN o.geom IS NOT NULL THEN
                    o.geom
                ELSE
                    i.geom
            END AS geom
        FROM temp_outer_polygons o
        FULL OUTER JOIN temp_inner_polygons i
        ON o.id = i.id
        """).createOrReplaceTempView(result_view)

    # Clean up temp views
    try:
        spark.sql("DROP VIEW IF EXISTS temp_outer_polygons")
        spark.sql("DROP VIEW IF EXISTS temp_inner_polygons")
    except:
        pass


def aggregate_polygons_query():
    """Returns SQL query for aggregating polygons using ST_SymDifference."""
    return """
        aggregate(
            slice(collect_list(geom), 2, cardinality(collect_list(geom)) - 1),
            IF(
                ST_IsValid(element_at(collect_list(geom), 1)),
                element_at(collect_list(geom), 1),
                ST_Buffer(ST_MakeValid(element_at(collect_list(geom), 1)), 0)
            ),
            (acc, x) -> ST_SymDifference(
                ST_ReducePrecision(
                    ST_MakeValid(
                        IF(ST_GeometryType(acc) = 'ST_GeometryCollection', ST_CollectionExtract(acc, 3), acc)
                    ),
                    7
                ),
                IF(ST_IsValid(x), x, ST_Buffer(ST_MakeValid(x), 0))
            )
        ) AS geom
    """


def aggregate_polygons_for_relation(spark: SparkSession, member_polygons: str, result_view: str):
    """
    Builds a relation geometry by aggregating all polygons.

    Args:
        spark: Spark session
        member_polygons: Name of the view containing member polygons (with geom column)
        result_view: Name of the view to create with aggregated geometries
    """
    # The member_polygons view now contains final geometries from merge_ways_into_multipolygon.
    # We just need to clean them up and pass them through.
    spark.sql(f"""
        SELECT 
            id, 
            latest_ts, 
            ST_ReducePrecision(ST_MakeValid(geom), 7) AS geom
        FROM {member_polygons}
        WHERE geom IS NOT NULL
        """).createOrReplaceTempView(result_view)


def relation_merge_geometry_data(
    spark: SparkSession, relations_data: str, geometry_only_data: str, result_view: str
):
    """
    Create a view for all relations with geometry.

    Args:
        spark: Spark session
        relations_data: Name of the view containing relation data
        geometry_only_data: Name of the view containing geometries
        result_view: Name of the view to create
    """
    spark.sql(f"""
        SELECT
            a.id, 
            CAST(a.version AS BIGINT) as version, 
            CAST(a.timestamp AS TIMESTAMP) as timestamp, 
            CAST(a.uid AS BIGINT) as uid, 
            a.user, 
            CAST(a.changeset AS BIGINT) as changeset, 
            a.tags, 
            a.lat, 
            a.lon, 
            CAST(NULL AS ARRAY<BIGINT>) as refs, 
            a.members,
            GREATEST(COALESCE(a.timestamp, current_timestamp()), COALESCE(b.latest_ts, timestamp_seconds(0))) AS latest_ts,
            IF(ST_IsEmpty(b.geom), NULL, ST_ForcePolygonCCW(ST_MakeValid(b.geom))) AS geom
        FROM {relations_data} a
        LEFT OUTER JOIN {geometry_only_data} b
        ON a.id = b.id
        """).createOrReplaceTempView(result_view)


# ============================================================================
# OSC Application Functions
# ============================================================================


def all_dirty_ways(
    spark: SparkSession,
    base_ways: str,
    new_or_property_updated_ways: str,
    dirty_nodes: str,
    result_view: str,
):
    """
    Identify ways that need geometry rebuild.

    Args:
        spark: Spark session
        base_ways: Name of the view containing base ways
        new_or_property_updated_ways: Name of the view containing updated ways
        dirty_nodes: Name of the view containing dirty nodes
        result_view: Name of the view to create
    """
    spark.sql(f"""
        SELECT
            IF(a.id IS NULL, b.id, a.id) AS id,
            IF(a.id IS NULL, b.version, a.version) AS version,
            IF(a.id IS NULL, b.timestamp, a.timestamp) AS timestamp,
            IF(a.id IS NULL, b.uid, a.uid) AS uid,
            IF(a.id IS NULL, b.user, a.user) AS user,
            IF(a.id IS NULL, b.changeset, a.changeset) AS changeset,
            IF(a.id IS NULL, b.tags, a.tags) AS tags,
            IF(a.id IS NULL, b.lat, a.lat) AS lat,
            IF(a.id IS NULL, b.lon, a.lon) AS lon,
            IF(a.id IS NULL, b.refs, a.refs) AS refs,
            IF(a.id IS NULL, b.members, a.members) AS members,
            IF(a.id IS NULL, b.latest_ts, a.latest_ts) AS latest_ts
        FROM {new_or_property_updated_ways} a
        FULL OUTER JOIN (
            SELECT id, version, timestamp, uid, user, changeset, tags, lat, lon, refs, members, latest_ts
            FROM {base_ways}
            WHERE id IN (
                SELECT a.id
                FROM (
                    SELECT
                        id,
                        explode(refs) AS node_id
                    FROM {base_ways}
                ) a
                JOIN {dirty_nodes} b
                ON a.node_id = b.id
            )
        ) b
        ON a.id = b.id
        """).createOrReplaceTempView(result_view)


def all_dirty_relations(
    spark: SparkSession,
    base_relations: str,
    new_or_property_updated_relations: str,
    dirty_ways: str,
    result_view: str,
):
    """
    Identify relations that need geometry rebuild.

    Args:
        spark: Spark session
        base_relations: Name of the view containing base relations
        new_or_property_updated_relations: Name of the view containing updated relations
        dirty_ways: Name of the view containing dirty ways
        result_view: Name of the view to create
    """
    spark.sql(f"""
        SELECT
            IF(a.id IS NULL, b.id, a.id) AS id,
            IF(a.id IS NULL, b.version, a.version) AS version,
            IF(a.id IS NULL, b.timestamp, a.timestamp) AS timestamp,
            IF(a.id IS NULL, b.uid, a.uid) AS uid,
            IF(a.id IS NULL, b.user, a.user) AS user,
            IF(a.id IS NULL, b.changeset, a.changeset) AS changeset,
            IF(a.id IS NULL, b.tags, a.tags) AS tags,
            IF(a.id IS NULL, b.lat, a.lat) AS lat,
            IF(a.id IS NULL, b.lon, a.lon) AS lon,
            IF(a.id IS NULL, b.refs, a.refs) AS refs,
            IF(a.id IS NULL, b.members, a.members) AS members,
            IF(a.id IS NULL, b.latest_ts, a.latest_ts) AS latest_ts
        FROM {new_or_property_updated_relations} a
        FULL OUTER JOIN (
            SELECT
                rel.id,
                version,
                timestamp,
                uid,
                user,
                changeset,
                tags,
                lat,
                lon,
                refs,
                members,
                GREATEST(rel.latest_ts, COALESCE(way_ts.latest_ts, timestamp_seconds(0))) AS latest_ts
            FROM {base_relations} rel
            LEFT JOIN (
                SELECT
                    a.id,
                    MAX(b.latest_ts) AS latest_ts
                FROM (
                    SELECT
                        id,
                        explode(members) AS member
                    FROM {base_relations}
                    WHERE tags['type'] = 'multipolygon'
                ) a
                JOIN {dirty_ways} b
                ON a.member.ref = b.id
                WHERE a.member.type = 'way'
                GROUP BY a.id
            ) way_ts
            ON rel.id = way_ts.id
            WHERE way_ts.id IS NOT NULL
        ) b
        ON a.id = b.id
        """).createOrReplaceTempView(result_view)


def apply_osc_with_geometry(
    spark: SparkSession,
    base_data: str,
    updated_data: str,
    deleted_data: str,
    result_view: str,
):
    """
    Apply changes from an OSC to a base dataset.

    Args:
        spark: Spark session
        base_data: Name of the view containing base data
        updated_data: Name of the view containing updated data
        deleted_data: Name of the view containing deleted IDs
        result_view: Name of the view to create
    """
    spark.sql(f"""
        SELECT a.id, a.version, a.timestamp, a.uid, a.user, a.changeset, a.tags, a.lat, a.lon, a.refs, a.members, a.latest_ts, a.geom
        FROM (
            SELECT
                IF (b.id IS NULL, a.id, b.id) AS id,
                IF (b.id IS NULL, a.version, b.version) AS version,
                IF (b.id IS NULL, a.timestamp, b.timestamp) AS timestamp,
                IF (b.id IS NULL, a.uid, b.uid) AS uid,
                IF (b.id IS NULL, a.user, b.user) AS user,
                IF (b.id IS NULL, a.changeset, b.changeset) AS changeset,
                IF (b.id IS NULL, a.tags, b.tags) AS tags,
                IF (b.id IS NULL, a.lat, b.lat) AS lat,
                IF (b.id IS NULL, a.lon, b.lon) AS lon,
                IF (b.id IS NULL, a.refs, b.refs) AS refs,
                IF (b.id IS NULL, a.members, b.members) AS members,
                IF (b.id IS NULL, a.latest_ts, b.latest_ts) AS latest_ts,
                IF (b.id IS NULL, a.geom, b.geom) AS geom
            FROM {base_data} a
            FULL OUTER JOIN {updated_data} b
            ON a.id = b.id
        ) a
        LEFT OUTER JOIN {deleted_data} b
        ON a.id = b.id
        WHERE b.id IS NULL
        """).createOrReplaceTempView(result_view)


# ============================================================================
# Iceberg Preparation Functions
# ============================================================================


def prepare_for_iceberg(
    spark: SparkSession,
    data_view: str,
    osm_type: str,
    result_view: str,
    partition_number: int = 200,
):
    """
    Prepare data for Iceberg storage by converting geometry to WKB and adding bbox.

    Args:
        spark: Spark session
        data_view: Name of the view containing data with geom column
        osm_type: OSM type ('node', 'way', or 'relation')
        result_view: Name of the view to create
        partition_number: Number of partitions
    """
    df = spark.sql(f"""
        SELECT id, 
               CAST(version AS BIGINT) as version, 
               CAST(timestamp AS TIMESTAMP) as timestamp, 
               CAST(uid AS BIGINT) as uid, 
               user, 
               CAST(changeset AS BIGINT) as changeset, 
               tags, 
               lat, 
               lon, 
               refs, 
               members, 
               CAST(latest_ts AS TIMESTAMP) as latest_ts, 
               geom
        FROM {data_view}
        WHERE geom IS NOT NULL
    """)

    # Add bbox and convert geometry to WKB
    df = (
        df.withColumn(
            "bbox",
            F.struct(
                F.expr("ST_XMin(geom)").cast("float").alias("xmin"),
                F.expr("ST_XMax(geom)").cast("float").alias("xmax"),
                F.expr("ST_YMin(geom)").cast("float").alias("ymin"),
                F.expr("ST_YMax(geom)").cast("float").alias("ymax"),
            ),
        )
        .withColumn(
            "geometry",
            (
                F.expr("ST_AsBinary(geom)")
                if osm_type != "relation"
                else F.expr(f"""
                IF(
                    LENGTH(ST_AsBinary(geom)) < {MAXIMUM_RELATION_GEOMETRY_SIZE},
                    ST_AsBinary(geom),
                    ST_AsBinary(ST_SimplifyPreserveTopology(geom, {HUGE_GEOMETRY_SIMPLIFICATION_FACTOR}))
                )
            """)
            ),
        )
        .drop("geom")
    )

    # Add type column
    df = df.withColumn("type", F.lit(osm_type))

    df.createOrReplaceTempView(result_view)
