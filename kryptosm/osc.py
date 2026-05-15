"""
OSC (OpenStreetMap Change) ingestion and application.

Parsing is Python (XML). Once parsed, everything is a Spark DataFrame and
downstream logic stays in SQL.
"""

import gzip
import logging
import os
from datetime import timezone
from typing import Optional
from xml.etree import ElementTree

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from .geometry.iceberg_prep import prepare_for_iceberg
from .geometry.nodes import build_node_geometry
from .geometry.osc_apply import all_dirty_relations, all_dirty_ways
from .geometry.relations import (
    construct_multipolygon,
    relation_merge_geometry_data,
    relations_need_geometry,
)
from .geometry.ways import build_way_linestrings, promote_closed_ways_to_areas
from .iceberg import (
    append_osc_archive,
    get_last_applied_sequence,
    get_last_archived_sequence,
    get_min_applied_sequence,
    get_table_max_timestamp,
    load_with_geom,
    merge_upsert_delete,
    refresh_node_to_relations,
    refresh_node_to_ways,
    refresh_relation_to_relations,
    refresh_way_to_relations,
    set_current_osc_file,
    set_last_applied_sequence,
    set_last_archived_sequence,
)
from .replication import DC_REPLICATION_URL

logger = logging.getLogger(__name__)

OSC_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), False),
        T.StructField("type", T.StringType(), False),
        T.StructField("op", T.StringType(), False),
        T.StructField("version", T.LongType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("uid", T.LongType(), True),
        T.StructField("user", T.StringType(), True),
        T.StructField("changeset", T.LongType(), True),
        T.StructField("tags", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("lat", T.DoubleType(), True),
        T.StructField("lon", T.DoubleType(), True),
        T.StructField("refs", T.ArrayType(T.LongType()), True),
        T.StructField(
            "members",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("type", T.StringType(), True),
                        T.StructField("ref", T.LongType(), True),
                        T.StructField("role", T.StringType(), True),
                    ]
                )
            ),
            True,
        ),
        T.StructField("latest_ts", T.StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


def osc_dedup(spark: SparkSession, osc_view: str, result_view: str):
    """Keep the latest version per (id, type) from the OSC, cast timestamps.

    Also captures every non-winning version's ``changeset`` into
    ``additional_changesets`` so attribution from collapsed versions
    isn't lost. Example: if one OSC contains node N at version 5
    (changeset A) and version 6 (changeset B), the winner row keeps
    changeset B and emits ``additional_changesets = [A]``.

    Uses ROW_NUMBER() over (version DESC, timestamp DESC) to pick the
    canonical winner, then aggregates losers' distinct changesets per
    (id, type) and joins them back to the winner row.

    NULL changesets in the source are coerced to ``0`` (matching the
    convention from ``_iter_osc_records`` and OSM's anonymous edits) so
    downstream views see no NULLs in this column.
    """
    spark.sql(f"""
        WITH _ranked AS (
            SELECT
                id, type, op, version, timestamp, uid, user,
                COALESCE(changeset, 0) AS changeset,
                tags, lat, lon, refs, members,
                ROW_NUMBER() OVER (
                    PARTITION BY id, type
                    ORDER BY version DESC NULLS LAST,
                             timestamp DESC NULLS LAST
                ) AS _rn
            FROM {osc_view}
        ),
        _losers AS (
            SELECT
                id, type,
                array_distinct(collect_list(changeset)) AS additional_changesets
            FROM _ranked
            WHERE _rn > 1
            GROUP BY id, type
        )
        SELECT
            w.id,
            w.type,
            w.op,
            w.version,
            CAST(w.timestamp AS TIMESTAMP)               AS timestamp,
            w.uid,
            w.user,
            w.changeset,
            w.tags,
            w.lat,
            w.lon,
            w.refs,
            w.members,
            CAST(w.timestamp AS TIMESTAMP)               AS latest_ts,
            COALESCE(l.additional_changesets,
                     CAST(array() AS ARRAY<BIGINT>))     AS additional_changesets
        FROM _ranked w
        LEFT JOIN _losers l ON w.id = l.id AND w.type = l.type
        WHERE w._rn = 1
    """).createOrReplaceTempView(result_view)


# ---------------------------------------------------------------------------
# Parsing OSC XML -> DataFrame
# ---------------------------------------------------------------------------


def _iter_osc_records(file_path: str):
    """Stream OSC XML, yielding one record per element.

    Uses ``iterparse`` + ``elem.clear()`` so peak memory is O(1) in the size
    of the document — only the currently-being-parsed element is resident.
    Daily-planet OSCs can decompress to multiple GB; the previous
    ``ElementTree.fromstring`` approach materialized the whole DOM in driver
    memory and triggered ``Parse error: out of memory`` on Glue.
    """
    if file_path.startswith("s3://"):
        raise NotImplementedError("S3 OSC files not yet implemented")

    # Open the gz directly as a stream — never read the full payload into bytes.
    try:
        stream = gzip.open(file_path, "rb")
    except OSError:
        stream = open(file_path, "rb")

    try:
        current_op: Optional[str] = None
        for event, element in ElementTree.iterparse(stream, events=("start", "end")):
            tag = element.tag
            if event == "start":
                if tag in ("create", "modify", "delete"):
                    current_op = tag
                continue

            # event == "end"
            if tag in ("create", "modify", "delete"):
                # Action done — free it; children were already freed below.
                element.clear()
                continue

            if tag not in ("node", "way", "relation") or current_op is None:
                continue

            tags: dict = {}
            refs: Optional[list] = None
            members: Optional[list] = None
            for child in element:
                if child.tag == "tag":
                    tags[child.attrib["k"]] = child.attrib["v"]
                elif child.tag == "nd":
                    refs = refs or []
                    refs.append(int(child.attrib["ref"]))
                elif child.tag == "member":
                    members = members or []
                    members.append(
                        {
                            "type": child.attrib["type"],
                            "ref": int(child.attrib["ref"]),
                            "role": child.attrib.get("role", ""),
                        }
                    )

            ts = element.attrib["timestamp"]
            yield {
                "id": int(element.attrib["id"]),
                "type": tag,
                "op": current_op,
                "version": int(element.attrib["version"]),
                "timestamp": ts,
                "uid": int(element.attrib["uid"]) if "uid" in element.attrib else None,
                "user": element.attrib.get("user"),
                # Default missing OSC changeset to 0 so the column is never NULL
                # downstream — matches OSM Parquet's anonymous-edit convention
                # and lets every later "x > a.changeset" filter stay total.
                "changeset": int(element.attrib["changeset"]) if "changeset" in element.attrib else 0,
                "tags": tags,
                "lat": float(element.attrib["lat"]) if tag == "node" else None,
                "lon": float(element.attrib["lon"]) if tag == "node" else None,
                "refs": refs,
                "members": members,
                "latest_ts": ts,
            }
            # Free this element so iterparse doesn't accumulate the document.
            element.clear()
    finally:
        stream.close()


def read_osc_from_file(spark: SparkSession, file_path: str) -> DataFrame:
    """Read a local .osc or .osc.gz (or plain XML) into a DataFrame.

    Streams the XML through ``_iter_osc_records`` so the peak driver memory
    is bounded by the materialized record list (one Python dict per change),
    not by the parsed XML DOM.
    """
    return spark.createDataFrame(
        list(_iter_osc_records(file_path)),
        OSC_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Fetch: download the next pending OSC file
# ---------------------------------------------------------------------------


def _sequence_from_path(path: str) -> Optional[int]:
    """Extract a sequence number from a filename like ``4780.osc.gz``."""
    stem = os.path.basename(path).split(".")[0]
    return int(stem) if stem.isdigit() else None


def next_osc_path(
    spark: SparkSession,
    nodes_table: str,
    ways_table: str,
    relations_table: str,
    download_dir: str,
    base_url: str = DC_REPLICATION_URL,
) -> Optional[str]:
    """Download the next pending OSC file and return its local path.

    Returns ``None`` if the database is fully up to date (all three per-type
    tables stamped at the same head sequence).

    The "current" sequence is the MIN of ``last-applied-osc-sequence`` across
    nodes/ways/relations. If a previous apply was interrupted mid-flight,
    the lagging per-type table holds back the MIN \u2014 ``apply_osc`` will
    pick up that sequence again and skip the per-type sections that already
    completed.

    If none of the three per-type tables has a stamp yet (fresh init), we
    estimate the starting sequence from MAX(timestamp) across the tables.
    """
    from osmium.replication.server import ReplicationServer

    from .replication import download_osc_file, pending_sequences

    last_seq = get_min_applied_sequence(spark, nodes_table, ways_table, relations_table)
    logger.info("Last applied sequence: %s", last_seq)

    logger.info("Connecting to replication server: %s", base_url)
    with ReplicationServer(base_url) as server:
        remote_state = server.get_state_info()
        if remote_state is None:
            raise RuntimeError(f"Could not fetch remote state from {base_url}")
        logger.info(
            "Remote state: sequence=%d, timestamp=%s",
            remote_state.sequence, remote_state.timestamp,
        )

        if last_seq is None:
            logger.info("No stored sequence \u2014 estimating from MAX(timestamp)")
            ts = get_table_max_timestamp(spark, nodes_table, ways_table, relations_table)
            if ts is None:
                raise ValueError(
                    "Krypton database appears empty \u2014 run init first."
                )
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            logger.info("MAX(timestamp) across tables: %s", ts)
            last_seq = server.timestamp_to_sequence(ts)
            if last_seq is None:
                raise RuntimeError(f"Could not map timestamp {ts} to a sequence number")
            logger.info("Estimated starting sequence: %d", last_seq)

        seqs = pending_sequences(last_seq, remote_state.sequence)
        if not seqs:
            logger.info("Database is current at sequence %d", last_seq)
            return None

        logger.info(
            "Pending: %d file(s) (%d .. %d), downloading %d",
            len(seqs), seqs[0], seqs[-1], seqs[0],
        )
        path = download_osc_file(server, seqs[0], download_dir)
        logger.info("Downloaded %s (%d bytes)", os.path.basename(path), os.path.getsize(path))
        return path


# ---------------------------------------------------------------------------
# Apply: apply a single OSC file to the table
# ---------------------------------------------------------------------------


def _classify_node_upserts(
    spark: SparkSession,
    osc_node_upserts: str,
    base_nodes: str,
    geom_dirty_view: str,
    tag_only_view: str,
) -> None:
    """Split node upserts into geom-dirty (coords moved or new) vs tag-only.

    A node is geom-dirty if it didn't previously exist in the table, or if
    its (lat, lon) differs from the existing row. Otherwise it's tag-only —
    its row in ``osm`` still gets updated (version, tags, changeset...) but
    its parents (ways/relations) don't need geometry rebuilt.
    """
    spark.sql(f"""
        SELECT o.*
        FROM {osc_node_upserts} o
        LEFT JOIN {base_nodes} b ON o.id = b.id
        WHERE b.id IS NULL
           OR NOT (o.lat <=> b.lat AND o.lon <=> b.lon)
    """).createOrReplaceTempView(geom_dirty_view)

    spark.sql(f"""
        SELECT o.*
        FROM {osc_node_upserts} o
        JOIN {base_nodes} b ON o.id = b.id
        WHERE o.lat <=> b.lat AND o.lon <=> b.lon
    """).createOrReplaceTempView(tag_only_view)


def _read_and_dedup_osc(spark: SparkSession, osc_path: str) -> None:
    """Parse an OSC file into ``osc_raw`` and dedup into ``osc_latest``."""
    read_osc_from_file(spark, osc_path).createOrReplaceTempView("osc_raw")
    osc_dedup(spark, "osc_raw", "osc_latest")


def _slice_osc_by_type(spark: SparkSession) -> None:
    """Create per-type ``osc_<type>_upserts`` and ``osc_<type>_deletes`` views.

    Reads from the canonical ``osc_latest`` view and slices it by element
    type and op for each downstream per-type apply section.
    """
    for osm_type in ("node", "way", "relation"):
        spark.sql(f"""
            SELECT * FROM osc_latest
            WHERE type = '{osm_type}' AND op IN ('create', 'modify')
        """).createOrReplaceTempView(f"osc_{osm_type}_upserts")

        spark.sql(f"""
            SELECT id FROM osc_latest WHERE type = '{osm_type}' AND op = 'delete'
        """).createOrReplaceTempView(f"osc_{osm_type}_deletes")


def _archive_osc_records(
    spark: SparkSession,
    osc_archive: str,
    label: str,
    seq: Optional[int],
    archive_done: bool,
) -> None:
    """Append OSC records to the archive table (idempotent on resume)."""
    if archive_done:
        logger.info("%s: archive already has sequence %s, skipping append", label, seq)
        return
    append_osc_archive(spark, osc_archive, "osc_latest", seq, label)
    if seq is not None:
        set_last_archived_sequence(spark, osc_archive, seq)


def _apply_node_section(
    spark: SparkSession,
    nodes_table: str,
    label: str,
    seq: Optional[int],
    nodes_done: bool,
) -> None:
    """Apply node changes, leaving a ``geom_dirty_nodes`` view for downstream.

    Two paths:
      * ``nodes_done``: skip the MERGE; expose all OSC node upserts as
        geom-dirty (over-eager but correct for downstream widening, since we
        can't classify post-merge).
      * Full apply: classify pre-merge, MERGE upserts + deletes, stamp.
    """
    if nodes_done:
        logger.info("%s: nodes already at seq %s, skipping", label, seq)
        spark.sql("SELECT * FROM osc_node_upserts").createOrReplaceTempView("geom_dirty_nodes")
        return

    spark.sql(f"""
        SELECT *, ST_GeomFromWKB(geometry) AS geom
        FROM {nodes_table}
        WHERE id IN (SELECT id FROM osc_node_upserts)
    """).createOrReplaceTempView("base_nodes")
    _classify_node_upserts(
        spark, "osc_node_upserts", "base_nodes",
        "geom_dirty_nodes", "tag_only_nodes",
    )
    logger.info("%s: classified node upserts (geom-dirty vs tag-only)", label)
    # Carry-forward additional_changesets from the existing row before we
    # rebuild the node geometry. Nodes don't go through all_dirty_nodes
    # (only ways/relations have a dirty-set view), so the union has to
    # happen explicitly here. Result: the OSC-side additional_changesets
    # (containing OSC-dedup losers) is unioned with the previously-stored
    # additional_changesets, so attribution accumulates monotonically across
    # OSC applies.
    spark.sql("""
        SELECT
            o.id, o.type, o.op, o.version, o.timestamp, o.uid, o.user,
            o.changeset, o.tags, o.lat, o.lon, o.refs, o.members, o.latest_ts,
            array_distinct(
                array_union(
                    COALESCE(o.additional_changesets,
                             CAST(array() AS ARRAY<BIGINT>)),
                    COALESCE(b.additional_changesets,
                             CAST(array() AS ARRAY<BIGINT>))
                )
            ) AS additional_changesets
        FROM osc_node_upserts o
        LEFT JOIN base_nodes b ON o.id = b.id
    """).createOrReplaceTempView("osc_node_upserts_carried")
    build_node_geometry(spark, "osc_node_upserts_carried", "updated_nodes_geom")
    prepare_for_iceberg(spark, "updated_nodes_geom", "node", "nodes_iceberg")
    logger.info("%s: node geometry built, merging into %s", label, nodes_table)
    merge_upsert_delete(spark, nodes_table, "nodes_iceberg", "osc_node_deletes")
    if seq is not None:
        set_last_applied_sequence(spark, nodes_table, seq)
    logger.info("%s: nodes applied, stamped seq=%s", label, seq)


def _apply_way_section(
    spark: SparkSession,
    ways_table: str,
    nodes_table: str,
    node_to_ways: str,
    label: str,
    seq: Optional[int],
    ways_done: bool,
) -> None:
    """Apply way changes, leaving a ``dirty_ways`` view for relations widening.

    Reads ``geom_dirty_nodes`` from upstream.
    """
    if ways_done:
        logger.info("%s: ways already at seq %s, skipping", label, seq)
        spark.sql("SELECT id FROM osc_way_upserts").createOrReplaceTempView("dirty_ways")
        return

    load_with_geom(spark, ways_table, "base_ways")
    all_dirty_ways(
        spark, "base_ways", "osc_way_upserts", "geom_dirty_nodes",
        node_to_ways, "dirty_ways",
    )
    spark.sql("SELECT DISTINCT id FROM dirty_ways").persist().createOrReplaceTempView("_dirty_way_ids")
    logger.info("%s: computed dirty ways (direct + node-widened)", label)

    spark.sql(f"""
        SELECT *, ST_GeomFromWKB(geometry) AS geom
        FROM {nodes_table}
        WHERE id IN (
            SELECT DISTINCT node_id
            FROM (SELECT explode(refs) AS node_id FROM dirty_ways)
        )
    """).createOrReplaceTempView("nodes_with_geom")

    logger.info("%s: building way geometry", label)
    build_way_linestrings(spark, "dirty_ways", "nodes_with_geom", "dirty_ways_lines")
    promote_closed_ways_to_areas(spark, "dirty_ways_lines", "dirty_ways_geom")
    prepare_for_iceberg(spark, "dirty_ways_geom", "way", "ways_iceberg")
    logger.info("%s: way geometry built, merging into %s", label, ways_table)
    merge_upsert_delete(spark, ways_table, "ways_iceberg", "osc_way_deletes")

    logger.info("%s: refreshing node_to_ways index", label)
    refresh_node_to_ways(spark, ways_table, node_to_ways, "_dirty_way_ids")
    spark.sql(f"DELETE FROM {node_to_ways} WHERE way_id IN (SELECT id FROM osc_way_deletes)")
    if seq is not None:
        set_last_applied_sequence(spark, ways_table, seq)
    logger.info("%s: ways applied, stamped seq=%s", label, seq)


def _apply_relation_section(
    spark: SparkSession,
    relations_table: str,
    ways_table: str,
    nodes_table: str,
    way_to_relations: str,
    node_to_relations: str,
    relation_to_relations: str,
    label: str,
    seq: Optional[int],
    rels_done: bool,
) -> None:
    """Apply relation changes. Reads ``dirty_ways`` and ``geom_dirty_nodes``
    from upstream.
    """
    if rels_done:
        logger.info("%s: relations already at seq %s, skipping", label, seq)
        return

    load_with_geom(spark, relations_table, "base_relations")
    all_dirty_relations(
        spark, "base_relations", "osc_relation_upserts", "dirty_ways",
        way_to_relations, "dirty_relations",
        dirty_nodes="geom_dirty_nodes",
        node_to_relations_table=node_to_relations,
        relation_to_relations_table=relation_to_relations,
    )
    spark.sql(
        "SELECT DISTINCT id FROM dirty_relations"
    ).persist().createOrReplaceTempView("_dirty_rel_ids")
    logger.info("%s: computed dirty relations (direct + widened)", label)

    spark.sql(f"""
        SELECT *, ST_GeomFromWKB(geometry) AS geom
        FROM {ways_table}
        WHERE id IN (
            SELECT DISTINCT member.ref
            FROM (SELECT explode(members) AS member FROM dirty_relations)
            WHERE member.type = 'way'
        )
    """).createOrReplaceTempView("ways_with_geom")

    spark.sql(f"""
        SELECT *, ST_GeomFromWKB(geometry) AS geom
        FROM {nodes_table}
        WHERE id IN (
            SELECT DISTINCT member.ref
            FROM (SELECT explode(members) AS member FROM dirty_relations)
            WHERE member.type = 'node'
        )
    """).createOrReplaceTempView("nodes_with_geom")

    relations_need_geometry(spark, "dirty_relations", "rels_need_geom")
    logger.info("%s: building relation geometry", label)
    construct_multipolygon(
        spark, "rels_need_geom", "ways_with_geom", "rels_geom",
        nodes_geometry="nodes_with_geom",
    )
    relation_merge_geometry_data(
        spark, "dirty_relations", "rels_geom", "dirty_rels_geom",
        ways_geometry="ways_with_geom", nodes_geometry="nodes_with_geom",
    )
    prepare_for_iceberg(spark, "dirty_rels_geom", "relation", "relations_iceberg")
    logger.info("%s: relation geometry built, merging into %s", label, relations_table)
    merge_upsert_delete(spark, relations_table, "relations_iceberg", "osc_relation_deletes")

    logger.info("%s: refreshing relation indexes", label)
    refresh_way_to_relations(spark, relations_table, way_to_relations, "_dirty_rel_ids")
    spark.sql(
        f"DELETE FROM {way_to_relations} WHERE relation_id IN (SELECT id FROM osc_relation_deletes)"
    )
    refresh_node_to_relations(spark, relations_table, node_to_relations, "_dirty_rel_ids")
    spark.sql(
        f"DELETE FROM {node_to_relations} WHERE relation_id IN (SELECT id FROM osc_relation_deletes)"
    )
    refresh_relation_to_relations(
        spark, relations_table, relation_to_relations, "_dirty_rel_ids"
    )
    spark.sql(
        f"DELETE FROM {relation_to_relations} WHERE parent_relation_id IN (SELECT id FROM osc_relation_deletes)"
    )
    if seq is not None:
        set_last_applied_sequence(spark, relations_table, seq)
    logger.info("%s: relations applied, stamped seq=%s", label, seq)


def apply_osc(
    spark: SparkSession,
    osc_path: str,
    nodes_table: str,
    ways_table: str,
    relations_table: str,
    node_to_ways: str,
    way_to_relations: str,
    node_to_relations: str,
    relation_to_relations: str,
    osc_archive: str,
) -> None:
    """Apply a single OSC file to the Krypton per-type tables.

    Resumable per-table apply: each per-type table carries its own
    ``last-applied-osc-sequence`` stamp, and each section can independently
    skip work when its target table is already at this sequence (resume from
    a partial mid-flight failure) or when the OSC slice for that type has
    no new info (redundant section).

    The function is intentionally just orchestration. The query logic for
    each section lives in ``_apply_node_section``, ``_apply_way_section``,
    and ``_apply_relation_section``.
    """
    label = os.path.basename(osc_path)
    seq = _sequence_from_path(osc_path)

    # Read per-table state.
    nodes_seq   = get_last_applied_sequence(spark, nodes_table)
    ways_seq    = get_last_applied_sequence(spark, ways_table)
    rels_seq    = get_last_applied_sequence(spark, relations_table)
    archive_seq = get_last_archived_sequence(spark, osc_archive)

    nodes_done   = seq is not None and nodes_seq   is not None and nodes_seq   >= seq
    ways_done    = seq is not None and ways_seq    is not None and ways_seq    >= seq
    rels_done    = seq is not None and rels_seq    is not None and rels_seq    >= seq
    archive_done = seq is not None and archive_seq is not None and archive_seq >= seq

    set_current_osc_file(spark, osc_archive, label)
    _read_and_dedup_osc(spark, osc_path)
    _archive_osc_records(spark, osc_archive, label, seq, archive_done)

    if nodes_done and ways_done and rels_done:
        logger.info("%s: all per-type tables already at sequence %s, nothing to do", label, seq)
        return

    logger.info(
        "%s: applying changes (nodes_done=%s ways_done=%s rels_done=%s)",
        label, nodes_done, ways_done, rels_done,
    )
    _slice_osc_by_type(spark)

    _apply_node_section(spark, nodes_table, label, seq, nodes_done)
    spark.sql(f"REFRESH TABLE {nodes_table}")

    _apply_way_section(spark, ways_table, nodes_table, node_to_ways, label, seq, ways_done)
    spark.sql(f"REFRESH TABLE {ways_table}")

    _apply_relation_section(
        spark, relations_table, ways_table, nodes_table,
        way_to_relations, node_to_relations, relation_to_relations,
        label, seq, rels_done,
    )
