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
from .geometry.ways import (
    build_linestring_for_ways,
    build_ways_geometry_from_linestring,
)
from .iceberg import (
    delete_from_table,
    get_last_applied_sequence,
    get_table_max_timestamp,
    load_with_geom,
    merge_into_table,
    refresh_node_to_ways,
    refresh_way_to_relations,
    set_current_osc_file,
    set_last_applied_sequence,
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
    """Keep the latest version per (id, type) from the OSC, cast timestamps."""
    spark.sql(f"""
        SELECT
            id,
            type,
            max_by(op, version)                          AS op,
            max(version)                                 AS version,
            CAST(max_by(timestamp, version) AS TIMESTAMP) AS timestamp,
            max_by(uid, version)                         AS uid,
            max_by(user, version)                        AS user,
            max_by(changeset, version)                   AS changeset,
            max_by(tags, version)                        AS tags,
            max_by(lat, version)                         AS lat,
            max_by(lon, version)                         AS lon,
            max_by(refs, version)                        AS refs,
            max_by(members, version)                     AS members,
            CAST(max_by(timestamp, version) AS TIMESTAMP) AS latest_ts
        FROM {osc_view}
        GROUP BY id, type
    """).createOrReplaceTempView(result_view)


# ---------------------------------------------------------------------------
# Parsing OSC XML -> DataFrame
# ---------------------------------------------------------------------------


def _fetch_xml(file_path: str) -> bytes:
    """Return raw OSC XML bytes from a local file."""
    if file_path.startswith("s3://"):
        raise NotImplementedError("S3 OSC files not yet implemented")
    try:
        with gzip.GzipFile(file_path, "rb") as f:
            return f.read()
    except OSError:
        with open(file_path, "rb") as f:
            return f.read()


def _parse_osc(xml_bytes: bytes) -> list:
    """Flatten OSC XML into a list of records (one row per change)."""
    records = []
    for action in ElementTree.fromstring(xml_bytes):
        op = action.tag
        for element in action:
            elem_type = element.tag
            tags, refs, members = {}, None, None
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
            records.append(
                {
                    "id": int(element.attrib["id"]),
                    "type": elem_type,
                    "op": op,
                    "version": int(element.attrib["version"]),
                    "timestamp": ts,
                    "uid": int(element.attrib.get("uid", 0)),
                    "user": element.attrib.get("user", ""),
                    "changeset": int(element.attrib.get("changeset", 0)),
                    "tags": tags,
                    "lat": float(element.attrib["lat"]) if elem_type == "node" else None,
                    "lon": float(element.attrib["lon"]) if elem_type == "node" else None,
                    "refs": refs,
                    "members": members,
                    "latest_ts": ts,
                }
            )
    return records


def read_osc_from_file(spark: SparkSession, file_path: str) -> DataFrame:
    """Read a local .osc or .osc.gz (or plain XML) into a DataFrame."""
    return spark.createDataFrame(
        _parse_osc(_fetch_xml(file_path)),
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
    table_name: str,
    download_dir: str,
    base_url: str = DC_REPLICATION_URL,
) -> Optional[str]:
    """Download the next pending OSC file and return its local path.

    Returns ``None`` if the table is already up to date.
    """
    from osmium.replication.server import ReplicationServer

    from .replication import download_osc_file, pending_sequences

    last_seq = get_last_applied_sequence(spark, table_name)

    with ReplicationServer(base_url) as server:
        remote_state = server.get_state_info()
        if remote_state is None:
            raise RuntimeError(f"Could not fetch remote state from {base_url}")

        if last_seq is None:
            ts = get_table_max_timestamp(spark, table_name)
            if ts is None:
                raise ValueError(f"Table {table_name} is empty — run init first.")
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            last_seq = server.timestamp_to_sequence(ts)
            if last_seq is None:
                raise RuntimeError(f"Could not map timestamp {ts} to a sequence number")
            logger.info("No stored sequence — estimated %d from MAX(timestamp) %s", last_seq, ts)

        seqs = pending_sequences(last_seq, remote_state.sequence)
        if not seqs:
            logger.info("Table is current at sequence %d", last_seq)
            return None

        logger.info(
            "Pending: %d file(s) (%d .. %d), downloading %d",
            len(seqs), seqs[0], seqs[-1], seqs[0],
        )
        return download_osc_file(server, seqs[0], download_dir)


# ---------------------------------------------------------------------------
# Apply: apply a single OSC file to the table
# ---------------------------------------------------------------------------


def _osc_is_redundant(spark: SparkSession, table_name: str) -> bool:
    """True if the OSC has nothing new for this table.

    A feature needs applying if it's a create/modify with a higher version
    than the table (or doesn't exist in the table at all), or a delete for
    an ID that exists in the table.
    """
    row = spark.sql(f"""
        SELECT COUNT(*) AS need_apply
        FROM osc_latest o
        LEFT JOIN {table_name} t ON o.id = t.id AND o.type = t.type
        WHERE (o.op IN ('create', 'modify') AND (t.id IS NULL OR t.version < o.version))
           OR (o.op = 'delete' AND t.id IS NOT NULL)
    """).collect()[0]
    redundant = row["need_apply"] == 0
    if redundant:
        logger.info("OSC is redundant — all %d features already at current version",
                     spark.sql("SELECT COUNT(*) FROM osc_latest").collect()[0][0])
    else:
        logger.info("OSC has %d features to apply", row["need_apply"])
    return redundant


def apply_osc(
    spark: SparkSession,
    table_name: str,
    osc_path: str,
    node_to_ways: str,
    way_to_relations: str,
) -> None:
    """Apply a single OSC file to the Iceberg table.

    One file -> one dedup -> one geometry rebuild -> one MERGE per type.
    Stamps ``current-osc-file`` and ``last-applied-osc-sequence`` on the
    table so the next call to ``next_osc_path`` picks up where we left off.

    Skips the expensive geometry/MERGE pipeline entirely if every feature
    in the OSC is already in the table at the same or higher version.
    """
    label = os.path.basename(osc_path)
    set_current_osc_file(spark, table_name, label)

    read_osc_from_file(spark, osc_path).createOrReplaceTempView("osc_raw")
    osc_dedup(spark, "osc_raw", "osc_latest")

    if _osc_is_redundant(spark, table_name):
        logger.info("%s: fully redundant, skipping", label)
        seq = _sequence_from_path(osc_path)
        if seq is not None:
            set_last_applied_sequence(spark, table_name, seq)
        return

    logger.info("%s: applying changes", label)

    for osm_type in ("node", "way", "relation"):
        load_with_geom(spark, table_name, osm_type, f"base_{osm_type}s")

        spark.sql(f"""
            SELECT * FROM osc_latest
            WHERE type = '{osm_type}' AND op IN ('create', 'modify')
        """).createOrReplaceTempView(f"osc_{osm_type}_upserts")

        spark.sql(f"""
            SELECT id FROM osc_latest WHERE type = '{osm_type}' AND op = 'delete'
        """).createOrReplaceTempView(f"osc_{osm_type}_deletes")

    # --- Nodes ---------------------------------------------------------------
    build_node_geometry(spark, "osc_node_upserts", "updated_nodes_geom")
    prepare_for_iceberg(spark, "updated_nodes_geom", "node", "nodes_iceberg")
    merge_into_table(spark, table_name, "nodes_iceberg", "t.id = s.id AND t.type = 'node'")
    delete_from_table(spark, table_name, "osc_node_deletes", "t.id = s.id AND t.type = 'node'")
    load_with_geom(spark, table_name, "node", "nodes_with_geom")
    logger.info("%s: nodes merged", label)

    # --- Ways ----------------------------------------------------------------
    all_dirty_ways(
        spark, "base_ways", "osc_way_upserts", "osc_node_upserts",
        node_to_ways, "dirty_ways",
    )
    spark.sql("SELECT DISTINCT id FROM dirty_ways").persist().createOrReplaceTempView("_dirty_way_ids")

    build_linestring_for_ways(spark, "dirty_ways", "nodes_with_geom", "dirty_ways_lines")
    build_ways_geometry_from_linestring(spark, "dirty_ways_lines", "dirty_ways_geom")
    prepare_for_iceberg(spark, "dirty_ways_geom", "way", "ways_iceberg")
    merge_into_table(spark, table_name, "ways_iceberg", "t.id = s.id AND t.type = 'way'")
    delete_from_table(spark, table_name, "osc_way_deletes", "t.id = s.id AND t.type = 'way'")

    refresh_node_to_ways(spark, table_name, node_to_ways, "_dirty_way_ids")
    spark.sql(f"DELETE FROM {node_to_ways} WHERE way_id IN (SELECT id FROM osc_way_deletes)")
    load_with_geom(spark, table_name, "way", "ways_with_geom")
    logger.info("%s: ways merged + index updated", label)

    # --- Relations -----------------------------------------------------------
    all_dirty_relations(
        spark, "base_relations", "osc_relation_upserts", "dirty_ways",
        way_to_relations, "dirty_relations",
    )
    spark.sql(
        "SELECT DISTINCT id FROM dirty_relations"
    ).persist().createOrReplaceTempView("_dirty_rel_ids")

    relations_need_geometry(spark, "dirty_relations", "rels_need_geom")
    construct_multipolygon(spark, "rels_need_geom", "ways_with_geom", "rels_geom")
    relation_merge_geometry_data(spark, "dirty_relations", "rels_geom", "dirty_rels_geom")
    prepare_for_iceberg(spark, "dirty_rels_geom", "relation", "relations_iceberg")
    merge_into_table(
        spark, table_name, "relations_iceberg", "t.id = s.id AND t.type = 'relation'"
    )
    delete_from_table(
        spark, table_name, "osc_relation_deletes", "t.id = s.id AND t.type = 'relation'"
    )

    refresh_way_to_relations(spark, table_name, way_to_relations, "_dirty_rel_ids")
    spark.sql(
        f"DELETE FROM {way_to_relations} WHERE relation_id IN (SELECT id FROM osc_relation_deletes)"
    )
    logger.info("%s: relations merged + index updated", label)

    seq = _sequence_from_path(osc_path)
    if seq is not None:
        set_last_applied_sequence(spark, table_name, seq)
        logger.info("%s: stamped sequence %d", label, seq)
