"""
Iceberg table operations for the Krypton OSM database.

Krypton has three per-type tables (`nodes`, `ways`, `relations`), three
reverse-index tables (`node_to_ways`, `way_to_relations`, `node_to_relations`),
and one OSC archive table (`osc_changes`). The archive table also holds the
``last-applied-osc-sequence`` and ``current-osc-file`` table properties — it's
the natural single home for "what state the krypton database is in".
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Tunable Iceberg table properties for different data scales."""

    distribution_mode: str = "range"
    bloom_filter_enabled: bool = True
    bloom_filter_max_bytes: Optional[int] = 1_048_576

    @classmethod
    def testing(cls) -> "TableConfig":
        """Lightweight settings for local tests across all three per-type tables."""
        return cls(
            distribution_mode="range",
            bloom_filter_enabled=True,
            bloom_filter_max_bytes=131_072,
        )

    @classmethod
    def nodes_production(cls) -> "TableConfig":
        """Planet-scale tuning for ~10B-row nodes table.

        Larger bloom budget per file because the node-id keyspace is huge and
        point lookups against the nodes table are common (e.g. way-rebuild
        joins).
        """
        return cls(
            distribution_mode="range",
            bloom_filter_enabled=True,
            bloom_filter_max_bytes=8_388_608,
        )

    @classmethod
    def ways_production(cls) -> "TableConfig":
        """Planet-scale tuning for ~1.2B-row ways table."""
        return cls(
            distribution_mode="range",
            bloom_filter_enabled=True,
            bloom_filter_max_bytes=1_048_576,
        )

    @classmethod
    def relations_production(cls) -> "TableConfig":
        """Planet-scale tuning for ~12M-row relations table.

        Smaller bloom budget — relations are few, files are wide (members
        column is bulky), spending lots of bytes on the bloom is wasteful.
        """
        return cls(
            distribution_mode="range",
            bloom_filter_enabled=True,
            bloom_filter_max_bytes=262_144,
        )

    @classmethod
    def production(cls) -> "TableConfig":
        """Backwards-compatible default — same shape as the ways factory."""
        return cls.ways_production()

    def _main_table_props(self) -> str:
        lines = [
            "'format'='parquet'",
            "'write.parquet.compression-codec'='snappy'",
            "'format-version'='2'",
            "'write.metadata.compression-codec'='none'",
            f"'write.distribution-mode'='{self.distribution_mode}'",
            "'write.delete.mode'='merge-on-read'",
            "'write.update.mode'='merge-on-read'",
            "'write.merge.mode'='merge-on-read'",
        ]
        if self.distribution_mode == "range":
            lines.append("'write.sort-order'='id ASC'")
        if self.bloom_filter_enabled:
            lines.append("'write.parquet.bloom-filter-enabled.column.id'='true'")
            if self.bloom_filter_max_bytes:
                lines.append(
                    f"'write.parquet.bloom-filter-max-bytes.column.id'='{self.bloom_filter_max_bytes}'"
                )
        return ",\n            ".join(lines)

    def _index_table_props(self, sort_col: str) -> str:
        lines = [
            "'format'='parquet'",
            "'write.parquet.compression-codec'='snappy'",
            "'format-version'='2'",
            f"'write.distribution-mode'='{self.distribution_mode}'",
        ]
        if self.distribution_mode == "range":
            lines.append(f"'write.sort-order'='{sort_col} ASC'")
        if self.bloom_filter_enabled:
            lines.append(f"'write.parquet.bloom-filter-enabled.column.{sort_col}'='true'")
            if self.bloom_filter_max_bytes:
                lines.append(
                    f"'write.parquet.bloom-filter-max-bytes.column.{sort_col}'"
                    f"='{self.bloom_filter_max_bytes}'"
                )
        return ",\n            ".join(lines)


@dataclass
class KryptonDatabase:
    """Derives all Krypton table names from a catalog + database pair.

    Eliminates the repeated ``f"{CATALOG}.{DB_NAME}.nodes"`` pattern and
    guarantees the table-name set stays consistent across scripts.
    """

    catalog: str
    db_name: str

    def _fqn(self, table: str) -> str:
        return f"{self.catalog}.{self.db_name}.{table}"

    @property
    def nodes(self) -> str:
        return self._fqn("nodes")

    @property
    def ways(self) -> str:
        return self._fqn("ways")

    @property
    def relations(self) -> str:
        return self._fqn("relations")

    @property
    def node_to_ways(self) -> str:
        return self._fqn("node_to_ways")

    @property
    def way_to_relations(self) -> str:
        return self._fqn("way_to_relations")

    @property
    def node_to_relations(self) -> str:
        return self._fqn("node_to_relations")

    @property
    def relation_to_relations(self) -> str:
        return self._fqn("relation_to_relations")

    @property
    def osc_archive(self) -> str:
        return self._fqn("osc_changes")


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Return True if the Iceberg table exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def load_with_geom(spark: SparkSession, table_name: str, view_name: str):
    """Read a per-type table, decoding WKB geometry back to Sedona geom.

    The caller passes the per-type table they want (nodes / ways / relations).
    All native columns plus a synthesized ``geom`` column are exposed on the
    resulting view.
    """
    spark.sql(f"""
        SELECT *, ST_GeomFromWKB(geometry) AS geom
        FROM {table_name}
    """).createOrReplaceTempView(view_name)


def _ensure_db(spark: SparkSession, table_name: str):
    parts = table_name.split(".")
    if len(parts) >= 2:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {'.'.join(parts[:-1])}")


def _create_typed_table(
    spark: SparkSession,
    table_name: str,
    schema_sql: str,
    table_location: Optional[str],
    config: TableConfig,
) -> None:
    """Drop+create an Iceberg per-type table with the given column schema."""
    _ensure_db(spark, table_name)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    location_clause = (
        f"LOCATION '{table_location}'"
        if table_location and table_location.startswith("s3://")
        else ""
    )
    spark.sql(f"""
        CREATE TABLE {table_name} (
            {schema_sql}
        )
        USING iceberg
        {location_clause}
        TBLPROPERTIES (
            {config._main_table_props()}
        )
    """)


def create_nodes_table(
    spark: SparkSession,
    table_name: str,
    table_location: Optional[str] = None,
    config: Optional[TableConfig] = None,
):
    """Create the krypton nodes Iceberg table (idempotent: drops first).

    Nodes are a single point each — ``lat`` / ``lon`` already define the
    geometry, so there's no separate ``bbox`` column. Spatial filters use
    ``lat`` / ``lon`` directly.
    """
    if config is None:
        config = TableConfig()
    schema = """id                    BIGINT,
            version               BIGINT,
            timestamp             TIMESTAMP,
            changeset             BIGINT,
            uid                   BIGINT,
            user                  STRING,
            tags                  MAP<STRING, STRING>,
            lat                   DOUBLE,
            lon                   DOUBLE,
            latest_ts             TIMESTAMP,
            additional_changesets ARRAY<BIGINT>,
            geometry              BINARY"""
    _create_typed_table(spark, table_name, schema, table_location, config)
    logger.info("Created nodes table %s (config=%s)", table_name, config)


def create_ways_table(
    spark: SparkSession,
    table_name: str,
    table_location: Optional[str] = None,
    config: Optional[TableConfig] = None,
):
    """Create the krypton ways Iceberg table (idempotent: drops first)."""
    if config is None:
        config = TableConfig()
    schema = """id                    BIGINT,
            version               BIGINT,
            timestamp             TIMESTAMP,
            changeset             BIGINT,
            uid                   BIGINT,
            user                  STRING,
            tags                  MAP<STRING, STRING>,
            refs                  ARRAY<BIGINT>,
            latest_ts             TIMESTAMP,
            additional_changesets ARRAY<BIGINT>,
            geometry              BINARY,
            bbox                  STRUCT<xmin: FLOAT, xmax: FLOAT, ymin: FLOAT, ymax: FLOAT>"""
    _create_typed_table(spark, table_name, schema, table_location, config)
    logger.info("Created ways table %s (config=%s)", table_name, config)


def create_relations_table(
    spark: SparkSession,
    table_name: str,
    table_location: Optional[str] = None,
    config: Optional[TableConfig] = None,
):
    """Create the krypton relations Iceberg table (idempotent: drops first)."""
    if config is None:
        config = TableConfig()
    schema = """id                    BIGINT,
            version               BIGINT,
            timestamp             TIMESTAMP,
            changeset             BIGINT,
            uid                   BIGINT,
            user                  STRING,
            tags                  MAP<STRING, STRING>,
            members               ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
            latest_ts             TIMESTAMP,
            additional_changesets ARRAY<BIGINT>,
            geometry              BINARY,
            bbox                  STRUCT<xmin: FLOAT, xmax: FLOAT, ymin: FLOAT, ymax: FLOAT>"""
    _create_typed_table(spark, table_name, schema, table_location, config)
    logger.info("Created relations table %s (config=%s)", table_name, config)


# ---------------------------------------------------------------------------
# Index tables: node_to_ways, way_to_relations, node_to_relations,
# relation_to_relations
# ---------------------------------------------------------------------------


def create_index_tables(
    spark: SparkSession,
    node_to_ways: str,
    way_to_relations: str,
    node_to_relations: Optional[str] = None,
    relation_to_relations: Optional[str] = None,
    config: Optional[TableConfig] = None,
):
    """Create the reverse-index tables used for dirty-set computation.

    Three indexes mirror the three member-types a relation can carry: nodes,
    ways, and other relations. Together they give 1-level widening coverage
    for every parent feature whose child might have changed.
    """
    if config is None:
        config = TableConfig()
    indexes = [node_to_ways, way_to_relations]
    if node_to_relations is not None:
        indexes.append(node_to_relations)
    if relation_to_relations is not None:
        indexes.append(relation_to_relations)
    for idx in indexes:
        _ensure_db(spark, idx)

    specs = [
        (node_to_ways, "node_id BIGINT, way_id BIGINT", "node_id"),
        (way_to_relations, "way_id BIGINT, relation_id BIGINT", "way_id"),
    ]
    if node_to_relations is not None:
        specs.append(
            (node_to_relations, "node_id BIGINT, relation_id BIGINT", "node_id")
        )
    if relation_to_relations is not None:
        specs.append(
            (
                relation_to_relations,
                "child_relation_id BIGINT, parent_relation_id BIGINT",
                "child_relation_id",
            )
        )

    for idx, cols, sort_col in specs:
        spark.sql(f"DROP TABLE IF EXISTS {idx}")
        spark.sql(f"""
            CREATE TABLE {idx} ({cols})
            USING iceberg
            TBLPROPERTIES (
                {config._index_table_props(sort_col)}
            )
        """)


def populate_node_to_ways(spark: SparkSession, ways_table: str, node_to_ways: str):
    """Bulk-populate node_to_ways from the ways table."""
    spark.sql(f"""
        INSERT INTO {node_to_ways}
        SELECT explode(refs) AS node_id, id AS way_id
        FROM {ways_table}
        WHERE refs IS NOT NULL
    """)


def populate_way_to_relations(spark: SparkSession, relations_table: str, way_to_relations: str):
    """Bulk-populate way_to_relations from the relations table."""
    from .geometry.relations import GEOMETRY_RELATION_TYPES
    types = ", ".join(f"'{t}'" for t in GEOMETRY_RELATION_TYPES)
    spark.sql(f"""
        INSERT INTO {way_to_relations}
        SELECT member.ref AS way_id, id AS relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {relations_table}
            WHERE tags['type'] IN ({types})
        )
        WHERE member.type = 'way'
    """)


def refresh_node_to_ways(spark: SparkSession, ways_table: str, node_to_ways: str, dirty_way_ids: str):
    """Delete and re-insert index entries for dirty ways after a MERGE."""
    spark.sql(f"DELETE FROM {node_to_ways} WHERE way_id IN (SELECT id FROM {dirty_way_ids})")
    spark.sql(f"""
        INSERT INTO {node_to_ways}
        SELECT explode(refs) AS node_id, id AS way_id
        FROM {ways_table}
        WHERE id IN (SELECT id FROM {dirty_way_ids})
    """)


def refresh_way_to_relations(spark: SparkSession, relations_table: str, way_to_relations: str, dirty_rel_ids: str):
    """Delete and re-insert index entries for dirty relations after a MERGE."""
    from .geometry.relations import GEOMETRY_RELATION_TYPES
    types = ", ".join(f"'{t}'" for t in GEOMETRY_RELATION_TYPES)
    spark.sql(f"DELETE FROM {way_to_relations} WHERE relation_id IN (SELECT id FROM {dirty_rel_ids})")
    spark.sql(f"""
        INSERT INTO {way_to_relations}
        SELECT member.ref AS way_id, id AS relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {relations_table}
            WHERE id IN (SELECT id FROM {dirty_rel_ids})
                  AND tags['type'] IN ({types})
        )
        WHERE member.type = 'way'
    """)


def populate_node_to_relations(spark: SparkSession, relations_table: str, node_to_relations: str):
    """Bulk-populate node_to_relations from the relations table.

    Indexes ALL relations (no tag filter) so future relation types that carry
    node members work without a re-index migration. Most relations have few
    node members, so the table stays small relative to way_to_relations.
    """
    spark.sql(f"""
        INSERT INTO {node_to_relations}
        SELECT member.ref AS node_id, id AS relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {relations_table}
            WHERE members IS NOT NULL
        )
        WHERE member.type = 'node'
    """)


def refresh_node_to_relations(
    spark: SparkSession, relations_table: str, node_to_relations: str, dirty_rel_ids: str
):
    """Delete and re-insert node_to_relations entries for dirty relations after a MERGE."""
    spark.sql(
        f"DELETE FROM {node_to_relations} WHERE relation_id IN (SELECT id FROM {dirty_rel_ids})"
    )
    spark.sql(f"""
        INSERT INTO {node_to_relations}
        SELECT member.ref AS node_id, id AS relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {relations_table}
            WHERE id IN (SELECT id FROM {dirty_rel_ids})
                  AND members IS NOT NULL
        )
        WHERE member.type = 'node'
    """)


def populate_relation_to_relations(
    spark: SparkSession, relations_table: str, relation_to_relations: str
):
    """Bulk-populate relation_to_relations (child \u2192 parent edges).

    Lets dirty-set widening cover the case where a sub-relation is edited
    and we need to also rebuild every parent relation that contains it as a
    member. Indexes ALL relations regardless of tag-type \u2014 same
    rationale as node_to_relations.
    """
    spark.sql(f"""
        INSERT INTO {relation_to_relations}
        SELECT member.ref AS child_relation_id, id AS parent_relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {relations_table}
            WHERE members IS NOT NULL
        )
        WHERE member.type = 'relation'
    """)


def refresh_relation_to_relations(
    spark: SparkSession,
    relations_table: str,
    relation_to_relations: str,
    dirty_rel_ids: str,
):
    """Delete and re-insert relation_to_relations entries for dirty parents.

    Note: this refreshes edges where the parent (the dirty relation we just
    merged) sits. It does NOT refresh edges where this dirty relation is the
    child of someone else \u2014 those parent edges remain valid (members
    didn't change just because the child relation got edited).
    """
    spark.sql(
        f"DELETE FROM {relation_to_relations} "
        f"WHERE parent_relation_id IN (SELECT id FROM {dirty_rel_ids})"
    )
    spark.sql(f"""
        INSERT INTO {relation_to_relations}
        SELECT member.ref AS child_relation_id, id AS parent_relation_id
        FROM (
            SELECT id, explode(members) AS member
            FROM {relations_table}
            WHERE id IN (SELECT id FROM {dirty_rel_ids})
                  AND members IS NOT NULL
        )
        WHERE member.type = 'relation'
    """)


# ---------------------------------------------------------------------------
# OSC archive table: a queryable history of every applied OSC change.
# Also the home of the ``last-applied-osc-sequence`` / ``current-osc-file``
# table properties — the natural single source of truth for "what state the
# krypton database is in".
# ---------------------------------------------------------------------------


def create_osc_archive_table(
    spark: SparkSession,
    archive_table: str,
    table_location: Optional[str] = None,
    config: Optional[TableConfig] = None,
):
    """Create the OSC archive table (idempotent: drops first to guarantee schema).

    One row per OSC change record, partitioned by ``sequence`` so that queries
    like ``WHERE sequence = 4776`` prune to a single partition.
    """
    if config is None:
        config = TableConfig()
    _ensure_db(spark, archive_table)
    spark.sql(f"DROP TABLE IF EXISTS {archive_table}")

    location_clause = (
        f"LOCATION '{table_location}'"
        if table_location and table_location.startswith("s3://")
        else ""
    )

    spark.sql(f"""
        CREATE TABLE {archive_table} (
            sequence    BIGINT,
            osc_file    STRING,
            applied_at  TIMESTAMP,
            id          BIGINT,
            type        STRING,
            op          STRING,
            version     BIGINT,
            timestamp   TIMESTAMP,
            changeset   BIGINT,
            uid         BIGINT,
            user        STRING,
            tags        MAP<STRING, STRING>,
            lat         DOUBLE,
            lon         DOUBLE,
            refs        ARRAY<BIGINT>,
            members     ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>
        )
        USING iceberg
        PARTITIONED BY (sequence)
        {location_clause}
        TBLPROPERTIES (
            {config._main_table_props()}
        )
    """)
    logger.info("Created OSC archive table %s (config=%s)", archive_table, config)


def append_osc_archive(
    spark: SparkSession,
    archive_table: str,
    osc_view: str,
    sequence: Optional[int],
    osc_file: str,
):
    """Append the records from ``osc_view`` to the archive table for one OSC.

    ``osc_view`` is expected to expose the OSC-record columns (matching the
    OSC_SCHEMA shape: id, type, op, version, timestamp, uid, user, changeset,
    tags, lat, lon, refs, members). The ``sequence``, ``osc_file``, and
    ``applied_at`` meta columns are added here.
    """
    seq_expr = "CAST(NULL AS BIGINT)" if sequence is None else f"CAST({sequence} AS BIGINT)"
    file_lit = osc_file.replace("'", "''")
    spark.sql(f"""
        INSERT INTO {archive_table}
        SELECT
            {seq_expr}                          AS sequence,
            CAST('{file_lit}' AS STRING)        AS osc_file,
            current_timestamp()                 AS applied_at,
            CAST(id AS BIGINT)                  AS id,
            CAST(type AS STRING)                AS type,
            CAST(op AS STRING)                  AS op,
            CAST(version AS BIGINT)             AS version,
            CAST(timestamp AS TIMESTAMP)        AS timestamp,
            CAST(changeset AS BIGINT)           AS changeset,
            CAST(uid AS BIGINT)                 AS uid,
            user,
            tags,
            lat,
            lon,
            refs,
            members
        FROM {osc_view}
    """)


# ---------------------------------------------------------------------------
# Counts / max timestamp across the three per-type tables
# ---------------------------------------------------------------------------


def get_table_count(
    spark: SparkSession,
    nodes_table: str,
    ways_table: str,
    relations_table: str,
) -> dict:
    """Return ``{'node': N, 'way': W, 'relation': R}`` counts."""
    counts = {}
    for kind, name in (
        ("node", nodes_table),
        ("way", ways_table),
        ("relation", relations_table),
    ):
        rows = spark.sql(f"SELECT COUNT(*) AS n FROM {name}").collect()
        counts[kind] = rows[0]["n"]
    return counts


def get_table_max_timestamp(
    spark: SparkSession,
    nodes_table: str,
    ways_table: str,
    relations_table: str,
) -> Optional[datetime]:
    """Return the newest ``timestamp`` across all three per-type tables, or None."""
    union = " UNION ALL ".join(
        f"SELECT MAX(timestamp) AS ts FROM {t}"
        for t in (nodes_table, ways_table, relations_table)
    )
    row = spark.sql(f"SELECT MAX(ts) AS max_ts FROM ({union})").collect()[0]
    return row["max_ts"]


# ---------------------------------------------------------------------------
# Sequence stamping — every per-type table and the archive carry their own
# stamp so a partially-completed OSC apply can be resumed.
#
# Properties:
#   * ``last-applied-osc-sequence`` — on each per-type table; means
#     "this table has been brought to the state implied by applying every
#     OSC up to and including this sequence."
#   * ``last-archived-osc-sequence`` — on the archive table; means "every
#     OSC up to and including this sequence has had its records appended
#     to the archive partition."
#   * ``current-osc-file`` — on the archive table; the OSC file being
#     applied right now (cleared / overwritten by the next apply).
#
# Database-level "current at N" = MIN of last-applied across nodes/ways/
# relations. If they disagree, an apply was interrupted; the next call to
# ``apply_osc`` will pick up where it left off and only re-do the missing
# per-type sections.
# ---------------------------------------------------------------------------

_OSC_SEQ_PROPERTY = "last-applied-osc-sequence"
_OSC_ARCHIVED_PROPERTY = "last-archived-osc-sequence"
_OSC_FILE_PROPERTY = "current-osc-file"


def _read_seq_property(spark: SparkSession, table_name: str, prop: str) -> Optional[int]:
    """Read an integer-valued table property. Returns None if not set."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_name} ('{prop}')").collect()
    if not rows:
        return None
    value = rows[0][1]
    # Spark returns `Table {table} does not have property: {prop}` when unset.
    if value.startswith("Table"):
        return None
    return int(value)


def get_last_applied_sequence(spark: SparkSession, table_name: str) -> Optional[int]:
    """Return the ``last-applied-osc-sequence`` on any per-type table, or None."""
    return _read_seq_property(spark, table_name, _OSC_SEQ_PROPERTY)


def set_last_applied_sequence(spark: SparkSession, table_name: str, seq: int) -> None:
    """Stamp a per-type table with the last-applied OSC sequence number."""
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{_OSC_SEQ_PROPERTY}' = '{seq}')"
    )


def get_last_archived_sequence(spark: SparkSession, archive_table: str) -> Optional[int]:
    """Return the ``last-archived-osc-sequence`` on the archive table, or None."""
    return _read_seq_property(spark, archive_table, _OSC_ARCHIVED_PROPERTY)


def set_last_archived_sequence(spark: SparkSession, archive_table: str, seq: int) -> None:
    """Stamp the archive table with the highest sequence whose records are archived."""
    spark.sql(
        f"ALTER TABLE {archive_table} SET TBLPROPERTIES ('{_OSC_ARCHIVED_PROPERTY}' = '{seq}')"
    )


def set_current_osc_file(spark: SparkSession, archive_table: str, filename: str) -> None:
    """Stamp the archive table with the OSC file currently being applied."""
    spark.sql(
        f"ALTER TABLE {archive_table} SET TBLPROPERTIES ('{_OSC_FILE_PROPERTY}' = '{filename}')"
    )


def get_min_applied_sequence(
    spark: SparkSession,
    nodes_table: str,
    ways_table: str,
    relations_table: str,
) -> Optional[int]:
    """Database-wide last-applied = MIN across the three per-type tables.

    If any per-type table has never had an OSC applied (property is None),
    returns None — the caller falls back to estimating from MAX(timestamp).
    """
    seqs = [
        get_last_applied_sequence(spark, t)
        for t in (nodes_table, ways_table, relations_table)
    ]
    if any(s is None for s in seqs):
        return None
    return min(s for s in seqs if s is not None)


# ---------------------------------------------------------------------------
# MERGE / DELETE
# ---------------------------------------------------------------------------

def merge_upsert_delete(
    spark: SparkSession,
    table_name: str,
    upsert_view: str,
    delete_view: str,
):
    """Upsert + delete in a single MERGE, halving the Iceberg operations.

    Delete rows are padded with NULLs and routed to the DELETE clause via
    a ``version IS NULL`` sentinel (real upserts always have a version).

    Deletes take precedence: if an ID appears in both the upsert and delete
    views (e.g. a node-widened way that the OSC also deletes), the upsert
    side is excluded so each ID appears exactly once in the MERGE source.
    """
    cols = spark.table(table_name).columns
    null_selects = ", ".join(
        "d.id" if c == "id" else f"NULL AS {c}"
        for c in cols
    )
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING (
            SELECT * FROM {upsert_view}
            WHERE id NOT IN (SELECT id FROM {delete_view})
            UNION ALL
            SELECT {null_selects} FROM {delete_view} d
        ) s
        ON t.id = s.id
        WHEN MATCHED AND s.version IS NULL THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s.version IS NOT NULL THEN INSERT *
    """)
