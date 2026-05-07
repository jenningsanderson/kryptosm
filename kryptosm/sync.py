"""
Standalone CLI for keeping a local OSC download directory in sync with a
Geofabrik replication feed.

Reads the current table timestamp from the Iceberg table, determines
which Geofabrik OSC files are needed, and downloads them.

Usage::

    # Show what's pending
    kryptosm-sync status \\
        --table-name hadoop_catalog.test_db.osm \\
        --catalog-warehouse /tmp/warehouse

    # Download all pending OSC files (up to now)
    kryptosm-sync sync \\
        --table-name hadoop_catalog.test_db.osm \\
        --catalog-warehouse /tmp/warehouse

    # Download up to a specific date
    kryptosm-sync sync \\
        --table-name hadoop_catalog.test_db.osm \\
        --catalog-warehouse /tmp/warehouse \\
        --target-date 2026-03-01

Designed to run from cron or a scheduler independently of the Spark job.
The Spark job later applies the downloaded ``.osc.gz`` files via
``--osc-path``.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from osmium.replication.server import ReplicationServer

from .iceberg import get_table_max_timestamp, table_exists
from .replication import (
    DC_REPLICATION_URL,
    pending_sequences,
    resolve_target_sequence,
    sync,
)
from .spark import create_spark_session


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="kryptosm-sync",
        description="Sync Geofabrik OSC replication files for an Iceberg table.",
    )

    parser.add_argument(
        "--replication-url",
        default=DC_REPLICATION_URL,
        help="Geofabrik replication base URL (default: DC)",
    )

    # Iceberg / catalog config (shared across subcommands)
    parser.add_argument(
        "--table-name",
        required=True,
        help="Iceberg table name (e.g. hadoop_catalog.test_db.osm)",
    )
    parser.add_argument(
        "--catalog-type",
        choices=["hadoop", "glue"],
        default="hadoop",
        help="Iceberg catalog type (default: hadoop)",
    )
    parser.add_argument(
        "--catalog-warehouse",
        help="Warehouse path (required for hadoop catalog)",
    )
    parser.add_argument(
        "--catalog-name",
        default="glue_catalog",
        help="Catalog name for Glue (default: glue_catalog)",
    )
    parser.add_argument(
        "--spark-master",
        default="local[1]",
        help="Spark master (default: local[1])",
    )

    sub = parser.add_subparsers(dest="command")
    sub.required = True

    # --- status -------------------------------------------------------------
    sub.add_parser("status", help="Show table timestamp, remote head, and pending count")

    # --- sync ---------------------------------------------------------------
    sync_p = sub.add_parser("sync", help="Download pending OSC files")
    sync_p.add_argument(
        "--download-dir",
        default="osc_files",
        help="Directory to save .osc.gz files (default: osc_files/)",
    )
    sync_p.add_argument(
        "--target-date",
        help="Sync up to this date (YYYY-MM-DD). Default: latest available.",
    )

    return parser.parse_args(argv)


def _read_table_timestamp(args) -> datetime:
    """Spin up a minimal Spark session, read MAX(timestamp), shut down."""
    spark = create_spark_session(
        app_name="kryptosm-sync",
        master=args.spark_master,
        catalog_type=args.catalog_type,
        catalog_name=args.catalog_name,
        warehouse=args.catalog_warehouse,
    )
    try:
        if not table_exists(spark, args.table_name):
            print(f"Error: table {args.table_name} does not exist.", file=sys.stderr)
            sys.exit(1)
        ts = get_table_max_timestamp(spark, args.table_name)
        if ts is None:
            print("Error: table is empty (no rows).", file=sys.stderr)
            sys.exit(1)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    finally:
        spark.stop()


def cmd_status(args):
    base_url = args.replication_url
    table_ts = _read_table_timestamp(args)

    with ReplicationServer(base_url) as server:
        remote = server.get_state_info()
        if remote is None:
            print("Error: could not reach replication server.", file=sys.stderr)
            sys.exit(1)

        table_seq = server.timestamp_to_sequence(table_ts)

    print("Table state")
    print(f"  timestamp: {table_ts.isoformat()}")
    print(f"  sequence:  {table_seq}")

    print("\nRemote head")
    print(f"  sequence:  {remote.sequence}")
    print(f"  timestamp: {remote.timestamp.isoformat()}")

    if table_seq is not None:
        seqs = pending_sequences(table_seq, remote.sequence)
        print(f"\nPending: {len(seqs)} file(s)")
        if seqs:
            print(f"  range: {seqs[0]} .. {seqs[-1]}")


def cmd_sync(args):
    base_url = args.replication_url
    target_date = None
    if args.target_date:
        target_date = datetime.strptime(args.target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    table_ts = _read_table_timestamp(args)

    with ReplicationServer(base_url) as server:
        remote = server.get_state_info()
        if remote is None:
            print("Error: could not reach replication server.", file=sys.stderr)
            sys.exit(1)

        table_seq = server.timestamp_to_sequence(table_ts)
        if table_seq is None:
            print("Error: could not map table timestamp to a sequence.", file=sys.stderr)
            sys.exit(1)

        target_seq = resolve_target_sequence(server, remote.sequence, target_date)

    seqs = pending_sequences(table_seq, target_seq)
    if not seqs:
        print("Already up to date.")
        return

    print(f"Table at sequence {table_seq} ({table_ts.isoformat()})")
    print(f"Downloading {len(seqs)} file(s): {seqs[0]} .. {seqs[-1]}")
    paths = sync(
        table_timestamp=table_ts,
        download_dir=args.download_dir,
        base_url=base_url,
        target_date=target_date,
    )
    print(f"Done. {len(paths)} file(s) saved to {args.download_dir}/")


def main(argv=None):
    args = _parse_args(argv)
    if args.command == "status":
        cmd_status(args)
    elif args.command == "sync":
        cmd_sync(args)


if __name__ == "__main__":
    main()
