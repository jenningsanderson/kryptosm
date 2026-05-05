"""
Command-line interface for KryptOSM.
"""

import argparse
import sys


def create_parser():
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="KryptOSM Utility - Process OSM data into Iceberg tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initial load with Hadoop catalog
  kryptosm --mode init \\
    --input-path s3://bucket/osm/parquet/ \\
    --table-name osm.geometry \\
    --table-location s3://bucket/iceberg/osm/ \\
    --catalog-type hadoop \\
    --catalog-warehouse s3://bucket/iceberg/warehouse/

  # Initial load with Glue catalog
  kryptosm --mode init \\
    --input-path s3://bucket/osm/parquet/ \\
    --table-name glue_catalog.osm.geometry \\
    --table-location s3://bucket/iceberg/osm/ \\
    --catalog-type glue

  # Apply OSC update (download)
  kryptosm --mode update \\
    --table-name glue_catalog.osm.geometry \\
    --download-osc --osc-date 2024-01-15 \\
    --catalog-type glue

  # Apply OSC update (local files)
  kryptosm --mode update \\
    --table-name osm.geometry \\
    --osc-path s3://bucket/osc/2024-01-15/ \\
    --catalog-type hadoop \\
    --catalog-warehouse s3://bucket/iceberg/warehouse/
        """,
    )

    # Mode
    parser.add_argument(
        "--mode",
        choices=["init", "update"],
        required=True,
        help="Operation mode: 'init' for initial load, 'update' for OSC updates",
    )

    # Input/Output paths
    parser.add_argument("--input-path", help="Path to OSM Parquet data (required for init mode)")
    parser.add_argument(
        "--table-name",
        required=True,
        help="Iceberg table name (e.g., 'osm.geometry' or 'glue_catalog.osm.geometry')",
    )
    parser.add_argument(
        "--table-location",
        help="S3 path for Iceberg table storage (required for init mode)",
    )

    # OSC options
    parser.add_argument(
        "--osc-path",
        help="Path to OSC file(s) or directory (for update mode with local files)",
    )
    parser.add_argument(
        "--download-osc",
        action="store_true",
        help="Download OSC from OSM replication (for update mode)",
    )
    parser.add_argument(
        "--osc-date",
        help="Date for OSC download (YYYY-MM-DD, required with --download-osc)",
    )

    # Catalog configuration
    parser.add_argument(
        "--catalog-type",
        choices=["glue", "hadoop"],
        default="hadoop",
        help="Iceberg catalog type (default: hadoop)",
    )
    parser.add_argument(
        "--catalog-warehouse",
        help="Warehouse path for Hadoop catalog (required for hadoop catalog)",
    )
    parser.add_argument(
        "--catalog-name",
        default="glue_catalog",
        help="Catalog name for Glue (default: glue_catalog)",
    )

    # Spark configuration
    parser.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )

    return parser


def validate_args(args):
    """Validate command line arguments."""
    errors = []

    # Mode-specific validation
    if args.mode == "init":
        if not args.input_path:
            errors.append("--input-path is required for init mode")
        if not args.table_location:
            errors.append("--table-location is required for init mode")

    if args.mode == "update":
        if not args.osc_path and not args.download_osc:
            errors.append("Either --osc-path or --download-osc is required for update mode")
        if args.download_osc and not args.osc_date:
            errors.append("--osc-date is required with --download-osc")

    # Catalog validation
    if args.catalog_type == "hadoop" and not args.catalog_warehouse:
        if "glue_catalog" not in args.table_name:
            errors.append("--catalog-warehouse is required for hadoop catalog")

    if errors:
        for error in errors:
            print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)


def parse_args(argv=None):
    """Parse and validate command line arguments."""
    parser = create_parser()
    args = parser.parse_args(argv)
    validate_args(args)
    return args


def main():
    """Entry point for CLI."""
    args = parse_args()
    print(f"Mode: {args.mode}")
    print(f"Table: {args.table_name}")
    # The actual processing is done in main.py
    return args


if __name__ == "__main__":
    main()
