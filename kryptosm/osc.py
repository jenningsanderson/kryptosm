"""
OSC (OpenStreetMap Change) ingestion.

OSC files are XML, so parsing is Python. Once parsed, everything is a Spark
DataFrame and downstream logic stays in SQL.
"""

import gzip
from datetime import datetime, timezone
from functools import reduce
from posixpath import join as urljoin
from typing import List
from xml.etree import ElementTree

import pyspark.sql.types as T
import requests
from pyspark.sql import DataFrame, SparkSession

# Reference point for the OSC daily-replication sequence number.
_REF_DATE = datetime(2024, 3, 23, tzinfo=timezone.utc)
_REF_SEQ = 4210
_REPLICATION_BASE = "https://planet.openstreetmap.org/replication/day/"

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


def osc_dedup(spark: SparkSession, osc_view: str, result_view: str):
    """
    Keep the latest version per (id, type) from the OSC stream.

    Also casts the OSC's ISO-string timestamps into TIMESTAMP so that all
    downstream views match the Iceberg table's TIMESTAMP columns. This is
    the single funnel for OSC data, so doing the cast here means every
    consumer (`all_dirty_ways`, `apply_osc_with_geometry`, `build_*`,
    ...) sees the canonical type.

    Input view (`osc_view`) columns:
        id, type, op, version, timestamp (STRING), uid, user, changeset,
        tags, lat, lon, refs, members, latest_ts (STRING)
    Output view (`result_view`) columns:
        same shape, but `timestamp` and `latest_ts` are TIMESTAMP.
    """
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


def get_osc_day_sequence_number(publish_date: str) -> int:
    """Convert a YYYY-MM-DD date to the OSM daily-replication sequence number."""
    target = datetime.strptime(publish_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return _REF_SEQ + (target - _REF_DATE).days


# ----------------------------------------------------------------------------
# Reading OSC into a DataFrame
# ----------------------------------------------------------------------------


def _build_osc_url(sequence_number: int) -> str:
    s = str(sequence_number).zfill(9)
    return urljoin(_REPLICATION_BASE, f"{s[:3]}/{s[3:6]}/{s[6:9]}.osc.gz")


def _fetch_xml(file_path: str = None, sequence_number: int = None) -> bytes:
    """Return raw OSC XML bytes from either a local file or the replication server."""
    if file_path:
        if file_path.startswith("s3://"):
            raise NotImplementedError("S3 OSC files not yet implemented")
        try:
            with gzip.GzipFile(file_path, "rb") as f:
                return f.read()
        except OSError:
            with open(file_path, "rb") as f:
                return f.read()

    response = requests.get(_build_osc_url(sequence_number))
    response.raise_for_status()
    return gzip.decompress(response.content)


def _parse_osc(xml_bytes: bytes) -> list:
    """Flatten OSC XML into a list of records (one row per change)."""
    records = []
    for action in ElementTree.fromstring(xml_bytes):
        op = action.tag  # create | modify | delete
        for element in action:
            elem_type = element.tag  # node | way | relation
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


def _osc_dataframe(
    spark: SparkSession, *, file_path: str = None, sequence_number: int = None
) -> DataFrame:
    return spark.createDataFrame(
        _parse_osc(_fetch_xml(file_path=file_path, sequence_number=sequence_number)),
        OSC_SCHEMA,
    )


def download_osc_to_dataframe(spark: SparkSession, publish_date: str) -> DataFrame:
    """Download the daily OSC for `publish_date` (YYYY-MM-DD) and return a DataFrame."""
    return _osc_dataframe(spark, sequence_number=get_osc_day_sequence_number(publish_date))


def read_osc_from_file(spark: SparkSession, file_path: str) -> DataFrame:
    """Read a local .osc or .osc.gz (or plain XML) into a DataFrame."""
    return _osc_dataframe(spark, file_path=file_path)


def read_osc_files(spark: SparkSession, file_paths: List[str]) -> DataFrame:
    """Read multiple OSC files and union them into a single DataFrame."""
    dfs = [_osc_dataframe(spark, file_path=p) for p in file_paths]
    return reduce(DataFrame.unionAll, dfs)


def read_osc_from_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read OSC data from pre-parsed Parquet files."""
    return spark.read.parquet(path)
