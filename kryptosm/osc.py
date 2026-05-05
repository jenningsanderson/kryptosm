"""
OSC (OpenStreetMap Change) file handling.
"""

from datetime import datetime, timezone
from gzip import GzipFile
from posixpath import join as urljoin
from xml.etree import ElementTree
from typing import Optional

import pyspark.sql.types as T
import requests
from pyspark.sql import SparkSession


def osc_dedup(spark: SparkSession, osc_view: str, result_view: str):
    """
    Deduplicate OSC data by keeping the latest version per id+type.

    Args:
        spark: Spark session
        osc_view: Name of the view containing OSC data
        result_view: Name of the view to create with deduplicated data
    """
    spark.sql(f"""
        SELECT
            id,
            type,
            max_by(op, version) AS op,
            max(version) AS version,
            max_by(timestamp, version) AS timestamp,
            max_by(uid, version) AS uid,
            max_by(user, version) AS user,
            max_by(changeset, version) AS changeset,
            max_by(tags, version) AS tags,
            max_by(lat, version) AS lat,
            max_by(lon, version) AS lon,
            max_by(refs, version) AS refs,
            max_by(members, version) AS members,
            max_by(timestamp, version) AS latest_ts
        FROM {osc_view}
        GROUP BY id, type
        """).createOrReplaceTempView(result_view)


def get_osc_day_sequence_number(publish_date: str) -> int:
    """
    Calculate OSC sequence number from date.
    Reference: 2024-03-23 = sequence 4210

    Args:
        publish_date: Date string in YYYY-MM-DD format

    Returns:
        Sequence number
    """
    ref_date = datetime(2024, 3, 23, tzinfo=timezone.utc)
    ref_seq = 4210
    target_date = datetime.strptime(publish_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days_diff = (target_date - ref_date).days
    return ref_seq + days_diff


class OSCData:
    """Handles OpenStreetMap replication files."""

    def __init__(self, file_path=None, frequency="day", sequence_number=None):
        self.frequency = frequency
        self.base_url = f"https://planet.openstreetmap.org/replication/{frequency}/"

        if file_path:
            self.file_path = file_path
            self.data = self._read_osc_data()
        elif sequence_number:
            self.sequence_number = sequence_number
            self.data = self._read_osc_data()
        else:
            raise ValueError("Either file_path or sequence_number must be provided")

    def _build_osc_url(self, sequence_number):
        """Build URL for OSC file."""
        seq_str = str(sequence_number).zfill(9)
        return urljoin(self.base_url, f"{seq_str[:3]}/{seq_str[3:6]}/{seq_str[6:9]}.osc.gz")

    def _read_osc_data(self):
        """Download and parse OSC data."""
        if hasattr(self, "file_path"):
            # Read from local file
            if self.file_path.startswith("s3://"):
                # TODO: Implement S3 reading
                raise NotImplementedError("S3 OSC files not yet implemented")
            else:
                with GzipFile(self.file_path, "rb") as f:
                    xml_content = f.read()
        else:
            # Download from URL
            url = self._build_osc_url(self.sequence_number)
            print(f"Downloading OSC from: {url}")
            response = requests.get(url)
            response.raise_for_status()

            import gzip

            xml_content = gzip.decompress(response.content)

        return self._parse_osc_xml(xml_content)

    def _parse_osc_xml(self, xml_content):
        """Parse OSC XML into structured data."""
        data = {
            "create": {"node": [], "way": [], "relation": []},
            "modify": {"node": [], "way": [], "relation": []},
            "delete": {"node": [], "way": [], "relation": []},
        }

        root = ElementTree.fromstring(xml_content)

        for action in root:
            op = action.tag  # create, modify, or delete

            for element in action:
                elem_type = element.tag  # node, way, or relation
                elem_data = {
                    "id": int(element.attrib["id"]),
                    "version": int(element.attrib["version"]),
                    "timestamp": element.attrib["timestamp"],
                    "uid": int(element.attrib.get("uid", 0)),
                    "user": element.attrib.get("user", ""),
                    "changeset": int(element.attrib.get("changeset", 0)),
                    "tags": {},
                    "lat": None,
                    "lon": None,
                    "refs": None,
                    "members": None,
                }

                if elem_type == "node":
                    elem_data["lat"] = float(element.attrib["lat"])
                    elem_data["lon"] = float(element.attrib["lon"])
                elif elem_type == "way":
                    elem_data["refs"] = []
                    for child in element:
                        if child.tag == "nd":
                            elem_data["refs"].append(int(child.attrib["ref"]))
                        elif child.tag == "tag":
                            elem_data["tags"][child.attrib["k"]] = child.attrib["v"]
                elif elem_type == "relation":
                    elem_data["members"] = []
                    for child in element:
                        if child.tag == "member":
                            elem_data["members"].append(
                                {
                                    "type": child.attrib["type"],
                                    "ref": int(child.attrib["ref"]),
                                    "role": child.attrib.get("role", ""),
                                }
                            )
                        elif child.tag == "tag":
                            elem_data["tags"][child.attrib["k"]] = child.attrib["v"]

                # Handle tags for nodes and ways
                if elem_type in ("node", "way"):
                    for child in element:
                        if child.tag == "tag":
                            elem_data["tags"][child.attrib["k"]] = child.attrib["v"]

                data[op][elem_type].append(elem_data)

        return data


def download_osc_to_dataframe(spark: SparkSession, publish_date: str):
    """
    Download OSC data for a given date and convert to DataFrame.

    Args:
        spark: Spark session
        publish_date: Date string in YYYY-MM-DD format

    Returns:
        DataFrame with OSC data
    """
    seq_num = get_osc_day_sequence_number(publish_date)
    osc = OSCData(frequency="day", sequence_number=seq_num)

    # Convert to list of records
    records = []
    for op in ["create", "modify", "delete"]:
        for elem_type in ["node", "way", "relation"]:
            for elem in osc.data[op][elem_type]:
                record = {
                    "id": elem["id"],
                    "type": elem_type,
                    "op": op,
                    "version": elem["version"],
                    "timestamp": elem["timestamp"],
                    "uid": elem["uid"],
                    "user": elem["user"],
                    "changeset": elem["changeset"],
                    "tags": elem["tags"],
                    "lat": elem["lat"],
                    "lon": elem["lon"],
                    "refs": elem["refs"],
                    "members": elem["members"],
                    "latest_ts": elem["timestamp"],
                }
                records.append(record)

    # Create DataFrame
    schema = T.StructType(
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

    return spark.createDataFrame(records, schema)


def read_osc_from_parquet(spark: SparkSession, path: str):
    """
    Read OSC data from Parquet files.

    Args:
        spark: Spark session
        path: Path to OSC Parquet files

    Returns:
        DataFrame with OSC data
    """
    return spark.read.parquet(path)


def read_osc_from_file(spark: SparkSession, file_path: str):
    """
    Read OSC data from a .osc.gz file.

    Args:
        spark: Spark session
        file_path: Path to .osc.gz file

    Returns:
        DataFrame with OSC data
    """
    osc = OSCData(file_path=file_path)

    # Convert to list of records
    records = []
    for op in ["create", "modify", "delete"]:
        for elem_type in ["node", "way", "relation"]:
            for elem in osc.data[op][elem_type]:
                record = {
                    "id": elem["id"],
                    "type": elem_type,
                    "op": op,
                    "version": elem["version"],
                    "timestamp": elem["timestamp"],
                    "uid": elem["uid"],
                    "user": elem["user"],
                    "changeset": elem["changeset"],
                    "tags": elem["tags"],
                    "lat": elem["lat"],
                    "lon": elem["lon"],
                    "refs": elem["refs"],
                    "members": elem["members"],
                    "latest_ts": elem["timestamp"],
                }
                records.append(record)

    # Create DataFrame with schema
    schema = T.StructType(
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

    return spark.createDataFrame(records, schema)
