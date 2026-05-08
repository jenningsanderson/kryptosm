"""
Geofabrik OSC replication downloads.

Runs on top of ``pyosmium``'s :class:`ReplicationServer`.  Given either
a last-applied sequence number or the newest timestamp in the Iceberg
table, this module figures out what's available on the server and
downloads the gap.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional

from osmium.replication.server import ReplicationServer

DC_REPLICATION_URL = "https://download.geofabrik.de/north-america/us/district-of-columbia-updates/"


# ---------------------------------------------------------------------------
# Sequence math
# ---------------------------------------------------------------------------


def pending_sequences(last_applied: int, target: int) -> List[int]:
    """Return the ordered list of sequence numbers that need to be applied."""
    if last_applied >= target:
        return []
    return list(range(last_applied + 1, target + 1))


def resolve_target_sequence(
    server: ReplicationServer,
    remote_seq: int,
    target_date: Optional[datetime] = None,
) -> int:
    """Decide which sequence number to fetch up to."""
    if target_date is None:
        return remote_seq
    remote_state = server.get_state_info(remote_seq)
    if remote_state is None or target_date >= remote_state.timestamp:
        return remote_seq
    seq = server.timestamp_to_sequence(target_date)
    return seq if seq is not None else remote_seq


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------


def download_osc_file(server: ReplicationServer, seq: int, download_dir: str) -> str:
    """Download a single ``.osc.gz`` file.  Skips if already present."""
    os.makedirs(download_dir, exist_ok=True)
    local_path = os.path.join(download_dir, f"{seq}.osc.gz")
    if os.path.exists(local_path):
        return local_path

    data = server.get_diff_block(seq)
    tmp_path = local_path + ".tmp"
    with open(tmp_path, "wb") as f:
        f.write(data)
    os.rename(tmp_path, local_path)
    return local_path


# ---------------------------------------------------------------------------
# High-level fetch
# ---------------------------------------------------------------------------


def fetch_osc_files(
    download_dir: str,
    base_url: str = DC_REPLICATION_URL,
    last_applied_sequence: Optional[int] = None,
    table_timestamp: Optional[datetime] = None,
    target_date: Optional[datetime] = None,
) -> List[str]:
    """Download all pending OSC files and return their local paths.

    Supply *last_applied_sequence* when the table has a stored sequence
    property (fast, exact).  Falls back to *table_timestamp* which uses
    ``timestamp_to_sequence`` (slower, may re-download the last file).
    """
    if last_applied_sequence is None and table_timestamp is None:
        raise ValueError("Provide either last_applied_sequence or table_timestamp")

    with ReplicationServer(base_url) as server:
        remote_state = server.get_state_info()
        if remote_state is None:
            raise RuntimeError(f"Could not fetch remote state from {base_url}")

        if last_applied_sequence is not None:
            start_seq = last_applied_sequence
        else:
            start_seq = server.timestamp_to_sequence(table_timestamp)
            if start_seq is None:
                raise RuntimeError(
                    f"Could not map table timestamp {table_timestamp} to a sequence number"
                )

        target_seq = resolve_target_sequence(server, remote_state.sequence, target_date)
        seqs = pending_sequences(start_seq, target_seq)
        if not seqs:
            return []

        paths = []
        for seq in seqs:
            path = download_osc_file(server, seq, download_dir)
            paths.append(path)

    return paths
