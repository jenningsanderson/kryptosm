"""
Tests for the replication sync utility.

The ``test_pending_*`` tests are pure-offline.  The ``test_live_*``
tests hit the Geofabrik DC replication server and are marked with
``@pytest.mark.integration`` so they can be skipped in CI.
"""

import os

import pytest
from osmium.replication.server import ReplicationServer

from kryptosm.replication import (
    DC_REPLICATION_URL,
    download_osc_file,
    pending_sequences,
    sync,
)

# ---------------------------------------------------------------------------
# Offline: pending_sequences
# ---------------------------------------------------------------------------


def test_pending_none_when_up_to_date():
    assert pending_sequences(100, 100) == []


def test_pending_range():
    assert pending_sequences(10, 13) == [11, 12, 13]


def test_pending_past_target():
    assert pending_sequences(50, 40) == []


def test_pending_one():
    assert pending_sequences(99, 100) == [100]


# ---------------------------------------------------------------------------
# Live: Geofabrik DC server
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_fetch_remote_state():
    with ReplicationServer(DC_REPLICATION_URL) as server:
        state = server.get_state_info()
    assert state is not None
    assert state.sequence > 0
    assert state.timestamp.tzinfo is not None


@pytest.mark.integration
def test_live_fetch_state_for_sequence():
    with ReplicationServer(DC_REPLICATION_URL) as server:
        latest = server.get_state_info()
        assert latest is not None
        specific = server.get_state_info(latest.sequence)
    assert specific is not None
    assert specific.sequence == latest.sequence


@pytest.mark.integration
def test_live_timestamp_to_sequence():
    with ReplicationServer(DC_REPLICATION_URL) as server:
        latest = server.get_state_info()
        assert latest is not None
        seq = server.timestamp_to_sequence(latest.timestamp)
    assert seq is not None
    assert seq <= latest.sequence


@pytest.mark.integration
def test_live_download_osc(tmp_path):
    with ReplicationServer(DC_REPLICATION_URL) as server:
        latest = server.get_state_info()
        assert latest is not None
        path = download_osc_file(server, latest.sequence, str(tmp_path))
    assert os.path.isfile(path)
    assert path.endswith(".osc.gz")
    assert os.path.getsize(path) > 0


@pytest.mark.integration
def test_live_download_idempotent(tmp_path):
    """Downloading the same sequence twice should skip the second time."""
    with ReplicationServer(DC_REPLICATION_URL) as server:
        latest = server.get_state_info()
        assert latest is not None
        path1 = download_osc_file(server, latest.sequence, str(tmp_path))
        size1 = os.path.getsize(path1)
        path2 = download_osc_file(server, latest.sequence, str(tmp_path))
    assert path1 == path2
    assert os.path.getsize(path2) == size1


@pytest.mark.integration
def test_live_sync_one_file(tmp_path):
    """Use a timestamp matching head-1 so sync downloads the remaining files."""
    dl_dir = str(tmp_path / "osc")

    with ReplicationServer(DC_REPLICATION_URL) as server:
        latest = server.get_state_info()
        assert latest is not None
        prev = server.get_state_info(latest.sequence - 1)
        assert prev is not None

    paths = sync(
        table_timestamp=prev.timestamp,
        download_dir=dl_dir,
        base_url=DC_REPLICATION_URL,
    )
    assert len(paths) >= 1
    for p in paths:
        assert os.path.isfile(p)


@pytest.mark.integration
def test_live_sync_already_current(tmp_path):
    """Sync when already at head returns an empty list."""
    dl_dir = str(tmp_path / "osc")

    with ReplicationServer(DC_REPLICATION_URL) as server:
        latest = server.get_state_info()
        assert latest is not None

    paths = sync(
        table_timestamp=latest.timestamp,
        download_dir=dl_dir,
        base_url=DC_REPLICATION_URL,
    )
    assert paths == []
