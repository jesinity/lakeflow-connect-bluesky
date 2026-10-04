"""Characterize the published client's compatibility with Lakeflow rows."""

import json

import pytest

from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sources.bluesky.errors import ProtocolError
from databricks.labs.community_connector.sources.bluesky.library_adapter import snapshot_rows
from databricks.labs.community_connector.sources.bluesky.options import Options


@pytest.mark.parametrize(
    "settings",
    [
        {},
        {"collections": "app.bsky.feed.post"},
        {"kinds": "identity,account"},
        {"dids": "did:plc:unknown"},
        {"include_raw_payload": "false"},
        {"max_events_per_refresh": "2"},
    ],
)
def test_published_snapshot_matches_connector_rows(service, settings):
    options = Options.parse(settings, {})
    existing = BlueskyLakeflowConnect(options)
    old_rows, old_offset = existing.read_table("events", None, {})
    new_rows, new_cursor = snapshot_rows(options, options.starting_cursor, 64 * 1024 * 1024)

    old_rows = list(old_rows)
    assert new_cursor == old_offset["seq"]
    assert len(new_rows) == len(old_rows)
    for old, new in zip(old_rows, new_rows):
        assert old.keys() == new.keys()
        for key in old:
            if key in ("record", "event_payload"):
                assert (json.loads(old[key]) if old[key] is not None else None) == (
                    json.loads(new[key]) if new[key] is not None else None
                )
            else:
                assert old[key] == new[key]


def test_published_snapshot_preserves_delete_row_schema(service):
    rows, _ = snapshot_rows(Options.parse({}, {}), 0, 64 * 1024 * 1024)
    deleted = next(row for row in rows if row["operation"] == "delete")
    assert deleted["record"] is None
    assert deleted["event_payload"] == "null"
    assert deleted["raw_payload"] == ""


def test_published_snapshot_maps_protocol_errors(service):
    service.data = service.data[:-1] + bytes([service.data[-1] ^ 1])
    with pytest.raises(ProtocolError, match="checksum"):
        snapshot_rows(Options.parse({}, {}), 0, 64 * 1024 * 1024)
