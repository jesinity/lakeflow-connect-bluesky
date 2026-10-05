# Bluesky Lakeflow community connector

Source: `bluesky`. Table: `events`. Ingestion: append, primary key/cursor `seq`.
Implements `LakeflowConnect` and `SupportsPartitionedStream`.

Register
`BlueskyDataSource` through `spark.dataSource.register` or `find_data_source("bluesky")`.
This directory is the upstream-compatible connector package; do not install its
per-source distribution alongside the standalone distribution (they own the same files).
It depends on `jetstream-lakehouse==0.1.0` for archive transport and decoding.

`data_source.py` registers the Spark data source, `options.py` validates settings,
`bluesky.py` plans bounded windows and replays recorded checkpoints, and
`jetstream_lakeflow_adapter.py` converts library events to Lakeflow rows. The
archive transport and binary decoder are provided by `jetstream-lakehouse`.

Jetstream v2 HTTP snapshots provide bounded replay of the retrievable sealed archive.
Each triggered update covers at most 10,000 sequence positions by default. The tip
and archive checksums are recorded in the candidate offset; Spark commits it only
after its Delta batch succeeds. Retries read the recorded plan and fail if compaction
has removed its generation. Continuous pipelines and legacy v1 sockets are unsupported.

Connection options: `endpoint` (HTTPS origin, default
`https://jetstream.us-east.bsky.network`), `api_key` (raw bearer key; public archive
access requires one). Mark the key secret in Unity Catalog; see `connector_spec.yaml`.
Create a COMMUNITY connection with `sourceName=bluesky`, then install the connector,
the matching Lakeflow framework, and `jetstream-lakehouse` in a triggered pipeline.
For example, the pipeline spec can select posts with:

```json
{
  "connection_name": "bluesky_jetstream",
  "objects": [{"table": {
    "source_table": "events",
    "destination_catalog": "main",
    "destination_schema": "bluesky",
    "destination_table": "events",
    "table_configuration": {
      "scd_type": "APPEND_ONLY",
      "starting_cursor": "0",
      "collections": "app.bsky.feed.post"
    }
  }}]
}
```

Replace the destination and start cursor for your workspace. A zero start cursor
can take many bounded updates to reach current events.

Table options are strings. Pydantic validates them after merging connection and
table settings.

| Option | Default |
| --- | --- |
| `starting_cursor` (exclusive sequence, not timestamp) | `0` |
| `collections` (comma-separated NSIDs or namespace wildcards) | all |
| `kinds` (comma-separated `commit`, `identity`, `account`, or `sync`) | all |
| `dids` (comma-separated DIDs) | all |
| `max_events_per_refresh` (sequence span, 1–100,000) | `10000` |
| `request_timeout_seconds` | `20` |
| `refresh_timeout_seconds` | `120` |
| `max_retries` | `3` |
| `include_raw_payload` | `true` |

Kinds are filtered by the planner and checked again after decoding. Set `collections`
only when `kinds` includes `commit`; collection filters apply to commit events.

`events` retains sequence, display/witness timestamps, DID, kind, operation, collection,
rkey, CID, revision, a resync flag, decoded record/event JSON strings, and optional raw
DAG-CBOR bytes. Delete/account/sync markers remain append events. Collection filtering
does not remove non-commit markers. Use them when deriving current record state.

Archive compaction removes superseded historical records before some reads. The API
does not expose a comprehensive compaction floor, so this source cannot certify a
complete historical audit log. A sealed archive also has greater latency than a live
socket. Never change endpoints with an existing checkpoint; sequences are instance-local.
Checkpoint resets into an existing append table may produce duplicates.

Repository events are not hydrated Bluesky views. Profiles, usernames, threads, and
engagement counts need AppView enrichment. Full setup, SQL, and operational guidance
are in the [standalone project's README](https://github.com/jesinity/lakeflow-connect-bluesky)
and its `docs/protocol.md`.
