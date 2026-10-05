# Protocol and compatibility verification — 2026-10-02

Read-only reference checkouts were inspected before implementation:

- Lakeflow community connectors: `ddc153b9741b686759ad85f25d2d45ca8a6ac24b`.
- Bluesky Jetstream: `3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d`.
- Upstream source/tests contained no Bluesky/ATProto/Jetstream connector. GitHub's
  issue search returned zero results for `repo:databrickslabs/lakeflow-community-connectors
  bluesky` and the alternate terms `jetstream OR atproto`, including open and closed PRs.
  Recheck before proposing a contribution.

## Primary references

- [Databricks custom connectors](https://docs.databricks.com/aws/en/ingestion/custom-connectors)
- [Lakeflow interface](https://github.com/databrickslabs/lakeflow-community-connectors/blob/ddc153b9741b686759ad85f25d2d45ca8a6ac24b/src/databricks/labs/community_connector/interface/lakeflow_connect.py)
- [Partitioned-stream interface](https://github.com/databrickslabs/lakeflow-community-connectors/blob/ddc153b9741b686759ad85f25d2d45ca8a6ac24b/src/databricks/labs/community_connector/interface/supports_partition.py)
- [Spark adapter and replay behavior](https://github.com/databrickslabs/lakeflow-community-connectors/blob/ddc153b9741b686759ad85f25d2d45ca8a6ac24b/src/databricks/labs/community_connector/sparkpds/lakeflow_datasource.py)
- [Jetstream v2 overview](https://bsky.network/docs/jetstream/)
- [Replay and snapshots](https://bsky.network/docs/jetstream-replay/)
- [Archive layout and invariants](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/docs/README.md)
- [Snapshot lexicon](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/lexicons/network/bsky/jetstream/planSnapshot.json)
- [Segment HTTP handler](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/internal/xrpcapi/getsegment.go)
- [Official event decoder](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/decode.go)
- [Bluesky getFeed](https://docs.bsky.app/docs/api/app-bsky-feed-get-feed)

## Decisions

Use HTTP-only archive snapshots, not the SDK's replay-plus-live loop. The planner's
exclusive `afterSeq` and inclusive `beforeSeq` establish a finite interval. Its first
`sealedTipSeq` caps all following pages. `plannedThroughSeq` advances even when exact
filters yield no records. `kinds`, `collections`, and `dids` are sent to the planner and
rechecked against decoded rows; collections require `commit` in the selected kinds. The
live socket's inclusive cursor and 36-hour lookback floor
are not part of this connector's transport.

Both planner modes are supported through ranged `getSegment` reads. Blocks mode
selects the listed block indexes; segment mode uses the segment's block index and
the bounded sequence interval. Downloading a full segment is unnecessary. Every range
uses the plan checksum as `If-Match` and validates the returned strong ETag. The decoder
checks jss0/v1 metadata with xxh3 and Zstandard frame content checksums, then decodes
the columnar metadata and DAG-CBOR into the AT Protocol JSON data model.

Planning is separate from Spark's durable commit. Offsets contain primitive values,
including a JSON string holding the plan. A retried partition never discovers a new
tip or replans. This avoids the simple reader's current `readBetweenOffsets` behavior
that ignores its `end` argument. The recorded plan pins archive generations; archive
mutability cannot be turned into full historical exactly-once delivery without durable
staging outside Jetstream. This connector fails when a pinned generation becomes unavailable.

No time-based start option is offered: current archive plans accept sequences, while
legacy timestamp cursor translation is a live-socket feature. No fabricated endpoint,
retention-floor field, JSON archive endpoint, or pagination token is assumed.

The major unresolved source constraint is historical completeness. The current API
does not expose enough information to reject every already-compacted historical start.
The connector detects explicit source errors and generation loss but cannot prove an
arbitrary archive range contains all events that originally existed. Users requiring
that guarantee need an independently retained event log/mirror, not an automatic
fallback that silently changes semantics.
