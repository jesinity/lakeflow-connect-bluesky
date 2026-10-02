# Bluesky Connector for Databricks Lakeflow Connect

Ingest Bluesky and AT Protocol events into Databricks using Lakeflow Connect and Jetstream.

This community connector exposes one incremental, append-only `events` table using
**Jetstream v2 HTTP archive snapshots**. It implements `LakeflowConnect` and
`SupportsPartitionedStream`. It does not open a live WebSocket. The Python distribution
is `lakeflow-connect-bluesky`; the Lakeflow source name is `bluesky`.

Status: an offline-tested MVP with passing authenticated Jetstream and triggered
Databricks Spark pipeline smoke tests. Two successive direct Spark updates produced
200 distinct sequences, and the documented `ingest.py` path produced 100 rows in a
separate table. Managed Community Connector pipeline validation is still pending
because Databricks failed during Python runtime initialization, before connector code
ran. It is not an official Databricks or Bluesky product. See [protocol verification](docs/protocol.md)
for pinned upstream revisions and constraints.

## Architecture and delivery contract

1. Resume after the last committed `seq`, or `starting_cursor` on the first run.
2. Call `planSnapshot` with a conservative sequence window. Capture its sealed tip,
   pin `beforeSeq`, and paginate until `plannedThroughSeq` reaches that tip.
3. Store the complete bounded plan and its segment checksums in the candidate offset.
4. Read the header/footer and selected blocks using resumable HTTP byte ranges.
   Verify ETags, metadata xxh3, Zstandard checksums, lengths, kinds, and sequence order.
5. Decode DAG-CBOR, apply exact kind/collection/DID filters, and emit `(start, end]` records.
   False-positive archive filter matches are normal; kinds default to all four Jetstream kinds.
6. Spark commits the offset with the successful Delta micro-batch. On failure, replay
   the recorded plan. No connector method writes an external checkpoint itself.

Each connector instance pins **one refresh** and stops after at most
`max_events_per_refresh` sequence positions, including filtered or vacant positions.
This is a strict upper bound on emitted events, not a target row count. Use a
**triggered/scheduled pipeline**, with a fresh connector instance per update; continuous
mode is not supported. Large backfills require repeated updates. The instance also
pins the tip so even direct repeated `read_table` calls terminate under ongoing traffic.

The partitioned interface is used for its explicit start/end replay contract; this
MVP uses a single ordered partition. The upstream simple reader currently ignores
the end offset during recovery. Do not wrap this connector in that simple reader.
`read_table` remains available for the interface/test harness and eagerly completes
bounded I/O before returning `(iterator, candidate_offset)`. A direct caller must
consume all records and durably write them before storing that candidate offset.

HTTP snapshots use **exclusive `afterSeq`, inclusive `beforeSeq`**. This avoids the
inclusive WebSocket resume boundary. `seq` is the stable event key **within one
Jetstream instance**. Checkpoint fingerprints reject endpoint/filter changes. Delta
streaming checkpoints handle retries; append metadata alone is not a uniqueness
constraint. Resetting a checkpoint into an existing table can duplicate rows. Use
the SQL deduplication example, or a new target table for intentional replays.

## Install, test and build with uv

Python 3.10+ is required. Local compatibility tests use Python 3.12 and PySpark 4.2.
The Databricks runtime supplies PySpark, so it is a development dependency only.

```sh
uv sync --locked --extra dev --no-editable
uv run --no-editable ruff check .
uv run --no-editable ruff format --check .
uv run --no-editable pytest
uv build
```

Use `--no-editable`: the upstream framework owns a regular parent package, so a
separate editable namespace extension does not import reliably. uv tracks changes
to connector Python/YAML files and rebuilds the local wheel as needed.
`uv.lock` pins the tested framework Git revision. `uv build` uses setuptools, matching
the upstream packaging convention. Installing the wheel with pip alone does not
apply `[tool.uv.sources]`; install the matching framework revision/wheel explicitly.

CI is offline with respect to source APIs: synthetic archives exercise the real
HTTP transport, binary decoder, Spark schema conversion, and Lakeflow streaming
adapter. Dependency installation needs the normal package/Git network access.

## Configuration

Connection parameters belong in a Unity Catalog COMMUNITY connection; table options
belong in `table_configuration`. All option values are strings. Unknown options fail
validation. The packaged [connector spec](src/databricks/labs/community_connector/sources/bluesky/connector_spec.yaml)
defines the secret field and external options allowlist.

| Option | Default | Meaning |
| --- | --- | --- |
| `endpoint` | `https://jetstream.us-east.bsky.network` | Connection: v2 HTTPS origin; no path/query/userinfo. |
| `api_key` | empty | Connection: raw bearer key. Required for public archive endpoints; optional for a self-hosted archive that allows anonymous access. |
| `starting_cursor` | `0` | Exclusive starting sequence; zero means the beginning of the retrievable archive. Not a timestamp. |
| `collections` | empty | Comma-separated NSIDs or namespace wildcards, max 100; empty means all. Applies to commits only and requires `commit` in `kinds`. |
| `kinds` | all (`commit,identity,account,sync`) | Comma-separated event kinds; select any non-empty subset. |
| `dids` | empty | Comma-separated DIDs, max 10,000; empty means all. |
| `max_events_per_refresh` | `10000` | Maximum sequence span per update, 1–100,000; sparse filters can yield fewer or no rows. |
| `request_timeout_seconds` | `20` | Connect/read timeout, 1–120 seconds. |
| `refresh_timeout_seconds` | `120` | Time budget per planning/read operation, 1–900 seconds. |
| `max_retries` | `3` | Additional attempts per HTTP request, 0–8. |
| `include_raw_payload` | `true` | Retain exact event payload DAG-CBOR in a binary column. |

Retries cover connection/read failures, HTTP 408/429 and selected 5xx responses.
Backoff includes jitter and honors `Retry-After`; an excessive wait fails the refresh
instead of sleeping indefinitely. Partial byte ranges resume with the same ETag.
Authentication errors and protocol corruption fail immediately. HTTPS is mandatory,
redirects are rejected, credentials appear only in Authorization headers, and transport
exceptions never include source response bodies. Proxy/netrc auto-discovery is disabled.

Time budgets are checked between requests/chunks/blocks; a currently blocked socket
can run until its configured read timeout. Planner pages are capped at 100, serialized
plans at 2 MiB, and each compressed/decompressed block or metadata range at 32 MiB.
Unusually large self-hosted blocks fail clearly rather than allocating without limit.
The direct `read_table` API also caps materialized JSON at 64 MiB; reduce the sequence
window if that limit is hit. The Spark partition reader emits blocks incrementally.

## Table schema

| Columns | Spark type | Meaning |
| --- | --- | --- |
| `seq` | BIGINT, required | Instance-local event identifier and cursor. |
| `event_time`, `witnessed_at` | TIMESTAMP, required | Display timestamp (imported timestamp if present), and immutable witness time. UTC. |
| `did`, `kind` | STRING, required | Account and `commit`/`identity`/`account`/`sync` kind. |
| `operation`, `collection`, `rkey` | STRING | Commit create/update/delete and record location. |
| `cid`, `rev` | STRING | CID derived from exact record DAG-CBOR; revision where available. |
| `is_resync` | BOOLEAN, required | Archive kind 7, a resync replacement create. |
| `record` | STRING | Decoded record as JSON; null on deletes and non-commit events. |
| `event_payload` | STRING, required | Decoded JSON payload for every kind; JSON `null` for empty delete payload. |
| `raw_payload` | BINARY | Exact archived DAG-CBOR payload, optional. |

JSON strings accommodate arbitrary AT Protocol lexicons and nested `$link`/`$bytes`
objects without inventing a fixed record schema. The raw column is archive payload
bytes, not a fabricated WebSocket envelope. Event metadata occupies separate columns.
Malformed events and unknown kinds fail the batch; they are never silently skipped.
Delete events are appended as records, so `read_table_deletes` is intentionally not
implemented. Future normalized tables can implement CDC separately.

## Deploy to Databricks

Requires Unity Catalog, the custom/community connector feature, permissions for the
connection and destination, HTTPS access to Jetstream, and a runtime supporting the
tested Spark Python Data Source streaming interface. The deployment examples have
been exercised with a triggered Spark pipeline; adapt their names and paths to your
own workspace.

1. Build this wheel with `uv build`. Build the framework wheel from revision
   `ddc153b9741b686759ad85f25d2d45ca8a6ac24b` in a separate checkout using `uv build`.
   Upload both wheels to a Unity Catalog volume and add them and this connector's
   runtime dependencies to the pipeline environment. Do not install PySpark over the
   Databricks runtime. Dependency versions are recorded in `uv.lock`.
2. Create a COMMUNITY connection named `bluesky_jetstream`, with `sourceName=bluesky`,
   `endpoint` and the secret `api_key`. Use the current Community Connector CLI
   `create_connection` command with this repository's local `--spec` path, or your
   workspace's custom connector UI. The CLI merges framework options into the
   allowlist. Avoid supplying credentials in committed files or shell history.
3. Create a **triggered** pipeline (`continuous: false`). Install the wheels in its
   environment and add [examples/ingest.py](examples/ingest.py) as pipeline source.
   It registers `BlueskyDataSource` directly, then calls upstream `ingest`.
   Update the catalog/schema/connection and filters. An equivalent
   [pipeline spec](examples/pipeline_spec.json) is provided. Both examples use
   `starting_cursor: 0` to show the full archive boundary; for a new live feed,
   set it to a recent sealed `seq` from the same Jetstream endpoint. Leaving it
   at zero can require a very large number of bounded updates to catch up.
4. Run an initial small update, inspect the table and checkpoint, then schedule
   subsequent updates. Preserve checkpoints and the same Jetstream origin.
5. Use [examples/posts.sql](examples/posts.sql) to derive current post state with
   record deletes, account deletions, and sync reset markers applied.

For an eventual upstream contribution, copy the `bluesky` source directory and
`tests/unit/sources/bluesky` into the matching upstream paths. See
[CONTRIBUTING.md](CONTRIBUTING.md) for packaging and validation requirements.

## Limitations and operations

- **Archive events are not a complete immutable historical audit log.** Jetstream
  compaction physically removes superseded creates/updates. Earlier events may already
  be absent when a first plan is captured. The snapshot API exposes no comprehensive
  retention/compaction floor, so a client cannot certify all original events are still
  present. Do not interpret successful replay as proof of complete history.
- Explicit `CursorTooOld`, missing segments, checksum changes, regressed tips, stalled
  plans, and corrupt payloads fail without advancing a committed offset. If a segment
  compacts between planning and a Spark retry, the recorded plan cannot be recovered
  from that generation. Preserve the checkpoint and investigate; recovering requires
  the pinned bytes from a retained mirror or an explicitly reviewed replay to a new
  target/checkpoint. The connector never automatically accepts a different generation.
- Sequence holes are valid after compaction, filtering, or crash-reserved vacancies.
  Numeric gaps alone cannot diagnose lost data. A private server silently pruning
  archives without reporting that fact is not detectable through this API.
- Only sealed segments are visible. Latency includes segment sealing; this is not a
  low-latency firehose connector. No automatic failover between origins is attempted.
- Defaults prioritize small bounded updates. Full-network catch-up will need measured
  sizing and update cadence; do not assume the default 10,000-sequence window keeps
  pace with the network. Monitor cursor progress, sealed-tip lag, rows/bytes, refresh
  duration, retry/429 rates, and failed checksum checks.
- Bootstrap archive rows are repository state observations, not necessarily every
  original historical operation. API replay guarantees cover retrievable records.
- Jetstream provides repository events, not hydrated Bluesky views. Handles, profiles,
  threads and engagement counts need AppView enrichment. `getFeed` is a hydrated feed
  API, not a replacement durable cursor source.
- The append table retains data until you explicitly apply downstream retention and
  deletion policies. SQL current-state views do not physically erase the raw table.

Opt-in live smoke test (reads at most 100 sequence positions and may consume metered
archive bytes): set `BLUESKY_LIVE_TEST=1`, then run
`uv run --extra dev --no-editable pytest tests/integration -m live`. The test uses
Dynaconf to read `JETSTREAM_API_KEY` from the environment or `API_KEY` from a local
`.secrets.toml` file (copy [secrets.example.toml](secrets.example.toml)); environment
variables override file values. On macOS it can also read a generic password from
Keychain Access named `Bluesky Jetstream API key`, or an item named by
`KEYCHAIN_ITEM`/`KEYCHAIN_ACCOUNT` in `.secrets.toml`. Apple's Passwords app entries
are not generic Keychain items; use a local secrets file or environment variable
for those. To use the file, run `cp secrets.example.toml .secrets.toml` and
`chmod 600 .secrets.toml`, then fill in `API_KEY`; remove an empty
`JETSTREAM_API_KEY` from the shell with `unset JETSTREAM_API_KEY`. Never commit
`.secrets.toml` or print the key. The test discovers the current sealed tip
and reads a recent window by default. Set
`JETSTREAM_STARTING_CURSOR` or `STARTING_CURSOR` in `.secrets.toml` only to test a
specific historical range; optionally set `JETSTREAM_ENDPOINT` or `ENDPOINT`.
CI does not set these variables.

Next milestone: validate the managed Community Connector ingestion path after the
Databricks Python runtime startup issue is resolved, exercise forced task recovery,
and benchmark sealing lag/throughput before upstream review.

## License

Apache-2.0. See [LICENSE](LICENSE) and [CONTRIBUTING.md](CONTRIBUTING.md).
