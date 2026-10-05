# Jetstream Lakehouse integration

The connector's archive HTTP transport, DAG-CBOR decoder, segment reader, and
integrity checks now come from the pinned `jetstream-lakehouse==0.1.0` package.
`jetstream_lakeflow_adapter.py` converts decoded `Event` objects into the existing Lakeflow
schema and maps package failures to the connector's exception types. The
connector no longer contains separate archive or transport implementations.

Lakeflow's `SupportsPartitionedStream` requires two phases: `latest_offset()`
records a compact plan with segment checksums in the candidate checkpoint, and
`read_partition()` executes that exact plan on Spark, including retries. The
package's public `Client.snapshot()` plans and reads in one call. Using it inside
`read_partition()` would replan after compaction and could read a different
generation. For now, the adapter uses the package's private `Transport` and
`segment_events` entry points to retain Lakeflow's pinned replay contract. The
exact dependency version and offline replay/compaction tests limit the risk of
this private API dependency.

The next package release should expose a stable, serializable two-phase archive
plan and a read-by-plan method. The connector can then switch to that public API
without changing its options, row schema, or checkpoint scope. The standalone
wheel passed an authenticated bounded update in a Databricks test workspace.
Before applying this branch to the Databricks community repo, verify that its
per-source wheel installs the package in that environment.
