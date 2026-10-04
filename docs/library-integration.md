# Jetstream Lakehouse integration branch

This branch evaluates `jetstream-lakehouse==0.1.0` against the connector without
changing the Databricks submission. `library_adapter.py` maps Lakeflow options to
the public `Client` and restores the current row schema from `Event` values.
Compatibility tests compare the published library's snapshots with the existing
connector over the same simulated archive, including filters, bounded windows,
empty payloads, and corruption failures.

The production streaming path still uses the connector's existing archive code.
Lakeflow's `SupportsPartitionedStream` splits a refresh into two phases:
`latest_offset()` writes a compact, serializable plan with pinned segment
checksums into the candidate offset; `read_partition()` later executes exactly
that plan, including on Spark retries. Replanning a snapshot on an executor
could read a different archive generation after compaction. The public library
currently offers `Client.snapshot()`, which plans **and** reads in one call but
does not expose a serializable plan or a read-by-plan method. Replacing these
two Lakeflow methods with `snapshot()` would therefore weaken retry behavior.

To finish the refactor, first add a stable public two-phase archive API to the
library: plan one bounded window and return a validated, serializable descriptor;
read exactly that descriptor and fail if any pinned generation changed. Keep
the library's existing size, page, checksum, and ordering limits. The connector
can then store the descriptor in its offset, translate `Event` values to its
unchanged schema, and remove its duplicate `archive.py` and `transport.py`.
Keep the option names and checkpoint scope compatible. Verify the per-source
wheel and Databricks pipeline can install the released library version before
changing the upstream PR.

Until that public API exists, the compatibility adapter is groundwork only.
The existing PR and its runtime behavior remain separate from this branch.
