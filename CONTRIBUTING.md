# Contributing

Use Python 3.10+ and uv. Run:

```sh
uv sync --locked --extra dev --no-editable
uv run --no-editable ruff format .
uv run --no-editable ruff check .
uv run --no-editable pytest
uv build
```

Tests live under `tests/unit/sources/bluesky`, following the upstream per-source layout.
The local HTTP simulator serves actual compressed archive bytes. The upstream shared
test harness is also configured in `test_bluesky_lakeflow_connect.py`, using custom
simulator handlers for the JSON planner and binary HTTP Range endpoint. It runs when
copied into Databricks' repository; the standalone package does not vendor the shared
test suite. No source credentials are needed for offline tests. Live tests are opt-in.

Preserve `(start, end]` boundaries, marker events, generation pinning, bounded resources,
and fail-closed decoding. Add regression tests for changes to these behaviors. Do not
log response bodies, headers, tokens, DIDs, or record content. Report only sanitized
failures. Keep secrets in environment variables or secret stores, never fixtures.

For upstreaming, recheck the latest interfaces and existing proposals. Copy the source
directory and tests to the matching upstream paths, plus the Bluesky simulator spec
from `src/databricks/labs/community_connector/source_simulator/specs/bluesky`.
Add `data_source.py` to the Bluesky entry in upstream's
`tools/scripts/merge_exclude_config.json` (the reviewed version is included here),
then run `python tools/scripts/merge_python_source.py bluesky` and commit the output.
Run `tools/scripts/regenerate-locks.sh` and commit the updated `requirements/sources.txt`;
upstream CI installs per-source wheels with `--no-deps`. The source directory includes
its own `pyproject.toml` for that wheel. Root `uv.lock` is only this standalone
project's development lock. At the reviewed upstream revision, the lock generator's
global PyPI cutoff is 2026-04-22 while `jetstream-lakehouse==0.1.0` was published
on 2026-10-03. Coordinate a reviewed cutoff update or a targeted package exception
with maintainers before regenerating on Linux; silently bypassing the cutoff is
not appropriate. Run upstream's generic tests, live/record validation
where appropriate, and self-review before requesting merge. The archive's binary
Range responses and changing sealed sequence tip cannot be compared byte-for-byte
to a fixed JSON corpus by the generic record-mode drift validator. Run the three
bounded authenticated tests in `tests/integration/test_live.py` as live contract
evidence and discuss the validator limitation with maintainers. Follow its current
maintainer CI approval rules.

The upstream self-review checklist is written primarily for JSON list APIs.
For this source, `max_events_per_refresh` bounds a unique sequence interval,
so its row count cannot exceed the configured span; the pinned sealed tip
serves the termination role of the checklist's wall-clock `_init_time` cap.
`BlueskyDataSource` remains in `data_source.py` and is re-exported from
`__init__.py`; its registration is exercised by unit tests. These are
intentional protocol/packaging adaptations to explain in the upstream review,
not missing bounds or an unregistered source.

Contributions are licensed under Apache-2.0, the same license as this project. Do not
add third-party fixtures without recording their source and license in NOTICE.
