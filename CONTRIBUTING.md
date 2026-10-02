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
They simulate HTTP at `requests.Session.send`, including actual compressed archive
bytes, and exercise the real framework adapter and schema conversion. The upstream
generic JSON API simulator is not sufficient for the binary ranged archive protocol.
No source credentials or live service are needed. Live tests are explicitly opt-in.

Preserve `(start, end]` boundaries, marker events, generation pinning, bounded resources,
and fail-closed decoding. Add regression tests for changes to these behaviors. Do not
log response bodies, headers, tokens, DIDs, or record content. Report only sanitized
failures. Keep secrets in environment variables or secret stores, never fixtures.

For upstreaming, recheck the latest interfaces and existing proposals. Copy the source
directory to `src/databricks/labs/community_connector/sources/bluesky`, and the tests to
the corresponding test directory. The source directory includes its own `pyproject.toml`
for the upstream per-connector wheel builder. Root `uv.lock` remains the standalone
project's development lock. Run upstream's required generic/record-mode validation
and self-review procedures before requesting merge; the offline tests here do not
substitute for authenticated workspace validation. Follow upstream CONTRIBUTING.md
and its current maintainer CI approval rules.

Contributions are licensed under Apache-2.0, the same license as this project. Do not
add third-party fixtures without recording their source and license in NOTICE.
