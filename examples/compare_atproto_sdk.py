"""Compare this connector with the released AT Protocol Python SDK.

Run from the repository root after installing the dev dependencies::

    uv run --no-sync --no-editable --with 'atproto==0.0.72' \
      python examples/compare_atproto_sdk.py

Add ``--live`` to compare a recent, bounded archive window. Live mode uses the
same Dynaconf, .secrets.toml, and macOS Keychain lookup as our integration test.
"""

import argparse
import struct
import subprocess
import sys
from pathlib import Path

import zstandard
from atproto import JetstreamClient
from atproto_jetstream.archive.segment import decode_block as sdk_decode_block
from dynaconf import Dynaconf
from jetstream_lakehouse.archive import MAX_BLOCK, decode_block
from jetstream_lakehouse.transport import Transport

from databricks.labs.community_connector.sources.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sources.bluesky.options import Options

ROOT = Path(__file__).resolve().parents[1]


def compare_fixture() -> None:
    """Check both decoders against an archive produced by the official Go writer."""
    segment = (ROOT / "tests/unit/sources/bluesky/fixtures/native.jss").read_bytes()
    block_count = struct.unpack_from("<I", segment, 14)[0]
    position = 256
    our_rows = []
    sdk_rows = []
    for _ in range(block_count):
        compressed = struct.unpack_from("<Q", segment, position)[0]
        frame = segment[position + 8 : position + 8 + compressed]
        expected_size = len(
            zstandard.ZstdDecompressor().decompress(frame, max_output_size=MAX_BLOCK)
        )
        our_rows.extend(decode_block(frame, expected_size, False))
        sdk_rows.extend(sdk_decode_block(frame))
        position += 8 + compressed
    ours = [(row["seq"], row["did"], row["operation"]) for row in our_rows]
    theirs = [(row.seq, row.did, row.operation) for row in sdk_rows]
    print(f"Official-writer fixture: {len(ours)} connector rows, {len(theirs)} SDK rows")
    print(f"Sequence, DID, and operation agree: {ours == theirs}")
    if ours != theirs:
        raise SystemExit("Fixture parity failed")


def live_settings() -> tuple[str, str]:
    """Read the existing local test configuration without printing the API key."""
    settings = Dynaconf(
        envvar_prefix="JETSTREAM",
        settings_files=[str(ROOT / ".secrets.toml")],
        environments=False,
        load_dotenv=False,
    )
    key = settings.get("API_KEY")
    if not key and sys.platform == "darwin":
        command = [
            "security",
            "find-generic-password",
            "-s",
            str(settings.get("KEYCHAIN_ITEM", "Bluesky Jetstream API key")),
        ]
        account = settings.get("KEYCHAIN_ACCOUNT")
        if account:
            command.extend(["-a", str(account)])
        try:
            result = subprocess.run(
                [*command, "-w"], capture_output=True, text=True, check=False, timeout=60
            )
            if result.returncode == 0:
                key = result.stdout.strip()
        except (OSError, subprocess.TimeoutExpired):
            pass
    if not key:
        raise SystemExit(
            "Jetstream key unavailable. Set JETSTREAM_API_KEY in this shell, or configure "
            "the same .secrets.toml/Keychain item used by tests/integration/test_live.py."
        )
    return str(settings.get("ENDPOINT", "https://jetstream.us-east.bsky.network")), str(key)


def compare_live() -> None:
    """Compare one DID in a recent 100-sequence sealed archive window."""
    endpoint, key = live_settings()
    connection = {"endpoint": endpoint, "api_key": key}
    with Transport(Options.parse(connection, {})) as transport:
        tip = transport.plan(0)["sealedTipSeq"]
    after = max(0, tip - 100)
    window = {"starting_cursor": str(after), "max_events_per_refresh": "100"}
    sample = BlueskyLakeflowConnect(Options.parse(connection, window))
    sample_rows, end = sample.read_table("events", None, {})
    sample_rows = list(sample_rows)
    if not sample_rows:
        raise SystemExit("Recent window has no rows; set a different window in this script")
    did = sample_rows[0]["did"]
    through = end["seq"]

    filtered = {**window, "dids": did}
    config = Options.parse(connection, filtered)
    # The SDK downloads an entire segment when the planner returns segment mode.
    # Refuse that case so this smoke test stays small and inexpensive.
    with Transport(config) as transport:
        page = transport.plan(after, through)
    if any(entry["mode"] != "blocks" for entry in page["segments"]):
        raise SystemExit("Planner selected a whole segment; stopping before the SDK download")

    connector = BlueskyLakeflowConnect(config)
    our_rows, _ = connector.read_table("events", None, {})
    base_uri = endpoint.replace("https://", "wss://", 1).rstrip("/") + "/xrpc"
    sdk = JetstreamClient(params={"dids": [did]}, base_uri=base_uri, api_key=key)
    sdk_rows = list(sdk.snapshot(after_seq=after, before_seq=through))
    ours = [(row["seq"], row["did"], row["operation"]) for row in our_rows]
    theirs = [(row.seq, row.did, getattr(row, "operation", None)) for row in sdk_rows]
    print(f"Archive window: ({after}, {through}], DID: {did}")
    print(f"Rows: connector={len(ours)}, SDK={len(theirs)}")
    print(f"SDK bytes downloaded: {sdk.bytes_downloaded:,}")
    print(f"Sequence, DID, and operation agree: {ours == theirs}")
    if ours != theirs:
        print(f"Only in connector: {sorted(set(ours) - set(theirs))}")
        print(f"Only in SDK: {sorted(set(theirs) - set(ours))}")
        raise SystemExit("Live parity failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="compare a bounded live archive window")
    args = parser.parse_args()
    if args.live:
        compare_live()
    else:
        compare_fixture()
