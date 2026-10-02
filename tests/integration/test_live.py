"""Explicitly opt in; public archive reads are authenticated and metered."""

import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from dynaconf import Dynaconf

from databricks.labs.community_connector.sources.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sources.bluesky.options import Options
from databricks.labs.community_connector.sources.bluesky.transport import Transport

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENDPOINT = "https://jetstream.us-east.bsky.network"


@dataclass(frozen=True)
class LiveSettings:
    """Credentials and cursor used by explicitly enabled live contract tests.

    Attributes
    ----------
    endpoint : str
        Jetstream HTTPS origin used by the test.
    api_key : str
        Authentication token, excluded from the dataclass repr.
    starting_cursor : int or None
        Optional exclusive cursor for a caller-selected archive range.
    """

    endpoint: str
    api_key: str = field(repr=False)
    starting_cursor: int | None = None


def keychain_key(settings: Dynaconf) -> str | None:
    if sys.platform != "darwin":
        return None
    command = [
        "security",
        "find-generic-password",
        "-s",
        str(settings.get("KEYCHAIN_ITEM", "Bluesky Jetstream API key")),
    ]
    account = settings.get("KEYCHAIN_ACCOUNT")
    if account:
        command.extend(["-a", str(account)])
    command.append("-w")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def live_settings() -> LiveSettings:
    settings = Dynaconf(
        envvar_prefix="JETSTREAM",
        settings_files=[str(ROOT / ".secrets.toml")],
        environments=False,
        load_dotenv=False,
    )
    api_key = settings.get("API_KEY") or keychain_key(settings)
    if not isinstance(api_key, str) or not api_key:
        pytest.fail(
            "Jetstream key missing: set JETSTREAM_API_KEY, add API_KEY to the ignored "
            ".secrets.toml, or configure a generic Keychain Access password. "
            "Apple Passwords app entries are not read by the Keychain lookup."
        )
    configured_cursor = settings.get("STARTING_CURSOR")
    return LiveSettings(
        endpoint=settings.get("ENDPOINT", DEFAULT_ENDPOINT),
        api_key=api_key,
        starting_cursor=int(configured_cursor) if configured_cursor is not None else None,
    )


@pytest.mark.live
@pytest.mark.skipif(os.getenv("BLUESKY_LIVE_TEST") != "1", reason="live test is opt-in")
def test_live_bounded_read():
    config = live_settings()
    endpoint, api_key, cursor = config.endpoint, config.api_key, config.starting_cursor
    if cursor is None:
        with Transport(Options.parse({"endpoint": endpoint, "api_key": api_key}, {})) as transport:
            sealed_tip = transport.plan(0)["sealedTipSeq"]
        cursor = str(max(0, sealed_tip - 100))
    else:
        cursor = str(cursor)
    connector = BlueskyLakeflowConnect(
        Options.parse(
            {
                "endpoint": endpoint,
                "api_key": api_key,
                "starting_cursor": cursor,
                "max_events_per_refresh": "100",
                "refresh_timeout_seconds": "120",
            },
            {},
        )
    )
    rows, end = connector.read_table("events", None, {})
    rows = list(rows)
    assert len(rows) <= 100
    assert all(int(cursor) < row["seq"] <= end["seq"] for row in rows)
    empty, same = connector.read_table("events", end, {})
    assert list(empty) == [] and same == end


@pytest.mark.live
@pytest.mark.skipif(os.getenv("BLUESKY_LIVE_TEST") != "1", reason="live test is opt-in")
def test_live_kinds_and_collections_filter_contract():
    config = live_settings()
    endpoint, api_key = config.endpoint, config.api_key
    cursor = config.starting_cursor
    if cursor is None:
        with Transport(Options.parse({"endpoint": endpoint, "api_key": api_key}, {})) as transport:
            tip = transport.plan(0)["sealedTipSeq"]
        cursor = max(0, tip - 100)
    connector = BlueskyLakeflowConnect(
        Options.parse(
            {
                "endpoint": endpoint,
                "api_key": api_key,
                "starting_cursor": str(cursor),
                "max_events_per_refresh": "100",
                "refresh_timeout_seconds": "120",
                "kinds": "commit",
                "collections": "app.bsky.feed.post",
            },
            {},
        )
    )
    rows, end = connector.read_table("events", None, {})
    rows = list(rows)
    assert all(row["kind"] == "commit" for row in rows)
    assert all(row["collection"] == "app.bsky.feed.post" for row in rows)
    assert int(cursor) < end["seq"] <= int(cursor) + 100


@pytest.mark.live
@pytest.mark.skipif(os.getenv("BLUESKY_LIVE_TEST") != "1", reason="live test is opt-in")
def test_live_planner_pagination_contract():
    config = live_settings()
    endpoint, api_key = config.endpoint, config.api_key
    cursor = config.starting_cursor
    if cursor is None:
        with Transport(Options.parse({"endpoint": endpoint, "api_key": api_key}, {})) as transport:
            tip = transport.plan(0)["sealedTipSeq"]
        cursor = max(0, tip - 100)
    cursor = int(cursor)
    before = cursor + 100
    options = Options.parse(
        {
            "endpoint": endpoint,
            "api_key": api_key,
            "refresh_timeout_seconds": "120",
        },
        {"kinds": "commit,identity,account,sync"},
    )
    pages = []
    covered = cursor
    pinned_tip = None
    with Transport(options) as transport:
        for _ in range(100):
            page = transport.plan(covered, before)
            sealed_tip = int(page["sealedTipSeq"])
            planned_through = int(page["plannedThroughSeq"])
            if pinned_tip is None:
                pinned_tip = sealed_tip
            assert sealed_tip == pinned_tip
            assert cursor <= covered <= planned_through <= sealed_tip <= before
            assert isinstance(page["segments"], list)
            pages.append(page)
            if planned_through == sealed_tip:
                break
            assert planned_through > covered
            covered = planned_through
        else:
            pytest.fail("planner exceeded 100 pages for a 100-sequence window")
    assert pages
