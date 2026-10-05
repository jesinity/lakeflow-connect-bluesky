"""Adapt Jetstream archive reads to Lakeflow's pinned checkpoint contract."""

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from jetstream_lakehouse import Event
from jetstream_lakehouse import errors as library_errors
from jetstream_lakehouse.archive import segment_events
from jetstream_lakehouse.transport import Transport

from databricks.labs.community_connector.sources.bluesky import errors
from databricks.labs.community_connector.sources.bluesky.options import Options


def lakeflow_row(event: Event, include_raw_payload: bool) -> dict:
    """Restore Lakeflow's base64 and non-null JSON values from a library event."""
    row = event.as_dict()
    raw = row.pop("raw_payload_base64")
    # Spark's BinaryType parser decodes base64; the existing connector returns text.
    row["raw_payload"] = raw if raw is not None else "" if include_raw_payload else None
    # Lakeflow's event_payload column is non-nullable; empty CBOR is JSON null.
    row["event_payload"] = row["event_payload"] or "null"
    return row


@contextmanager
def translated_errors() -> Iterator[None]:
    """Keep the connector's exception types at the package boundary."""
    try:
        yield
    except library_errors.ArchiveChanged as exc:
        raise errors.ArchiveChanged(str(exc)) from None
    except library_errors.CursorTooOld as exc:
        raise errors.CursorTooOld(str(exc)) from None
    except library_errors.RefreshTimeout as exc:
        raise errors.RefreshTimeout(str(exc)) from None
    except library_errors.ProtocolError as exc:
        raise errors.ProtocolError(str(exc)) from None
    except library_errors.JetstreamError as exc:
        raise errors.JetstreamError(str(exc)) from None


@contextmanager
def pinned_transport(options: Options) -> Iterator[Transport]:
    """Open the package transport while retaining the recorded archive plan.

    Notes
    -----
    Version 0.1.0 has no public two-phase archive API. Its private transport
    and decoder preserve the pinned segment checksums in Lakeflow offsets;
    the exact dependency pin and connector tests guard this temporary bridge.
    """
    with translated_errors(), Transport(options) as transport:
        yield transport


def pinned_rows(
    transport: Transport,
    entry: Mapping[str, Any],
    after: int,
    through: int,
    include_raw_payload: bool,
) -> Iterator[dict[str, Any]]:
    """Decode one recorded segment without replanning it."""
    for row in segment_events(transport, entry, after, through, include_raw_payload):
        yield lakeflow_row(Event.from_archive(row), include_raw_payload)
