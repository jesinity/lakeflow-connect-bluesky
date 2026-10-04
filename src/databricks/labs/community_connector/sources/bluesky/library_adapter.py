"""Translate the public Jetstream client API to Lakeflow's existing row contract."""

from jetstream_lakehouse import Client, Config, Event
from jetstream_lakehouse import errors as library_errors

from databricks.labs.community_connector.sources.bluesky import errors
from databricks.labs.community_connector.sources.bluesky.options import Options


def source_client(options: Options, max_batch_bytes: int) -> Client:
    """Construct the published client without changing Lakeflow option defaults."""
    return Client(
        Config(
            endpoint=options.endpoint,
            api_key=options.api_key,
            collections=options.collections,
            kinds=options.kinds,
            dids=options.dids,
            max_sequence_span=options.max_events_per_refresh,
            max_batch_bytes=max_batch_bytes,
            request_timeout_seconds=options.request_timeout_seconds,
            refresh_timeout_seconds=options.refresh_timeout_seconds,
            max_retries=options.max_retries,
            include_raw_payload=options.include_raw_payload,
        )
    )


def lakeflow_row(event: Event, include_raw_payload: bool) -> dict:
    """Restore Lakeflow's base64 and non-null JSON values from a library event."""
    row = event.as_dict()
    raw = row.pop("raw_payload_base64")
    # Spark's BinaryType parser decodes base64; the existing connector returns text.
    row["raw_payload"] = raw if raw is not None else "" if include_raw_payload else None
    # Lakeflow's event_payload column is non-nullable; empty CBOR is JSON null.
    row["event_payload"] = row["event_payload"] or "null"
    return row


def snapshot_rows(options: Options, after: int, max_batch_bytes: int) -> tuple[list[dict], int]:
    """Read a complete public-library batch and translate sanitized failures."""
    try:
        batch = source_client(options, max_batch_bytes).snapshot(after)
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
    rows = [lakeflow_row(event, options.include_raw_payload) for event in batch.events]
    return rows, batch.through_seq
