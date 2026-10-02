-- Current retrievable post state from a single Jetstream instance.
-- Keep account and sync markers: collection filters do not remove them.
-- Replace main.bluesky with your catalog/schema. SQL is illustrative and
-- has not been executed against a Databricks workspace.
CREATE OR REPLACE VIEW main.bluesky.posts AS
WITH unique_events AS (
  SELECT * FROM main.bluesky.events
  QUALIFY ROW_NUMBER() OVER (PARTITION BY seq ORDER BY witnessed_at) = 1
), account_resets AS (
  SELECT did, MAX(seq) AS reset_seq
  FROM unique_events
  WHERE kind = 'sync'
     OR (kind = 'account'
         AND get_json_object(event_payload, '$.active') = 'false'
         AND get_json_object(event_payload, '$.status') = 'deleted')
  GROUP BY did
), latest_posts AS (
  SELECT e.*
  FROM unique_events e
  LEFT JOIN account_resets a ON e.did = a.did
  WHERE e.kind = 'commit' AND e.collection = 'app.bsky.feed.post'
    AND e.seq > COALESCE(a.reset_seq, 0)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.did, e.rkey ORDER BY e.seq DESC) = 1
)
SELECT seq, did, rkey,
       CONCAT('at://', did, '/app.bsky.feed.post/', rkey) AS uri,
       cid, rev, event_time,
       get_json_object(record, '$.text') AS text,
       TRY_CAST(get_json_object(record, '$.createdAt') AS TIMESTAMP) AS created_at,
       record
FROM latest_posts
WHERE operation IN ('create', 'update');
