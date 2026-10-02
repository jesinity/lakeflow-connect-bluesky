"""Run as a triggered Lakeflow pipeline with both wheels installed.

Create a COMMUNITY Unity Catalog connection named bluesky_jetstream first.
See README.md for dependency and connection setup.
"""

from databricks.labs.community_connector.pipeline import ingest

from databricks.labs.community_connector.sources.bluesky import BlueskyDataSource

# Databricks supplies spark to this pipeline module.
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")  # noqa: F821
spark.dataSource.register(BlueskyDataSource)  # noqa: F821

pipeline_spec = {
    "connection_name": "bluesky_jetstream",
    "objects": [
        {
            "table": {
                "source_table": "events",
                "destination_catalog": "main",
                "destination_schema": "bluesky",
                "destination_table": "events",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "starting_cursor": "0",
                    "collections": "app.bsky.feed.post",
                    "max_events_per_refresh": "10000",
                },
            }
        }
    ],
}

ingest(spark, pipeline_spec)  # noqa: F821
