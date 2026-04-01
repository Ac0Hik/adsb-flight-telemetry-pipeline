# ✈️ ADS-B Flight Telemetry Pipeline

A production-grade data engineering pipeline that ingests real-time aircraft position data from the OpenSky Network and processes it through a Lambda architecture — a continuous streaming layer for near real-time ingestion, and a nightly batch layer for flight reconstruction, anomaly detection, and analytics aggregation. The serving layer exposes business-ready metrics via dbt models on top of Delta Lake gold tables.

Built entirely on free infrastructure: Databricks Free Edition for Delta Lake storage and serverless SQL, Apache Airflow running locally via Docker, and OpenSky Network for live ADS-B data.

---

## Infrastructure

All infrastructure is managed as code via Terraform using two providers.

**Docker provider** — provisions the full Airflow stack locally:
- Postgres container as Airflow's metadata database
- Airflow webserver, scheduler, and init containers
- Docker network and volumes for DAGs, logs, and Postgres data
- Credentials and config passed via Terraform variables

**Databricks provider** — provisions storage and jobs on Databricks Free Edition:
- Secret scope for OpenSky credentials — never hardcoded
- 4 serverless job definitions for each Spark script
- Delta tables stored in Databricks Volumes, dbt models served via the serverless SQL warehouse


`terraform apply` spins everything up. `terraform destroy` tears it all down cleanly.

Data Ingestion
OpenSky API client (spark/utils/opensky_client.py) — a lightweight wrapper around the OpenSky REST API:

fetch_states() — polls https://opensky-network.org/api/states/all with optional bounding box, handles errors gracefully
parse_states() — maps raw state vectors to typed dicts with named fields, strips callsign whitespace, adds ingested_at UTC timestamp
poll_forever() — generator that yields parsed batches on a configurable interval

Supports authenticated requests (4,000 calls/day) and anonymous fallback (400/day).

## Streaming Ingestion
 
**PySpark Structured Streaming** (`spark/jobs/01_stream_ingest.py`) — continuously polls the OpenSky API every 60 seconds and writes raw ADS-B state vectors to a local Delta table.
 
- Rate source trigger fires every 60 seconds, calling `foreachBatch` to fetch and ingest live data
- Full schema explicitly defined (check the the official docs)
- Partitioned by `ingest_date` and `ingest_hour` for efficient downstream querying
- Checkpoint location ensures the job recovers from failures without duplicate records
- Produces ~11,000 aircraft position records per batch (European airspace)
 
Delta table written to `data/delta/bronze/live_states` then uploaded to Databricks Volumes:
```bash
databricks fs cp -r "data/delta/bronze/live_states" "dbfs:/Volumes/workspace/default/adsb_data/bronze/live_states" --overwrite
```
 
> **Note:** Part of the pipeline [stream_ingest] runs locally in `local[*]` mode due to Databricks Free Edition no longer supporting classic cluster compute. The architecture is designed for Databricks — switching from local paths to DBFS and from `local[*]` to a Databricks cluster requires changing config values.