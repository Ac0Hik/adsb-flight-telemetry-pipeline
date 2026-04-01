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
- Delta tables stored on DBFS, dbt models served via the serverless SQL warehouse

Spark jobs run locally in `local[*]` mode and write directly to DBFS.

`terraform apply` spins everything up. `terraform destroy` tears it all down cleanly.

Data Ingestion
OpenSky API client (spark/utils/opensky_client.py) — a lightweight wrapper around the OpenSky REST API:

fetch_states() — polls https://opensky-network.org/api/states/all with optional bounding box, handles errors gracefully
parse_states() — maps raw state vectors to typed dicts with named fields, strips callsign whitespace, adds ingested_at UTC timestamp
poll_forever() — generator that yields parsed batches on a configurable interval

Supports authenticated requests (4,000 calls/day) and anonymous fallback (400/day).