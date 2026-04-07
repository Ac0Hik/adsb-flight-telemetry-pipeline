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

## Silver Layer

**Flight reconstruction** (`spark/notebooks/03_silver_flights.ipynb`) — runs on Databricks, reads bronze Delta table from Volumes and reconstructs logical flights from raw ADS-B snapshots.

- Deduplicates on `(icao24, api_timestamp)` and drops null coordinates
- Detects flight segments using window functions — new flight on takeoff or signal gap > 600 seconds
- Aggregates to one row per flight with departure/arrival timestamps, altitude, speed, and position metrics
- Applies geo UDFs to identify origin/destination airports and estimate flight distance
- Filters noise — flights shorter than 5 minutes or fewer than 10 state reports excluded
- Writes to silver Volumes via Delta MERGE — idempotent, safe to re-run

Silver table written to `/Volumes/workspace/default/adsb_data/silver/flights`.

> See `spark/NOTES.md` for known limitations and future improvements.


**Anomaly detection** is the process of identifying aircraft behaviour that deviates from what is expected during normal flight operations. In aviation, anomalies can indicate emergencies, equipment failures, or safety-critical situations that require immediate attention from air traffic control.

**Anomaly detection** (`spark/notebooks/04_anomaly_detect.ipynb`) — runs on Databricks, reads bronze Delta table from Volumes and applies 5 detection rules across all state vectors. Anomaly detection identifies aircraft behaviour that deviates from normal flight operations — flagging emergencies, equipment failures, and safety-critical situations that require attention from air traffic control.

- `EMERGENCY_SQUAWK` — transponder code 7700 (general emergency), 7600 (radio failure), or 7500 (hijack in progress). Severity: CRITICAL
- `RAPID_ALTITUDE_DROP` — altitude drops more than 500m between consecutive states while airborne above 1000m. Severity: HIGH
- `EXTREME_VERTICAL_RATE` — climb or descent rate exceeds 25 m/s while airborne. Severity: CRITICAL if above 40 m/s, HIGH otherwise
- `UNUSUAL_SPEED` — velocity exceeds 350 m/s, or falls below 50 m/s at altitude above 6000m while airborne. Severity: MEDIUM
- `SIGNAL_GAP` — same aircraft reappears after a 5–60 minute gap while still airborne. Severity: MEDIUM

Each detected event is written as a row with the aircraft state, anomaly type, severity, and a human-readable description of the event.

Anomaly table written to `/Volumes/workspace/default/adsb_data/silver/anomalies`.
