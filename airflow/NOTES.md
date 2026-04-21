<!-- airflow notes -->
## Airflow — Streaming Health Monitor Limitations

**Auto-restart not implemented**
The `restart_stream` task currently logs a warning instead of restarting the streaming job. This is because the streaming job runs locally on the host machine while Airflow runs inside Docker — the container cannot directly manage host processes.

**Volume upload not automated**
The bronze Delta table upload to Databricks Volumes is currently manual:
```bash
databricks fs cp -r "data/delta/bronze/live_states" "dbfs:/Volumes/workspace/default/adsb_data/bronze/live_states" --overwrite
```

**Future improvement**
Expose a local REST endpoint on the host machine that Airflow can call to trigger both the stream restart and the Volume upload. Airflow's `restart_stream` task would POST to this endpoint instead of calling the Databricks Jobs API. OR move the streaming job to a docker container


## FastAPI Bridge — Partial Solution

A local REST API (`api/main.py`) has been implemented to partially address the limitations above. Airflow calls it via `SimpleHttpOperator` using `host.docker.internal:8000`.

**What is now solved:**
- `run_dbt` and `test_dbt` — fully automated via `/run-dbt` and `/test-dbt` endpoints

**What is partially solved:**
- `upload_bronze` — endpoint exists at `/upload-bronze` but not yet wired to airflow (DRB-12)

**What is still not solved:**
- `restart_stream` — endpoint exists but not implemented. Stream restart still requires manual intervention.

**Airflow connection setup** — add manually via Airflow UI (Admin → Connections):
- Conn ID: `fastapi_default`
- Conn Type: `HTTP`
- Host: `http://host.docker.internal`
- Port: `8000`
