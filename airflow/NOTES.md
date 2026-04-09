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
