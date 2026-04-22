# Local REST API

Bridges Airflow (Docker) to host processes. Must be started from the project root:

```bash
uvicorn api.main:app --port 8000 --reload
```

See `airflow/NOTES.md` for Airflow connection setup.

## Notes

**`/restart-stream` — OS compatibility**
The current implementation uses `psutil` and `subprocess.DETACHED_PROCESS` which is Windows-specific. On Linux or macOS the `creationflags=subprocess.DETACHED_PROCESS` flag is not available — replace it with `start_new_session=True` instead:

```python
# Linux/macOS
subprocess.Popen(
    ["python", "-m", "spark.jobs.01_stream_ingest"],
    cwd=PROJECT_ROOT,
    start_new_session=True
)

# Windows
subprocess.Popen(
    ["python", "-m", "spark.jobs.01_stream_ingest"],
    cwd=PROJECT_ROOT,
    creationflags=subprocess.DETACHED_PROCESS
)
```