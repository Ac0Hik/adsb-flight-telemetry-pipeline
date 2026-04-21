# Local REST API

Bridges Airflow (Docker) to host processes. Must be started from the project root:

```bash
uvicorn api.main:app --port 8000 --reload
```

See `airflow/NOTES.md` for Airflow connection setup.