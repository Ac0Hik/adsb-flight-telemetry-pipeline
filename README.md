# ✈️ ADS-B Flight Telemetry Pipeline

A production-grade data engineering pipeline that ingests real-time aircraft position data from the OpenSky Network and processes it through a Lambda architecture — a continuous streaming layer for near real-time ingestion, and a nightly batch layer for flight reconstruction, anomaly detection, and analytics aggregation. The serving layer exposes business-ready metrics via dbt models on top of Delta Lake gold tables.

Built entirely on free infrastructure: Databricks Community Edition for Spark compute and Delta Lake storage, Apache Airflow running locally via Docker, and OpenSky Network for live ADS-B data.

---

Infrastructure
All infrastructure is managed as code via Terraform using two providers.
Local — Docker (✅ complete)
The full Airflow stack runs locally via Docker, provisioned entirely by Terraform:

Postgres — Airflow metadata database with a healthcheck (pg_isready) and a persistent volume so history survives restarts
Airflow init — one-shot container that runs db migrate and creates the admin user, then exits
Airflow webserver — UI accessible at http://localhost:8080
Airflow scheduler — monitors and triggers DAG tasks

All containers share a dedicated Docker network and communicate by container name. DAGs and logs are mounted as volumes so changes are reflected instantly without rebuilding.
terraform apply spins the full stack up. terraform destroy tears it down cleanly.
Cloud — Databricks (🔲 in progress)